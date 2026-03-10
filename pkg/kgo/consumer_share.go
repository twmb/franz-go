package kgo

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

var errNotShareGroup = errors.New("client is not configured as a share group consumer")

// AckStatus represents the acknowledgement type for share group records.
type AckStatus int8

const (
	// AckAccept acknowledges successful processing (type 1).
	AckAccept AckStatus = 1
	// AckRelease releases the record for redelivery (type 2).
	AckRelease AckStatus = 2
	// AckReject marks the record as unprocessable (type 3).
	AckReject AckStatus = 3
	// AckRenew extends the acquisition lock (type 4, KIP-1222).
	AckRenew AckStatus = 4
)

// ackFinalized is set on shareAckState.status once the ack has been committed
// for sending. After this bit is set, Acknowledge() is a no-op.
const ackFinalized = 0x80

// ackGap is the wire type for gap offsets (compacted records within an
// acquired range that have no physical record). The broker uses this to
// release the acquisition without treating the offset as processed or
// rejected.
const ackGap int8 = 0

// shareAckState is injected into each share group Record's Context via
// context.WithValue. It tracks the ack status (pending, user-set, or
// finalized) and the delivery count from the broker.
type shareAckState struct {
	mu                      sync.Mutex
	status                  uint8 // 0=pending, low 7 bits=AckStatus, high bit=ackFinalized
	deliveryCount           int32
	acquisitionLockDeadline time.Time
	source                  *source // the source that fetched this record
}

var ctxShareAck = new(string) // pointer key for context.WithValue

func shareAckFromCtx(r *Record) *shareAckState {
	if r.Context == nil {
		return nil
	}
	v := r.Context.Value(ctxShareAck)
	if v == nil {
		return nil
	}
	return v.(*shareAckState)
}

// shareAckBatch represents a contiguous range of offsets with an ack type,
// ready to be sent in a ShareFetch or ShareAcknowledge request.
type shareAckBatch struct {
	firstOffset int64
	lastOffset  int64
	ackType     int8 // single type for the whole batch
}

// topicPartIdx identifies a topic+partition by topicID, used as a map key
// for O(1) partition lookup when building ShareFetch requests.
type topicPartIdx struct {
	topicID   [16]byte
	partition int32
}

// shareMove collects a partition whose leader changed, detected via
// CurrentLeader in a ShareFetch response. Applied after all parallel
// fetches complete to move the share cursor to the new leader.
type shareMove struct {
	topicID     [16]byte
	partition   int32
	leaderID    int32
	leaderEpoch int32
}

// shareFetchResult bundles the output of a single ShareFetch round-trip.
type shareFetchResult struct {
	fetch   Fetch
	moves   []shareMove
	brokers []BrokerMetadata // from resp.NodeEndpoints, for ensuring move targets
}

// ShareAckPartitionError is returned within the error from CommitAcks when
// a per-partition error occurs in a ShareAcknowledge response.
type ShareAckPartitionError struct {
	Topic     string
	Partition int32
	Err       error
}

func (e *ShareAckPartitionError) Error() string {
	return fmt.Sprintf("share ack error for %s[%d]: %v", e.Topic, e.Partition, e.Err)
}

func (e *ShareAckPartitionError) Unwrap() error {
	return e.Err
}

type shareCursor struct {
	topic      string
	topicID    [16]byte
	partition  int32
	source     *source
	cursorsIdx int // index in source.shareCursors, under source.cursorsMu
}

// sharePolledRecord tracks a record from the previous poll so we can
// auto-ack and route the ack to the right source.
type sharePolledRecord struct {
	state     *shareAckState
	source    *source // the source that fetched this record
	topic     string
	partition int32
	offset    int64
}

type shareConsumer struct {
	c   *consumer
	cl  *Client
	cfg *cfg

	ctx    context.Context
	cancel func()

	manageDone chan struct{}
	tps        *topicsPartitions
	reSeen     map[string]bool

	memberGen   groupMemberGen
	nowAssigned amtps

	unresolvedAssigned map[topicID][]int32

	mu       sync.Mutex
	managing bool
	dying    atomic.Bool
	left     chan struct{}
	leaveErr error

	usingShareCursors map[string]map[int32]*shareCursor

	// sourcesWithForgotten tracks sources that had share cursors
	// removed and may still need to send ForgottenTopicsData to the
	// broker. Cleaned up by cleanupForgottenSources after poll
	// rounds drain the state. Protected by c.mu.
	sourcesWithForgotten map[*source]struct{}

	// pollSourceOffset rotates which sources are selected when
	// maxConcurrentFetches < len(sources), ensuring fair coverage
	// across poll calls. Protected by c.mu.
	pollSourceOffset int

	// cursorsChanged is signaled by assignSharePartitions (under c.mu)
	// when share cursors are added or removed. pollShareFetches waits
	// on this when no sources are available, avoiding a busy-wait loop.
	cursorsChanged *sync.Cond

	// lastPolled tracks records from the previous poll for auto-ACCEPT.
	// Protected by consumer.mu (grabbed during PollFetches/PollRecords).
	lastPolled []sharePolledRecord

	// bufferedFetches holds records from a previous ShareFetch that
	// were not returned to the user due to maxPollRecords. On the next
	// poll, records are served from this buffer before issuing new
	// ShareFetch requests. Protected by consumer.mu.
	bufferedFetches Fetches
}

func (c *consumer) initShare() {
	ctx, cancel := context.WithCancel(c.cl.ctx)
	s := &shareConsumer{
		c:   c,
		cl:  c.cl,
		cfg: &c.cl.cfg,

		ctx:    ctx,
		cancel: cancel,

		reSeen: make(map[string]bool),

		manageDone:           make(chan struct{}),
		tps:                  newTopicsPartitions(),
		left:                 make(chan struct{}),
		usingShareCursors:    make(map[string]map[int32]*shareCursor),
		sourcesWithForgotten: make(map[*source]struct{}),
	}
	s.cursorsChanged = sync.NewCond(&c.mu)
	c.s = s

	if len(s.cfg.topics) > 0 && !s.cfg.regex {
		topics := make([]string, 0, len(s.cfg.topics))
		for topic := range s.cfg.topics {
			topics = append(topics, topic)
		}
		s.tps.storeTopics(topics)
	}
}

// findNewAssignments is called on metadata update (under c.mu). It starts
// the manage goroutine the first time it detects topics to consume, and
// migrates share cursors to new sources when partition leaders change.
func (s *shareConsumer) findNewAssignments() {
	topics := s.tps.load()

	var hasTopics bool
	for _, topicPartitions := range topics {
		parts := topicPartitions.load()
		if len(parts.partitions) > 0 {
			hasTopics = true
			break
		}
	}
	if !hasTopics {
		return
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.dying.Load() {
		return
	}

	if !s.managing {
		s.managing = true
		go s.manageShareGroup()
	}

	s.refreshShareCursorSources()
}

// refreshShareCursorSources checks whether any share cursor's partition
// leader has changed (e.g. broker failover) and migrates the cursor to
// the new source. Without this, ShareFetch requests would continue going
// to the old broker indefinitely since share cursors are only placed
// when the assignment itself changes, not when leaders move.
//
// Must be called under c.mu.
func (s *shareConsumer) refreshShareCursorSources() {
	topicParts := s.tps.load()
	var moved bool
	for topic, parts := range s.usingShareCursors {
		tp, ok := topicParts[topic]
		if !ok {
			continue
		}
		tpData := tp.load()
		for p, sc := range parts {
			if int(p) >= len(tpData.partitions) {
				continue
			}
			newLeader := tpData.partitions[p].leader
			if newLeader < 0 || sc.source.nodeID == newLeader {
				continue
			}

			s.cl.sinksAndSourcesMu.Lock()
			sns := s.cl.sinksAndSources[newLeader]
			s.cl.sinksAndSourcesMu.Unlock()
			if sns.source == nil {
				continue
			}

			s.cfg.logger.Log(LogLevelInfo, "migrating share cursor to new leader",
				"topic", topic,
				"partition", p,
				"old_leader", sc.source.nodeID,
				"new_leader", newLeader,
			)
			s.sourcesWithForgotten[sc.source] = struct{}{}
			sc.source.removeShareCursor(sc)
			sc.source = sns.source
			sns.source.addShareCursor(sc)
			moved = true
		}
	}
	if moved {
		s.cursorsChanged.Broadcast()
	}
}

// applyShareMoves proactively moves share cursors to new leaders detected
// via CurrentLeader in ShareFetch responses, avoiding the delay of a full
// metadata refresh. Follows the same broker/source setup pattern as
// kip951move in topics_and_partitions.go.
func (s *shareConsumer) applyShareMoves(moves []shareMove, brokers []BrokerMetadata) {
	// Ensure brokers exist for move targets. Same pattern as
	// kip951move.ensureBrokers: add/replace individual brokers
	// without removing unrelated ones.
	if len(brokers) > 0 {
		s.cl.brokersMu.Lock()
		if !s.cl.stopBrokers {
			var changed bool
			for _, b := range brokers {
				nb := kmsg.MetadataResponseBroker{
					NodeID: b.NodeID,
					Host:   b.Host,
					Port:   b.Port,
					Rack:   b.Rack,
				}
				var found bool
				for i, existing := range s.cl.brokers {
					if existing.meta.NodeID != b.NodeID {
						continue
					}
					found = true
					if !existing.meta.equals(nb) {
						existing.stopForever()
						s.cl.brokers[i] = s.cl.newBroker(b.NodeID, b.Host, b.Port, b.Rack)
						changed = true
					}
					break
				}
				if !found {
					s.cl.brokers = append(s.cl.brokers, s.cl.newBroker(b.NodeID, b.Host, b.Port, b.Rack))
					changed = true
				}
			}
			if changed {
				sort.Slice(s.cl.brokers, func(i, j int) bool {
					return s.cl.brokers[i].meta.NodeID < s.cl.brokers[j].meta.NodeID
				})
				s.cl.reinitAnyBrokerOrd()
			}
		}
		s.cl.brokersMu.Unlock()
	}

	// Ensure sinks and sources exist for move targets.
	targets := make(map[int32]struct{})
	for _, m := range moves {
		targets[m.leaderID] = struct{}{}
	}
	s.cl.sinksAndSourcesMu.Lock()
	for leader := range targets {
		if _, exists := s.cl.sinksAndSources[leader]; !exists {
			s.cl.sinksAndSources[leader] = sinkAndSource{
				sink:   s.cl.newSink(leader),
				source: s.cl.newSource(leader),
			}
		}
	}
	s.cl.sinksAndSourcesMu.Unlock()

	// Move cursors under c.mu.
	id2t := s.cl.id2tMap()
	s.c.mu.Lock()
	var moved bool
	for _, m := range moves {
		topicName := id2t[m.topicID]
		if topicName == "" {
			continue
		}
		parts := s.usingShareCursors[topicName]
		if parts == nil {
			continue
		}
		sc := parts[m.partition]
		if sc == nil || sc.source.nodeID == m.leaderID {
			continue
		}

		s.cl.sinksAndSourcesMu.Lock()
		sns := s.cl.sinksAndSources[m.leaderID]
		s.cl.sinksAndSourcesMu.Unlock()
		if sns.source == nil {
			continue
		}

		s.cfg.logger.Log(LogLevelInfo, "share cursor move via CurrentLeader",
			"topic", topicName,
			"partition", m.partition,
			"old_leader", sc.source.nodeID,
			"new_leader", m.leaderID,
		)
		s.sourcesWithForgotten[sc.source] = struct{}{}
		sc.source.removeShareCursor(sc)
		sc.source = sns.source
		sns.source.addShareCursor(sc)
		moved = true
	}
	if moved {
		s.cursorsChanged.Broadcast()
	}
	s.c.mu.Unlock()
}

func (s *shareConsumer) manageShareGroup() {
	defer close(s.manageDone)
	s.cfg.logger.Log(LogLevelInfo, "beginning to manage the share group lifecycle", "group", s.cfg.shareGroup)

	s.memberGen.store(newStringUUID(), 0)

	var consecutiveErrors int
	for {
		sleep, err := s.sendHeartbeat()

		if err == nil {
			consecutiveErrors = 0

			select {
			case <-s.ctx.Done():
				return
			case <-time.After(sleep):
			}
			continue
		}

		switch {
		case errors.Is(err, context.Canceled):
			return

		case errors.Is(err, kerr.UnknownMemberID):
			member, gen := s.memberGen.load()
			s.memberGen.store(newStringUUID(), 0)
			s.cfg.logger.Log(LogLevelInfo, "share group heartbeat unknown member, restarting with new member id",
				"group", s.cfg.shareGroup,
				"member_id", member,
				"generation", gen,
			)
			s.reconcileAssignment(nil)
			s.clearSourceShareState()
			s.unresolvedAssigned = nil
			consecutiveErrors = 0
			continue

		case errors.Is(err, kerr.FencedMemberEpoch):
			// The broker fenced our epoch but still knows our
			// member ID. Keep the same ID and reset the epoch
			// so the broker can reconnect us to existing state.
			// This matches the Java client's behavior.
			member, gen := s.memberGen.load()
			s.memberGen.storeGeneration(0)
			s.cfg.logger.Log(LogLevelInfo, "share group heartbeat fenced, resetting epoch",
				"group", s.cfg.shareGroup,
				"member_id", member,
				"generation", gen,
			)
			s.reconcileAssignment(nil)
			s.clearSourceShareState()
			s.unresolvedAssigned = nil
			consecutiveErrors = 0
			continue

		case isRetryableBrokerErr(err),
			isAnyDialErr(err),
			s.cl.maybeDeleteStaleCoordinator(s.cfg.shareGroup, coordinatorTypeGroup, err):
			// Retryable -- backoff and retry.

		default:
			// Fatal or unknown -- inject error, backoff, retry.
			s.c.addFakeReadyForDraining("", 0, &ErrGroupSession{err}, "notification of share group management error")
			s.cfg.hooks.each(func(h Hook) {
				if h, ok := h.(HookGroupManageError); ok {
					h.OnGroupManageError(err)
				}
			})
		}

		consecutiveErrors++
		backoff := s.cfg.retryBackoff(consecutiveErrors)
		s.cfg.logger.Log(LogLevelError, "share group manage loop errored",
			"group", s.cfg.shareGroup,
			"err", err,
			"consecutive_errors", consecutiveErrors,
			"backoff", backoff,
		)
		deadline := time.Now().Add(backoff)
		s.cl.waitmeta(s.ctx, backoff, "waitmeta during share group manage backoff")
		after := time.NewTimer(time.Until(deadline))
		select {
		case <-s.ctx.Done():
			after.Stop()
			return
		case <-after.C:
		}
	}
}

func (s *shareConsumer) sendHeartbeat() (time.Duration, error) {
	req := kmsg.NewPtrShareGroupHeartbeatRequest()
	req.GroupID = s.cfg.shareGroup
	req.MemberID, req.MemberEpoch = s.memberGen.load()

	if s.cfg.rack != "" {
		req.RackID = &s.cfg.rack
	}

	// Share heartbeats always include SubscribedTopicNames -- there is no
	// Topics field to diff against like ConsumerGroupHeartbeat has.
	tps := s.tps.load()
	subscribedTopics := make([]string, 0, len(tps))
	for t := range tps {
		subscribedTopics = append(subscribedTopics, t)
	}
	slices.Sort(subscribedTopics)
	req.SubscribedTopicNames = subscribedTopics

	resp, err := req.RequestWith(s.ctx, s.cl)
	sleep := s.cfg.heartbeatInterval
	if err == nil {
		err = errCodeMessage(resp.ErrorCode, resp.ErrorMessage)
		sleep = time.Duration(resp.HeartbeatIntervalMillis) * time.Millisecond
	}
	if err != nil {
		return sleep, err
	}

	newAssigned := s.handleResp(resp)
	if newAssigned != nil {
		s.reconcileAssignment(newAssigned)
	}
	return sleep, nil
}

func (s *shareConsumer) handleResp(resp *kmsg.ShareGroupHeartbeatResponse) map[string][]int32 {
	if resp.MemberID != nil {
		s.memberGen.store(*resp.MemberID, resp.MemberEpoch)
		s.cfg.logger.Log(LogLevelDebug, "storing share member and epoch", "group", s.cfg.shareGroup, "member", *resp.MemberID, "epoch", resp.MemberEpoch)
	} else {
		s.memberGen.storeGeneration(resp.MemberEpoch)
		s.cfg.logger.Log(LogLevelDebug, "storing share epoch", "group", s.cfg.shareGroup, "epoch", resp.MemberEpoch)
	}

	if resp.Assignment == nil {
		return nil
	}

	id2t := s.cl.id2tMap()
	newAssigned := make(map[string][]int32)

	// Fresh assignment from server -- replace unresolved state.
	s.unresolvedAssigned = nil
	for _, t := range resp.Assignment.TopicPartitions {
		name := id2t[t.TopicID]
		if name == "" {
			if s.unresolvedAssigned == nil {
				s.unresolvedAssigned = make(map[topicID][]int32)
			}
			s.unresolvedAssigned[topicID(t.TopicID)] = slices.Clone(t.Partitions)
			continue
		}
		slices.Sort(t.Partitions)
		newAssigned[name] = t.Partitions
	}

	// Try to resolve previously-unresolved topic IDs.
	for id, ps := range s.unresolvedAssigned {
		if name := id2t[[16]byte(id)]; name != "" {
			slices.Sort(ps)
			newAssigned[name] = ps
			delete(s.unresolvedAssigned, id)
		}
	}
	if len(s.unresolvedAssigned) > 0 {
		s.cl.triggerUpdateMetadataNow("share group heartbeat has unresolved topic IDs in assignment")
	}

	current := s.nowAssigned.read()
	if !mapi32sDeepEq(current, newAssigned) {
		return newAssigned
	}
	return nil
}

func (s *shareConsumer) reconcileAssignment(assignments map[string][]int32) {
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	s.assignSharePartitions(assignments)
}

func (s *shareConsumer) assignSharePartitions(assignments map[string][]int32) {
	s.cfg.logger.Log(LogLevelInfo, "assigning share partitions",
		"group", s.cfg.shareGroup,
		"assignments", assignments,
	)

	_, _ = s.c.stopSession()

	// Remove stale shareCursors from sources. Track which sources
	// lost cursors so we can send ForgottenTopicsData even if a
	// source ends up with no remaining share cursors.
	for topic, parts := range s.usingShareCursors {
		newParts, ok := assignments[topic]
		if !ok {
			for _, sc := range parts {
				s.sourcesWithForgotten[sc.source] = struct{}{}
				sc.source.removeShareCursor(sc)
			}
			delete(s.usingShareCursors, topic)
			continue
		}
		newSet := make(map[int32]struct{}, len(newParts))
		for _, p := range newParts {
			newSet[p] = struct{}{}
		}
		for p, sc := range parts {
			if _, keep := newSet[p]; !keep {
				s.sourcesWithForgotten[sc.source] = struct{}{}
				sc.source.removeShareCursor(sc)
				delete(parts, p)
			}
		}
		if len(parts) == 0 {
			delete(s.usingShareCursors, topic)
		}
	}

	// Add new shareCursors.
	for topic, parts := range assignments {
		existing := s.usingShareCursors[topic]
		if existing == nil {
			existing = make(map[int32]*shareCursor)
			s.usingShareCursors[topic] = existing
		}
		for _, p := range parts {
			if _, ok := existing[p]; ok {
				continue
			}

			topicParts := s.tps.load()
			tp, ok := topicParts[topic]
			if !ok {
				continue
			}
			tpData := tp.load()
			if int(p) >= len(tpData.partitions) {
				continue
			}
			leader := tpData.partitions[p].leader

			s.cfg.logger.Log(LogLevelDebug, "share cursor placement",
				"topic", topic,
				"partition", p,
				"leader", leader,
				"topic_id", tpData.id,
			)

			s.cl.sinksAndSourcesMu.Lock()
			sns := s.cl.sinksAndSources[leader]
			s.cl.sinksAndSourcesMu.Unlock()

			if sns.source == nil {
				s.cfg.logger.Log(LogLevelWarn, "no source for share cursor leader",
					"topic", topic, "partition", p, "leader", leader)
				continue
			}

			sc := &shareCursor{
				topic:     topic,
				topicID:   tpData.id,
				partition: p,
				source:    sns.source,
			}
			sns.source.addShareCursor(sc)
			existing[p] = sc
		}
	}

	s.nowAssigned.store(assignments)
	session := s.c.startNewSession(s.tps)
	session.decWorker()

	// Wake any pollShareFetches blocked waiting for cursors.
	s.cursorsChanged.Broadcast()
}

// routeAcksToSources iterates lastPolled, finalizes records, and routes their
// acks directly to each source's pending ack queue. If autoAcceptPending is
// true, records with no explicit ack status (status==0) are auto-accepted;
// otherwise they are skipped.
//
// Must be called under consumer.mu.
func (s *shareConsumer) routeAcksToSources(autoAcceptPending bool) {
	for i := range s.lastPolled {
		pr := &s.lastPolled[i]
		st := pr.state
		st.mu.Lock()
		if st.status&ackFinalized != 0 {
			st.mu.Unlock()
			continue
		}
		if st.status == 0 {
			if !autoAcceptPending {
				st.mu.Unlock()
				continue
			}
			st.status = uint8(AckAccept) | ackFinalized
		} else {
			st.status |= ackFinalized
		}
		ackType := int8(st.status &^ ackFinalized)
		st.mu.Unlock()

		// Look up the current source for this partition. If the
		// partition was revoked, the cursor is gone and we drop the
		// ack -- the broker already revoked the acquisition.
		parts := s.usingShareCursors[pr.topic]
		if parts == nil {
			continue
		}
		sc := parts[pr.partition]
		if sc == nil {
			continue
		}
		// If the partition leader changed since these records were
		// fetched, the new leader's share session doesn't know
		// about these acquisitions. Drop the ack -- the old
		// leader's acquisition lock will time out and the records
		// will be redelivered. This matches the Java client's
		// isLeaderKnownToHaveChanged check.
		if pr.source != nil && pr.source != sc.source {
			continue
		}

		// Route directly to the source. Merging of contiguous
		// same-type batches happens at drain time.
		sc.source.addShareAcks(sc.topicID, pr.partition, []shareAckBatch{{
			firstOffset: pr.offset,
			lastOffset:  pr.offset,
			ackType:     ackType,
		}})
	}
}

// finalizePreviousPoll auto-ACCEPTs any records from the previous poll that
// were not explicitly acknowledged, then routes all finalized acks to their
// respective source's pending ack queues.
//
// Called under consumer.mu from PollFetches/PollRecords.
func (s *shareConsumer) finalizePreviousPoll() {
	if len(s.lastPolled) == 0 {
		return
	}
	s.routeAcksToSources(true)
	s.lastPolled = s.lastPolled[:0]
}

// markAcks sets the ack status on records that are still pending (status==0
// and not finalized). If records is empty, all lastPolled records are
// considered; otherwise only the specified records are marked.
func (s *shareConsumer) markAcks(status AckStatus, records []*Record) {
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	if len(records) == 0 {
		for i := range s.lastPolled {
			st := s.lastPolled[i].state
			st.mu.Lock()
			if st.status == 0 {
				st.status = uint8(status)
			}
			st.mu.Unlock()
		}
		return
	}
	for _, r := range records {
		st := shareAckFromCtx(r)
		if st == nil {
			continue
		}
		st.mu.Lock()
		if st.status == 0 {
			st.status = uint8(status)
		}
		st.mu.Unlock()
	}
}

// commitAcks finalizes all records that have been marked via Record.Ack,
// routes them to their sources, and sends ShareAcknowledge requests to each
// affected broker.
func (s *shareConsumer) commitAcks(ctx context.Context) error {
	// Finalize marked records and route to sources, under consumer.mu.
	// Unlike finalizePreviousPoll, this does NOT auto-ACCEPT unmarked
	// records and does NOT clear lastPolled.
	//
	// We also collect the set of share sources here (under the same
	// lock) so we only visit sources that actually have share cursors
	// rather than iterating every source in the client.
	s.c.mu.Lock()
	s.routeAcksToSources(false)
	sources := s.collectShareSources()
	s.c.mu.Unlock()

	return s.sendAcksFromSources(ctx, sources)
}

// sendAcksFromSources drains pending acks from the given sources and sends
// ShareAcknowledge requests to each broker in parallel. Returns errors
// including per-partition errors from the broker via errors.Join.
func (s *shareConsumer) sendAcksFromSources(ctx context.Context, sources []*source) error {
	type sourceAcks struct {
		src  *source
		acks map[[16]byte]map[int32][]shareAckBatch
	}
	var pending []sourceAcks
	for _, src := range sources {
		acks := src.drainShareAcks()
		if len(acks) > 0 {
			pending = append(pending, sourceAcks{src, acks})
		}
	}
	if len(pending) == 0 {
		return nil
	}

	memberID, _ := s.memberGen.load()
	errs := make(chan error, len(pending))
	for _, sa := range pending {
		go func() {
			errs <- s.sendSourceAck(ctx, sa.src, sa.acks, memberID)
		}()
	}
	var allErrs []error
	for range pending {
		if err := <-errs; err != nil {
			allErrs = append(allErrs, err)
		}
	}
	return errors.Join(allErrs...)
}

// sendSourceAck sends a single ShareAcknowledge request to one source's
// broker for the given pending ack batches.
func (s *shareConsumer) sendSourceAck(ctx context.Context, src *source, acks map[[16]byte]map[int32][]shareAckBatch, memberID string) error {
	req := kmsg.NewPtrShareAcknowledgeRequest()
	req.GroupID = &s.cfg.shareGroup
	req.MemberID = &memberID

	src.cursorsMu.Lock()
	req.ShareSessionEpoch = src.shareSessionEpoch
	src.cursorsMu.Unlock()

	topicIdx := make(map[[16]byte]int)
	for tid, parts := range acks {
		idx, ok := topicIdx[tid]
		if !ok {
			idx = len(req.Topics)
			topicIdx[tid] = idx
			t := kmsg.NewShareAcknowledgeRequestTopic()
			t.TopicID = tid
			req.Topics = append(req.Topics, t)
		}
		for partition, batches := range parts {
			p := kmsg.NewShareAcknowledgeRequestTopicPartition()
			p.Partition = partition
			for _, batch := range batches {
				ab := kmsg.NewShareAcknowledgeRequestTopicPartitionAcknowledgementBatche()
				ab.FirstOffset = batch.firstOffset
				ab.LastOffset = batch.lastOffset
				ab.AcknowledgeTypes = []int8{batch.ackType}
				p.AcknowledgementBatches = append(p.AcknowledgementBatches, ab)
			}
			req.Topics[idx].Partitions = append(req.Topics[idx].Partitions, p)
		}
	}
	req.IsRenewAck = hasRenewAck(acks)

	kresp, err := s.cl.retryableBrokerFn(func() (*broker, error) {
		return s.cl.brokerOrErr(ctx, src.nodeID, errUnknownBroker)
	}).Request(ctx, req)
	if err != nil {
		return err
	}

	var errs []error
	resp := kresp.(*kmsg.ShareAcknowledgeResponse)
	if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
		switch {
		case errors.Is(err, kerr.ShareSessionNotFound),
			errors.Is(err, kerr.InvalidShareSessionEpoch),
			errors.Is(err, kerr.ShareSessionLimitReached):
			// Session errors: the broker did NOT advance the
			// epoch. Reset to 0 so the next ShareFetch creates
			// a fresh session.
			src.cursorsMu.Lock()
			src.shareSessionEpoch = 0
			src.cursorsMu.Unlock()
		default:
			// Non-session errors: the broker may have advanced
			// the epoch before returning the error. Match the
			// Java client's defensive increment.
			src.cursorsMu.Lock()
			if src.shareSessionEpoch == math.MaxInt32 {
				src.shareSessionEpoch = 1
			} else {
				src.shareSessionEpoch++
			}
			src.cursorsMu.Unlock()
		}
		if isAckRetryable(err) {
			for tid, parts := range acks {
				for p, batches := range parts {
					src.addShareAcks(tid, p, batches)
				}
			}
			s.cfg.logger.Log(LogLevelInfo, "re-queued all acks due to retryable top-level error",
				"broker", src.nodeID,
				"err", err,
			)
			return nil
		}
		errs = append(errs, err)
	} else {
		// Success: the broker incremented the session epoch.
		src.cursorsMu.Lock()
		if src.shareSessionEpoch == math.MaxInt32 {
			src.shareSessionEpoch = 1
		} else {
			src.shareSessionEpoch++
		}
		src.cursorsMu.Unlock()
	}

	id2t := s.cl.id2tMap()
	for _, rt := range resp.Topics {
		topicName := id2t[rt.TopicID]
		for _, rp := range rt.Partitions {
			if rp.ErrorCode == 0 {
				continue
			}
			err := kerr.ErrorForCode(rp.ErrorCode)
			if isAckRetryable(err) {
				if batches, ok := acks[rt.TopicID][rp.Partition]; ok {
					src.addShareAcks(rt.TopicID, rp.Partition, batches)
				}
				s.cfg.logger.Log(LogLevelInfo, "re-queued retryable share ack error",
					"topic", topicName,
					"partition", rp.Partition,
					"err", err,
				)
				continue
			}
			s.cfg.logger.Log(LogLevelWarn, "share acknowledge partition error",
				"topic", topicName,
				"partition", rp.Partition,
				"err", err,
			)
			errs = append(errs, &ShareAckPartitionError{
				Topic:     topicName,
				Partition: rp.Partition,
				Err:       err,
			})
		}
	}
	return errors.Join(errs...)
}

// mergeAckBatches sorts ack batches by offset and merges contiguous ranges
// with the same ack type.
func mergeAckBatches(batches []shareAckBatch) []shareAckBatch {
	if len(batches) <= 1 {
		return batches
	}
	sort.Slice(batches, func(i, j int) bool {
		return batches[i].firstOffset < batches[j].firstOffset
	})
	merged := batches[:1]
	for _, b := range batches[1:] {
		last := &merged[len(merged)-1]
		if b.ackType == last.ackType && b.firstOffset == last.lastOffset+1 {
			last.lastOffset = b.lastOffset
		} else {
			merged = append(merged, b)
		}
	}
	return merged
}

func (s *shareConsumer) leave(ctx context.Context) {
	s.mu.Lock()
	wasDead := s.dying.Swap(true)
	wasManaging := s.managing
	s.cancel()
	s.mu.Unlock()

	// Wake any pollShareFetches blocked waiting for cursors so it
	// sees s.dying and returns ErrClientClosed.
	s.cursorsChanged.Broadcast()

	go func() {
		if wasManaging {
			<-s.manageDone
		}
		if wasDead {
			return
		}

		// Finalize all outstanding records as RELEASE before leaving.
		s.finalizeForLeave()

		memberID := s.memberGen.memberID()
		s.cfg.logger.Log(LogLevelInfo, "leaving share group",
			"group", s.cfg.shareGroup,
			"member_id", memberID,
		)

		// Close share sessions. Pending acks are piggybacked on the
		// close request (epoch -1) so the broker processes them in
		// one round trip rather than needing a separate
		// ShareAcknowledge followed by a close.
		s.closeShareSessions(ctx)

		req := kmsg.NewPtrShareGroupHeartbeatRequest()
		req.GroupID = s.cfg.shareGroup
		req.MemberID = memberID
		req.MemberEpoch = -1

		resp, err := req.RequestWith(ctx, s.cl)
		if err == nil {
			err = errCodeMessage(resp.ErrorCode, resp.ErrorMessage)
		}
		s.leaveErr = err
		close(s.left)
	}()
}

// finalizeForLeave defaults any pending (unacked) records to RELEASE, then
// calls finalizePreviousPoll to finalize all records and route them to
// sources. Records the user explicitly acknowledged keep their status.
//
// This grabs c.mu because finalizePreviousPoll accesses lastPolled and
// usingShareCursors. Without the lock, a concurrent PollFetches that is
// mid-flight (released c.mu for network I/O) could race writing lastPolled
// while we read it here.
func (s *shareConsumer) finalizeForLeave() {
	s.c.mu.Lock()
	defer s.c.mu.Unlock()

	// Release any buffered records that were never returned to the
	// user. Track them in lastPolled so finalizePreviousPoll routes
	// the releases to sources.
	if len(s.bufferedFetches) > 0 {
		s.trackPolledRecords(s.bufferedFetches)
		s.bufferedFetches = nil
	}

	for i := range s.lastPolled {
		pr := &s.lastPolled[i]
		st := pr.state
		st.mu.Lock()
		if st.status == 0 {
			st.status = uint8(AckRelease)
		}
		st.mu.Unlock()
	}
	// finalizePreviousPoll sets ackFinalized and routes to sources.
	s.finalizePreviousPoll()
}

// clearSourceShareState clears stale share state (pending acks, forgotten
// partitions, session epochs) from all sources. Called when the member is
// fenced or unknown -- the broker invalidated our state, so pending acks
// and sessions are stale and should not be sent.
func (s *shareConsumer) clearSourceShareState() {
	s.cl.sinksAndSourcesMu.Lock()
	defer s.cl.sinksAndSourcesMu.Unlock()
	for _, sns := range s.cl.sinksAndSources {
		if sns.source == nil {
			continue
		}
		sns.source.cursorsMu.Lock()
		sns.source.sharePendingAcks = nil
		sns.source.shareForgotten = nil
		sns.source.shareSessionEpoch = 0
		sns.source.cursorsMu.Unlock()
	}
}

// collectAllSources returns all sources known to the client.
func (s *shareConsumer) collectAllSources() map[*source]struct{} {
	s.cl.sinksAndSourcesMu.Lock()
	defer s.cl.sinksAndSourcesMu.Unlock()
	sources := make(map[*source]struct{}, len(s.cl.sinksAndSources))
	for _, sns := range s.cl.sinksAndSources {
		if sns.source != nil {
			sources[sns.source] = struct{}{}
		}
	}
	return sources
}

// hasRenewAck returns true if any batch in the ack map has AckRenew type,
// which requires setting IsRenewAck on the request.
func hasRenewAck(acks map[[16]byte]map[int32][]shareAckBatch) bool {
	for _, parts := range acks {
		for _, batches := range parts {
			for _, b := range batches {
				if b.ackType == int8(AckRenew) {
					return true
				}
			}
		}
	}
	return false
}

// isAckRetryable returns true if a ShareAcknowledge / piggybacked ack error
// should be retried by re-queuing the acks. Leader changes and topic
// deletions release acquisition locks on the broker, so retrying the ack
// is pointless -- the records will be redelivered by the new leader or
// archived. Other retryable errors (timeouts, broker unavailable) should
// be retried.
func isAckRetryable(err error) bool {
	switch {
	case errors.Is(err, kerr.NotLeaderForPartition),
		errors.Is(err, kerr.FencedLeaderEpoch),
		errors.Is(err, kerr.UnknownTopicOrPartition),
		errors.Is(err, kerr.UnknownTopicID):
		return false
	}
	return kerr.IsRetriable(err)
}

// requeueAcksForPartition extracts piggybacked ack batches for a specific
// topic+partition from a ShareFetch request and re-queues them on the source
// for retry on the next request.
func requeueAcksForPartition(src *source, req *kmsg.ShareFetchRequest, topicID [16]byte, partition int32) {
	for i := range req.Topics {
		if req.Topics[i].TopicID != topicID {
			continue
		}
		for j := range req.Topics[i].Partitions {
			rp := &req.Topics[i].Partitions[j]
			if rp.Partition != partition || len(rp.AcknowledgementBatches) == 0 {
				continue
			}
			var batches []shareAckBatch
			for _, ab := range rp.AcknowledgementBatches {
				for _, at := range ab.AcknowledgeTypes {
					batches = append(batches, shareAckBatch{
						firstOffset: ab.FirstOffset,
						lastOffset:  ab.LastOffset,
						ackType:     at,
					})
				}
			}
			src.addShareAcks(topicID, partition, batches)
			return
		}
	}
}

// collectShareSources returns the deduplicated sources that have share
// cursors or pending forgotten/ack state, sorted by node ID for stable
// rotation. Must be called under c.mu.
func (s *shareConsumer) collectShareSources() []*source {
	seen := make(map[*source]struct{})
	for _, parts := range s.usingShareCursors {
		for _, sc := range parts {
			seen[sc.source] = struct{}{}
		}
	}
	// Include sources that had cursors removed and may still need
	// to send ForgottenTopicsData or flush pending acks.
	for src := range s.sourcesWithForgotten {
		seen[src] = struct{}{}
	}
	sources := make([]*source, 0, len(seen))
	for src := range seen {
		sources = append(sources, src)
	}
	sort.Slice(sources, func(i, j int) bool {
		return sources[i].nodeID < sources[j].nodeID
	})
	return sources
}

// cleanupForgottenSources removes entries from sourcesWithForgotten for
// sources that have active cursors (they'll be visited anyway) or have
// no remaining forgotten/ack state. Must be called under c.mu.
func (s *shareConsumer) cleanupForgottenSources() {
	for src := range s.sourcesWithForgotten {
		// If the source has active cursors, it will be visited
		// during polling regardless -- stop tracking it here.
		hasCursors := false
		for _, parts := range s.usingShareCursors {
			for _, sc := range parts {
				if sc.source == src {
					hasCursors = true
					break
				}
			}
			if hasCursors {
				break
			}
		}
		if hasCursors {
			delete(s.sourcesWithForgotten, src)
			continue
		}
		src.cursorsMu.Lock()
		drained := len(src.shareForgotten) == 0 && len(src.sharePendingAcks) == 0
		src.cursorsMu.Unlock()
		if drained {
			delete(s.sourcesWithForgotten, src)
		}
	}
}

// trackPolledRecords adds records from the given fetches to lastPolled so
// that finalizePreviousPoll can auto-ACCEPT or route explicit acks.
//
// Must be called under consumer.mu.
func (s *shareConsumer) trackPolledRecords(fetches Fetches) {
	for i := range fetches {
		for j := range fetches[i].Topics {
			t := &fetches[i].Topics[j]
			for k := range t.Partitions {
				p := &t.Partitions[k]
				for _, r := range p.Records {
					if st := shareAckFromCtx(r); st != nil {
						s.lastPolled = append(s.lastPolled, sharePolledRecord{
							state:     st,
							source:    st.source,
							topic:     t.Topic,
							partition: p.Partition,
							offset:    r.Offset,
						})
					}
				}
			}
		}
	}
}

// bufferExcess splits fetches so that at most maxPollRecords are returned
// and excess records are stored in s.bufferedFetches for the next poll.
// Returns the trimmed fetches to return to the user.
//
// Must be called under consumer.mu.
func (s *shareConsumer) bufferExcess(fetches Fetches, maxPollRecords int) Fetches {
	remaining := maxPollRecords
	for i := range fetches {
		for j := range fetches[i].Topics {
			t := &fetches[i].Topics[j]
			for k := range t.Partitions {
				p := &t.Partitions[k]
				if remaining >= len(p.Records) {
					remaining -= len(p.Records)
					continue
				}
				// This partition has excess records. Buffer
				// the tail and trim the returned slice.
				if len(p.Records[remaining:]) > 0 {
					buf := Fetch{Topics: []FetchTopic{{
						Topic: t.Topic,
						Partitions: []FetchPartition{{
							Partition: p.Partition,
							Records:   p.Records[remaining:],
						}},
					}}}
					s.bufferedFetches = append(s.bufferedFetches, buf)
				}
				p.Records = p.Records[:remaining]
				remaining = 0
			}
		}
	}
	return fetches
}

// takeBuffered returns up to maxPollRecords from the buffered fetches,
// moving excess back into the buffer. The returned records are tracked
// in lastPolled. If maxPollRecords <= 0, all buffered records are returned.
//
// Must be called under consumer.mu.
func (s *shareConsumer) takeBuffered(maxPollRecords int) Fetches {
	fetches := s.bufferedFetches
	s.bufferedFetches = nil
	if maxPollRecords > 0 {
		fetches = s.bufferExcess(fetches, maxPollRecords)
	}
	s.trackPolledRecords(fetches)
	return fetches
}

// pollShareFetches sends ShareFetch requests to all sources with share
// cursors in parallel and returns the merged results. This is the pull
// model: fetches happen synchronously within PollFetches so records are
// only acquired when the user is ready to process them.
//
// maxPollRecords limits the total number of records returned. A value
// <= 0 means no limit (return everything from one round of fetches).
//
// Called from PollRecords with consumer.mu held for finalizePreviousPoll,
// then released for the network calls.
func (s *shareConsumer) pollShareFetches(ctx context.Context, maxPollRecords int) Fetches {
	if s.dying.Load() {
		return NewErrFetch(ErrClientClosed)
	}

	// Nil context means "return immediately with whatever is
	// available." Return buffered records if we have them,
	// otherwise drain any injected errors.
	if ctx == nil {
		s.c.mu.Lock()
		s.finalizePreviousPoll()
		if len(s.bufferedFetches) > 0 {
			fetches := s.takeBuffered(maxPollRecords)
			s.c.mu.Unlock()
			return fetches
		}
		s.c.mu.Unlock()
		if fake := s.c.drainFakeReady(); len(fake) > 0 {
			return Fetches(fake)
		}
		return nil
	}

	// Finalize the previous poll, then check the buffer. If we have
	// buffered records from a prior fetch, return from those without
	// issuing new ShareFetch requests.
	s.c.mu.Lock()
	s.finalizePreviousPoll()
	if len(s.bufferedFetches) > 0 {
		fetches := s.takeBuffered(maxPollRecords)
		s.c.mu.Unlock()
		return fetches
	}

	// Collect unique sources that have share cursors. If none exist
	// yet, block until the heartbeat loop assigns partitions, an
	// injected error arrives, or the context is cancelled.
	sources := s.collectShareSources()
	if len(sources) == 0 {
		// Spin up a goroutine that broadcasts on ctx cancellation
		// so that our Cond.Wait unblocks. We cannot select on a
		// Cond, so this is the standard workaround.
		done := make(chan struct{})
		go func() {
			select {
			case <-ctx.Done():
				s.cursorsChanged.Broadcast()
			case <-done:
			}
		}()
		for len(sources) == 0 && !s.dying.Load() && ctx.Err() == nil {
			// Break if injected errors (e.g., fatal heartbeat
			// failures) are waiting -- we surface those even
			// without any share cursors.
			if s.c.hasFakeReady() {
				break
			}
			s.cursorsChanged.Wait()
			sources = s.collectShareSources()
		}
		close(done)
	}
	s.c.mu.Unlock()

	// Drain any injected error fetches (fatal heartbeat errors from
	// manageShareGroup, metadata load errors, etc.).
	fakeFetches := s.c.drainFakeReady()

	if len(sources) == 0 {
		if len(fakeFetches) > 0 {
			return Fetches(fakeFetches)
		}
		if s.dying.Load() {
			return NewErrFetch(ErrClientClosed)
		}
		return NewErrFetch(ctx.Err())
	}

	// Fetch from up to maxConcurrentFetches sources in parallel.
	// This option gates memory: with many brokers, you may not
	// want all in-flight at once. Extra capacity beyond the
	// number of sources is a no-op. We rotate which sources are
	// selected across poll calls so that all brokers get fair
	// coverage when maxConcurrentFetches < len(sources).
	nTotal := len(sources)
	nFetch := nTotal
	if n := s.cfg.maxConcurrentFetches; n > 0 && n < nFetch {
		nFetch = n
	}

	var fetches Fetches
	var allMoves []shareMove
	var allBrokers []BrokerMetadata
	results := make(chan shareFetchResult, nFetch)
	s.c.mu.Lock()
	offset := s.pollSourceOffset
	s.pollSourceOffset = (offset + nFetch) % nTotal
	s.c.mu.Unlock()
	for i := range nFetch {
		src := sources[(offset+i)%nTotal]
		go func() {
			results <- s.doShareFetch(ctx, src)
		}()
	}
	for range nFetch {
		r := <-results
		allMoves = append(allMoves, r.moves...)
		allBrokers = append(allBrokers, r.brokers...)
		if r.fetch.hasErrorsOrRecords() {
			fetches = append(fetches, r.fetch)
		}
	}

	// Move share cursors to new leaders detected via CurrentLeader
	// in ShareFetch responses. This avoids waiting for the periodic
	// metadata refresh to discover the leader change.
	if len(allMoves) > 0 {
		s.applyShareMoves(allMoves, allBrokers)
	}

	// Append any injected error fetches that arrived during the share
	// fetch round-trips.
	fetches = append(fetches, fakeFetches...)

	// If maxPollRecords is set, buffer excess records for the next
	// poll instead of releasing them. This avoids re-fetching records
	// the broker already acquired for us.
	s.c.mu.Lock()
	if maxPollRecords > 0 {
		fetches = s.bufferExcess(fetches, maxPollRecords)
	}

	// Track only the records we're returning to the user. Buffered
	// records stay untouched until a future poll returns them.
	s.trackPolledRecords(fetches)

	// The fetch round just sent requests which drained forgotten
	// state from sources -- clean up tracking entries for sources
	// that no longer need visiting.
	s.cleanupForgottenSources()
	s.c.mu.Unlock()

	return fetches
}

// doShareFetch sends a single ShareFetch request to one source and
// returns the decoded Fetch. On session errors (e.g. SHARE_SESSION_NOT_FOUND),
// the epoch is reset and the request is retried once.
func (s *shareConsumer) doShareFetch(ctx context.Context, src *source) shareFetchResult {
	for attempt := range 2 {
		req := s.buildShareFetchRequest(src)
		if req == nil {
			return shareFetchResult{}
		}

		br, err := s.cl.brokerOrErr(ctx, src.nodeID, errUnknownBroker)
		if err != nil {
			src.restoreShareForgotten(req)
			src.restoreSharePendingAcks(req)
			return shareFetchResult{}
		}

		// Use the same br.do + select pattern as regular fetch
		// (source.go) so that context cancellation immediately
		// unblocks us rather than relying on readConn's ctx.Done
		// propagation alone.
		var kresp kmsg.Response
		requested := make(chan struct{})
		br.do(ctx, req, func(k kmsg.Response, e error) {
			kresp, err = k, e
			close(requested)
		})

		select {
		case <-requested:
			if isContextErr(err) && ctx.Err() != nil {
				// The request may have been processed by the
				// broker before the context was cancelled,
				// advancing the broker's session epoch. Reset
				// to 0 so the next request starts a fresh
				// session rather than hitting
				// InvalidShareSessionEpoch.
				src.cursorsMu.Lock()
				src.shareSessionEpoch = 0
				src.cursorsMu.Unlock()
				src.restoreShareForgotten(req)
				src.restoreSharePendingAcks(req)
				return shareFetchResult{}
			}
		case <-ctx.Done():
			// The in-flight request may still be processed by
			// the broker, advancing its session epoch. Reset
			// ours so the next request starts a new session.
			src.cursorsMu.Lock()
			src.shareSessionEpoch = 0
			src.cursorsMu.Unlock()
			src.restoreShareForgotten(req)
			src.restoreSharePendingAcks(req)
			return shareFetchResult{}
		}

		if err != nil {
			src.consecutiveFailures++
			// The broker may have processed the request before
			// the error occurred, advancing its session epoch.
			// Reset ours so the next request starts a fresh
			// session.
			src.cursorsMu.Lock()
			src.shareSessionEpoch = 0
			src.cursorsMu.Unlock()
			src.restoreShareForgotten(req)
			src.restoreSharePendingAcks(req)
			after := time.NewTimer(s.cfg.retryBackoff(src.consecutiveFailures))
			select {
			case <-after.C:
			case <-ctx.Done():
			}
			after.Stop()
			return shareFetchResult{}
		}
		src.consecutiveFailures = 0

		resp := kresp.(*kmsg.ShareFetchResponse)

		s.cfg.logger.Log(LogLevelDebug, "share fetch response",
			"broker", src.nodeID,
			"error_code", resp.ErrorCode,
			"num_topics", len(resp.Topics),
		)

		fetch, retry, moves := s.handleShareFetchResponse(src, req, resp)
		if !retry {
			var brokers []BrokerMetadata
			if len(moves) > 0 {
				for _, ne := range resp.NodeEndpoints {
					brokers = append(brokers, BrokerMetadata{
						NodeID: ne.NodeID,
						Host:   ne.Host,
						Port:   ne.Port,
						Rack:   ne.Rack,
					})
				}
			}
			return shareFetchResult{fetch, moves, brokers}
		}
		// Session error -- epoch was reset by
		// handleShareFetchResponse. Restore forgotten and ack
		// data from this request (the broker rejected it without
		// processing) and retry once to re-establish the session.
		if attempt == 0 {
			src.restoreShareForgotten(req)
			src.restoreSharePendingAcks(req)
		}
	}
	return shareFetchResult{}
}

// restoreShareForgotten puts ForgottenTopicsData from a failed ShareFetch
// request back onto the source so it will be included in the next request.
// buildShareFetchRequest drains shareForgotten when building the request;
// if the request fails before reaching the broker, the forgotten state
// would be permanently lost, causing the client to never send
// ForgottenTopicsData and potentially leaving stale share sessions.
func (s *source) restoreShareForgotten(req *kmsg.ShareFetchRequest) {
	if len(req.ForgottenTopicsData) == 0 {
		return
	}
	s.cursorsMu.Lock()
	defer s.cursorsMu.Unlock()
	if s.shareForgotten == nil {
		s.shareForgotten = make(map[[16]byte][]int32)
	}
	for _, f := range req.ForgottenTopicsData {
		s.shareForgotten[f.TopicID] = append(s.shareForgotten[f.TopicID], f.Partitions...)
	}
}

// restoreSharePendingAcks reconstructs piggybacked acks from a failed
// ShareFetch request and puts them back on the source so the next request
// retries them. Without this, acks are silently lost when a ShareFetch
// fails after buildShareFetchRequest drained them from the source.
func (s *source) restoreSharePendingAcks(req *kmsg.ShareFetchRequest) {
	s.cursorsMu.Lock()
	defer s.cursorsMu.Unlock()
	for i := range req.Topics {
		rt := &req.Topics[i]
		for j := range rt.Partitions {
			rp := &rt.Partitions[j]
			if len(rp.AcknowledgementBatches) == 0 {
				continue
			}
			var batches []shareAckBatch
			for _, ab := range rp.AcknowledgementBatches {
				for _, at := range ab.AcknowledgeTypes {
					batches = append(batches, shareAckBatch{
						firstOffset: ab.FirstOffset,
						lastOffset:  ab.LastOffset,
						ackType:     at,
					})
				}
			}
			s.restoreSharePendingAcksLocked(map[[16]byte]map[int32][]shareAckBatch{
				rt.TopicID: {rp.Partition: batches},
			})
		}
	}
}

// restoreSharePendingAcksLocked merges pending acks back onto the source.
// Must be called with cursorsMu held.
func (s *source) restoreSharePendingAcksLocked(acks map[[16]byte]map[int32][]shareAckBatch) {
	for tid, parts := range acks {
		if s.sharePendingAcks == nil {
			s.sharePendingAcks = make(map[[16]byte]map[int32][]shareAckBatch)
		}
		if s.sharePendingAcks[tid] == nil {
			s.sharePendingAcks[tid] = make(map[int32][]shareAckBatch)
		}
		for p, batches := range parts {
			s.sharePendingAcks[tid][p] = append(s.sharePendingAcks[tid][p], batches...)
		}
	}
}

// buildShareFetchRequest constructs a ShareFetchRequest for the given source.
// Snapshots the source's share state under cursorsMu, then builds the request
// outside the lock. The snapshots are safe because:
//   - shareCursor fields (topic, topicID, partition) are immutable after construction
//   - pendingAcks and forgotten are transferred by ownership (source nils its refs)
//   - sessionEpoch is a plain value copy
func (s *shareConsumer) buildShareFetchRequest(src *source) *kmsg.ShareFetchRequest {
	src.cursorsMu.Lock()
	if len(src.shareCursors) == 0 && len(src.sharePendingAcks) == 0 && len(src.shareForgotten) == 0 {
		src.cursorsMu.Unlock()
		return nil
	}
	var (
		cursors      = slices.Clone(src.shareCursors)
		pendingAcks  = src.sharePendingAcks
		forgotten    = src.shareForgotten
		sessionEpoch = src.shareSessionEpoch
	)
	src.sharePendingAcks = nil
	src.shareForgotten = nil
	src.cursorsMu.Unlock()

	memberID, _ := s.memberGen.load()

	req := kmsg.NewPtrShareFetchRequest()
	req.GroupID = &s.cfg.shareGroup
	req.MemberID = &memberID
	req.ShareSessionEpoch = sessionEpoch
	req.MaxWaitMillis = s.cfg.maxWait
	req.MinBytes = s.cfg.minBytes
	req.MaxBytes = int32(s.cfg.maxBytes)

	maxRecords := s.cfg.shareMaxRecords
	if maxRecords <= 0 {
		maxRecords = 500
	}
	req.MaxRecords = maxRecords
	req.BatchSize = maxRecords
	if s.cfg.shareStrictMaxRecords {
		req.ShareAcquireMode = 1
	}

	// Always send all share cursors in every request. Unlike regular
	// fetch sessions where the broker handles delta tracking, share
	// sessions require topics to be present for the broker to return
	// data from them.
	topicIdx := make(map[[16]byte]int)    // topicID -> index in req.Topics
	partIdx := make(map[topicPartIdx]int) // (topicID, partition) -> index in topic's Partitions
	for _, sc := range cursors {
		tidx, ok := topicIdx[sc.topicID]
		if !ok {
			tidx = len(req.Topics)
			topicIdx[sc.topicID] = tidx
			t := kmsg.NewShareFetchRequestTopic()
			t.TopicID = sc.topicID
			req.Topics = append(req.Topics, t)
		}
		p := kmsg.NewShareFetchRequestTopicPartition()
		p.Partition = sc.partition
		p.PartitionMaxBytes = int32(s.cfg.maxBytes)
		partIdx[topicPartIdx{sc.topicID, sc.partition}] = len(req.Topics[tidx].Partitions)
		req.Topics[tidx].Partitions = append(req.Topics[tidx].Partitions, p)
	}

	// Attach piggybacked acks. The broker rejects acks on the initial
	// epoch (epoch 0 creates a new session), so hold them for the next
	// request. This matches the Java client's maybeAddAcknowledgements
	// check for isNewSession.
	if len(pendingAcks) > 0 {
		if sessionEpoch == 0 {
			src.cursorsMu.Lock()
			src.restoreSharePendingAcksLocked(pendingAcks)
			src.cursorsMu.Unlock()
		} else {
			s.attachAcksToShareFetch(req, pendingAcks, topicIdx, partIdx)
		}
	}

	// Attach forgotten topics. Only meaningful on incremental requests
	// (epoch > 0) -- on epoch 0 the broker creates a fresh session with
	// no prior state to forget.
	if sessionEpoch > 0 {
		for tid, parts := range forgotten {
			f := kmsg.NewShareFetchRequestForgottenTopicsData()
			f.TopicID = tid
			f.Partitions = parts
			req.ForgottenTopicsData = append(req.ForgottenTopicsData, f)
		}
	}

	// KIP-1222: when IsRenewAck is set, all fetch params must be zero
	// because the broker skips fetching entirely. The broker validates
	// maxBytes, minBytes, maxRecords, and maxWaitMs are all 0.
	if req.IsRenewAck {
		req.MaxWaitMillis = 0
		req.MinBytes = 0
		req.MaxBytes = 0
		req.MaxRecords = 0
		req.BatchSize = 0
	}

	s.cfg.logger.Log(LogLevelDebug, "built share fetch request",
		"broker", src.nodeID,
		"session_epoch", req.ShareSessionEpoch,
		"num_topics", len(req.Topics),
		"num_forgotten", len(req.ForgottenTopicsData),
	)

	return req
}

// attachAcksToShareFetch piggybacks pending ack batches onto a ShareFetch
// request. Acks are added to existing topic/partition entries (via the
// partition index) or new ack-only entries are created.
func (s *shareConsumer) attachAcksToShareFetch(
	req *kmsg.ShareFetchRequest,
	pendingAcks map[[16]byte]map[int32][]shareAckBatch,
	topicIdx map[[16]byte]int,
	pIdx map[topicPartIdx]int,
) {
	for tid, parts := range pendingAcks {
		tidx, ok := topicIdx[tid]
		if !ok {
			tidx = len(req.Topics)
			topicIdx[tid] = tidx
			t := kmsg.NewShareFetchRequestTopic()
			t.TopicID = tid
			req.Topics = append(req.Topics, t)
		}

		for partition, batches := range parts {
			key := topicPartIdx{tid, partition}
			pi, ok := pIdx[key]
			if !ok {
				pi = len(req.Topics[tidx].Partitions)
				pIdx[key] = pi
				p := kmsg.NewShareFetchRequestTopicPartition()
				p.Partition = partition
				p.PartitionMaxBytes = 0
				req.Topics[tidx].Partitions = append(req.Topics[tidx].Partitions, p)
			}

			for _, batch := range batches {
				ab := kmsg.NewShareFetchRequestTopicPartitionAcknowledgementBatche()
				ab.FirstOffset = batch.firstOffset
				ab.LastOffset = batch.lastOffset
				ab.AcknowledgeTypes = []int8{batch.ackType}
				req.Topics[tidx].Partitions[pi].AcknowledgementBatches = append(
					req.Topics[tidx].Partitions[pi].AcknowledgementBatches, ab,
				)
			}
		}
	}
	req.IsRenewAck = hasRenewAck(pendingAcks)
}

// handleShareFetchResponse processes a ShareFetch response, updating session
// epoch and building a Fetch. Returns the Fetch and whether the caller should
// retry (on session errors). Each acquired record has a shareAckState
// injected into its Context for ack tracking.
//
// The req parameter is used to re-queue piggybacked acks when the broker
// returns a retryable AcknowledgeErrorCode for a partition.
func (s *shareConsumer) handleShareFetchResponse(
	src *source,
	req *kmsg.ShareFetchRequest,
	resp *kmsg.ShareFetchResponse,
) (Fetch, bool, []shareMove) {
	if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
		switch {
		case errors.Is(err, kerr.ShareSessionNotFound),
			errors.Is(err, kerr.InvalidShareSessionEpoch),
			errors.Is(err, kerr.ShareSessionLimitReached):
			s.cfg.logger.Log(LogLevelInfo, "share session error, resetting epoch",
				"broker", src.nodeID,
				"err", err,
			)
			src.cursorsMu.Lock()
			src.shareSessionEpoch = 0
			src.cursorsMu.Unlock()
			return Fetch{}, true, nil

		default:
			// Non-session error. The Java client increments the
			// epoch (the broker may or may not have advanced
			// its epoch before returning the error). Match the
			// reference implementation: increment and return the
			// error without retrying the session.
			s.cfg.logger.Log(LogLevelInfo, "share fetch top-level error, incrementing epoch",
				"broker", src.nodeID,
				"err", err,
			)
			src.cursorsMu.Lock()
			if src.shareSessionEpoch == math.MaxInt32 {
				src.shareSessionEpoch = 1
			} else {
				src.shareSessionEpoch++
			}
			src.cursorsMu.Unlock()
			return Fetch{Topics: []FetchTopic{{
				Partitions: []FetchPartition{{Partition: -1, Err: err}},
			}}}, false, nil
		}
	}

	// Success -- advance session epoch. We wrap from MaxInt32 to 1,
	// skipping 0 because epoch 0 means "new session" and would cause
	// the broker to start a fresh session rather than continue the
	// existing one.
	src.cursorsMu.Lock()
	if src.shareSessionEpoch == math.MaxInt32 {
		src.shareSessionEpoch = 1
	} else {
		src.shareSessionEpoch++
	}
	src.cursorsMu.Unlock()

	acqLockDeadline := time.Now().Add(time.Duration(resp.AcquisitionLockTimeoutMillis) * time.Millisecond)

	// Track piggybacked ack partitions from the request. If the broker
	// omits a partition from the response entirely, we re-queue the acks
	// so they are retried on the next request rather than silently lost.
	piggybackedAcks := make(map[topicPartIdx]struct{})
	for i := range req.Topics {
		for j := range req.Topics[i].Partitions {
			if len(req.Topics[i].Partitions[j].AcknowledgementBatches) > 0 {
				piggybackedAcks[topicPartIdx{req.Topics[i].TopicID, req.Topics[i].Partitions[j].Partition}] = struct{}{}
			}
		}
	}

	id2t := s.cl.id2tMap()
	var fetch Fetch
	var moves []shareMove
	topicIdx := make(map[string]int)

	for i := range resp.Topics {
		rt := &resp.Topics[i]
		topicName := id2t[rt.TopicID]
		if topicName == "" {
			continue
		}

		for j := range rt.Partitions {
			rp := &rt.Partitions[j]
			delete(piggybackedAcks, topicPartIdx{rt.TopicID, rp.Partition})

			if rp.AcknowledgeErrorCode != 0 {
				ackErr := kerr.ErrorForCode(rp.AcknowledgeErrorCode)
				requeued := false
				if isAckRetryable(ackErr) {
					requeueAcksForPartition(src, req, rt.TopicID, rp.Partition)
					requeued = true
				}
				s.cfg.logger.Log(LogLevelWarn, "share fetch acknowledge error",
					"topic", topicName,
					"partition", rp.Partition,
					"err", ackErr,
					"requeued", requeued,
				)
			}

			if rp.ErrorCode != 0 {
				// If the broker tells us the new leader, collect
				// a move instead of surfacing the error. The
				// piggybacked acks are for acquisitions from the
				// old leader and are invalid on the new leader,
				// so we drop them rather than re-queuing.
				if errors.Is(kerr.ErrorForCode(rp.ErrorCode), kerr.NotLeaderForPartition) &&
					rp.CurrentLeader.LeaderID >= 0 &&
					rp.CurrentLeader.LeaderEpoch >= 0 {
					moves = append(moves, shareMove{
						topicID:     rt.TopicID,
						partition:   rp.Partition,
						leaderID:    rp.CurrentLeader.LeaderID,
						leaderEpoch: rp.CurrentLeader.LeaderEpoch,
					})
					delete(piggybackedAcks, topicPartIdx{rt.TopicID, rp.Partition})
					continue
				}
				idx, ok := topicIdx[topicName]
				if !ok {
					idx = len(fetch.Topics)
					topicIdx[topicName] = idx
					fetch.Topics = append(fetch.Topics, FetchTopic{Topic: topicName})
				}
				fetch.Topics[idx].Partitions = append(fetch.Topics[idx].Partitions, FetchPartition{
					Partition: rp.Partition,
					Err:       kerr.ErrorForCode(rp.ErrorCode),
				})
				continue
			}

			if len(rp.Records) == 0 && len(rp.AcquiredRecords) == 0 {
				continue
			}

			// Reuse ProcessFetchPartition to decode record
			// batches. We build a synthetic
			// FetchResponseTopicPartition because ShareFetch
			// uses the same wire format for records. The
			// sentinel -1 values signal "not applicable" for
			// fields that only exist in regular fetch responses
			// (HWM, LSO, LogStartOffset). IsolationLevel and
			// Offset are zero because share groups have no
			// concept of read-committed and we want all decoded
			// records (filtering by AcquiredRecords below).
			fakePart := kmsg.NewFetchResponseTopicPartition()
			fakePart.Partition = rp.Partition
			fakePart.RecordBatches = rp.Records
			fakePart.HighWatermark = -1
			fakePart.LastStableOffset = -1
			fakePart.LogStartOffset = -1

			fp, _ := ProcessFetchPartition(ProcessFetchPartitionOpts{
				KeepControlRecords:   s.cfg.keepControl,
				DisableCRCValidation: s.cfg.disableFetchCRCValidation,
				Topic:                topicName,
				Partition:            rp.Partition,
				Pools:                s.cfg.pools,
			}, &fakePart, s.cfg.decompressor, nil)

			// Filter to only records within AcquiredRecords ranges
			// and inject shareAckState into each. Both fp.Records
			// and AcquiredRecords are sorted by offset, so we use a
			// two-pointer scan for O(n+m).
			//
			// Gap handling: if compaction removed records from the
			// middle of an acquired range, those offsets have no
			// physical record but are still "acquired" by the
			// broker. We immediately queue GAP acks (type 0) for
			// gap offsets so the broker releases them. Without
			// this, gap offsets remain permanently acquired.
			var (
				acquired []*Record
				gapAcks  []shareAckBatch
				ri       int
			)
			for _, ar := range rp.AcquiredRecords {
				nextExpected := ar.FirstOffset
				for ri < len(fp.Records) && fp.Records[ri].Offset < ar.FirstOffset {
					ri++
				}
				for ri < len(fp.Records) && fp.Records[ri].Offset <= ar.LastOffset {
					r := fp.Records[ri]
					if r.Offset > nextExpected {
						gapAcks = append(gapAcks, shareAckBatch{
							firstOffset: nextExpected,
							lastOffset:  r.Offset - 1,
							ackType:     ackGap,
						})
					}
					ackState := &shareAckState{
						deliveryCount:           int32(ar.DeliveryCount),
						acquisitionLockDeadline: acqLockDeadline,
						source:                  src,
					}
					ctx := r.Context
					if ctx == nil {
						ctx = context.Background()
					}
					r.Context = context.WithValue(ctx, ctxShareAck, ackState)
					acquired = append(acquired, r)
					nextExpected = r.Offset + 1
					ri++
				}
				if nextExpected <= ar.LastOffset {
					gapAcks = append(gapAcks, shareAckBatch{
						firstOffset: nextExpected,
						lastOffset:  ar.LastOffset,
						ackType:     ackGap,
					})
				}
			}
			if len(gapAcks) > 0 {
				src.addShareAcks(rt.TopicID, rp.Partition, gapAcks)
			}

			if len(acquired) == 0 && fp.Err == nil {
				continue
			}

			idx, ok := topicIdx[topicName]
			if !ok {
				idx = len(fetch.Topics)
				topicIdx[topicName] = idx
				fetch.Topics = append(fetch.Topics, FetchTopic{Topic: topicName})
			}
			fp.Records = acquired
			fetch.Topics[idx].Partitions = append(fetch.Topics[idx].Partitions, fp)
		}
	}

	// Re-queue piggybacked acks for partitions the broker omitted from
	// the response. This can happen if the partition was removed from the
	// share session between request and response.
	for key := range piggybackedAcks {
		requeueAcksForPartition(src, req, key.topicID, key.partition)
		s.cfg.logger.Log(LogLevelWarn, "re-queued piggybacked acks for partition missing from share fetch response",
			"broker", src.nodeID,
			"partition", key.partition,
		)
	}

	return fetch, false, moves
}

// closeShareSessions sends ShareFetch with ShareSessionEpoch=-1 to each
// source that has an active share session or pending acks, telling the
// broker to close the session. Pending acks are piggybacked on the close
// request so the broker processes them in one round trip. Without the
// close, the broker keeps the session alive and the group may remain
// NON_EMPTY after the leave heartbeat.
func (s *shareConsumer) closeShareSessions(ctx context.Context) {
	memberID, _ := s.memberGen.load()

	for src := range s.collectAllSources() {
		acks := src.drainShareAcks()

		src.cursorsMu.Lock()
		epoch := src.shareSessionEpoch
		hasSession := epoch > 0 || len(src.shareCursors) > 0
		src.cursorsMu.Unlock()

		if !hasSession && len(acks) == 0 {
			continue
		}

		req := kmsg.NewPtrShareFetchRequest()
		req.GroupID = &s.cfg.shareGroup
		req.MemberID = &memberID
		req.ShareSessionEpoch = -1 // close session

		if len(acks) > 0 {
			topicIdx := make(map[[16]byte]int)
			partIdx := make(map[topicPartIdx]int)
			s.attachAcksToShareFetch(req, acks, topicIdx, partIdx)
		}

		br, err := s.cl.brokerOrErr(ctx, src.nodeID, errUnknownBroker)
		if err != nil {
			continue
		}

		kresp, err := br.waitResp(ctx, req)
		if err != nil {
			s.cfg.logger.Log(LogLevelDebug, "failed to close share session",
				"broker", src.nodeID, "err", err,
			)
			continue
		}
		resp := kresp.(*kmsg.ShareFetchResponse)
		if resp.ErrorCode != 0 {
			s.cfg.logger.Log(LogLevelDebug, "share session close returned error",
				"broker", src.nodeID, "err", kerr.ErrorForCode(resp.ErrorCode),
			)
		}
		src.cursorsMu.Lock()
		src.shareSessionEpoch = 0
		src.cursorsMu.Unlock()
	}
}
