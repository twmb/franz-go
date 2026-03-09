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

// shareAckState is injected into each share group Record's Context via
// context.WithValue. It tracks the ack status (pending, user-set, or
// finalized) and the delivery count from the broker.
type shareAckState struct {
	mu            sync.Mutex
	status        uint8 // 0=pending, low 7 bits=AckStatus, high bit=ackFinalized
	deliveryCount int32
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

// sharePartKey identifies a partition for ack routing, using topicID
// to avoid topic-name-to-ID lookups when building ack requests.
type sharePartKey struct {
	topicID   [16]byte
	partition int32
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
	// broker. Cleaned up lazily in collectShareSources once the
	// forgotten state is drained. Protected by c.mu.
	sourcesWithForgotten map[*source]struct{}

	// cursorsChanged is signaled by assignSharePartitions (under c.mu)
	// when share cursors are added or removed. pollShareFetches waits
	// on this when no sources are available, avoiding a busy-wait loop.
	cursorsChanged *sync.Cond

	// lastPolled tracks records from the previous poll for auto-ACCEPT.
	// Protected by consumer.mu (grabbed during PollFetches/PollRecords).
	lastPolled []sharePolledRecord
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
			s.cfg.logger.Log(LogLevelInfo, "share group heartbeat error, restarting with new member id",
				"group", s.cfg.shareGroup,
				"member_id", member,
				"generation", gen,
				"err", err,
			)
			// Clear all cursors immediately so in-flight polls
			// stop sending ShareFetch with the old member ID.
			s.reconcileAssignment(nil)
			s.unresolvedAssigned = nil
			consecutiveErrors = 0
			continue

		case errors.Is(err, kerr.FencedMemberEpoch):
			member, gen := s.memberGen.load()
			s.memberGen.store(newStringUUID(), 0)
			s.cfg.logger.Log(LogLevelInfo, "share group heartbeat fenced, restarting with new member id",
				"group", s.cfg.shareGroup,
				"member_id", member,
				"generation", gen,
				"err", err,
			)
			s.reconcileAssignment(nil)
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
		s.cl.cfg.logger.Log(LogLevelDebug, "storing share member and epoch", "group", s.cfg.shareGroup, "member", *resp.MemberID, "epoch", resp.MemberEpoch)
	} else {
		s.memberGen.storeGeneration(resp.MemberEpoch)
		s.cl.cfg.logger.Log(LogLevelDebug, "storing share epoch", "group", s.cfg.shareGroup, "epoch", resp.MemberEpoch)
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
// acks to sources. If autoAcceptPending is true, records with no explicit ack
// status (status==0) are auto-accepted; otherwise they are skipped.
//
// Must be called under consumer.mu.
func (s *shareConsumer) routeAcksToSources(autoAcceptPending bool) {
	bySource := make(map[*source]map[sharePartKey][]shareAckBatch)

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

		m := bySource[sc.source]
		if m == nil {
			m = make(map[sharePartKey][]shareAckBatch)
			bySource[sc.source] = m
		}
		key := sharePartKey{sc.topicID, pr.partition}
		m[key] = append(m[key], shareAckBatch{
			firstOffset: pr.offset,
			lastOffset:  pr.offset,
			ackType:     ackType,
		})
	}

	// Merge contiguous same-type batches and route to sources.
	for src, partBatches := range bySource {
		for key, batches := range partBatches {
			batches = mergeAckBatches(batches)
			src.addShareAcks(key.topicID, key.partition, batches)
		}
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
func (s *shareConsumer) sendAcksFromSources(ctx context.Context, sources map[*source]struct{}) error {
	type sourceAcks struct {
		src  *source
		acks map[[16]byte]map[int32][]shareAckBatch
	}
	var pending []sourceAcks
	for src := range sources {
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
	hasRenew := s.buildAckRequests(acks, func(tid [16]byte, partition int32, batches []shareAckBatch) {
		idx, ok := topicIdx[tid]
		if !ok {
			idx = len(req.Topics)
			topicIdx[tid] = idx
			t := kmsg.NewShareAcknowledgeRequestTopic()
			t.TopicID = tid
			req.Topics = append(req.Topics, t)
		}
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
	})
	req.IsRenewAck = hasRenew

	kresp, err := s.cl.retryableBrokerFn(func() (*broker, error) {
		return s.cl.brokerOrErr(ctx, src.nodeID, errUnknownBroker)
	}).Request(ctx, req)
	if err != nil {
		return err
	}

	var errs []error
	resp := kresp.(*kmsg.ShareAcknowledgeResponse)
	if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
		errs = append(errs, err)
	}

	id2t := s.cl.id2tMap()
	for _, rt := range resp.Topics {
		topicName := id2t[rt.TopicID]
		for _, rp := range rt.Partitions {
			if rp.ErrorCode == 0 {
				continue
			}
			err := kerr.ErrorForCode(rp.ErrorCode)
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

		// Send standalone ShareAcknowledge for any pending acks, then
		// close the share session (epoch -1) on each source.
		s.sendPendingAcknowledgements(ctx)
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

// buildAckRequests converts pending ack batches (topicID -> partition ->
// batches) into kmsg request-level topic/partition structures. The fn callback
// receives (topicID, partition, ackBatches) and should append to the caller's
// request. Returns true if any batch has AckRenew type.
func (s *shareConsumer) buildAckRequests(
	acks map[[16]byte]map[int32][]shareAckBatch,
	fn func(tid [16]byte, partition int32, batches []shareAckBatch),
) bool {
	hasRenew := false
	for tid, parts := range acks {
		for partition, batches := range parts {
			fn(tid, partition, batches)
			for _, b := range batches {
				if b.ackType == int8(AckRenew) {
					hasRenew = true
				}
			}
		}
	}
	return hasRenew
}

// collectShareSources returns the set of sources that have share cursors
// or pending forgotten state. Sources from sourcesWithForgotten are
// lazily cleaned up once their forgotten/pending-ack state is drained.
// Must be called under c.mu.
func (s *shareConsumer) collectShareSources() map[*source]struct{} {
	sources := make(map[*source]struct{})
	for _, parts := range s.usingShareCursors {
		for _, sc := range parts {
			sources[sc.source] = struct{}{}
		}
	}
	// Include sources that still need to send ForgottenTopicsData
	// or flush pending acks. Once buildShareFetchRequest drains the
	// state, the next call here cleans up the tracking entry.
	for src := range s.sourcesWithForgotten {
		if _, already := sources[src]; already {
			delete(s.sourcesWithForgotten, src)
			continue
		}
		src.cursorsMu.Lock()
		needsVisit := len(src.shareForgotten) > 0 || len(src.sharePendingAcks) > 0
		src.cursorsMu.Unlock()
		if needsVisit {
			sources[src] = struct{}{}
		} else {
			delete(s.sourcesWithForgotten, src)
		}
	}
	return sources
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
	// available." Share consumers have no buffered fetches, so we
	// just finalize the previous poll and drain any injected errors.
	if ctx == nil {
		s.c.mu.Lock()
		s.finalizePreviousPoll()
		s.c.mu.Unlock()
		if fake := s.c.drainFakeReady(); len(fake) > 0 {
			return Fetches(fake)
		}
		return nil
	}

	// Collect unique sources that have share cursors. If none exist
	// yet, block until the heartbeat loop assigns partitions, an
	// injected error arrives, or the context is cancelled.
	s.c.mu.Lock()
	s.finalizePreviousPoll()
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

	// Determine how many fetch rounds to issue. Each round sends one
	// ShareFetch per source in parallel. For share consumers, the
	// default (maxConcurrentFetches == 0) means one round rather than
	// unbounded -- share fetches are synchronous in the poll loop, so
	// unbounded rounds would block the caller for too long.
	nSources := len(sources)
	rounds := 1
	if n := s.cfg.maxConcurrentFetches; n > nSources {
		rounds = (n + nSources - 1) / nSources
	}

	var fetches Fetches
	for range rounds {
		results := make(chan Fetch, nSources)
		for src := range sources {
			go func() {
				results <- s.doShareFetch(ctx, src)
			}()
		}

		gotRecords := false
		for range nSources {
			f := <-results
			if f.hasErrorsOrRecords() {
				fetches = append(fetches, f)
				gotRecords = true
			}
		}

		// Stop issuing rounds once we have some data or the context
		// is cancelled, rather than always exhausting all rounds.
		if gotRecords || ctx.Err() != nil {
			break
		}
	}

	// Append any injected error fetches that arrived during the share
	// fetch round-trips.
	fetches = append(fetches, fakeFetches...)

	// Track ALL records from this poll for auto-ACCEPT on the next
	// poll. We must do this before any maxPollRecords trimming so
	// that records beyond the limit are still tracked and will be
	// auto-ACCEPTed -- the broker already considers them acquired.
	s.c.mu.Lock()
	for i := range fetches {
		for j := range fetches[i].Topics {
			t := &fetches[i].Topics[j]
			for k := range t.Partitions {
				p := &t.Partitions[k]
				for _, r := range p.Records {
					if st := shareAckFromCtx(r); st != nil {
						s.lastPolled = append(s.lastPolled, sharePolledRecord{
							state:     st,
							topic:     t.Topic,
							partition: p.Partition,
							offset:    r.Offset,
						})
					}
				}
			}
		}
	}
	s.c.mu.Unlock()

	// If maxPollRecords is set, trim fetches to the limit. Records
	// beyond the limit are still tracked in lastPolled above and
	// will be auto-ACCEPTed on the next poll.
	if maxPollRecords > 0 {
		remaining := maxPollRecords
		for i := range fetches {
			for j := range fetches[i].Topics {
				t := &fetches[i].Topics[j]
				for k := range t.Partitions {
					p := &t.Partitions[k]
					if remaining >= len(p.Records) {
						remaining -= len(p.Records)
					} else {
						p.Records = p.Records[:remaining]
						remaining = 0
					}
				}
			}
		}
	}

	return fetches
}

// doShareFetch sends a single ShareFetch request to one source and
// returns the decoded Fetch. On session errors, it retries once with a
// reset epoch.
func (s *shareConsumer) doShareFetch(ctx context.Context, src *source) Fetch {
	return s.doShareFetchInner(ctx, src, true)
}

func (s *shareConsumer) doShareFetchInner(ctx context.Context, src *source, canRetry bool) Fetch {
	req := s.buildShareFetchRequest(src)
	if req == nil {
		return Fetch{}
	}

	br, err := s.cl.brokerOrErr(ctx, src.nodeID, errUnknownBroker)
	if err != nil {
		restoreShareForgotten(src, req)
		return Fetch{}
	}

	// Use the same br.do + select pattern as regular fetch (source.go)
	// so that context cancellation immediately unblocks us rather than
	// relying on readConn's ctx.Done propagation alone.
	var kresp kmsg.Response
	requested := make(chan struct{})
	br.do(ctx, req, func(k kmsg.Response, e error) {
		kresp, err = k, e
		close(requested)
	})

	select {
	case <-requested:
		if isContextErr(err) && ctx.Err() != nil {
			restoreShareForgotten(src, req)
			return Fetch{}
		}
	case <-ctx.Done():
		restoreShareForgotten(src, req)
		return Fetch{}
	}

	if err != nil {
		src.consecutiveFailures++
		restoreShareForgotten(src, req)
		after := time.NewTimer(s.cfg.retryBackoff(src.consecutiveFailures))
		select {
		case <-after.C:
		case <-ctx.Done():
		}
		after.Stop()
		return Fetch{}
	}
	src.consecutiveFailures = 0

	resp := kresp.(*kmsg.ShareFetchResponse)

	s.cfg.logger.Log(LogLevelDebug, "share fetch response",
		"broker", src.nodeID,
		"error_code", resp.ErrorCode,
		"num_topics", len(resp.Topics),
	)

	fetch, retry := s.handleShareFetchResponse(src, resp)
	if retry && canRetry {
		// Session error (e.g. SHARE_SESSION_NOT_FOUND) -- epoch was
		// reset by handleShareFetchResponse. Retry once with the new
		// epoch to re-establish the session.
		return s.doShareFetchInner(ctx, src, false)
	}

	return fetch
}

// restoreShareForgotten puts ForgottenTopicsData from a failed ShareFetch
// request back onto the source so it will be included in the next request.
// buildShareFetchRequest drains shareForgotten when building the request;
// if the request fails before reaching the broker, the forgotten state
// would be permanently lost, causing the client to never send
// ForgottenTopicsData and potentially leaving stale share sessions.
func restoreShareForgotten(src *source, req *kmsg.ShareFetchRequest) {
	if len(req.ForgottenTopicsData) == 0 {
		return
	}
	src.cursorsMu.Lock()
	defer src.cursorsMu.Unlock()
	if src.shareForgotten == nil {
		src.shareForgotten = make(map[[16]byte][]int32)
	}
	for _, f := range req.ForgottenTopicsData {
		src.shareForgotten[f.TopicID] = append(src.shareForgotten[f.TopicID], f.Partitions...)
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
	topicIdx := make(map[[16]byte]int) // topicID -> index in req.Topics
	for _, sc := range cursors {
		idx, ok := topicIdx[sc.topicID]
		if !ok {
			idx = len(req.Topics)
			topicIdx[sc.topicID] = idx
			t := kmsg.NewShareFetchRequestTopic()
			t.TopicID = sc.topicID
			req.Topics = append(req.Topics, t)
		}
		p := kmsg.NewShareFetchRequestTopicPartition()
		p.Partition = sc.partition
		p.PartitionMaxBytes = int32(s.cfg.maxBytes)
		req.Topics[idx].Partitions = append(req.Topics[idx].Partitions, p)
	}

	// Attach piggybacked acks.
	if len(pendingAcks) > 0 {
		s.attachAcksToShareFetch(req, pendingAcks, topicIdx)
	}

	// Attach forgotten topics.
	for tid, parts := range forgotten {
		f := kmsg.NewShareFetchRequestForgottenTopicsData()
		f.TopicID = tid
		f.Partitions = parts
		req.ForgottenTopicsData = append(req.ForgottenTopicsData, f)
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
// request. Acks are added to existing topic/partition entries or new
// ack-only entries are created.
func (s *shareConsumer) attachAcksToShareFetch(
	req *kmsg.ShareFetchRequest,
	pendingAcks map[[16]byte]map[int32][]shareAckBatch,
	topicIdx map[[16]byte]int,
) {
	hasRenew := s.buildAckRequests(pendingAcks, func(tid [16]byte, partition int32, batches []shareAckBatch) {
		idx, ok := topicIdx[tid]
		if !ok {
			idx = len(req.Topics)
			topicIdx[tid] = idx
			t := kmsg.NewShareFetchRequestTopic()
			t.TopicID = tid
			req.Topics = append(req.Topics, t)
		}

		partIdx := -1
		for i, p := range req.Topics[idx].Partitions {
			if p.Partition == partition {
				partIdx = i
				break
			}
		}
		if partIdx < 0 {
			partIdx = len(req.Topics[idx].Partitions)
			p := kmsg.NewShareFetchRequestTopicPartition()
			p.Partition = partition
			p.PartitionMaxBytes = 0
			req.Topics[idx].Partitions = append(req.Topics[idx].Partitions, p)
		}

		for _, batch := range batches {
			ab := kmsg.NewShareFetchRequestTopicPartitionAcknowledgementBatche()
			ab.FirstOffset = batch.firstOffset
			ab.LastOffset = batch.lastOffset
			ab.AcknowledgeTypes = []int8{batch.ackType}
			req.Topics[idx].Partitions[partIdx].AcknowledgementBatches = append(
				req.Topics[idx].Partitions[partIdx].AcknowledgementBatches, ab,
			)
		}
	})
	req.IsRenewAck = hasRenew
}

// handleShareFetchResponse processes a ShareFetch response, updating session
// epoch and building a Fetch. Returns the Fetch and whether the caller should
// retry (on session errors). Each acquired record has a shareAckState
// injected into its Context for ack tracking.
func (s *shareConsumer) handleShareFetchResponse(
	src *source,
	resp *kmsg.ShareFetchResponse,
) (Fetch, bool) {
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
			return Fetch{}, true

		case errors.Is(err, kerr.UnknownMemberID),
			errors.Is(err, kerr.FencedMemberEpoch):
			// The member was fenced or removed. The heartbeat
			// loop is already recovering by assigning a new
			// member ID, so this is just an in-flight response
			// on the old member. Reset the session epoch (the
			// old session is dead) and return an empty fetch
			// without surfacing an error to the application.
			s.cfg.logger.Log(LogLevelInfo, "share fetch member error, resetting epoch",
				"broker", src.nodeID,
				"err", err,
			)
			src.cursorsMu.Lock()
			src.shareSessionEpoch = 0
			src.cursorsMu.Unlock()
			return Fetch{}, false

		default:
			s.cfg.logger.Log(LogLevelError, "share fetch top-level error",
				"broker", src.nodeID,
				"err", err,
			)
			return Fetch{Topics: []FetchTopic{{
				Partitions: []FetchPartition{{Partition: -1, Err: err}},
			}}}, false
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

	id2t := s.cl.id2tMap()
	var fetch Fetch
	topicIdx := make(map[string]int)

	for i := range resp.Topics {
		rt := &resp.Topics[i]
		topicName := id2t[rt.TopicID]
		if topicName == "" {
			continue
		}

		for j := range rt.Partitions {
			rp := &rt.Partitions[j]

			if rp.AcknowledgeErrorCode != 0 {
				s.cfg.logger.Log(LogLevelWarn, "share fetch acknowledge error",
					"topic", topicName,
					"partition", rp.Partition,
					"err", kerr.ErrorForCode(rp.AcknowledgeErrorCode),
				)
			}

			if rp.ErrorCode != 0 {
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

			// Decode records using ProcessFetchPartition with a
			// synthetic FetchResponseTopicPartition.
			fakePart := kmsg.NewFetchResponseTopicPartition()
			fakePart.Partition = rp.Partition
			fakePart.ErrorCode = 0
			fakePart.RecordBatches = rp.Records
			fakePart.HighWatermark = -1
			fakePart.LastStableOffset = -1
			fakePart.LogStartOffset = -1

			opts := ProcessFetchPartitionOpts{
				KeepControlRecords:   s.cfg.keepControl,
				DisableCRCValidation: s.cfg.disableFetchCRCValidation,
				Offset:               0,
				Topic:                topicName,
				Partition:            rp.Partition,
				Pools:                s.cfg.pools,
			}
			fp, _ := ProcessFetchPartition(opts, &fakePart, s.cfg.decompressor, nil)

			// Filter to only records within AcquiredRecords ranges
			// and inject shareAckState into each. Both fp.Records
			// and AcquiredRecords are sorted by offset, so we use a
			// two-pointer scan for O(n+m).
			var (
				acquired []*Record
				ri       int
			)
			for _, ar := range rp.AcquiredRecords {
				for ri < len(fp.Records) && fp.Records[ri].Offset < ar.FirstOffset {
					ri++
				}
				for ri < len(fp.Records) && fp.Records[ri].Offset <= ar.LastOffset {
					r := fp.Records[ri]
					ackState := &shareAckState{
						deliveryCount: int32(ar.DeliveryCount),
					}
					ctx := r.Context
					if ctx == nil {
						ctx = context.Background()
					}
					r.Context = context.WithValue(ctx, ctxShareAck, ackState)
					acquired = append(acquired, r)
					ri++
				}
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

	return fetch, false
}

// sendPendingAcknowledgements sends standalone ShareAcknowledge requests
// for all pending acks across all sources. Used on the close/leave path
// where share cursors may already be torn down, so we iterate every
// source in the client rather than just those with active cursors.
func (s *shareConsumer) sendPendingAcknowledgements(ctx context.Context) {
	if err := s.sendAcksFromSources(ctx, s.collectAllSources()); err != nil {
		s.cfg.logger.Log(LogLevelWarn, "ShareAcknowledge on leave failed", "err", err)
	}
}

// closeShareSessions sends ShareFetch with ShareSessionEpoch=-1 to each
// source that has an active share session, telling the broker to close the
// session. Without this, the broker keeps the session alive and the group
// may remain NON_EMPTY after the leave heartbeat.
func (s *shareConsumer) closeShareSessions(ctx context.Context) {
	memberID, _ := s.memberGen.load()

	for src := range s.collectAllSources() {
		src.cursorsMu.Lock()
		epoch := src.shareSessionEpoch
		hasSession := epoch > 0 || len(src.shareCursors) > 0
		src.cursorsMu.Unlock()

		if !hasSession {
			continue
		}

		req := kmsg.NewPtrShareFetchRequest()
		req.GroupID = &s.cfg.shareGroup
		req.MemberID = &memberID
		req.ShareSessionEpoch = -1 // close session

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
	}
}
