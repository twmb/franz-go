package kgo

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

var (
	errNotShareGroup          = errors.New("client is not configured as a share group consumer")
	errAckMissingFromResponse = errors.New("broker omitted partition from share fetch response")
)

// topicPartIdx identifies a topic+partition by topicID, used as a map key
// for O(1) partition lookup in share session tracking and request building.
type topicPartIdx struct {
	topicID   [16]byte
	partition int32
}

type AckStatus int8

const (
	AckAccept  AckStatus = 1 // successful processing
	AckRelease AckStatus = 2 // release for redelivery
	AckReject  AckStatus = 3 // unprocessable
	AckRenew   AckStatus = 4 // extend acquisition lock (KIP-1222)
)

func (s AckStatus) String() string {
	switch s {
	case AckAccept:
		return "accept"
	case AckRelease:
		return "release"
	case AckReject:
		return "reject"
	case AckRenew:
		return "renew"
	default:
		return fmt.Sprintf("AckStatus(%d)", s)
	}
}

// shareAckState is injected into each share group Record's Context via
// context.WithValue. It tracks the ack status and the delivery count.
//
// State machine (transitions under mu):
//
//	PENDING   (status=0, !finalized, !renewSent)
//	  -- user calls Record.Ack -->
//	MARKED    (status=1..4, !finalized, !renewSent)
//	  -- routeAcksToSources (non-renew) -->
//	FINALIZED (finalized=true) -- terminal, record is committed
//
//	MARKED with AckRenew:
//	  -- routeAcksToSources resets status to 0, sets renewSent -->
//	RENEWING  (status=0, !finalized, renewSent=true)
//	  -- user calls Record.Ack with terminal status -->
//	MARKED    (status=1..3, !finalized, renewSent=true)
//	  -- routeAcksToSources -->
//	FINALIZED
//
// Auto-ACCEPT on next poll: any record still in PENDING becomes FINALIZED
// with AckAccept. Records in RENEWING are skipped (the user must re-ack).
type shareAckState struct {
	mu                      sync.Mutex
	status                  AckStatus // 0=pending, 1-4=set by user
	finalized               bool      // true once the ack has been committed for sending
	renewSent               bool      // true after a RENEW has been routed, awaiting re-ack
	deliveryCount           int32
	acquisitionLockDeadline time.Time
	source                  *source
	sessionGen              uint64 // source.shareSessionGen at acquire time; stale if mismatched
}

// shareAckKeyType is an unexported type used as a context.WithValue
// key so that no other package can look up or overwrite the value.
type shareAckKeyType struct{}

var shareAckKey shareAckKeyType

func shareAckFromCtx(r *Record) *shareAckState {
	if r.Context == nil {
		return nil
	}
	v := r.Context.Value(shareAckKey)
	if v == nil {
		return nil
	}
	return v.(*shareAckState)
}

type shareAckBatch struct {
	firstOffset int64
	lastOffset  int64
	// Per-offset ack types matching the protocol's AcknowledgeTypes
	// array. If len==1, the single type applies to all offsets in
	// [firstOffset, lastOffset]. Otherwise len must equal
	// lastOffset-firstOffset+1 with one type per offset.
	ackTypes []int8
}

// sharePendingAcks maps topicID -> partition -> pending ack batches.
// This type centralizes the two-level lazy initialization that appears
// across source and share consumer code.
type sharePendingAcks map[[16]byte]map[int32][]shareAckBatch

func (a sharePendingAcks) add(
	topicID [16]byte,
	partition int32,
	batches []shareAckBatch,
) {
	parts := a[topicID]
	if parts == nil {
		parts = make(map[int32][]shareAckBatch)
		a[topicID] = parts
	}
	parts[partition] = append(parts[partition], batches...)
}

func (a sharePendingAcks) merge(other sharePendingAcks) {
	for tid, parts := range other {
		for p, batches := range parts {
			a.add(tid, p, batches)
		}
	}
}

type shareMove struct {
	topicID   [16]byte
	partition int32
	leaderID  int32
}

// tryLeaderMove checks whether err indicates a leader change and the
// response includes a valid CurrentLeader hint. Both ShareFetch and
// ShareAcknowledge responses have identical CurrentLeader fields but
// different Go types, so this accepts the fields directly.
func tryLeaderMove(topicID [16]byte, partition int32, err error, leaderID, leaderEpoch int32) (shareMove, bool) {
	if (errors.Is(err, kerr.NotLeaderForPartition) || errors.Is(err, kerr.FencedLeaderEpoch)) &&
		leaderID >= 0 &&
		leaderEpoch >= 0 {
		return shareMove{
			topicID:   topicID,
			partition: partition,
			leaderID:  leaderID,
		}, true
	}
	return shareMove{}, false
}

type shareFetchResult struct {
	fetch   Fetch
	moves   []shareMove
	brokers []BrokerMetadata // from resp.NodeEndpoints, for ensuring move targets
}

// ShareAckResult is a per-partition result from a share group acknowledge;
// Err is nil on success.
type ShareAckResult struct {
	Topic     string
	Partition int32
	Err       error
}

type shareCursor struct {
	topic      string
	topicID    [16]byte
	partition  int32
	source     *source
	cursorsIdx int // index in source.shareCursors, under source.cursorsMu
}

// Lock ordering: c.mu -> s.mu -> sinksAndSourcesMu -> source.cursorsMu
type shareConsumer struct {
	c   *consumer
	cl  *Client
	cfg *cfg

	ctx    context.Context
	cancel func()

	manageDone chan struct{}
	tps        *topicsPartitions
	reSeen     map[string]bool

	// memberGen and nowAssigned are read and written from the manage
	// goroutine and during leave. memberGen is atomically accessed.
	memberGen   groupMemberGen
	nowAssigned amtps

	// unresolvedAssigned holds topic IDs from heartbeat responses
	// that could not be mapped to names yet. Manage goroutine only.
	unresolvedAssigned map[topicID][]int32

	// lastSentSubscribedTopics and sentRack track what was last
	// sent in a heartbeat to avoid re-sending unchanged data.
	// Manage goroutine only; reset on fence/unknown.
	lastSentSubscribedTopics []string
	sentRack                 bool

	//////////////
	// mu block //
	//////////////

	mu       sync.Mutex
	managing bool
	dying    atomic.Bool // also read atomically outside mu from pollShareFetches
	left     chan struct{}
	leaveErr error

	////////////////
	// c.mu block //
	////////////////

	usingShareCursors    map[string]map[int32]*shareCursor // topic -> partition -> shareCursor
	sourcesWithForgotten map[*source]struct{}              // sources with removed share cursors needing ForgottenTopicsData
	assignChanged        *sync.Cond                        // signaled when share cursors change

	lastPolled      []*Record // records from previous poll for auto-ACCEPT
	bufferedFetches Fetches   // excess records from prior ShareFetch, served before new fetches
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
	s.assignChanged = sync.NewCond(&c.mu)
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
		go s.manage()
	}

	s.refreshShareCursorSources()
}

// revokeShareCursor removes a share cursor from its current source and
// marks the source as having forgotten partitions. Must be called under c.mu.
func (s *shareConsumer) revokeShareCursor(sc *shareCursor) {
	s.sourcesWithForgotten[sc.source] = struct{}{}
	sc.source.removeShareCursor(sc)
}

// moveShareCursor migrates a share cursor from its current source to dst,
// marking the old source as having forgotten partitions. Must be called
// under c.mu.
func (s *shareConsumer) moveShareCursor(sc *shareCursor, dst *source) {
	s.revokeShareCursor(sc)
	sc.source = dst
	dst.addShareCursor(sc)
}

// moveShareCursorToLeader moves a share cursor to the source for the given
// leader broker. Returns true if a source exists for the leader and the
// cursor was moved.
//
// Must be called under c.mu.
func (s *shareConsumer) moveShareCursorToLeader(sc *shareCursor, leaderID int32) bool {
	s.cl.sinksAndSourcesMu.Lock()
	sns := s.cl.sinksAndSources[leaderID]
	s.cl.sinksAndSourcesMu.Unlock()
	if sns.source == nil {
		return false
	}
	s.moveShareCursor(sc, sns.source)
	return true
}

// refreshShareCursorSources migrates share cursors to new sources when
// partition leaders change. Called from findNewAssignments under both
// c.mu and s.mu (the metadata update path holds c.mu, findNewAssignments
// holds s.mu).
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
			oldLeader := sc.source.nodeID
			if s.moveShareCursorToLeader(sc, newLeader) {
				s.cfg.logger.Log(LogLevelInfo, "migrating share cursor to new leader",
					"topic", topic,
					"partition", p,
					"old_leader", oldLeader,
					"new_leader", newLeader,
				)
				moved = true
			}
		}
	}
	if moved {
		s.assignChanged.Broadcast()
	}
}

// applyShareMoves proactively moves share cursors to new leaders detected
// via CurrentLeader in ShareFetch responses, avoiding the delay of a full
// metadata refresh. Follows the same broker/source setup pattern as
// kip951move in topics_and_partitions.go.
func (s *shareConsumer) applyShareMoves(moves []shareMove, brokers []BrokerMetadata) {
	s.cl.ensureBrokers(brokers)

	leaders := make([]int32, 0, len(moves))
	for _, m := range moves {
		leaders = append(leaders, m.leaderID)
	}
	s.cl.ensureSinksAndSources(leaders)

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
		oldLeader := sc.source.nodeID
		if s.moveShareCursorToLeader(sc, m.leaderID) {
			s.cfg.logger.Log(LogLevelInfo, "share cursor move via CurrentLeader",
				"topic", topicName,
				"partition", m.partition,
				"old_leader", oldLeader,
				"new_leader", m.leaderID,
			)
			moved = true
		}
	}
	if moved {
		s.assignChanged.Broadcast()
	}
	s.c.mu.Unlock()
}

func (s *shareConsumer) manage() {
	defer close(s.manageDone)
	s.cfg.logger.Log(LogLevelInfo, "beginning to manage the share group lifecycle", "group", s.cfg.shareGroup)

	s.memberGen.store(newStringUUID(), 0)

	var consecutiveErrors int
	for {
		sleep, err := s.heartbeat()

		if err == nil {
			consecutiveErrors = 0

			timer := time.NewTimer(sleep)
			select {
			case <-s.ctx.Done():
				timer.Stop()
				return
			case <-timer.C:
			}
			continue
		}

		switch {
		case errors.Is(err, context.Canceled):
			return

		case errors.Is(err, kerr.UnknownMemberID),
			errors.Is(err, kerr.FencedMemberEpoch):
			// UnknownMemberID: the broker does not know our ID.
			// FencedMemberEpoch: the broker fenced our epoch.
			// In both cases, keep the same UUID and reset to
			// epoch 0 so the next heartbeat re-introduces us.
			// The Java client also keeps the same UUID (final).
			member, gen := s.memberGen.load()
			s.memberGen.storeGeneration(0)
			s.cfg.logger.Log(LogLevelInfo, "share group heartbeat lost membership, resetting epoch",
				"group", s.cfg.shareGroup,
				"member_id", member,
				"generation", gen,
				"err", err,
			)
			s.lastSentSubscribedTopics = nil
			s.sentRack = false
			s.c.mu.Lock()
			s.assignSharePartitions(nil)
			clear(s.sourcesWithForgotten)
			s.c.mu.Unlock()
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

		// Reset sent-field tracking on any error so we re-send
		// SubscribedTopicNames and RackID on the next heartbeat.
		// If the coordinator changed, the new coordinator may
		// not have our state yet. Matches the Java client's
		// resetHeartbeatState on every error/failure.
		s.lastSentSubscribedTopics = nil
		s.sentRack = false

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

func (s *shareConsumer) heartbeat() (time.Duration, error) {
	req := kmsg.NewPtrShareGroupHeartbeatRequest()
	req.GroupID = s.cfg.shareGroup
	req.MemberID, req.MemberEpoch = s.memberGen.load()

	// Only send RackID once; reset on fence/unknown forces re-send.
	if s.cfg.rack != "" && !s.sentRack {
		req.RackID = &s.cfg.rack
	}

	// Only include SubscribedTopicNames if the set changed since the
	// last successful heartbeat (or on the first heartbeat / after
	// reset). This avoids resending the same list every interval.
	tps := s.tps.load()
	subscribedTopics := make([]string, 0, len(tps))
	for t := range tps {
		subscribedTopics = append(subscribedTopics, t)
	}
	slices.Sort(subscribedTopics)
	if s.lastSentSubscribedTopics == nil || !slices.Equal(subscribedTopics, s.lastSentSubscribedTopics) {
		req.SubscribedTopicNames = subscribedTopics
	}

	resp, err := req.RequestWith(s.ctx, s.cl)
	sleep := s.cfg.heartbeatInterval
	if err == nil {
		err = errCodeMessage(resp.ErrorCode, resp.ErrorMessage)
		sleep = time.Duration(resp.HeartbeatIntervalMillis) * time.Millisecond
	}
	if err != nil {
		return sleep, err
	}

	// Heartbeat succeeded -- mark fields as sent.
	s.lastSentSubscribedTopics = subscribedTopics
	if req.RackID != nil {
		s.sentRack = true
	}

	newAssigned := s.handleResp(resp)
	if newAssigned != nil {
		s.c.mu.Lock()
		s.assignSharePartitions(newAssigned)
		s.c.mu.Unlock()
	}
	return sleep, nil
}

func (s *shareConsumer) handleResp(resp *kmsg.ShareGroupHeartbeatResponse) map[string][]int32 {
	// The member ID is client-generated and immutable for the
	// member's lifetime (Java declares it final). The broker
	// echoes it back but we must not overwrite ours -- doing so
	// would create a dependency on the broker's response that
	// the protocol does not intend.
	s.memberGen.storeGeneration(resp.MemberEpoch)
	s.cfg.logger.Log(LogLevelDebug, "storing share epoch", "group", s.cfg.shareGroup, "epoch", resp.MemberEpoch)

	if resp.Assignment == nil {
		if len(s.unresolvedAssigned) == 0 {
			return nil
		}
		// No new assignment from the server, but we have topic
		// IDs from a previous assignment that could not be mapped
		// to names. Metadata may have arrived since then -- try
		// to resolve them now.
		resolved := s.resolveUnresolvedTopicIDs(nil)
		if len(resolved) == 0 {
			return nil
		}
		// Merge newly-resolved topics into the current assignment.
		current := s.nowAssigned.read()
		merged := make(map[string][]int32, len(current)+len(resolved))
		maps.Copy(merged, current)
		maps.Copy(merged, resolved)
		return merged
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

	s.resolveUnresolvedTopicIDs(newAssigned)
	if len(s.unresolvedAssigned) > 0 {
		s.cl.triggerUpdateMetadataNow("share group heartbeat has unresolved topic IDs in assignment")
	}

	// Don't revoke everything when the server assigned topics we
	// can't resolve yet. Wait for metadata resolution instead of
	// treating the assignment as empty.
	if len(newAssigned) == 0 && len(s.unresolvedAssigned) > 0 {
		return nil
	}

	current := s.nowAssigned.read()
	if !mapi32sDeepEq(current, newAssigned) {
		return newAssigned
	}
	return nil
}

// resolveUnresolvedTopicIDs attempts to map unresolved topic IDs to names
// via metadata. Resolved entries are removed from unresolvedAssigned and
// added to dst (if non-nil) or returned as a new map.
func (s *shareConsumer) resolveUnresolvedTopicIDs(dst map[string][]int32) map[string][]int32 {
	if len(s.unresolvedAssigned) == 0 {
		return dst
	}
	id2t := s.cl.id2tMap()
	for id, ps := range s.unresolvedAssigned {
		name := id2t[[16]byte(id)]
		if name == "" {
			continue
		}
		if dst == nil {
			dst = make(map[string][]int32)
		}
		slices.Sort(ps)
		dst[name] = ps
		delete(s.unresolvedAssigned, id)
	}
	return dst
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
				s.revokeShareCursor(sc)
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
				s.revokeShareCursor(sc)
				delete(parts, p)
			}
		}
		if len(parts) == 0 {
			delete(s.usingShareCursors, topic)
		}
	}

	topicParts := s.tps.load()
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
	s.assignChanged.Broadcast()
}

// routeAcksToSources iterates lastPolled, finalizes records, and routes their
// acks directly to each source's pending ack queue. If autoAcceptPending is
// true, records with no explicit ack status (status==0) are auto-accepted;
// otherwise they are skipped.
//
// Must be called under consumer.mu.
func (s *shareConsumer) routeAcksToSources(autoAcceptPending bool) {
	for _, r := range s.lastPolled {
		st := shareAckFromCtx(r)
		if st == nil {
			continue
		}
		st.mu.Lock()
		if st.finalized {
			st.mu.Unlock()
			continue
		}
		// A RENEW was already routed, awaiting the user's
		// final ack (ACCEPT/REJECT/RELEASE). Skip until the
		// user calls Record.Ack with a terminal status.
		if st.renewSent && st.status == 0 {
			st.mu.Unlock()
			continue
		}
		var ackType int8
		if st.status == 0 {
			if !autoAcceptPending {
				st.mu.Unlock()
				continue
			}
			st.status = AckAccept
			st.finalized = true
			ackType = int8(AckAccept)
		} else if st.status == AckRenew {
			// Route the RENEW ack but don't finalize -- the
			// record must be re-acked with a terminal status
			// (ACCEPT/REJECT/RELEASE) after the lock is extended.
			ackType = int8(AckRenew)
			st.status = 0
			st.renewSent = true
		} else {
			st.finalized = true
			ackType = int8(st.status)
		}
		st.mu.Unlock()

		// Look up the current source for this partition. If the
		// partition was revoked, the cursor is gone and we drop the
		// ack -- the broker already revoked the acquisition.
		parts := s.usingShareCursors[r.Topic]
		if parts == nil {
			continue
		}
		sc := parts[r.Partition]
		if sc == nil {
			continue
		}
		// If the partition leader changed since these records were
		// fetched, the new leader's share session doesn't know
		// about these acquisitions. Drop the ack -- the old
		// leader's acquisition lock will time out and the records
		// will be redelivered. This matches the Java client's
		// isLeaderKnownToHaveChanged check.
		if st.source != nil && st.source != sc.source {
			continue
		}
		// If the session was reset since these records were
		// acquired (e.g. broker restart), the acquisitions are
		// released and these acks are stale. Sending them on the
		// new session causes INVALID_RECORD_STATE.
		if st.sessionGen != sc.source.shareSessionGen.Load() {
			continue
		}

		sc.source.addShareAcks(sc.topicID, r.Partition, []shareAckBatch{{
			firstOffset: r.Offset,
			lastOffset:  r.Offset,
			ackTypes:    []int8{ackType},
		}}, ackType == int8(AckRenew))
	}
}

// routeAndCompactLastPolled routes finalized acks to their sources and
// compacts lastPolled, keeping only records that are still awaiting
// re-ack after a RENEW. All other records are cleared.
//
// Must be called under consumer.mu.
func (s *shareConsumer) routeAndCompactLastPolled(autoAcceptPending bool) {
	s.routeAcksToSources(autoAcceptPending)
	n := 0
	for _, r := range s.lastPolled {
		st := shareAckFromCtx(r)
		if st == nil {
			continue
		}
		st.mu.Lock()
		keep := st.renewSent && !st.finalized
		st.mu.Unlock()
		if keep {
			s.lastPolled[n] = r
			n++
		}
	}
	s.lastPolled = s.lastPolled[:n]
}

// finalizePreviousPoll auto-ACCEPTs any records from the previous poll that
// were not explicitly acknowledged, then routes all finalized acks to their
// respective source's pending ack queues. Records in RENEWING state are
// kept so the user can still call Record.Ack with a terminal status.
//
// Called under consumer.mu from PollFetches/PollRecords.
func (s *shareConsumer) finalizePreviousPoll() {
	if len(s.lastPolled) == 0 {
		return
	}
	s.routeAndCompactLastPolled(true)
}

// markAcks sets the ack status on records that are still pending (status==0
// and not finalized). If records is empty, all lastPolled records are
// considered; otherwise only the specified records are marked.
func (s *shareConsumer) markAcks(status AckStatus, records []*Record) {
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	if len(records) == 0 {
		records = s.lastPolled
	}
	for _, r := range records {
		st := shareAckFromCtx(r)
		if st == nil {
			continue
		}
		st.mu.Lock()
		if st.status == 0 && !st.finalized && !st.renewSent {
			st.status = status
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

// batchesHaveRenew reports whether any ack batch contains an AckRenew type.
func batchesHaveRenew(batches []shareAckBatch) bool {
	for _, b := range batches {
		for _, t := range b.ackTypes {
			if t == int8(AckRenew) {
				return true
			}
		}
	}
	return false
}

// sendAcksFromSources drains pending acks from the given sources and sends
// ShareAcknowledge requests to each broker in parallel. Returns errors
// including per-partition errors from the broker via errors.Join.
func (s *shareConsumer) sendAcksFromSources(ctx context.Context, sources []*source) error {
	type sourceAcks struct {
		src      *source
		acks     sharePendingAcks
		hasRenew bool
	}
	var pending []sourceAcks
	for _, src := range sources {
		acks, hasRenew := src.drainShareAcks()
		if len(acks) > 0 {
			pending = append(pending, sourceAcks{src, acks, hasRenew})
		}
	}
	if len(pending) == 0 {
		return nil
	}

	memberID, _ := s.memberGen.load()
	errs := make(chan error, len(pending))
	for _, sa := range pending {
		go func() {
			errs <- s.sendSourceAck(ctx, sa.src, sa.acks, sa.hasRenew, memberID)
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

// buildShareAckTopics constructs the Topics slice for a ShareAcknowledge
// request from the given ack batches. Used by both sendSourceAck and
// closeShareSessions.
func buildShareAckTopics(acks sharePendingAcks) []kmsg.ShareAcknowledgeRequestTopic {
	var topics []kmsg.ShareAcknowledgeRequestTopic
	topicIdx := make(map[[16]byte]int)
	for tid, parts := range acks {
		idx, ok := topicIdx[tid]
		if !ok {
			idx = len(topics)
			topicIdx[tid] = idx
			t := kmsg.NewShareAcknowledgeRequestTopic()
			t.TopicID = tid
			topics = append(topics, t)
		}
		for partition, batches := range parts {
			p := kmsg.NewShareAcknowledgeRequestTopicPartition()
			p.Partition = partition
			for _, batch := range batches {
				ab := kmsg.NewShareAcknowledgeRequestTopicPartitionAcknowledgementBatche()
				ab.FirstOffset = batch.firstOffset
				ab.LastOffset = batch.lastOffset
				ab.AcknowledgeTypes = batch.ackTypes
				p.AcknowledgementBatches = append(p.AcknowledgementBatches, ab)
			}
			topics[idx].Partitions = append(topics[idx].Partitions, p)
		}
	}
	return topics
}

// sendSourceAck sends a single ShareAcknowledge request to one source's
// broker for the given pending ack batches.
func (s *shareConsumer) sendSourceAck(ctx context.Context, src *source, acks sharePendingAcks, hasRenew bool, memberID string) error {
	req := kmsg.NewPtrShareAcknowledgeRequest()
	req.GroupID = &s.cfg.shareGroup
	req.MemberID = &memberID

	src.cursorsMu.Lock()
	req.ShareSessionEpoch = src.shareSessionEpoch
	src.cursorsMu.Unlock()

	// Cannot send ShareAcknowledge on a new session (epoch 0) --
	// there is no session to acknowledge against. Epoch 0 is only
	// reachable after a session reset (at initial startup, no acks
	// exist because no records have been fetched). Drop the acks
	// rather than holding them: the old session's acquisitions
	// are released, so these acks are stale. The Java client
	// fails them with INVALID_SHARE_SESSION_EPOCH.
	if req.ShareSessionEpoch == 0 {
		s.cfg.logger.Log(LogLevelInfo, "dropping stale acks, share session epoch is 0",
			"broker", src.nodeID,
		)
		return nil
	}

	req.Topics = buildShareAckTopics(acks)
	req.IsRenewAck = hasRenew

	kresp, err := s.cl.retryableBrokerFn(func() (*broker, error) {
		return s.cl.brokerOrErr(ctx, src.nodeID, errUnknownBroker)
	}).Request(ctx, req)
	if err != nil {
		// The broker may have processed the request before the
		// error, advancing its session epoch. Reset ours so the
		// next request starts a fresh session. Drop the acks --
		// the session is gone and the old acquisitions are
		// released. Re-queuing stale acks causes
		// INVALID_RECORD_STATE on the next session.
		src.resetShareSession()
		return err
	}

	var errs []error
	resp := kresp.(*kmsg.ShareAcknowledgeResponse)
	if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
		switch {
		case errors.Is(err, kerr.ShareSessionNotFound),
			errors.Is(err, kerr.InvalidShareSessionEpoch):
			// Session destroyed: the broker released all
			// acquisitions. Drop the acks -- they refer to
			// acquisitions that no longer exist. Reset to 0 so
			// the next ShareFetch creates a fresh session.
			src.resetShareSession()
			s.cfg.logger.Log(LogLevelInfo, "dropped acks due to share session error",
				"broker", src.nodeID,
				"err", err,
			)
			return nil

		case errors.Is(err, kerr.ShareSessionLimitReached):
			// The broker has too many sessions but ours may
			// still be valid. It did NOT process the acks.
			// Unlike NOT_FOUND / INVALID_EPOCH, do NOT reset
			// the session -- the Java client only resets on
			// NOT_FOUND and INVALID_EPOCH for ShareAcknowledge.
			// Defensively bump the epoch (the Java client's
			// handleResponse increments for all non-reset errors)
			// and re-queue the acks for the next attempt.
			src.bumpShareSessionEpoch()
			src.requeueShareAcks(acks, hasRenew)
			s.cfg.logger.Log(LogLevelInfo, "re-queued all acks due to share session limit",
				"broker", src.nodeID,
				"err", err,
			)
			return nil
		default:
			// Non-session errors: the broker may have advanced
			// the epoch before returning the error. Match the
			// Java client's defensive increment.
			src.bumpShareSessionEpoch()
		}
		if isAckRetryable(err) {
			src.requeueShareAcks(acks, hasRenew)
			s.cfg.logger.Log(LogLevelInfo, "re-queued all acks due to retryable top-level error",
				"broker", src.nodeID,
				"err", err,
			)
			return nil
		}
		errs = append(errs, err)
	} else {
		// Success: the broker incremented the session epoch.
		src.bumpShareSessionEpoch()
	}

	id2t := s.cl.id2tMap()
	var ackResults []ShareAckResult
	var moves []shareMove
	for _, rt := range resp.Topics {
		topicName := id2t[rt.TopicID]
		for _, rp := range rt.Partitions {
			if rp.ErrorCode == 0 {
				ackResults = append(ackResults, ShareAckResult{topicName, rp.Partition, nil})
				continue
			}
			err := kerr.ErrorForCode(rp.ErrorCode)
			if isAckRetryable(err) {
				if batches, ok := acks[rt.TopicID][rp.Partition]; ok {
					src.addShareAcks(rt.TopicID, rp.Partition, batches, batchesHaveRenew(batches))
				}
				s.cfg.logger.Log(LogLevelInfo, "re-queued retryable share ack error",
					"topic", topicName,
					"partition", rp.Partition,
					"err", err,
				)
				continue
			}
			if m, ok := tryLeaderMove(rt.TopicID, rp.Partition, err, rp.CurrentLeader.LeaderID, rp.CurrentLeader.LeaderEpoch); ok {
				moves = append(moves, m)
			}
			s.cfg.logger.Log(LogLevelWarn, "share acknowledge partition error",
				"topic", topicName,
				"partition", rp.Partition,
				"err", err,
			)
			ackResults = append(ackResults, ShareAckResult{topicName, rp.Partition, err})
			errs = append(errs, fmt.Errorf("share ack %s[%d]: %w", topicName, rp.Partition, err))
		}
	}
	if len(ackResults) > 0 && s.cfg.shareAckCallback != nil {
		s.cfg.shareAckCallback(s.cl, ackResults)
	}
	if len(moves) > 0 {
		brokers := make([]BrokerMetadata, len(resp.NodeEndpoints))
		for i, ne := range resp.NodeEndpoints {
			brokers[i] = BrokerMetadata{NodeID: ne.NodeID, Host: ne.Host, Port: ne.Port, Rack: ne.Rack}
		}
		s.applyShareMoves(moves, brokers)
	}
	return errors.Join(errs...)
}

// mergeAckBatches sorts ack batches by offset and merges contiguous
// ranges. Adjacent batches with the same uniform type stay compact
// (single-element ackTypes). Mixed-type adjacencies are expanded to
// per-offset arrays and concatenated.
func mergeAckBatches(batches []shareAckBatch) []shareAckBatch {
	if len(batches) <= 1 {
		return batches
	}
	slices.SortFunc(batches, func(a, b shareAckBatch) int {
		return cmp.Compare(a.firstOffset, b.firstOffset)
	})
	merged := batches[:1]
	for _, b := range batches[1:] {
		last := &merged[len(merged)-1]
		if b.firstOffset != last.lastOffset+1 {
			merged = append(merged, b)
			continue
		}
		// Contiguous -- merge. If both are uniform with the
		// same type, keep as single-element.
		if len(last.ackTypes) == 1 && len(b.ackTypes) == 1 && last.ackTypes[0] == b.ackTypes[0] {
			last.lastOffset = b.lastOffset
		} else {
			last.ackTypes = expandAckTypes(last)
			last.ackTypes = append(last.ackTypes, expandAckTypes(&b)...)
			last.lastOffset = b.lastOffset
		}
	}
	return merged
}

// expandAckTypes returns a per-offset slice. If b.ackTypes already has
// per-offset length it is returned as-is; if single-element (uniform)
// it is expanded to cover the full range.
func expandAckTypes(b *shareAckBatch) []int8 {
	n := int(b.lastOffset - b.firstOffset + 1)
	if len(b.ackTypes) == n {
		return b.ackTypes
	}
	expanded := make([]int8, n)
	for i := range expanded {
		expanded[i] = b.ackTypes[0]
	}
	return expanded
}

func (s *shareConsumer) leave(ctx context.Context) {
	s.mu.Lock()
	wasDead := s.dying.Swap(true)
	wasManaging := s.managing
	s.cancel()
	s.mu.Unlock()

	// Wake any pollShareFetches blocked waiting for cursors so it
	// sees s.dying and returns ErrClientClosed.
	s.assignChanged.Broadcast()

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

		// Close share sessions with ShareAcknowledge (epoch -1).
		// Pending acks are included so the broker processes them
		// and closes the session in one round trip.
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

// finalizeForLeave releases records the user saw but did not explicitly
// ack, releases buffered records the user never saw, and routes all
// finalized acks to sources. Records the user explicitly acknowledged
// keep their status.
//
// Releasing (not auto-accepting) user-seen records is safer than the
// next-poll auto-accept behavior: on leave the consumer is going away,
// so unprocessed records should be redelivered rather than silently
// accepted.
//
// This grabs c.mu because it accesses lastPolled and usingShareCursors.
// Without the lock, a concurrent PollFetches that is mid-flight
// (released c.mu for network I/O) could race writing lastPolled while
// we read it here.
func (s *shareConsumer) finalizeForLeave() {
	s.c.mu.Lock()
	defer s.c.mu.Unlock()

	// markPendingAsRelease sets AckRelease on records that are still
	// pending (no explicit ack, not finalized, not awaiting re-ack).
	markPendingAsRelease := func(records []*Record) {
		for _, r := range records {
			st := shareAckFromCtx(r)
			if st == nil {
				continue
			}
			st.mu.Lock()
			if !st.finalized && st.status == 0 && !st.renewSent {
				st.status = AckRelease
			}
			st.mu.Unlock()
		}
	}

	// Phase 1: Records the user saw but did not explicitly ack are
	// released so they become available for other consumers.
	markPendingAsRelease(s.lastPolled)
	s.routeAndCompactLastPolled(false)

	// Phase 2: Release any buffered records that were never returned
	// to the user. These were acquired by the server but never
	// delivered to the application via PollFetches/PollRecords.
	if len(s.bufferedFetches) > 0 {
		s.trackPolledRecords(s.bufferedFetches)
		s.bufferedFetches = nil
		markPendingAsRelease(s.lastPolled)
		s.routeAcksToSources(false)
		s.lastPolled = s.lastPolled[:0]
	}
}

// purgeTopics removes the given topics from the share consumer's
// subscription and share cursor tracking. The next heartbeat will send
// the updated subscription list, and the broker will revoke any
// partitions from the purged topics. Must be called under c.mu.
func (s *shareConsumer) purgeTopics(topics []string) {
	s.cfg.logger.Log(LogLevelInfo, "purging share group topics",
		"group", s.cfg.shareGroup,
		"topics", topics,
	)
	s.tps.purgeTopics(topics)
	purgeSet := make(map[string]struct{}, len(topics))
	for _, t := range topics {
		purgeSet[t] = struct{}{}
		delete(s.reSeen, t)
	}
	// Remove share cursors for purged topics so we stop fetching.
	// The broker will also revoke them when it receives the updated
	// subscription in the next heartbeat.
	for topic, parts := range s.usingShareCursors {
		if _, ok := purgeSet[topic]; !ok {
			continue
		}
		for _, sc := range parts {
			s.revokeShareCursor(sc)
		}
		delete(s.usingShareCursors, topic)
	}
	// Reset sent subscription so the next heartbeat sends the
	// updated list without the purged topics.
	s.lastSentSubscribedTopics = nil
	s.assignChanged.Broadcast()
}

// clearSourceShareState clears stale share state (pending acks, forgotten
// partitions, session epochs) from all sources. Called when the member is
// fenced or unknown -- the broker invalidated our state, so pending acks
// and sessions are stale and should not be sent.
func (s *shareConsumer) clearSourceShareState() {
	s.cl.sinksAndSourcesMu.Lock()
	defer s.cl.sinksAndSourcesMu.Unlock()
	for _, sns := range s.cl.sinksAndSources {
		if sns.source != nil {
			sns.source.clearShareState()
		}
	}
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
	for src := range s.sourcesWithForgotten {
		seen[src] = struct{}{}
	}
	sources := make([]*source, 0, len(seen))
	for src := range seen {
		sources = append(sources, src)
	}
	slices.SortFunc(sources, func(a, b *source) int {
		return cmp.Compare(a.nodeID, b.nodeID)
	})
	return sources
}

// pruneShareSources removes stale entries from sourcesWithForgotten.
// A source is stale if it has active cursors (tracked via
// usingShareCursors) or has drained all its forgotten and pending
// ack state. Must be called under c.mu.
func (s *shareConsumer) pruneShareSources() {
	activeSources := make(map[*source]struct{})
	for _, parts := range s.usingShareCursors {
		for _, sc := range parts {
			activeSources[sc.source] = struct{}{}
		}
	}
	for src := range s.sourcesWithForgotten {
		if _, active := activeSources[src]; active {
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
				s.lastPolled = append(s.lastPolled, t.Partitions[k].Records...)
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
	var excess Fetch
	topicIdx := make(map[string]int)
	for i := range fetches {
		for j := range fetches[i].Topics {
			t := &fetches[i].Topics[j]
			for k := range t.Partitions {
				p := &t.Partitions[k]
				if remaining >= len(p.Records) {
					remaining -= len(p.Records)
					continue
				}
				idx, ok := topicIdx[t.Topic]
				if !ok {
					idx = len(excess.Topics)
					topicIdx[t.Topic] = idx
					excess.Topics = append(excess.Topics, FetchTopic{Topic: t.Topic})
				}
				excess.Topics[idx].Partitions = append(excess.Topics[idx].Partitions, FetchPartition{
					Partition: p.Partition,
					Records:   p.Records[remaining:],
				})
				p.Records = p.Records[:remaining]
				remaining = 0
			}
		}
	}
	if len(excess.Topics) > 0 {
		s.bufferedFetches = append(s.bufferedFetches, excess)
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

	// Block until sources with share cursors are available, an
	// injected error arrives, or the context/client is cancelled.
	// Returns with c.mu still held.
	sources := s.waitForShareSources(ctx)
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

	fetches, allMoves, allBrokers := s.fanOutShareFetches(ctx, sources)

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
	// state from sources -- prune stale sourcesWithForgotten entries.
	s.pruneShareSources()
	s.c.mu.Unlock()

	return fetches
}

// waitForShareSources blocks until at least one source with share
// cursors is available, the context is cancelled, the client is dying,
// or injected errors are waiting. Must be called with c.mu held;
// returns with c.mu still held.
func (s *shareConsumer) waitForShareSources(ctx context.Context) []*source {
	sources := s.collectShareSources()
	if len(sources) > 0 {
		return sources
	}
	// Spin up a goroutine that broadcasts on ctx cancellation so
	// that our Cond.Wait unblocks. We cannot select on a Cond, so
	// this is the standard workaround.
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			s.assignChanged.Broadcast()
		case <-s.ctx.Done():
			s.assignChanged.Broadcast()
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
		s.assignChanged.Wait()
		sources = s.collectShareSources()
	}
	close(done)
	return sources
}

// fanOutShareFetches sends ShareFetch requests to sources in parallel
// and returns the merged results. Up to maxConcurrentFetches sources
// get full fetches; the rest get maintenance-only requests to deliver
// piggybacked acks and forgotten topics promptly.
//
// Partition-level fairness is handled by shareCursorsStart on each
// source (rotated in buildShareFetchRequest), so we always select
// the first N sources here (sorted by nodeID from collectShareSources).
func (s *shareConsumer) fanOutShareFetches(
	ctx context.Context,
	sources []*source,
) (Fetches, []shareMove, []BrokerMetadata) {
	nTotal := len(sources)
	nFetch := nTotal
	if n := s.cfg.maxConcurrentFetches; n > 0 && n < nFetch {
		nFetch = n
	}

	results := make(chan shareFetchResult, nTotal)

	for i, src := range sources {
		maintenanceOnly := i >= nFetch
		go func() {
			results <- s.doShareFetch(ctx, src, maintenanceOnly)
		}()
	}

	var fetches Fetches
	var allMoves []shareMove
	var allBrokers []BrokerMetadata
	for range nTotal {
		r := <-results
		allMoves = append(allMoves, r.moves...)
		allBrokers = append(allBrokers, r.brokers...)
		if r.fetch.hasErrorsOrRecords() {
			fetches = append(fetches, r.fetch)
		}
	}
	return fetches, allMoves, allBrokers
}

// doShareFetch sends a single ShareFetch request to one source and
// returns the decoded Fetch. On session errors (e.g. SHARE_SESSION_NOT_FOUND),
// the epoch is reset and the request is retried once.
//
// If maintenanceOnly is true, the request carries acks, forgotten topics, and
// session state but zeroes out all fetch parameters so the broker returns
// immediately without acquiring new records. This is used for non-selected
// sources when maxConcurrentFetches limits how many sources get full fetches.
func (s *shareConsumer) doShareFetch(ctx context.Context, src *source, maintenanceOnly bool) shareFetchResult {
	result, retry, req := s.doShareFetchAttempt(ctx, src, maintenanceOnly)
	if !retry {
		return result
	}
	// Session error on first attempt -- restore forgotten data for
	// the retry. Piggybacked acks are NOT restored: the broker
	// destroyed the session, releasing all acquisitions those acks
	// referred to. Re-queuing stale acks causes INVALID_RECORD_STATE
	// on the new session.
	src.restoreShareForgotten(req)
	result, _, _ = s.doShareFetchAttempt(ctx, src, maintenanceOnly)
	return result
}

// doShareFetchAttempt performs a single ShareFetch round trip. Returns
// the result, whether the caller should retry (session error), and the
// request (so the caller can restore forgotten data on retry).
func (s *shareConsumer) doShareFetchAttempt(
	ctx context.Context,
	src *source,
	maintenanceOnly bool,
) (shareFetchResult, bool, *kmsg.ShareFetchRequest) {
	req, piggybackedAcks, acksHasRenew := s.buildShareFetchRequest(src, maintenanceOnly)
	if req == nil {
		return shareFetchResult{}, false, nil
	}

	br, err := s.cl.brokerOrErr(ctx, src.nodeID, errUnknownBroker)
	if err != nil {
		src.restoreShareForgotten(req)
		src.requeueShareAcks(piggybackedAcks, acksHasRenew)
		return shareFetchResult{}, false, nil
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
		// As select is pseudo-random when both cases are
		// ready, check for context error to avoid falling
		// through to the retry+backoff path with a dead
		// context.
		if isContextErr(err) && ctx.Err() != nil {
			src.resetShareSession()
			src.restoreShareForgotten(req)
			return shareFetchResult{}, false, nil
		}
	case <-ctx.Done():
		go func() { <-requested }()
		// The session is destroyed -- do not restore acks.
		// The old acquisitions are released and the acks are
		// stale. Re-queuing stale acks causes an infinite
		// INVALID_RECORD_STATE retry loop on the next session.
		src.resetShareSession()
		src.restoreShareForgotten(req)
		return shareFetchResult{}, false, nil
	}

	if err != nil {
		src.consecutiveFailures++
		// The broker may have processed the request before
		// the error occurred, advancing its session epoch.
		// Reset ours so the next request starts a fresh
		// session. Drop piggybacked acks -- the session is
		// gone, so old acquisitions are released and the
		// acks are stale.
		src.resetShareSession()
		src.restoreShareForgotten(req)
		after := time.NewTimer(s.cfg.retryBackoff(src.consecutiveFailures))
		select {
		case <-after.C:
		case <-ctx.Done():
		}
		after.Stop()
		return shareFetchResult{}, false, nil
	}
	src.consecutiveFailures = 0

	resp := kresp.(*kmsg.ShareFetchResponse)

	s.cfg.logger.Log(LogLevelDebug, "share fetch response",
		"broker", src.nodeID,
		"error_code", resp.ErrorCode,
		"num_topics", len(resp.Topics),
	)

	fetch, retry, moves := s.handleShareFetchResponse(src, req, resp, piggybackedAcks, acksHasRenew)
	if !retry {
		var brokers []BrokerMetadata
		if len(moves) > 0 {
			brokers = make([]BrokerMetadata, len(resp.NodeEndpoints))
			for i, ne := range resp.NodeEndpoints {
				brokers[i] = BrokerMetadata{NodeID: ne.NodeID, Host: ne.Host, Port: ne.Port, Rack: ne.Rack}
			}
		}
		return shareFetchResult{fetch, moves, brokers}, false, nil
	}
	return shareFetchResult{}, true, req
}

// buildShareFetchRequest constructs a ShareFetchRequest for the given source.
// Snapshots the source's share state under cursorsMu, then builds the request
// outside the lock. The snapshots are safe because:
//   - shareCursor fields (topic, topicID, partition) are immutable after construction
//   - pendingAcks and forgotten are transferred by ownership (source nils its refs)
//   - sessionEpoch is a plain value copy
//
// The second return value is the ack map that was attached to the request
// (nil if no acks were attached). Callers use this to re-queue acks on
// failure without reconstructing them from the wire format.
//
// If maintenanceOnly is true, all fetch parameters are zeroed so the broker
// responds immediately without acquiring new records. The request still
// carries acks, forgotten topics, and session cursor updates. Returns nil if
// there is nothing to maintain (no new cursors, no acks, no forgotten).
func (s *shareConsumer) buildShareFetchRequest(src *source, maintenanceOnly bool) (*kmsg.ShareFetchRequest, sharePendingAcks, bool) {
	// Snapshot share state under cursorsMu, then release the lock
	// before merging acks (which sorts and allocates). We take
	// ownership of pendingAcks/forgotten (source nils its refs),
	// so merging outside the lock is safe and reduces contention.
	src.cursorsMu.Lock()
	if len(src.shareCursors) == 0 && len(src.sharePendingAcks) == 0 && len(src.shareForgotten) == 0 {
		src.cursorsMu.Unlock()
		return nil, nil, false
	}
	var (
		pendingAcks  = src.sharePendingAcks
		forgotten    = src.shareForgotten
		sessionEpoch = src.shareSessionEpoch
		hasRenew     = src.shareHasRenew
	)
	// On epoch 0 (new session), send all cursors. On epoch > 0
	// (incremental), only send cursors not yet known to the broker's
	// session -- the broker continues fetching from session partitions
	// that are omitted from incremental requests.
	//
	// Iterate from shareCursorsStart so the broker sees partitions in
	// rotated order. When MaxRecords limits the response, different
	// partitions get priority across poll rounds (mirrors
	// source.cursorsStart for regular fetches).
	var cursors []*shareCursor
	nShareCursors := len(src.shareCursors)
	ci := src.shareCursorsStart
	for range src.shareCursors {
		sc := src.shareCursors[ci]
		ci = (ci + 1) % nShareCursors
		if sessionEpoch > 0 {
			if _, inSession := src.shareSessionParts[topicPartIdx{sc.topicID, sc.partition}]; inSession {
				continue
			}
		}
		cursors = append(cursors, sc)
	}
	if nShareCursors > 0 {
		src.shareCursorsStart = (src.shareCursorsStart + 1) % nShareCursors
	}
	src.sharePendingAcks = nil
	src.shareForgotten = nil
	src.shareHasRenew = false
	src.cursorsMu.Unlock()

	// Merge pending ack batches outside the lock. Without merging,
	// piggybacked acks ship many single-offset batches instead of
	// merged contiguous ranges. Same merge that drainShareAcks
	// applies for standalone ShareAcknowledge.
	for _, parts := range pendingAcks {
		for p, batches := range parts {
			parts[p] = mergeAckBatches(batches)
		}
	}

	memberID, _ := s.memberGen.load()

	req := kmsg.NewPtrShareFetchRequest()
	req.GroupID = &s.cfg.shareGroup
	req.MemberID = &memberID
	req.ShareSessionEpoch = sessionEpoch
	req.MaxWaitMillis = s.cfg.maxWait
	req.MinBytes = s.cfg.minBytes
	req.MaxBytes = s.cfg.maxBytes.load()

	maxRecords := s.cfg.shareMaxRecords
	if maxRecords <= 0 {
		maxRecords = 500
	}
	req.MaxRecords = maxRecords
	req.BatchSize = maxRecords
	if s.cfg.shareStrictMaxRecords {
		req.ShareAcquireMode = 1
	}

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
		p.PartitionMaxBytes = s.cfg.maxPartBytes.load()
		partIdx[topicPartIdx{sc.topicID, sc.partition}] = len(req.Topics[tidx].Partitions)
		req.Topics[tidx].Partitions = append(req.Topics[tidx].Partitions, p)
	}

	// Attach piggybacked acks. The broker rejects acks on the initial
	// epoch (epoch 0 creates a new session), so they cannot be sent.
	// Epoch 0 is only reachable after a session reset (at initial
	// startup no acks exist because no records have been fetched),
	// so any pending acks are stale -- drop them. The Java client
	// fails them with INVALID_SHARE_SESSION_EPOCH.
	if len(pendingAcks) > 0 {
		if sessionEpoch == 0 {
			s.cfg.logger.Log(LogLevelInfo, "dropping stale piggybacked acks, share session epoch is 0",
				"broker", src.nodeID,
			)
			pendingAcks = nil
			hasRenew = false
		} else {
			s.attachAcksToShareFetch(req, pendingAcks, topicIdx, partIdx)
			req.IsRenewAck = hasRenew
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

	// Zero all fetch params when the broker should not acquire new
	// records: KIP-1222 requires it for IsRenewAck (the broker
	// validates all are 0), and maintenance-only requests zero them
	// so the broker responds immediately.
	if req.IsRenewAck || maintenanceOnly {
		req.MaxWaitMillis = 0
		req.MinBytes = 0
		req.MaxBytes = 0
		req.MaxRecords = 0
		req.BatchSize = 0
	}
	// If maintenance-only has nothing useful (no topics, no forgotten
	// data), skip the request entirely -- share sessions are
	// connection-bound so there is no idle timeout to worry about.
	if maintenanceOnly && len(req.Topics) == 0 && len(req.ForgottenTopicsData) == 0 {
		return nil, nil, false
	}

	s.cfg.logger.Log(LogLevelDebug, "built share fetch request",
		"broker", src.nodeID,
		"session_epoch", req.ShareSessionEpoch,
		"num_topics", len(req.Topics),
		"num_forgotten", len(req.ForgottenTopicsData),
		"maintenance_only", maintenanceOnly,
	)

	return req, pendingAcks, hasRenew
}

// attachAcksToShareFetch piggybacks pending ack batches onto a ShareFetch
// request. Acks are added to existing topic/partition entries (via the
// partition index) or new ack-only entries are created.
func (s *shareConsumer) attachAcksToShareFetch(
	req *kmsg.ShareFetchRequest,
	pendingAcks sharePendingAcks,
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
				ab.AcknowledgeTypes = batch.ackTypes
				req.Topics[tidx].Partitions[pi].AcknowledgementBatches = append(
					req.Topics[tidx].Partitions[pi].AcknowledgementBatches, ab,
				)
			}
		}
	}
}

// handleShareFetchResponse processes a ShareFetch response, updating session
// epoch and building a Fetch. Returns the Fetch and whether the caller should
// retry (on session errors). Each acquired record has a shareAckState
// injected into its Context for ack tracking.
//
// piggybackedAcks is the ack map returned by buildShareFetchRequest. It is
// used to re-queue acks on retryable errors and detect missing partitions.
func (s *shareConsumer) handleShareFetchResponse(
	src *source,
	req *kmsg.ShareFetchRequest,
	resp *kmsg.ShareFetchResponse,
	piggybackedAcks sharePendingAcks,
	acksHasRenew bool,
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
			src.resetShareSession()
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
			src.bumpShareSessionEpoch()
			src.requeueShareAcks(piggybackedAcks, acksHasRenew)
			return Fetch{Topics: []FetchTopic{{
				Partitions: []FetchPartition{{Partition: -1, Err: err}},
			}}}, false, nil
		}
	}

	// Success -- advance session epoch and update partition tracking.
	src.updateShareSessionParts(req)

	acqLockDeadline := time.Now().Add(time.Duration(resp.AcquisitionLockTimeoutMillis) * time.Millisecond)

	// Build set of partitions that had piggybacked acks. After processing
	// all response partitions, remaining entries are partitions the broker
	// omitted (buggy broker).
	unackedParts := make(map[topicPartIdx]struct{})
	for tid, parts := range piggybackedAcks {
		for p := range parts {
			unackedParts[topicPartIdx{tid, p}] = struct{}{}
		}
	}

	id2t := s.cl.id2tMap()
	var fetch Fetch
	var moves []shareMove
	var ackResults []ShareAckResult
	topicIdx := make(map[string]int)

	for i := range resp.Topics {
		rt := &resp.Topics[i]
		topicName := id2t[rt.TopicID]
		if topicName == "" {
			continue
		}

		for j := range rt.Partitions {
			rp := &rt.Partitions[j]
			tpKey := topicPartIdx{rt.TopicID, rp.Partition}
			_, hadAcks := unackedParts[tpKey]
			delete(unackedParts, tpKey)

			if rp.AcknowledgeErrorCode != 0 {
				ackErr := kerr.ErrorForCode(rp.AcknowledgeErrorCode)
				requeued := false
				if isAckRetryable(ackErr) {
					if batches, ok := piggybackedAcks[rt.TopicID][rp.Partition]; ok {
						src.addShareAcks(rt.TopicID, rp.Partition, batches, batchesHaveRenew(batches))
					}
					requeued = true
				} else {
					ackResults = append(ackResults, ShareAckResult{topicName, rp.Partition, ackErr})
				}
				s.cfg.logger.Log(LogLevelWarn, "share fetch acknowledge error",
					"topic", topicName,
					"partition", rp.Partition,
					"err", ackErr,
					"requeued", requeued,
				)
			} else if hadAcks {
				ackResults = append(ackResults, ShareAckResult{topicName, rp.Partition, nil})
			}

			if rp.ErrorCode != 0 {
				partErr := kerr.ErrorForCode(rp.ErrorCode)
				if m, ok := tryLeaderMove(rt.TopicID, rp.Partition, partErr, rp.CurrentLeader.LeaderID, rp.CurrentLeader.LeaderEpoch); ok {
					moves = append(moves, m)
					delete(unackedParts, tpKey)
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
					Err:       partErr,
				})
				continue
			}

			if len(rp.Records) == 0 && len(rp.AcquiredRecords) == 0 {
				continue
			}

			fp, gapAcks := s.decodeSharePartition(src, topicName, rp, acqLockDeadline)
			if len(gapAcks) > 0 {
				src.addShareAcks(rt.TopicID, rp.Partition, gapAcks, false) // gap acks are never renew
			}
			if len(fp.Records) == 0 && fp.Err == nil {
				continue
			}

			idx, ok := topicIdx[topicName]
			if !ok {
				idx = len(fetch.Topics)
				topicIdx[topicName] = idx
				fetch.Topics = append(fetch.Topics, FetchTopic{Topic: topicName})
			}
			fetch.Topics[idx].Partitions = append(fetch.Topics[idx].Partitions, fp)
		}
	}

	// Drop piggybacked acks for partitions the broker omitted from the
	// response. The broker should always include a partition-level result
	// for every partition in the request; if it doesn't, the broker is
	// buggy. We drop rather than re-queue to avoid carrying stale acks
	// indefinitely in a retry loop.
	for key := range unackedParts {
		topicName := id2t[key.topicID]
		s.cfg.logger.Log(LogLevelWarn, "dropping piggybacked acks for partition missing from share fetch response",
			"broker", src.nodeID,
			"topic", topicName,
			"partition", key.partition,
		)
		ackResults = append(ackResults, ShareAckResult{topicName, key.partition, errAckMissingFromResponse})
	}

	if len(ackResults) > 0 && s.cfg.shareAckCallback != nil {
		s.cfg.shareAckCallback(s.cl, ackResults)
	}

	return fetch, false, moves
}

// decodeSharePartition decodes record batches from a ShareFetch response
// partition, filters to acquired records, injects shareAckState into each
// record's Context, and generates gap acks for compacted offsets.
func (s *shareConsumer) decodeSharePartition(
	src *source,
	topicName string,
	rp *kmsg.ShareFetchResponseTopicPartition,
	acqLockDeadline time.Time,
) (FetchPartition, []shareAckBatch) {
	sessionGen := src.shareSessionGen.Load()

	// Build a synthetic FetchResponseTopicPartition because ShareFetch
	// uses the same wire format for records. The sentinel -1 values
	// signal "not applicable" for fields that only exist in regular
	// fetch responses (HWM, LSO, LogStartOffset).
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

	// Filter to only records within AcquiredRecords ranges and inject
	// shareAckState into each. Both fp.Records and AcquiredRecords are
	// sorted by offset, so we use a two-pointer scan for O(n+m).
	//
	// Gap handling: offsets in an acquired range that have no physical
	// record are immediately acked so the broker releases them.
	// - Normal gaps (compaction): type 0 (gap), broker releases without
	//   counting toward delivery attempts.
	// - CRC/decode errors (fp.Err != nil): type 3 (reject), broker
	//   archives the records. Corruption is persistent so redelivery
	//   is pointless.
	gapType := int8(0)
	if fp.Err != nil {
		gapType = int8(AckReject)
	}
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
					ackTypes:    []int8{gapType},
				})
			}
			ackState := &shareAckState{
				deliveryCount:           int32(ar.DeliveryCount),
				acquisitionLockDeadline: acqLockDeadline,
				source:                  src,
				sessionGen:              sessionGen,
			}
			ctx := r.Context
			if ctx == nil {
				ctx = context.Background()
			}
			r.Context = context.WithValue(ctx, shareAckKey, ackState)
			acquired = append(acquired, r)
			nextExpected = r.Offset + 1
			ri++
		}
		if nextExpected <= ar.LastOffset {
			gapAcks = append(gapAcks, shareAckBatch{
				firstOffset: nextExpected,
				lastOffset:  ar.LastOffset,
				ackTypes:    []int8{gapType},
			})
		}
	}

	fp.Records = acquired
	return fp, gapAcks
}

// closeShareSessions sends ShareAcknowledge with ShareSessionEpoch=-1 to
// each source that has an active share session or pending acks, telling
// the broker to close the session. Pending acks are included in the close
// request so the broker processes them in one round trip. Without the
// close, the broker keeps the session alive and the group may remain
// NON_EMPTY after the leave heartbeat.
func (s *shareConsumer) closeShareSessions(ctx context.Context) {
	memberID, _ := s.memberGen.load()

	type closeReq struct {
		src *source
		req *kmsg.ShareAcknowledgeRequest
	}
	var reqs []closeReq
	s.cl.sinksAndSourcesMu.Lock()
	allSources := make([]*source, 0, len(s.cl.sinksAndSources))
	for _, sns := range s.cl.sinksAndSources {
		if sns.source != nil {
			allSources = append(allSources, sns.source)
		}
	}
	s.cl.sinksAndSourcesMu.Unlock()
	for _, src := range allSources {
		acks, hasRenew := src.drainShareAcks()

		src.cursorsMu.Lock()
		epoch := src.shareSessionEpoch
		hasSession := epoch > 0 || len(src.shareCursors) > 0
		src.cursorsMu.Unlock()

		if !hasSession && len(acks) == 0 {
			continue
		}

		req := kmsg.NewPtrShareAcknowledgeRequest()
		req.GroupID = &s.cfg.shareGroup
		req.MemberID = &memberID
		req.ShareSessionEpoch = -1 // close session

		if len(acks) > 0 {
			req.Topics = buildShareAckTopics(acks)
			req.IsRenewAck = hasRenew
		}
		reqs = append(reqs, closeReq{src, req})
	}
	if len(reqs) == 0 {
		return
	}

	var wg sync.WaitGroup
	wg.Add(len(reqs))
	for _, cr := range reqs {
		go func() {
			defer wg.Done()
			br, err := s.cl.brokerOrErr(ctx, cr.src.nodeID, errUnknownBroker)
			if err != nil {
				return
			}
			kresp, err := br.waitResp(ctx, cr.req)
			if err != nil {
				s.cfg.logger.Log(LogLevelDebug, "failed to close share session",
					"broker", cr.src.nodeID, "err", err,
				)
				return
			}
			resp := kresp.(*kmsg.ShareAcknowledgeResponse)
			if resp.ErrorCode != 0 {
				s.cfg.logger.Log(LogLevelDebug, "share session close returned error",
					"broker", cr.src.nodeID, "err", kerr.ErrorForCode(resp.ErrorCode),
				)
			}
			cr.src.resetShareSession()
		}()
	}
	wg.Wait()
}
