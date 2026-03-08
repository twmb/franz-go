package kgo

import (
	"context"
	"errors"
	"math"
	"slices"
	"sort"
	"sync"
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
	dying    bool
	left     chan struct{}
	leaveErr error

	usingShareCursors map[string]map[int32]*shareCursor

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

		manageDone:        make(chan struct{}),
		tps:               newTopicsPartitions(),
		left:              make(chan struct{}),
		usingShareCursors: make(map[string]map[int32]*shareCursor),
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

// findNewAssignments is called on metadata update. It starts the manage
// goroutine the first time it detects topics to consume.
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

	if s.dying {
		return
	}

	if !s.managing {
		s.managing = true
		go s.manageShareGroup()
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
			s.nowAssigned.store(nil)
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
			s.nowAssigned.store(nil)
			s.unresolvedAssigned = nil
			consecutiveErrors = 0
			continue

		case isRetryableBrokerErr(err),
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

	// If all topics are unresolved, wait for metadata rather than
	// treating it as "revoke everything".
	if len(newAssigned) == 0 && len(s.unresolvedAssigned) > 0 {
		return nil
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

	// Remove stale shareCursors from sources.
	for topic, parts := range s.usingShareCursors {
		newParts, ok := assignments[topic]
		if !ok {
			for _, sc := range parts {
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

// finalizePreviousPoll auto-ACCEPTs any records from the previous poll that
// were not explicitly acknowledged, then routes all finalized acks to their
// respective source's pending ack queues.
//
// Called under consumer.mu from PollFetches/PollRecords.
func (s *shareConsumer) finalizePreviousPoll() {
	if len(s.lastPolled) == 0 {
		return
	}

	type partKey struct {
		topic     string
		partition int32
	}
	type sourceAckMap = map[partKey][]shareAckBatch
	bySource := make(map[*source]sourceAckMap)

	for i := range s.lastPolled {
		pr := &s.lastPolled[i]
		st := pr.state
		st.mu.Lock()
		if st.status&ackFinalized != 0 {
			st.mu.Unlock()
			continue
		}
		if st.status == 0 {
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
			m = make(sourceAckMap)
			bySource[sc.source] = m
		}
		key := partKey{pr.topic, pr.partition}
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
			src.addShareAcks(key.topic, key.partition, batches)
		}
	}

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
	s.c.mu.Lock()
	s.finalizeMarkedRecords()
	s.c.mu.Unlock()

	// Send ShareAcknowledge to each source that has pending acks.
	return s.sendPendingAcknowledgementsCtx(ctx)
}

// finalizeMarkedRecords iterates lastPolled and finalizes any records
// that have been explicitly marked (non-zero status, not yet finalized),
// routing their acks to the appropriate source. Unlike finalizePreviousPoll,
// this does NOT auto-ACCEPT unmarked records and does NOT clear lastPolled.
//
// Must be called under consumer.mu.
func (s *shareConsumer) finalizeMarkedRecords() {
	type partKey struct {
		topic     string
		partition int32
	}
	type sourceAckMap = map[partKey][]shareAckBatch
	bySource := make(map[*source]sourceAckMap)

	for i := range s.lastPolled {
		pr := &s.lastPolled[i]
		st := pr.state
		st.mu.Lock()
		if st.status&ackFinalized != 0 || st.status == 0 {
			st.mu.Unlock()
			continue
		}
		st.status |= ackFinalized
		ackType := int8(st.status &^ ackFinalized)
		st.mu.Unlock()

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
			m = make(sourceAckMap)
			bySource[sc.source] = m
		}
		key := partKey{pr.topic, pr.partition}
		m[key] = append(m[key], shareAckBatch{
			firstOffset: pr.offset,
			lastOffset:  pr.offset,
			ackType:     ackType,
		})
	}

	for src, partBatches := range bySource {
		for key, batches := range partBatches {
			batches = mergeAckBatches(batches)
			src.addShareAcks(key.topic, key.partition, batches)
		}
	}
}

// sendPendingAcknowledgementsCtx sends ShareAcknowledge for all pending acks
// across all sources that have them. Returns the first error encountered.
func (s *shareConsumer) sendPendingAcknowledgementsCtx(ctx context.Context) error {
	memberID, _ := s.memberGen.load()
	var firstErr error

	for _, src := range s.collectAllSources() {
		acks := src.drainShareAcks()
		if len(acks) == 0 {
			continue
		}

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

		br, err := s.cl.brokerOrErr(nil, src.nodeID, errUnknownBroker)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		kresp, err := br.waitResp(ctx, req)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		resp := kresp.(*kmsg.ShareAcknowledgeResponse)
		if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return firstErr
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
	wasDead := s.dying
	s.dying = true
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
func (s *shareConsumer) finalizeForLeave() {
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

// collectAllSources returns all sources known to the client, keyed by nodeID.
func (s *shareConsumer) collectAllSources() map[int32]*source {
	s.cl.sinksAndSourcesMu.Lock()
	defer s.cl.sinksAndSourcesMu.Unlock()
	sources := make(map[int32]*source, len(s.cl.sinksAndSources))
	for nodeID, sns := range s.cl.sinksAndSources {
		if sns.source != nil {
			sources[nodeID] = sns.source
		}
	}
	return sources
}

// buildAckTopicPartitions converts pending ack batches (topic -> partition ->
// batches) into kmsg request-level topic/partition structures. The fn callback
// receives (topicID, partition, ackBatches) and should append to the caller's
// request. Returns true if any batch has AckRenew type.
func (s *shareConsumer) buildAckRequests(
	acks map[string]map[int32][]shareAckBatch,
	fn func(tid [16]byte, partition int32, batches []shareAckBatch),
) bool {
	t2id := s.cl.t2idMap()
	hasRenew := false
	for topic, parts := range acks {
		tid := t2id[topic]
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

// collectShareSources returns the set of sources that have share cursors.
// Must be called under c.mu.
func (s *shareConsumer) collectShareSources() map[*source]struct{} {
	sources := make(map[*source]struct{})
	for _, parts := range s.usingShareCursors {
		for _, sc := range parts {
			sources[sc.source] = struct{}{}
		}
	}
	return sources
}

// pollShareFetches sends ShareFetch requests to all sources with share
// cursors in parallel and returns the merged results. This is the pull
// model: fetches happen synchronously within PollFetches so records are
// only acquired when the user is ready to process them.
//
// Called from PollRecords with consumer.mu held for finalizePreviousPoll,
// then released for the network calls.
func (s *shareConsumer) pollShareFetches(ctx context.Context) Fetches {
	if s.dying {
		return NewErrFetch(ErrClientClosed)
	}

	// Collect unique sources that have share cursors. If none exist
	// yet, block until the heartbeat loop assigns partitions or the
	// context is cancelled.
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
		for len(sources) == 0 && !s.dying && ctx.Err() == nil {
			s.cursorsChanged.Wait()
			sources = s.collectShareSources()
		}
		close(done)
	}
	s.c.mu.Unlock()

	if len(sources) == 0 {
		if s.dying {
			return NewErrFetch(ErrClientClosed)
		}
		return NewErrFetch(ctx.Err())
	}

	// Determine how many fetch rounds to issue. Each round sends one
	// ShareFetch per source in parallel. MaxConcurrentFetches controls
	// the total number of requests: with N sources, we do up to
	// ceil(maxConcurrentFetches / N) rounds. 0 means one round
	// (unbounded is interpreted as "fetch once from every broker").
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

	// Track records from this poll for auto-ACCEPT on the next poll.
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

	return fetches
}

// doShareFetch sends a single ShareFetch request to one source and
// returns the decoded Fetch. On session errors, it retries with a
// reset epoch.
func (s *shareConsumer) doShareFetch(ctx context.Context, src *source) Fetch {
	req := s.buildShareFetchRequest(src)
	if req == nil {
		return Fetch{}
	}

	br, err := s.cl.brokerOrErr(nil, src.nodeID, errUnknownBroker)
	if err != nil {
		return Fetch{}
	}

	kresp, err := br.waitResp(ctx, req)
	if err != nil {
		src.consecutiveFailures++
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
	if retry {
		// Session error (e.g. SHARE_SESSION_NOT_FOUND) -- epoch was
		// reset by handleShareFetchResponse. Retry once with the new
		// epoch to re-establish the session.
		return s.doShareFetch(ctx, src)
	}

	return fetch
}

// buildShareFetchRequest constructs a ShareFetchRequest for the given source.
// Grabs src.cursorsMu internally.
func (s *shareConsumer) buildShareFetchRequest(src *source) *kmsg.ShareFetchRequest {
	src.cursorsMu.Lock()
	if len(src.shareCursors) == 0 && len(src.sharePendingAcks) == 0 {
		src.cursorsMu.Unlock()
		return nil
	}

	memberID, _ := s.memberGen.load()

	req := kmsg.NewPtrShareFetchRequest()
	req.GroupID = &s.cfg.shareGroup
	req.MemberID = &memberID
	req.ShareSessionEpoch = src.shareSessionEpoch
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
	for _, sc := range src.shareCursors {
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
	if len(src.sharePendingAcks) > 0 {
		s.attachAcksToShareFetch(req, src.sharePendingAcks, topicIdx)
		src.sharePendingAcks = nil
	}

	// Attach forgotten topics.
	for tid, parts := range src.shareForgotten {
		f := kmsg.NewShareFetchRequestForgottenTopicsData()
		f.TopicID = tid
		f.Partitions = parts
		req.ForgottenTopicsData = append(req.ForgottenTopicsData, f)
	}
	src.shareForgotten = nil

	s.cfg.logger.Log(LogLevelDebug, "built share fetch request",
		"broker", src.nodeID,
		"session_epoch", req.ShareSessionEpoch,
		"num_topics", len(req.Topics),
		"num_forgotten", len(req.ForgottenTopicsData),
	)

	src.cursorsMu.Unlock()
	return req
}

// attachAcksToShareFetch piggybacks pending ack batches onto a ShareFetch
// request. Acks are added to existing topic/partition entries or new
// ack-only entries are created.
func (s *shareConsumer) attachAcksToShareFetch(
	req *kmsg.ShareFetchRequest,
	pendingAcks map[string]map[int32][]shareAckBatch,
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
		default:
			s.cfg.logger.Log(LogLevelError, "share fetch top-level error",
				"broker", src.nodeID,
				"err", err,
			)
			return Fetch{Topics: []FetchTopic{{
				Partitions: []FetchPartition{{Err: err}},
			}}}, false
		}
	}

	// Success -- advance session epoch.
	src.cursorsMu.Lock()
	if src.shareSessionEpoch == math.MaxInt32 {
		src.shareSessionEpoch = 1
	} else {
		src.shareSessionEpoch++
	}
	src.cursorsMu.Unlock()

	id2t := s.cl.id2tMap()
	var fetch Fetch

	for i := range resp.Topics {
		rt := &resp.Topics[i]
		topicName := id2t[rt.TopicID]
		if topicName == "" {
			continue
		}

		var ft *FetchTopic
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
				if ft == nil {
					ft = s.getOrAddFetchTopic(&fetch, topicName)
				}
				ft.Partitions = append(ft.Partitions, FetchPartition{
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

			// Filter to only include records within AcquiredRecords
			// ranges, inject shareAckState into each, and build
			// polled record tracking in the same pass.
			var acquired []*Record
			for _, ar := range rp.AcquiredRecords {
				for k := range fp.Records {
					r := fp.Records[k]
					if r.Offset >= ar.FirstOffset && r.Offset <= ar.LastOffset {
						ackState := &shareAckState{
							deliveryCount: int32(ar.DeliveryCount),
						}
						ctx := r.Context
						if ctx == nil {
							ctx = context.Background()
						}
						r.Context = context.WithValue(ctx, ctxShareAck, ackState)
						acquired = append(acquired, r)
					}
				}
			}

			if len(acquired) == 0 && fp.Err == nil {
				continue
			}

			if ft == nil {
				ft = s.getOrAddFetchTopic(&fetch, topicName)
			}
			fp.Records = acquired
			ft.Partitions = append(ft.Partitions, fp)
		}
	}

	return fetch, false
}

func (s *shareConsumer) getOrAddFetchTopic(f *Fetch, topic string) *FetchTopic {
	for i := range f.Topics {
		if f.Topics[i].Topic == topic {
			return &f.Topics[i]
		}
	}
	f.Topics = append(f.Topics, FetchTopic{Topic: topic})
	return &f.Topics[len(f.Topics)-1]
}

// sendPendingAcknowledgements sends standalone ShareAcknowledge requests
// for all pending acks across all sources. Used on the close/leave path.
func (s *shareConsumer) sendPendingAcknowledgements(ctx context.Context) {
	if err := s.sendPendingAcknowledgementsCtx(ctx); err != nil {
		s.cfg.logger.Log(LogLevelWarn, "ShareAcknowledge on leave failed", "err", err)
	}
}

// closeShareSessions sends ShareFetch with ShareSessionEpoch=-1 to each
// source that has an active share session, telling the broker to close the
// session. Without this, the broker keeps the session alive and the group
// may remain NON_EMPTY after the leave heartbeat.
func (s *shareConsumer) closeShareSessions(ctx context.Context) {
	memberID, _ := s.memberGen.load()

	for _, src := range s.collectAllSources() {
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

		br, err := s.cl.brokerOrErr(nil, src.nodeID, errUnknownBroker)
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
