package kgo

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"slices"
	"sync"
	"unsafe"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

var errAckMissingFromResponse = errors.New("broker omitted partition from share fetch response")

// defShareMaxRecords is the default MaxRecords/BatchSize for ShareFetch
// requests.
const defShareMaxRecords = 500

// topicPartIdx is a map key for share session partition tracking.
type topicPartIdx struct {
	topicID   [16]byte
	partition int32
}

// AckStatus defines how the broker should handle an acquired share group
// record: accept, release for redelivery, reject, or renew the lock.
type AckStatus int8

const (
	// AckAccept marks a record as successfully processed. The broker
	// advances the share group's cursor past this record.
	AckAccept AckStatus = 1

	// AckRelease releases a record back to the broker for redelivery
	// to another consumer. The delivery count is incremented.
	AckRelease AckStatus = 2

	// AckReject marks a record as permanently unprocessable. The broker
	// archives the record and does not redeliver it.
	AckReject AckStatus = 3

	// AckRenew extends the acquisition lock on a record without
	// completing it (KIP-1222). The consumer must subsequently call
	// Ack with a terminal status (Accept, Release, or Reject).
	AckRenew AckStatus = 4
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
// State machine (all transitions under mu):
//
//	PENDING  -->  user calls Ack  -->  MARKED  -->  finalizeAndRouteAcks  -->  FINALIZED
//	                                     |
//	                              (if AckRenew)
//	                                     |
//	                                     v
//	                                  RENEWING  -->  user calls Ack(terminal)  -->  MARKED  -->  FINALIZED
//
// States:
//
//	PENDING   : status=0, finalized=false, renewSent=false  -- awaiting user ack
//	MARKED    : status=1..4, finalized=false                -- user has called Ack
//	RENEWING  : status=0, finalized=false, renewSent=true   -- renew sent, awaiting terminal ack
//	FINALIZED : finalized=true                              -- terminal, ack committed for sending
//
// On next poll, any PENDING record becomes FINALIZED with AckAccept.
// RENEWING records are skipped (the user must provide a terminal ack).
type shareAckState struct {
	mu        sync.Mutex
	status    AckStatus // 0=pending, 1-4=set by user
	finalized bool      // true once the ack has been committed for sending
	// renewSent and finalized are kept as separate bools rather
	// than merged into a bitfield. They occupy the same alignment
	// padding (after sync.Mutex + status, before deliveryCount's
	// 4-byte boundary), so merging would not reduce struct size.
	renewSent               bool // true after a RENEW has been routed, awaiting re-ack
	deliveryCount           int32
	acquisitionLockDeadline time.Time
	source                  *source
	sessionGen              uint64 // source.shareSessionGen at acquire time; stale if mismatched
}

var shareAckKey = strp("share-ack")

// shareAckSlab holds slab-allocated shareAckState objects for all
// records from one batch within a ShareFetch partition decode. One
// slab per batch is stored in the records' context via
// batchContext. Records within a batch are contiguous in memory
// (from a single []Record allocation), so pointer arithmetic from
// records0 gives the correct slab index.
type shareAckSlab struct {
	states   []shareAckState
	records0 *Record // first record in the batch's backing array
}

func shareAckFromCtx(r *Record) *shareAckState {
	if r.Context == nil {
		return nil
	}
	v := r.Context.Value(shareAckKey)
	if v == nil {
		return nil
	}
	slab := v.(*shareAckSlab)
	idx := int((uintptr(unsafe.Pointer(r)) - uintptr(unsafe.Pointer(slab.records0))) / recSize) //nolint:gosec
	if idx < 0 || idx >= len(slab.states) {
		return nil
	}
	st := &slab.states[idx]
	if st.source == nil {
		return nil
	}
	return st
}

type fetchResult struct {
	r shareFetchResult
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
	for tid, otherParts := range other {
		myParts := a[tid]
		if myParts == nil {
			// Transfer ownership of the inner map rather than
			// copying entry-by-entry. The caller does not use
			// other after merge, so this is safe and avoids an
			// inner map allocation per topic.
			a[tid] = otherParts
			continue
		}
		for p, batches := range otherParts {
			myParts[p] = append(myParts[p], batches...)
		}
	}
}

// mergeAll sorts and coalesces contiguous ack batches for every
// partition. Called after draining pending acks from a source.
func (a sharePendingAcks) mergeAll() {
	for _, parts := range a {
		for p, batches := range parts {
			parts[p] = mergeAckBatches(batches)
		}
	}
}

type shareMove struct {
	topicID   [16]byte
	partition int32
	leaderID  int32
}

// tryLeaderMove checks whether err indicates a leader change and the
// response includes a valid CurrentLeader hint. Returns true with a
// move target if the error is a leader change and the hint is usable,
// false otherwise (non-leader error, or the hint has invalid IDs).
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

type (
	shareFetchResult struct {
		fetch   Fetch
		moves   []shareMove
		brokers []BrokerMetadata // from resp.NodeEndpoints, for ensuring move targets
	}

	shareCursor struct {
		topic      string
		topicID    [16]byte
		partition  int32
		source     *source
		cursorsIdx int // index in source.shareCursors, under source.cursorsMu
	}

	// ShareAckResult is a per-partition result from a share group acknowledge;
	// Err is nil on success.
	ShareAckResult struct {
		Topic     string
		Partition int32
		Err       error
	}
)

// ShareAckResults is a slice of per-partition ack results.
type ShareAckResults []ShareAckResult

// Error returns the first non-nil error in the results, or nil.
func (rs ShareAckResults) Error() error {
	for _, r := range rs {
		if r.Err != nil {
			return r.Err
		}
	}
	return nil
}

// Ok returns true if all results succeeded.
func (rs ShareAckResults) Ok() bool {
	return rs.Error() == nil
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
	dying    atomic.Bool // also read atomically outside mu from poll
	left     chan struct{}
	leaveErr error

	////////////////
	// c.mu block //
	////////////////

	usingShareCursors map[string]map[int32]*shareCursor // topic -> partition -> shareCursor
	assignChanged     *sync.Cond                        // signaled when share cursors change

	// sourcesWithForgotten tracks sources that had share cursors
	// removed via revokeCursor. The forgotten set is computed
	// at snapshot time by diffing shareSessionParts against live
	// cursors (see snapshotShareState), so there is no separate
	// shareForgotten field.
	//
	// This consumer-level set exists because after a cursor is
	// removed, the source may have zero remaining shareCursors.
	// collectSources only iterates usingShareCursors to find
	// active sources, so it would miss the cursor-less source
	// that still has shareSessionParts entries needing a final
	// ShareFetch with ForgottenTopicsData. sourcesWithForgotten
	// ensures those sources are included. Once the source's
	// shareSessionParts and pending acks are drained,
	// pruneSourcesWithForgotten removes the entry.
	sourcesWithForgotten map[*source]struct{}

	lastPolled      []*Record // records from previous poll for auto-ACCEPT
	bufferedFetches Fetches   // excess records from prior ShareFetch, served before new fetches

	// sourceFetchStart rotates which sources get full fetch
	// priority in fanOutFetches, preventing maxConcurrentFetches
	// from always favoring the same brokers. Accessed only from
	// the single-threaded poll path (no lock needed).
	sourceFetchStart int

	// fetchSlots is a semaphore channel limiting in-flight full
	// fetches to maxConcurrentFetches. Each full-fetch goroutine
	// acquires a slot before starting and releases it on
	// completion. Persists across poll rounds so goroutines from
	// a previous poll that are still blocked on MaxWait hold
	// their slots until they finish.
	fetchSlots chan struct{}

	// sourceMu serializes doFetch calls per source across poll
	// rounds. Keyed by source pointer, created lazily. Prevents
	// two poll rounds from issuing concurrent requests to the
	// same broker (which would race on the share session epoch).
	sourceMu map[*source]chan struct{}

	// fetchResults is a persistent channel that all share fetch
	// goroutines send to, across all poll rounds. fanOutFetches
	// reads from it. Large buffer to prevent goroutines from
	// blocking on send.
	fetchResults chan fetchResult

	// fetchWg tracks ALL in-flight share fetch goroutines (full
	// + maintenance) for clean shutdown.
	fetchWg sync.WaitGroup
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
	// fetchSlots limits concurrent full-fetch requests. 0 means
	// unlimited (buffer = large). The semaphore persists across
	// poll rounds so goroutines blocked on MaxWait hold their slot.
	slotCount := s.cfg.maxConcurrentFetches
	if slotCount <= 0 {
		slotCount = 1000 // effectively unlimited
	}
	s.fetchSlots = make(chan struct{}, slotCount)
	s.sourceMu = make(map[*source]chan struct{})
	s.fetchResults = make(chan fetchResult, 100)
	c.s = s

	if len(s.cfg.topics) > 0 && !s.cfg.regex {
		s.tps.storeTopics(slices.Collect(maps.Keys(s.cfg.topics)))
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

	s.refreshCursorSources()
}

// revokeCursor removes a share cursor from its current source and
// marks the source as having forgotten partitions. Must be called under c.mu.
func (s *shareConsumer) revokeCursor(sc *shareCursor) {
	s.sourcesWithForgotten[sc.source] = struct{}{}
	sc.source.removeShareCursor(sc)
}

// moveCursorToLeader moves a share cursor to the source for the given
// leader broker, marking the old source as having forgotten partitions.
// Returns true if a source exists for the leader and the cursor was moved.
//
// Must be called under c.mu.
func (s *shareConsumer) moveCursorToLeader(sc *shareCursor, leaderID int32) bool {
	s.cl.sinksAndSourcesMu.Lock()
	sns := s.cl.sinksAndSources[leaderID]
	s.cl.sinksAndSourcesMu.Unlock()
	if sns.source == nil {
		return false
	}
	s.revokeCursor(sc)
	sc.source = sns.source
	sns.source.addShareCursor(sc)
	return true
}

// refreshCursorSources migrates share cursors to new sources when
// partition leaders change. Called from findNewAssignments under both
// c.mu and s.mu (the metadata update path holds c.mu, findNewAssignments
// holds s.mu).
func (s *shareConsumer) refreshCursorSources() {
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
			if s.moveCursorToLeader(sc, newLeader) {
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

// applyMoves proactively moves share cursors to new leaders detected
// via CurrentLeader in ShareFetch responses, avoiding the delay of a full
// metadata refresh. Follows the same broker/source setup pattern as
// kip951move in topics_and_partitions.go.
func (s *shareConsumer) applyMoves(moves []shareMove, brokers []BrokerMetadata) {
	s.cl.ensureBrokers(brokers)

	leaders := make([]int32, 0, len(moves))
	for _, m := range moves {
		leaders = append(leaders, m.leaderID)
	}
	s.cl.ensureSinksAndSources(leaders)

	// Move cursors under c.mu.
	id2t := s.cl.id2tMap()
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
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
		if s.moveCursorToLeader(sc, m.leaderID) {
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
			s.assignPartitions(nil)
			clear(s.sourcesWithForgotten)
			s.c.mu.Unlock()
			s.clearSourceState()
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
	subscribedTopics := slices.Sorted(maps.Keys(tps))
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

	newAssigned := s.handleHeartbeatResp(resp)
	if newAssigned != nil {
		s.c.mu.Lock()
		s.assignPartitions(newAssigned)
		s.c.mu.Unlock()
	}
	return sleep, nil
}

func (s *shareConsumer) handleHeartbeatResp(resp *kmsg.ShareGroupHeartbeatResponse) map[string][]int32 {
	// The member ID is client-generated and immutable; only
	// update the epoch from the response.
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
		// SORTING CONTRACT: partition slices must be sorted
		// so that assignPartitions can use binary search
		// to detect removed partitions. resolveUnresolvedTopicIDs
		// maintains the same invariant for its resolved slices.
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
		// SORTING CONTRACT: partition slices must be sorted
		// so that assignPartitions can use binary search
		// to detect removed partitions. handleHeartbeatResp
		// maintains the same invariant for directly resolved
		// partitions.
		slices.Sort(ps)
		dst[name] = ps
		delete(s.unresolvedAssigned, id)
	}
	return dst
}

func (s *shareConsumer) assignPartitions(assignments map[string][]int32) {
	s.cfg.logger.Log(LogLevelInfo, "assigning share partitions",
		"group", s.cfg.shareGroup,
		"assignments", assignments,
	)

	// Share consumers do not use the normal consumer session for
	// offset loading or cursor management -- share cursors are
	// managed directly via usingShareCursors and the pull-based
	// poll. The consumer session exists solely to
	// gate PollFetches/PollRecords: the polling path checks for
	// a non-nil session before proceeding. We stop and
	// immediately restart a session here to reset that gate.
	// The returned loads and cursors are unused because share
	// consumers have no list-offset or epoch-loading work.
	_, _ = s.c.stopSession()

	// Remove stale shareCursors from sources. Track which sources
	// lost cursors so we can send ForgottenTopicsData even if a
	// source ends up with no remaining share cursors.
	for topic, parts := range s.usingShareCursors {
		newParts, ok := assignments[topic]
		if !ok {
			for _, sc := range parts {
				s.revokeCursor(sc)
			}
			delete(s.usingShareCursors, topic)
			continue
		}
		// newParts is sorted (by handleHeartbeatResp /
		// resolveUnresolvedTopicIDs), so we use binary search.
		for p, sc := range parts {
			if _, found := slices.BinarySearch(newParts, p); !found {
				s.revokeCursor(sc)
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

	// Wake any poll blocked waiting for cursors.
	s.assignChanged.Broadcast()
}

// finalizeAndRouteAcks iterates lastPolled, marks each record's ack state
// as finalized, and routes the resulting ack batches to each source's
// pending queue. If autoAcceptPending is true, records with no explicit
// ack status (status==0) are auto-accepted; otherwise they are skipped.
//
// Acks are accumulated per-source locally and routed in one lock
// acquisition per source (via requeueShareAcks).
//
// Must be called under consumer.mu.
func (s *shareConsumer) finalizeAndRouteAcks(autoAcceptPending bool) {
	type pendingRoute struct {
		acks     sharePendingAcks
		hasRenew bool
	}
	var routes map[*source]*pendingRoute

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
		// renewSent persists across polls: routeAndCompactLastPolled
		// keeps these records in lastPolled (not compacted away),
		// and each subsequent finalizePreviousPoll re-checks them.
		// This is intentional -- the user must provide a terminal
		// ack regardless of how many polls elapse.
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

		if routes == nil {
			routes = make(map[*source]*pendingRoute)
		}
		pr := routes[sc.source]
		if pr == nil {
			pr = &pendingRoute{acks: make(sharePendingAcks)}
			routes[sc.source] = pr
		}
		pr.acks.add(sc.topicID, r.Partition, []shareAckBatch{{
			firstOffset: r.Offset,
			lastOffset:  r.Offset,
			ackTypes:    []int8{ackType},
		}})
		if ackType == int8(AckRenew) {
			pr.hasRenew = true
		}
	}

	for src, pr := range routes {
		src.requeueShareAcks(pr.acks, pr.hasRenew)
	}
}

// routeAndCompactLastPolled finalizes and routes acks to their sources,
// then compacts lastPolled to keep only records still awaiting re-ack
// after a RENEW. All other records are cleared.
//
// Must be called under consumer.mu.
func (s *shareConsumer) routeAndCompactLastPolled(autoAcceptPending bool) {
	s.finalizeAndRouteAcks(autoAcceptPending)
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
	clear(s.lastPolled[n:]) // release stale record pointers for GC
	s.lastPolled = s.lastPolled[:n]
}

// finalizePreviousPoll auto-ACCEPTs any records from the previous poll that
// were not explicitly acknowledged, then routes all finalized acks to their
// respective source's pending ack queues. Records in RENEWING state are
// kept so the user can still call Record.Ack with a terminal status.
//
// This is a thin semantic wrapper over routeAndCompactLastPolled with
// autoAcceptPending=true. The two are not merged because finalizeForLeave
// calls routeAndCompactLastPolled directly with autoAcceptPending=false
// (release, not accept).
//
// Called under consumer.mu from PollFetches/PollRecords.
func (s *shareConsumer) finalizePreviousPoll() {
	if len(s.lastPolled) == 0 {
		return
	}
	s.routeAndCompactLastPolled(true)
}

// markAcks sets the ack status on records. If records is empty, all
// lastPolled records that have not already been explicitly marked are
// filled in. If specific records are provided, their status is set
// unconditionally (overriding any previous status).
func (s *shareConsumer) markAcks(status AckStatus, records []*Record) {
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	fillOnly := len(records) == 0
	if fillOnly {
		records = s.lastPolled
	}
	for _, r := range records {
		st := shareAckFromCtx(r)
		if st == nil {
			continue
		}
		st.mu.Lock()
		if !st.finalized && !st.renewSent {
			if !fillOnly || st.status == 0 {
				st.status = status
			}
		}
		st.mu.Unlock()
	}
}

// commitAcks finalizes all records that have been marked via Record.Ack,
// routes them to their sources, and sends ShareAcknowledge requests to each
// affected broker.
func (s *shareConsumer) commitAcks(ctx context.Context) (ShareAckResults, error) {
	// Finalize marked records and route to sources, under consumer.mu.
	// Unlike finalizePreviousPoll, this does NOT auto-ACCEPT unmarked
	// records and does NOT clear lastPolled.
	//
	// We also collect the set of share sources here (under the same
	// lock) so we only visit sources that actually have share cursors
	// rather than iterating every source in the client.
	s.c.mu.Lock()
	s.finalizeAndRouteAcks(false)
	sources := s.collectSources()
	s.c.mu.Unlock()

	return s.sendAcksFromSources(ctx, sources, true)
}

// batchesHaveRenew reports whether any ack batch contains an AckRenew type.
func batchesHaveRenew(batches []shareAckBatch) bool {
	for _, b := range batches {
		if slices.Contains(b.ackTypes, int8(AckRenew)) {
			return true
		}
	}
	return false
}

// sendAcksFromSources drains pending acks from the given sources and sends
// ShareAcknowledge requests to each broker in parallel. Returns per-partition
// results and any transport-level errors.
//
// When suppressCallback is true (i.e. called from commitAcks), the
// shareAckCallback is NOT called -- the caller returns results directly.
// When false (piggybacked ack path), the callback fires as usual.
func (s *shareConsumer) sendAcksFromSources(ctx context.Context, sources []*source, suppressCallback bool) (ShareAckResults, error) {
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
		return nil, nil
	}

	type ackResult struct {
		results ShareAckResults
		err     error
	}
	memberID, _ := s.memberGen.load()
	ch := make(chan ackResult, len(pending))
	for _, sa := range pending {
		go func() {
			results, err := s.sendSourceAck(ctx, sa.src, sa.acks, sa.hasRenew, memberID, suppressCallback)
			ch <- ackResult{results, err}
		}()
	}
	var allResults ShareAckResults
	var allErrs []error
	for range pending {
		r := <-ch
		allResults = append(allResults, r.results...)
		if r.err != nil {
			allErrs = append(allErrs, r.err)
		}
	}
	return allResults, errors.Join(allErrs...)
}

// buildShareAckTopics constructs the Topics slice for a ShareAcknowledge
// request from the given ack batches. Used by both sendSourceAck and
// closeSessions.
func buildShareAckTopics(acks sharePendingAcks) []kmsg.ShareAcknowledgeRequestTopic {
	topics := make([]kmsg.ShareAcknowledgeRequestTopic, 0, len(acks))
	for tid, parts := range acks {
		t := kmsg.NewShareAcknowledgeRequestTopic()
		t.TopicID = tid
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
			t.Partitions = append(t.Partitions, p)
		}
		topics = append(topics, t)
	}
	return topics
}

// sendSourceAck sends a single ShareAcknowledge request to one source's
// broker for the given pending ack batches.
func (s *shareConsumer) sendSourceAck(ctx context.Context, src *source, acks sharePendingAcks, hasRenew bool, memberID string, suppressCallback bool) (ShareAckResults, error) {
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
		// Notify the user that these acks were dropped. The
		// acquisitions no longer exist (session was reset), so
		// the broker will redeliver the records. This is not an
		// application error but the user should know.
		s.notifyDroppedAcks(acks, kerr.InvalidShareSessionEpoch)
		return nil, nil
	}

	req.Topics = buildShareAckTopics(acks)
	req.IsRenewAck = hasRenew

	// retryableBrokerFn retries transient dial/connection errors.
	kresp, err := s.cl.retryableBrokerFn(func() (*broker, error) {
		return s.cl.brokerOrErr(ctx, src.nodeID, errUnknownBroker)
	}).Request(ctx, req)
	if err != nil {
		// The broker may have processed the request before the
		// error, advancing its session epoch. Reset ours so the
		// next request starts a fresh session.
		//
		// Pending acks are intentionally dropped (not re-queued):
		// resetShareSession bumps shareSessionGen, causing
		// finalizeAndRouteAcks to skip remaining records from
		// the old session. The old acquisitions are released by
		// the broker, so these acks are stale -- re-queuing them
		// causes INVALID_RECORD_STATE on the next session. The
		// user receives the transport error via commitAcks.
		src.resetShareSession()
		return nil, err
	}

	// reqErr tracks transport-level or top-level response errors only.
	// Per-partition errors are reported via ackResults, not here.
	var reqErr error
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
			return nil, nil

		default:
			// ShareSessionLimitReached is retriable (Kafka marks
			// it as such) and falls through to the isAckRetryable
			// check below, which re-queues the acks. No dedicated
			// case is needed: unlike NOT_FOUND / INVALID_EPOCH,
			// it does not require a session reset.
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
			return nil, nil
		}
		// Non-retryable top-level error: return as the request error.
		// We still parse per-partition results below for the callback.
		reqErr = err
	} else {
		// Success: the broker incremented the session epoch.
		src.bumpShareSessionEpoch()
	}

	id2t := s.cl.id2tMap()
	var ackResults ShareAckResults
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
		}
	}
	if !suppressCallback && len(ackResults) > 0 && s.cfg.shareAckCallback != nil {
		s.cfg.shareAckCallback(s.cl, ackResults)
	}
	if len(moves) > 0 {
		brokers := make([]BrokerMetadata, len(resp.NodeEndpoints))
		for i, ne := range resp.NodeEndpoints {
			brokers[i] = BrokerMetadata{NodeID: ne.NodeID, Host: ne.Host, Port: ne.Port, Rack: ne.Rack}
		}
		s.applyMoves(moves, brokers)
	}
	return ackResults, reqErr
}

// notifyDroppedAcks fires the shareAckCallback (if configured) for all
// partitions in the given acks, reporting the provided error for each.
// Used when acks are dropped without being sent (e.g. stale session).
func (s *shareConsumer) notifyDroppedAcks(acks sharePendingAcks, err error) {
	if s.cfg.shareAckCallback == nil {
		return
	}
	id2t := s.cl.id2tMap()
	var results ShareAckResults
	for tid, parts := range acks {
		topicName := id2t[tid]
		for p := range parts {
			results = append(results, ShareAckResult{topicName, p, err})
		}
	}
	if len(results) > 0 {
		s.cfg.shareAckCallback(s.cl, results)
	}
}

// mergeAckBatches sorts ack batches by offset and merges contiguous
// ranges. Adjacent batches with the same uniform type stay compact
// (single-element ackTypes). Mixed-type adjacencies are expanded to
// per-offset arrays and concatenated.
//
// Examples (A=accept, R=release):
//
//	Input:  [5-7 A] [8-9 A]          -- same uniform type
//	Output: [5-9 A]                   -- merged, still uniform
//
//	Input:  [5-7 A] [8-9 R]          -- different uniform types
//	Output: [5-9 A,A,A,R,R]          -- merged, expanded per-offset
//
//	Input:  [5-7 A,R,A] [8-9 R]      -- already per-offset + uniform
//	Output: [5-9 A,R,A,R,R]          -- merged, concatenated
//
//	Input:  [5-7 A] [10-12 R]        -- gap (non-contiguous)
//	Output: [5-7 A] [10-12 R]        -- no merge
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
	return slices.Repeat(b.ackTypes[:1], n)
}

func (s *shareConsumer) leave(ctx context.Context) {
	s.mu.Lock()
	wasDead := s.dying.Swap(true)
	wasManaging := s.managing
	s.cancel()
	s.mu.Unlock()

	// Wake any poll blocked waiting for cursors so it
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
		s.closeSessions(ctx)

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
	// Wait for in-flight fetch goroutines so their piggybacked
	// acks are delivered before we close sessions.
	s.fetchWg.Wait()

	s.c.mu.Lock()
	defer s.c.mu.Unlock()

	// markPendingAsRelease sets AckRelease on records that are still
	// pending or have an unrouted renew. Unlike the normal poll path,
	// we also release records in RENEWING state (renewSent=true,
	// status=0) and records whose renew hasn't been routed yet
	// (status=AckRenew): on leave the consumer is going away and
	// will never provide a terminal ack. Explicitly releasing avoids
	// waiting for the broker's acquisition lock timeout before
	// redelivery. Java abandons these records (relying on lock
	// timeout); we diverge intentionally for faster redelivery.
	markPendingAsRelease := func(records []*Record) {
		for _, r := range records {
			st := shareAckFromCtx(r)
			if st == nil {
				continue
			}
			st.mu.Lock()
			if !st.finalized && (st.status == 0 || st.status == AckRenew) {
				st.status = AckRelease
				st.renewSent = false
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
		s.appendToLastPolled(s.bufferedFetches)
		s.bufferedFetches = nil
		markPendingAsRelease(s.lastPolled)
		s.finalizeAndRouteAcks(false)
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
			s.revokeCursor(sc)
		}
		delete(s.usingShareCursors, topic)
	}
	// Reset sent subscription so the next heartbeat sends the
	// updated list without the purged topics.
	s.lastSentSubscribedTopics = nil
	s.assignChanged.Broadcast()
}

// clearSourceState clears stale share state (pending acks, forgotten
// partitions, session epochs) from all sources. Called when the member is
// fenced or unknown -- the broker invalidated our state, so pending acks
// and sessions are stale and should not be sent.
func (s *shareConsumer) clearSourceState() {
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

// collectSources returns the deduplicated sources that have share
// cursors or pending forgotten/ack state, sorted by node ID for stable
// rotation. Must be called under c.mu.
func (s *shareConsumer) collectSources() []*source {
	if len(s.usingShareCursors) == 0 && len(s.sourcesWithForgotten) == 0 {
		return nil
	}
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

// pruneSourcesWithForgotten cleans up sourcesWithForgotten after a
// fetch round drains forgotten state from sources. A source is removed
// if it has active cursors (already visible via usingShareCursors, so
// redundant in the set) or has fully drained its forgotten and pending
// ack state (no longer needs the extra visibility).
//
// This function exists because sourcesWithForgotten is the only way
// collectSources discovers cursor-less sources that still need a
// ShareFetch to deliver ForgottenTopicsData. Without pruning, the set
// would grow unboundedly as partitions are reassigned across brokers.
// An alternative would be to scan all client sources in
// collectSources, but that is O(brokers) per poll vs. O(recently
// revoked) here.
//
// Must be called under c.mu.
func (s *shareConsumer) pruneSourcesWithForgotten() {
	if len(s.sourcesWithForgotten) == 0 {
		return
	}
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
		drained := len(src.shareSessionParts) == 0 && len(src.sharePendingAcks) == 0
		src.cursorsMu.Unlock()
		if drained {
			delete(s.sourcesWithForgotten, src)
		}
	}
}

// appendToLastPolled adds records from the given fetches to lastPolled
// so that finalizePreviousPoll can auto-ACCEPT or route explicit acks.
//
// Must be called under consumer.mu.
func (s *shareConsumer) appendToLastPolled(fetches Fetches) {
	n := fetches.NumRecords()
	if n == 0 {
		return
	}
	if cap(s.lastPolled)-len(s.lastPolled) < n {
		grown := make([]*Record, len(s.lastPolled), len(s.lastPolled)+n)
		copy(grown, s.lastPolled)
		s.lastPolled = grown
	}
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
	s.appendToLastPolled(fetches)
	return fetches
}

// poll sends ShareFetch requests to all sources with share cursors in
// parallel and returns the merged results. This is the pull model:
// fetches happen synchronously within PollFetches so records are only
// acquired when the user is ready to process them.
//
// maxPollRecords limits the total number of records returned. A value
// <= 0 means no limit (return everything from one round of fetches).
//
// Steps:
//  1. Finalize previous poll (auto-ACCEPT unacked records, route acks).
//  2. Return buffered records from a prior fetch if available.
//  3. (nil ctx only) Return injected errors or nil immediately.
//  4. Wait for sources with share cursors to become available.
//  5. Fan out ShareFetch requests to all sources in parallel.
//  6. Apply leader moves from CurrentLeader hints.
//  7. Buffer excess records, track returned records, prune stale state.
func (s *shareConsumer) poll(ctx context.Context, maxPollRecords int) Fetches {
	// Step 0: bail immediately if the client is shutting down.
	if s.dying.Load() {
		return NewErrFetch(ErrClientClosed)
	}

	// Step 1: finalize the previous poll and check for buffered
	// records. Common to both nil-context and normal paths.
	s.c.mu.Lock()
	s.finalizePreviousPoll()

	// Step 2: return buffered records from a prior over-fetch.
	if len(s.bufferedFetches) > 0 {
		fetches := s.takeBuffered(maxPollRecords)
		s.c.mu.Unlock()
		return fetches
	}

	// Step 3: nil context means "return immediately with whatever
	// is available." No buffered records, so check injected errors.
	if ctx == nil {
		s.c.mu.Unlock()
		if fake := s.c.drainFakeReady(); len(fake) > 0 {
			return Fetches(fake)
		}
		return nil
	}

	// Step 4: block until sources with share cursors are available,
	// an injected error arrives, or the context/client is cancelled.
	// waitForSources uses a sync.Cond which internally unlocks
	// c.mu while blocked in Wait and re-locks before returning,
	// so we are not holding the mutex during the actual wait.
	sources := s.waitForSources(ctx)
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

	// Step 5: fan out ShareFetch requests to all sources.
	fetches, allMoves, allBrokers := s.fanOutFetches(ctx, sources)

	// Step 6: apply leader moves from CurrentLeader hints.
	if len(allMoves) > 0 {
		s.applyMoves(allMoves, allBrokers)
	}

	// Append any injected error fetches that arrived during the
	// share fetch round-trips.
	fetches = append(fetches, fakeFetches...)

	// Step 7: buffer excess, track returned records, prune.
	s.c.mu.Lock()
	defer s.c.mu.Unlock()
	if maxPollRecords > 0 {
		fetches = s.bufferExcess(fetches, maxPollRecords)
	}
	s.appendToLastPolled(fetches)
	s.pruneSourcesWithForgotten()

	return fetches
}

// waitForSources blocks until at least one source with share
// cursors is available, the context is cancelled, the client is dying,
// or injected errors are waiting. Must be called with c.mu held;
// returns with c.mu still held.
func (s *shareConsumer) waitForSources(ctx context.Context) []*source {
	sources := s.collectSources()
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
		sources = s.collectSources()
	}
	close(done)
	return sources
}

// fanOutFetches sends ShareFetch requests to all sources and returns
// as soon as any source returns records. Maintenance requests (acks,
// forgotten topics) go to all sources immediately. Full-fetch requests
// are limited to maxConcurrentFetches via a semaphore (s.fetchSlots).
//
// Goroutines from a previous poll round that are still blocked on
// broker MaxWait hold their slot. The current poll acquires a slot
// before sending each full fetch, naturally throttling concurrency
// across poll rounds.
//
// Results are read from a channel. The first result with records
// triggers an immediate return. Remaining goroutines continue in the
// background -- their results are drained at the start of the next
// poll.
//
// Goroutines use a fetch-scoped context derived from s.ctx with
// the same deadline as the caller's poll context (if any). The
// fetch context is NOT cancelled on early return -- goroutines
// that already sent a ShareFetch must receive the response,
// otherwise acquired records are locked on the broker until the
// acquisition timeout expires. Goroutines run until the broker
// responds (within MaxWait) or the poll deadline fires. Using
// s.ctx as the parent ensures goroutines are cancelled when the
// share consumer leaves.
func (s *shareConsumer) fanOutFetches(
	ctx context.Context,
	sources []*source,
) (Fetches, []shareMove, []BrokerMetadata) {
	nTotal := len(sources)

	// Rotate source ordering for fairness.
	start := s.sourceFetchStart % nTotal
	s.sourceFetchStart++

	// Build a fetch-scoped context: child of s.ctx so it is
	// cancelled on leave, but also inherits the caller's deadline
	// (poll timeout) so goroutines do not outlive the poll.
	//
	// We do NOT cancel this context when fanOutFetches returns
	// early. Goroutines may have in-flight broker requests that
	// have already acquired records -- cancelling them would
	// leave those records locked on the broker until the
	// acquisition timeout expires. Instead, goroutines run until
	// the broker responds (within MaxWait) or the deadline fires.
	// Their results are drained on the next poll.
	fetchCtx := s.ctx
	var roundWg sync.WaitGroup // tracks this round's goroutines
	if deadline, ok := ctx.Deadline(); ok {
		var cancel context.CancelFunc
		fetchCtx, cancel = context.WithDeadline(s.ctx, deadline)
		// Cancel is deferred via a goroutine that waits for
		// this round's goroutines to finish. This releases
		// the deadline timer without cancelling goroutines
		// prematurely on early return.
		defer func() {
			go func() {
				roundWg.Wait()
				cancel()
			}()
		}()
	}

	results := s.fetchResults

	// Launch one goroutine per source. Full fetches acquire a slot
	// from the semaphore; maintenance requests don't.
	//
	// Each goroutine sends to fetchResults with a select on
	// s.ctx.Done as an escape hatch: during normal operation
	// the send blocks until the poll loop drains the channel;
	// during shutdown (s.ctx cancelled) the goroutine exits
	// without sending so finalizeForLeave's fetchWg.Wait
	// completes. A plain non-blocking send would silently
	// drop results when the channel is full, causing the
	// outstanding counter to never reach zero and the poll
	// loop to hang forever (especially with context.Background).
	launched := 0
	for i := range nTotal {
		src := sources[(start+i)%nTotal]

		// Lazy-init per-source mutex (buffered chan of 1).
		mu, ok := s.sourceMu[src]
		if !ok {
			mu = make(chan struct{}, 1)
			s.sourceMu[src] = mu
		}

		// Skip sources that still have a goroutine running
		// from a previous poll round. They'll finish on their
		// own and be available next poll.
		select {
		case mu <- struct{}{}:
		default:
			continue
		}

		launched++
		s.fetchWg.Add(1)
		roundWg.Add(1)
		go func() {
			defer s.fetchWg.Done()
			defer roundWg.Done()
			defer func() { <-mu }()

			// Try to acquire a full-fetch slot. If we can't
			// get one immediately, fall back to maintenance so
			// acks and forgotten topics are delivered promptly.
			maintenance := false
			select {
			case s.fetchSlots <- struct{}{}:
				defer func() { <-s.fetchSlots }()
			default:
				maintenance = true
			}

			r := s.doFetch(fetchCtx, src, maintenance)
			select {
			case results <- fetchResult{r}:
			case <-s.ctx.Done():
			}
		}()
	}

	var (
		fetches    Fetches
		allMoves   []shareMove
		allBrokers []BrokerMetadata
	)

	collect := func(r fetchResult) {
		allMoves = append(allMoves, r.r.moves...)
		allBrokers = append(allBrokers, r.r.brokers...)
		if r.r.fetch.hasErrorsOrRecords() {
			fetches = append(fetches, r.r.fetch)
		}
	}

	// First, drain any results from previous poll rounds that
	// arrived after we returned early.
	for {
		select {
		case r := <-results:
			collect(r)
		default:
			goto drained
		}
	}
drained:

	// If we already have records from draining, return early.
	if len(fetches) > 0 {
		return fetches, allMoves, allBrokers
	}

	// If no goroutines were launched (all sources busy from a
	// previous round), block on the results channel rather than
	// spin-returning empty. This avoids a CPU-burning loop when
	// previous-round goroutines hold all source mutexes. We also
	// watch s.ctx.Done so shutdown unblocks even when the poll
	// context has no deadline (context.Background).
	if launched == 0 {
		select {
		case r := <-results:
			collect(r)
		case <-ctx.Done():
		case <-s.ctx.Done():
		}
		return fetches, allMoves, allBrokers
	}

	// Wait for results from this round's goroutines. Return as
	// soon as any result has records. Stale results from previous
	// rounds may arrive here; they are harmlessly collected and
	// decrement outstanding (at worst we stop one iteration early,
	// collecting that round's remaining result on the next drain).
	//
	// We watch s.ctx.Done alongside ctx.Done because a goroutine
	// whose send select picked s.ctx.Done exits without sending,
	// so outstanding would never reach zero. Without this, a
	// Close() during poll with context.Background() hangs forever.
	for outstanding := launched; outstanding > 0; outstanding-- {
		select {
		case r := <-results:
			collect(r)
			if len(fetches) > 0 {
				return fetches, allMoves, allBrokers
			}
		case <-ctx.Done():
			return fetches, allMoves, allBrokers
		case <-s.ctx.Done():
			return fetches, allMoves, allBrokers
		}
	}

	return fetches, allMoves, allBrokers
}

// doFetch sends a single ShareFetch request to one source and
// returns the decoded Fetch. On session errors (e.g.
// SHARE_SESSION_NOT_FOUND), the epoch is reset and the request is
// retried exactly once.
//
// Retry-once rationale: session errors reset the epoch to 0, so the
// retry creates a fresh session. If the fresh session also fails with
// a session error, the broker is rejecting new sessions (e.g. limit
// reached persistently) and retrying further won't help -- the next
// poll round will try again. Piggybacked acks are NOT restored on
// retry because the broker destroyed the old session, releasing all
// acquisitions those acks referred to. Re-queuing stale acks causes
// INVALID_RECORD_STATE on the new session.
//
// If maintenanceOnly is true, the request carries acks, forgotten
// topics, and session state but zeroes out all fetch parameters so the
// broker responds immediately without acquiring new records.
// Each source maps to exactly one broker, so a failed fetch for one
// source does not fall over to another -- the next poll round retries
// the same source (and may discover a leader change via metadata).
func (s *shareConsumer) doFetch(ctx context.Context, src *source, maintenanceOnly bool) shareFetchResult {
	var retried bool
attempt:
	result, retry := s.doFetchAttempt(ctx, src, maintenanceOnly)
	if retry && !retried {
		retried = true
		goto attempt
	}
	return result
}

// doFetchAttempt performs a single ShareFetch round trip. Returns
// the result and whether the caller should retry (session error).
func (s *shareConsumer) doFetchAttempt(
	ctx context.Context,
	src *source,
	maintenanceOnly bool,
) (shareFetchResult, bool) {
	req, piggybackedAcks, acksHasRenew := s.buildFetchRequest(src, maintenanceOnly)
	if req == nil {
		return shareFetchResult{}, false
	}

	br, err := s.cl.brokerOrErr(ctx, src.nodeID, errUnknownBroker)
	if err != nil {
		src.requeueShareAcks(piggybackedAcks, acksHasRenew)
		return shareFetchResult{}, false
	}

	// Use the same br.do + select pattern as regular fetch
	// (source.go) so that context cancellation immediately
	// unblocks us rather than relying on readConn's ctx.Done
	// propagation alone.
	var kresp kmsg.Response
	requested := make(chan struct{}, 1)
	br.do(ctx, req, func(k kmsg.Response, e error) {
		kresp, err = k, e
		requested <- struct{}{}
	})

	select {
	case <-requested:
		// As select is pseudo-random when both cases are
		// ready, check for context error to avoid falling
		// through to the retry+backoff path with a dead
		// context.
		if isContextErr(err) && ctx.Err() != nil {
			// Reset the session only if the share consumer
			// itself is shutting down (s.ctx cancelled). A
			// fetch-scoped context deadline means the poll
			// expired -- the session is still valid for the
			// next poll.
			if s.ctx.Err() != nil {
				src.resetShareSession()
			} else {
				src.requeueShareAcks(piggybackedAcks, acksHasRenew)
			}
			return shareFetchResult{}, false
		}
	case <-ctx.Done():
		// The 1-size buffer on requested lets the callback
		// complete without a reader, so no goroutine drain needed.
		if s.ctx.Err() != nil {
			src.resetShareSession()
		} else {
			src.requeueShareAcks(piggybackedAcks, acksHasRenew)
		}
		return shareFetchResult{}, false
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
		after := time.NewTimer(s.cfg.retryBackoff(src.consecutiveFailures))
		select {
		case <-after.C:
		case <-ctx.Done():
		}
		after.Stop()
		return shareFetchResult{}, false
	}
	src.consecutiveFailures = 0

	resp := kresp.(*kmsg.ShareFetchResponse)

	s.cfg.logger.Log(LogLevelDebug, "share fetch response",
		"broker", src.nodeID,
		"error_code", resp.ErrorCode,
		"num_topics", len(resp.Topics),
	)

	fetch, retry, moves := s.handleFetchResponse(src, req, resp, piggybackedAcks, acksHasRenew)
	if !retry {
		var brokers []BrokerMetadata
		if len(moves) > 0 {
			brokers = make([]BrokerMetadata, len(resp.NodeEndpoints))
			for i, ne := range resp.NodeEndpoints {
				brokers[i] = BrokerMetadata{NodeID: ne.NodeID, Host: ne.Host, Port: ne.Port, Rack: ne.Rack}
			}
		}
		return shareFetchResult{fetch, moves, brokers}, false
	}
	return shareFetchResult{}, true
}

// shareSourceSnapshot holds a point-in-time view of a source's share state,
// taken under cursorsMu and then used outside the lock to build requests.
// Snapshot fields are safe to use without locks because:
//   - shareCursor fields are immutable after construction
//   - pendingAcks is transferred by ownership (source nils its ref)
//   - forgotten is computed from the diff of shareSessionParts vs live cursors
//   - sessionEpoch is a plain value copy
type shareSourceSnapshot struct {
	cursors      []*shareCursor
	pendingAcks  sharePendingAcks
	forgotten    map[[16]byte][]int32
	sessionEpoch int32
	hasRenew     bool
}

// snapshotShareState snapshots this source's share session state under
// cursorsMu, transferring ownership of pendingAcks to the caller and
// computing the forgotten set by diffing shareSessionParts against the
// live cursor set. Returns ok=false if the source has no share work.
//
// The forgotten set is computed at snapshot time rather than maintained
// as a separate field because it eliminates the need to restore
// forgotten data on failed requests. shareSessionParts is the canonical
// record of what the broker knows; entries are only removed from it in
// updateShareSessionParts on successful response. If a request fails,
// the diff is simply recomputed on the next attempt.
func (s *source) snapshotShareState() (snap shareSourceSnapshot, ok bool) {
	s.cursorsMu.Lock()
	if len(s.shareCursors) == 0 && len(s.sharePendingAcks) == 0 && len(s.shareSessionParts) == 0 {
		s.cursorsMu.Unlock()
		return snap, false
	}
	snap.pendingAcks = s.sharePendingAcks
	snap.sessionEpoch = s.shareSessionEpoch
	snap.hasRenew = s.shareHasRenew

	// Build a set of live cursors so we can identify which session
	// partitions need to be forgotten and which cursors are new.
	cursorSet := make(map[topicPartIdx]struct{}, len(s.shareCursors))
	for _, sc := range s.shareCursors {
		cursorSet[topicPartIdx{sc.topicID, sc.partition}] = struct{}{}
	}

	// Compute forgotten: partitions the broker knows about (in
	// shareSessionParts) that no longer have a live cursor. We do
	// NOT remove these from shareSessionParts here --
	// updateShareSessionParts does that on successful response.
	// This means failed requests naturally retry the forgotten set
	// without any restore logic.
	for key := range s.shareSessionParts {
		if _, hasCursor := cursorSet[key]; !hasCursor {
			if snap.forgotten == nil {
				snap.forgotten = make(map[[16]byte][]int32)
			}
			snap.forgotten[key.topicID] = append(snap.forgotten[key.topicID], key.partition)
		}
	}

	// On epoch 0 (new session), send all cursors. On epoch > 0
	// (incremental), only send cursors not yet known to the broker's
	// session -- the broker continues fetching from session partitions
	// that are omitted from incremental requests.
	//
	// Iterate from shareCursorsStart so the broker sees partitions in
	// rotated order. When MaxRecords limits the response, different
	// partitions get priority across poll rounds (mirrors
	// source.cursorsStart for regular fetches).
	nShareCursors := len(s.shareCursors)
	ci := s.shareCursorsStart
	for range s.shareCursors {
		sc := s.shareCursors[ci]
		ci = (ci + 1) % nShareCursors
		if snap.sessionEpoch > 0 {
			if _, inSession := s.shareSessionParts[topicPartIdx{sc.topicID, sc.partition}]; inSession {
				continue
			}
		}
		snap.cursors = append(snap.cursors, sc)
	}
	if nShareCursors > 0 {
		s.shareCursorsStart = (s.shareCursorsStart + 1) % nShareCursors
	}
	s.sharePendingAcks = nil
	s.shareHasRenew = false
	s.cursorsMu.Unlock()

	// Merge pending ack batches outside the lock -- sorting and
	// allocating does not need cursorsMu, matching drainShareAcks.
	snap.pendingAcks.mergeAll()
	return snap, true
}

// buildFetchRequest constructs a ShareFetchRequest for the given source.
// The second return value is the ack map that was attached to the request
// (nil if no acks were attached). Callers use this to re-queue acks on
// failure without reconstructing them from the wire format.
//
// If maintenanceOnly is true, all fetch parameters are zeroed so the broker
// responds immediately without acquiring new records. The request still
// carries acks, forgotten topics, and session cursor updates. Returns nil if
// there is nothing to maintain (no new cursors, no acks, no forgotten).
func (s *shareConsumer) buildFetchRequest(src *source, maintenanceOnly bool) (*kmsg.ShareFetchRequest, sharePendingAcks, bool) {
	snap, ok := src.snapshotShareState()
	if !ok {
		return nil, nil, false
	}

	memberID, _ := s.memberGen.load()

	req := kmsg.NewPtrShareFetchRequest()
	req.GroupID = &s.cfg.shareGroup
	req.MemberID = &memberID
	req.ShareSessionEpoch = snap.sessionEpoch
	req.MaxWaitMillis = s.cfg.maxWait
	req.MinBytes = s.cfg.minBytes
	req.MaxBytes = s.cfg.maxBytes.load()

	maxRecords := s.cfg.shareMaxRecords
	if maxRecords <= 0 {
		maxRecords = defShareMaxRecords
	}
	req.MaxRecords = maxRecords
	req.BatchSize = maxRecords
	if s.cfg.shareMaxRecordsStrict {
		req.ShareAcquireMode = 1
	}

	topicIdx := make(map[[16]byte]int)    // topicID -> index in req.Topics
	partIdx := make(map[topicPartIdx]int) // (topicID, partition) -> index in topic's Partitions
	for _, sc := range snap.cursors {
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
	if len(snap.pendingAcks) > 0 {
		if snap.sessionEpoch == 0 {
			s.cfg.logger.Log(LogLevelInfo, "dropping stale piggybacked acks, share session epoch is 0",
				"broker", src.nodeID,
			)
			s.notifyDroppedAcks(snap.pendingAcks, kerr.InvalidShareSessionEpoch)
			snap.pendingAcks = nil
			snap.hasRenew = false
		} else {
			attachAcksToShareFetch(req, snap.pendingAcks, topicIdx, partIdx)
			req.IsRenewAck = snap.hasRenew
		}
	}

	// Attach forgotten topics. Only meaningful on incremental requests
	// (epoch > 0) -- on epoch 0 the broker creates a fresh session with
	// no prior state to forget.
	if snap.sessionEpoch > 0 {
		for tid, parts := range snap.forgotten {
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

	return req, snap.pendingAcks, snap.hasRenew
}

// attachAcksToShareFetch piggybacks pending ack batches onto a ShareFetch
// request. Acks are added to existing topic/partition entries (via the
// partition index) or new ack-only entries are created.
func attachAcksToShareFetch(
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

		rt := &req.Topics[tidx]
		for partition, batches := range parts {
			key := topicPartIdx{tid, partition}
			pi, ok := pIdx[key]
			if !ok {
				pi = len(rt.Partitions)
				pIdx[key] = pi
				p := kmsg.NewShareFetchRequestTopicPartition()
				p.Partition = partition
				p.PartitionMaxBytes = 0
				rt.Partitions = append(rt.Partitions, p)
			}

			rp := &rt.Partitions[pi]
			for _, batch := range batches {
				ab := kmsg.NewShareFetchRequestTopicPartitionAcknowledgementBatche()
				ab.FirstOffset = batch.firstOffset
				ab.LastOffset = batch.lastOffset
				ab.AcknowledgeTypes = batch.ackTypes
				rp.AcknowledgementBatches = append(rp.AcknowledgementBatches, ab)
			}
		}
	}
}

// handleFetchResponse processes a ShareFetch response, updating session
// epoch and building a Fetch. Returns the Fetch and whether the caller should
// retry (on session errors). Each acquired record has a shareAckState
// injected into its Context for ack tracking.
//
// piggybackedAcks is the ack map returned by buildFetchRequest. It is
// used to re-queue acks on retryable errors and detect missing partitions.
func (s *shareConsumer) handleFetchResponse(
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
	sentAckParts := make(map[topicPartIdx]struct{})
	for tid, parts := range piggybackedAcks {
		for p := range parts {
			sentAckParts[topicPartIdx{tid, p}] = struct{}{}
		}
	}

	id2t := s.cl.id2tMap()
	var fetch Fetch
	var moves []shareMove
	var ackResults ShareAckResults

	for i := range resp.Topics {
		rt := &resp.Topics[i]
		topicName := id2t[rt.TopicID]
		if topicName == "" {
			continue
		}

		var partitions []FetchPartition
		for j := range rt.Partitions {
			rp := &rt.Partitions[j]
			tpKey := topicPartIdx{rt.TopicID, rp.Partition}
			_, hadAcks := sentAckParts[tpKey]
			delete(sentAckParts, tpKey)

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
					continue
				}
				partitions = append(partitions, FetchPartition{
					Partition: rp.Partition,
					Err:       partErr,
				})
				continue
			}

			if len(rp.Records) == 0 && len(rp.AcquiredRecords) == 0 {
				continue
			}

			fp, gapAcks := s.decodePartition(src, topicName, rp, acqLockDeadline)
			if len(gapAcks) > 0 {
				src.addShareAcks(rt.TopicID, rp.Partition, gapAcks, false) // gap acks are never renew
			}
			if len(fp.Records) == 0 && fp.Err == nil {
				continue
			}
			partitions = append(partitions, fp)
		}
		if len(partitions) > 0 {
			fetch.Topics = append(fetch.Topics, FetchTopic{
				Topic:      topicName,
				Partitions: partitions,
			})
		}
	}

	// Drop piggybacked acks for partitions the broker omitted from the
	// response. The broker should always include a partition-level result
	// for every partition in the request; if it doesn't, the broker is
	// buggy. We drop rather than re-queue to avoid carrying stale acks
	// indefinitely in a retry loop.
	for key := range sentAckParts {
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

// decodePartition decodes record batches from a ShareFetch response
// partition, filters to acquired records, injects shareAckState into each
// record's Context, and generates gap acks for compacted offsets.
func (s *shareConsumer) decodePartition(
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

	// slabs tracks all per-batch shareAckSlab allocations so we can
	// populate them after ProcessFetchPartition decodes the records.
	var slabs []*shareAckSlab
	fp, _ := ProcessFetchPartition(ProcessFetchPartitionOpts{
		KeepControlRecords:   s.cfg.keepControl,
		DisableCRCValidation: s.cfg.disableFetchCRCValidation,
		Topic:                topicName,
		Partition:            rp.Partition,
		Pools:                s.cfg.pools,
		shareAckSlab: func(numRecords int, firstRecord *Record) *shareAckSlab {
			slab := &shareAckSlab{
				states:   make([]shareAckState, numRecords),
				records0: firstRecord,
			}
			slabs = append(slabs, slab)
			return slab
		},
	}, &fakePart, s.cfg.decompressor, nil)

	// Filter to only records within AcquiredRecords ranges and
	// populate the slab states. Both fp.Records and AcquiredRecords
	// are sorted by offset, so we use a two-pointer scan for O(n+m).
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
		gapAcks []shareAckBatch
		ri, n   int
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
			// The record's context already has a slab from
			// batchContext. Find the state via pointer
			// arithmetic (safe: records within a batch are
			// contiguous) and populate it.
			// Look up the slab state directly via pointer
			// arithmetic (not shareAckFromCtx, which checks
			// source==nil and would return nil for unpopulated
			// states). Populate it with the acquired record's
			// metadata.
			slab := r.Context.Value(shareAckKey).(*shareAckSlab)
			idx := int((uintptr(unsafe.Pointer(r)) - uintptr(unsafe.Pointer(slab.records0))) / recSize) //nolint:gosec
			slab.states[idx] = shareAckState{
				deliveryCount:           int32(ar.DeliveryCount),
				source:                  src,
				acquisitionLockDeadline: acqLockDeadline,
				sessionGen:              sessionGen,
			}
			fp.Records[n] = r
			n++
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

	clear(fp.Records[n:])
	fp.Records = fp.Records[:n]
	return fp, gapAcks
}

// closeSessions sends ShareAcknowledge with ShareSessionEpoch=-1 to
// each source that has an active share session or pending acks, telling
// the broker to close the session. Pending acks are included in the close
// request so the broker processes them in one round trip. Without the
// close, the broker keeps the session alive and the group may remain
// NON_EMPTY after the leave heartbeat.
func (s *shareConsumer) closeSessions(ctx context.Context) {
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
		// Drain acks and read epoch in one lock acquisition
		// rather than two (drainShareAcks + separate epoch read).
		src.cursorsMu.Lock()
		acks := src.sharePendingAcks
		hasRenew := src.shareHasRenew
		epoch := src.shareSessionEpoch
		src.sharePendingAcks = nil
		src.shareHasRenew = false
		src.cursorsMu.Unlock()

		acks.mergeAll()

		if epoch <= 0 && len(acks) == 0 {
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
