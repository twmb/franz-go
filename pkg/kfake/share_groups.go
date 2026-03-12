package kfake

import (
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Share group state: per-group membership and per-(group,topic,partition) record
// acquisition tracking. Share groups (KIP-932) provide at-least-once delivery
// where multiple consumers in the same group receive records from the same
// partitions concurrently. The broker tracks per-record state: which consumer
// acquired each record, the delivery count, and whether the record has been
// acknowledged (accepted/rejected/released).

type shareGroups struct {
	c *Cluster

	gs map[string]*shareGroup

	// sweepCh receives notifications from manage goroutines when the
	// sweep timer releases records. run() receives from this and fires
	// share watchers (pd.shareWatch is only safe from run()).
	sweepCh chan *shareGroup
}

type shareGroup struct {
	c    *Cluster
	name string

	groupEpoch    int32
	members       map[string]*shareMember
	lastTopicMeta topicMetaSnap // cached snapshot from run(), for recomputation on member removal/fencing

	// Per-(topic,partition) record acquisition state.
	// Accessed from both the run() goroutine (ShareFetch/ShareAcknowledge)
	// and the manage goroutine (sweep timer, member fencing). Must be
	// accessed under mu.
	mu         sync.Mutex
	partitions tps[sharePartition]

	reqCh     chan *clientReq
	controlCh chan func()

	quit   sync.Once
	quitCh chan struct{}
}

type shareMember struct {
	memberID   string
	clientID   string
	clientHost string
	rackID     *string

	memberEpoch         int32
	previousMemberEpoch int32
	subscribedTopics    []string

	// assignment: topicID -> partitions
	assignment map[uuid][]int32

	t    *time.Timer
	last time.Time
}

// shareRecordState tracks the per-record state in a share partition.
type shareRecordState int8

const (
	shareRecordAvailable    shareRecordState = iota // can be acquired
	shareRecordAcquired                             // locked by a member
	shareRecordAcknowledged                         // accepted, pending SPSO advance
	shareRecordArchived                             // rejected, pending SPSO advance
)

// shareRecord tracks one record's acquisition state within a share partition.
type shareRecord struct {
	state         shareRecordState
	acquiredBy    string // memberID
	deliveryCount int32
	acquireTime   time.Time
}

// sharePartition tracks the SPSO and per-record state for one (group, topic, partition).
//
// records is a sparse map from offset to state. Entries are created on
// first acquisition and deleted when SPSO advances past them. The map
// may grow large if a consumer acquires many records without acking --
// acceptable for a test broker.
type sharePartition struct {
	spso      int64 // Share-Partition Start Offset: first unfinalized offset
	endOffset int64 // one past highest tracked offset, for in-flight count
	records   map[int64]*shareRecord

	// scanOffset tracks the next offset to scan for available records.
	// Advances past acquired/archived records to avoid re-scanning them.
	// Reset back toward SPSO when records are released (ack release,
	// sweep, member fencing).
	scanOffset int64
}

// shareSessionKey identifies a share session.
type shareSessionKey struct {
	group    string
	memberID string
	broker   int32
}

// shareSession tracks a share fetch session's epoch and the set of
// partitions currently in the session. On epoch 0 (new session), the
// request's Topics become the session's partitions. On epoch > 0
// (incremental), the request's Topics are ADDED to the session and
// ForgottenTopicsData are REMOVED (matching Kafka's ShareSession.update).
type shareSession struct {
	epoch      int32
	partitions map[uuid]map[int32]*cachedSharePart // topicID -> partition -> cached state
	cc         *clientConn                         // owning connection, for disconnect cleanup
}

// cachedSharePart tracks per-partition session state for incremental
// response filtering (matching Java's CachedSharePartition).
type cachedSharePart struct {
	// requiresUpdate is true when the partition must appear in the next
	// incremental response even if it has no data. Set on:
	//   - partition first added to session
	//   - response included an error (so the "error cleared" transition
	//     is sent on the next response)
	// Cleared after the partition appears in a response without errors.
	requiresUpdate bool
}

// watchShareFetch suspends a ShareFetch request until new records are
// available or MaxWait expires. Registered on partData.shareWatch;
// when pushBatch adds records, the watcher fires via do(). The
// cleanup and re-invocation happen in the cluster run() loop.
type watchShareFetch struct {
	creq    *clientReq
	session *shareSession // session at registration time; stale if overwritten
	in      []*partData
	cb      func()
	t       *time.Timer

	once    sync.Once
	cleaned bool
}

func (w *watchShareFetch) do() {
	w.once.Do(func() {
		go w.cb()
	})
}

func (w *watchShareFetch) cleanup() {
	w.cleaned = true
	for _, pd := range w.in {
		delete(pd.shareWatch, w)
	}
	w.t.Stop()
}

func (sgs *shareGroups) handleHeartbeat(creq *clientReq) {
	req := creq.kreq.(*kmsg.ShareGroupHeartbeatRequest)

	// Group type exclusivity: if this group ID is already a consumer
	// group, reject the share group heartbeat (matching Kafka's
	// GroupCoordinatorService which prevents mixing group types).
	if _, isConsumer := sgs.c.groups.gs[req.GroupID]; isConsumer {
		resp := req.ResponseKind().(*kmsg.ShareGroupHeartbeatResponse)
		resp.ErrorCode = kerr.GroupIDNotFound.Code
		select {
		case creq.cc.respCh <- clientResp{kresp: resp, corr: creq.corr, seq: creq.seq}:
		case <-creq.cc.done:
		case <-sgs.c.die:
		}
		return
	}

	// For non-join heartbeats (epoch != 0), the group must exist.
	// Java returns GROUP_ID_NOT_FOUND for heartbeats to unknown groups.
	if req.MemberEpoch != 0 {
		if sgs.gs == nil || sgs.gs[req.GroupID] == nil {
			resp := req.ResponseKind().(*kmsg.ShareGroupHeartbeatResponse)
			resp.ErrorCode = kerr.GroupIDNotFound.Code
			select {
			case creq.cc.respCh <- clientResp{kresp: resp, corr: creq.corr, seq: creq.seq}:
			case <-creq.cc.done:
			case <-sgs.c.die:
			}
			return
		}
	}

	// Snapshot topic metadata while in run() where c.data is safe to
	// read. manage() will use this snapshot for assignment computation,
	// avoiding a concurrent map read on c.data.tps.
	creq.topicMeta = sgs.c.snapshotTopicMeta()
	g := sgs.getOrCreate(req.GroupID)
	select {
	case g.reqCh <- creq:
	case <-g.quitCh:
		// Group quit -- restart.
		delete(sgs.gs, req.GroupID)
		g = sgs.getOrCreate(req.GroupID)
		select {
		case g.reqCh <- creq:
		case <-g.c.die:
		}
	case <-g.c.die:
	}
}

func (sgs *shareGroups) getOrCreate(name string) *shareGroup {
	if sgs.gs == nil {
		sgs.gs = make(map[string]*shareGroup)
	}
	g := sgs.gs[name]
	if g == nil {
		g = &shareGroup{
			c:          sgs.c,
			name:       name,
			members:    make(map[string]*shareMember),
			partitions: make(tps[sharePartition]),
			reqCh:      make(chan *clientReq, 16),
			controlCh:  make(chan func(), 1),
			quitCh:     make(chan struct{}),
		}
		sgs.gs[name] = g
		go g.manage()
	}
	return g
}

func (g *shareGroup) manage() {
	sweepInterval := time.Duration(g.c.shareAcqLockSweepIntervalMs()) * time.Millisecond
	acqLockTicker := time.NewTicker(sweepInterval)
	defer acqLockTicker.Stop()
	defer func() {
		for _, m := range g.members {
			if m.t != nil {
				m.t.Stop()
			}
		}
	}()
	for {
		select {
		case <-g.quitCh:
			return
		case <-g.c.die:
			return
		case creq := <-g.reqCh:
			var kresp kmsg.Response
			switch creq.kreq.(type) {
			case *kmsg.ShareGroupHeartbeatRequest:
				g.lastTopicMeta = creq.topicMeta
				kresp = g.handleHeartbeat(creq)
			}
			if kresp != nil {
				g.reply(creq, kresp)
			}
		case fn := <-g.controlCh:
			fn()
		case <-acqLockTicker.C:
			g.sweepExpiredAcquisitions()
		}
	}
}

// sweepExpiredAcquisitions releases records whose acquisition lock has
// expired. If a record has been delivered maxDeliveryCount times, it is
// archived instead of released. After releasing records, notifies run()
// via sweepNotifyCh so it can fire share watchers (pd.shareWatch is only
// safe to access from run()).
func (g *shareGroup) sweepExpiredAcquisitions() {
	g.mu.Lock()
	now := time.Now()
	lockDuration := time.Duration(g.c.shareRecordLockDurationMs()) * time.Millisecond
	maxDelivery := g.c.shareMaxDeliveryAttempts()
	released := false
	g.partitions.each(func(_ string, _ int32, sp *sharePartition) {
		for offset, sr := range sp.records {
			if sr.state != shareRecordAcquired {
				continue
			}
			if now.Sub(sr.acquireTime) < lockDuration {
				continue
			}
			if sr.deliveryCount >= maxDelivery {
				sr.state = shareRecordArchived
			} else {
				sr.state = shareRecordAvailable
				released = true
				if offset < sp.scanOffset {
					sp.scanOffset = offset
				}
			}
			sr.acquiredBy = ""
		}
		sp.advanceSPSO()
	})
	g.mu.Unlock()
	if released {
		select {
		case g.c.shareGroups.sweepCh <- g:
		default:
		}
	}
}

// waitControl sends fn to the manage loop's controlCh and blocks until
// it completes. Used for external callers (persistence, shutdown) that
// need safe access to share group state.
func (g *shareGroup) waitControl(fn func()) bool {
	return waitManageControl(g.controlCh, g.quitCh, g.c, fn)
}

// quitOnce shuts down the manage goroutine. Called when the group is
// truly empty (no members and no partition state).
func (g *shareGroup) quitOnce() {
	g.quit.Do(func() {
		close(g.quitCh)
	})
}

func (g *shareGroup) reply(creq *clientReq, kresp kmsg.Response) {
	select {
	case creq.cc.respCh <- clientResp{kresp: kresp, corr: creq.corr, seq: creq.seq}:
	case <-creq.cc.done:
	case <-g.c.die:
	}
}

func (g *shareGroup) handleHeartbeat(creq *clientReq) kmsg.Response {
	req := creq.kreq.(*kmsg.ShareGroupHeartbeatRequest)
	resp := req.ResponseKind().(*kmsg.ShareGroupHeartbeatResponse)
	resp.HeartbeatIntervalMillis = g.c.shareHeartbeatIntervalMs()

	switch req.MemberEpoch {
	case 0:
		return g.handleJoin(creq, req, resp)
	case -1:
		return g.handleLeave(req, resp)
	default:
		return g.handleRegularHeartbeat(creq, req, resp)
	}
}

func (g *shareGroup) handleJoin(creq *clientReq, req *kmsg.ShareGroupHeartbeatRequest, resp *kmsg.ShareGroupHeartbeatResponse) *kmsg.ShareGroupHeartbeatResponse {
	memberID := req.MemberID

	// If existing member, treat as rejoin. Only bump groupEpoch if
	// subscriptions actually changed (matching Kafka's behavior where
	// groupEpoch only bumps on metadata/subscription changes).
	if m := g.members[memberID]; m != nil {
		newSubs := slices.Clone(req.SubscribedTopicNames)
		slices.Sort(newSubs)
		subsChanged := !slices.Equal(m.subscribedTopics, newSubs)
		m.subscribedTopics = newSubs
		m.last = time.Now()
		g.resetSessionTimeout(m)
		if subsChanged {
			g.groupEpoch++
			g.recomputeAssignments()
		}
		g.reconcileMember(m)
		resp.MemberID = &memberID
		resp.MemberEpoch = m.memberEpoch
		resp.Assignment = g.makeAssignment(m)
		return resp
	}

	// Check share group max size (matching Kafka's
	// throwIfShareGroupIsFull, default 200).
	if int32(len(g.members)) >= g.c.shareMaxGroupSize() {
		resp.ErrorCode = kerr.GroupMaxSizeReached.Code
		return resp
	}

	m := &shareMember{
		memberID:   memberID,
		clientID:   creq.cid,
		clientHost: creq.cc.conn.RemoteAddr().String(),
		rackID:     req.RackID,
		assignment: make(map[uuid][]int32),
		last:       time.Now(),
	}
	if req.SubscribedTopicNames != nil {
		m.subscribedTopics = slices.Clone(req.SubscribedTopicNames)
		slices.Sort(m.subscribedTopics)
	}

	g.members[memberID] = m
	g.groupEpoch++
	g.recomputeAssignments()
	g.reconcileMember(m)
	g.resetSessionTimeout(m)

	resp.MemberID = &memberID
	resp.MemberEpoch = m.memberEpoch
	resp.Assignment = g.makeAssignment(m)
	return resp
}

func (g *shareGroup) handleLeave(req *kmsg.ShareGroupHeartbeatRequest, resp *kmsg.ShareGroupHeartbeatResponse) *kmsg.ShareGroupHeartbeatResponse {
	m := g.members[req.MemberID]
	if m == nil {
		resp.ErrorCode = kerr.UnknownMemberID.Code
		return resp
	}
	if m.t != nil {
		m.t.Stop()
	}
	delete(g.members, req.MemberID)
	g.groupEpoch++
	if len(g.members) > 0 {
		g.recomputeAssignments()
	}

	// Release any records acquired by this member.
	g.releaseRecordsForMember(req.MemberID)

	g.maybeQuit()

	resp.MemberID = &req.MemberID
	resp.MemberEpoch = -1
	return resp
}

func (g *shareGroup) handleRegularHeartbeat(creq *clientReq, req *kmsg.ShareGroupHeartbeatRequest, resp *kmsg.ShareGroupHeartbeatResponse) *kmsg.ShareGroupHeartbeatResponse {
	m := g.members[req.MemberID]
	if m == nil {
		resp.ErrorCode = kerr.UnknownMemberID.Code
		return resp
	}

	// Epoch validation matching Kafka's throwIfShareGroupMemberEpochIsInvalid
	// (GroupMetadataManager.java:1605-1625): accept the current epoch or the
	// previous epoch (in case the response that bumped the epoch was lost).
	if req.MemberEpoch != m.memberEpoch && req.MemberEpoch != m.previousMemberEpoch {
		resp.ErrorCode = kerr.FencedMemberEpoch.Code
		return resp
	}

	m.last = time.Now()
	m.clientID = creq.cid
	m.clientHost = creq.cc.conn.RemoteAddr().String()
	if req.RackID != nil {
		m.rackID = req.RackID
	}
	g.resetSessionTimeout(m)

	// Check if subscription changed.
	if req.SubscribedTopicNames != nil {
		sorted := slices.Clone(req.SubscribedTopicNames)
		slices.Sort(sorted)
		if !slices.Equal(m.subscribedTopics, sorted) {
			m.subscribedTopics = sorted
			g.groupEpoch++
			g.recomputeAssignments()
		}
	}

	// Reconcile: bump this member's epoch to the group epoch if behind.
	// Like Kafka's ShareGroupAssignmentBuilder.build(), the epoch is only
	// advanced during the member's own heartbeat, not when other members
	// join/leave.
	g.reconcileMember(m)

	resp.MemberID = &req.MemberID
	resp.MemberEpoch = m.memberEpoch

	// Only send assignment when it may have changed (matching Java's
	// ShareGroupAssignmentBuilder which only sets the assignment when
	// subscriptions changed or the member epoch was bumped).
	// kgo handles nil Assignment gracefully (skips reconciliation).
	if req.SubscribedTopicNames != nil || m.memberEpoch != m.previousMemberEpoch {
		resp.Assignment = g.makeAssignment(m)
	}
	return resp
}

// recomputeAssignments implements Kafka's SimpleAssignor for share groups
// with stickiness: existing valid assignments are preserved and only the
// minimum changes are made to achieve balance (matching Java's
// SimpleHomogeneousAssignmentBuilder / SimpleHeterogeneousAssignmentBuilder).
//
// Algorithm (per topic):
//  1. Retain assignments for subscribed topics, revoke unsubscribed
//  2. Revoke from overfilled members (too many partitions)
//  3. Revoke overshared partitions (too many members per partition)
//  4. Assign remaining capacity to underfilled members
//
// Like Kafka's ShareGroupAssignmentBuilder, this updates the assignment but
// does NOT bump member epochs. Each member's epoch is only bumped when it
// heartbeats and receives the updated assignment (via reconcileMember).
//
// Must only be called from the manage() goroutine.
func (g *shareGroup) recomputeAssignments() {
	snap := g.lastTopicMeta

	memberIDs := make([]string, 0, len(g.members))
	for id := range g.members {
		memberIDs = append(memberIDs, id)
	}
	slices.Sort(memberIDs)

	// Build topic→subscribers index.
	topicSubs := make(map[string][]string)
	for _, id := range memberIDs {
		for _, topic := range g.members[id].subscribedTopics {
			topicSubs[topic] = append(topicSubs[topic], id)
		}
	}

	// Reverse lookup: topicID → topicName.
	topicForID := make(map[uuid]string)
	for topic, si := range snap {
		topicForID[si.id] = topic
	}

	// Build subscribed set per member.
	subscribedSet := make(map[string]map[string]struct{})
	for _, id := range memberIDs {
		s := make(map[string]struct{})
		for _, t := range g.members[id].subscribedTopics {
			s[t] = struct{}{}
		}
		subscribedSet[id] = s
	}

	type partKey struct {
		topic string
		part  int32
	}

	// Phase 1: Retain valid assignments, revoke invalid ones.
	partMembers := make(map[partKey]map[string]struct{})

	for _, id := range memberIDs {
		m := g.members[id]
		newAssign := make(map[uuid][]int32)
		for tid, parts := range m.assignment {
			topic := topicForID[tid]
			if topic == "" {
				continue // topic deleted
			}
			si := snap[topic]
			if _, ok := subscribedSet[id][topic]; !ok {
				continue // unsubscribed
			}
			var kept []int32
			for _, p := range parts {
				if p >= si.partitions {
					continue // partition removed
				}
				pk := partKey{topic, p}
				if partMembers[pk] == nil {
					partMembers[pk] = make(map[string]struct{})
				}
				partMembers[pk][id] = struct{}{}
				kept = append(kept, p)
			}
			if len(kept) > 0 {
				newAssign[tid] = kept
			}
		}
		m.assignment = newAssign
	}

	// Phase 2: Per-topic rebalancing.
	for topic, subs := range topicSubs {
		si, ok := snap[topic]
		if !ok || si.partitions == 0 {
			continue
		}
		nSubs := len(subs)
		nParts := int(si.partitions)
		desiredSharing := (nSubs + nParts - 1) / nParts
		totalSlots := desiredSharing * nParts

		// Compute per-member desired count for this topic (matching
		// Java's cumulative ceiling formula for fair distribution).
		desiredCount := make(map[string]int, nSubs)
		cum := 0
		for i, id := range subs {
			target := ((i+1)*totalSlots + nSubs - 1) / nSubs
			desiredCount[id] = target - cum
			cum = target
		}

		// Current count per member for this topic.
		memberTopicCount := make(map[string]int, nSubs)
		for _, id := range subs {
			memberTopicCount[id] = len(g.members[id].assignment[si.id])
		}

		// Phase 2a: Revoke from overfilled members.
		for _, id := range subs {
			m := g.members[id]
			parts := m.assignment[si.id]
			desired := desiredCount[id]
			if len(parts) <= desired {
				continue
			}
			removed := parts[desired:]
			if desired > 0 {
				m.assignment[si.id] = parts[:desired]
			} else {
				delete(m.assignment, si.id)
			}
			for _, p := range removed {
				pk := partKey{topic, p}
				delete(partMembers[pk], id)
			}
			memberTopicCount[id] = desired
		}

		// Phase 2b: Revoke overshared partitions. Prefer removing
		// from the most-overfilled members for better balance.
		for p := int32(0); p < si.partitions; p++ {
			pk := partKey{topic, p}
			members := partMembers[pk]
			excess := len(members) - desiredSharing
			if excess <= 0 {
				continue
			}
			// Collect and sort: most overfilled first, then by ID for determinism.
			type candidate struct {
				id    string
				extra int // current - desired
			}
			var cands []candidate
			for id := range members {
				cands = append(cands, candidate{id, memberTopicCount[id] - desiredCount[id]})
			}
			slices.SortFunc(cands, func(a, b candidate) int {
				if a.extra != b.extra {
					return b.extra - a.extra // descending by overfill
				}
				return strings.Compare(a.id, b.id)
			})
			for _, c := range cands {
				if excess <= 0 {
					break
				}
				delete(members, c.id)
				m := g.members[c.id]
				m.assignment[si.id] = removePartition(m.assignment[si.id], p)
				if len(m.assignment[si.id]) == 0 {
					delete(m.assignment, si.id)
				}
				memberTopicCount[c.id]--
				excess--
			}
		}

		// Phase 2c: Assign remaining capacity to underfilled members.
		// Build a list of members needing more partitions.
		type unfilled struct {
			id      string
			desired int
		}
		var uf []unfilled
		for _, id := range subs {
			if memberTopicCount[id] < desiredCount[id] {
				uf = append(uf, unfilled{id, desiredCount[id]})
			}
		}
		if len(uf) == 0 {
			continue
		}

		uidx := 0
		for p := int32(0); p < si.partitions && len(uf) > 0; p++ {
			pk := partKey{topic, p}
			members := partMembers[pk]
			if members == nil {
				members = make(map[string]struct{})
				partMembers[pk] = members
			}
			for len(members) < desiredSharing && len(uf) > 0 {
				start := uidx
				found := false
				for {
					u := &uf[uidx%len(uf)]
					next := (uidx + 1) % len(uf)
					if _, already := members[u.id]; !already {
						members[u.id] = struct{}{}
						m := g.members[u.id]
						m.assignment[si.id] = append(m.assignment[si.id], p)
						memberTopicCount[u.id]++
						if memberTopicCount[u.id] >= u.desired {
							i := uidx % len(uf)
							uf = slices.Delete(uf, i, i+1)
							if len(uf) > 0 {
								uidx = i % len(uf)
							}
						} else {
							uidx = next
						}
						found = true
						break
					}
					uidx = next
					if uidx == start {
						break // all candidates already assigned
					}
				}
				if !found {
					break
				}
			}
		}
	}
}

// removePartition removes partition p from the slice, preserving order.
func removePartition(parts []int32, p int32) []int32 {
	for i, v := range parts {
		if v == p {
			return slices.Delete(parts, i, i+1)
		}
	}
	return parts
}

// reconcileMember bumps the member's epoch to the current group epoch if
// it is behind, mirroring Kafka's ShareGroupAssignmentBuilder.build()
// which only advances a member's epoch during that member's own heartbeat.
func (g *shareGroup) reconcileMember(m *shareMember) {
	if m.memberEpoch != g.groupEpoch {
		m.previousMemberEpoch = m.memberEpoch
		m.memberEpoch = g.groupEpoch
	}
}

func (g *shareGroup) makeAssignment(m *shareMember) *kmsg.ShareGroupHeartbeatResponseAssignment {
	a := new(kmsg.ShareGroupHeartbeatResponseAssignment)
	for tid, parts := range m.assignment {
		tp := kmsg.NewShareGroupHeartbeatResponseAssignmentTopicPartition()
		tp.TopicID = tid
		tp.Partitions = parts
		a.TopicPartitions = append(a.TopicPartitions, tp)
	}
	return a
}

func (g *shareGroup) resetSessionTimeout(m *shareMember) {
	if m.t != nil {
		m.t.Stop()
	}
	timeout := time.Duration(g.c.shareSessionTimeoutMs()) * time.Millisecond
	m.t = time.AfterFunc(timeout, func() {
		select {
		case g.controlCh <- func() {
			g.fenceMember(m.memberID)
		}:
		case <-g.quitCh:
		case <-g.c.die:
		}
	})
}

func (g *shareGroup) fenceMember(memberID string) {
	m := g.members[memberID]
	if m == nil {
		return
	}
	if m.t != nil {
		m.t.Stop()
	}
	delete(g.members, memberID)

	g.releaseRecordsForMember(memberID)
	if len(g.members) > 0 {
		g.groupEpoch++
		g.recomputeAssignments()
	}
	g.maybeQuit()
}

// maybeQuit shuts down the manage goroutine if the group is truly empty:
// no members and no partition state. Only called from manage(), which
// owns g.members. Holds mu while checking partitions AND calling quitOnce
// to prevent run() from creating partition state (via getSharePartition)
// between the check and the quit.
func (g *shareGroup) maybeQuit() {
	if len(g.members) > 0 {
		return
	}
	g.mu.Lock()
	empty := len(g.partitions) == 0
	if empty {
		g.quitOnce()
	}
	g.mu.Unlock()
}

// releaseRecordsForMember releases all records acquired by the given member.
// If a record has hit max delivery count, it is archived instead. If any
// records were released to AVAILABLE, notifies run() via sweepCh so it can
// fire share watchers for waiting consumers.
func (g *shareGroup) releaseRecordsForMember(memberID string) {
	g.mu.Lock()
	released := g.releaseRecordsForMemberLocked(memberID, g.c.shareMaxDeliveryAttempts())
	g.mu.Unlock()
	if released {
		select {
		case g.c.shareGroups.sweepCh <- g:
		default:
		}
	}
}

// releaseRecordsForMemberLocked is the inner loop of releaseRecordsForMember.
// Returns true if any records were released to AVAILABLE (callers may need
// to fire share watchers). Must be called with g.mu held.
func (g *shareGroup) releaseRecordsForMemberLocked(memberID string, maxDelivery int32) bool {
	released := false
	g.partitions.each(func(_ string, _ int32, sp *sharePartition) {
		for offset, sr := range sp.records {
			if sr.state == shareRecordAcquired && sr.acquiredBy == memberID {
				if sr.deliveryCount >= maxDelivery {
					sr.state = shareRecordArchived
				} else {
					sr.state = shareRecordAvailable
					released = true
					if offset < sp.scanOffset {
						sp.scanOffset = offset
					}
				}
				sr.acquiredBy = ""
			}
		}
		sp.advanceSPSO()
	})
	return released
}

// getSharePartition returns the share partition state, initializing it if needed.
func (g *shareGroup) getSharePartition(topic string, partition int32, pd *partData) *sharePartition {
	sp, ok := g.partitions.getp(topic, partition)
	if ok {
		return sp
	}
	// Initialize SPSO based on group config.
	spso := pd.highWatermark // default: latest
	if gc := g.c.groupConfigs[g.name]; gc != nil {
		if v := gc["share.auto.offset.reset"]; v != nil && *v == "earliest" {
			spso = pd.logStartOffset
		}
	}
	sp = g.partitions.mkp(topic, partition, func() *sharePartition {
		return &sharePartition{
			spso:       spso,
			scanOffset: spso,
			records:    make(map[int64]*shareRecord),
		}
	})
	return sp
}

// acquireOpts bundles per-request parameters for acquireRecords that are
// constant across all partitions in a single ShareFetch request.
type acquireOpts struct {
	memberID         string
	maxDeliveryCount int32
	maxRecordLocks   int32
	readCommitted    bool
}

// acquireRecords acquires available records from [scanOffset, hwm) for the
// given member, up to maxRecords. Records that have hit maxDeliveryCount are
// archived instead of acquired. Returns the list of acquired offset ranges
// with delivery counts.
//
// pd is used for compaction-aware gap detection: offsets that fall between
// batches (compacted away) are skipped rather than tracked as phantom records.
//
// When readCommitted is true, offsets belonging to aborted transactions are
// archived immediately (matching Java's SharePartition.acquire filtering).
func (sp *sharePartition) acquireRecords(pd *partData, hwm int64, maxRecords int32, opts *acquireOpts) (acquired []kmsg.ShareFetchResponseTopicPartitionAcquiredRecord, blocked bool) {
	var count int32

	if sp.scanOffset < sp.spso {
		sp.scanOffset = sp.spso
	}

	// In-flight limit (matching Kafka's lastOffsetAndMaxRecordsToAcquire).
	// The in-flight window is [spso, endOffset). When at capacity, records
	// within the existing window can still be re-acquired (e.g., released
	// records near SPSO) because they don't extend the window. Only
	// acquisition of records BEYOND endOffset is blocked. Kafka handles
	// this with: if maxRecordsToAcquire <= 0 && fetchOffset <= endOffset,
	// recalculate as min(maxFetch, endOffset - fetchOffset + 1).
	acquireLimit := hwm // default: scan up to HWM
	if opts.maxRecordLocks > 0 && sp.endOffset > sp.spso {
		windowSize := int32(sp.endOffset - sp.spso)
		if windowSize >= opts.maxRecordLocks {
			// At capacity: only allow re-acquisition within the
			// existing window (up to endOffset), not beyond.
			if sp.scanOffset < sp.endOffset {
				acquireLimit = sp.endOffset
			} else {
				return nil, true
			}
		}
	}
	if acquireLimit > hwm {
		acquireLimit = hwm
	}

	// Find starting batch position for gap detection.
	curSeg, curMeta, hasBatch, atEnd := pd.searchOffset(sp.scanOffset)
	if atEnd {
		hasBatch = false
	}

	lastScanned := sp.scanOffset

	// Walk offsets from scanOffset to HWM looking for available records.
	for offset := sp.scanOffset; offset < acquireLimit && count < maxRecords; offset++ {
		// In-flight limit per iteration: stop if extending beyond endOffset
		// while at capacity (records within the window are always ok).
		// This check MUST come before advancing lastScanned, otherwise
		// scanOffset jumps past this offset without creating a record
		// for it. Later, advanceSPSO would skip the nil entry as a
		// "compacted gap", silently losing the record.
		if opts.maxRecordLocks > 0 && offset >= sp.endOffset && sp.endOffset > sp.spso && int32(sp.endOffset-sp.spso) >= opts.maxRecordLocks {
			break
		}
		lastScanned = offset + 1

		// Gap detection: skip offsets between batches (compacted away).
		if hasBatch {
			curBatch := &pd.segments[curSeg].index[curMeta]
			if offset > curBatch.firstOffset+int64(curBatch.lastOffsetDelta) {
				// Past current batch, advance to next.
				curMeta++
				for curMeta >= len(pd.segments[curSeg].index) {
					curSeg++
					curMeta = 0
					if curSeg >= len(pd.segments) {
						hasBatch = false
						break
					}
				}
				if hasBatch {
					curBatch = &pd.segments[curSeg].index[curMeta]
				}
			}
			if hasBatch && offset < curBatch.firstOffset {
				// Offset is in a gap between batches. Jump past it.
				lastScanned = curBatch.firstOffset
				offset = curBatch.firstOffset - 1 // -1 because for loop increments
				continue
			}
		}

		// READ_COMMITTED: check if this offset belongs to an aborted
		// transaction. If so, archive it immediately (matching Java's
		// SharePartition.maybeFilterAbortedTransactionalAcquiredRecords).
		// We piggyback on the batch cursor to get the producerID.
		if opts.readCommitted && hasBatch {
			curBatch := &pd.segments[curSeg].index[curMeta]
			if offset >= curBatch.firstOffset && offset <= curBatch.firstOffset+int64(curBatch.lastOffsetDelta) {
				if curBatch.inTx && pd.isOffsetAborted(curBatch.firstOffset, curBatch.producerID) {
					sr := sp.records[offset]
					if sr == nil {
						sr = &shareRecord{}
						sp.records[offset] = sr
					}
					sr.state = shareRecordArchived
					sr.acquiredBy = ""
					continue
				}
			}
		}

		sr := sp.records[offset]
		if sr == nil {
			// No state yet -- new record, available.
			sr = &shareRecord{
				state:         shareRecordAvailable,
				deliveryCount: 0,
			}
			sp.records[offset] = sr
		}
		if sr.state != shareRecordAvailable {
			continue
		}

		// If this record has been delivered too many times, archive it
		// rather than delivering again.
		if sr.deliveryCount >= opts.maxDeliveryCount {
			sr.state = shareRecordArchived
			sr.acquiredBy = ""
			continue
		}

		// Acquire.
		sr.state = shareRecordAcquired
		sr.acquiredBy = opts.memberID
		sr.deliveryCount++
		sr.acquireTime = time.Now()
		count++
		if offset+1 > sp.endOffset {
			sp.endOffset = offset + 1
		}

		// Try to extend the last AcquiredRecord range.
		if n := len(acquired); n > 0 {
			last := &acquired[n-1]
			if last.LastOffset == offset-1 && last.DeliveryCount == int16(sr.deliveryCount) {
				last.LastOffset = offset
				continue
			}
		}
		ar := kmsg.NewShareFetchResponseTopicPartitionAcquiredRecord()
		ar.FirstOffset = offset
		ar.LastOffset = offset
		ar.DeliveryCount = int16(sr.deliveryCount)
		acquired = append(acquired, ar)
	}

	// Always advance scanOffset to reflect how far we scanned,
	// even when nothing was acquired. This lets advanceSPSO
	// detect compacted gaps (nil entries below scanOffset).
	if lastScanned > sp.scanOffset {
		sp.scanOffset = lastScanned
	}

	// After acquiring, advance SPSO in case we archived some records
	// at the front.
	sp.advanceSPSO()

	return acquired, false
}

// validateAcks checks that all offsets in [first, last] are valid for acking
// by memberID without applying any state changes. Returns 0 if valid, error
// code otherwise. Used for atomic validate-then-apply across multiple batches
// (matching Kafka's rollbackOrProcessStateUpdates pattern: validate all, then
// apply all, rollback everything on any error).
//
// Error code distinction matches Java's SharePartition.acknowledge:
//   - INVALID_REQUEST: batch extends beyond tracked/cached records
//   - INVALID_RECORD_STATE: record exists but wrong state or wrong owner
func (sp *sharePartition) validateAcks(memberID string, first, last int64) int16 {
	// Check if the batch extends beyond tracked records (matching Java's
	// "The first/last offset in request is past acquired records" check).
	if sp.endOffset > sp.spso {
		if first >= sp.endOffset || last >= sp.endOffset {
			return kerr.InvalidRequest.Code
		}
	} else if last >= sp.spso {
		return kerr.InvalidRequest.Code
	}

	for offset := first; offset <= last; offset++ {
		if offset < sp.spso {
			continue
		}
		sr := sp.records[offset]
		if sr == nil {
			return kerr.InvalidRecordState.Code
		}
		if sr.state != shareRecordAcquired {
			return kerr.InvalidRecordState.Code
		}
		if sr.acquiredBy != memberID {
			return kerr.InvalidRecordState.Code
		}
	}
	return 0
}

// processAcks applies ack types to the offset range [first, last].
//
// AcknowledgeTypes can be:
//   - empty or single element: uniform type for the whole range
//   - per-offset: one element per offset in the range
//
// Types: 0=Gap (skip), 1=Accept, 2=Release, 3=Reject, 4=Renew.
// Offsets below SPSO are silently skipped (already finalized).
//
// maxDeliveryCount is checked on RELEASE: if the record has already been
// delivered maxDeliveryCount times, it is archived instead of released
// (matching Kafka's InFlightState.tryUpdateState check on AVAILABLE
// transition).
//
// Must be called only after validateAcks succeeds for all batches in the
// request. processAcks assumes records are in valid state (ACQUIRED by
// memberID) and skips any that aren't (defensive, shouldn't happen after
// validation).
func (sp *sharePartition) processAcks(memberID string, first, last int64, ackTypes []int8, maxDeliveryCount int32) {
	perOffset := len(ackTypes) > 1
	uniformType := int8(1) // default: accept
	if len(ackTypes) == 1 {
		uniformType = ackTypes[0]
	}
	for offset := first; offset <= last; offset++ {
		if offset < sp.spso {
			continue // already finalized
		}
		sr := sp.records[offset]
		if sr == nil {
			continue // compacted or never tracked
		}
		if sr.state != shareRecordAcquired {
			continue // already released/archived/acknowledged
		}
		if sr.acquiredBy != memberID {
			continue // acquired by someone else (e.g., after sweep)
		}
		ackType := uniformType
		if perOffset {
			idx := int(offset - first)
			if idx < len(ackTypes) {
				ackType = ackTypes[idx]
			}
		}
		switch ackType {
		case 0: // Gap: archive (Kafka maps gap to ARCHIVED)
			sr.state = shareRecordArchived
			sr.acquiredBy = ""
		case 1: // Accept
			sr.state = shareRecordAcknowledged
			sr.acquiredBy = ""
		case 2: // Release
			// Like Kafka's InFlightState.tryUpdateState: if
			// deliveryCount >= maxDeliveryCount, redirect to
			// ARCHIVED instead of AVAILABLE.
			if sr.deliveryCount >= maxDeliveryCount {
				sr.state = shareRecordArchived
			} else {
				sr.state = shareRecordAvailable
				if offset < sp.scanOffset {
					sp.scanOffset = offset
				}
			}
			sr.acquiredBy = ""
		case 3: // Reject
			sr.state = shareRecordArchived
			sr.acquiredBy = ""
		case 4: // Renew
			sr.acquireTime = time.Now()
		}
	}
}

// advanceSPSO advances the SPSO past contiguous acknowledged/archived
// records, cleaning up their state entries. Also skips compacted gaps:
// if records[spso] is nil but we've already scanned past that offset
// (spso < scanOffset), the nil means the offset was compacted away.
func (sp *sharePartition) advanceSPSO() {
	for {
		sr := sp.records[sp.spso]
		if sr == nil {
			// If we've scanned past this offset and there's no
			// record, it's a compacted gap — skip it.
			if sp.spso < sp.scanOffset {
				sp.spso++
				continue
			}
			break
		}
		if sr.state != shareRecordAcknowledged && sr.state != shareRecordArchived {
			break
		}
		delete(sp.records, sp.spso)
		sp.spso++
	}
}

// cleanupShareSessionsForConn removes all share sessions owned by the given
// connection and releases their acquired records. This matches Kafka's
// ConnectionDisconnectListener behavior: when a TCP connection drops, all
// sessions on that connection are removed and acquired records are released.
// Must only be called from run().
func (c *Cluster) cleanupShareSessionsForConn(cc *clientConn) {
	maxDelivery := c.shareMaxDeliveryAttempts()
	for key, session := range c.shareSessions {
		if session.cc != cc {
			continue
		}
		delete(c.shareSessions, key)
		sg := c.shareGroups.gs[key.group]
		if sg == nil {
			continue
		}
		sg.mu.Lock()
		released := sg.releaseRecordsForMemberLocked(key.memberID, maxDelivery)
		sg.mu.Unlock()
		if released {
			sg.fireAllShareWatchers(c)
		}
	}
}

// fireShareWatchers wakes any pending ShareFetch watchers on the given
// partition. Called after acks that may have released records.
// Must only be called from run().
func fireShareWatchers(pd *partData) {
	for w := range pd.shareWatch {
		w.do()
	}
}

// fireAllShareWatchers wakes share watchers on all partitions tracked by
// this share group. Called from run() when the manage goroutine's sweep
// timer releases records.
func (sg *shareGroup) fireAllShareWatchers(c *Cluster) {
	sg.mu.Lock()
	topics := make([]string, 0, len(sg.partitions))
	partsByTopic := make(map[string][]int32, len(sg.partitions))
	for topic, ps := range sg.partitions {
		topics = append(topics, topic)
		for p := range ps {
			partsByTopic[topic] = append(partsByTopic[topic], p)
		}
	}
	sg.mu.Unlock()
	for _, topic := range topics {
		for _, p := range partsByTopic[topic] {
			pd, ok := c.data.tps.getp(topic, p)
			if ok {
				fireShareWatchers(pd)
			}
		}
	}
}
