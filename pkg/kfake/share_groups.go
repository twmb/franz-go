package kfake

import (
	"slices"
	"sync"
	"sync/atomic"
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

	// memberIDs is an atomically published snapshot of member keys.
	//
	// Why atomic: members is owned by the manage() goroutine, but
	// ShareFetch/ShareAcknowledge run in the cluster's run() goroutine
	// and need to validate that a memberID is known before acquiring
	// records. We can't use sg.mu (which protects partitions, not
	// members), and waitControl would block run() on every fetch.
	//
	// Safety: manage() creates a new immutable map on each membership
	// change and stores the pointer atomically. run() loads the pointer
	// and reads the map -- no concurrent writes to the same map
	// instance. The check is best-effort: a member could be fenced
	// between the check and record acquisition, but the per-record
	// acquiredBy field provides the actual safety guarantee.
	memberIDs atomic.Pointer[map[string]struct{}]

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
	spso    int64 // Share-Partition Start Offset: first unfinalized offset
	records map[int64]*shareRecord

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
	partitions map[uuid]map[int32]struct{} // topicID -> partition set
	cc         *clientConn                 // owning connection, for disconnect cleanup
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
		return g.handleRegularHeartbeat(req, resp)
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
	g.publishMemberIDs()

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
	g.publishMemberIDs()

	// Release any records acquired by this member.
	g.releaseRecordsForMember(req.MemberID)

	g.maybeQuit()

	resp.MemberID = &req.MemberID
	resp.MemberEpoch = -1
	return resp
}

func (g *shareGroup) handleRegularHeartbeat(req *kmsg.ShareGroupHeartbeatRequest, resp *kmsg.ShareGroupHeartbeatResponse) *kmsg.ShareGroupHeartbeatResponse {
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

	// Always send assignment so the client stays in sync.
	resp.Assignment = g.makeAssignment(m)
	return resp
}

// recomputeAssignments computes target assignments for all members.
// In share groups, all members that subscribe to a topic get all partitions
// of that topic -- there is no exclusive partitioning.
//
// Like Kafka's ShareGroupAssignmentBuilder, this updates the assignment but
// does NOT bump member epochs. Each member's epoch is only bumped when it
// heartbeats and receives the updated assignment (via reconcileMember).
//
// Must only be called from the manage() goroutine. Uses lastTopicMeta
// (set from creq.topicMeta or notifyTopicChange) to avoid reading c.data
// from manage(), which would race with run() mutating the maps.
func (g *shareGroup) recomputeAssignments() {
	snap := g.lastTopicMeta
	for _, m := range g.members {
		newAssign := make(map[uuid][]int32)
		for _, topic := range m.subscribedTopics {
			si, ok := snap[topic]
			if !ok {
				continue
			}
			parts := make([]int32, si.partitions)
			for i := range parts {
				parts[i] = int32(i)
			}
			newAssign[si.id] = parts
		}
		m.assignment = newAssign
	}
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

// publishMemberIDs atomically publishes the current member ID set so
// that run() can validate ShareFetch memberIDs without blocking.
func (g *shareGroup) publishMemberIDs() {
	snap := make(map[string]struct{}, len(g.members))
	for id := range g.members {
		snap[id] = struct{}{}
	}
	g.memberIDs.Store(&snap)
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
	g.publishMemberIDs()
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
// If a record has hit max delivery count, it is archived instead.
func (g *shareGroup) releaseRecordsForMember(memberID string) {
	g.mu.Lock()
	g.releaseRecordsForMemberLocked(memberID, g.c.shareMaxDeliveryAttempts())
	g.mu.Unlock()
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
			spso:    spso,
			records: make(map[int64]*shareRecord),
		}
	})
	return sp
}

// acquireRecords acquires available records from [scanOffset, hwm) for the
// given member, up to maxRecords. Records that have hit maxDeliveryCount are
// archived instead of acquired. Returns the list of acquired offset ranges
// with delivery counts.
//
// pd is used for compaction-aware gap detection: offsets that fall between
// batches (compacted away) are skipped rather than tracked as phantom records.
// maxRecordLocks limits the total tracked records per partition (matching
// Kafka's group.share.partition.max.record.locks).
func (sp *sharePartition) acquireRecords(memberID string, pd *partData, hwm int64, maxRecords int32, maxDeliveryCount int32, maxRecordLocks int32) []kmsg.ShareFetchResponseTopicPartitionAcquiredRecord {
	var acquired []kmsg.ShareFetchResponseTopicPartitionAcquiredRecord
	var count int32

	if sp.scanOffset < sp.spso {
		sp.scanOffset = sp.spso
	}

	// Check in-flight limit before starting.
	if maxRecordLocks > 0 && int32(len(sp.records)) >= maxRecordLocks {
		return nil
	}

	// Find starting batch position for gap detection.
	curSeg, curMeta, hasBatch, atEnd := pd.searchOffset(sp.scanOffset)
	if atEnd {
		hasBatch = false
	}

	lastScanned := sp.scanOffset

	// Walk offsets from scanOffset to HWM looking for available records.
	for offset := sp.scanOffset; offset < hwm && count < maxRecords; offset++ {
		lastScanned = offset + 1

		// In-flight limit per iteration.
		if maxRecordLocks > 0 && int32(len(sp.records)) >= maxRecordLocks {
			break
		}

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
		if sr.deliveryCount >= maxDeliveryCount {
			sr.state = shareRecordArchived
			sr.acquiredBy = ""
			continue
		}

		// Acquire.
		sr.state = shareRecordAcquired
		sr.acquiredBy = memberID
		sr.deliveryCount++
		sr.acquireTime = time.Now()
		count++

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

	return acquired
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
// Returns 0 on success. Returns INVALID_RECORD_STATE if an offset is not
// in the ACQUIRED state or is not owned by memberID (matching Kafka's
// SharePartition.acknowledge behavior).
func (sp *sharePartition) processAcks(memberID string, first, last int64, ackTypes []int8, maxDeliveryCount int32) int16 {
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
			return kerr.InvalidRecordState.Code
		}
		if sr.state != shareRecordAcquired {
			return kerr.InvalidRecordState.Code
		}
		if sr.acquiredBy != memberID {
			return kerr.InvalidRecordState.Code
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
	return 0
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
