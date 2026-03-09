package kfake

import (
	"slices"
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

const (
	// Default sweep interval for expired acquisition locks.
	defaultShareAcqLockSweepInterval = 5 * time.Second
)

type shareGroups struct {
	c  *Cluster
	gs map[string]*shareGroup
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

	memberEpoch      int32
	subscribedTopics []string

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
type sharePartition struct {
	spso    int64 // Share-Partition Start Offset: first unfinalized offset
	records map[int64]*shareRecord
}

// shareSessionKey identifies a share session.
type shareSessionKey struct {
	group    string
	memberID string
	broker   int32
}

type shareSession struct {
	epoch int32
}

type shareSessions struct {
	sessions map[shareSessionKey]*shareSession
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
// archived instead of released.
func (g *shareGroup) sweepExpiredAcquisitions() {
	g.mu.Lock()
	defer g.mu.Unlock()
	now := time.Now()
	g.partitions.each(func(_ string, _ int32, sp *sharePartition) {
		for _, sr := range sp.records {
			if sr.state != shareRecordAcquired {
				continue
			}
			if now.Sub(sr.acquireTime) < time.Duration(g.c.shareRecordLockDurationMs())*time.Millisecond {
				continue
			}
			if sr.deliveryCount >= g.c.shareMaxDeliveryAttempts() {
				sr.state = shareRecordArchived
			} else {
				sr.state = shareRecordAvailable
			}
			sr.acquiredBy = ""
		}
		sp.advanceSPSO()
	})
}

// waitControl sends fn to the manage loop's controlCh and blocks until
// it completes. Used for external callers (persistence, shutdown) that
// need safe access to share group state.
func (g *shareGroup) waitControl(fn func()) bool {
	wait := make(chan struct{})
	wfn := func() { fn(); close(wait) }
	for {
		select {
		case <-g.quitCh:
			return false
		case <-g.c.die:
			return false
		case g.controlCh <- wfn:
			goto sent
		case admin := <-g.c.adminCh:
			admin()
		}
	}
sent:
	// Once sent, the manage goroutine will run fn synchronously.
	// We must not select on quitCh here: fn itself may call
	// quitOnce (e.g. deleting an empty group), closing quitCh
	// before close(wait) executes. If the scheduler preempts
	// between the two closes, a quitCh select case would see
	// quitCh ready but wait not yet closed and incorrectly
	// return false.
	for {
		select {
		case <-wait:
			return true
		case <-g.c.die:
			return false
		case admin := <-g.c.adminCh:
			admin()
		}
	}
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
	resp.HeartbeatIntervalMillis = 5000

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
	if memberID == "" {
		memberID = generateMemberID(creq.cid, nil)
	}

	// If existing member, treat as rejoin.
	if m := g.members[memberID]; m != nil {
		m.subscribedTopics = slices.Clone(req.SubscribedTopicNames)
		slices.Sort(m.subscribedTopics)
		m.last = time.Now()
		g.resetSessionTimeout(m)
		g.groupEpoch++
		g.recomputeAssignments()
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
	if req.MemberEpoch != m.memberEpoch {
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
}

// releaseRecordsForMember releases all records acquired by the given member.
// If a record has hit max delivery count, it is archived instead.
func (g *shareGroup) releaseRecordsForMember(memberID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.partitions.each(func(_ string, _ int32, sp *sharePartition) {
		for _, sr := range sp.records {
			if sr.state == shareRecordAcquired && sr.acquiredBy == memberID {
				if sr.deliveryCount >= g.c.shareMaxDeliveryAttempts() {
					sr.state = shareRecordArchived
				} else {
					sr.state = shareRecordAvailable
				}
				sr.acquiredBy = ""
			}
		}
		sp.advanceSPSO()
	})
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

// acquireRecords acquires available records from [spso, hwm) for the given member,
// up to maxRecords. Records that have hit maxDeliveryCount are archived instead
// of acquired. Returns the list of acquired offset ranges with delivery counts.
func (sp *sharePartition) acquireRecords(memberID string, hwm int64, maxRecords int32, maxDeliveryCount int32) []kmsg.ShareFetchResponseTopicPartitionAcquiredRecord {
	if maxRecords <= 0 {
		maxRecords = 500
	}
	var acquired []kmsg.ShareFetchResponseTopicPartitionAcquiredRecord
	var count int32

	// Walk offsets from SPSO to HWM looking for available records.
	for offset := sp.spso; offset < hwm && count < maxRecords; offset++ {
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

	// After acquiring, advance SPSO in case we archived some records
	// at the front.
	sp.advanceSPSO()

	return acquired
}

// processAcks processes piggybacked acknowledgement batches from ShareFetch.
func (sp *sharePartition) processAcks(batches []kmsg.ShareFetchRequestTopicPartitionAcknowledgementBatche) int16 {
	for _, batch := range batches {
		sp.applyAcks(batch.FirstOffset, batch.LastOffset, batch.AcknowledgeTypes)
	}
	sp.advanceSPSO()
	return 0
}

// processStandaloneAcks processes acknowledgement batches from ShareAcknowledge.
func (sp *sharePartition) processStandaloneAcks(batches []kmsg.ShareAcknowledgeRequestTopicPartitionAcknowledgementBatche) int16 {
	for _, batch := range batches {
		sp.applyAcks(batch.FirstOffset, batch.LastOffset, batch.AcknowledgeTypes)
	}
	sp.advanceSPSO()
	return 0
}

// applyAcks applies ack types to the offset range [first, last].
//
// AcknowledgeTypes can be:
//   - empty or single element: uniform type for the whole range
//   - per-offset: one element per offset in the range
//
// Types: 0=Gap (skip), 1=Accept, 2=Release, 3=Reject, 4=Renew.
// Offsets below SPSO are silently skipped (already finalized).
// Non-acquired records are silently skipped (already acked or not yet acquired).
func (sp *sharePartition) applyAcks(first, last int64, ackTypes []int8) {
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
		if sr == nil || sr.state != shareRecordAcquired {
			continue
		}
		ackType := uniformType
		if perOffset {
			idx := int(offset - first)
			if idx < len(ackTypes) {
				ackType = ackTypes[idx]
			}
		}
		switch ackType {
		case 0: // Gap -- no ack for this offset, skip
		case 1: // Accept
			sr.state = shareRecordAcknowledged
			sr.acquiredBy = ""
		case 2: // Release
			sr.state = shareRecordAvailable
			sr.acquiredBy = ""
		case 3: // Reject
			sr.state = shareRecordArchived
			sr.acquiredBy = ""
		case 4: // Renew
			sr.acquireTime = time.Now()
		}
	}
}

// advanceSPSO advances the SPSO past contiguous acknowledged/archived records,
// cleaning up their state entries.
func (sp *sharePartition) advanceSPSO() {
	for {
		sr := sp.records[sp.spso]
		if sr == nil {
			break
		}
		if sr.state != shareRecordAcknowledged && sr.state != shareRecordArchived {
			break
		}
		delete(sp.records, sp.spso)
		sp.spso++
	}
}
