package kfake

import (
	"bytes"
	"cmp"
	"fmt"
	"math"
	"regexp"
	"slices"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TODO instance IDs
// TODO persisting groups so commits can happen to client-managed groups
//      we need lastCommit, and need to better prune empty groups

type (
	groups struct {
		c  *Cluster
		gs map[string]*group
	}

	group struct {
		c    *Cluster
		gs   *groups
		name string
		typ  string

		state groupState

		leader  string
		members map[string]*groupMember
		pending map[string]*groupMember

		commits tps[offsetCommit]

		generation   int32
		protocolType string
		protocols    map[string]int
		protocol     string

		reqCh     chan *clientReq
		controlCh chan func()

		nJoining int

		tRebalance *time.Timer

		// KIP-848 consumer group fields
		assignorName           string
		consumerMembers        map[string]*consumerMember
		partitionEpochs        map[uuid]map[int32]int32 // (topicID, partition) -> owning member's epoch; -1 or absent means free
		targetAssignmentsStale bool                      // session timeout cannot snapshot metadata, defers to next heartbeat
		lastTopicMeta          topicMetaSnap // last snapshot received, for deferred recomputation

		quit   sync.Once
		quitCh chan struct{}
	}

	groupMember struct {
		memberID   string
		clientID   string
		clientHost string

		join *kmsg.JoinGroupRequest // the latest join request

		// waitingReply is non-nil if a client is waiting for a reply
		// from us for a JoinGroupRequest or a SyncGroupRequest.
		waitingReply *clientReq

		assignment []byte

		t    *time.Timer
		last time.Time
	}

	consumerMember struct {
		memberID   string
		clientID   string
		clientHost string
		instanceID *string
		rackID     *string

		memberEpoch         int32 // confirmed epoch
		previousMemberEpoch int32 // epoch before last advance
		assignmentEpoch     int32 // epoch at which the current reconciled assignment was established (KIP-1251)

		rebalanceTimeoutMs int32
		serverAssignor     string

		subscribedTopics     []string
		subscribedTopicRegex *regexp.Regexp

		currentAssignment  map[uuid][]int32 // what member reports owning
		targetAssignment   map[uuid][]int32 // what server wants member to own
		lastSentAssignment map[uuid][]int32 // superset of sent partitions; only pruned when member confirms release
		lastReconciledSent map[uuid][]int32 // exact reconciled assignment last included in a response

		t    *time.Timer // session timeout: fences member if no heartbeats
		last time.Time

		tRebal *time.Timer // rebalance timeout: fences member if slow to revoke
	}

	offsetCommit struct {
		offset      int64
		leaderEpoch int32
		metadata    *string
	}

	// topicMetaSnap is a snapshot of topic metadata taken in Cluster.run
	// and passed to group.manage for server-side assignment.
	topicMetaSnap = map[string]topicSnapInfo

	topicSnapInfo struct {
		id         uuid
		partitions int32
	}

	groupState int8
)

const (
	groupEmpty groupState = iota
	groupStable
	groupPreparingRebalance
	groupCompletingRebalance
	groupDead
	groupReconciling
)

func (gs groupState) String() string {
	switch gs {
	case groupEmpty:
		return "Empty"
	case groupStable:
		return "Stable"
	case groupPreparingRebalance:
		return "PreparingRebalance"
	case groupCompletingRebalance:
		return "CompletingRebalance"
	case groupDead:
		return "Dead"
	case groupReconciling:
		return "Reconciling"
	default:
		return "Unknown"
	}
}

// emptyConsumerAssignment is a pre-serialized empty ConsumerMemberAssignment.
// Used to ensure followers in a "consumer" protocol group always receive a
// syntactically valid assignment blob even when the leader assigns them nothing.
// Technically, the broker should pass through whatever bytes the leader sends
// (including empty), but some clients fail to decode an empty assignment.
// We only apply this workaround for the "consumer" protocol type since other
// protocol types may use entirely different assignment formats.
var emptyConsumerAssignment = func() []byte {
	var assignment kmsg.ConsumerMemberAssignment
	return assignment.AppendTo(nil)
}()

func (c *Cluster) coordinator(id string) *broker {
	gen := c.coordinatorGen.Load()
	n := hashString(fmt.Sprintf("%d", gen)+"\x00\x00"+id) % uint64(len(c.bs))
	return c.bs[n]
}

func (c *Cluster) snapshotTopicMeta() topicMetaSnap {
	snap := make(topicMetaSnap, len(c.data.tps))
	for topic, ps := range c.data.tps {
		snap[topic] = topicSnapInfo{id: c.data.t2id[topic], partitions: int32(len(ps))}
	}
	return snap
}

// notifyTopicChange marks all 848 consumer groups as needing
// recomputation after a topic is created, deleted, or has partitions
// added. We notify all groups rather than filtering by subscription
// because group member state is owned by the group's manage goroutine
// and cannot be read safely from here.
func (c *Cluster) notifyTopicChange() {
	for _, g := range c.groups.gs {
		select {
		case g.controlCh <- func() {
			if len(g.consumerMembers) > 0 {
				g.targetAssignmentsStale = true
				g.updateConsumerStateField()
			}
		}:
		default:
			// Unbuffered channel with no receiver waiting - the
			// next heartbeat will pick up fresh metadata anyway.
		}
	}
}

func (c *Cluster) validateGroup(creq *clientReq, group string) *kerr.Error {
	switch key := kmsg.Key(creq.kreq.Key()); key {
	case kmsg.OffsetCommit, kmsg.OffsetFetch, kmsg.DescribeGroups, kmsg.DeleteGroups, kmsg.ConsumerGroupDescribe:
	default:
		if group == "" {
			return kerr.InvalidGroupID
		}
	}
	coordinator := c.coordinator(group).node
	if coordinator != creq.cc.b.node {
		return kerr.NotCoordinator
	}
	return nil
}

func generateMemberID(clientID string, instanceID *string) string {
	if instanceID == nil {
		return clientID + "-" + randStrUUID()
	}
	return *instanceID + "-" + randStrUUID()
}

////////////
// GROUPS //
////////////

func (gs *groups) newGroup(name string) *group {
	return &group{
		c:         gs.c,
		gs:        gs,
		name:      name,
		typ:       "classic", // group-coordinator/src/main/java/org/apache/kafka/coordinator/group/Group.java
		members:   make(map[string]*groupMember),
		pending:   make(map[string]*groupMember),
		protocols: make(map[string]int),
		reqCh:     make(chan *clientReq),
		controlCh: make(chan func(), 10), // Buffered to prevent blocking on send
		quitCh:    make(chan struct{}),
	}
}

// handleJoin completely hijacks the incoming request.
func (gs *groups) handleJoin(creq *clientReq) {
	if gs.gs == nil {
		gs.gs = make(map[string]*group)
	}
	req := creq.kreq.(*kmsg.JoinGroupRequest)
start:
	g := gs.gs[req.Group]
	if g == nil {
		g = gs.newGroup(req.Group)
		waitJoin := make(chan struct{})
		gs.gs[req.Group] = g
		go g.manage(func() { close(waitJoin) })
		defer func() { <-waitJoin }()
	}
	select {
	case g.reqCh <- creq:
	case <-g.quitCh:
		goto start
	case <-g.c.die:
	}
}

// Returns true if the request is hijacked and handled, otherwise false if the
// group does not exist.
func (gs *groups) handleHijack(group string, creq *clientReq) bool {
	if gs.gs == nil {
		return false
	}
	g := gs.gs[group]
	if g == nil {
		return false
	}
	select {
	case g.reqCh <- creq:
		return true
	case <-g.quitCh:
		return false
	case <-g.c.die:
		return false
	}
}

func (gs *groups) handleSync(creq *clientReq) bool {
	return gs.handleHijack(creq.kreq.(*kmsg.SyncGroupRequest).Group, creq)
}

func (gs *groups) handleHeartbeat(creq *clientReq) bool {
	return gs.handleHijack(creq.kreq.(*kmsg.HeartbeatRequest).Group, creq)
}

func (gs *groups) handleLeave(creq *clientReq) bool {
	return gs.handleHijack(creq.kreq.(*kmsg.LeaveGroupRequest).Group, creq)
}

func (gs *groups) handleOffsetCommit(creq *clientReq) {
	if gs.gs == nil {
		gs.gs = make(map[string]*group)
	}
	req := creq.kreq.(*kmsg.OffsetCommitRequest)
start:
	g := gs.gs[req.Group]
	if g == nil {
		g = gs.newGroup(req.Group)
		waitCommit := make(chan struct{})
		gs.gs[req.Group] = g
		go g.manage(func() { close(waitCommit) })
		defer func() { <-waitCommit }()
	}
	select {
	case g.reqCh <- creq:
	case <-g.quitCh:
		goto start
	case <-g.c.die:
	}
}

func (gs *groups) handleOffsetDelete(creq *clientReq) bool {
	return gs.handleHijack(creq.kreq.(*kmsg.OffsetDeleteRequest).Group, creq)
}

func (gs *groups) handleList(creq *clientReq) *kmsg.ListGroupsResponse {
	req := creq.kreq.(*kmsg.ListGroupsRequest)
	resp := req.ResponseKind().(*kmsg.ListGroupsResponse)

	var states map[string]struct{}
	if len(req.StatesFilter) > 0 {
		states = make(map[string]struct{})
		for _, state := range req.StatesFilter {
			states[state] = struct{}{}
		}
	}

	var types map[string]struct{}
	if len(req.TypesFilter) > 0 {
		types = make(map[string]struct{})
		for _, typ := range req.TypesFilter {
			types[typ] = struct{}{}
		}
	}

	for _, g := range gs.gs {
		if g.c.coordinator(g.name).node != creq.cc.b.node {
			continue
		}
		// ACL check: DESCRIBE on Group - filter out groups without permission
		if !g.c.allowedACL(creq, g.name, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationDescribe) {
			continue
		}
		g.waitControl(func() {
			if states != nil {
				if _, ok := states[g.state.String()]; !ok {
					return
				}
			}
			if types != nil {
				if _, ok := types[g.typ]; !ok {
					return
				}
			}
			sg := kmsg.NewListGroupsResponseGroup()
			sg.Group = g.name
			sg.ProtocolType = g.protocolType
			sg.GroupState = g.state.String()
			sg.GroupType = g.typ
			resp.Groups = append(resp.Groups, sg)
		})
	}
	return resp
}

func (gs *groups) handleDescribe(creq *clientReq) *kmsg.DescribeGroupsResponse {
	req := creq.kreq.(*kmsg.DescribeGroupsRequest)
	resp := req.ResponseKind().(*kmsg.DescribeGroupsResponse)

	doneg := func(name string) *kmsg.DescribeGroupsResponseGroup {
		sg := kmsg.NewDescribeGroupsResponseGroup()
		sg.Group = name
		resp.Groups = append(resp.Groups, sg)
		return &resp.Groups[len(resp.Groups)-1]
	}

	for _, rg := range req.Groups {
		sg := doneg(rg)
		// ACL check: DESCRIBE on Group
		if !gs.c.allowedACL(creq, rg, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationDescribe) {
			sg.ErrorCode = kerr.GroupAuthorizationFailed.Code
			continue
		}
		if kerr := gs.c.validateGroup(creq, rg); kerr != nil {
			sg.ErrorCode = kerr.Code
			continue
		}
		g, ok := gs.gs[rg]
		if !ok {
			sg.State = groupDead.String()
			if req.Version >= 6 {
				sg.ErrorCode = kerr.GroupIDNotFound.Code
			}
			if req.IncludeAuthorizedOperations {
				sg.AuthorizedOperations = gs.c.groupAuthorizedOps(creq, rg)
			}
			continue
		}
		if !g.waitControl(func() {
			sg.State = g.state.String()
			sg.ProtocolType = g.protocolType
			if g.state == groupStable {
				sg.Protocol = g.protocol
			}
			for _, m := range g.members {
				sm := kmsg.NewDescribeGroupsResponseGroupMember()
				sm.MemberID = m.memberID
				sm.ClientID = m.clientID
				sm.ClientHost = m.clientHost
				if g.state == groupStable {
					for _, p := range m.join.Protocols {
						if p.Name == g.protocol {
							sm.ProtocolMetadata = p.Metadata
							break
						}
					}
					sm.MemberAssignment = m.assignment
				}
				sg.Members = append(sg.Members, sm)
			}
			if req.IncludeAuthorizedOperations {
				sg.AuthorizedOperations = gs.c.groupAuthorizedOps(creq, rg)
			}
		}) {
			sg.State = groupDead.String()
		}
	}
	return resp
}

func (gs *groups) handleDelete(creq *clientReq) *kmsg.DeleteGroupsResponse {
	req := creq.kreq.(*kmsg.DeleteGroupsRequest)
	resp := req.ResponseKind().(*kmsg.DeleteGroupsResponse)

	doneg := func(name string) *kmsg.DeleteGroupsResponseGroup {
		sg := kmsg.NewDeleteGroupsResponseGroup()
		sg.Group = name
		resp.Groups = append(resp.Groups, sg)
		return &resp.Groups[len(resp.Groups)-1]
	}

	for _, rg := range req.Groups {
		sg := doneg(rg)
		// ACL check: DELETE on Group
		if !gs.c.allowedACL(creq, rg, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationDelete) {
			sg.ErrorCode = kerr.GroupAuthorizationFailed.Code
			continue
		}
		if kerr := gs.c.validateGroup(creq, rg); kerr != nil {
			sg.ErrorCode = kerr.Code
			continue
		}
		g, ok := gs.gs[rg]
		if !ok {
			sg.ErrorCode = kerr.GroupIDNotFound.Code
			continue
		}
		if !g.waitControl(func() {
			if g.typ == "consumer" {
				if len(g.consumerMembers) == 0 {
					g.quitOnce()
					delete(gs.gs, rg)
				} else {
					sg.ErrorCode = kerr.NonEmptyGroup.Code
				}
			} else {
				switch g.state {
				case groupDead:
					sg.ErrorCode = kerr.GroupIDNotFound.Code
				case groupEmpty:
					g.quitOnce()
					delete(gs.gs, rg)
				case groupPreparingRebalance, groupCompletingRebalance, groupStable:
					sg.ErrorCode = kerr.NonEmptyGroup.Code
				}
			}
		}) {
			sg.ErrorCode = kerr.GroupIDNotFound.Code
		}
	}
	return resp
}

func (gs *groups) handleOffsetFetch(creq *clientReq) *kmsg.OffsetFetchResponse {
	req := creq.kreq.(*kmsg.OffsetFetchRequest)
	resp := req.ResponseKind().(*kmsg.OffsetFetchResponse)

	if req.Version <= 7 {
		rg := kmsg.NewOffsetFetchRequestGroup()
		rg.Group = req.Group
		if req.Topics != nil {
			rg.Topics = make([]kmsg.OffsetFetchRequestGroupTopic, 0, len(req.Topics))
		}
		for _, t := range req.Topics {
			rt := kmsg.NewOffsetFetchRequestGroupTopic()
			rt.Topic = t.Topic
			rt.Partitions = t.Partitions
			rg.Topics = append(rg.Topics, rt)
		}
		req.Groups = append(req.Groups, rg)

		defer func() {
			g0 := resp.Groups[0]
			resp.ErrorCode = g0.ErrorCode
			for _, t := range g0.Topics {
				st := kmsg.NewOffsetFetchResponseTopic()
				st.Topic = t.Topic
				for _, p := range t.Partitions {
					sp := kmsg.NewOffsetFetchResponseTopicPartition()
					sp.Partition = p.Partition
					sp.Offset = p.Offset
					sp.LeaderEpoch = p.LeaderEpoch
					sp.Metadata = p.Metadata
					sp.ErrorCode = p.ErrorCode
					st.Partitions = append(st.Partitions, sp)
				}
				resp.Topics = append(resp.Topics, st)
			}
		}()
	}

	doneg := func(name string) *kmsg.OffsetFetchResponseGroup {
		sg := kmsg.NewOffsetFetchResponseGroup()
		sg.Group = name
		resp.Groups = append(resp.Groups, sg)
		return &resp.Groups[len(resp.Groups)-1]
	}

	for _, rg := range req.Groups {
		sg := doneg(rg.Group)
		// ACL check: DESCRIBE on Group
		if !gs.c.allowedACL(creq, rg.Group, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationDescribe) {
			sg.ErrorCode = kerr.GroupAuthorizationFailed.Code
			continue
		}
		if kerr := gs.c.validateGroup(creq, rg.Group); kerr != nil {
			sg.ErrorCode = kerr.Code
			continue
		}
		// KIP-447: If RequireStable is set, check for pending transactional offsets
		if req.RequireStable && gs.c.pids.hasUnstableOffsets(rg.Group) {
			sg.ErrorCode = kerr.UnstableOffsetCommit.Code
			continue
		}
		g, ok := gs.gs[rg.Group]
		if !ok {
			sg.ErrorCode = kerr.GroupIDNotFound.Code
			continue
		}
		if !g.waitControl(func() {
			if rg.Topics == nil {
				for t, ps := range g.commits {
					st := kmsg.NewOffsetFetchResponseGroupTopic()
					st.Topic = t
					for p, c := range ps {
						sp := kmsg.NewOffsetFetchResponseGroupTopicPartition()
						sp.Partition = p
						sp.Offset = c.offset
						sp.LeaderEpoch = c.leaderEpoch
						sp.Metadata = c.metadata
						st.Partitions = append(st.Partitions, sp)
					}
					sg.Topics = append(sg.Topics, st)
				}
			} else {
				for _, t := range rg.Topics {
					st := kmsg.NewOffsetFetchResponseGroupTopic()
					st.Topic = t.Topic
					for _, p := range t.Partitions {
						sp := kmsg.NewOffsetFetchResponseGroupTopicPartition()
						sp.Partition = p
						c, ok := g.commits.getp(t.Topic, p)
						if !ok {
							sp.Offset = -1
							sp.LeaderEpoch = -1
						} else {
							sp.Offset = c.offset
							sp.LeaderEpoch = c.leaderEpoch
							sp.Metadata = c.metadata
						}
						st.Partitions = append(st.Partitions, sp)
					}
					sg.Topics = append(sg.Topics, st)
				}
			}
		}) {
			sg.ErrorCode = kerr.GroupIDNotFound.Code
		}
	}
	return resp
}

func (g *group) handleOffsetDelete(creq *clientReq) *kmsg.OffsetDeleteResponse {
	req := creq.kreq.(*kmsg.OffsetDeleteRequest)
	resp := req.ResponseKind().(*kmsg.OffsetDeleteResponse)

	// ACL check: DELETE on Group
	if !g.c.allowedACL(creq, req.Group, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationDelete) {
		resp.ErrorCode = kerr.GroupAuthorizationFailed.Code
		return resp
	}

	if kerr := g.c.validateGroup(creq, req.Group); kerr != nil {
		resp.ErrorCode = kerr.Code
		return resp
	}

	tidx := make(map[string]int)
	donet := func(t string) *kmsg.OffsetDeleteResponseTopic {
		if i, ok := tidx[t]; ok {
			return &resp.Topics[i]
		}
		tidx[t] = len(resp.Topics)
		st := kmsg.NewOffsetDeleteResponseTopic()
		st.Topic = t
		resp.Topics = append(resp.Topics, st)
		return &resp.Topics[len(resp.Topics)-1]
	}
	donep := func(t string, p int32, errCode int16) *kmsg.OffsetDeleteResponseTopicPartition {
		sp := kmsg.NewOffsetDeleteResponseTopicPartition()
		sp.Partition = p
		sp.ErrorCode = errCode
		st := donet(t)
		st.Partitions = append(st.Partitions, sp)
		return &st.Partitions[len(st.Partitions)-1]
	}

	// empty: delete everything in request
	// preparingRebalance, completingRebalance, stable:
	//   * if consumer, delete everything not subscribed to
	//   * if not consumer, delete nothing, error with non_empty_group
	subTopics := make(map[string]struct{})
	switch g.state {
	default:
		resp.ErrorCode = kerr.GroupIDNotFound.Code
		return resp
	case groupEmpty:
	case groupPreparingRebalance, groupCompletingRebalance, groupStable:
		if g.protocolType != "consumer" {
			resp.ErrorCode = kerr.NonEmptyGroup.Code
			return resp
		}
		for _, m := range []map[string]*groupMember{
			g.members,
			g.pending,
		} {
			for _, m := range m {
				if m.join == nil {
					continue
				}
				for _, proto := range m.join.Protocols {
					var m kmsg.ConsumerMemberMetadata
					if err := m.ReadFrom(proto.Metadata); err == nil {
						for _, topic := range m.Topics {
							subTopics[topic] = struct{}{}
						}
					}
				}
			}
		}
	}

	for _, t := range req.Topics {
		for _, p := range t.Partitions {
			if _, ok := subTopics[t.Topic]; ok {
				donep(t.Topic, p.Partition, kerr.GroupSubscribedToTopic.Code)
				continue
			}
			g.commits.delp(t.Topic, p.Partition)
			donep(t.Topic, p.Partition, 0)
		}
	}

	return resp
}

////////////////////
// GROUP HANDLING //
////////////////////

func (g *group) manage(detachNew func()) {
	// On the first join only, we want to ensure that if the join is
	// invalid, we clean the group up before we detach from the cluster
	// serialization loop that is initializing us.
	var firstJoin func(bool)
	firstJoin = func(ok bool) {
		firstJoin = func(bool) {}
		if !ok {
			delete(g.gs.gs, g.name)
			g.quitOnce()
		}
		detachNew()
	}

	defer func() {
		for _, m := range g.members {
			if m.t != nil {
				m.t.Stop()
			}
		}
		for _, m := range g.pending {
			if m.t != nil {
				m.t.Stop()
			}
		}
		for _, m := range g.consumerMembers {
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
			case *kmsg.JoinGroupRequest:
				var ok bool
				kresp, ok = g.handleJoin(creq)
				firstJoin(ok)
			case *kmsg.SyncGroupRequest:
				kresp = g.handleSync(creq)
			case *kmsg.HeartbeatRequest:
				kresp = g.handleHeartbeat(creq)
			case *kmsg.LeaveGroupRequest:
				kresp = g.handleLeave(creq)
			case *kmsg.OffsetCommitRequest:
				if g.typ == "consumer" {
					kresp = g.handleConsumerOffsetCommit(creq)
					firstJoin(true)
				} else {
					var ok bool
					kresp, ok = g.handleOffsetCommit(creq)
					firstJoin(ok)
				}
			case *kmsg.OffsetDeleteRequest:
				kresp = g.handleOffsetDelete(creq)
			case *kmsg.ConsumerGroupHeartbeatRequest:
				g.lastTopicMeta = creq.topicMeta
				if g.targetAssignmentsStale {
					g.targetAssignmentsStale = false
					g.computeTargetAssignment(creq.topicMeta)
				}
				kresp = g.handleConsumerHeartbeat(creq)
				firstJoin(true)
			}
			if kresp != nil {
				g.reply(creq, kresp, nil)
			}

		case fn := <-g.controlCh:
			fn()
		}
	}
}

// The group manage loop does not block: it sends to respCh which eventually
// writes; but that write is fast. There is no long-blocking code in the manage
// loop.
func (g *group) waitControl(fn func()) bool {
	wait := make(chan struct{})
	wfn := func() { fn(); close(wait) }
	// Drain adminCh while waiting to avoid deadlock: the pids
	// manage loop may call c.admin() (e.g. transaction timeout
	// abort) while we're blocked sending to controlCh or waiting
	// for the function to complete.
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
	for {
		select {
		case <-wait:
			return true
		case <-g.quitCh:
			return false
		case <-g.c.die:
			return false
		case admin := <-g.c.adminCh:
			admin()
		}
	}
}

// Called in the manage loop.
func (g *group) quitOnce() {
	g.quit.Do(func() {
		g.state = groupDead
		close(g.quitCh)
	})
}

// Handles a join. We do not do the delayed join aspects in Kafka, we just punt
// to the client to immediately rejoin if a new client enters the group.
//
// If this returns nil, the request will be replied to later.
func (g *group) handleJoin(creq *clientReq) (kmsg.Response, bool) {
	req := creq.kreq.(*kmsg.JoinGroupRequest)
	resp := req.ResponseKind().(*kmsg.JoinGroupResponse)

	if kerr := g.c.validateGroup(creq, req.Group); kerr != nil {
		resp.ErrorCode = kerr.Code
		return resp, false
	}
	if !g.c.allowedACL(creq, req.Group, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationRead) {
		resp.ErrorCode = kerr.GroupAuthorizationFailed.Code
		return resp, false
	}
	if req.InstanceID != nil {
		resp.ErrorCode = kerr.InvalidGroupID.Code
		return resp, false
	}
	if st := int64(req.SessionTimeoutMillis); st < g.c.cfg.minSessionTimeout.Milliseconds() || st > g.c.cfg.maxSessionTimeout.Milliseconds() {
		resp.ErrorCode = kerr.InvalidSessionTimeout.Code
		return resp, false
	}
	if !g.protocolsMatch(req.ProtocolType, req.Protocols) {
		resp.ErrorCode = kerr.InconsistentGroupProtocol.Code
		return resp, false
	}

	// Clients first join with no member ID. For join v4+, we generate
	// the member ID and add the member to pending. For v3 and below,
	// we immediately enter rebalance.
	if req.MemberID == "" {
		memberID := generateMemberID(creq.cid, req.InstanceID)
		resp.MemberID = memberID
		m := &groupMember{
			memberID:   memberID,
			clientID:   creq.cid,
			clientHost: creq.cc.conn.RemoteAddr().String(),
			join:       req,
		}
		if req.Version >= 4 {
			g.addPendingRebalance(m)
			resp.ErrorCode = kerr.MemberIDRequired.Code
			return resp, true
		}
		g.addMemberAndRebalance(m, creq, req)
		return nil, true
	}

	// Pending members rejoining immediately enters rebalance.
	if m, ok := g.pending[req.MemberID]; ok {
		g.addMemberAndRebalance(m, creq, req)
		return nil, true
	}
	m, ok := g.members[req.MemberID]
	if !ok {
		resp.ErrorCode = kerr.UnknownMemberID.Code
		return resp, false
	}

	switch g.state {
	default:
		resp.ErrorCode = kerr.UnknownMemberID.Code
		return resp, false
	case groupPreparingRebalance:
		g.updateMemberAndRebalance(m, creq, req)
	case groupCompletingRebalance:
		if m.sameJoin(req) {
			g.fillJoinResp(m.memberID, resp)
			return resp, true
		}
		g.updateMemberAndRebalance(m, creq, req)
	case groupStable:
		if g.leader != req.MemberID && m.sameJoin(req) {
			// Non-leader with same metadata - no change needed
			g.fillJoinResp(m.memberID, resp)
			return resp, true
		}
		g.updateMemberAndRebalance(m, creq, req)
	}
	return nil, true
}

// Handles a sync, which can transition us to stable.
func (g *group) handleSync(creq *clientReq) kmsg.Response {
	req := creq.kreq.(*kmsg.SyncGroupRequest)
	resp := req.ResponseKind().(*kmsg.SyncGroupResponse)

	if kerr := g.c.validateGroup(creq, req.Group); kerr != nil {
		resp.ErrorCode = kerr.Code
		return resp
	}
	if !g.c.allowedACL(creq, req.Group, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationRead) {
		resp.ErrorCode = kerr.GroupAuthorizationFailed.Code
		return resp
	}
	if req.InstanceID != nil {
		resp.ErrorCode = kerr.InvalidGroupID.Code
		return resp
	}
	m, ok := g.members[req.MemberID]
	if !ok {
		resp.ErrorCode = kerr.UnknownMemberID.Code
		return resp
	}
	if req.Generation != g.generation {
		resp.ErrorCode = kerr.IllegalGeneration.Code
		return resp
	}
	if req.ProtocolType != nil && *req.ProtocolType != g.protocolType {
		resp.ErrorCode = kerr.InconsistentGroupProtocol.Code
		return resp
	}
	if req.Protocol != nil && *req.Protocol != g.protocol {
		resp.ErrorCode = kerr.InconsistentGroupProtocol.Code
		return resp
	}

	switch g.state {
	default:
		resp.ErrorCode = kerr.UnknownMemberID.Code
	case groupPreparingRebalance:
		resp.ErrorCode = kerr.RebalanceInProgress.Code
	case groupCompletingRebalance:
		m.waitingReply = creq
		if req.MemberID == g.leader {
			g.completeLeaderSync(req)
		}
		return nil
	case groupStable: // member saw join and is now finally calling sync
		resp.ProtocolType = kmsg.StringPtr(g.protocolType)
		resp.Protocol = kmsg.StringPtr(g.protocol)
		resp.MemberAssignment = m.assignment
	}
	return resp
}

// Handles a heartbeat, a relatively simple request that just delays our
// session timeout timer.
func (g *group) handleHeartbeat(creq *clientReq) kmsg.Response {
	req := creq.kreq.(*kmsg.HeartbeatRequest)
	resp := req.ResponseKind().(*kmsg.HeartbeatResponse)

	if kerr := g.c.validateGroup(creq, req.Group); kerr != nil {
		resp.ErrorCode = kerr.Code
		return resp
	}
	if !g.c.allowedACL(creq, req.Group, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationRead) {
		resp.ErrorCode = kerr.GroupAuthorizationFailed.Code
		return resp
	}
	if req.InstanceID != nil {
		resp.ErrorCode = kerr.InvalidGroupID.Code
		return resp
	}
	m, ok := g.members[req.MemberID]
	if !ok {
		resp.ErrorCode = kerr.UnknownMemberID.Code
		return resp
	}
	if req.Generation != g.generation {
		resp.ErrorCode = kerr.IllegalGeneration.Code
		return resp
	}

	switch g.state {
	default:
		resp.ErrorCode = kerr.UnknownMemberID.Code
	case groupPreparingRebalance:
		resp.ErrorCode = kerr.RebalanceInProgress.Code
		g.updateHeartbeat(m)
	case groupCompletingRebalance, groupStable:
		g.updateHeartbeat(m)
	}
	return resp
}

// Handles a leave. We trigger a rebalance for every member leaving in a batch
// request, but that's fine because of our manage serialization.
func (g *group) handleLeave(creq *clientReq) kmsg.Response {
	req := creq.kreq.(*kmsg.LeaveGroupRequest)
	resp := req.ResponseKind().(*kmsg.LeaveGroupResponse)

	if kerr := g.c.validateGroup(creq, req.Group); kerr != nil {
		resp.ErrorCode = kerr.Code
		return resp
	}
	if !g.c.allowedACL(creq, req.Group, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationRead) {
		resp.ErrorCode = kerr.GroupAuthorizationFailed.Code
		return resp
	}
	if req.Version < 3 {
		req.Members = append(req.Members, kmsg.LeaveGroupRequestMember{
			MemberID: req.MemberID,
		})
		defer func() { resp.ErrorCode = resp.Members[0].ErrorCode }()
	}

	for _, rm := range req.Members {
		mresp := kmsg.NewLeaveGroupResponseMember()
		mresp.MemberID = rm.MemberID
		mresp.InstanceID = rm.InstanceID
		resp.Members = append(resp.Members, mresp)

		r := &resp.Members[len(resp.Members)-1]
		if rm.InstanceID != nil {
			r.ErrorCode = kerr.UnknownMemberID.Code
			continue
		}
		if m, ok := g.members[rm.MemberID]; !ok {
			if p, ok := g.pending[rm.MemberID]; !ok {
				r.ErrorCode = kerr.UnknownMemberID.Code
			} else {
				g.stopPending(p)
			}
		} else {
			g.updateMemberAndRebalance(m, nil, nil)
		}
	}

	return resp
}

func fillOffsetCommit(req *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, code int16) {
	for _, t := range req.Topics {
		st := kmsg.NewOffsetCommitResponseTopic()
		st.Topic = t.Topic
		for _, p := range t.Partitions {
			sp := kmsg.NewOffsetCommitResponseTopicPartition()
			sp.Partition = p.Partition
			sp.ErrorCode = code
			st.Partitions = append(st.Partitions, sp)
		}
		resp.Topics = append(resp.Topics, st)
	}
}

// fillOffsetCommitWithACL fills the response with per-topic ACL checks.
// Returns topics that passed ACL check.
func (g *group) fillOffsetCommitWithACL(creq *clientReq, req *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse) []kmsg.OffsetCommitRequestTopic {
	var allowed []kmsg.OffsetCommitRequestTopic
	for _, t := range req.Topics {
		st := kmsg.NewOffsetCommitResponseTopic()
		st.Topic = t.Topic
		if !g.c.allowedACL(creq, t.Topic, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationRead) {
			for _, p := range t.Partitions {
				sp := kmsg.NewOffsetCommitResponseTopicPartition()
				sp.Partition = p.Partition
				sp.ErrorCode = kerr.TopicAuthorizationFailed.Code
				st.Partitions = append(st.Partitions, sp)
			}
		} else {
			allowed = append(allowed, t)
			for _, p := range t.Partitions {
				sp := kmsg.NewOffsetCommitResponseTopicPartition()
				sp.Partition = p.Partition
				sp.ErrorCode = 0
				st.Partitions = append(st.Partitions, sp)
			}
		}
		resp.Topics = append(resp.Topics, st)
	}
	return allowed
}

// Handles a commit.
func (g *group) handleOffsetCommit(creq *clientReq) (*kmsg.OffsetCommitResponse, bool) {
	req := creq.kreq.(*kmsg.OffsetCommitRequest)
	resp := req.ResponseKind().(*kmsg.OffsetCommitResponse)

	if kerr := g.c.validateGroup(creq, req.Group); kerr != nil {
		fillOffsetCommit(req, resp, kerr.Code)
		return resp, false
	}

	// ACL check: READ on GROUP (if denied, fail all topics)
	if !g.c.allowedACL(creq, req.Group, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationRead) {
		fillOffsetCommit(req, resp, kerr.GroupAuthorizationFailed.Code)
		return resp, false
	}

	if req.InstanceID != nil {
		fillOffsetCommit(req, resp, kerr.InvalidGroupID.Code)
		return resp, false
	}

	var m *groupMember
	if len(g.members) > 0 {
		var ok bool
		m, ok = g.members[req.MemberID]
		if !ok {
			fillOffsetCommit(req, resp, kerr.UnknownMemberID.Code)
			return resp, false
		}
		if req.Generation != g.generation {
			fillOffsetCommit(req, resp, kerr.IllegalGeneration.Code)
			return resp, false
		}
	} else if req.Generation >= 0 {
		// Empty group: only accept simple commits (generation < 0).
		fillOffsetCommit(req, resp, kerr.IllegalGeneration.Code)
		return resp, false
	}

	switch g.state {
	default:
		fillOffsetCommit(req, resp, kerr.GroupIDNotFound.Code)
		return resp, true
	case groupEmpty, groupPreparingRebalance, groupStable:
		allowed := g.fillOffsetCommitWithACL(creq, req, resp)
		for _, t := range allowed {
			for _, p := range t.Partitions {
				g.commits.set(t.Topic, p.Partition, offsetCommit{
					offset:      p.Offset,
					leaderEpoch: p.LeaderEpoch,
					metadata:    p.Metadata,
				})
			}
		}
		if m != nil {
			g.updateHeartbeat(m)
		}
	case groupCompletingRebalance:
		fillOffsetCommit(req, resp, kerr.RebalanceInProgress.Code)
		g.updateHeartbeat(m)
	}
	return resp, true
}

// Transitions the group to the preparing rebalance state. We first need to
// clear any member that is currently sitting in sync. If enough members have
// entered join, we immediately proceed to completeRebalance, otherwise we
// begin a wait timer.
func (g *group) rebalance() {
	if g.state == groupCompletingRebalance {
		for _, m := range g.members {
			m.assignment = nil
			if m.waitingReply.empty() {
				continue
			}
			sync, ok := m.waitingReply.kreq.(*kmsg.SyncGroupRequest)
			if !ok {
				continue
			}
			resp := sync.ResponseKind().(*kmsg.SyncGroupResponse)
			resp.ErrorCode = kerr.RebalanceInProgress.Code
			g.reply(m.waitingReply, resp, m)
		}
	}

	g.state = groupPreparingRebalance

	if g.nJoining >= len(g.members) {
		g.completeRebalance()
		return
	}

	var rebalanceTimeoutMs int32
	for _, m := range g.members {
		if m.join.RebalanceTimeoutMillis > rebalanceTimeoutMs {
			rebalanceTimeoutMs = m.join.RebalanceTimeoutMillis
		}
	}
	if g.tRebalance == nil {
		g.tRebalance = time.AfterFunc(time.Duration(rebalanceTimeoutMs)*time.Millisecond, func() {
			select {
			case <-g.quitCh:
			case <-g.c.die:
			case g.controlCh <- func() {
				g.completeRebalance()
			}:
			}
		})
	}
}

// Transitions the group to either dead or stable, depending on if any members
// remain by the time we clear those that are not waiting in join.
func (g *group) completeRebalance() {
	if g.tRebalance != nil {
		g.tRebalance.Stop()
		g.tRebalance = nil
	}
	g.nJoining = 0

	var foundLeader bool
	for _, m := range g.members {
		if m.waitingReply.empty() {
			for _, p := range m.join.Protocols {
				g.protocols[p.Name]--
			}
			delete(g.members, m.memberID)
			if m.t != nil {
				m.t.Stop()
			}
			continue
		}
		if m.memberID == g.leader {
			foundLeader = true
		}
	}

	g.generation++
	if g.generation < 0 {
		g.generation = 1
	}
	if len(g.members) == 0 {
		g.state = groupEmpty
		return
	}
	g.state = groupCompletingRebalance

	var foundProto bool
	for proto, nsupport := range g.protocols {
		if nsupport == len(g.members) {
			g.protocol = proto
			foundProto = true
			break
		}
	}
	if !foundProto {
		panic(fmt.Sprint("unable to find commonly supported protocol!", g.protocols, len(g.members)))
	}

	for _, m := range g.members {
		if !foundLeader {
			g.leader = m.memberID
		}
		req := m.join
		resp := req.ResponseKind().(*kmsg.JoinGroupResponse)
		g.fillJoinResp(m.memberID, resp)
		g.reply(m.waitingReply, resp, m)
	}
}

// Transitions the group to stable, the final step of a rebalance.
func (g *group) completeLeaderSync(req *kmsg.SyncGroupRequest) {
	for _, m := range g.members {
		m.assignment = nil
	}
	for _, a := range req.GroupAssignment {
		m, ok := g.members[a.MemberID]
		if !ok {
			continue
		}
		m.assignment = g.assignmentOrEmpty(a.MemberAssignment)
	}
	for _, m := range g.members {
		if m.waitingReply.empty() {
			continue // this member saw join but has not yet called sync
		}
		resp := m.waitingReply.kreq.ResponseKind().(*kmsg.SyncGroupResponse)
		resp.ProtocolType = kmsg.StringPtr(g.protocolType)
		resp.Protocol = kmsg.StringPtr(g.protocol)
		resp.MemberAssignment = m.assignment
		g.reply(m.waitingReply, resp, m)
	}
	g.state = groupStable
}

// assignmentOrEmpty returns the assignment bytes, or a pre-serialized empty
// ConsumerMemberAssignment if the assignment is empty and this is a "consumer"
// protocol group. This ensures followers always receive a decodable assignment.
func (g *group) assignmentOrEmpty(assignment []byte) []byte {
	if len(assignment) == 0 && g.protocolType == "consumer" {
		return append([]byte(nil), emptyConsumerAssignment...)
	}
	return assignment
}

func (g *group) updateHeartbeat(m *groupMember) {
	g.atSessionTimeout(m, func() {
		g.updateMemberAndRebalance(m, nil, nil)
	})
}

func (g *group) addPendingRebalance(m *groupMember) {
	g.pending[m.memberID] = m
	g.atSessionTimeout(m, func() {
		delete(g.pending, m.memberID)
	})
}

func (g *group) stopPending(m *groupMember) {
	delete(g.pending, m.memberID)
	if m.t != nil {
		m.t.Stop()
	}
}

// timerControlFn starts a timer that, on expiry, sends fn to the
// group's control channel. If the group is shutting down, the send
// is abandoned.
func (g *group) timerControlFn(d time.Duration, fn func()) *time.Timer {
	return time.AfterFunc(d, func() {
		select {
		case <-g.quitCh:
		case <-g.c.die:
		case g.controlCh <- fn:
		}
	})
}

func (g *group) atSessionTimeout(m *groupMember, fn func()) {
	if m.t != nil {
		m.t.Stop()
	}
	timeout := time.Millisecond * time.Duration(m.join.SessionTimeoutMillis)
	m.last = time.Now()
	m.t = g.timerControlFn(timeout, func() {
		if time.Since(m.last) >= timeout {
			fn()
		}
	})
}

// This is used to update a member from a new join request, or to clear a
// member from failed heartbeats.
func (g *group) updateMemberAndRebalance(m *groupMember, waitingReply *clientReq, newJoin *kmsg.JoinGroupRequest) {
	for _, p := range m.join.Protocols {
		g.protocols[p.Name]--
	}
	m.join = newJoin
	if m.join != nil {
		for _, p := range m.join.Protocols {
			g.protocols[p.Name]++
		}
		if m.waitingReply.empty() && !waitingReply.empty() {
			g.nJoining++
		}
		m.waitingReply = waitingReply
	} else {
		delete(g.members, m.memberID)
		if m.t != nil {
			m.t.Stop()
		}
		if !m.waitingReply.empty() {
			g.nJoining--
		}
	}
	g.rebalance()
}

// Adds a new member to the group and rebalances.
func (g *group) addMemberAndRebalance(m *groupMember, waitingReply *clientReq, join *kmsg.JoinGroupRequest) {
	g.stopPending(m)
	m.join = join
	for _, p := range m.join.Protocols {
		g.protocols[p.Name]++
	}
	g.members[m.memberID] = m
	g.nJoining++
	m.waitingReply = waitingReply
	g.rebalance()
}

// Returns if a new join can even join the group based on the join's supported
// protocols.
func (g *group) protocolsMatch(protocolType string, protocols []kmsg.JoinGroupRequestProtocol) bool {
	if g.protocolType == "" {
		if protocolType == "" || len(protocols) == 0 {
			return false
		}
		g.protocolType = protocolType
		return true
	}
	if protocolType != g.protocolType {
		return false
	}
	if len(g.protocols) == 0 {
		return true
	}
	for _, p := range protocols {
		if _, ok := g.protocols[p.Name]; ok {
			return true
		}
	}
	return false
}

// Returns if a new join request is the same as an old request; if so, for
// non-leaders, we just return the old join response.
func (m *groupMember) sameJoin(req *kmsg.JoinGroupRequest) bool {
	if len(m.join.Protocols) != len(req.Protocols) {
		return false
	}
	for i := range m.join.Protocols {
		if m.join.Protocols[i].Name != req.Protocols[i].Name {
			return false
		}
		if !bytes.Equal(m.join.Protocols[i].Metadata, req.Protocols[i].Metadata) {
			return false
		}
	}
	return true
}

func (g *group) fillJoinResp(memberID string, resp *kmsg.JoinGroupResponse) {
	resp.Generation = g.generation
	resp.ProtocolType = kmsg.StringPtr(g.protocolType)
	resp.Protocol = kmsg.StringPtr(g.protocol)
	resp.LeaderID = g.leader
	resp.MemberID = memberID
	if g.leader == memberID {
		resp.Members = g.joinResponseMetadata()
	}
}

func (g *group) joinResponseMetadata() []kmsg.JoinGroupResponseMember {
	metadata := make([]kmsg.JoinGroupResponseMember, 0, len(g.members))
members:
	for _, m := range g.members {
		for _, p := range m.join.Protocols {
			if p.Name == g.protocol {
				metadata = append(metadata, kmsg.JoinGroupResponseMember{
					MemberID:         m.memberID,
					ProtocolMetadata: p.Metadata,
				})
				continue members
			}
		}
		panic("inconsistent group protocol within saved members")
	}
	return metadata
}

func (g *group) reply(creq *clientReq, kresp kmsg.Response, m *groupMember) {
	select {
	case creq.cc.respCh <- clientResp{kresp: kresp, corr: creq.corr, seq: creq.seq}:
	case <-g.c.die:
		return
	}
	if m != nil {
		m.waitingReply = nil
		g.updateHeartbeat(m)
	}
}

///////////////////////////
// KIP-848 CONSUMER GROUPS
///////////////////////////

// Hijacks the consumer group heartbeat request into the group's manage
// goroutine. We snapshot topic metadata here (running in Cluster.run,
// safe access to c.data) so that computeAssignment does not need to
// call c.admin(), which would deadlock.
func (gs *groups) handleConsumerGroupHeartbeat(creq *clientReq) {
	if gs.gs == nil {
		gs.gs = make(map[string]*group)
	}
	req := creq.kreq.(*kmsg.ConsumerGroupHeartbeatRequest)
	g := gs.gs[req.Group]
	if g == nil {
		g = gs.newGroup(req.Group)
		gs.gs[req.Group] = g
		go g.manage(func() {})
	}
	creq.topicMeta = gs.c.snapshotTopicMeta()
	select {
	case g.reqCh <- creq:
	case <-g.quitCh:
	case <-g.c.die:
	}
}

func (gs *groups) handleConsumerGroupDescribe(creq *clientReq) *kmsg.ConsumerGroupDescribeResponse {
	req := creq.kreq.(*kmsg.ConsumerGroupDescribeRequest)
	resp := req.ResponseKind().(*kmsg.ConsumerGroupDescribeResponse)

	doneg := func(name string) *kmsg.ConsumerGroupDescribeResponseGroup {
		sg := kmsg.NewConsumerGroupDescribeResponseGroup()
		sg.Group = name
		sg.AuthorizedOperations = math.MinInt32
		resp.Groups = append(resp.Groups, sg)
		return &resp.Groups[len(resp.Groups)-1]
	}

	for _, rg := range req.Groups {
		sg := doneg(rg)
		if !gs.c.allowedACL(creq, rg, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationDescribe) {
			sg.ErrorCode = kerr.GroupAuthorizationFailed.Code
			continue
		}
		if kerr := gs.c.validateGroup(creq, rg); kerr != nil {
			sg.ErrorCode = kerr.Code
			continue
		}
		g, ok := gs.gs[rg]
		if !ok {
			sg.ErrorCode = kerr.GroupIDNotFound.Code
			if req.IncludeAuthorizedOperations {
				sg.AuthorizedOperations = gs.c.groupAuthorizedOps(creq, rg)
			}
			continue
		}
		if !g.waitControl(func() {
			if g.typ != "consumer" {
				sg.ErrorCode = kerr.GroupIDNotFound.Code
				if req.IncludeAuthorizedOperations {
					sg.AuthorizedOperations = gs.c.groupAuthorizedOps(creq, rg)
				}
				return
			}
			sg.State = g.state.String()
			sg.Epoch = g.generation
			sg.AssignmentEpoch = g.generation
			sg.AssignorName = g.assignorName
			for _, m := range g.consumerMembers {
				sm := kmsg.NewConsumerGroupDescribeResponseGroupMember()
				sm.MemberID = m.memberID
				sm.InstanceID = m.instanceID
				sm.RackID = m.rackID
				sm.MemberEpoch = m.memberEpoch
				sm.ClientID = m.clientID
				sm.ClientHost = m.clientHost
				sm.SubscribedTopics = m.subscribedTopics
				sm.MemberType = 1 // consumer
				sm.Assignment = uuidAssignmentToKmsg(m.currentAssignment)
				sm.TargetAssignment = uuidAssignmentToKmsg(m.targetAssignment)
				sg.Members = append(sg.Members, sm)
			}
			if req.IncludeAuthorizedOperations {
				sg.AuthorizedOperations = gs.c.groupAuthorizedOps(creq, rg)
			}
		}) {
			sg.ErrorCode = kerr.GroupIDNotFound.Code
		}
	}
	return resp
}

func uuidAssignmentToKmsg(a map[uuid][]int32) kmsg.Assignment {
	var ka kmsg.Assignment
	for id, parts := range a {
		tp := kmsg.NewAssignmentTopicPartition()
		tp.TopicID = id
		tp.Partitions = parts
		ka.TopicPartitions = append(ka.TopicPartitions, tp)
	}
	return ka
}

// A "full request" carries subscription and assignment info. Keepalive
// heartbeats set RebalanceTimeoutMillis to -1 and leave all optional
// fields nil; the server should skip subscription processing and omit
// assignment from the response unless the member is still reconciling.
// Kafka requires ALL three conditions: timeout present, subscription
// present, and owned partitions present.
func isFullRequest(req *kmsg.ConsumerGroupHeartbeatRequest) bool {
	return req.RebalanceTimeoutMillis != -1 &&
		(req.SubscribedTopicNames != nil || req.SubscribedTopicRegex != nil) &&
		req.Topics != nil
}

// Handles a KIP-848 consumer group heartbeat, routing to join, leave, or
// regular heartbeat based on MemberEpoch.
func (g *group) handleConsumerHeartbeat(creq *clientReq) kmsg.Response {
	req := creq.kreq.(*kmsg.ConsumerGroupHeartbeatRequest)
	resp := req.ResponseKind().(*kmsg.ConsumerGroupHeartbeatResponse)
	resp.HeartbeatIntervalMillis = g.c.consumerHeartbeatIntervalMs()

	if kerr := g.c.validateGroup(creq, req.Group); kerr != nil {
		resp.ErrorCode = kerr.Code
		return resp
	}
	if !g.c.allowedACL(creq, req.Group, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationRead) {
		resp.ErrorCode = kerr.GroupAuthorizationFailed.Code
		return resp
	}

	// Groups start as "classic" (the default in newGroup, matching
	// Kafka's GroupCoordinator). The first ConsumerGroupHeartbeat
	// upgrades the group to "consumer", unless classic members are
	// already active.
	if g.typ == "classic" {
		if len(g.members) > 0 {
			resp.ErrorCode = kerr.GroupIDNotFound.Code
			return resp
		}
		g.typ = "consumer"
		g.consumerMembers = make(map[string]*consumerMember)
		g.partitionEpochs = make(map[uuid]map[int32]int32)
	}

	switch req.MemberEpoch {
	case 0:
		return g.consumerJoin(creq, req, resp)
	case -1:
		return g.consumerLeave(req, resp)
	default:
		return g.consumerRegularHeartbeat(req, resp)
	}
}

func (g *group) consumerJoin(creq *clientReq, req *kmsg.ConsumerGroupHeartbeatRequest, resp *kmsg.ConsumerGroupHeartbeatResponse) *kmsg.ConsumerGroupHeartbeatResponse {
	memberID := req.MemberID
	if memberID == "" {
		memberID = generateMemberID(creq.cid, nil)
	}

	// Rejoin (e.g. after fencing): remove and re-add.
	var old *consumerMember
	if prev, ok := g.consumerMembers[memberID]; ok {
		old = prev
		g.fenceConsumerMember(old)
		delete(g.consumerMembers, memberID)
	}

	m := &consumerMember{
		memberID:           memberID,
		clientID:           creq.cid,
		clientHost:         creq.cc.conn.RemoteAddr().String(),
		instanceID:         req.InstanceID,
		rackID:             req.RackID,
		rebalanceTimeoutMs: req.RebalanceTimeoutMillis,
		currentAssignment:  make(map[uuid][]int32),
		targetAssignment:   make(map[uuid][]int32),
	}
	if req.ServerAssignor != nil {
		if !validServerAssignor(*req.ServerAssignor) {
			resp.ErrorCode = kerr.UnsupportedAssignor.Code
			return resp
		}
		m.serverAssignor = *req.ServerAssignor
	}
	if req.SubscribedTopicNames != nil {
		m.subscribedTopics = req.SubscribedTopicNames
	}
	if req.SubscribedTopicRegex != nil {
		re, err := regexp.Compile(*req.SubscribedTopicRegex)
		if err != nil {
			resp.ErrorCode = kerr.InvalidRequest.Code
			return resp
		}
		m.subscribedTopicRegex = re
	}
	if m.rebalanceTimeoutMs <= 0 {
		m.rebalanceTimeoutMs = 45000
	}

	g.consumerMembers[memberID] = m
	g.assignorName = m.serverAssignor

	// Kafka bumps the group epoch when: the group is at epoch 0 (newly
	// created), or the subscription changed. We detect subscription
	// change by checking if this is a new member or a rejoin that
	// changed topics.
	if g.generation == 0 || old == nil || !slices.Equal(old.subscribedTopics, m.subscribedTopics) {
		g.generation++
		g.computeTargetAssignment(g.lastTopicMeta)
	}

	m.memberEpoch = g.generation

	g.updateConsumerStateField()
	g.atConsumerSessionTimeout(m)

	resp.MemberID = &memberID
	resp.MemberEpoch = m.memberEpoch
	reconciled := g.reconciledAssignment(m)
	resp.Assignment = makeAssignment(reconciled)
	m.lastSentAssignment = copyAssignment(reconciled)
	m.lastReconciledSent = copyAssignment(reconciled)
	m.assignmentEpoch = m.memberEpoch
	g.addPartitionEpochs(reconciled, m.memberEpoch)
	return resp
}

func (g *group) consumerLeave(req *kmsg.ConsumerGroupHeartbeatRequest, resp *kmsg.ConsumerGroupHeartbeatResponse) *kmsg.ConsumerGroupHeartbeatResponse {
	m, ok := g.consumerMembers[req.MemberID]
	if !ok {
		resp.ErrorCode = kerr.UnknownMemberID.Code
		return resp
	}
	g.fenceConsumerMember(m)
	delete(g.consumerMembers, req.MemberID)

	g.generation++
	g.computeTargetAssignment(g.lastTopicMeta)
	g.updateConsumerStateField()

	resp.MemberID = &req.MemberID
	resp.MemberEpoch = -1
	return resp
}

func (g *group) consumerRegularHeartbeat(req *kmsg.ConsumerGroupHeartbeatRequest, resp *kmsg.ConsumerGroupHeartbeatResponse) *kmsg.ConsumerGroupHeartbeatResponse {
	m, ok := g.consumerMembers[req.MemberID]
	if !ok {
		resp.ErrorCode = kerr.UnknownMemberID.Code
		return resp
	}

	// Previous-epoch recovery: accept the member's previous epoch if
	// the reported partitions are present and are a subset of the
	// member's current assignment (what the server believes the member
	// currently has).
	if req.MemberEpoch != m.memberEpoch {
		if req.MemberEpoch != m.previousMemberEpoch || req.Topics == nil || !isSubsetAssignment(req.Topics, m.currentAssignment) {
			resp.ErrorCode = kerr.FencedMemberEpoch.Code
			return resp
		}
	}

	g.atConsumerSessionTimeout(m)

	full := isFullRequest(req)
	needRecompute := false
	if full {
		if req.SubscribedTopicNames != nil {
			if !slices.Equal(m.subscribedTopics, req.SubscribedTopicNames) {
				m.subscribedTopics = req.SubscribedTopicNames
				needRecompute = true
			}
		}
		if req.SubscribedTopicRegex != nil {
			re, err := regexp.Compile(*req.SubscribedTopicRegex)
			if err != nil {
				resp.ErrorCode = kerr.InvalidRequest.Code
				return resp
			}
			m.subscribedTopicRegex = re
			needRecompute = true
		}
		if req.ServerAssignor != nil && *req.ServerAssignor != m.serverAssignor {
			if !validServerAssignor(*req.ServerAssignor) {
				resp.ErrorCode = kerr.UnsupportedAssignor.Code
				return resp
			}
			m.serverAssignor = *req.ServerAssignor
			g.assignorName = m.serverAssignor
		}
		if req.Topics != nil {
			reported := make(map[uuid][]int32, len(req.Topics))
			for _, t := range req.Topics {
				reported[t.TopicID] = t.Partitions
			}
			m.currentAssignment = reported

			// Prune lastSentAssignment: if the member no longer
			// has a partition and it's not in the target, the
			// member has released it.
			var released map[uuid][]int32
			for id, parts := range m.lastSentAssignment {
				keep := parts[:0]
				for _, p := range parts {
					if slices.Contains(m.currentAssignment[id], p) || slices.Contains(m.targetAssignment[id], p) {
						keep = append(keep, p)
					} else {
						if released == nil {
							released = make(map[uuid][]int32)
						}
						released[id] = append(released[id], p)
					}
				}
				if len(keep) > 0 {
					m.lastSentAssignment[id] = keep
				} else {
					delete(m.lastSentAssignment, id)
				}
			}
			g.removePartitionEpochs(released, m.memberEpoch)
			g.addPartitionEpochs(m.currentAssignment, m.memberEpoch)
		}
	}

	if needRecompute {
		g.generation++
		g.computeTargetAssignment(g.lastTopicMeta)
	}
	if assignmentsEqual(m.currentAssignment, m.targetAssignment) && m.memberEpoch < g.generation {
		m.previousMemberEpoch = m.memberEpoch
		m.memberEpoch = g.generation
	}

	g.updateConsumerStateField()

	resp.MemberID = &req.MemberID
	resp.MemberEpoch = m.memberEpoch
	// Include assignment on: full request, or reconciled assignment
	// differs from what we last sent (tracked separately from
	// lastSentAssignment which may be a superset due to pending
	// revocations).
	reconciled := g.reconciledAssignment(m)
	if full || !assignmentsEqual(m.lastReconciledSent, reconciled) {
		resp.Assignment = makeAssignment(reconciled)
		m.lastReconciledSent = copyAssignment(reconciled)
		// Merge reconciled into lastSentAssignment: we only add
		// partitions, never remove. Partitions are removed from
		// lastSentAssignment only when the member confirms release
		// via currentAssignment (see above). This prevents another
		// member from being assigned a partition before this member
		// acknowledges the revocation.
		for id, parts := range reconciled {
			existing := m.lastSentAssignment[id]
			for _, p := range parts {
				if !slices.Contains(existing, p) {
					m.lastSentAssignment[id] = append(m.lastSentAssignment[id], p)
				}
			}
		}
		m.assignmentEpoch = m.memberEpoch
		g.addPartitionEpochs(reconciled, m.memberEpoch)
	}

	// Schedule or cancel the rebalance timeout: active only when
	// lastSentAssignment has partitions not in targetAssignment
	// (the member was told to revoke but hasn't confirmed).
	if g.memberHasUnrevokedPartitions(m) {
		if m.tRebal == nil {
			g.scheduleConsumerRebalanceTimeout(m)
		}
	} else {
		g.cancelConsumerRebalanceTimeout(m)
	}

	return resp
}

// memberHasUnrevokedPartitions returns true if the member's
// lastSentAssignment contains any partitions not in targetAssignment,
// meaning the member has been told to revoke but hasn't confirmed.
func (g *group) memberHasUnrevokedPartitions(m *consumerMember) bool {
	for id, parts := range m.lastSentAssignment {
		target := m.targetAssignment[id]
		for _, p := range parts {
			if !slices.Contains(target, p) {
				return true
			}
		}
	}
	return false
}

// validServerAssignor returns whether the given assignor name is
// supported for consumer groups. Only "uniform" and "range" are valid
// ("simple" is for share groups only - KIP-932).
func validServerAssignor(name string) bool {
	return name == "uniform" || name == "range"
}

type assignorTP struct {
	topic string
	id    uuid
	part  int32
}

// computeTargetAssignment resolves subscriptions against the topic
// metadata snapshot and dispatches to the appropriate assignor based on
// g.assignorName. Updates targetAssignment on each consumerMember.
func (g *group) computeTargetAssignment(snap topicMetaSnap) {
	memberSubs := make(map[string]map[string]struct{}, len(g.consumerMembers))
	var allTPs []assignorTP

	subscribedSet := make(map[string]struct{})
	for mid, m := range g.consumerMembers {
		subs := make(map[string]struct{}, len(m.subscribedTopics))
		for _, t := range m.subscribedTopics {
			subs[t] = struct{}{}
		}
		if m.subscribedTopicRegex != nil {
			for topic := range snap {
				if m.subscribedTopicRegex.MatchString(topic) {
					subs[topic] = struct{}{}
				}
			}
		}
		memberSubs[mid] = subs
		for t := range subs {
			subscribedSet[t] = struct{}{}
		}
	}
	for topic := range subscribedSet {
		info, ok := snap[topic]
		if !ok {
			continue
		}
		for p := int32(0); p < info.partitions; p++ {
			allTPs = append(allTPs, assignorTP{topic: topic, id: info.id, part: p})
		}
	}

	// Sort deterministically: by topic name, then partition.
	slices.SortFunc(allTPs, func(a, b assignorTP) int {
		if c := cmp.Compare(a.topic, b.topic); c != 0 {
			return c
		}
		return cmp.Compare(a.part, b.part)
	})

	// Sort members by memberID for deterministic assignment.
	memberIDs := make([]string, 0, len(g.consumerMembers))
	for id := range g.consumerMembers {
		memberIDs = append(memberIDs, id)
	}
	slices.Sort(memberIDs)

	// Clear all target assignments.
	for _, m := range g.consumerMembers {
		m.targetAssignment = make(map[uuid][]int32)
	}

	if len(memberIDs) == 0 {
		return
	}

	switch g.assignorName {
	case "range":
		g.assignRange(allTPs, memberIDs, memberSubs)
	default: // "uniform" or "" (pre-assignor groups) - validated at heartbeat time
		g.assignUniform(allTPs, memberIDs, memberSubs)
	}

	// Sort partition lists for determinism.
	for _, m := range g.consumerMembers {
		for id := range m.targetAssignment {
			slices.Sort(m.targetAssignment[id])
		}
	}
}

// assignUniform distributes partitions round-robin across all eligible
// members.
func (g *group) assignUniform(allTPs []assignorTP, memberIDs []string, memberSubs map[string]map[string]struct{}) {
	idx := 0
	for _, tp := range allTPs {
		startIdx := idx
		for {
			mid := memberIDs[idx%len(memberIDs)]
			idx++
			if _, ok := memberSubs[mid][tp.topic]; ok {
				m := g.consumerMembers[mid]
				m.targetAssignment[tp.id] = append(m.targetAssignment[tp.id], tp.part)
				break
			}
			if idx-startIdx >= len(memberIDs) {
				break
			}
		}
	}
}

// assignRange distributes contiguous partition ranges per topic. For
// each topic, members subscribed to that topic (in sorted order) get
// a contiguous block. If partitions don't divide evenly, the first
// members get one extra partition.
func (g *group) assignRange(allTPs []assignorTP, memberIDs []string, memberSubs map[string]map[string]struct{}) {
	// Group TPs by topic. allTPs is sorted by (topic, partition),
	// so partitions for each topic are contiguous.
	type topicSlice struct {
		topic      string
		partitions []assignorTP
	}
	var topics []topicSlice
	for i, tp := range allTPs {
		if i == 0 || tp.topic != allTPs[i-1].topic {
			topics = append(topics, topicSlice{topic: tp.topic})
		}
		topics[len(topics)-1].partitions = append(topics[len(topics)-1].partitions, tp)
	}

	for _, ts := range topics {
		topic := ts.topic
		partitions := ts.partitions

		// Filter to members subscribed to this topic, preserving
		// the sorted order from memberIDs.
		var subs []string
		for _, mid := range memberIDs {
			if _, ok := memberSubs[mid][topic]; ok {
				subs = append(subs, mid)
			}
		}
		if len(subs) == 0 {
			continue
		}

		numP := len(partitions)
		numM := len(subs)
		minQuota := numP / numM
		extra := numP % numM
		nextRange := 0

		for _, mid := range subs {
			quota := minQuota
			if extra > 0 {
				quota++
				extra--
			}
			m := g.consumerMembers[mid]
			for _, tp := range partitions[nextRange : nextRange+quota] {
				m.targetAssignment[tp.id] = append(m.targetAssignment[tp.id], tp.part)
			}
			nextRange += quota
		}
	}
}

// reconciledAssignment returns the subset of m's target assignment that
// is safe to send right now. Partitions the member already owns (in
// currentAssignment) are always included; new partitions are only
// included if no other member currently owns them (partitionEpochs
// entry is absent).
func (g *group) reconciledAssignment(m *consumerMember) map[uuid][]int32 {
	result := make(map[uuid][]int32, len(m.targetAssignment))
	for id, parts := range m.targetAssignment {
		for _, p := range parts {
			if slices.Contains(m.currentAssignment[id], p) || g.currentPartitionEpoch(id, p) == -1 {
				result[id] = append(result[id], p)
			}
		}
	}
	return result
}

// Builds the response assignment from a partition map.
func makeAssignment(assigned map[uuid][]int32) *kmsg.ConsumerGroupHeartbeatResponseAssignment {
	a := new(kmsg.ConsumerGroupHeartbeatResponseAssignment)
	for id, parts := range assigned {
		t := kmsg.NewConsumerGroupHeartbeatResponseAssignmentTopic()
		t.TopicID = id
		t.Partitions = parts
		a.Topics = append(a.Topics, t)
	}
	return a
}

// Updates the consumer group state based on member reconciliation status.
func (g *group) updateConsumerStateField() {
	if len(g.consumerMembers) == 0 {
		g.state = groupEmpty
		return
	}
	for _, m := range g.consumerMembers {
		if !assignmentsEqual(m.currentAssignment, m.targetAssignment) || m.memberEpoch != g.generation {
			g.state = groupReconciling
			return
		}
	}
	g.state = groupStable
}

// atConsumerSessionTimeout sets up the session timeout for a consumer
// member. The session timeout uses the server-level config
// group.consumer.session.timeout.ms and fences the member entirely if
// no heartbeats are received within the timeout.
func (g *group) atConsumerSessionTimeout(m *consumerMember) {
	if m.t != nil {
		m.t.Stop()
	}
	timeout := time.Duration(g.c.consumerSessionTimeoutMs()) * time.Millisecond
	m.last = time.Now()
	m.t = g.timerControlFn(timeout, func() {
		if time.Since(m.last) >= timeout {
			g.fenceConsumerMember(m)
			delete(g.consumerMembers, m.memberID)
			g.generation++
			g.targetAssignmentsStale = true
			g.updateConsumerStateField()
		}
	})
}

// scheduleConsumerRebalanceTimeout starts a per-member rebalance
// timeout that fences the member if it does not complete partition
// revocation within rebalanceTimeoutMs. Only active when the member
// has partitions to release. If the member's epoch has advanced by
// the time the timer fires, the timeout is ignored.
func (g *group) scheduleConsumerRebalanceTimeout(m *consumerMember) {
	g.cancelConsumerRebalanceTimeout(m)
	timeout := time.Duration(m.rebalanceTimeoutMs) * time.Millisecond
	epoch := m.memberEpoch
	memberID := m.memberID
	m.tRebal = g.timerControlFn(timeout, func() {
		// Check the member still exists and hasn't
		// progressed past the epoch we were watching.
		cur, ok := g.consumerMembers[memberID]
		if !ok || cur.memberEpoch != epoch {
			return
		}
		g.fenceConsumerMember(cur)
		delete(g.consumerMembers, memberID)
		g.generation++
		g.targetAssignmentsStale = true
		g.updateConsumerStateField()
	})
}

func (g *group) cancelConsumerRebalanceTimeout(m *consumerMember) {
	if m.tRebal != nil {
		m.tRebal.Stop()
		m.tRebal = nil
	}
}

// fenceConsumerMember stops all timers and clears partition epoch
// entries for the member. The caller must delete the member from
// consumerMembers separately.
func (g *group) fenceConsumerMember(m *consumerMember) {
	if m.t != nil {
		m.t.Stop()
	}
	g.cancelConsumerRebalanceTimeout(m)
	g.removeAllPartitionEpochs(m.currentAssignment)
	g.removeAllPartitionEpochs(m.lastSentAssignment)
}

// Handles a commit for consumer groups with relaxed validation per KIP-1251.
func (g *group) handleConsumerOffsetCommit(creq *clientReq) *kmsg.OffsetCommitResponse {
	req := creq.kreq.(*kmsg.OffsetCommitRequest)
	resp := req.ResponseKind().(*kmsg.OffsetCommitResponse)

	if kerr := g.c.validateGroup(creq, req.Group); kerr != nil {
		fillOffsetCommit(req, resp, kerr.Code)
		return resp
	}
	if !g.c.allowedACL(creq, req.Group, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationRead) {
		fillOffsetCommit(req, resp, kerr.GroupAuthorizationFailed.Code)
		return resp
	}

	// Consumer groups require OffsetCommit v9+ (KIP-848). Older versions
	// use GenerationID which does not map to member epochs.
	if req.Version < 9 {
		fillOffsetCommit(req, resp, kerr.UnsupportedVersion.Code)
		return resp
	}

	// Empty group with negative epoch: accept without validation
	// (admin / kadm commits).
	if len(g.consumerMembers) == 0 && req.Generation < 0 {
		// Fall through to commit.
	} else if req.MemberID == "" {
		fillOffsetCommit(req, resp, kerr.UnknownMemberID.Code)
		return resp
	} else {
		// KIP-1251: Generation is the member epoch. Accept if
		// assignmentEpoch <= req epoch <= memberEpoch.
		m, ok := g.consumerMembers[req.MemberID]
		if !ok {
			fillOffsetCommit(req, resp, kerr.UnknownMemberID.Code)
			return resp
		}
		if req.Generation > m.memberEpoch || req.Generation < m.assignmentEpoch {
			fillOffsetCommit(req, resp, kerr.StaleMemberEpoch.Code)
			return resp
		}
		g.atConsumerSessionTimeout(m)
	}

	allowed := g.fillOffsetCommitWithACL(creq, req, resp)
	for _, t := range allowed {
		for _, p := range t.Partitions {
			g.commits.set(t.Topic, p.Partition, offsetCommit{
				offset:      p.Offset,
				leaderEpoch: p.LeaderEpoch,
				metadata:    p.Metadata,
			})
		}
	}
	return resp
}

func copyAssignment(a map[uuid][]int32) map[uuid][]int32 {
	c := make(map[uuid][]int32, len(a))
	for id, parts := range a {
		c[id] = slices.Clone(parts)
	}
	return c
}

// Returns true if every partition in owned is present in target.
func isSubsetAssignment(owned []kmsg.ConsumerGroupHeartbeatRequestTopic, target map[uuid][]int32) bool {
	for _, t := range owned {
		tParts, ok := target[t.TopicID]
		if !ok {
			return false
		}
		for _, p := range t.Partitions {
			if !slices.Contains(tParts, p) {
				return false
			}
		}
	}
	return true
}

func assignmentsEqual(a, b map[uuid][]int32) bool {
	if len(a) != len(b) {
		return false
	}
	for id, aParts := range a {
		bParts, ok := b[id]
		if !ok || !slices.Equal(aParts, bParts) {
			return false
		}
	}
	return true
}

// currentPartitionEpoch returns the epoch of the member that currently
// owns the given partition, or -1 if no member owns it.
func (g *group) currentPartitionEpoch(topicID uuid, partition int32) int32 {
	if pm := g.partitionEpochs[topicID]; pm != nil {
		if epoch, ok := pm[partition]; ok {
			return epoch
		}
	}
	return -1
}

// addPartitionEpochs records that a member at the given epoch owns the
// given partitions.
func (g *group) addPartitionEpochs(a map[uuid][]int32, epoch int32) {
	for id, parts := range a {
		pm := g.partitionEpochs[id]
		if pm == nil {
			pm = make(map[int32]int32, len(parts))
			g.partitionEpochs[id] = pm
		}
		for _, p := range parts {
			pm[p] = epoch
		}
	}
}

// removePartitionEpochs clears epoch entries for the given partitions,
// but only if the stored epoch matches expectedEpoch. This prevents a
// stale removal from clearing a newer owner's entry.
func (g *group) removePartitionEpochs(a map[uuid][]int32, expectedEpoch int32) {
	for id, parts := range a {
		pm := g.partitionEpochs[id]
		if pm == nil {
			continue
		}
		for _, p := range parts {
			if pm[p] == expectedEpoch {
				delete(pm, p)
			}
		}
		if len(pm) == 0 {
			delete(g.partitionEpochs, id)
		}
	}
}

// removeAllPartitionEpochs clears all epoch entries for the given
// partitions regardless of stored epoch. Used when a member leaves or
// is fenced and we know all their partitions should be freed.
func (g *group) removeAllPartitionEpochs(a map[uuid][]int32) {
	for id, parts := range a {
		pm := g.partitionEpochs[id]
		if pm == nil {
			continue
		}
		for _, p := range parts {
			delete(pm, p)
		}
		if len(pm) == 0 {
			delete(g.partitionEpochs, id)
		}
	}
}
