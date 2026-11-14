package kfake

import (
	"bytes"
	"cmp"
	"fmt"
	"maps"
	"math"
	"regexp"
	"slices"
	"strings"
	"time"
	"unicode/utf16"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type (
	groups struct {
		c  *Cluster
		gs map[string]*group
	}

	group struct {
		c    *Cluster
		name string
		typ  string

		state   groupState
		emptyAt time.Time // when classic group entered Empty state (for offset expiration)

		leader        string
		members       map[string]*groupMember
		pending       map[string]*groupMember
		staticMembers map[string]string // instanceID -> memberID; shared by classic and 848

		commits tps[offsetCommit]

		generation            int32
		groupEpoch            int32 // 848: bumped on subscription/membership change
		targetAssignmentEpoch int32 // 848: set to groupEpoch after target computation

		protocolType string
		protocols    map[string]int
		protocol     string

		nJoining int

		tRebalance     *time.Timer
		pendingSyncIDs map[string]struct{} // members that got JoinGroup but haven't sent SyncGroup yet
		tPendingSync   *time.Timer         // fires when pending sync members should be removed

		// KIP-848 consumer group fields
		consumerMembers map[string]*consumerMember
		partitionEpochs map[uuid]map[int32]partitionOwner // (topicID, partition) -> who owns it and at what epoch; absent means free
		subscribedTPs   []assignorTP                      // the subscribed partitions the last target assignment saw; notifyTopicChange skips the group if they still match
	}

	// partitionOwner is the member holding a partition and the epoch it
	// held when it took it. The epoch does not identify the member: two
	// members legitimately sit at the same epoch, both at the group's
	// target assignment epoch, so we key ownership on the member.
	partitionOwner struct {
		Member string `json:"member"`
		Epoch  int32  `json:"epoch"`
	}

	groupMember struct {
		memberID   string
		instanceID *string
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
		state               consumerMemberState

		// KIP-1251: per-partition assignment epoch. Each partition
		// tracks the member epoch at which it was first assigned.
		// OffsetCommit accepts reqEpoch >= partition assignment epoch.
		partAssignmentEpochs map[uuid]map[int32]int32

		rebalanceTimeoutMs int32
		serverAssignor     string

		subscribedTopics      []string
		subscribedTopicRegex  *regexp.Regexp
		subscribedRegexSource string // source string for change detection

		targetAssignment            map[uuid][]int32 // what server wants member to own
		partitionsPendingRevocation map[uuid][]int32 // told to revoke, awaiting client confirmation; contributes to epoch map
		lastReconciledSent          map[uuid][]int32 // exact reconciled assignment last included in a response

		t    *time.Timer // session timeout: fences member if no heartbeats
		last time.Time

		tRebal *time.Timer // rebalance timeout: fences member if slow to revoke
	}

	offsetCommit struct {
		offset      int64
		leaderEpoch int32
		metadata    *string
		lastCommit  time.Time
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

type consumerMemberState int8

const (
	cmStable               consumerMemberState = iota
	cmUnrevokedPartitions                      // waiting for client to release revoked partitions
	cmUnreleasedPartitions                     // waiting for other members to release partitions
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

// topicInfo returns a topic's ID and partition count, and whether we
// know the topic at all.
func (c *Cluster) topicInfo(topic string) (id uuid, parts int32, ok bool) {
	ps, ok := c.data.tps[topic]
	if !ok {
		return id, 0, false
	}
	return c.data.t2id[topic], int32(len(ps)), true
}

// dropGroupCommits removes every group's committed offsets for a deleted
// topic, as Kafka and Redpanda do, including those an open transaction
// would commit. The commits are gone when we return, so the DeleteTopics
// response we are answering cannot beat the drop.
func (c *Cluster) dropGroupCommits(topic string) {
	for _, g := range c.groups.gs {
		for part := range g.commits[topic] {
			g.deleteCommitAndPersist(topic, part)
		}
	}
	for pidinf := range c.pids.txs {
		for group, offsets := range pidinf.txOffsets {
			delete(offsets, topic)
			if len(offsets) == 0 {
				delete(pidinf.txOffsets, group)
			}
		}
	}
}

// notifyTopicChange recomputes target assignments for the consumer and
// share groups after a topic is created, deleted, or has partitions
// added. Every group is recomputed before we return, so the response to
// the request that changed the topic cannot beat the recomputation.
//
// Note that on a delete, DeleteTopics calls us after deleteTopic drops the
// topic from c.data: the recomputation no longer sees the topic and stops
// assigning its partitions.
//
// As in Kafka, a consumer group's epoch is bumped only if a topic it
// subscribes to (by name or regex) changed. The bump keeps heartbeat
// responses re-sending the assignment until the member epoch catches up,
// which only happens when the client confirms the assignment via the
// heartbeat Topics field. Without it, a client that receives an assignment
// with an unknown topic ID (metadata not yet refreshed) would miss the
// assignment permanently because the server recorded it as delivered.
func (c *Cluster) notifyTopicChange() {
	for _, g := range c.groups.gs {
		if len(g.consumerMembers) == 0 {
			continue
		}
		if _, tps := g.subscriptions(); slices.Equal(tps, g.subscribedTPs) {
			continue
		}
		g.groupEpoch++
		g.computeTargetAssignment()
		g.updateConsumerStateField()
		g.persistMeta848()
	}
	// Like Kafka, a share group's epoch moves only if the change touches
	// a topic it subscribes to, which always changes its assignment.
	for _, sg := range c.shareGroups.gs {
		if sg.recomputeAssignments() {
			sg.bumpEpoch()
		}
	}
}

// isCoordinator reports whether this request reached key's coordinator.
// Faults on a coordinator request fire only there; any other broker answers
// NOT_COORDINATOR.
func (c *Cluster) isCoordinator(creq *clientReq, key string) bool {
	return c.coordinator(key) == creq.cc.b
}

func (c *Cluster) validateGroup(creq *clientReq, group string) *kerr.Error {
	switch key := kmsg.Key(creq.kreq.Key()); key {
	case kmsg.OffsetCommit, kmsg.OffsetFetch, kmsg.DescribeGroups, kmsg.DeleteGroups:
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

func (g *group) logName() string { return g.name[:min(16, len(g.name))] }

func (gs *groups) newGroup(name string) *group {
	return &group{
		c:             gs.c,
		name:          name,
		typ:           "classic", // group-coordinator/src/main/java/org/apache/kafka/coordinator/group/Group.java
		members:       make(map[string]*groupMember),
		pending:       make(map[string]*groupMember),
		staticMembers: make(map[string]string),
		protocols:     make(map[string]int),
	}
}

// newOrExisting returns the group for name, creating it if it does not
// exist. The second return is whether we created it: if the very first
// request to a new group is invalid, the caller drops the group again.
func (gs *groups) newOrExisting(name string) (*group, bool) {
	if g := gs.gs[name]; g != nil {
		return g, false
	}
	g := gs.newGroup(name)
	gs.gs[name] = g
	return g, true
}

// handleJoin creates the group if needed and dispatches the join. A nil
// response means the member is parked in the join barrier and will be
// replied to when the rebalance completes.
func (gs *groups) handleJoin(creq *clientReq) kmsg.Response {
	req := creq.kreq.(*kmsg.JoinGroupRequest)
	resp := req.ResponseKind().(*kmsg.JoinGroupResponse)

	// Kafka checks the ACL, the group ID, the session timeout, and
	// routing, in that order, before it looks at the group.
	coordinator := gs.c.isCoordinator(creq, req.Group)
	if e := gs.c.deny(creq, req.Group, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationRead, faultKey{group: req.Group, misrouted: !coordinator}); e != nil {
		resp.ErrorCode = e.Code
		return resp
	}
	if req.Group == "" {
		resp.ErrorCode = kerr.InvalidGroupID.Code
		return resp
	}
	if st := req.SessionTimeoutMillis; st < gs.c.groupMinSessionTimeoutMs() || st > gs.c.groupMaxSessionTimeoutMs() {
		resp.ErrorCode = kerr.InvalidSessionTimeout.Code
		return resp
	}
	if !coordinator {
		resp.ErrorCode = kerr.NotCoordinator.Code
		return resp
	}
	if _, isShare := gs.c.shareGroups.gs[req.Group]; isShare {
		resp.ErrorCode = kerr.InconsistentGroupProtocol.Code
		return resp
	}
	// v0 has no rebalance timeout; Kafka uses the session timeout.
	if req.Version == 0 {
		req.RebalanceTimeoutMillis = req.SessionTimeoutMillis
	}

	// Kafka creates a group only for a join without a member ID.
	if gs.gs[req.Group] == nil && req.MemberID != "" {
		resp.MemberID = req.MemberID
		resp.ErrorCode = kerr.UnknownMemberID.Code
		return resp
	}
	g, isNew := gs.newOrExisting(req.Group)
	if g.typ == "consumer" {
		// Kafka turns an empty consumer group into a classic group,
		// keeping its offsets. We do not run classic members in a
		// group with consumer members, which Kafka can.
		if len(g.consumerMembers) > 0 {
			resp.ErrorCode = kerr.InconsistentGroupProtocol.Code
			return resp
		}
		g.typ = "classic"
		g.state = groupEmpty
		g.consumerMembers = nil
		g.partitionEpochs = nil
		g.subscribedTPs = nil
		g.groupEpoch, g.targetAssignmentEpoch = 0, 0
		g.persistClassicMeta()
	}
	kresp, ok := g.handleJoin(creq)
	if isNew && !ok {
		delete(gs.gs, req.Group)
	}
	return kresp
}

// memberReq checks a SyncGroup, Heartbeat, LeaveGroup, or OffsetDelete as
// Kafka does before it looks at the group: the ACL for op, then the group
// ID, then routing. It returns the group, or nil if there is none: Kafka
// answers a member of a missing group UNKNOWN_MEMBER_ID.
func (gs *groups) memberReq(creq *clientReq, group string, op kmsg.ACLOperation) (*group, *kerr.Error) {
	if e := gs.c.deny(creq, group, kmsg.ACLResourceTypeGroup, op, faultKey{group: group, misrouted: !gs.c.isCoordinator(creq, group)}); e != nil {
		return nil, e
	}
	if kerr := gs.c.validateGroup(creq, group); kerr != nil {
		return nil, kerr
	}
	return gs.gs[group], nil
}

func (gs *groups) handleOffsetCommit(creq *clientReq) *kmsg.OffsetCommitResponse {
	req := creq.kreq.(*kmsg.OffsetCommitRequest)
	resp := req.ResponseKind().(*kmsg.OffsetCommitResponse)
	defer func() { resp.Topics = append(resp.Topics, creq.offsetCommitErrTopics...) }()

	if e := gs.c.deny(creq, req.Group, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationRead, faultKey{group: req.Group, misrouted: !gs.c.isCoordinator(creq, req.Group)}); e != nil && creq.skipsWork(e) {
		fillOffsetCommit(req, resp, e.Code)
		return resp
	}

	// Kafka checks every topic before the group, and a commit with none
	// left never reaches the group.
	allowed, answers := gs.c.fillOffsetCommitWithACL(creq, req, resp)
	if len(allowed) == 0 {
		return resp
	}
	if kerr := gs.c.validateGroup(creq, req.Group); kerr != nil {
		failOffsetCommit(answers, kerr.Code)
		return resp
	}

	// Group type exclusivity: a share group holds no committed offsets,
	// and creating a consumer group under its name would list the id
	// twice. Kafka answers GROUP_ID_NOT_FOUND on every partition that
	// passes the topic checks.
	if _, isShare := gs.c.shareGroups.gs[req.Group]; isShare {
		failOffsetCommit(answers, kerr.GroupIDNotFound.Code)
		return resp
	}

	g, isNew := gs.newOrExisting(req.Group)
	var stored bool
	if g.typ == "consumer" {
		stored = g.handleConsumerOffsetCommit(creq, allowed, answers)
	} else {
		stored = g.handleOffsetCommit(creq, allowed, answers, isNew)
	}
	if isNew && !stored {
		delete(gs.gs, req.Group)
	}
	// Never nil: a commit is always answered, never parked. A nil
	// pointer here becomes a non-nil kmsg.Response in the caller, which
	// the loop would then try to write.
	return resp
}

func (gs *groups) handleOffsetDelete(creq *clientReq) *kmsg.OffsetDeleteResponse {
	req := creq.kreq.(*kmsg.OffsetDeleteRequest)
	resp := req.ResponseKind().(*kmsg.OffsetDeleteResponse)

	g, e := gs.memberReq(creq, req.Group, kmsg.ACLOperationDelete)
	if e != nil {
		resp.ErrorCode = e.Code
		return resp
	}
	// A share group holds no offsets to delete; Kafka answers
	// GROUP_ID_NOT_FOUND for it as for a missing group.
	if g == nil {
		resp.ErrorCode = kerr.GroupIDNotFound.Code
		return resp
	}
	return g.handleOffsetDelete(creq, resp)
}

func (gs *groups) handleList(creq *clientReq) *kmsg.ListGroupsResponse {
	req := creq.kreq.(*kmsg.ListGroupsRequest)
	resp := req.ResponseKind().(*kmsg.ListGroupsResponse)

	clusterDescribe := gs.c.allowedClusterACL(creq, kmsg.ACLOperationDescribe)
	for _, g := range gs.gs {
		if g.c.coordinator(g.name).node != creq.cc.b.node {
			continue
		}
		if !listFilterMatch(req, g.state.String(), g.typ) {
			continue
		}
		if gs.c.listDenied(creq, clusterDescribe, g.name) {
			continue
		}
		sg := kmsg.NewListGroupsResponseGroup()
		sg.Group = g.name
		sg.ProtocolType = g.protocolType
		if g.typ == "consumer" {
			sg.ProtocolType = "consumer"
		}
		sg.GroupState = g.state.String()
		sg.GroupType = g.typ
		resp.Groups = append(resp.Groups, sg)
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
		if e := gs.c.deny(creq, rg, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationDescribe, faultKey{group: rg, misrouted: !gs.c.isCoordinator(creq, rg)}); e != nil {
			sg.ErrorCode = e.Code
			continue
		}
		if kerr := gs.c.validateGroup(creq, rg); kerr != nil {
			sg.ErrorCode = kerr.Code
			continue
		}
		// Kafka describes only classic groups here; a consumer group
		// is as missing as a group that does not exist.
		g, ok := gs.gs[rg]
		if !ok || g.typ == "consumer" {
			sg.State = groupDead.String()
			if req.Version >= 6 {
				sg.ErrorCode = kerr.GroupIDNotFound.Code
				msg := fmt.Sprintf("Group %s not found.", rg)
				if ok {
					msg = fmt.Sprintf("Group %s is not a classic group.", rg)
				}
				sg.ErrorMessage = &msg
			}
			// Kafka fills operations only for a group with no error.
			if req.IncludeAuthorizedOperations && sg.ErrorCode == 0 {
				sg.AuthorizedOperations = gs.c.groupAuthorizedOps(creq, rg)
			}
			continue
		}
		sg.State = g.state.String()
		sg.ProtocolType = g.protocolType
		if g.state == groupStable {
			sg.Protocol = g.protocol
		}
		for _, m := range g.members {
			sm := kmsg.NewDescribeGroupsResponseGroupMember()
			sm.MemberID = m.memberID
			sm.InstanceID = m.instanceID
			sm.ClientID = m.clientID
			sm.ClientHost = m.clientHost
			sm.MemberAssignment = m.assignment
			if g.state == groupStable {
				for _, p := range m.join.Protocols {
					if p.Name == g.protocol {
						sm.ProtocolMetadata = p.Metadata
						break
					}
				}
			}
			sg.Members = append(sg.Members, sm)
		}
		if req.IncludeAuthorizedOperations {
			sg.AuthorizedOperations = gs.c.groupAuthorizedOps(creq, rg)
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

	seen := make(map[string]bool, len(req.Groups))
	for _, rg := range req.Groups {
		// Kafka answers each group once.
		if seen[rg] {
			continue
		}
		seen[rg] = true
		sg := doneg(rg)
		// setErr sets the code unless a fault already answered.
		setErr := func(code int16) {
			if sg.ErrorCode == 0 {
				sg.ErrorCode = code
			}
		}
		// ACL check: DELETE on Group
		if e := gs.c.deny(creq, rg, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationDelete, faultKey{group: rg, misrouted: !gs.c.isCoordinator(creq, rg)}); e != nil {
			sg.ErrorCode = e.Code
			if creq.skipsWork(e) { // a timed-out delete still deletes
				continue
			}
		}
		if kerr := gs.c.validateGroup(creq, rg); kerr != nil {
			setErr(kerr.Code)
			continue
		}
		g, ok := gs.gs[rg]
		if !ok {
			// Kafka deletes share groups through this API too;
			// there is no share-specific delete.
			if shg := gs.c.shareGroups.get(rg); shg != nil {
				if len(shg.members) > 0 {
					setErr(kerr.NonEmptyGroup.Code)
				} else {
					shg.kill()
				}
				continue
			}
			setErr(kerr.GroupIDNotFound.Code)
			continue
		}
		if g.typ == "consumer" {
			// A static member that left at epoch -2 is still a
			// member until its session ends.
			if len(g.consumerMembers) == 0 {
				g.kill()
			} else {
				setErr(kerr.NonEmptyGroup.Code)
			}
		} else {
			switch g.state {
			case groupDead:
				setErr(kerr.GroupIDNotFound.Code)
			case groupEmpty:
				g.kill()
			case groupPreparingRebalance, groupCompletingRebalance, groupStable, groupReconciling:
				setErr(kerr.NonEmptyGroup.Code)
			}
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
			// v0-1 has no top-level ErrorCode on the wire, so a
			// group error goes on every requested partition.
			if req.Version <= 1 && g0.ErrorCode != 0 && len(g0.Topics) == 0 {
				for _, t := range req.Topics {
					st := kmsg.NewOffsetFetchResponseTopic()
					st.Topic = t.Topic
					for _, p := range t.Partitions {
						sp := kmsg.NewOffsetFetchResponseTopicPartition()
						sp.Partition = p
						sp.Offset = -1
						sp.ErrorCode = g0.ErrorCode
						st.Partitions = append(st.Partitions, sp)
					}
					resp.Topics = append(resp.Topics, st)
				}
				return
			}
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
		if e := gs.c.deny(creq, rg.Group, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationDescribe, faultKey{group: rg.Group, misrouted: !gs.c.isCoordinator(creq, rg.Group)}); e != nil {
			sg.ErrorCode = e.Code
			continue
		}
		if kerr := gs.c.validateGroup(creq, rg.Group); kerr != nil {
			sg.ErrorCode = kerr.Code
			continue
		}

		// v10: resolve TopicIDs to topic names before processing.
		if req.Version >= 10 && rg.Topics != nil {
			for i := range rg.Topics {
				t := &rg.Topics[i]
				if name, ok := gs.c.data.id2t[t.TopicID]; ok {
					t.Topic = name
				}
			}
		}

		// Kafka answers a group that does not exist with no offsets.
		g, ok := gs.gs[rg.Group]
		if !ok {
			g = gs.newGroup(rg.Group)
		}
		g.fillOffsetFetch(creq, req, rg, sg)
	}
	return resp
}

// fillOffsetFetch fills one group's portion of an OffsetFetch response.
func (g *group) fillOffsetFetch(creq *clientReq, req *kmsg.OffsetFetchRequest, rg kmsg.OffsetFetchRequestGroup, sg *kmsg.OffsetFetchResponseGroup) {
	// KIP-848: validate MemberID/MemberEpoch for consumer
	// groups. Admin fetches (MemberID absent with
	// MemberEpoch < 0) skip validation. MemberID="" is a
	// real-but-invalid memberId per Java, not an admin
	// signal - it falls through to the member lookup and
	// returns UnknownMemberID.
	adminFetch := rg.MemberID == nil && rg.MemberEpoch < 0
	if g.typ == "consumer" && !adminFetch {
		if rg.MemberID == nil || *rg.MemberID == "" {
			sg.ErrorCode = kerr.UnknownMemberID.Code
			return
		}
		m, ok := g.consumerMembers[*rg.MemberID]
		if !ok {
			sg.ErrorCode = kerr.UnknownMemberID.Code
			return
		}
		if rg.MemberEpoch != m.memberEpoch {
			sg.ErrorCode = kerr.StaleMemberEpoch.Code
			return
		}
	}
	// KIP-447: an offset that an open transaction commits is unstable
	// until the transaction ends.
	unstable := func(t string, p int32) bool {
		return req.RequireStable && g.c.pids.hasUnstableOffset(g.name, t, p)
	}
	if rg.Topics == nil {
		for t, ps := range g.commits {
			// Kafka leaves out the topics you may not describe.
			id := g.c.data.t2id[t]
			if g.c.deny(creq, t, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationDescribe, faultKey{group: g.name, topic: t, topicID: id}) != nil {
				continue
			}
			st := kmsg.NewOffsetFetchResponseGroupTopic()
			st.Topic = t
			st.TopicID = id
			for p, c := range ps {
				sp := kmsg.NewOffsetFetchResponseGroupTopicPartition()
				sp.Partition = p
				if unstable(t, p) {
					sp.ErrorCode = kerr.UnstableOffsetCommit.Code
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
	} else {
		for _, t := range rg.Topics {
			st := kmsg.NewOffsetFetchResponseGroupTopic()
			st.Topic = t.Topic
			st.TopicID = t.TopicID
			tk := faultKey{group: g.name, topic: t.Topic, topicID: t.TopicID}
			// From v10, a topic is named by ID, and an ID we do not
			// know leaves the name empty.
			unknownID := req.Version >= 10 && t.Topic == ""
			var terr *kerr.Error
			if !unknownID {
				terr = g.c.deny(creq, t.Topic, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationDescribe, tk)
			}
			for _, p := range t.Partitions {
				sp := kmsg.NewOffsetFetchResponseGroupTopicPartition()
				sp.Partition = p
				sp.Offset = -1
				sp.LeaderEpoch = -1
				e := terr
				if e == nil {
					e = creq.faults.check(tk.part(p))
				}
				if e == nil && unknownID {
					e = kerr.UnknownTopicID
				}
				switch {
				case e != nil:
					sp.ErrorCode = e.Code
				case unstable(t.Topic, p):
					sp.ErrorCode = kerr.UnstableOffsetCommit.Code
				default:
					if c, ok := g.commits.getp(t.Topic, p); ok {
						sp.Offset = c.offset
						sp.LeaderEpoch = c.leaderEpoch
						sp.Metadata = c.metadata
					}
				}
				st.Partitions = append(st.Partitions, sp)
			}
			sg.Topics = append(sg.Topics, st)
		}
	}
}

func (g *group) handleOffsetDelete(creq *clientReq, resp *kmsg.OffsetDeleteResponse) *kmsg.OffsetDeleteResponse {
	req := creq.kreq.(*kmsg.OffsetDeleteRequest)

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
	donep := func(t string, p int32, errCode int16) {
		sp := kmsg.NewOffsetDeleteResponseTopicPartition()
		sp.Partition = p
		sp.ErrorCode = errCode
		st := donet(t)
		st.Partitions = append(st.Partitions, sp)
	}

	// 848 consumer group, any state: delete everything not subscribed to
	// classic, empty: delete everything in request
	// classic, preparingRebalance, completingRebalance, stable:
	//   * if consumer, delete everything not subscribed to
	//   * if not consumer, delete nothing, error with non_empty_group
	var subTopics map[string]struct{}
	switch {
	case g.typ == "consumer":
		subTopics = g.consumerSubscribedTopics()
	case g.state == groupEmpty:
	case g.state == groupPreparingRebalance, g.state == groupCompletingRebalance, g.state == groupStable:
		if g.protocolType != "consumer" {
			resp.ErrorCode = kerr.NonEmptyGroup.Code
			return resp
		}
		subTopics = g.classicSubscribedTopics()
	default:
		resp.ErrorCode = kerr.GroupIDNotFound.Code
		return resp
	}

	for _, t := range req.Topics {
		tk := faultKey{group: req.Group, topic: t.Topic}
		_, nparts, topicExists := g.c.topicInfo(t.Topic)
		var topicErr int16
		switch e := g.c.deny(creq, t.Topic, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationRead, tk); {
		case e != nil:
			topicErr = e.Code
		case !topicExists:
			topicErr = kerr.UnknownTopicOrPartition.Code
		}
		_, subscribed := subTopics[t.Topic]
		for _, p := range t.Partitions {
			if topicErr != 0 {
				donep(t.Topic, p.Partition, topicErr)
				continue
			}
			if e := creq.faults.check(tk.part(p.Partition)); e != nil {
				donep(t.Topic, p.Partition, e.Code)
				continue
			}
			switch {
			case p.Partition < 0 || p.Partition >= nparts:
				donep(t.Topic, p.Partition, kerr.UnknownTopicOrPartition.Code)
			case subscribed:
				donep(t.Topic, p.Partition, kerr.GroupSubscribedToTopic.Code)
			default:
				if _, ok := g.commits.getp(t.Topic, p.Partition); ok {
					g.deleteCommitAndPersist(t.Topic, p.Partition)
				}
				// The delete also drops a commit an open
				// transaction holds, so ending the transaction
				// does not bring the offset back.
				g.dropTxnOffset(t.Topic, p.Partition)
				donep(t.Topic, p.Partition, 0)
			}
		}
	}

	return resp
}

// dropTxnOffset drops the commit every open transaction holds for the
// group's partition.
func (g *group) dropTxnOffset(topic string, part int32) {
	for pidinf := range g.c.pids.txs {
		offsets, ok := pidinf.txOffsets[g.name]
		if !ok {
			continue
		}
		offsets.delp(topic, part)
		if len(offsets) == 0 {
			delete(pidinf.txOffsets, g.name)
		}
	}
}

////////////////////
// GROUP HANDLING //
////////////////////

// stopTimers stops every timer the group owns. An unstopped timer keeps
// the group, and through it the cluster, alive in the runtime timer heap
// until it fires.
func (g *group) stopTimers() {
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
		g.cancelConsumerRebalanceTimeout(m)
	}
	if g.tRebalance != nil {
		g.tRebalance.Stop()
	}
	if g.tPendingSync != nil {
		g.tPendingSync.Stop()
	}
}

// kill marks the group dead and drops it from the cluster, along with
// its offsets in open transactions: as in Kafka, committing such a
// transaction later must not bring the group back. Timers that fired
// before this see the group is gone and do nothing.
//
// The group must be empty: a member with a parked JoinGroup or SyncGroup
// is dropped here with no reply, leaving the client to time out.
func (g *group) kill() {
	g.state = groupDead
	g.stopTimers()
	delete(g.c.groups.gs, g.name)
	for pidinf := range g.c.pids.txs {
		delete(pidinf.txOffsets, g.name)
	}
	g.c.persistGroupEntry(groupLogEntry{Type: "deleteGroup", Group: g.name})
}

// Handles a join. We do not do the delayed join aspects in Kafka, we just punt
// to the client to immediately rejoin if a new client enters the group.
//
// If this returns nil, the request will be replied to later.
func (g *group) handleJoin(creq *clientReq) (kmsg.Response, bool) {
	req := creq.kreq.(*kmsg.JoinGroupRequest)
	resp := req.ResponseKind().(*kmsg.JoinGroupResponse)

	// Like Kafka, a member we have no room for leaves the group.
	if !g.acceptJoin(req.MemberID) {
		if m := g.members[req.MemberID]; m != nil {
			g.removeMember(m)
		} else if m := g.pending[req.MemberID]; m != nil {
			g.stopPending(m)
		}
		resp.ErrorCode = kerr.GroupMaxSizeReached.Code
		return resp, true
	}

	if !g.protocolsMatch(req.ProtocolType, req.Protocols) {
		resp.ErrorCode = kerr.InconsistentGroupProtocol.Code
		return resp, req.MemberID == "" // Kafka keeps a group a new member's refused join created
	}

	// Clients first join with no member ID. For join v4+, we generate
	// the member ID and add the member to pending. For v3 and below,
	// we immediately enter rebalance. Static members (instanceID set)
	// may rejoin with an empty memberID - check the static mapping.
	if req.MemberID == "" {
		// Kafka maps an instance ID only to a live member. We can keep a
		// mapping whose member did not survive a restart, and a join for
		// it is a new member's.
		if req.InstanceID != nil {
			if old := g.members[g.staticMembers[*req.InstanceID]]; old != nil {
				return g.replaceStaticMember(old, creq, req, resp)
			}
		}
		memberID := generateMemberID(creq.cid, req.InstanceID)
		resp.MemberID = memberID
		m := &groupMember{
			memberID:   memberID,
			instanceID: req.InstanceID,
			clientID:   creq.cid,
			clientHost: "/" + creq.clientHost(),
			join:       req,
		}
		if req.InstanceID != nil {
			g.staticMembers[*req.InstanceID] = memberID
			g.persistStaticMember(*req.InstanceID, memberID)
		}
		// Java's requiresKnownMemberId gate only triggers for DYNAMIC
		// members; static members (groupInstanceId != null) are
		// accepted with empty MemberID on v4+ JoinGroup and join
		// immediately.
		if req.Version >= 4 && req.InstanceID == nil {
			g.addPendingRebalance(m)
			resp.ErrorCode = kerr.MemberIDRequired.Code
			return resp, true
		}
		g.addMemberAndRebalance(m, creq, req)
		return nil, true
	}

	// Validate instanceID consistency for known members.
	if req.InstanceID != nil {
		if err := g.validateInstanceID(req.InstanceID, req.MemberID); err != nil {
			resp.ErrorCode = err.Code
			return resp, false
		}
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
		// Leader rejoining OR any member with changed metadata: trigger rebalance
		g.updateMemberAndRebalance(m, creq, req)
	}
	return nil, true
}

// acceptJoin reports whether the group has room for a join from memberID, as
// Kafka counts it: an empty group takes every join, a rebalancing group counts
// the members waiting in a join, and otherwise we count every member. A join
// without a member ID, a static member's replacement included, is a new
// member's.
func (g *group) acceptJoin(memberID string) bool {
	maxSize := int(g.c.groupMaxSize())
	m := g.members[memberID]
	switch g.state {
	case groupEmpty, groupDead:
		return true
	case groupPreparingRebalance:
		if m != nil && !m.waitingReply.empty() {
			if _, joining := m.waitingReply.kreq.(*kmsg.JoinGroupRequest); joining {
				return true
			}
		}
		return g.nJoining < maxSize
	default:
		return m != nil || len(g.members) < maxSize
	}
}

// replaceStaticMember handles a static member rejoining with the same
// instanceID. If the group is stable and the selected protocol would not
// change, we skip the rebalance (KIP-345).
func (g *group) replaceStaticMember(old *groupMember, creq *clientReq, req *kmsg.JoinGroupRequest, resp *kmsg.JoinGroupResponse) (kmsg.Response, bool) {
	// Generate a new memberID for the replacement.
	memberID := generateMemberID(creq.cid, req.InstanceID)

	// Create the new member, preserving the old assignment.
	m := &groupMember{
		memberID:   memberID,
		instanceID: req.InstanceID,
		clientID:   creq.cid,
		clientHost: "/" + creq.clientHost(),
		join:       req,
		assignment: old.assignment,
	}

	// Fence the old member: if it's waiting for a reply, send FENCED_INSTANCE_ID.
	if !old.waitingReply.empty() {
		oldResp := old.waitingReply.kreq.ResponseKind()
		switch r := oldResp.(type) {
		case *kmsg.JoinGroupResponse:
			r.ErrorCode = kerr.FencedInstanceID.Code
		case *kmsg.SyncGroupResponse:
			r.ErrorCode = kerr.FencedInstanceID.Code
		}
		g.reply(old.waitingReply, oldResp, nil)
	}

	// Remove old member state without triggering rebalance.
	g.removeMember(old)

	// Register new member in static mapping.
	g.staticMembers[*req.InstanceID] = memberID
	g.persistStaticMember(*req.InstanceID, memberID)
	g.members[memberID] = m
	for _, p := range m.join.Protocols {
		g.protocols[p.Name]++
	}
	// The replacement inherits leadership, in a rebalance as well.
	oldLeader := g.leader
	if g.leader == old.memberID {
		g.leader = memberID
	}

	// If the group is stable and the protocol we would select is
	// unchanged, skip the rebalance and keep the old assignment, whatever
	// the new metadata says (KIP-345). For the leader, set SkipAssignment
	// so it knows to re-send the existing assignment without running the
	// balancer (KIP-814).
	if g.state == groupStable && g.selectProtocol() == g.protocol {
		g.updateHeartbeat(m)
		g.fillJoinResp(memberID, resp)
		switch {
		case req.Version < 9:
			// Without SkipAssignment, Kafka names the leader from
			// before the replacement and sends no members, so a
			// replaced leader does not run the balancer.
			resp.LeaderID = oldLeader
			resp.Members = nil
		case resp.LeaderID == memberID:
			resp.SkipAssignment = true
		}
		return resp, true
	}

	// Otherwise trigger a rebalance, or join the one in progress.
	g.nJoining++
	m.waitingReply = creq
	g.rebalance()
	return nil, true
}

// Handles a sync, which can transition us to stable.
func (g *group) handleSync(creq *clientReq) kmsg.Response {
	req := creq.kreq.(*kmsg.SyncGroupRequest)
	resp := req.ResponseKind().(*kmsg.SyncGroupResponse)

	if err := g.validateInstanceID(req.InstanceID, req.MemberID); err != nil {
		resp.ErrorCode = err.Code
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
		delete(g.pendingSyncIDs, req.MemberID)
		// Kafka answers the sync this one replaces NOT_COORDINATOR.
		if !m.waitingReply.empty() {
			oldResp := m.waitingReply.kreq.ResponseKind()
			if r, ok := oldResp.(*kmsg.SyncGroupResponse); ok {
				r.ErrorCode = kerr.NotCoordinator.Code
				m.waitingReply.reply(r)
			}
		}
		m.waitingReply = creq
		if req.MemberID == g.leader {
			g.completeLeaderSync(req)
		}
		return nil
	case groupStable: // member saw join and is now finally calling sync
		delete(g.pendingSyncIDs, req.MemberID)
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

	if err := g.validateInstanceID(req.InstanceID, req.MemberID); err != nil {
		resp.ErrorCode = err.Code
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

// Handles a leave; g is nil for a group that does not exist. We trigger a
// rebalance for every member leaving in a batch request, but that's fine:
// the whole batch runs on the cluster loop before anything else touches the
// group.
func (g *group) handleLeave(creq *clientReq) kmsg.Response {
	req := creq.kreq.(*kmsg.LeaveGroupRequest)
	resp := req.ResponseKind().(*kmsg.LeaveGroupResponse)

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

		if e := creq.faults.check(faultKey{group: req.Group, resource: rm.MemberID}); e != nil {
			r.ErrorCode = e.Code
			continue
		}

		if g == nil {
			// Kafka answers each member of a missing group so.
			r.ErrorCode = kerr.UnknownMemberID.Code
			continue
		}
		if g.typ == "consumer" {
			r.ErrorCode = g.leaveConsumerMember(rm)
			continue
		}

		// Resolve memberID from instanceID for static members. An
		// admin names only the instance; a member that also names
		// itself must still hold the instance.
		if rm.InstanceID != nil {
			memberID, ok := g.staticMembers[*rm.InstanceID]
			if !ok {
				r.ErrorCode = kerr.UnknownMemberID.Code
				continue
			}
			if rm.MemberID != "" && rm.MemberID != memberID {
				r.ErrorCode = kerr.FencedInstanceID.Code
				continue
			}
			rm.MemberID = memberID
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

// leaveConsumerMember removes a member of a consumer group that a
// LeaveGroup names, as Kafka does for an admin removing members, and
// returns the member's error code.
func (g *group) leaveConsumerMember(rm kmsg.LeaveGroupRequestMember) int16 {
	var m *consumerMember
	if rm.InstanceID == nil {
		m = g.consumerMembers[rm.MemberID]
	} else {
		m = g.consumerMembers[g.staticMembers[*rm.InstanceID]]
		if m != nil && rm.MemberID != "" && rm.MemberID != m.memberID {
			return kerr.FencedInstanceID.Code
		}
	}
	if m == nil {
		return kerr.UnknownMemberID.Code
	}
	g.evictConsumerMember(m)
	return 0
}

func fillOffsetCommit(req *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, code int16) {
	for _, t := range req.Topics {
		st := kmsg.NewOffsetCommitResponseTopic()
		st.Topic = t.Topic
		st.TopicID = t.TopicID
		for _, p := range t.Partitions {
			sp := kmsg.NewOffsetCommitResponseTopicPartition()
			sp.Partition = p.Partition
			sp.ErrorCode = code
			st.Partitions = append(st.Partitions, sp)
		}
		resp.Topics = append(resp.Topics, st)
	}
}

// failOffsetCommit answers code on every allowed partition's answer.
func failOffsetCommit(answers []*kmsg.OffsetCommitResponseTopicPartition, code int16) {
	for _, sp := range answers {
		sp.ErrorCode = code
	}
}

// commitAllowed stores each offset in allowed and returns whether it stored
// any. Like Kafka's coordinator, it answers OFFSET_METADATA_TOO_LARGE for an
// offset whose metadata is too large.
func (g *group) commitAllowed(answers []*kmsg.OffsetCommitResponseTopicPartition, allowed []kmsg.OffsetCommitRequestTopic) bool {
	var stored bool
	var i int
	for _, t := range allowed {
		for _, p := range t.Partitions {
			sp := answers[i]
			i++
			if offsetMetadataTooLarge(p.Metadata) {
				sp.ErrorCode = kerr.OffsetMetadataTooLarge.Code
				continue
			}
			g.commitAndPersist(t.Topic, p.Partition, offsetCommit{
				offset:      p.Offset,
				leaderEpoch: p.LeaderEpoch,
				metadata:    p.Metadata,
			})
			stored = true
		}
	}
	return stored
}

// fillOffsetCommitWithACL fills the response with per-topic ACL checks and
// per-partition existence checks. A real broker's API layer rejects commits
// for partitions its metadata does not know with UNKNOWN_TOPIC_OR_PARTITION
// before the group coordinator ever sees them (auth is checked first).
// Returns the topics+partitions that passed, which the group checks, and the
// answer of each in order; a group error goes only on these.
func (c *Cluster) fillOffsetCommitWithACL(creq *clientReq, req *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse) ([]kmsg.OffsetCommitRequestTopic, []*kmsg.OffsetCommitResponseTopicPartition) {
	var (
		allowed []kmsg.OffsetCommitRequestTopic
		answers []*kmsg.OffsetCommitResponseTopicPartition
	)
	misrouted := !c.isCoordinator(creq, req.Group)
	for _, t := range req.Topics {
		st := kmsg.NewOffsetCommitResponseTopic()
		st.Topic = t.Topic
		st.TopicID = t.TopicID
		tk := faultKey{group: req.Group, topic: t.Topic, topicID: t.TopicID, misrouted: misrouted}
		if e := c.deny(creq, t.Topic, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationRead, tk); e != nil && creq.skipsWork(e) { // a timed-out commit falls through to the per-partition checks
			for _, p := range t.Partitions {
				sp := kmsg.NewOffsetCommitResponseTopicPartition()
				sp.Partition = p.Partition
				sp.ErrorCode = e.Code
				st.Partitions = append(st.Partitions, sp)
			}
		} else {
			_, nparts, topicExists := c.topicInfo(t.Topic)
			at := t
			at.Partitions = nil
			var passed []int
			for j, p := range t.Partitions {
				sp := kmsg.NewOffsetCommitResponseTopicPartition()
				sp.Partition = p.Partition
				e := creq.faults.check(tk.part(p.Partition))
				switch {
				case e != nil && creq.skipsWork(e): // a timed-out commit is still stored
					sp.ErrorCode = e.Code
				case !topicExists || p.Partition < 0 || p.Partition >= nparts:
					sp.ErrorCode = kerr.UnknownTopicOrPartition.Code
				default:
					if e != nil {
						sp.ErrorCode = e.Code
					}
					at.Partitions = append(at.Partitions, p)
					passed = append(passed, j)
				}
				st.Partitions = append(st.Partitions, sp)
			}
			for _, j := range passed {
				answers = append(answers, &st.Partitions[j])
			}
			if len(at.Partitions) > 0 {
				allowed = append(allowed, at)
			}
		}
		resp.Topics = append(resp.Topics, st)
	}
	return allowed, answers
}

// offsetMetadataTooLarge reports whether commit metadata is over 4096,
// Kafka's default offset.metadata.max.bytes, which Kafka counts in UTF-16
// code units. A string has no more UTF-16 units than UTF-8 bytes.
func offsetMetadataTooLarge(metadata *string) bool {
	if metadata == nil || len(*metadata) <= 4096 {
		return false
	}
	var n int
	for _, r := range *metadata {
		n += utf16.RuneLen(r)
	}
	return n > 4096
}

// Handles a commit to a classic group: allowed and answers are the
// partitions that passed the topic checks. isNew is whether the commit
// created the group. Returns whether the commit stored an offset.
func (g *group) handleOffsetCommit(creq *clientReq, allowed []kmsg.OffsetCommitRequestTopic, answers []*kmsg.OffsetCommitResponseTopicPartition, isNew bool) bool {
	req := creq.kreq.(*kmsg.OffsetCommitRequest)
	fail := func(code int16) bool {
		failOffsetCommit(answers, code)
		return false
	}

	// A commit without a generation to an empty group is a simple
	// commit, whatever member it names. Anything else must come from
	// a member of this generation.
	var m *groupMember
	switch {
	case isNew && req.Generation >= 0:
		if req.Version >= 9 {
			return fail(kerr.GroupIDNotFound.Code)
		}
		return fail(kerr.IllegalGeneration.Code)
	case req.Generation < 0 && g.state == groupEmpty:
	default:
		if err := g.validateInstanceID(req.InstanceID, req.MemberID); err != nil {
			return fail(err.Code)
		}
		var ok bool
		if m, ok = g.members[req.MemberID]; !ok {
			return fail(kerr.UnknownMemberID.Code)
		}
		if req.Generation != g.generation {
			return fail(kerr.IllegalGeneration.Code)
		}
		if g.state == groupCompletingRebalance {
			return fail(kerr.RebalanceInProgress.Code)
		}
	}

	stored := g.commitAllowed(answers, allowed)
	if m != nil {
		g.updateHeartbeat(m)
	}
	return stored
}

// Transitions the group to the preparing rebalance state. We first need to
// clear any member that is currently sitting in sync. If enough members have
// entered join, we immediately proceed to completeRebalance, otherwise we
// wait, starting the deadline if the rebalance just began.
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
	g.clearPendingSync()

	if g.nJoining >= len(g.members) {
		g.completeRebalance()
		return
	}

	// Kafka sets the deadline when the rebalance begins, from the largest
	// rebalance timeout at that moment. A join or leave during the
	// rebalance does not extend it. The timer is nil outside of
	// PreparingRebalance.
	if g.tRebalance != nil {
		return
	}
	// The deadline belongs to this generation. completeRebalance's state
	// check alone misses preparing to completing and back to preparing: a
	// group back in preparing under a new generation has a new barrier,
	// and a late run would clear it. Repeated rebalance() calls at the
	// same generation are fine, completing early is what the deadline
	// means.
	gen := g.generation
	g.tRebalance = g.timerWork(time.Duration(g.maxRebalanceTimeoutMs())*time.Millisecond, func() {
		if g.generation != gen {
			return
		}
		g.completeRebalance()
	})
}

// Ends the join phase: the group moves to CompletingRebalance, or to Empty if
// no member remains. Dynamic members that did not rejoin are removed. Static
// members that did not rejoin stay with their stored metadata, as in Kafka,
// until their session times out, they leave, or they are replaced.
func (g *group) completeRebalance() {
	// A timer that already fired cannot be retracted. If the rebalance
	// completed inline since, every member is waiting in sync with an
	// empty waitingReply and a late run would remove all of them.
	if g.state != groupPreparingRebalance {
		return
	}
	if g.tRebalance != nil {
		g.tRebalance.Stop()
		g.tRebalance = nil
	}
	g.nJoining = 0

	var (
		leaderJoined bool
		joined       string
	)
	for _, m := range g.members {
		if m.waitingReply.empty() {
			if m.instanceID == nil {
				g.removeMember(m)
			}
			continue
		}
		joined = m.memberID
		if m.memberID == g.leader {
			leaderJoined = true
		}
	}
	// Only static members that did not rejoin remain. Kafka has no
	// leader to give the next generation to, so it waits another
	// rebalance timeout.
	if joined == "" && len(g.members) > 0 {
		g.rebalance()
		return
	}

	g.generation++
	if g.generation < 0 {
		g.generation = 1
	}
	if len(g.members) == 0 {
		g.state = groupEmpty
		g.emptyAt = time.Now()
		g.persistClassicMeta()
		return
	}
	if !leaderJoined {
		g.leader = joined
	}
	g.state = groupCompletingRebalance
	g.protocol = g.selectProtocol()
	g.persistClassicMeta()

	// Track which members need to send SyncGroup. Kafka restarts every
	// member's session timer here, including a static member that did
	// not rejoin.
	g.pendingSyncIDs = make(map[string]struct{}, len(g.members))
	for _, m := range g.members {
		g.pendingSyncIDs[m.memberID] = struct{}{}
		if m.waitingReply.empty() {
			g.updateHeartbeat(m)
			continue
		}
		req := m.join
		resp := req.ResponseKind().(*kmsg.JoinGroupResponse)
		g.fillJoinResp(m.memberID, resp)
		g.reply(m.waitingReply, resp, m)
	}

	// Start pending sync timeout: remove members that don't send
	// SyncGroup within the rebalance timeout.
	if g.tPendingSync != nil {
		g.tPendingSync.Stop()
	}
	gen := g.generation
	g.tPendingSync = g.timerWork(time.Duration(g.maxRebalanceTimeoutMs())*time.Millisecond, func() {
		// An empty barrier means everyone synced. A different
		// generation means this timer belongs to a barrier that is
		// already over and the members waiting now are not ours.
		if g.generation != gen || len(g.pendingSyncIDs) == 0 {
			return
		}
		// Remove all pending members, then trigger one rebalance.
		for id := range g.pendingSyncIDs {
			if m := g.members[id]; m != nil {
				g.removeMember(m)
			}
		}
		g.pendingSyncIDs = nil
		g.rebalance()
	})
}

// Kafka-style protocol voting: find candidate protocols (supported by all
// members), then each member votes for their most-preferred candidate. The
// protocol with the most votes wins.
//
// On a tie Kafka keeps the first protocol in the iteration order of the Java
// HashMap holding the votes: by bucket, then by the order of each protocol's
// first vote, which follows the iteration order of its member map. We order
// both maps the same way, except that within a member bucket we go by member
// ID where Kafka goes by join order, and we size the member table for the
// current members where Kafka's keeps the size it once grew to.
func (g *group) selectProtocol() string {
	candidates := make(map[string]struct{})
	for proto, nsupport := range g.protocols {
		if nsupport == len(g.members) {
			candidates[proto] = struct{}{}
		}
	}
	if len(candidates) == 0 {
		panic(fmt.Sprint("unable to find commonly supported protocol!", g.protocols, len(g.members)))
	}

	type bucketed struct {
		m      *groupMember
		bucket uint32
	}
	members := make([]bucketed, 0, len(g.members))
	for _, m := range g.members {
		members = append(members, bucketed{m, javaHashBucket(m.memberID, len(g.members))})
	}
	slices.SortFunc(members, func(a, b bucketed) int {
		return cmp.Or(cmp.Compare(a.bucket, b.bucket), cmp.Compare(a.m.memberID, b.m.memberID))
	})

	votes := make(map[string]int, len(candidates))
	var voted []string // in first-vote order
	for _, b := range members {
		for _, p := range b.m.join.Protocols {
			if _, ok := candidates[p.Name]; ok {
				if votes[p.Name] == 0 {
					voted = append(voted, p.Name)
				}
				votes[p.Name]++
				break
			}
		}
	}
	slices.SortStableFunc(voted, func(a, b string) int {
		return cmp.Compare(javaHashBucket(a, len(voted)), javaHashBucket(b, len(voted)))
	})
	var best string
	for i, p := range voted {
		if i == 0 || votes[p] > votes[best] {
			best = p
		}
	}
	return best
}

// javaHashBucket returns the bucket of key in a Java HashMap holding size
// string keys. The table starts at 16 buckets and doubles once it is more
// than 3/4 full. The bucket is String.hashCode, which hashes UTF-16 code
// units, with its high 16 bits folded into the low 16.
func javaHashBucket(key string, size int) uint32 {
	n := 16
	for size > n/4*3 {
		n *= 2
	}
	var h uint32
	for _, c := range utf16.Encode([]rune(key)) {
		h = 31*h + uint32(c)
	}
	return (h ^ h>>16) & uint32(n-1)
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
			continue
		}
		resp := m.waitingReply.kreq.ResponseKind().(*kmsg.SyncGroupResponse)
		resp.ProtocolType = kmsg.StringPtr(g.protocolType)
		resp.Protocol = kmsg.StringPtr(g.protocol)
		resp.MemberAssignment = m.assignment
		g.reply(m.waitingReply, resp, m)
	}
	// Members that have not synced stay pending: as Kafka does, the
	// pending sync timer removes them in Stable too.
	g.state = groupStable
}

func (g *group) clearPendingSync() {
	g.pendingSyncIDs = nil
	if g.tPendingSync != nil {
		g.tPendingSync.Stop()
		g.tPendingSync = nil
	}
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
	g.atSessionTimeout(m, func() { g.expireSession(m) })
}

// expireSession removes m when its session timer fires.
func (g *group) expireSession(m *groupMember) {
	// A timer that already fired cannot be retracted: the member
	// may have left and been removed since. removeMember is NOT
	// idempotent, it decrements g.protocols and g.nJoining and
	// drops the static mapping, so a second run skews counters
	// that are never rebuilt.
	if g.members[m.memberID] != m {
		return
	}
	// Kafka counts a member waiting in JoinGroup or SyncGroup as
	// heartbeating, and restarts its session when it answers, as reply
	// does. Until then, we keep restarting the timer.
	if !m.waitingReply.empty() {
		g.updateHeartbeat(m)
		return
	}
	g.updateMemberAndRebalance(m, nil, nil)
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

func (g *group) maxRebalanceTimeoutMs() int32 {
	var max int32
	for _, m := range g.members {
		if m.join.RebalanceTimeoutMillis > max {
			max = m.join.RebalanceTimeoutMillis
		}
	}
	return max
}

// timerWork runs fn on the cluster run loop after d. We only guarantee
// that fn does not run for a group that has since been deleted or
// replaced; see [Cluster.afterFuncOnLoop] for the rest.
func (g *group) timerWork(d time.Duration, fn func()) *time.Timer {
	return g.c.afterFuncOnLoop(d, func() {
		if g.c.groups.gs[g.name] != g {
			return
		}
		fn()
	})
}

func (g *group) atSessionTimeout(m *groupMember, fn func()) {
	g.atSessionTimeoutIn(m, time.Millisecond*time.Duration(m.join.SessionTimeoutMillis), fn)
}

// removeMember removes a member from the group, cleaning up its protocol
// counts, session timer, join counter, and static membership. Does not
// trigger a rebalance.
func (g *group) removeMember(m *groupMember) {
	for _, p := range m.join.Protocols {
		g.protocols[p.Name]--
	}
	delete(g.members, m.memberID)
	if m.instanceID != nil {
		delete(g.staticMembers, *m.instanceID)
		g.persistStaticMember(*m.instanceID, "")
	}
	if m.t != nil {
		m.t.Stop()
	}
	if !m.waitingReply.empty() {
		// nJoining counts parked joins only, not a sync parked in
		// CompletingRebalance.
		if _, ok := m.waitingReply.kreq.(*kmsg.JoinGroupRequest); ok {
			g.nJoining--
		}
	}
}

// validateInstanceID checks that the given instanceID and memberID are
// consistent with the group's static membership state. Returns nil on
// success or a kerr error.
func (g *group) validateInstanceID(instanceID *string, memberID string) *kerr.Error {
	if instanceID == nil {
		return nil
	}
	knownMID, ok := g.staticMembers[*instanceID]
	if !ok {
		return kerr.UnknownMemberID
	}
	if knownMID != memberID {
		return kerr.FencedInstanceID
	}
	return nil
}

// This is used to update a member from a new join request, or to clear a
// member from failed heartbeats.
func (g *group) updateMemberAndRebalance(m *groupMember, waitingReply *clientReq, newJoin *kmsg.JoinGroupRequest) {
	if newJoin != nil {
		for _, p := range m.join.Protocols {
			g.protocols[p.Name]--
		}
		m.join = newJoin
		for _, p := range m.join.Protocols {
			g.protocols[p.Name]++
		}
		// Leaving CompletingRebalance answers every parked sync with
		// REBALANCE_IN_PROGRESS, as Kafka does, but this member's is
		// about to be replaced by its join. Kafka answers a join this
		// one replaces NOT_COORDINATOR; the member stays counted as
		// joining.
		if !m.waitingReply.empty() {
			switch r := m.waitingReply.kreq.(type) {
			case *kmsg.SyncGroupRequest:
				resp := r.ResponseKind().(*kmsg.SyncGroupResponse)
				resp.ErrorCode = kerr.RebalanceInProgress.Code
				g.reply(m.waitingReply, resp, m)
			case *kmsg.JoinGroupRequest:
				resp := r.ResponseKind().(*kmsg.JoinGroupResponse)
				resp.ErrorCode = kerr.NotCoordinator.Code
				resp.MemberID = m.memberID
				m.waitingReply.reply(resp)
			}
		}
		if m.waitingReply.empty() && !waitingReply.empty() {
			g.nJoining++
		}
		m.waitingReply = waitingReply
	} else {
		// Kafka answers a removed member's parked JoinGroup with
		// UNKNOWN_MEMBER_ID. It never answers a parked SyncGroup.
		if !m.waitingReply.empty() {
			if join, ok := m.waitingReply.kreq.(*kmsg.JoinGroupRequest); ok {
				resp := join.ResponseKind().(*kmsg.JoinGroupResponse)
				resp.ErrorCode = kerr.UnknownMemberID.Code
				g.reply(m.waitingReply, resp, nil)
			}
		}
		g.removeMember(m)
	}
	g.rebalance()
}

// Adds a new member to the group and rebalances.
func (g *group) addMemberAndRebalance(m *groupMember, waitingReply *clientReq, join *kmsg.JoinGroupRequest) {
	g.stopPending(m)
	if len(g.members) == 0 {
		g.protocolType = join.ProtocolType
	}
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
	// Kafka lets any protocol type into an empty group; the first
	// member to join sets the group's.
	if g.state == groupEmpty {
		return protocolType != "" && len(protocols) > 0
	}
	if protocolType != g.protocolType {
		return false
	}
	// The joining member must support at least one protocol that
	// is universally supported by all existing members.
	for _, p := range protocols {
		if g.protocols[p.Name] == len(g.members) {
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
					InstanceID:       m.instanceID,
					ProtocolMetadata: p.Metadata,
				})
				continue members
			}
		}
		panic("inconsistent group protocol within saved members")
	}
	return metadata
}

// reply sends kresp back to the caller of creq. If m is non-nil,
// m.waitingReply is cleared before the send attempt so a dead
// connection does not leave a stale reply behind, and m's session timer
// restarts even if the send fails, as in Kafka: a member that a join
// added has no timer until we answer it.
func (g *group) reply(creq *clientReq, kresp kmsg.Response, m *groupMember) {
	if m != nil {
		m.waitingReply = nil
		g.updateHeartbeat(m)
	}
	creq.reply(kresp)
}

///////////////////////////
// KIP-848 CONSUMER GROUPS
///////////////////////////

func (gs *groups) handleConsumerGroupHeartbeat(creq *clientReq) kmsg.Response {
	req := creq.kreq.(*kmsg.ConsumerGroupHeartbeatRequest)
	resp := req.ResponseKind().(*kmsg.ConsumerGroupHeartbeatResponse)
	errResp := func(code int16, msg string) kmsg.Response {
		resp.ErrorCode = code
		if msg != "" {
			resp.ErrorMessage = &msg
		}
		return resp
	}

	misrouted := !gs.c.isCoordinator(creq, req.Group)
	if e := gs.c.deny(creq, req.Group, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationRead, faultKey{group: req.Group, misrouted: misrouted}); e != nil {
		return errResp(e.Code, "")
	}
	for _, t := range req.SubscribedTopicNames {
		if e := gs.c.deny(creq, t, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationDescribe, faultKey{group: req.Group, topic: t, misrouted: misrouted}); e != nil {
			return errResp(e.Code, "")
		}
	}
	if code, msg := validateConsumerHeartbeat(req); code != 0 {
		return errResp(code, msg)
	}
	if misrouted {
		return errResp(kerr.NotCoordinator.Code, "")
	}

	// Group type exclusivity: if this group ID is already a share
	// group, reject the consumer group heartbeat.
	if _, isShare := gs.c.shareGroups.gs[req.Group]; isShare {
		return errResp(kerr.GroupIDNotFound.Code, "")
	}

	// Only a join creates a group, and a join we reject does not leave
	// an empty group behind.
	g := gs.gs[req.Group]
	if req.MemberEpoch != 0 && (g == nil || g.typ != "consumer") {
		return errResp(kerr.GroupIDNotFound.Code, "")
	}
	var isNew bool
	if g == nil {
		g, isNew = gs.newOrExisting(req.Group)
	}
	resp = g.handleConsumerHeartbeat(creq)
	if isNew && resp.ErrorCode != 0 {
		// A rejected join changed nothing. Unlike a delete, it leaves
		// the offsets an open transaction commits to the group.
		delete(gs.gs, req.Group)
	}
	return resp
}

// validateConsumerHeartbeat checks the request fields Kafka checks before
// the request reaches the group, returning an error code and message.
//
// Note that we do not reject a static leave (epoch -2) without an instance
// ID: we take the instance ID from the member.
func validateConsumerHeartbeat(req *kmsg.ConsumerGroupHeartbeatRequest) (int16, string) {
	blank := func(s *string) bool { return s != nil && strings.TrimSpace(*s) == "" }
	switch {
	case (req.Version >= 1 || req.MemberEpoch > 0 || req.MemberEpoch == -1) && blank(&req.MemberID):
		return kerr.InvalidRequest.Code, "MemberId can't be empty."
	case blank(&req.Group):
		return kerr.InvalidRequest.Code, "GroupId can't be empty."
	case blank(req.InstanceID):
		return kerr.InvalidRequest.Code, "InstanceId can't be empty."
	case blank(req.RackID):
		return kerr.InvalidRequest.Code, "RackId can't be empty."
	case req.MemberEpoch == 0 && req.RebalanceTimeoutMillis == -1:
		return kerr.InvalidRequest.Code, "RebalanceTimeoutMs must be provided in first request."
	case req.MemberEpoch == 0 && (req.Topics == nil || len(req.Topics) > 0):
		// A client that leaks stale owned partitions into its join is
		// rejected on every attempt, forever.
		return kerr.InvalidRequest.Code, "TopicPartitions must be empty when (re-)joining."
	case req.MemberEpoch == 0 && req.SubscribedTopicNames == nil && req.SubscribedTopicRegex == nil:
		return kerr.InvalidRequest.Code, "Either SubscribedTopicNames or SubscribedTopicRegex must be non-null when (re-)joining."
	case req.MemberEpoch < -2:
		return kerr.InvalidRequest.Code, "MemberEpoch is invalid."
	case req.ServerAssignor != nil && !validServerAssignor(*req.ServerAssignor):
		return kerr.UnsupportedAssignor.Code, "ServerAssignor " + *req.ServerAssignor + " is not supported. Supported assignors: " + strings.Join(serverAssignors, ", ") + "."
	}
	return 0, ""
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
		if e := gs.c.deny(creq, rg, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationDescribe, faultKey{group: rg, misrouted: !gs.c.isCoordinator(creq, rg)}); e != nil {
			sg.ErrorCode = e.Code
			continue
		}
		if kerr := gs.c.validateGroup(creq, rg); kerr != nil {
			sg.ErrorCode = kerr.Code
			continue
		}
		// Kafka fills in the authorized operations only for a group
		// it describes.
		g, ok := gs.gs[rg]
		if !ok || g.typ != "consumer" {
			sg.ErrorCode = kerr.GroupIDNotFound.Code
			continue
		}
		sg.State = g.state.String()
		sg.Epoch = g.groupEpoch
		sg.AssignmentEpoch = g.targetAssignmentEpoch
		sg.AssignorName = g.preferredAssignor()
		for _, m := range g.consumerMembers {
			sm := kmsg.NewConsumerGroupDescribeResponseGroupMember()
			sm.MemberID = m.memberID
			sm.InstanceID = m.instanceID
			sm.RackID = m.rackID
			sm.MemberEpoch = m.memberEpoch
			sm.ClientID = m.clientID
			sm.ClientHost = m.clientHost
			sm.SubscribedTopics = m.subscribedTopics
			if m.subscribedTopicRegex != nil {
				sm.SubscribedTopicRegex = &m.subscribedRegexSource
			}
			sm.MemberType = 1 // consumer
			// The member is still responsible for partitions it is
			// revoking, so they are part of its assignment.
			sm.Assignment = g.c.describeAssignment(m.lastReconciledSent, m.partitionsPendingRevocation)
			sm.TargetAssignment = g.c.describeAssignment(m.targetAssignment)
			sg.Members = append(sg.Members, sm)
		}
		// Kafka hides the whole group if any assigned topic is one you
		// cannot describe.
		for _, sm := range sg.Members {
			for _, tp := range slices.Concat(sm.Assignment.TopicPartitions, sm.TargetAssignment.TopicPartitions) {
				if e := gs.c.deny(creq, tp.Topic, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationDescribe, faultKey{group: rg, topic: tp.Topic}); e != nil {
					*sg = kmsg.NewConsumerGroupDescribeResponseGroup()
					sg.Group = rg
					sg.ErrorCode = e.Code
					sg.ErrorMessage = kmsg.StringPtr("The group has described topic(s) that the client is not authorized to describe.")
					break
				}
			}
			if sg.ErrorCode != 0 {
				break
			}
		}
		if req.IncludeAuthorizedOperations && sg.ErrorCode == 0 {
			sg.AuthorizedOperations = gs.c.groupAuthorizedOps(creq, rg)
		}
	}
	return resp
}

// describeAssignment merges partition maps into a described assignment
// with topic names, skipping topics that no longer exist.
func (c *Cluster) describeAssignment(as ...map[uuid][]int32) kmsg.Assignment {
	var ka kmsg.Assignment
	idx := make(map[uuid]int)
	for _, a := range as {
		for id, parts := range a {
			name, ok := c.data.id2t[id]
			if !ok || len(parts) == 0 {
				continue
			}
			i, ok := idx[id]
			if !ok {
				i = len(ka.TopicPartitions)
				idx[id] = i
				tp := kmsg.NewAssignmentTopicPartition()
				tp.TopicID = id
				tp.Topic = name
				ka.TopicPartitions = append(ka.TopicPartitions, tp)
			}
			ka.TopicPartitions[i].Partitions = append(ka.TopicPartitions[i].Partitions, parts...)
		}
	}
	return ka
}

// isFullRequest returns whether a heartbeat sets the rebalance timeout, a
// subscription, and its owned partitions, as Kafka requires of a full
// request; a keepalive heartbeat sets RebalanceTimeoutMillis to -1 and
// leaves the rest nil. We answer a full request with the member's
// assignment, and a partial one only when its reconciled assignment
// changed.
func isFullRequest(req *kmsg.ConsumerGroupHeartbeatRequest) bool {
	return req.RebalanceTimeoutMillis != -1 &&
		(req.SubscribedTopicNames != nil || req.SubscribedTopicRegex != nil) &&
		req.Topics != nil
}

// Handles a KIP-848 consumer group heartbeat, routing to join, leave, or
// regular heartbeat based on MemberEpoch.
func (g *group) handleConsumerHeartbeat(creq *clientReq) *kmsg.ConsumerGroupHeartbeatResponse {
	req := creq.kreq.(*kmsg.ConsumerGroupHeartbeatRequest)
	resp := req.ResponseKind().(*kmsg.ConsumerGroupHeartbeatResponse)

	// Groups start as "classic" (the default in newGroup). The first
	// ConsumerGroupHeartbeat upgrades the group to "consumer",
	// unless classic members are already active: we do not support
	// Kafka's online upgrade of a classic group that has members.
	converted := g.typ == "classic"
	if converted {
		if len(g.members) > 0 {
			resp.ErrorCode = kerr.GroupIDNotFound.Code
			return resp
		}
		g.typ = "consumer"
		g.consumerMembers = make(map[string]*consumerMember)
		g.partitionEpochs = make(map[uuid]map[int32]partitionOwner)
	}

	switch req.MemberEpoch {
	case 0:
		resp = g.consumerJoin(creq, req, resp)
	case -1:
		resp = g.consumerLeave(req, resp)
	case -2:
		resp = g.consumerStaticLeave(req, resp)
	default:
		resp = g.consumerRegularHeartbeat(creq, req, resp)
	}
	// Kafka converts the group only if the join succeeds.
	if converted && resp.ErrorCode != 0 {
		g.typ = "classic"
		g.consumerMembers, g.partitionEpochs = nil, nil
	}
	// Kafka leaves the interval at 0 on an error or a leave.
	if resp.ErrorCode == 0 && resp.MemberEpoch >= 0 {
		resp.HeartbeatIntervalMillis = g.c.consumerHeartbeatIntervalMsForGroup(g.name)
	}
	return resp
}

func (g *group) consumerJoin(creq *clientReq, req *kmsg.ConsumerGroupHeartbeatRequest, resp *kmsg.ConsumerGroupHeartbeatResponse) *kmsg.ConsumerGroupHeartbeatResponse {
	memberID := req.MemberID
	if memberID == "" {
		memberID = generateMemberID(creq.cid, req.InstanceID)
	}

	// Check group max size for truly new members (not rejoins).
	isRejoin := g.consumerMembers[memberID] != nil
	if req.InstanceID != nil {
		if oldMID, ok := g.staticMembers[*req.InstanceID]; ok {
			isRejoin = isRejoin || oldMID == memberID || g.consumerMembers[oldMID] != nil
		}
	}
	if !isRejoin && int32(len(g.consumerMembers)) >= g.c.consumerGroupMaxSize() {
		resp.ErrorCode = kerr.GroupMaxSizeReached.Code
		return resp
	}

	// The regex is checked before anything changes: a rejected join
	// must not replace a static member.
	var re *regexp.Regexp
	if req.SubscribedTopicRegex != nil {
		var err error
		if re, err = compileSubscribedTopicRegex(*req.SubscribedTopicRegex); err != nil {
			resp.ErrorCode = kerr.InvalidRegularExpression.Code
			return resp
		}
	}

	// Static member: if this instanceID maps to a different member,
	// the old member must have sent a static leave (epoch -2) before
	// replacement is allowed. If the old member is still active, we
	// return UNRELEASED_INSTANCE_ID, as Kafka does.
	prevAssignor := g.preferredAssignor()
	var old *consumerMember // the epoch -2 static member we take over from
	if req.InstanceID != nil {
		if oldMemberID, ok := g.staticMembers[*req.InstanceID]; ok && oldMemberID != memberID {
			if prev := g.consumerMembers[oldMemberID]; prev != nil {
				if prev.memberEpoch != -2 {
					resp.ErrorCode = kerr.UnreleasedInstanceID.Code
					return resp
				}
				old = prev
				g.fenceConsumerMember(old)
				delete(g.consumerMembers, oldMemberID)
			}
		}
		g.staticMembers[*req.InstanceID] = memberID
	}

	// Same-memberID: handle static rejoin from epoch -2 or
	// non-static rejoin (update in place).
	if prev := g.consumerMembers[memberID]; prev != nil {
		if prev.memberEpoch != -2 {
			// Active non-static rejoin: update subscription
			// in place, preserving partition state.
			return g.consumerRejoin(creq, req, resp, prev)
		}
		if old == nil {
			old = prev
		}
		g.fenceConsumerMember(prev)
		delete(g.consumerMembers, memberID)
	}

	m := &consumerMember{
		memberID:                    memberID,
		clientID:                    creq.cid,
		clientHost:                  "/" + creq.clientHost(),
		instanceID:                  req.InstanceID,
		rackID:                      req.RackID,
		rebalanceTimeoutMs:          req.RebalanceTimeoutMillis,
		targetAssignment:            make(map[uuid][]int32),
		partitionsPendingRevocation: make(map[uuid][]int32),
	}
	if req.ServerAssignor != nil {
		m.serverAssignor = *req.ServerAssignor
	}
	if req.SubscribedTopicNames != nil {
		m.subscribedTopics = slices.Clone(req.SubscribedTopicNames)
		slices.Sort(m.subscribedTopics)
	}
	if re != nil {
		m.subscribedTopicRegex = re
		m.subscribedRegexSource = *req.SubscribedTopicRegex
	}
	if m.rebalanceTimeoutMs <= 0 {
		m.rebalanceTimeoutMs = 45000
	}

	// Inherit state from epoch -2 static member so the sticky
	// assignor preserves the partition distribution and the
	// reconciliation sees the correct owned partitions.
	if old != nil {
		m.lastReconciledSent = copyAssignment(old.lastReconciledSent)
		m.targetAssignment = copyAssignment(old.targetAssignment)
		m.partAssignmentEpochs = old.partAssignmentEpochs
	}

	g.consumerMembers[memberID] = m

	// Bump groupEpoch when: group is new (epoch 0), truly new
	// member (no prior static member), subscription changed, or
	// the group's assignor changed.
	if g.groupEpoch == 0 || old == nil ||
		!slices.Equal(old.subscribedTopics, m.subscribedTopics) ||
		old.subscribedRegexSource != m.subscribedRegexSource ||
		g.preferredAssignor() != prevAssignor {
		g.groupEpoch++
		g.computeTargetAssignment()
		g.persistMeta848()
	}
	if m.instanceID != nil {
		g.persistStaticMember(*m.instanceID, memberID)
	}

	// Compute initial assignment for this new member. The join
	// reports that it owns nothing, so nothing it is told to revoke
	// holds its epoch back.
	newAssigned, newPending, _ := g.computeNextAssignment(m, req.Topics)
	m.lastReconciledSent = newAssigned
	m.partitionsPendingRevocation = newPending

	// Add to epoch map so other members can't claim these
	// partitions until this member releases them. Old state
	// is empty (new member or fenced rejoin).
	g.updateMemberEpochs(m, nil, nil)

	g.updateConsumerStateField()
	g.atConsumerSessionTimeout(m)

	resp.MemberID = &memberID
	resp.MemberEpoch = m.memberEpoch
	g.c.cfg.logger.Logf(LogLevelDebug, "RECONCILE SEND: member=%s epoch=%d partitions=%d", m.memberID, m.memberEpoch, countAssignment(m.lastReconciledSent))
	resp.Assignment = makeAssignment(m.lastReconciledSent)
	g.c.cfg.logger.Logf(LogLevelInfo, "consumerJoin: group=%s member=%s epoch=%d sent=%d epochMap=%d members=%d",
		g.logName(), m.memberID, m.memberEpoch, countAssignment(m.lastReconciledSent), g.countPartitionEpochs(), len(g.consumerMembers))
	m.updatePartAssignmentEpochs(m.lastReconciledSent)
	g.updateConsumerRebalanceTimeout(m, true)
	return resp
}

// updateMemberSubscriptions applies the subscription and assignor fields
// a heartbeat carries; a field the request leaves null keeps its value.
// Returns whether the subscription or the group's assignor changed, and
// an error code (0 on success). Nothing changes on an error.
func (g *group) updateMemberSubscriptions(m *consumerMember, req *kmsg.ConsumerGroupHeartbeatRequest) (changed bool, errCode int16) {
	if req.SubscribedTopicRegex != nil && *req.SubscribedTopicRegex != m.subscribedRegexSource {
		re, err := compileSubscribedTopicRegex(*req.SubscribedTopicRegex)
		if err != nil {
			return false, kerr.InvalidRegularExpression.Code
		}
		m.subscribedTopicRegex = re
		m.subscribedRegexSource = *req.SubscribedTopicRegex
		changed = true
	}
	if req.SubscribedTopicNames != nil {
		sorted := slices.Clone(req.SubscribedTopicNames)
		slices.Sort(sorted)
		if !slices.Equal(m.subscribedTopics, sorted) {
			m.subscribedTopics = sorted
			changed = true
		}
	}
	if req.ServerAssignor != nil && *req.ServerAssignor != m.serverAssignor {
		prev := g.preferredAssignor()
		m.serverAssignor = *req.ServerAssignor
		changed = changed || g.preferredAssignor() != prev
	}
	return changed, 0
}

// refreshConsumerMember applies the member fields every heartbeat may
// carry; a field the request leaves null keeps its value.
func refreshConsumerMember(creq *clientReq, req *kmsg.ConsumerGroupHeartbeatRequest, m *consumerMember) {
	m.clientID = creq.cid
	m.clientHost = "/" + creq.clientHost()
	if req.RackID != nil {
		m.rackID = req.RackID
	}
	if req.RebalanceTimeoutMillis > 0 {
		m.rebalanceTimeoutMs = req.RebalanceTimeoutMillis
	}
}

// consumerRejoin handles a non-static rejoin: an active member sends
// epoch 0 to update its subscription without losing partition state.
// Updates the member in place and runs reconciliation.
func (g *group) consumerRejoin(creq *clientReq, req *kmsg.ConsumerGroupHeartbeatRequest, resp *kmsg.ConsumerGroupHeartbeatResponse, m *consumerMember) *kmsg.ConsumerGroupHeartbeatResponse {
	hasSubscriptionChanged, errCode := g.updateMemberSubscriptions(m, req)
	if errCode != 0 {
		resp.ErrorCode = errCode
		return resp
	}
	refreshConsumerMember(creq, req, m)
	if hasSubscriptionChanged {
		g.groupEpoch++
		g.computeTargetAssignment()
		g.persistMeta848()
	}

	g.atConsumerSessionTimeout(m)
	resp.MemberID = &m.memberID

	changed := g.maybeReconcile(m, hasSubscriptionChanged, req.Topics)

	g.c.cfg.logger.Logf(LogLevelDebug, "RECONCILE SEND: member=%s epoch=%d partitions=%d (rejoin)", m.memberID, m.memberEpoch, countAssignment(m.lastReconciledSent))
	resp.Assignment = makeAssignment(m.lastReconciledSent)
	resp.MemberEpoch = m.memberEpoch
	g.updateConsumerStateField()
	g.updateConsumerRebalanceTimeout(m, changed)
	g.c.cfg.logger.Logf(LogLevelInfo, "consumerJoin: group=%s member=%s epoch=%d sent=%d epochMap=%d members=%d (rejoin)",
		g.logName(), m.memberID, m.memberEpoch, countAssignment(m.lastReconciledSent), g.countPartitionEpochs(), len(g.consumerMembers))
	return resp
}

func (g *group) consumerLeave(req *kmsg.ConsumerGroupHeartbeatRequest, resp *kmsg.ConsumerGroupHeartbeatResponse) *kmsg.ConsumerGroupHeartbeatResponse {
	if e := g.validateInstanceID(req.InstanceID, req.MemberID); e != nil {
		resp.ErrorCode = e.Code
		return resp
	}
	m, ok := g.consumerMembers[req.MemberID]
	if !ok {
		resp.ErrorCode = kerr.UnknownMemberID.Code
		return resp
	}
	g.c.cfg.logger.Logf(LogLevelInfo, "consumerLeave: group=%s member=%s epoch=%d sent=%d pending=%d remaining=%d",
		g.logName(), m.memberID, m.memberEpoch, countAssignment(m.lastReconciledSent), countAssignment(m.partitionsPendingRevocation), len(g.consumerMembers)-1)
	g.evictConsumerMember(m)

	resp.MemberID = &req.MemberID
	resp.MemberEpoch = -1
	return resp
}

// consumerStaticLeave handles epoch -2: static member leave. Matches
// Kafka's consumerGroupStaticMemberGroupLeave - the member is NOT
// removed from consumerMembers. Instead it stays with epoch -2 so its
// partition epoch entries remain alive (preventing other members from
// claiming those partitions). On rejoin, the old member is fenced and
// the new member inherits the assignment. If the member does not rejoin
// within the session timeout, the session timer evicts it.
func (g *group) consumerStaticLeave(req *kmsg.ConsumerGroupHeartbeatRequest, resp *kmsg.ConsumerGroupHeartbeatResponse) *kmsg.ConsumerGroupHeartbeatResponse {
	// Kafka finds a static leave's member by its instance ID, which the
	// request must carry.
	if req.InstanceID == nil {
		resp.ErrorCode = kerr.InvalidRequest.Code
		resp.ErrorMessage = kmsg.StringPtr("InstanceId can't be null.")
		return resp
	}
	mid, ok := g.staticMembers[*req.InstanceID]
	m := g.consumerMembers[mid]
	if !ok || m == nil {
		resp.ErrorCode = kerr.UnknownMemberID.Code
		return resp
	}
	if mid != req.MemberID {
		resp.ErrorCode = kerr.FencedInstanceID.Code
		return resp
	}
	g.c.cfg.logger.Logf(LogLevelInfo, "consumerStaticLeave: group=%s member=%s instance=%s epoch=%d sent=%d remaining=%d",
		g.logName(), m.memberID, *m.instanceID, m.memberEpoch, countAssignment(m.lastReconciledSent), len(g.consumerMembers)-1)

	// Mark as temporarily departed - do NOT fence, do NOT
	// delete from consumerMembers, do NOT bump generation.
	//
	// Re-stamp epoch map: drop this member's entries, free its
	// pending revocation partitions, and add its sent entries back
	// at -2. This keeps lastReconciledSent partitions reserved for
	// the member until it rejoins.
	g.removePartitionEpochs(m.lastReconciledSent, m.memberID)
	g.removePartitionEpochs(m.partitionsPendingRevocation, m.memberID)
	for id, parts := range m.partitionsPendingRevocation {
		epochs := m.partAssignmentEpochs[id]
		for _, p := range parts {
			delete(epochs, p)
		}
	}
	m.partitionsPendingRevocation = make(map[uuid][]int32)
	m.memberEpoch = -2
	g.addPartitionEpochs(m.lastReconciledSent, m.memberID, -2)
	g.cancelConsumerRebalanceTimeout(m)
	// Keep g.staticMembers[*m.instanceID] and the session timer.

	g.updateConsumerStateField()

	resp.MemberID = &req.MemberID
	resp.MemberEpoch = -2
	return resp
}

func (g *group) consumerRegularHeartbeat(creq *clientReq, req *kmsg.ConsumerGroupHeartbeatRequest, resp *kmsg.ConsumerGroupHeartbeatResponse) *kmsg.ConsumerGroupHeartbeatResponse {
	// Like Kafka, a full group answers a member it does not know with
	// GROUP_MAX_SIZE_REACHED, unless the instance ID is a static member's.
	var static bool
	if req.InstanceID != nil {
		_, static = g.staticMembers[*req.InstanceID]
	}
	if !static && g.consumerMembers[req.MemberID] == nil && int32(len(g.consumerMembers)) >= g.c.consumerGroupMaxSize() {
		resp.ErrorCode = kerr.GroupMaxSizeReached.Code
		return resp
	}
	if e := g.validateInstanceID(req.InstanceID, req.MemberID); e != nil {
		resp.ErrorCode = e.Code
		return resp
	}
	m, ok := g.consumerMembers[req.MemberID]
	if !ok {
		resp.ErrorCode = kerr.UnknownMemberID.Code
		return resp
	}

	full := isFullRequest(req)

	// Previous-epoch recovery: accept the member's previous epoch if
	// the reported partitions are a subset of the server's
	// authoritative assigned set (lastReconciledSent).
	if req.MemberEpoch != m.memberEpoch {
		matchesPrev := req.MemberEpoch == m.previousMemberEpoch
		topicsNil := req.Topics == nil
		isSubset := false
		if !topicsNil {
			isSubset = isSubsetAssignment(req.Topics, m.lastReconciledSent)
		}
		if !matchesPrev || topicsNil || !isSubset {
			resp.ErrorCode = kerr.FencedMemberEpoch.Code
			return resp
		}
	}

	// A partial heartbeat may carry subscription changes too: Kafka
	// applies every field the request sets.
	hasSubscriptionChanged, errCode := g.updateMemberSubscriptions(m, req)
	if errCode != 0 {
		resp.ErrorCode = errCode
		return resp
	}
	refreshConsumerMember(creq, req, m)

	if interval := g.c.consumerHeartbeatIntervalMsForGroup(g.name); time.Since(m.last) > 2*time.Duration(interval)*time.Millisecond {
		g.c.cfg.logger.Logf(LogLevelWarn, "consumerHeartbeatLate: group=%s member=%s gap=%dms interval=%dms",
			g.logName(), m.memberID, time.Since(m.last).Milliseconds(), interval)
	}
	g.atConsumerSessionTimeout(m)

	if hasSubscriptionChanged {
		g.groupEpoch++
		g.computeTargetAssignment()
		g.persistMeta848()
	}
	resp.MemberID = &req.MemberID

	changed := g.maybeReconcile(m, hasSubscriptionChanged, req.Topics)

	// Send the assignment in the response on full requests
	// or when the reconciled set changed.
	if full || changed {
		g.c.cfg.logger.Logf(LogLevelDebug, "RECONCILE SEND: member=%s epoch=%d partitions=%d",
			m.memberID, m.memberEpoch, countAssignment(m.lastReconciledSent))
		resp.Assignment = makeAssignment(m.lastReconciledSent)
	}

	resp.MemberEpoch = m.memberEpoch

	g.updateConsumerStateField()
	g.updateConsumerRebalanceTimeout(m, changed)
	return resp
}

// updateConsumerRebalanceTimeout starts the rebalance timeout when the
// member is newly told to revoke partitions and cancels it once nothing
// is pending. As in Kafka, a heartbeat that changes nothing leaves a
// running timeout alone, so a member that never revokes is fenced even
// while it keeps heartbeating.
func (g *group) updateConsumerRebalanceTimeout(m *consumerMember, changed bool) {
	switch {
	case len(m.partitionsPendingRevocation) == 0:
		g.cancelConsumerRebalanceTimeout(m)
	case changed || m.tRebal == nil:
		g.scheduleConsumerRebalanceTimeout(m)
	}
}

// serverAssignors are the server assignors a consumer group supports,
// Kafka's default first. "simple" is for share groups only (KIP-932).
var serverAssignors = []string{"uniform", "range"}

func validServerAssignor(name string) bool {
	return slices.Contains(serverAssignors, name)
}

type assignorTP struct {
	topic string
	id    uuid
	part  int32
}

// preferredAssignor returns the server assignor the most members ask for,
// or uniform, Kafka's default, if none asks for one.
func (g *group) preferredAssignor() string {
	counts := make(map[string]int, 2)
	for _, m := range g.consumerMembers {
		counts[m.serverAssignor]++
	}
	pick := serverAssignors[0]
	for _, a := range serverAssignors {
		if counts[a] > counts[pick] {
			pick = a
		}
	}
	return pick
}

// memberTopics returns the topics m subscribes to, by name or through its
// regex against the topics that exist now.
func (g *group) memberTopics(m *consumerMember) map[string]struct{} {
	subs := make(map[string]struct{}, len(m.subscribedTopics))
	for _, t := range m.subscribedTopics {
		subs[t] = struct{}{}
	}
	if m.subscribedTopicRegex != nil {
		for topic := range g.c.data.tps {
			if m.subscribedTopicRegex.MatchString(topic) {
				subs[topic] = struct{}{}
			}
		}
	}
	return subs
}

// consumerSubscribedTopics returns every topic a member of the group
// subscribes to. A static member parked at epoch -2 still counts.
func (g *group) consumerSubscribedTopics() map[string]struct{} {
	all := make(map[string]struct{})
	for _, m := range g.consumerMembers {
		maps.Copy(all, g.memberTopics(m))
	}
	return all
}

// subscriptions returns each member's subscribed topics, and every
// partition of every subscribed topic that exists, sorted by topic and
// partition.
func (g *group) subscriptions() (map[string]map[string]struct{}, []assignorTP) {
	memberSubs := make(map[string]map[string]struct{}, len(g.consumerMembers))
	subscribedSet := make(map[string]struct{})
	for mid, m := range g.consumerMembers {
		memberSubs[mid] = g.memberTopics(m)
		maps.Copy(subscribedSet, memberSubs[mid])
	}
	var allTPs []assignorTP
	for topic := range subscribedSet {
		id, nparts, ok := g.c.topicInfo(topic)
		if !ok {
			continue
		}
		for p := int32(0); p < nparts; p++ {
			allTPs = append(allTPs, assignorTP{topic: topic, id: id, part: p})
		}
	}
	slices.SortFunc(allTPs, func(a, b assignorTP) int {
		if c := cmp.Compare(a.topic, b.topic); c != 0 {
			return c
		}
		return cmp.Compare(a.part, b.part)
	})
	return memberSubs, allTPs
}

// computeTargetAssignment resolves subscriptions against the current
// topics and dispatches to the group's preferred assignor. Updates
// targetAssignment on each consumerMember.
//
// As in Kafka, a static member parked at epoch -2 is still assigned
// partitions. Uniform can move one of its partitions when the member holds
// more than its share, and range can move any. The member that gets such a
// partition waits for it while the parked member's partition epoch at -2
// remains.
func (g *group) computeTargetAssignment() {
	assignor := g.preferredAssignor()
	memberSubs, allTPs := g.subscriptions()
	g.subscribedTPs = allTPs

	// Sort members deterministically. For the range assignor, static
	// members (sorted by instanceID) come before dynamic members
	// (sorted by memberID), matching Kafka's behavior.
	memberIDs := slices.Collect(maps.Keys(memberSubs))
	if assignor == "range" {
		slices.SortFunc(memberIDs, func(a, b string) int {
			ma, mb := g.consumerMembers[a], g.consumerMembers[b]
			aStatic := ma.instanceID != nil
			bStatic := mb.instanceID != nil
			if aStatic != bStatic {
				if aStatic {
					return -1
				}
				return 1
			}
			if aStatic {
				return cmp.Compare(*ma.instanceID, *mb.instanceID)
			}
			return cmp.Compare(a, b)
		})
	} else {
		slices.Sort(memberIDs)
	}

	// The assignment epoch follows the group epoch even when the group
	// has no members left to assign to, as in Kafka.
	g.targetAssignmentEpoch = g.groupEpoch
	if len(memberIDs) == 0 {
		return
	}

	switch assignor {
	case "range":
		// Range is position-based, not sticky - clear and recompute.
		for _, mid := range memberIDs {
			g.consumerMembers[mid].targetAssignment = make(map[uuid][]int32)
		}
		g.assignRange(allTPs, memberIDs, memberSubs)
	default: // "uniform"
		// assignUniform is sticky: it preserves existing targets
		// and only reassigns partitions that need to move.
		g.assignUniform(allTPs, memberIDs, memberSubs)
	}

	// Sort partition lists for determinism.
	for _, mid := range memberIDs {
		m := g.consumerMembers[mid]
		for id := range m.targetAssignment {
			slices.Sort(m.targetAssignment[id])
		}
	}

	// Epoch map is NOT rebuilt here; it is managed
	// incrementally per-member:
	// - consumerJoin: updateMemberEpochs for new member
	// - maybeReconcile: updateMemberEpochs on assignment change
	// - epoch bump: updateMemberEpochs re-stamps at new epoch
	// - fenceConsumerMember: removePartitionEpochs
}

// assignUniform distributes partitions across all eligible members using
// a sticky algorithm: existing assignments are preserved when valid, and
// only unassigned partitions are redistributed. This minimizes partition
// movement during cooperative rebalances (KIP-848), where each moved
// partition requires multiple heartbeat rounds to revoke and reassign.
func (g *group) assignUniform(allTPs []assignorTP, memberIDs []string, memberSubs map[string]map[string]struct{}) {
	type tpKey struct {
		id   uuid
		part int32
	}
	allTPSet := make(map[tpKey]string, len(allTPs))
	for _, tp := range allTPs {
		allTPSet[tpKey{tp.id, tp.part}] = tp.topic
	}

	// Step 1: keep existing assignments that are still valid (member
	// exists, subscribes to the topic, partition still exists).
	assigned := make(map[tpKey]bool, len(allTPs))
	counts := make(map[string]int, len(memberIDs))
	for _, mid := range memberIDs {
		m := g.consumerMembers[mid]
		for id, parts := range m.targetAssignment {
			var kept []int32
			for _, p := range parts {
				k := tpKey{id, p}
				topic, exists := allTPSet[k]
				if !exists {
					continue
				}
				if _, ok := memberSubs[mid][topic]; !ok {
					continue
				}
				kept = append(kept, p)
				assigned[k] = true
			}
			if len(kept) > 0 {
				m.targetAssignment[id] = kept
			} else {
				delete(m.targetAssignment, id)
			}
		}
		for _, parts := range m.targetAssignment {
			counts[mid] += len(parts)
		}
	}

	// Step 2: balance - members with too many partitions shed
	// excess. With N total and M members, each gets floor(N/M)
	// or floor(N/M)+1.
	minCount := len(allTPs) / len(memberIDs)
	extra := len(allTPs) % len(memberIDs)

	// Determine the allowed max per member. Sort members by
	// current count descending - the top `extra` members are
	// allowed minCount+1, the rest get minCount. This minimizes
	// movement by letting the most-loaded members keep an extra
	// partition when possible.
	type memberCount struct {
		mid   string
		count int
	}
	sorted := make([]memberCount, len(memberIDs))
	for i, mid := range memberIDs {
		sorted[i] = memberCount{mid, counts[mid]}
	}
	slices.SortFunc(sorted, func(a, b memberCount) int {
		if c := cmp.Compare(b.count, a.count); c != 0 {
			return c // descending
		}
		return cmp.Compare(a.mid, b.mid) // tie-break by member ID
	})
	allowed := make(map[string]int, len(memberIDs))
	for i, mc := range sorted {
		if i < extra {
			allowed[mc.mid] = minCount + 1
		} else {
			allowed[mc.mid] = minCount
		}
	}

	// Shed excess from overloaded members. Only update
	// m.targetAssignment and remove from assigned; step 3
	// collects all unassigned partitions in one pass.
	for _, mid := range memberIDs {
		m := g.consumerMembers[mid]
		excess := counts[mid] - allowed[mid]
		if excess <= 0 {
			continue
		}
		// Remove excess partitions. Iterate topic IDs in sorted
		// order so the choice is deterministic.
		ids := slices.Collect(maps.Keys(m.targetAssignment))
		slices.SortFunc(ids, func(a, b uuid) int { return bytes.Compare(a[:], b[:]) })
		for _, id := range ids {
			if excess <= 0 {
				break
			}
			parts := m.targetAssignment[id]
			remove := min(excess, len(parts))
			for _, p := range parts[len(parts)-remove:] {
				delete(assigned, tpKey{id, p})
			}
			m.targetAssignment[id] = parts[:len(parts)-remove]
			if len(m.targetAssignment[id]) == 0 {
				delete(m.targetAssignment, id)
			}
			counts[mid] -= remove
			excess -= remove
		}
	}

	// Step 3: collect all unassigned partitions - from members
	// that left, partitions never assigned, and excess shed above.
	var unassigned []assignorTP
	for _, tp := range allTPs {
		if !assigned[tpKey{tp.id, tp.part}] {
			unassigned = append(unassigned, tp)
		}
	}
	if len(unassigned) == 0 {
		return
	}

	// Step 4: distribute unassigned partitions to members with
	// the fewest partitions. unassigned preserves allTPs order
	// (already sorted by topic, partition).
	for _, tp := range unassigned {
		bestMid := ""
		bestCount := math.MaxInt
		for _, mid := range memberIDs {
			if _, ok := memberSubs[mid][tp.topic]; !ok {
				continue
			}
			if counts[mid] < bestCount {
				bestMid = mid
				bestCount = counts[mid]
			}
		}
		if bestMid == "" {
			continue
		}
		m := g.consumerMembers[bestMid]
		m.targetAssignment[tp.id] = append(m.targetAssignment[tp.id], tp.part)
		counts[bestMid]++
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

// computeNextAssignment computes the next assignment and pending
// revocations for a member in a single pass (matching Kafka's
// CurrentAssignmentBuilder). Processes lastReconciledSent against
// targetAssignment, computing keep/revoke/assign sets:
//
//	a) Has revocations and client owns them - UNREVOKED_PARTITIONS
//	b) Has new partitions to assign - advance epoch, assign them
//	c) Has unreleased partitions only - advance epoch, wait
//	d) Stable - advance epoch, no change needed
//
// Sets m.state, m.memberEpoch, and m.previousMemberEpoch. Returns
// the new assigned and pending maps plus whether the assignment
// changed.
func (g *group) computeNextAssignment(m *consumerMember,
	ownedTopicPartitions []kmsg.ConsumerGroupHeartbeatRequestTopic,
) (
	newAssigned, newPending map[uuid][]int32, changed bool,
) {
	// Collect all topic IDs from both maps.
	allTopics := make(map[uuid]struct{})
	for id := range m.lastReconciledSent {
		allTopics[id] = struct{}{}
	}
	for id := range m.targetAssignment {
		allTopics[id] = struct{}{}
	}

	keep := make(map[uuid][]int32)
	pendingRevocation := make(map[uuid][]int32)
	pendingAssignment := make(map[uuid][]int32)
	hasUnreleasedPartitions := false
	var blockedCount int

	for id := range allTopics {
		sent := m.lastReconciledSent[id]
		target := m.targetAssignment[id]

		// keep = sent intersect target; revoke = sent - target.
		for _, p := range sent {
			if slices.Contains(target, p) {
				keep[id] = append(keep[id], p)
			} else {
				pendingRevocation[id] = append(pendingRevocation[id], p)
			}
		}

		// pendingAssignment = target - sent, filtered by epoch.
		for _, p := range target {
			if slices.Contains(sent, p) {
				continue // already in keep
			}
			epoch := g.currentPartitionEpoch(id, p)
			if epoch == -1 || slices.Contains(m.partitionsPendingRevocation[id], p) {
				pendingAssignment[id] = append(pendingAssignment[id], p)
			} else {
				hasUnreleasedPartitions = true
				blockedCount++
			}
		}
	}

	// Merge existing pending revocations (from previous rounds,
	// not yet confirmed by client) with newly computed ones.
	allPendingRevocation := copyAssignment(m.partitionsPendingRevocation)
	for id, parts := range pendingRevocation {
		for _, p := range parts {
			if !slices.Contains(allPendingRevocation[id], p) {
				allPendingRevocation[id] = append(allPendingRevocation[id], p)
			}
		}
	}

	hasAnyPendingRevocation := len(allPendingRevocation) > 0
	hasPendingAssignment := len(pendingAssignment) > 0

	switch {
	case hasAnyPendingRevocation && ownsRevokedPartitions(allPendingRevocation, ownedTopicPartitions):
		// Branch a: client still owns revoked partitions.
		m.state = cmUnrevokedPartitions
		// Epoch stays unchanged.
		newAssigned = keep
		newPending = allPendingRevocation
		g.c.cfg.logger.Logf(LogLevelDebug, "RECONCILE REVOKE-FIRST: member=%s reconciled=%d sent=%d target=%d pending=%d",
			m.memberID, countAssignment(keep), countAssignment(m.lastReconciledSent), countAssignment(m.targetAssignment), countAssignment(allPendingRevocation))

	case hasPendingAssignment:
		// Branch b: new partitions to assign.
		if hasUnreleasedPartitions {
			m.state = cmUnreleasedPartitions
		} else {
			m.state = cmStable
		}
		m.previousMemberEpoch = m.memberEpoch
		m.memberEpoch = g.targetAssignmentEpoch
		newAssigned = make(map[uuid][]int32, len(keep)+len(pendingAssignment))
		maps.Copy(newAssigned, keep)
		for id, parts := range pendingAssignment {
			newAssigned[id] = append(newAssigned[id], parts...)
		}
		newPending = make(map[uuid][]int32)
		g.c.cfg.logger.Logf(LogLevelDebug, "RECONCILE SUMMARY: member=%s reconciled=%d target=%d +%d blocked=%d",
			m.memberID, countAssignment(newAssigned), countAssignment(m.targetAssignment), countAssignment(pendingAssignment), blockedCount)

	default:
		// Branches c+d: no new partitions.
		if hasUnreleasedPartitions {
			m.state = cmUnreleasedPartitions
			g.c.cfg.logger.Logf(LogLevelDebug, "RECONCILE SUMMARY: member=%s reconciled=%d target=%d +%d blocked=%d",
				m.memberID, countAssignment(keep), countAssignment(m.targetAssignment), 0, blockedCount)
		} else {
			m.state = cmStable
		}
		m.previousMemberEpoch = m.memberEpoch
		m.memberEpoch = g.targetAssignmentEpoch
		newAssigned = keep
		newPending = make(map[uuid][]int32)
	}

	changed = !maps.EqualFunc(m.lastReconciledSent, newAssigned, slices.Equal) || !maps.EqualFunc(m.partitionsPendingRevocation, newPending, slices.Equal)

	return newAssigned, newPending, changed
}

// updateCurrentAssignment handles subscription changes for a member
// that is waiting for revocations. It only removes partitions for
// unsubscribed topics from lastReconciledSent.
func (g *group) updateCurrentAssignment(m *consumerMember,
	ownedTopicPartitions []kmsg.ConsumerGroupHeartbeatRequestTopic,
) (
	newAssigned, newPending map[uuid][]int32, changed bool,
) {
	// Compute subscribed topic IDs from subscriptions + regex.
	subscribedIDs := make(map[uuid]struct{})
	for topic := range g.memberTopics(m) {
		if id, _, ok := g.c.topicInfo(topic); ok {
			subscribedIDs[id] = struct{}{}
		}
	}

	// Remove unsubscribed topics from lastReconciledSent.
	pendingRevocation := make(map[uuid][]int32)
	newAssigned = make(map[uuid][]int32, len(m.lastReconciledSent))
	for id, parts := range m.lastReconciledSent {
		if _, ok := subscribedIDs[id]; ok {
			newAssigned[id] = parts
		} else {
			pendingRevocation[id] = parts
		}
	}

	if len(pendingRevocation) == 0 {
		return nil, nil, false
	}

	// Merge with existing pending.
	newPending = copyAssignment(m.partitionsPendingRevocation)
	for id, parts := range pendingRevocation {
		newPending[id] = append(newPending[id], parts...)
	}

	if ownsRevokedPartitions(newPending, ownedTopicPartitions) {
		m.state = cmUnrevokedPartitions
	}
	// Epoch doesn't change for subscription-only updates.

	changed = true
	return newAssigned, newPending, changed
}

// clearConfirmedRevocations removes partitions from
// m.partitionsPendingRevocation that are no longer reported in
// ownedTopicPartitions (the client confirmed releasing them).
// Updates the partition epoch map accordingly.
func (g *group) clearConfirmedRevocations(m *consumerMember,
	ownedTopicPartitions []kmsg.ConsumerGroupHeartbeatRequestTopic,
) {
	reported := topicsToAssignment(ownedTopicPartitions)

	oldPending := m.partitionsPendingRevocation
	newPending := make(map[uuid][]int32)
	for id, parts := range oldPending {
		owned, epochs := reported[id], m.partAssignmentEpochs[id]
		for _, p := range parts {
			if slices.Contains(owned, p) {
				newPending[id] = append(newPending[id], p)
			} else {
				delete(epochs, p) // the member no longer owns it
			}
		}
	}
	m.partitionsPendingRevocation = newPending

	if cleared := countAssignment(oldPending) - countAssignment(newPending); cleared > 0 {
		g.c.cfg.logger.Logf(LogLevelDebug, "pendingRevoke cleared: member=%s epoch=%d cleared=%d kept=%d",
			m.memberID, m.memberEpoch, cleared, countAssignment(newPending))
	}

	// Update epoch map: remove old pending, add new.
	g.removePartitionEpochs(oldPending, m.memberID)
	g.addPartitionEpochs(m.partitionsPendingRevocation, m.memberID, m.memberEpoch)
}

// maybeReconcile dispatches to the appropriate reconciliation path
// based on the member's current state.
func (g *group) maybeReconcile(m *consumerMember, hasSubscriptionChanged bool,
	ownedTopicPartitions []kmsg.ConsumerGroupHeartbeatRequestTopic,
) bool {
	// Clear confirmed revocations first.
	if ownedTopicPartitions != nil {
		g.clearConfirmedRevocations(m, ownedTopicPartitions)
	}

	// Early exit if nothing to do.
	if !hasSubscriptionChanged && m.state == cmStable && m.memberEpoch == g.targetAssignmentEpoch {
		return false
	}

	// Save state before computation for epoch map update.
	oldSent := m.lastReconciledSent
	oldPending := m.partitionsPendingRevocation
	oldEpoch := m.memberEpoch

	var newAssigned, newPending map[uuid][]int32
	var changed bool

	switch m.state {
	case cmStable:
		if m.memberEpoch != g.targetAssignmentEpoch {
			newAssigned, newPending, changed = g.computeNextAssignment(m, ownedTopicPartitions)
		} else if hasSubscriptionChanged {
			newAssigned, newPending, changed = g.updateCurrentAssignment(m, ownedTopicPartitions)
		}

	case cmUnrevokedPartitions:
		if ownsRevokedPartitions(m.partitionsPendingRevocation, ownedTopicPartitions) {
			if hasSubscriptionChanged {
				newAssigned, newPending, changed = g.updateCurrentAssignment(m, ownedTopicPartitions)
			}
		} else {
			newAssigned, newPending, changed = g.computeNextAssignment(m, ownedTopicPartitions)
		}

	case cmUnreleasedPartitions:
		newAssigned, newPending, changed = g.computeNextAssignment(m, ownedTopicPartitions)
	}

	if changed {
		m.lastReconciledSent = newAssigned
		m.partitionsPendingRevocation = newPending
		g.updateMemberEpochs(m, oldSent, oldPending)
		m.updatePartAssignmentEpochs(newAssigned)
	} else if m.memberEpoch != oldEpoch {
		// Epoch advanced but assignment didn't change.
		// Re-stamp epoch map entries at the new epoch.
		g.updateMemberEpochs(m, oldSent, oldPending)
	}

	return changed
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
// As in Kafka, a static member parked at epoch -2 is a member that is not
// reconciled: the group is Reconciling until it rejoins or times out.
func (g *group) updateConsumerStateField() {
	if len(g.consumerMembers) == 0 {
		g.state = groupEmpty
		return
	}
	for _, m := range g.consumerMembers {
		if m.state != cmStable || m.memberEpoch != g.targetAssignmentEpoch {
			g.state = groupReconciling
			return
		}
	}
	g.state = groupStable
}

func (g *group) meta848Entry() groupLogEntry {
	return groupLogEntry{
		Type:       "meta848",
		Group:      g.name,
		GroupType:  g.typ,
		GroupEpoch: g.groupEpoch,
	}
}

func (g *group) persistMeta848() { g.c.persistGroupEntry(g.meta848Entry()) }

func (g *group) classicMetaEntry() groupLogEntry {
	return groupLogEntry{
		Type:       "meta",
		Group:      g.name,
		GroupType:  g.typ,
		ProtoType:  g.protocolType,
		Protocol:   g.protocol,
		Generation: g.generation,
	}
}

func (g *group) persistClassicMeta() { g.c.persistGroupEntry(g.classicMetaEntry()) }

func (g *group) commitEntry(topic string, part int32, oc offsetCommit) groupLogEntry {
	return groupLogEntry{
		Type:       "commit",
		Group:      g.name,
		Topic:      topic,
		Part:       part,
		Offset:     oc.offset,
		Epoch:      oc.leaderEpoch,
		Metadata:   oc.metadata,
		LastCommit: oc.lastCommit.UnixNano(),
	}
}

func (g *group) commitAndPersist(topic string, part int32, oc offsetCommit) {
	oc.lastCommit = time.Now()
	g.commits.set(topic, part, oc)
	g.c.persistGroupEntry(g.commitEntry(topic, part, oc))
}

func (g *group) deleteCommitAndPersist(topic string, part int32) {
	g.commits.delp(topic, part)
	g.c.persistGroupEntry(groupLogEntry{
		Type:  "delete",
		Group: g.name,
		Topic: topic,
		Part:  part,
	})
}

// classicSubscribedTopics returns the set of topics subscribed by any
// member of a classic consumer group (from join protocol metadata).
func (g *group) classicSubscribedTopics() map[string]struct{} {
	sub := make(map[string]struct{})
	for _, ms := range []map[string]*groupMember{g.members, g.pending} {
		for _, m := range ms {
			if m.join == nil {
				continue
			}
			for _, proto := range m.join.Protocols {
				var meta kmsg.ConsumerMemberMetadata
				if err := meta.ReadFrom(proto.Metadata); err == nil {
					for _, topic := range meta.Topics {
						sub[topic] = struct{}{}
					}
				}
			}
		}
	}
	return sub
}

// expireOffsets removes committed offsets that have exceeded the retention
// period (KIP-211). Returns true if all offsets expired and the group can
// be deleted (no members, no pending txn offsets).
//
// Must be called from Cluster.run().
func (g *group) expireOffsets(retentionMs int64, hasUnstableOffsets bool) bool {
	subscribed := g.offsetExpirationFilter()
	if subscribed == nil {
		return false // no expiration applies in this state
	}

	now := time.Now()
	retention := time.Duration(retentionMs) * time.Millisecond

	// Collect expired offsets. We can't delete during iteration since
	// delp may remove the inner map.
	type tp struct {
		t string
		p int32
	}
	var expired []tp
	g.commits.each(func(topic string, part int32, oc *offsetCommit) {
		if _, ok := subscribed[topic]; ok {
			return // subscribed - skip
		}
		base := oc.lastCommit
		// For empty classic groups with protocolType, use max(emptyAt, commitTimestamp)
		if g.protocolType != "" && g.state == groupEmpty && g.emptyAt.After(base) {
			base = g.emptyAt
		}
		if now.Sub(base) >= retention {
			expired = append(expired, tp{topic, part})
		}
	})

	for _, e := range expired {
		g.deleteCommitAndPersist(e.t, e.p)
	}

	if hasUnstableOffsets || len(g.commits) > 0 {
		return false
	}
	if g.typ == "consumer" {
		// A static member parked at epoch -2 is still a member: it
		// keeps the instance ID mapping it rejoins through, and Kafka
		// calls the group empty only when it holds no members at all.
		// Deleting the group here would throw that mapping away while
		// the member is briefly gone.
		return len(g.consumerMembers) == 0
	}
	return g.state == groupEmpty
}

// expireAllTopics is a non-nil empty sentinel for offsetExpirationFilter:
// no topics are subscribed, so all are eligible for expiration.
var expireAllTopics = map[string]struct{}{}

// offsetExpirationFilter returns the set of subscribed topics whose
// offsets should NOT be expired, or nil if no expiration applies.
// A non-nil empty map means all topics are eligible for expiration.
//
// Expiration conditions per Kafka (KIP-211):
//   - Simple group (no protocolType): all topics, any state
//   - Classic with protocolType, Empty: all topics
//   - Classic consumer, Stable: unsubscribed topics only
//   - Classic with protocolType, other states: no expiration
//   - 848 consumer: unsubscribed topics, any state
func (g *group) offsetExpirationFilter() map[string]struct{} {
	switch g.typ {
	case "consumer":
		// 848: expire unsubscribed topics in any state. A topic a
		// regex matches or a parked static member subscribes to is
		// subscribed.
		return g.consumerSubscribedTopics()

	default:
		// Classic group
		if g.protocolType == "" {
			return expireAllTopics // simple group - expire all
		}
		switch g.state {
		case groupEmpty:
			return expireAllTopics
		case groupStable:
			if g.protocolType == "consumer" {
				return g.classicSubscribedTopics()
			}
			return nil
		case groupPreparingRebalance, groupCompletingRebalance, groupDead, groupReconciling:
			return nil
		}
		return nil
	}
}

// staticMemberEntry builds a static member log entry. Empty memberID
// signals deletion on replay.
func (g *group) staticMemberEntry(instanceID, memberID string) groupLogEntry {
	return groupLogEntry{
		Type:       "static",
		Group:      g.name,
		InstanceID: instanceID,
		MemberID:   memberID,
	}
}

func (g *group) persistStaticMember(instanceID, memberID string) {
	g.c.persistGroupEntry(g.staticMemberEntry(instanceID, memberID))
}

// releaseStaticMember releases the instance ID if memberID still holds it.
func (g *group) releaseStaticMember(instanceID *string, memberID string) {
	if instanceID != nil && g.staticMembers[*instanceID] == memberID {
		delete(g.staticMembers, *instanceID)
		g.persistStaticMember(*instanceID, "")
	}
}

// evictConsumerMember fences and removes a consumer member, then
// recomputes the target assignment. A static member's instance ID is
// released, so the next member using it joins as a new member.
func (g *group) evictConsumerMember(m *consumerMember) {
	g.fenceConsumerMember(m)
	delete(g.consumerMembers, m.memberID)
	g.releaseStaticMember(m.instanceID, m.memberID)
	g.groupEpoch++
	g.computeTargetAssignment()
	g.updateConsumerStateField()
	g.persistMeta848()
}

// atConsumerSessionTimeout sets up the session timeout for a consumer
// member. The session timeout uses the per-group or server-level config
// group.consumer.session.timeout.ms and fences the member entirely if
// no heartbeats are received within the timeout.
func (g *group) atConsumerSessionTimeout(m *consumerMember) {
	g.atConsumerSessionTimeoutIn(m, time.Duration(g.c.consumerSessionTimeoutMsForGroup(g.name))*time.Millisecond)
}

// scheduleConsumerRebalanceTimeout starts a per-member rebalance
// timeout that fences the member if it does not complete partition
// revocation within rebalanceTimeoutMs. Only active when the member
// has partitions to release. If the member is gone or replaced, or its
// epoch has advanced by the time the timer fires, the timeout is ignored.
func (g *group) scheduleConsumerRebalanceTimeout(m *consumerMember) {
	g.cancelConsumerRebalanceTimeout(m)
	timeout := time.Duration(m.rebalanceTimeoutMs) * time.Millisecond
	epoch := m.memberEpoch
	memberID := m.memberID
	m.tRebal = g.timerWork(timeout, func() {
		// Check the member is still this one and has not progressed
		// past the epoch we were watching. Identity is checked as
		// well as the epoch because a replacement can sit at the same
		// number: a brand new member and one inheriting from an epoch
		// -2 static predecessor are both at 0.
		if g.consumerMembers[memberID] != m || m.memberEpoch != epoch {
			return
		}
		g.c.cfg.logger.Logf(LogLevelWarn, "consumerRebalanceTimeout: group=%s member=%s epoch=%d remaining=%d",
			g.logName(), memberID, epoch, len(g.consumerMembers)-1)
		g.evictConsumerMember(m)
	})
}

func (*group) cancelConsumerRebalanceTimeout(m *consumerMember) {
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
	// Free what this member owns, and only what it owns: another
	// member may hold a partition this one still has a record of.
	g.removePartitionEpochs(m.lastReconciledSent, m.memberID)
	g.removePartitionEpochs(m.partitionsPendingRevocation, m.memberID)
}

// Handles a commit for consumer groups with relaxed validation per KIP-1251,
// as handleOffsetCommit does for a classic group.
func (g *group) handleConsumerOffsetCommit(creq *clientReq, allowed []kmsg.OffsetCommitRequestTopic, answers []*kmsg.OffsetCommitResponseTopicPartition) bool {
	req := creq.kreq.(*kmsg.OffsetCommitRequest)
	fail := func(code int16) bool {
		failOffsetCommit(answers, code)
		return false
	}

	// A commit at a negative epoch (an admin or kadm commit) is
	// accepted without validation, at any version, while the group has
	// no members. A static member parked at epoch -2 is a member.
	//
	// Note that unlike a classic commit, a commit does not count as a
	// heartbeat for the member's session.
	var cm *consumerMember
	if len(g.consumerMembers) > 0 || req.Generation >= 0 {
		m, ok := g.consumerMembers[req.MemberID]
		if !ok {
			return fail(kerr.UnknownMemberID.Code)
		}
		// Consumer groups require OffsetCommit v9+ (KIP-848). Older
		// versions use GenerationID which does not map to member
		// epochs.
		if req.Version < 9 {
			return fail(kerr.UnsupportedVersion.Code)
		}
		if req.Generation > m.memberEpoch {
			g.c.cfg.logger.Logf(LogLevelWarn, "OffsetCommit STALE: group=%s member=%s reqEpoch=%d memberEpoch=%d",
				g.logName(), req.MemberID, req.Generation, m.memberEpoch)
			return fail(kerr.StaleMemberEpoch.Code)
		}
		cm = m
	}

	// KIP-1251: a commit with an epoch older than the member's must be
	// valid for every partition it would store, or Kafka fails the whole
	// commit.
	if cm != nil && req.Generation < cm.memberEpoch {
		var stale bool
		for _, t := range allowed {
			id := g.c.data.t2id[t.Topic]
			for _, p := range t.Partitions {
				stale = stale || !offsetMetadataTooLarge(p.Metadata) && cm.staleCommit(req.Generation, id, p.Partition)
			}
		}
		if stale {
			g.c.cfg.logger.Logf(LogLevelWarn, "OffsetCommit STALE: group=%s member=%s reqEpoch=%d memberEpoch=%d, a partition is not owned at that epoch",
				g.logName(), req.MemberID, req.Generation, cm.memberEpoch)
			return fail(kerr.StaleMemberEpoch.Code)
		}
	}

	return g.commitAllowed(answers, allowed)
}

func copyAssignment(a map[uuid][]int32) map[uuid][]int32 {
	c := make(map[uuid][]int32, len(a))
	for id, parts := range a {
		c[id] = slices.Clone(parts)
	}
	return c
}

func countAssignment(a map[uuid][]int32) int {
	var n int
	for _, parts := range a {
		n += len(parts)
	}
	return n
}

func (g *group) countPartitionEpochs() int {
	var n int
	for _, pm := range g.partitionEpochs {
		n += len(pm)
	}
	return n
}

func topicsToAssignment(topics []kmsg.ConsumerGroupHeartbeatRequestTopic) map[uuid][]int32 {
	m := make(map[uuid][]int32, len(topics))
	for _, t := range topics {
		m[t.TopicID] = t.Partitions
	}
	return m
}

// updatePartAssignmentEpochs updates per-partition assignment epochs
// after reconciliation. Retained partitions keep their existing epoch, as
// do partitions being revoked; newly assigned partitions get the member's
// current epoch.
func (m *consumerMember) updatePartAssignmentEpochs(reconciled map[uuid][]int32) {
	newEpochs := make(map[uuid]map[int32]int32, len(reconciled))
	set := func(id uuid, p, epoch int32) {
		if newEpochs[id] == nil {
			newEpochs[id] = make(map[int32]int32)
		}
		newEpochs[id][p] = epoch
	}
	for id, parts := range reconciled {
		epochs := m.partAssignmentEpochs[id]
		for _, p := range parts {
			epoch, ok := epochs[p]
			if !ok {
				epoch = m.memberEpoch
			}
			set(id, p, epoch)
		}
	}
	for id, parts := range m.partitionsPendingRevocation {
		epochs := m.partAssignmentEpochs[id]
		for _, p := range parts {
			if epoch, ok := epochs[p]; ok {
				set(id, p, epoch)
			}
		}
	}
	m.partAssignmentEpochs = newEpochs
}

// staleCommit reports whether a commit at epoch, older than the member's
// current epoch, is rejected for a partition (KIP-1251). Like Kafka, we
// accept such a commit only for a partition the member owns or is
// revoking, and only if the epoch is not older than when the partition was
// assigned.
func (m *consumerMember) staleCommit(epoch int32, id uuid, part int32) bool {
	assignEpoch, owned := m.partAssignmentEpochs[id][part]
	return !owned || epoch < assignEpoch
}

// ownsRevokedPartitions matches Kafka's check: if topics is nil
// (keepalive), conservatively assume the client still owns the revoked
// partitions. Otherwise, return true if any pending partition is still
// reported by the client.
func ownsRevokedPartitions(pending map[uuid][]int32, topics []kmsg.ConsumerGroupHeartbeatRequestTopic) bool {
	if topics == nil {
		return len(pending) > 0
	}
	reported := topicsToAssignment(topics)
	for id, parts := range pending {
		for _, p := range parts {
			if slices.Contains(reported[id], p) {
				return true
			}
		}
	}
	return false
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

// currentPartitionEpoch returns the epoch of the member that currently
// owns the given partition, or -1 if no member owns it.
func (g *group) currentPartitionEpoch(topicID uuid, partition int32) int32 {
	if pm := g.partitionEpochs[topicID]; pm != nil {
		if o, ok := pm[partition]; ok {
			return o.Epoch
		}
	}
	return -1
}

// updateMemberEpochs updates the partition epoch map when a member's
// state changes. Removes the member's old contribution, then adds its
// new contribution from the member's current fields. Caller must save
// old state before mutating the member.
func (g *group) updateMemberEpochs(m *consumerMember, oldSent, oldPending map[uuid][]int32) {
	g.removePartitionEpochs(oldSent, m.memberID)
	g.removePartitionEpochs(oldPending, m.memberID)
	g.addPartitionEpochs(m.lastReconciledSent, m.memberID, m.memberEpoch)
	g.addPartitionEpochs(m.partitionsPendingRevocation, m.memberID, m.memberEpoch)
}

// addPartitionEpochs records that a member at the given epoch owns the
// given partitions. If a partition is already owned by a different
// member, the caller has a logic bug (double assignment) - we log and
// skip the update to avoid masking the real owner.
func (g *group) addPartitionEpochs(a map[uuid][]int32, memberID string, epoch int32) {
	for id, parts := range a {
		pm := g.partitionEpochs[id]
		if pm == nil {
			pm = make(map[int32]partitionOwner, len(parts))
			g.partitionEpochs[id] = pm
		}
		for _, p := range parts {
			if o, ok := pm[p]; ok && o.Member != memberID {
				g.c.cfg.logger.Logf(LogLevelError, "group %s: addPartitionEpochs: partition %d of topic %v is owned by member %s at epoch %d, skipping the claim by %s at epoch %d", g.name, p, id, o.Member, o.Epoch, memberID, epoch)
				continue
			}
			pm[p] = partitionOwner{Member: memberID, Epoch: epoch}
		}
	}
}

// removePartitionEpochs clears the given partitions, but only those the
// given member owns. Matching on the epoch instead would clear an entry
// belonging to another member sitting at the same epoch, which then lets
// a third member claim a partition that is still assigned.
func (g *group) removePartitionEpochs(a map[uuid][]int32, memberID string) {
	for id, parts := range a {
		pm := g.partitionEpochs[id]
		if pm == nil {
			continue
		}
		for _, p := range parts {
			if o, ok := pm[p]; ok && o.Member == memberID {
				delete(pm, p)
			}
		}
		if len(pm) == 0 {
			delete(g.partitionEpochs, id)
		}
	}
}

// validateMemberGeneration checks a transactional commit's member fields as
// Kafka does, returning 0 or an error code. A commit with no member fields
// is not checked, and neither is one at a negative generation to a group
// with no members. A consumer protocol member whose epoch is newer than its
// current one, or older and not valid for every partition (as for
// OffsetCommit), gets STALE_MEMBER_EPOCH; callers serving request versions
// that predate that error map it to ILLEGAL_GENERATION. A classic group
// checks the instance ID and the member, and wants the exact generation.
func (g *group) validateMemberGeneration(memberID string, instanceID *string, generation int32, topics []kmsg.TxnOffsetCommitRequestTopic) int16 {
	if g.typ == "consumer" {
		if generation < 0 && len(g.consumerMembers) == 0 || generation == -1 && memberID == "" && instanceID == nil {
			return 0
		}
		m, exists := g.consumerMembers[memberID]
		switch {
		case !exists:
			return kerr.UnknownMemberID.Code
		case generation == m.memberEpoch:
			return 0
		case generation > m.memberEpoch:
			return kerr.StaleMemberEpoch.Code
		}
		for _, t := range topics {
			id := g.c.data.t2id[t.Topic]
			for _, p := range t.Partitions {
				// Kafka answers an unknown partition and oversized
				// metadata before it checks the epoch.
				if g.c.data.tps.checkp(t.Topic, p.Partition) && !offsetMetadataTooLarge(p.Metadata) && m.staleCommit(generation, id, p.Partition) {
					return kerr.StaleMemberEpoch.Code
				}
			}
		}
		return 0
	}
	if generation < 0 && g.state == groupEmpty || generation < 0 && memberID == "" && instanceID == nil {
		return 0
	}
	if err := g.validateInstanceID(instanceID, memberID); err != nil {
		return err.Code
	}
	if _, exists := g.members[memberID]; !exists {
		return kerr.UnknownMemberID.Code
	}
	if generation != g.generation {
		return kerr.IllegalGeneration.Code
	}
	return 0
}

// restoreClassicMembers restores classic group members from persisted
// session state. A member whose session expired while we were down leaves
// as its session timeout would have made it: its instance ID is released and
// the group rebalances.
func (g *group) restoreClassicMembers(sg sessionClassicGroup) {
	// Provisionally set state to groupStable while restoring members;
	// if the persisted sg.State was non-Stable we re-trigger the
	// rebalance at the end of the function. See comment near the end.
	g.state = groupStable
	g.leader = sg.Leader
	var expired bool
	for _, sm := range sg.Members {
		lastHB := sm.LastHeartbeat
		remaining := time.Duration(sm.SessionTimeoutMs)*time.Millisecond - time.Since(lastHB)
		if remaining <= 0 {
			g.releaseStaticMember(sm.InstanceID, sm.ID)
			expired = true
			continue
		}
		m := &groupMember{
			memberID:   sm.ID,
			instanceID: sm.InstanceID,
			clientID:   sm.ClientID,
			clientHost: sm.ClientHost,
			assignment: sm.Assignment,
			join: &kmsg.JoinGroupRequest{
				SessionTimeoutMillis:   sm.SessionTimeoutMs,
				RebalanceTimeoutMillis: sm.RebalanceTimeoutMs,
			},
		}
		for _, p := range sm.Protocols {
			m.join.Protocols = append(m.join.Protocols, kmsg.JoinGroupRequestProtocol{Name: p.Name, Metadata: p.Metadata})
			g.protocols[p.Name]++
		}
		g.members[m.memberID] = m
		if m.instanceID != nil {
			g.staticMembers[*m.instanceID] = m.memberID
		}
		g.atSessionTimeoutIn(m, remaining, func() { g.expireSession(m) })
		// Preserve the real last heartbeat time so that across
		// repeated restarts, dead members accumulate elapsed time.
		m.last = lastHB
	}
	if len(g.members) == 0 && !expired {
		g.state = groupEmpty
		g.emptyAt = time.Now()
		g.leader = ""
		return
	}
	// If the pre-shutdown state was non-Stable, a rebalance was in
	// progress (e.g. a LeaveGroup had transitioned the group to
	// PreparingRebalance but the rebalance hadn't completed before
	// shutdown). Re-trigger rebalance so members rejoin and the
	// assignment reflects the current member set -- otherwise
	// partitions owned by departed members stay orphaned. If the
	// persisted state was Stable and every member is back, the old
	// assignment is still valid; do not force a rebalance.
	if expired || sg.State != groupStable && sg.State != groupEmpty {
		g.rebalance()
	}
}

// atSessionTimeoutIn starts a session timeout timer with a custom duration.
// atSessionTimeout is a convenience wrapper that reads the timeout from the
// member's join request.
func (g *group) atSessionTimeoutIn(m *groupMember, d time.Duration, fn func()) {
	if m.t != nil {
		m.t.Stop()
	}
	m.last = time.Now()
	m.t = g.timerWork(d, func() {
		if time.Since(m.last) >= d {
			fn()
		}
	})
}

// restoreConsumerMembers restores 848 consumer group members from persisted
// session state. A member whose session expired while we were down is
// evicted as its session timeout would have: what it owned is freed and
// the group epoch is bumped.
func (g *group) restoreConsumerMembers(sg sessionConsumerGroup) {
	sessionTimeout := time.Duration(g.c.consumerSessionTimeoutMsForGroup(g.name)) * time.Millisecond

	g.partitionEpochs = sg.PartitionEpochs
	if g.partitionEpochs == nil {
		g.partitionEpochs = make(map[uuid]map[int32]partitionOwner)
	}
	g.targetAssignmentEpoch = sg.TargetAssignmentEpoch

	var expired bool
	for _, sm := range sg.Members {
		lastHB := sm.LastHeartbeat
		remaining := sessionTimeout - time.Since(lastHB)
		if remaining <= 0 {
			g.c.cfg.logger.Logf(LogLevelDebug, "restoreConsumerMembers: group=%s member=%s expired (lastHB=%v ago)",
				g.logName(), sm.ID, time.Since(lastHB))
			g.removePartitionEpochs(sm.Reconciled, sm.ID)
			g.removePartitionEpochs(sm.PendingRevoke, sm.ID)
			g.releaseStaticMember(sm.InstanceID, sm.ID)
			expired = true
			continue
		}

		m := &consumerMember{
			memberID:                    sm.ID,
			instanceID:                  sm.InstanceID,
			clientID:                    sm.ClientID,
			clientHost:                  sm.ClientHost,
			memberEpoch:                 sm.Epoch,
			previousMemberEpoch:         sm.PrevEpoch,
			state:                       consumerMemberState(sm.CmState),
			subscribedTopics:            sm.Topics,
			lastReconciledSent:          sm.Reconciled,
			partitionsPendingRevocation: sm.PendingRevoke,
			targetAssignment:            sm.Target,
			partAssignmentEpochs:        sm.PartAssignmentEpochs,
			rackID:                      sm.Rack,
			serverAssignor:              sm.Assignor,
			rebalanceTimeoutMs:          sm.RebalanceTimeoutMs,
		}
		if sm.Regex != "" {
			if re, err := compileSubscribedTopicRegex(sm.Regex); err == nil {
				m.subscribedTopicRegex = re
				m.subscribedRegexSource = sm.Regex
			}
		}
		if m.instanceID != nil {
			g.staticMembers[*m.instanceID] = m.memberID
		}
		g.consumerMembers[m.memberID] = m
		g.atConsumerSessionTimeoutIn(m, remaining)
		// Preserve the real last heartbeat time so that across
		// repeated restarts, dead members accumulate elapsed time
		// instead of resetting to "just now" each restore.
		m.last = lastHB
	}
	if expired {
		g.groupEpoch++
		g.computeTargetAssignment()
		g.persistMeta848()
	} else {
		_, g.subscribedTPs = g.subscriptions()
	}
	g.updateConsumerStateField()
}

// atConsumerSessionTimeoutIn starts a consumer session timeout timer with a
// custom initial duration. Used after loading session state.
func (g *group) atConsumerSessionTimeoutIn(m *consumerMember, d time.Duration) {
	if m.t != nil {
		m.t.Stop()
	}
	m.last = time.Now()
	m.t = g.timerWork(d, func() {
		// A timer that already fired cannot be retracted. Member IDs
		// are client supplied, and a static member that leaves and
		// rejoins is a new object at the same key, so identity has to
		// be checked as well as staleness.
		if g.consumerMembers[m.memberID] != m || time.Since(m.last) < d {
			return
		}
		g.c.cfg.logger.Logf(LogLevelWarn, "consumerSessionTimeout: group=%s member=%s epoch=%d remaining=%d",
			g.logName(), m.memberID, m.memberEpoch, len(g.consumerMembers)-1)
		g.evictConsumerMember(m)
	})
}

// compileSubscribedTopicRegex compiles a KIP-848 subscription regex. Kafka
// matches the regex against the entire topic name, so we anchor it.
func compileSubscribedTopicRegex(pattern string) (*regexp.Regexp, error) {
	return regexp.Compile("^(?:" + pattern + ")$")
}
