package kfake

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestCompleteLeaderSyncEmitsEmptyAssignment(t *testing.T) {
	c := newTestCluster()
	defer close(c.die)

	b := &broker{c: c, node: 0}
	c.bs = []*broker{b}

	g := (&groups{c: c}).newGroup("group")
	g.protocolType = "consumer"
	g.protocol = "range"
	g.generation = 1
	g.state = groupCompletingRebalance
	g.leader = "leader"

	leader := newTestGroupMember("leader")
	follower := newTestGroupMember("follower")
	g.members = map[string]*groupMember{
		leader.memberID:   leader,
		follower.memberID: follower,
	}

	leaderReq := newSyncClientReq(c, b, "group", leader.memberID, g.generation)
	followerReq := newSyncClientReq(c, b, "group", follower.memberID, g.generation)
	leader.waitingReply = leaderReq
	follower.waitingReply = followerReq

	leaderSync := leaderReq.kreq.(*kmsg.SyncGroupRequest)
	leaderSync.GroupAssignment = append(leaderSync.GroupAssignment, newSyncAssignment("leader", map[string][]int32{
		"t": {0},
	}))

	g.completeLeaderSync(leaderSync)

	leaderResp := <-leaderReq.cc.respCh
	followerResp := <-followerReq.cc.respCh

	assertAssignment(t, leaderResp, func(a *kmsg.ConsumerMemberAssignment) {
		if len(a.Topics) != 1 || a.Topics[0].Topic != "t" || len(a.Topics[0].Partitions) != 1 || a.Topics[0].Partitions[0] != 0 {
			t.Fatalf("expected leader assignment topic t/0, got %#v", a.Topics)
		}
	})

	assertAssignment(t, followerResp, func(a *kmsg.ConsumerMemberAssignment) {
		if len(a.Topics) != 0 {
			t.Fatalf("expected follower assignment to be empty, got %#v", a.Topics)
		}
	})

	stopMemberTimer(leader)
	stopMemberTimer(follower)
}

func TestHandleSyncStableReturnsEmptyAssignment(t *testing.T) {
	c := newTestCluster()
	defer close(c.die)

	b := &broker{c: c, node: 0}
	c.bs = []*broker{b}

	g := (&groups{c: c}).newGroup("group")
	g.protocolType = "consumer"
	g.protocol = "range"
	g.generation = 3
	g.state = groupStable

	member := newTestGroupMember("member")
	g.members = map[string]*groupMember{
		member.memberID: member,
	}

	req := newSyncClientReq(c, b, "group", member.memberID, g.generation)
	resp := g.handleSync(req)

	sgResp, ok := resp.(*kmsg.SyncGroupResponse)
	if !ok {
		t.Fatalf("expected SyncGroupResponse, got %T", resp)
	}
	var assn kmsg.ConsumerMemberAssignment
	if err := assn.ReadFrom(sgResp.MemberAssignment); err != nil {
		t.Fatalf("failed to decode assignment: %v", err)
	}
	if len(assn.Topics) != 0 {
		t.Fatalf("expected empty assignment, got %#v", assn.Topics)
	}
}

func newTestCluster() *Cluster {
	c := &Cluster{
		cfg: cfg{
			minSessionTimeout: time.Second,
			maxSessionTimeout: time.Minute,
		},
		die: make(chan struct{}),
	}
	c.groups.c = c
	return c
}

func newTestGroupMember(id string) *groupMember {
	return &groupMember{
		memberID: id,
		join: &kmsg.JoinGroupRequest{
			ProtocolType:           "consumer",
			Protocols:              []kmsg.JoinGroupRequestProtocol{{Name: "range"}},
			SessionTimeoutMillis:   int32((10 * time.Minute) / time.Millisecond),
			RebalanceTimeoutMillis: int32((10 * time.Minute) / time.Millisecond),
		},
	}
}

func newSyncClientReq(c *Cluster, b *broker, group, member string, generation int32) *clientReq {
	req := kmsg.NewPtrSyncGroupRequest()
	req.Version = 5
	req.Group = group
	req.MemberID = member
	req.Generation = generation
	req.ProtocolType = kmsg.StringPtr("consumer")
	req.Protocol = kmsg.StringPtr("range")
	cc := &clientConn{
		c:      c,
		b:      b,
		respCh: make(chan clientResp, 1),
	}
	return &clientReq{
		cc:   cc,
		kreq: req,
	}
}

func newSyncAssignment(member string, topics map[string][]int32) kmsg.SyncGroupRequestGroupAssignment {
	var assignment kmsg.ConsumerMemberAssignment
	for topic, partitions := range topics {
		topicAssignment := kmsg.NewConsumerMemberAssignmentTopic()
		topicAssignment.Topic = topic
		topicAssignment.Partitions = append([]int32(nil), partitions...)
		assignment.Topics = append(assignment.Topics, topicAssignment)
	}
	sg := kmsg.NewSyncGroupRequestGroupAssignment()
	sg.MemberID = member
	sg.MemberAssignment = assignment.AppendTo(nil)
	return sg
}

func assertAssignment(t *testing.T, resp clientResp, fn func(*kmsg.ConsumerMemberAssignment)) {
	t.Helper()
	sgResp, ok := resp.kresp.(*kmsg.SyncGroupResponse)
	if !ok {
		t.Fatalf("expected SyncGroupResponse, got %T", resp.kresp)
	}
	if len(sgResp.MemberAssignment) == 0 {
		t.Fatalf("expected non-empty assignment payload")
	}
	var assn kmsg.ConsumerMemberAssignment
	if err := assn.ReadFrom(sgResp.MemberAssignment); err != nil {
		t.Fatalf("failed to decode assignment: %v", err)
	}
	fn(&assn)
}

func stopMemberTimer(m *groupMember) {
	if m != nil && m.t != nil {
		m.t.Stop()
	}
}
