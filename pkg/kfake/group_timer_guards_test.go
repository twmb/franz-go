package kfake

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// A timer that already fired cannot be retracted. The tests below reproduce
// that window rather than racing for it: c.admin runs on the cluster loop, so
// taking the fired closure off c.groupWorkCh from inside an admin function
// means the loop cannot deliver it while the test arranges what happens next.
// Delivering it afterward is what run() does with it. Each test fails with
// only its own guard removed.

// onGroup hands fn the group from the cluster loop, which owns its state.
func onGroup(t *testing.T, c *Cluster, groupID string, fn func(g *group)) {
	t.Helper()
	c.admin(func() {
		g := c.groups.gs[groupID]
		if g == nil {
			t.Errorf("group %s is missing", groupID)
			return
		}
		fn(g)
	})
}

// staleFire runs arm on the loop, where it must start a timer that fires at
// once, and takes the closure that timer queues before the loop can deliver
// it. inline then runs, and the stale closure is delivered after it.
func staleFire(t *testing.T, c *Cluster, groupID string, arm func(g *group), inline func()) {
	t.Helper()
	var stale func()
	onGroup(t, c, groupID, func(g *group) {
		arm(g)
		select {
		case stale = <-c.groupWorkCh:
		case <-time.After(5 * time.Second):
		}
	})
	if stale == nil {
		t.Fatal("the timer queued no work")
	}
	inline()
	c.admin(stale)
}

// newGuardCluster returns a one-broker cluster, a plain client on it, and a
// context for the requests below.
func newGuardCluster(t *testing.T, opts ...Opt) (*Cluster, *kgo.Client, context.Context) {
	t.Helper()
	c := newCluster(t, append([]Opt{NumBrokers(1)}, opts...)...)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	t.Cleanup(cancel)
	return c, newPlainClient(t, c), ctx
}

// classicJoin sends a JoinGroup for one "range" protocol, retrying with the
// member ID we hand back on MEMBER_ID_REQUIRED. Pass an empty member ID for a
// new member. what names the step in any failure.
func classicJoin(ctx context.Context, t *testing.T, cl *kgo.Client, what, groupID, memberID string) *kmsg.JoinGroupResponse {
	t.Helper()
	join := kmsg.NewPtrJoinGroupRequest()
	join.Group = groupID
	join.MemberID = memberID
	join.SessionTimeoutMillis = 30000
	join.RebalanceTimeoutMillis = 10000
	join.ProtocolType = "consumer"
	proto := kmsg.NewJoinGroupRequestProtocol()
	proto.Name = "range"
	proto.Metadata = []byte{0, 0, 0, 0, 0, 0, 0, 0}
	join.Protocols = append(join.Protocols, proto)
	resp, err := join.RequestWith(ctx, cl)
	if err == nil && resp.ErrorCode == kerr.MemberIDRequired.Code {
		join.MemberID = resp.MemberID
		resp, err = join.RequestWith(ctx, cl)
	}
	if err != nil {
		t.Fatalf("%s: %v", what, err)
	}
	if resp.ErrorCode != 0 {
		t.Fatalf("%s: %v", what, kerr.ErrorForCode(resp.ErrorCode))
	}
	return resp
}

// The classic session timeout closure must check that the member is still the
// one in g.members. removeMember is not idempotent, so a timer that fired
// before the member left would decrement g.protocols a second time, and that
// counter is never rebuilt.
func TestClassicSessionTimerStaleFireKeepsProtocolCounts(t *testing.T) {
	t.Parallel()
	const groupID = "g-classic-stale-session-fire"

	c, cl, ctx := newGuardCluster(t)

	memberID := classicJoin(ctx, t, cl, "join", groupID, "").MemberID

	// The session deadline fires, and inline the member leaves the way
	// LeaveGroup removes it.
	staleFire(t, c, groupID, func(g *group) {
		m := g.members[memberID]
		if m == nil {
			t.Error("member is missing")
			return
		}
		m.join.SessionTimeoutMillis = 10
		g.updateHeartbeat(m)
	}, func() {
		onGroup(t, c, groupID, func(g *group) {
			g.updateMemberAndRebalance(g.members[memberID], nil, nil)
		})
	})

	// A fresh member must still be able to join. A second removal leaves
	// g.protocols["range"] at -1, and protocolsMatch then answers
	// INCONSISTENT_GROUP_PROTOCOL to this and every later joiner.
	classicJoin(ctx, t, cl, "a later join, after the stale session timer", groupID, "")
}

// The classic rebalance deadline must check the generation it belongs to.
// completeRebalance's state check alone misses preparing to completing and
// back to preparing: the group is preparing again under a new generation, and
// a stale run clears that new barrier, removing every member that is not
// waiting in join.
func TestClassicRebalanceTimerStaleFireKeepsNewBarrier(t *testing.T) {
	t.Parallel()
	const groupID = "g-classic-stale-rebalance-fire"

	c, cl, ctx := newGuardCluster(t)

	memberID := classicJoin(ctx, t, cl, "join", groupID, "").MemberID

	// arm puts the group into a rebalance whose deadline fires at once.
	// Inline, the member rejoins, which completes that rebalance under a
	// new generation, and the group then enters another one. That second
	// rebalance is the barrier the stale closure must not clear.
	var gen int32
	staleFire(t, c, groupID, func(g *group) {
		m := g.members[memberID]
		if m == nil {
			t.Error("member is missing")
			return
		}
		m.join.RebalanceTimeoutMillis = 10
		g.rebalance()
	}, func() {
		gen = classicJoin(ctx, t, cl, "rejoin", groupID, memberID).Generation
		onGroup(t, c, groupID, func(g *group) { g.rebalance() })
	})

	// The member is still in the group, waiting on the new barrier. A
	// stale run completes the old rebalance instead, and the member is not
	// waiting in join, so it is removed.
	hb := kmsg.NewPtrHeartbeatRequest()
	hb.Group = groupID
	hb.MemberID = memberID
	hb.Generation = gen
	resp, err := hb.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if resp.ErrorCode != kerr.RebalanceInProgress.Code {
		t.Fatalf("heartbeat answered %v, want REBALANCE_IN_PROGRESS: the stale rebalance deadline cleared the new barrier", kerr.ErrorForCode(resp.ErrorCode))
	}
}

// consumer848 joins memberID into the group and acks the assignment it is
// given, so the member is reconciled, and returns a sender for its later
// heartbeats. The sender echoes that assignment back; a non-positive epoch is
// a join or a leave, which must own nothing.
func consumer848(ctx context.Context, t *testing.T, cl *kgo.Client, topic, groupID, memberID string, instanceID *string) func(what string, epoch int32) *kmsg.ConsumerGroupHeartbeatResponse {
	t.Helper()
	var owned []kmsg.ConsumerGroupHeartbeatRequestTopic
	send := func(what string, epoch int32) *kmsg.ConsumerGroupHeartbeatResponse {
		t.Helper()
		req := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		req.Group = groupID
		req.MemberID = memberID
		req.MemberEpoch = epoch
		req.InstanceID = instanceID
		req.RebalanceTimeoutMillis = 5000
		req.SubscribedTopicNames = []string{topic}
		req.Topics = []kmsg.ConsumerGroupHeartbeatRequestTopic{}
		if epoch > 0 {
			req.Topics = owned
		}
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatalf("%s: %v", what, err)
		}
		if resp.ErrorCode != 0 {
			t.Fatalf("%s: %v", what, kerr.ErrorForCode(resp.ErrorCode))
		}
		return resp
	}
	joined := send("848 join", 0)
	if joined.Assignment == nil || len(joined.Assignment.Topics) == 0 {
		t.Fatal("the 848 join returned no assignment")
	}
	for _, at := range joined.Assignment.Topics {
		tp := kmsg.NewConsumerGroupHeartbeatRequestTopic()
		tp.TopicID = at.TopicID
		tp.Partitions = at.Partitions
		owned = append(owned, tp)
	}
	send("848 ack", joined.MemberEpoch)
	return send
}

// The 848 session timeout closure must check that the member is still the one
// in g.consumerMembers. A static member that leaves and rejoins under the same
// member ID is a new object at the same key, so a timer that fired before the
// rejoin would evict the live replacement.
func TestConsumerSessionTimerStaleFireKeepsRejoinedMember(t *testing.T) {
	t.Parallel()
	const (
		topic      = "t-848-stale-session-fire"
		groupID    = "g-848-stale-session-fire"
		memberID   = "m-848-stale-session-fire"
		instanceID = "inst-848-stale-session-fire"
	)

	c, cl, ctx := newGuardCluster(t, SeedTopics(1, topic))
	hb := consumer848(ctx, t, cl, topic, groupID, memberID, kmsg.StringPtr(instanceID))

	// The session deadline fires, and inline the member leaves statically
	// and rejoins, which puts a new object at the same map key.
	var epoch int32
	staleFire(t, c, groupID, func(g *group) {
		m := g.consumerMembers[memberID]
		if m == nil {
			t.Error("member is missing")
			return
		}
		g.atConsumerSessionTimeoutIn(m, 10*time.Millisecond)
	}, func() {
		hb("static leave", -2)
		epoch = hb("rejoin", 0).MemberEpoch
	})

	// The replacement must still be a member with its partitions. The
	// stale closure evicts by the member ID it captured, so without the
	// identity check it deletes the replacement.
	hb("heartbeat after the rejoin", epoch)
	onGroup(t, c, groupID, func(g *group) {
		if m := g.consumerMembers[memberID]; m == nil || countAssignment(m.lastReconciledSent) == 0 {
			t.Error("the rejoined member lost its partitions to the stale session timer")
		}
	})
}

// The 848 per-member rebalance timeout must check identity as well as the
// epoch it captured. A full leave deletes the member without touching its
// epoch, so a member that leaves and rejoins under the same client supplied
// ID leaves the captured object at the epoch the timer was watching, with a
// different object now at that key.
func TestConsumerRebalanceTimerStaleFireKeepsRejoinedMember(t *testing.T) {
	t.Parallel()
	const (
		topic    = "t-848-stale-rebalance-fire"
		groupID  = "g-848-stale-rebalance-fire"
		memberID = "m-848-stale-rebalance-fire"
	)

	c, cl, ctx := newGuardCluster(t, SeedTopics(1, topic))
	hb := consumer848(ctx, t, cl, topic, groupID, memberID, nil)

	// The revocation deadline fires, and inline the member leaves and
	// rejoins, which puts a new object at the same map key.
	var epoch int32
	staleFire(t, c, groupID, func(g *group) {
		m := g.consumerMembers[memberID]
		if m == nil {
			t.Error("member is missing")
			return
		}
		m.rebalanceTimeoutMs = 10
		g.scheduleConsumerRebalanceTimeout(m)
	}, func() {
		hb("leave", -1)
		epoch = hb("rejoin", 0).MemberEpoch
	})

	// The replacement must still be a member.
	hb("heartbeat after the rejoin", epoch)
}
