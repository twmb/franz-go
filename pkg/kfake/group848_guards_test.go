package kfake

import (
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Two members of an 848 group sit at the same epoch as a matter of course:
// each one advances to the group's target assignment epoch. The epoch
// therefore cannot say which member owns a partition. A removal that matches
// on it takes the other member's entry along, and the partition then reads as
// free while it is still assigned.
func TestConsumerFenceKeepsOtherMemberPartitions(t *testing.T) {
	t.Parallel()
	const (
		topic   = "t-848-fence-other-owner"
		groupID = "g-848-fence-other-owner"
	)

	c, cl, ctx := newGuardCluster(t, SeedTopics(2, topic))
	hb1 := member848(ctx, t, cl, topic, groupID, "m1", nil)
	hb2 := member848(ctx, t, cl, topic, groupID, "m2", nil)

	// m1 takes both partitions. m2 then joins and is owed one, so m1 is
	// told to revoke it, m1 confirms the revocation, and m2 claims it.
	e1 := hb1("m1 join", 0).MemberEpoch
	e1 = hb1("m1 ack", e1).MemberEpoch
	e2 := hb2("m2 join", 0).MemberEpoch
	e1 = hb1("m1 revoke", e1).MemberEpoch
	e1 = hb1("m1 confirm the revocation", e1).MemberEpoch
	e2 = hb2("m2 claim", e2).MemberEpoch
	if e1 != e2 {
		t.Fatalf("m1 is at epoch %d and m2 at %d; this test needs them level", e1, e2)
	}

	onGroup(t, c, groupID, func(g *group) {
		m1, m2 := g.consumerMembers["m1"], g.consumerMembers["m2"]
		if m1 == nil || m2 == nil {
			t.Error("a member is missing")
			return
		}
		if n1, n2 := countAssignment(m1.lastReconciledSent), countAssignment(m2.lastReconciledSent); n1 != 1 || n2 != 1 {
			t.Errorf("m1 holds %d partitions and m2 holds %d, want one each", n1, n2)
			return
		}
		// m1's record of what it holds has gone stale and names m2's
		// partition as well. Fencing m1 must free only m1's.
		for id, parts := range m2.lastReconciledSent {
			m1.lastReconciledSent[id] = append(m1.lastReconciledSent[id], parts...)
		}
		g.fenceConsumerMember(m1)
		for id, parts := range m2.lastReconciledSent {
			for _, p := range parts {
				if g.currentPartitionEpoch(id, p) == -1 {
					t.Errorf("fencing m1 freed partition %d, which m2 still owns at epoch %d", p, m2.memberEpoch)
				}
			}
		}
	})
}

// A static member that temporarily leaves parks at epoch -2 and keeps its
// place in the group: it holds the instance ID mapping it rejoins through.
// Offset expiry must leave such a group alone, even once every offset the
// group held has expired, or that mapping goes with it.
func TestExpireOffsetsKeepsGroupHoldingParkedStaticMember(t *testing.T) {
	t.Parallel()
	const (
		topic      = "t-848-expire-parked-static"
		groupID    = "g-848-expire-parked-static"
		memberID   = "m-848-expire-parked-static"
		instanceID = "inst-848-expire-parked-static"
	)

	c, cl, ctx := newGuardCluster(t, SeedTopics(1, topic))
	hb := member848(ctx, t, cl, topic, groupID, memberID, kmsg.StringPtr(instanceID))
	joined := hb("join", 0)
	if joined.Assignment == nil || len(joined.Assignment.Topics) == 0 {
		t.Fatal("the 848 join returned no assignment")
	}
	epoch := hb("ack", joined.MemberEpoch).MemberEpoch

	commit := kmsg.NewPtrOffsetCommitRequest()
	commit.Group = groupID
	commit.MemberID = memberID
	commit.Generation = epoch
	ct := kmsg.NewOffsetCommitRequestTopic()
	ct.Topic = topic
	ct.TopicID = joined.Assignment.Topics[0].TopicID
	cp := kmsg.NewOffsetCommitRequestTopicPartition()
	cp.Offset = 1
	ct.Partitions = append(ct.Partitions, cp)
	commit.Topics = append(commit.Topics, ct)
	commitResp, err := commit.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	if code := commitResp.Topics[0].Partitions[0].ErrorCode; code != 0 {
		t.Fatalf("commit: %v", kerr.ErrorForCode(code))
	}

	hb("static leave", -2)

	// The offset ages past retention and the sweep runs. A parked member
	// does not count as a subscriber, so the offset does expire. The group
	// holding the member must not.
	c.admin(func() {
		g := c.groups.gs[groupID]
		if g == nil {
			t.Error("the group is missing before the sweep")
			return
		}
		old := time.Now().Add(-2 * time.Duration(c.offsetsRetentionMs()) * time.Millisecond)
		g.commits.each(func(_ string, _ int32, oc *offsetCommit) { oc.lastCommit = old })
		c.expireGroupOffsets()
	})

	c.admin(func() {
		g := c.groups.gs[groupID]
		if g == nil {
			t.Error("the sweep deleted the group, and the static member parked in it went with it")
			return
		}
		if n := len(g.commits); n != 0 {
			t.Errorf("the group still holds offsets for %d topics, so the sweep never reached the delete", n)
		}
		if got := g.staticMembers[instanceID]; got != memberID {
			t.Errorf("the instance maps to member %q, want %s", got, memberID)
		}
		if m := g.consumerMembers[memberID]; m == nil || m.memberEpoch != -2 {
			t.Error("the parked static member is gone")
		}
	})
}
