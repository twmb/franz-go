package kfake

import (
	"testing"
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
