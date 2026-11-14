package kfake

import (
	"slices"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Two members of an 848 group sit at the same epoch as a matter of course:
// each one advances to the group's target assignment epoch. The epoch
// therefore cannot say which member owns a partition. A removal that matches
// on it takes the other member's entry along, and the partition then reads as
// free while it is still assigned. An old-epoch commit is accepted only for
// a partition the member owned at that epoch or is revoking (KIP-1251).
func TestConsumerPartitionOwnership(t *testing.T) {
	t.Parallel()
	const (
		topic   = "t-848-fence-other-owner"
		groupID = "g-848-fence-other-owner"
	)

	c, cl, ctx := newGuardCluster(t, SeedTopics(2, topic))
	// commit commits parts at offset as member at epoch and returns each
	// partition's error.
	commit := func(member string, epoch int32, offset int64, parts ...int32) []int16 {
		req := kmsg.NewPtrOffsetCommitRequest()
		req.Group, req.MemberID, req.Generation = groupID, member, epoch
		ct := kmsg.NewOffsetCommitRequestTopic()
		ct.Topic, ct.TopicID = topic, c.TopicInfo(topic).TopicID
		for _, p := range parts {
			cp := kmsg.NewOffsetCommitRequestTopicPartition()
			cp.Partition, cp.Offset = p, offset
			ct.Partitions = append(ct.Partitions, cp)
		}
		req.Topics = append(req.Topics, ct)
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		var codes []int16
		for _, p := range resp.Topics[0].Partitions {
			codes = append(codes, p.ErrorCode)
		}
		return codes
	}
	hb1 := member848(ctx, t, cl, topic, groupID, "m1", nil)
	hb2 := member848(ctx, t, cl, topic, groupID, "m2", nil)

	// m1 takes both partitions. m2 then joins and is owed one, so m1 is
	// told to revoke it, m1 confirms the revocation, and m2 claims it.
	e1 := hb1("m1 join", 0).MemberEpoch
	e1 = hb1("m1 ack", e1).MemberEpoch
	e2 := hb2("m2 join", 0).MemberEpoch
	e1 = hb1("m1 revoke", e1).MemberEpoch
	revoking := int32(-1)
	onGroup(t, c, groupID, func(g *group) {
		for _, parts := range g.consumerMembers["m1"].partitionsPendingRevocation {
			revoking = parts[0]
		}
	})
	if revoking < 0 {
		t.Fatal("m1 is revoking nothing")
	}
	// Like Kafka, a commit of a partition being revoked is checked
	// against the epoch the partition was assigned at.
	if codes := commit("m1", e1-1, 5, revoking); codes[0] != kerr.StaleMemberEpoch.Code {
		t.Errorf("m1 commit at epoch %d, before it was assigned the partition it is revoking: %v, want STALE_MEMBER_EPOCH", e1-1, kerr.ErrorForCode(codes[0]))
	}
	if codes := commit("m1", e1, 5, revoking); codes[0] != 0 {
		t.Errorf("m1 commit at epoch %d of the partition it is revoking: %v", e1, kerr.ErrorForCode(codes[0]))
	}
	e1 = hb1("m1 confirm the revocation", e1).MemberEpoch
	e2 = hb2("m2 claim", e2).MemberEpoch
	if e1 != e2 {
		t.Fatalf("m1 is at epoch %d and m2 at %d; this test needs them level", e1, e2)
	}

	// A commit from m1 at an older epoch is accepted for the partition m1
	// has owned since then. One that also names m2's partition fails as a
	// whole, as in Kafka, and commits nothing. m2 has owned its partition
	// only since its current epoch. A transactional commit follows the
	// same rule.
	txn := initProducerID(t, cl, "txn-848-fence-other-owner", -1, -1, 60000)
	if txn.ErrorCode != 0 {
		t.Fatalf("init producer id: %v", kerr.ErrorForCode(txn.ErrorCode))
	}
	var m1Part, m2Part int32
	onGroup(t, c, groupID, func(g *group) {
		for _, parts := range g.consumerMembers["m1"].lastReconciledSent {
			m1Part = parts[0]
		}
		for _, parts := range g.consumerMembers["m2"].lastReconciledSent {
			m2Part = parts[0]
		}
	})
	for _, tc := range []struct {
		member string
		epoch  int32
		parts  []int32
		want   int16
	}{
		{"m1", e1 - 1, []int32{m1Part, m2Part}, kerr.StaleMemberEpoch.Code},
		{"m1", e1 - 1, []int32{m1Part}, 0},
		{"m1", -1, []int32{m1Part}, kerr.StaleMemberEpoch.Code}, // a member at no epoch is older than every assignment
	} {
		for i, code := range commit(tc.member, tc.epoch, int64(len(tc.parts)), tc.parts...) {
			if code != tc.want {
				t.Errorf("%s commit at epoch %d to partitions %v: partition %d got %v, want %v", tc.member, tc.epoch, tc.parts, tc.parts[i], kerr.ErrorForCode(code), kerr.ErrorForCode(tc.want))
			}
		}

		tcommit := kmsg.NewPtrTxnOffsetCommitRequest()
		tcommit.TransactionalID, tcommit.ProducerID, tcommit.ProducerEpoch = "txn-848-fence-other-owner", txn.ProducerID, txn.ProducerEpoch
		tcommit.Group, tcommit.MemberID, tcommit.Generation = groupID, tc.member, tc.epoch
		tt := kmsg.NewTxnOffsetCommitRequestTopic()
		tt.Topic, tt.TopicID = topic, c.TopicInfo(topic).TopicID
		for _, p := range tc.parts {
			tp := kmsg.NewTxnOffsetCommitRequestTopicPartition()
			tp.Partition, tp.Offset = p, 1
			tt.Partitions = append(tt.Partitions, tp)
		}
		tcommit.Topics = append(tcommit.Topics, tt)
		tresp, err := tcommit.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		want := tc.want
		if want != 0 && tresp.Version < 6 {
			want = kerr.IllegalGeneration.Code // KIP-1319
		}
		for _, p := range tresp.Topics[0].Partitions {
			if p.ErrorCode != want {
				t.Errorf("%s txn commit at epoch %d to partitions %v: partition %d got %v, want %v", tc.member, tc.epoch, tc.parts, p.Partition, kerr.ErrorForCode(p.ErrorCode), kerr.ErrorForCode(want))
			}
		}
	}
	// Only the revoking commit and the second commit, of one partition
	// at offset 1, landed.
	commits := groupCommits(c, groupID)[topic]
	if len(commits) != 2 || commits[m1Part].Offset != 1 || commits[m2Part].Offset != 5 {
		t.Errorf("after the commits, the group holds %v, want partition %d at offset 1 and %d at 5", commits, m1Part, m2Part)
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
// place in the group: it holds the instance ID mapping it rejoins through,
// and it still subscribes to its topics. Offsets of a topic it subscribes
// to can be neither expired nor deleted. Offset expiry must leave such a
// group alone, even once every offset the group held has expired, or that
// mapping goes with it.
func TestExpireOffsetsKeepsGroupHoldingParkedStaticMember(t *testing.T) {
	t.Parallel()
	const (
		topic      = "t-848-expire-parked-static"
		other      = "t-848-expire-parked-static-other"
		groupID    = "g-848-expire-parked-static"
		memberID   = "m-848-expire-parked-static"
		instanceID = "inst-848-expire-parked-static"
	)

	c, cl, ctx := newGuardCluster(t, SeedTopics(1, topic, other))
	hb := member848(ctx, t, cl, topic, groupID, memberID, kmsg.StringPtr(instanceID))
	joined := hb("join", 0)
	if joined.Assignment == nil || len(joined.Assignment.Topics) == 0 {
		t.Fatal("the 848 join returned no assignment")
	}
	epoch := hb("ack", joined.MemberEpoch).MemberEpoch

	// The member commits to its own topic and to one it does not
	// subscribe to.
	commit := kmsg.NewPtrOffsetCommitRequest()
	commit.Group = groupID
	commit.MemberID = memberID
	commit.Generation = epoch
	for _, name := range []string{topic, other} {
		ct := kmsg.NewOffsetCommitRequestTopic()
		ct.Topic = name
		ct.TopicID = c.TopicInfo(name).TopicID
		cp := kmsg.NewOffsetCommitRequestTopicPartition()
		cp.Offset = 1
		ct.Partitions = append(ct.Partitions, cp)
		commit.Topics = append(commit.Topics, ct)
	}
	commitResp, err := commit.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}
	for _, rt := range commitResp.Topics {
		if code := rt.Partitions[0].ErrorCode; code != 0 {
			t.Fatalf("commit %s: %v", rt.Topic, kerr.ErrorForCode(code))
		}
	}

	hb("static leave", -2)

	// Deleting the parked member's topic's offset is refused.
	del := kmsg.NewPtrOffsetDeleteRequest()
	del.Group = groupID
	dt := kmsg.NewOffsetDeleteRequestTopic()
	dt.Topic = topic
	dt.Partitions = append(dt.Partitions, kmsg.NewOffsetDeleteRequestTopicPartition())
	del.Topics = append(del.Topics, dt)
	delResp, err := del.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("offset delete: %v", err)
	}
	if delResp.ErrorCode != 0 || len(delResp.Topics) != 1 || delResp.Topics[0].Partitions[0].ErrorCode != kerr.GroupSubscribedToTopic.Code {
		t.Errorf("offset delete of the parked member's topic: got %+v, want GROUP_SUBSCRIBED_TO_TOPIC", delResp)
	}

	// The offsets age past retention and the sweep runs. Only the
	// offset of the topic nobody subscribes to expires.
	sweep := func() {
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
	}
	sweep()
	onGroup(t, c, groupID, func(g *group) {
		if _, ok := g.commits.getp(topic, 0); !ok {
			t.Error("the offset of the parked member's topic expired")
		}
		if _, ok := g.commits.getp(other, 0); ok {
			t.Error("the offset of the topic nobody subscribes to did not expire")
		}
		g.deleteCommitAndPersist(topic, 0)
	})

	// With every offset gone, the sweep reaches the group delete. The
	// group holding the member must stay.
	sweep()
	c.admin(func() {
		g := c.groups.gs[groupID]
		if g == nil {
			t.Error("the sweep deleted the group, and the static member parked in it went with it")
			return
		}
		if got := g.staticMembers[instanceID]; got != memberID {
			t.Errorf("the instance maps to member %q, want %s", got, memberID)
		}
		if m := g.consumerMembers[memberID]; m == nil || m.memberEpoch != -2 {
			t.Error("the parked static member is gone")
		}
	})
}

// A static member that leaves at epoch -2 keeps its partitions while it is
// away: the group is Reconciling, and the other member does not receive
// them. If it does not come back within the session timeout, it is evicted
// and the other member takes its partitions.
func TestConsumerParkedStaticMemberExpires(t *testing.T) {
	t.Parallel()
	const (
		topic   = "t-848-parked-static-expires"
		groupID = "g-848-parked-static-expires"
	)

	c, cl, ctx := newGuardCluster(t, SeedTopics(2, topic), BrokerConfigs(map[string]string{"group.consumer.session.timeout.ms": "2000"}))
	hb1 := member848(ctx, t, cl, topic, groupID, "m1", kmsg.StringPtr("i1"))
	hb2 := member848(ctx, t, cl, topic, groupID, "m2", nil)
	owned := func(resp *kmsg.ConsumerGroupHeartbeatResponse) (n int) {
		for _, at := range resp.Assignment.Topics {
			n += len(at.Partitions)
		}
		return n
	}

	// m1 and m2 end up with one partition each, then m1 parks.
	e1 := hb1("m1 join", 0).MemberEpoch
	e1 = hb1("m1 ack", e1).MemberEpoch
	e2 := hb2("m2 join", 0).MemberEpoch
	e1 = hb1("m1 revoke", e1).MemberEpoch
	hb1("m1 confirm the revocation", e1)
	e2 = hb2("m2 claim", e2).MemberEpoch
	hb1("m1 static leave", -2)
	if state := c.GroupInfo(groupID).State; state != "Reconciling" {
		t.Errorf("with m1 parked, the group is %s, want Reconciling", state)
	}

	// A third partition forces a rebalance; m2 must not be given m1's.
	cp := kmsg.NewPtrCreatePartitionsRequest()
	cpt := kmsg.NewCreatePartitionsRequestTopic()
	cpt.Topic, cpt.Count = topic, 3
	cp.Topics = append(cp.Topics, cpt)
	if _, err := cp.RequestWith(ctx, cl); err != nil {
		t.Fatal(err)
	}
	resp := hb2("m2 after the rebalance", e2)
	e2 = resp.MemberEpoch
	onGroup(t, c, groupID, func(g *group) {
		for id, parts := range g.consumerMembers["m1"].lastReconciledSent {
			for _, p := range parts {
				if slices.Contains(g.consumerMembers["m2"].targetAssignment[id], p) {
					t.Errorf("with m1 parked, m2's target assignment holds m1's partition %d", p)
				}
			}
		}
	})

	// m1's session times out and m2 takes everything.
	for start := time.Now(); owned(resp) != 3; time.Sleep(50 * time.Millisecond) {
		if time.Since(start) > 5*time.Second {
			t.Fatalf("m2 owns %d partitions 5s after m1 parked with a 2s session timeout, want 3", owned(resp))
		}
		resp = hb2("m2 heartbeat", e2)
		e2 = resp.MemberEpoch
	}
	if g := c.GroupInfo(groupID); len(g.Members) != 1 {
		t.Errorf("the group holds %d members after m1's session expired, want 1", len(g.Members))
	}
}
