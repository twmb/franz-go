package kfake

import (
	"context"
	"errors"
	"slices"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// The share group assignment goes to a full heartbeat (one carrying
// SubscribedTopicNames) or when this heartbeat changed it, as on a real
// broker. A keepalive does not carry it again. A member's assignment, as the
// group shows it, changes only with the member's own heartbeat. A full group
// refuses a member it does not know, and a topic the group does not
// subscribe to leaves its epoch alone.
func TestShareHeartbeat(t *testing.T) {
	t.Parallel()
	const (
		topic = "t"
		group = "g"
		m1    = "11111111-2222-3333-4444-555555555555"
		m2    = "66666666-2222-3333-4444-555555555555"
	)
	c := newCluster(t, NumBrokers(1), SeedTopics(2, topic), BrokerConfigs(map[string]string{"group.share.max.size": "2"}))
	cl := newPlainClient(t, c)
	ctx := context.Background()

	heartbeat := func(member string, epoch int32, subscribe bool) *kmsg.ShareGroupHeartbeatResponse {
		t.Helper()
		req := kmsg.NewPtrShareGroupHeartbeatRequest()
		req.GroupID = group
		req.MemberID = member
		req.MemberEpoch = epoch
		if subscribe {
			req.SubscribedTopicNames = []string{topic}
		}
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}
	ok := func(resp *kmsg.ShareGroupHeartbeatResponse) *kmsg.ShareGroupHeartbeatResponse {
		t.Helper()
		if resp.ErrorCode != 0 {
			t.Fatalf("heartbeat: %v", kerr.ErrorForCode(resp.ErrorCode))
		}
		return resp
	}
	assigned := func(member string) int {
		t.Helper()
		for _, m := range c.GroupInfo(group).Members {
			if m.MemberID == member {
				return m.NumAssigned()
			}
		}
		t.Fatalf("member %s not in the group", member)
		return 0
	}

	join := ok(heartbeat(m1, 0, true))
	if join.Assignment == nil || len(join.Assignment.TopicPartitions) != 1 {
		t.Fatalf("join returned no assignment: %+v", join.Assignment)
	}
	if keepalive := ok(heartbeat(m1, join.MemberEpoch, false)); keepalive.Assignment != nil {
		t.Fatalf("a keepalive carried the assignment again: %+v", keepalive.Assignment)
	}
	if full := ok(heartbeat(m1, join.MemberEpoch, true)); full.Assignment == nil || len(full.Assignment.TopicPartitions) != 1 {
		t.Fatalf("a full heartbeat did not carry the assignment: %+v", full.Assignment)
	}

	// Like Kafka, the group shows what a member has received: a second
	// member's join halves the first member's target, and the first
	// member's assignment follows at its next heartbeat.
	ok(heartbeat(m2, 0, true))
	if n := assigned(m1); n != 2 {
		t.Errorf("before its heartbeat, m1 shows %d partitions, want 2", n)
	}
	ok(heartbeat(m1, join.MemberEpoch, false))
	if n := assigned(m1); n != 1 {
		t.Errorf("after its heartbeat, m1 shows %d partitions, want 1", n)
	}

	// Like Kafka, a full group refuses a member it does not know with
	// GROUP_MAX_SIZE_REACHED, whatever the member's epoch, and an error
	// has no heartbeat interval.
	if resp := heartbeat("77777777-2222-3333-4444-555555555555", 5, false); resp.ErrorCode != kerr.GroupMaxSizeReached.Code || resp.HeartbeatIntervalMillis != 0 {
		t.Errorf("unknown member of a full group: got %v with interval %d, want GROUP_MAX_SIZE_REACHED with 0", kerr.ErrorForCode(resp.ErrorCode), resp.HeartbeatIntervalMillis)
	}

	// A topic the group does not subscribe to leaves its epoch alone.
	epoch := c.GroupInfo(group).Epoch
	create := kmsg.NewPtrCreateTopicsRequest()
	ct := kmsg.NewCreateTopicsRequestTopic()
	ct.Topic, ct.NumPartitions, ct.ReplicationFactor = "unrelated", 1, 1
	create.Topics = append(create.Topics, ct)
	if resp, err := create.RequestWith(ctx, cl); err != nil || resp.Topics[0].ErrorCode != 0 {
		t.Fatalf("create topic: %v %+v", err, resp)
	}
	if got := c.GroupInfo(group).Epoch; got != epoch {
		t.Errorf("creating an unrelated topic moved the group epoch from %d to %d", epoch, got)
	}
}

// DeleteGroups deletes share groups too: Kafka has no share-specific
// delete API. A share group with a member answers NON_EMPTY_GROUP; an
// empty one stays listed until DeleteGroups drops it.
func TestShareGroupDeleteGroups(t *testing.T) {
	t.Parallel()
	const (
		topic = "no-such-topic" // the group has no partition state
		group = "g"
	)
	c := newCluster(t, NumBrokers(1))
	cl := newPlainClient(t, c)
	ctx := context.Background()

	heartbeat := func(epoch int32) {
		t.Helper()
		req := kmsg.NewPtrShareGroupHeartbeatRequest()
		req.GroupID = group
		req.MemberID = "11111111-2222-3333-4444-555555555555"
		req.MemberEpoch = epoch
		if epoch == 0 {
			req.SubscribedTopicNames = []string{topic}
		}
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		if resp.ErrorCode != 0 {
			t.Fatalf("heartbeat at epoch %d: %v", epoch, kerr.ErrorForCode(resp.ErrorCode))
		}
	}
	deleteGroup := func() error {
		t.Helper()
		req := kmsg.NewPtrDeleteGroupsRequest()
		req.Groups = []string{group}
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Groups) != 1 || resp.Groups[0].Group != group {
			t.Fatalf("unexpected DeleteGroups response groups: %+v", resp.Groups)
		}
		return kerr.ErrorForCode(resp.Groups[0].ErrorCode)
	}
	listed := func() bool {
		t.Helper()
		resp, err := kmsg.NewPtrListGroupsRequest().RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		for _, g := range resp.Groups {
			if g.Group == group {
				return true
			}
		}
		return false
	}

	heartbeat(0) // join
	if err := deleteGroup(); !errors.Is(err, kerr.NonEmptyGroup) {
		t.Fatalf("deleting a share group with a member: got %v, want NON_EMPTY_GROUP", err)
	}
	if !listed() {
		t.Fatal("share group vanished after a refused delete")
	}

	heartbeat(-1) // leave
	if !listed() {
		t.Fatal("empty share group is not listed")
	}
	if err := deleteGroup(); err != nil {
		t.Fatalf("deleting an empty share group: %v", err)
	}
	if listed() {
		t.Fatal("share group still listed after delete")
	}
	if err := deleteGroup(); !errors.Is(err, kerr.GroupIDNotFound) {
		t.Fatalf("deleting a deleted share group: got %v, want GROUP_ID_NOT_FOUND", err)
	}
}

// A group id is exclusively one type. The handlers that create a group
// on demand refuse an id the other map already holds, as Kafka's
// getOrMaybeCreateShareGroup and ShareGroup.validateOffsetCommit do, so
// ListGroups never lists an id twice.
func TestShareGroupTypeExclusive(t *testing.T) {
	t.Parallel()
	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c)
	ctx := context.Background()
	topicID := c.TopicInfo(topic).TopicID

	// alterShare creates a share group and returns the top-level error.
	alterShare := func(group string) error {
		t.Helper()
		req := kmsg.NewPtrAlterShareGroupOffsetsRequest()
		req.GroupID = group
		at := kmsg.NewAlterShareGroupOffsetsRequestTopic()
		at.Topic = topic
		ap := kmsg.NewAlterShareGroupOffsetsRequestTopicPartition()
		ap.Partition = 0
		at.Partitions = append(at.Partitions, ap)
		req.Topics = append(req.Topics, at)
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		return kerr.ErrorForCode(resp.ErrorCode)
	}
	// commit is a simple (generation -1) commit, which creates an empty
	// classic group when the id is free, and returns the partition error.
	commit := func(group string) error {
		t.Helper()
		req := kmsg.NewPtrOffsetCommitRequest()
		req.Group = group
		req.Generation = -1
		rt := kmsg.NewOffsetCommitRequestTopic()
		rt.Topic, rt.TopicID = topic, topicID // v10+ matches by TopicID
		rp := kmsg.NewOffsetCommitRequestTopicPartition()
		rp.Partition = 0
		rp.Offset = 1
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
			t.Fatalf("unexpected OffsetCommit response topics: %+v", resp.Topics)
		}
		return kerr.ErrorForCode(resp.Topics[0].Partitions[0].ErrorCode)
	}
	// listedTypes returns the GroupType of every ListGroups entry for
	// group: one entry when the id is exclusive, two when it is not.
	listedTypes := func(group string) []string {
		t.Helper()
		resp, err := kmsg.NewPtrListGroupsRequest().RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		var types []string
		for _, g := range resp.Groups {
			if g.Group == group {
				types = append(types, g.GroupType)
			}
		}
		return types
	}

	t.Run("OffsetCommit to a share group", func(t *testing.T) {
		const group = "share-then-commit"
		if err := alterShare(group); err != nil {
			t.Fatalf("creating the share group: %v", err)
		}
		if err := commit(group); !errors.Is(err, kerr.GroupIDNotFound) {
			t.Fatalf("committing to a share group: got %v, want GROUP_ID_NOT_FOUND", err)
		}
		init := kmsg.NewPtrInitProducerIDRequest()
		init.TransactionalID, init.TransactionTimeoutMillis = new("txn"), 60000
		ir, err := init.RequestWith(ctx, cl)
		if err != nil || ir.ErrorCode != 0 {
			t.Fatalf("init producer ID: %v %v", err, ir)
		}
		tc := kmsg.NewPtrTxnOffsetCommitRequest()
		tc.TransactionalID, tc.Group, tc.ProducerID, tc.ProducerEpoch, tc.Generation = "txn", group, ir.ProducerID, ir.ProducerEpoch, -1
		tc.Topics = []kmsg.TxnOffsetCommitRequestTopic{{Topic: topic, TopicID: topicID, Partitions: []kmsg.TxnOffsetCommitRequestTopicPartition{{Offset: 1}}}}
		if tr, err := tc.RequestWith(ctx, cl); err != nil || tr.Topics[0].Partitions[0].ErrorCode != kerr.GroupIDNotFound.Code {
			t.Fatalf("transactional commit to a share group: %v %v, want GROUP_ID_NOT_FOUND", err, tr)
		}
		if got := listedTypes(group); !slices.Equal(got, []string{"share"}) {
			t.Fatalf("listed types after a refused commit: got %v, want [share]", got)
		}
	})

	t.Run("JoinGroup to a share group", func(t *testing.T) {
		const group = "share-then-join"
		if err := alterShare(group); err != nil {
			t.Fatalf("creating the share group: %v", err)
		}
		resp, err := classicJoinReq(group, "").RequestWith(ctx, cl)
		if err != nil || resp.ErrorCode != kerr.InconsistentGroupProtocol.Code {
			t.Fatalf("joining a share group: %v %v, want INCONSISTENT_GROUP_PROTOCOL", err, resp)
		}
		if got := listedTypes(group); !slices.Equal(got, []string{"share"}) {
			t.Fatalf("listed types after a refused join: got %v, want [share]", got)
		}
	})

	t.Run("AlterShareGroupOffsets to a consumer group", func(t *testing.T) {
		const group = "commit-then-alter"
		if err := commit(group); err != nil {
			t.Fatalf("creating the classic group: %v", err)
		}
		if err := alterShare(group); !errors.Is(err, kerr.GroupIDNotFound) {
			t.Fatalf("altering share offsets of a classic group: got %v, want GROUP_ID_NOT_FOUND", err)
		}
		if got := listedTypes(group); !slices.Equal(got, []string{"classic"}) {
			t.Fatalf("listed types after a refused alter: got %v, want [classic]", got)
		}
	})

	t.Run("ShareFetch to a consumer group", func(t *testing.T) {
		const group = "commit-then-fetch"
		if err := commit(group); err != nil {
			t.Fatalf("creating the classic group: %v", err)
		}
		resp, _ := rawShareFetch(t, cl, group, "11111111-2222-3333-4444-555555555555", topicID, 0)
		if err := kerr.ErrorForCode(resp.ErrorCode); !errors.Is(err, kerr.GroupIDNotFound) {
			t.Fatalf("share fetching from a classic group: got %v, want GROUP_ID_NOT_FOUND", err)
		}
		if got := listedTypes(group); !slices.Equal(got, []string{"classic"}) {
			t.Fatalf("listed types after a refused share fetch: got %v, want [classic]", got)
		}
	})
}
