package kfake

import (
	"context"
	"errors"
	"fmt"
	"maps"
	"math/rand"
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

// Test848RegexSubscription verifies that server-side regex subscription
// matches the correct topics and excludes non-matching topics.
func Test848RegexSubscription(t *testing.T) {
	t.Parallel()
	matchA := "t848-rx-aaa"
	matchB := "t848-rx-bbb"
	noMatch := "t848-other"
	group := "g848-regex"
	nRecords := 10

	c := newCluster(t, NumBrokers(1),
		SeedTopics(2, matchA),
		SeedTopics(2, matchB),
		SeedTopics(2, noMatch),
	)
	producer := newClient848(t, c)

	// Produce to all three topics.
	for i := range nRecords {
		for _, topic := range []string{matchA, matchB, noMatch} {
			r := kgo.StringRecord("v-" + strconv.Itoa(i))
			r.Topic = topic
			produceSync(t, producer, r)
		}
	}

	// Consumer with regex - should only match t848-rx-*.
	consumer := newClient848(t, c,
		kgo.ConsumeRegex(),
		kgo.ConsumeTopics("t848-rx-.*"),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.MetadataMinAge(50*time.Millisecond),
		kgo.MetadataMaxAge(100*time.Millisecond),
	)

	// Consume exactly the expected matching records.
	records := consumeN(t, consumer, nRecords*2, 10*time.Second)
	topics := make(map[string]int)
	for _, r := range records {
		topics[r.Topic]++
	}
	if topics[matchA] != nRecords {
		t.Fatalf("expected %d records from %s, got %d", nRecords, matchA, topics[matchA])
	}
	if topics[matchB] != nRecords {
		t.Fatalf("expected %d records from %s, got %d", nRecords, matchB, topics[matchB])
	}

	// Exclude path: poll briefly and verify no records from the
	// non-matching topic arrive.
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	for {
		fs := consumer.PollRecords(ctx, 100)
		if ctx.Err() != nil {
			break
		}
		fs.EachRecord(func(r *kgo.Record) {
			if r.Topic == noMatch {
				t.Fatalf("received record from non-matching topic %s", noMatch)
			}
		})
	}
}

// Test848GroupTypeIsConsumer verifies that a KIP-848 consumer group is
// reported with type "consumer" in ListGroups, confirming the client is
// actually using the 848 protocol and not falling back to classic.
func Test848GroupTypeIsConsumer(t *testing.T) {
	t.Parallel()
	topic := "t848-type"
	group := "g848-type"

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	producer := newClient848(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, 10)

	consumer := newClient848(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	_ = consumeN(t, consumer, 10, 10*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	adm := kadm.NewClient(newClient848(t, c))

	// ListGroupsByType with "consumer" filter should include our group.
	listed, err := adm.ListGroupsByType(ctx, []string{"consumer"})
	if err != nil {
		t.Fatalf("list groups by type failed: %v", err)
	}
	if _, ok := listed[group]; !ok {
		t.Fatalf("group %s not found in consumer-type list (got %v)", group, listed.Groups())
	}

	// ListGroupsByType with "classic" filter should NOT include our group.
	classicListed, err := adm.ListGroupsByType(ctx, []string{"classic"})
	if err != nil {
		t.Fatalf("list classic groups failed: %v", err)
	}
	if _, ok := classicListed[group]; ok {
		t.Fatalf("group %s should not appear in classic-type list", group)
	}
}

// Test848DeleteGroup verifies that deleting a non-empty consumer group
// returns NonEmptyGroup, and deleting an empty group succeeds.
func Test848DeleteGroup(t *testing.T) {
	t.Parallel()
	topic := "t848-delete"
	group := "g848-delete"

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	producer := newClient848(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, 10)

	consumer := newClient848(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	_ = consumeN(t, consumer, 10, 10*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	adm := kadm.NewClient(newClient848(t, c))

	// Deleting a non-empty group should fail.
	_, err := adm.DeleteGroup(ctx, group)
	if err == nil {
		t.Fatal("expected error deleting non-empty group")
	}
	if !errors.Is(err, kerr.NonEmptyGroup) {
		t.Fatalf("expected NonEmptyGroup error, got %v", err)
	}

	// Close consumer (sends leave heartbeat), making the group empty.
	consumer.Close()

	// Deleting the now-empty group should succeed.
	_, err = adm.DeleteGroup(ctx, group)
	if err != nil {
		t.Fatalf("expected successful delete of empty group, got %v", err)
	}

	// Verify group is gone.
	listed, err := adm.ListGroupsByType(ctx, []string{"consumer"})
	if err != nil {
		t.Fatalf("list groups failed: %v", err)
	}
	if _, ok := listed[group]; ok {
		t.Fatalf("group %s should not exist after deletion", group)
	}
}

// Test848ResumeAfterRestart verifies that a new consumer joining the same
// group resumes from committed offsets.
func Test848ResumeAfterRestart(t *testing.T) {
	t.Parallel()
	topic := "t848-resume"
	group := "g848-resume"
	nRecords := 20

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	producer := newClient848(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, nRecords)

	// First consumer: consume all and commit.
	c1 := newClient848(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	records := consumeN(t, c1, nRecords, 10*time.Second)
	if len(records) != nRecords {
		t.Fatalf("expected %d records, got %d", nRecords, len(records))
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := c1.CommitUncommittedOffsets(ctx); err != nil {
		t.Fatalf("commit failed: %v", err)
	}
	c1.Close()

	// Produce more records.
	moreRecords := 10
	produceNStrings(t, producer, topic, moreRecords)

	// Second consumer: should resume from committed offset and only
	// get the new records.
	c2 := newClient848(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	records = consumeN(t, c2, moreRecords, 10*time.Second)
	if len(records) != moreRecords {
		t.Fatalf("expected %d new records, got %d", moreRecords, len(records))
	}
	if records[0].Offset != int64(nRecords) {
		t.Fatalf("expected first offset %d (resuming), got %d", nRecords, records[0].Offset)
	}
}

// Test848MultipleTopics verifies assignment across multiple topics with
// different partition counts.
func Test848MultipleTopics(t *testing.T) {
	t.Parallel()
	topicA := "t848-multi-a"
	topicB := "t848-multi-b"
	group := "g848-multi"
	nRecords := 10

	c := newCluster(t, NumBrokers(1),
		SeedTopics(3, topicA),
		SeedTopics(6, topicB),
	)
	producer := newClient848(t, c)

	// Produce to both topics.
	for i := range nRecords {
		r := kgo.StringRecord("a-" + strconv.Itoa(i))
		r.Topic = topicA
		produceSync(t, producer, r)
		r2 := kgo.StringRecord("b-" + strconv.Itoa(i))
		r2.Topic = topicB
		produceSync(t, producer, r2)
	}

	consumer := newClient848(t, c,
		kgo.ConsumeTopics(topicA, topicB),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)

	records := consumeN(t, consumer, nRecords*2, 10*time.Second)
	topics := make(map[string]int)
	for _, r := range records {
		topics[r.Topic]++
	}
	if topics[topicA] != nRecords {
		t.Fatalf("expected %d records from %s, got %d", nRecords, topicA, topics[topicA])
	}
	if topics[topicB] != nRecords {
		t.Fatalf("expected %d records from %s, got %d", nRecords, topicB, topics[topicB])
	}
}

// Test848TransactionalConsume verifies that transactional records are correctly
// consumed through a KIP-848 consumer group with read_committed isolation.
func Test848TransactionalConsume(t *testing.T) {
	t.Parallel()
	topic := "t848-txn"
	group := "g848-txn"

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))

	// Produce 10 records in a committed transaction.
	txnClient := newClient848(t, c,
		kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID("t848-txn-id"),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := txnClient.BeginTransaction(); err != nil {
		t.Fatalf("begin txn: %v", err)
	}
	for i := range 10 {
		r := kgo.StringRecord("txn-" + strconv.Itoa(i))
		r.Topic = topic
		produceSync(t, txnClient, r)
	}
	if err := txnClient.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatalf("end txn: %v", err)
	}

	// Produce 5 records in an aborted transaction.
	if err := txnClient.BeginTransaction(); err != nil {
		t.Fatalf("begin txn 2: %v", err)
	}
	for i := range 5 {
		r := kgo.StringRecord("aborted-" + strconv.Itoa(i))
		r.Topic = topic
		produceSync(t, txnClient, r)
	}
	if err := txnClient.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatalf("abort txn: %v", err)
	}

	// Produce 5 more committed records.
	if err := txnClient.BeginTransaction(); err != nil {
		t.Fatalf("begin txn 3: %v", err)
	}
	for i := range 5 {
		r := kgo.StringRecord("committed2-" + strconv.Itoa(i))
		r.Topic = topic
		produceSync(t, txnClient, r)
	}
	if err := txnClient.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatalf("end txn 3: %v", err)
	}

	// Consumer with read_committed should see 15 records (10+5), not the
	// 5 aborted.
	consumer := newClient848(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)

	records := consumeN(t, consumer, 15, 10*time.Second)
	if len(records) != 15 {
		t.Fatalf("expected 15 committed records, got %d", len(records))
	}
	for _, r := range records {
		if len(r.Value) == 0 {
			continue
		}
		v := string(r.Value)
		if len(v) >= 7 && v[:7] == "aborted" {
			t.Fatalf("read_committed consumer saw aborted record: %s", v)
		}
	}
}

// Test848AddTopicSubscription verifies that adding a new topic to a
// consumer's subscription triggers a rebalance and the consumer picks
// up partitions from the newly added topic.
func Test848AddTopicSubscription(t *testing.T) {
	t.Parallel()
	topicA := "t848-addsub-a"
	topicB := "t848-addsub-b"
	group := "g848-addsub"
	nRecords := 10

	c := newCluster(t, NumBrokers(1),
		SeedTopics(1, topicA),
		SeedTopics(1, topicB),
	)
	producer := newClient848(t, c)

	// Produce to both topics.
	for i := range nRecords {
		a := kgo.StringRecord("a-" + strconv.Itoa(i))
		a.Topic = topicA
		b := kgo.StringRecord("b-" + strconv.Itoa(i))
		b.Topic = topicB
		produceSync(t, producer, a, b)
	}

	// Consumer starts subscribed to topicA only. Use fast metadata
	// refresh so AddConsumeTopics picks up the new topic quickly.
	consumer := newClient848(t, c,
		kgo.ConsumeTopics(topicA),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.MetadataMinAge(50*time.Millisecond),
		kgo.MetadataMaxAge(100*time.Millisecond),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	records := consumeN(t, consumer, nRecords, 10*time.Second)
	for _, r := range records {
		if r.Topic != topicA {
			t.Fatalf("expected records from %s, got %s", topicA, r.Topic)
		}
	}

	// Add topicB to the subscription (consumer now subscribes to both).
	consumer.AddConsumeTopics(topicB)

	// Should receive records from topicB.
	records = consumeN(t, consumer, nRecords, 15*time.Second)
	topicBCount := 0
	for _, r := range records {
		if r.Topic == topicB {
			topicBCount++
		}
	}
	if topicBCount != nRecords {
		t.Fatalf("expected %d records from %s after adding subscription, got %d", nRecords, topicB, topicBCount)
	}
}

// Test848RangeAssignorContiguousBlocks verifies that when using the range
// balancer, each consumer gets a contiguous block of partitions per topic.
// Uses DescribeConsumerGroups to check the assignment directly rather than
// inferring it from consumed records.
func Test848RangeAssignorContiguousBlocks(t *testing.T) {
	t.Parallel()
	topic := "t848-range"
	group := "g848-range"
	nPartitions := 6

	c := newCluster(t, NumBrokers(1), SeedTopics(int32(nPartitions), topic))
	producer := newClient848(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, 60)

	// Two consumers using the range balancer.
	c1 := newGroupConsumer(t, c, topic, group, kgo.Balancers(kgo.RangeBalancer()))
	c2 := newGroupConsumer(t, c, topic, group, kgo.Balancers(kgo.RangeBalancer()))

	// Wait for the group to stabilize with 2 members and verify
	// the assignment via DescribeConsumerGroups.
	dg := waitStable(t, c, group, 2)

	// Verify each member's assignment is contiguous.
	for _, m := range dg.Members {
		for topicName, ps := range m.Assignment {
			slices.Sort(ps)
			for i := 1; i < len(ps); i++ {
				if ps[i] != ps[i-1]+1 {
					t.Errorf("member %s has non-contiguous partitions for %s: %v", m.MemberID, topicName, ps)
				}
			}
		}
	}

	// Also confirm both consumers actually receive records.
	poll1FromEachClient(t, 10*time.Second, c1, c2)
}

// Test848UnsupportedAssignor verifies that an unknown server assignor
// name is rejected with UnsupportedAssignor.
func Test848UnsupportedAssignor(t *testing.T) {
	t.Parallel()
	group := "g848-bad-assignor"

	c := newCluster(t, NumBrokers(1), SeedTopics(1, "t"))

	// Send a raw ConsumerGroupHeartbeat with an unknown assignor.
	cl := newClient848(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := kmsg.NewConsumerGroupHeartbeatRequest()
	req.Group = group
	req.MemberEpoch = 0
	req.RebalanceTimeoutMillis = 5000
	bad := "nonexistent"
	req.ServerAssignor = &bad
	req.SubscribedTopicNames = []string{"t"}
	// Joins must carry an empty (non-null) owned-partitions list; null is
	// rejected with INVALID_REQUEST before assignor validation runs.
	req.Topics = []kmsg.ConsumerGroupHeartbeatRequestTopic{}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	if err := kerr.ErrorForCode(resp.ErrorCode); !errors.Is(err, kerr.UnsupportedAssignor) {
		t.Fatalf("expected UnsupportedAssignor, got %v", err)
	}
}

// TestOffsetCommitAfterLeaveClassic verifies that an admin-style OffsetCommit
// (empty memberID, generation -1) is accepted on an empty classic group after
// all members have left.
func TestOffsetCommitAfterLeaveClassic(t *testing.T) {
	t.Parallel()
	topic := "commit-after-leave"
	group := "commit-after-leave-group"

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	producer := newClient848(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, 10)

	// Classic group consumer.
	cl := newPlainClient(t, c,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	consumeN(t, cl, 10, 10*time.Second)

	// Leave the group.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.LeaveGroupContext(ctx); err != nil {
		t.Fatalf("leave failed: %v", err)
	}

	// Admin-style commit: empty memberID, generation -1.
	raw := newClient848(t, c)
	adm := kadm.NewClient(raw)
	offsets := kadm.Offsets{}
	offsets.Add(kadm.Offset{Topic: topic, Partition: 0, At: 10})
	_, err := adm.CommitOffsets(ctx, group, offsets)
	if err != nil {
		t.Fatalf("admin commit failed: %v", err)
	}

	// Verify committed offsets.
	o, ok := groupCommits(c, group)[topic][0]
	if !ok {
		t.Fatal("no committed offset found after commit-after-leave")
	}
	if o.Offset != 10 {
		t.Errorf("expected committed offset 10, got %d", o.Offset)
	}
}

// TestOffsetCommitAfterLeave848 verifies that an admin-style OffsetCommit
// (empty memberID, negative epoch) is accepted on an empty KIP-848 consumer
// group after the member has left.
func TestOffsetCommitAfterLeave848(t *testing.T) {
	t.Parallel()
	for _, tc := range offsetCommitVersions {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			topic := "commit-after-leave-848-" + tc.name
			group := "commit-after-leave-848-group-" + tc.name

			opts := append([]Opt{NumBrokers(1), SeedTopics(1, topic)}, tc.opts...)
			c := newCluster(t, opts...)
			producer := newClient848(t, c, kgo.DefaultProduceTopic(topic))
			produceNStrings(t, producer, topic, 10)

			// 848 consumer.
			consumer := newClient848(t, c,
				kgo.ConsumeTopics(topic),
				kgo.ConsumerGroup(group),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
				kgo.DisableAutoCommit(),
			)
			consumeN(t, consumer, 10, 10*time.Second)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Close the consumer (triggers leave via heartbeat with epoch -1).
			consumer.Close()

			// Admin-style commit: empty memberID, generation -1.
			// Set both Topic and TopicID: v9 uses Topic, v10 uses TopicID.
			ti := c.TopicInfo(topic)
			raw := newClient848(t, c)

			commitReq := kmsg.NewOffsetCommitRequest()
			commitReq.Group = group
			commitReq.Generation = -1
			rt := kmsg.NewOffsetCommitRequestTopic()
			rt.Topic = topic
			rt.TopicID = ti.TopicID
			rp := kmsg.NewOffsetCommitRequestTopicPartition()
			rp.Partition = 0
			rp.Offset = 10
			rt.Partitions = append(rt.Partitions, rp)
			commitReq.Topics = append(commitReq.Topics, rt)

			commitResp, err := commitReq.RequestWith(ctx, raw)
			if err != nil {
				t.Fatalf("commit request failed: %v", err)
			}
			for _, t2 := range commitResp.Topics {
				for _, p := range t2.Partitions {
					if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
						t.Fatalf("commit partition %d error: %v", p.Partition, err)
					}
				}
			}

			o, ok := groupCommits(c, group)[topic][0]
			if !ok {
				t.Fatal("no committed offset found after commit-after-leave")
			}
			if o.Offset != 10 {
				t.Errorf("expected committed offset 10, got %d", o.Offset)
			}

			// Also test kadm commit (auto-resolves TopicIDs from topic names via metadata).
			offsets := kadm.Offsets{}
			offsets.Add(kadm.Offset{Topic: topic, Partition: 0, At: 8, LeaderEpoch: -1})
			rs, err := kadm.NewClient(raw).CommitOffsets(ctx, group, offsets)
			if err != nil {
				t.Fatalf("kadm commit failed: %v", err)
			}
			if err := rs.Error(); err != nil {
				t.Fatalf("kadm commit response error: %v", err)
			}
			o, ok = groupCommits(c, group)[topic][0]
			if !ok {
				t.Fatal("no committed offset after kadm commit")
			}
			if o.Offset != 8 {
				t.Errorf("expected committed offset 8 after kadm commit, got %d", o.Offset)
			}
		})
	}
}

// Test848PartitionHandoffNoDuplicates verifies that when a consumer leaves,
// its partitions are reassigned to the remaining consumer without any
// partition being assigned to two consumers simultaneously.
func Test848PartitionHandoffNoDuplicates(t *testing.T) {
	t.Parallel()
	topic := "t848-handoff"
	group := "g848-handoff"
	nPartitions := 6
	nRecords := 60

	c := newCluster(t, NumBrokers(1), SeedTopics(int32(nPartitions), topic))
	producer := newClient848(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, nRecords)

	c1 := newGroupConsumer(t, c, topic, group)
	c2 := newGroupConsumer(t, c, topic, group)

	// Wait for stable 2-member group.
	dg := waitStable(t, c, group, 2)
	if total := dg.NumAssigned(); total != nPartitions {
		t.Fatalf("expected %d total partitions, got %d", nPartitions, total)
	}

	// Verify no partition overlap.
	seen := make(map[string]string) // "topic/partition" -> memberID
	for _, m := range dg.Members {
		for topicName, ps := range m.Assignment {
			for _, p := range ps {
				key := topicName + "/" + strconv.Itoa(int(p))
				if prev, ok := seen[key]; ok {
					t.Fatalf("partition %s assigned to both %s and %s", key, prev, m.MemberID)
				}
				seen[key] = m.MemberID
			}
		}
	}

	// Close c2; c1 should pick up all partitions.
	c2.Close()
	dg = waitStable(t, c, group, 1)
	if total := dg.NumAssigned(); total != nPartitions {
		t.Fatalf("expected %d partitions after c2 leave, got %d", nPartitions, total)
	}

	// Produce more and verify c1 consumes from all partitions.
	// We poll until records from all 6 partitions are seen rather than
	// consuming a fixed count: consumeN(60) can be satisfied entirely
	// from c1's original 3 partitions (batch-1 leftovers + batch-2
	// records) without ever touching the newly-assigned partitions.
	produceNStrings(t, producer, topic, nRecords)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	partitions := make(map[int32]bool)
	for len(partitions) < nPartitions {
		fs := c1.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("timeout: records from %d/%d partitions: %v", len(partitions), nPartitions, slices.Sorted(maps.Keys(partitions)))
		}
		fs.EachRecord(func(r *kgo.Record) {
			partitions[r.Partition] = true
		})
	}
}

// Test848CooperativeRevocationDuringConsumption verifies that cooperative
// rebalancing works correctly while records are actively being consumed.
// A third consumer joining should not cause data loss or duplication.
func Test848CooperativeRevocationDuringConsumption(t *testing.T) {
	t.Parallel()
	topic := "t848-coop-consume"
	group := "g848-coop-consume"
	nPartitions := 6
	nRecords := 60

	c := newCluster(t, NumBrokers(1), SeedTopics(int32(nPartitions), topic))
	producer := newClient848(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, nRecords)

	nocommit := kgo.DisableAutoCommit()
	c1 := newGroupConsumer(t, c, topic, group, nocommit)
	c2 := newGroupConsumer(t, c, topic, group, nocommit)

	waitStable(t, c, group, 2)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	got := 0
	for got < nRecords {
		for _, cl := range []*kgo.Client{c1, c2} {
			fs := cl.PollRecords(ctx, 100)
			fs.EachRecord(func(*kgo.Record) { got++ })
			if got >= nRecords {
				break
			}
		}
		if got < nRecords && ctx.Err() != nil {
			t.Fatalf("timeout consuming: got %d/%d", got, nRecords)
		}
	}

	if err := c1.CommitUncommittedOffsets(ctx); err != nil {
		t.Fatalf("c1 commit: %v", err)
	}
	if err := c2.CommitUncommittedOffsets(ctx); err != nil {
		t.Fatalf("c2 commit: %v", err)
	}

	produceNStrings(t, producer, topic, nRecords)
	c3 := newGroupConsumer(t, c, topic, group, nocommit)
	waitStable(t, c, group, 3)

	consumeCtx, consumeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer consumeCancel()
	got = 0
	for got < nRecords {
		for _, cl := range []*kgo.Client{c1, c2, c3} {
			// Short per-poll timeout so one slow client can't
			// block polling the others for the entire deadline.
			pollCtx, pollCancel := context.WithTimeout(consumeCtx, 250*time.Millisecond)
			fs := cl.PollRecords(pollCtx, 50)
			pollCancel()
			fs.EachRecord(func(*kgo.Record) { got++ })
			if got >= nRecords {
				break
			}
		}
		if got < nRecords && consumeCtx.Err() != nil {
			t.Fatalf("timeout consuming after rebalance: got %d/%d", got, nRecords)
		}
	}
}

// Test848RebalanceTimeout verifies that the per-member rebalance timeout
// fences a member that does not complete partition revocation in time.
// Member B joins via raw kmsg so we control exactly when it heartbeats.
// After a rebalance is triggered, we send B one heartbeat (gets the
// revocation instruction and schedules the rebalance timeout), then stop.
// The rebalance timeout fires and fences B.
func Test848RebalanceTimeout(t *testing.T) {
	t.Parallel()
	topic := "t848-rebal-timeout"
	group := "g848-rebal-timeout"
	nPartitions := 6

	c := newCluster(t,
		NumBrokers(1),
		SeedTopics(int32(nPartitions), topic),
		BrokerConfigs(map[string]string{
			// Long session timeout so it doesn't interfere.
			"group.consumer.session.timeout.ms": "30000",
		}),
	)

	// Plain client for raw requests and admin.
	raw := newPlainClient(t, c)

	// A joins via kgo (automatic heartbeating).
	a := newGroupConsumer(t, c, topic, group)
	produceNStrings(t, a, topic, 30)
	waitStable(t, c, group, 1)

	// B joins via raw heartbeat with a short rebalance timeout.
	ctx := context.Background()
	join := kmsg.NewConsumerGroupHeartbeatRequest()
	join.Group = group
	join.MemberEpoch = 0
	join.RebalanceTimeoutMillis = 500
	assignor := "uniform"
	join.ServerAssignor = &assignor
	join.SubscribedTopicNames = []string{topic}
	join.Topics = []kmsg.ConsumerGroupHeartbeatRequestTopic{}
	joinResp, err := join.RequestWith(ctx, raw)
	if err != nil {
		t.Fatalf("B join: %v", err)
	}
	if joinResp.ErrorCode != 0 {
		t.Fatalf("B join error: %v", kerr.ErrorForCode(joinResp.ErrorCode))
	}
	bMemberID := *joinResp.MemberID
	bEpoch := joinResp.MemberEpoch

	// Build B's current assignment from the join response.
	bAssignment := make(map[[16]byte][]int32)
	if joinResp.Assignment != nil {
		for _, at := range joinResp.Assignment.Topics {
			bAssignment[at.TopicID] = at.Partitions
		}
	}

	// Heartbeat B with its assigned partitions to confirm and stabilize.
	hb := kmsg.NewConsumerGroupHeartbeatRequest()
	hb.Group = group
	hb.MemberID = bMemberID
	hb.MemberEpoch = bEpoch
	hb.RebalanceTimeoutMillis = 500
	hb.SubscribedTopicNames = []string{topic}
	for id, parts := range bAssignment {
		tp := kmsg.NewConsumerGroupHeartbeatRequestTopic()
		tp.TopicID = id
		tp.Partitions = parts
		hb.Topics = append(hb.Topics, tp)
	}
	hbResp, err := hb.RequestWith(ctx, raw)
	if err != nil {
		t.Fatalf("B heartbeat: %v", err)
	}
	if hbResp.ErrorCode != 0 {
		t.Fatalf("B heartbeat error: %v", kerr.ErrorForCode(hbResp.ErrorCode))
	}
	if hbResp.MemberEpoch > bEpoch {
		bEpoch = hbResp.MemberEpoch
	}

	// B may need additional heartbeats to fully reconcile if the
	// epoch didn't advance on the first confirmation.
	for i := range 20 {
		if g := c.GroupInfo(group); g != nil && g.State == "Stable" && len(g.Members) == 2 {
			break
		}
		if i == 19 {
			t.Fatalf("timeout waiting for stable 2-member group: %+v", c.GroupInfo(group))
		}
		// Re-heartbeat B to nudge reconciliation.
		hb2 := kmsg.NewConsumerGroupHeartbeatRequest()
		hb2.Group = group
		hb2.MemberID = bMemberID
		hb2.MemberEpoch = bEpoch
		hb2.RebalanceTimeoutMillis = 500
		hb2.SubscribedTopicNames = []string{topic}
		for id, parts := range bAssignment {
			tp := kmsg.NewConsumerGroupHeartbeatRequestTopic()
			tp.TopicID = id
			tp.Partitions = parts
			hb2.Topics = append(hb2.Topics, tp)
		}
		resp, err := hb2.RequestWith(ctx, raw)
		if err != nil {
			t.Fatalf("B re-heartbeat: %v", err)
		}
		if resp.ErrorCode != 0 {
			t.Fatalf("B re-heartbeat error: %v", kerr.ErrorForCode(resp.ErrorCode))
		}
		if resp.MemberEpoch > bEpoch {
			bEpoch = resp.MemberEpoch
		}
		// Update bAssignment if the response includes one.
		if resp.Assignment != nil {
			bAssignment = make(map[[16]byte][]int32)
			for _, at := range resp.Assignment.Topics {
				bAssignment[at.TopicID] = at.Partitions
			}
		}
		time.Sleep(100 * time.Millisecond)
	}

	// C joins via kgo - triggers rebalance. B's target assignment
	// shrinks, so B needs to revoke partitions.
	cJoin := newGroupConsumer(t, c, topic, group)
	_ = cJoin

	// Wait for the group to leave Stable (rebalance triggered).
	deadline, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if _, err := c.WaitGroupInfo(deadline, group, func(g *GroupInfo) bool {
		return g != nil && g.State != "Stable"
	}); err != nil {
		t.Fatal("timeout waiting for rebalance to start")
	}

	// Send one full heartbeat for B, reporting its current assignment.
	// The server will send B the reduced reconciled assignment and
	// schedule the rebalance timeout.
	full := kmsg.NewConsumerGroupHeartbeatRequest()
	full.Group = group
	full.MemberID = bMemberID
	full.MemberEpoch = bEpoch
	full.RebalanceTimeoutMillis = 500
	full.SubscribedTopicNames = []string{topic}
	for id, parts := range bAssignment {
		tp := kmsg.NewConsumerGroupHeartbeatRequestTopic()
		tp.TopicID = id
		tp.Partitions = parts
		full.Topics = append(full.Topics, tp)
	}
	fullResp, err := full.RequestWith(ctx, raw)
	if err != nil {
		t.Fatalf("B full heartbeat: %v", err)
	}
	if fullResp.ErrorCode != 0 {
		t.Fatalf("B full heartbeat error: %v", kerr.ErrorForCode(fullResp.ErrorCode))
	}

	// Don't send any more heartbeats for B. The rebalance timeout
	// (500ms) should fire and fence B.
	time.Sleep(800 * time.Millisecond)

	dg := c.GroupInfo(group)
	for _, m := range dg.Members {
		if m.MemberID == bMemberID {
			t.Fatalf("member B (%s) should have been fenced by rebalance timeout, but is still in group (state=%s)", bMemberID, dg.State)
		}
	}
}

// initProducerID sends a raw InitProducerID request with the given
// parameters and returns the response.
func initProducerID(t *testing.T, cl *kgo.Client, txid string, pid int64, epoch int16, timeout int32) *kmsg.InitProducerIDResponse {
	t.Helper()
	req := kmsg.NewInitProducerIDRequest()
	req.TransactionalID = &txid
	req.TransactionTimeoutMillis = timeout
	req.ProducerID = pid
	req.ProducerEpoch = epoch
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("InitProducerID: %v", err)
	}
	return resp
}

// TestTxnInitProducerIDStaleEpochRecovery verifies that a client can
// recover after a transaction timeout bumps the epoch. The client
// sends InitProducerID with its stale epoch and should get a new
// valid epoch back - not INVALID_PRODUCER_EPOCH.
func TestTxnInitProducerIDStaleEpochRecovery(t *testing.T) {
	t.Parallel()

	c := newCluster(t, NumBrokers(1))
	cl := newPlainClient(t, c)

	// Init with a transactional ID.
	resp := initProducerID(t, cl, "txid-stale", -1, -1, 60000)
	if resp.ErrorCode != 0 {
		t.Fatalf("init: %v", kerr.ErrorForCode(resp.ErrorCode))
	}
	pid := resp.ProducerID
	epoch := resp.ProducerEpoch
	if epoch != 0 {
		t.Fatalf("expected epoch 0, got %d", epoch)
	}

	// Simulate a timeout bump: re-init with the same txid (creates
	// existing entry), then bump epoch via another init. This gives
	// us a server epoch that's ahead of the client.
	resp2 := initProducerID(t, cl, "txid-stale", pid, epoch, 60000)
	if resp2.ErrorCode != 0 {
		t.Fatalf("bump: %v", kerr.ErrorForCode(resp2.ErrorCode))
	}
	serverEpoch := resp2.ProducerEpoch
	if serverEpoch <= epoch {
		t.Fatalf("expected bumped epoch > %d, got %d", epoch, serverEpoch)
	}

	// Now try to recover with the STALE epoch (the original one).
	// This is the scenario that used to cause an infinite retry loop.
	resp3 := initProducerID(t, cl, "txid-stale", pid, epoch, 60000)
	if resp3.ErrorCode != 0 {
		t.Fatalf("stale recovery should succeed, got: %v", kerr.ErrorForCode(resp3.ErrorCode))
	}
	if resp3.ProducerEpoch <= serverEpoch {
		t.Fatalf("expected epoch > %d after stale recovery, got %d", serverEpoch, resp3.ProducerEpoch)
	}
}

// TestTxnInitProducerIDAbortOngoing verifies that InitProducerID
// aborts an in-flight transaction before returning a new epoch.
func TestTxnInitProducerIDAbortOngoing(t *testing.T) {
	t.Parallel()
	topic := "t-init-abort"

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c)

	// Start a transactional producer and begin a transaction.
	txnCl := newPlainClient(t, c,
		kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID("txid-abort-ongoing"),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := txnCl.BeginTransaction(); err != nil {
		t.Fatalf("begin: %v", err)
	}
	r := kgo.StringRecord("ongoing")
	r.Topic = topic
	produceSync(t, txnCl, r)

	// Fresh init with the SAME txid (no PID/epoch provided). The
	// server already has an ongoing transaction for this txid, so
	// it must abort the in-flight transaction before returning a
	// new PID.
	resp := initProducerID(t, cl, "txid-abort-ongoing", -1, -1, 60000)
	if resp.ErrorCode != 0 {
		t.Fatalf("reinit error: %v", kerr.ErrorForCode(resp.ErrorCode))
	}

	// The original txnCl's transaction should be aborted. Consume
	// with read_committed and verify no records are visible.
	consumer := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	// Poll briefly - should get nothing since the transaction was aborted.
	pollCtx, pollCancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer pollCancel()
	fs := consumer.PollFetches(pollCtx)
	if fs.NumRecords() > 0 {
		t.Fatalf("expected 0 committed records after abort, got %d", fs.NumRecords())
	}
}

// TestTxnEpochBumpMonotonic verifies that repeated InitProducerID
// calls with the current epoch bump monotonically.
func TestTxnEpochBumpMonotonic(t *testing.T) {
	t.Parallel()

	c := newCluster(t, NumBrokers(1))
	cl := newPlainClient(t, c)

	// Init producer - epoch starts at 0.
	resp := initProducerID(t, cl, "txid-exhaust", -1, -1, 60000)
	if resp.ErrorCode != 0 {
		t.Fatalf("init: %v", kerr.ErrorForCode(resp.ErrorCode))
	}
	pid := resp.ProducerID
	epoch := resp.ProducerEpoch
	if epoch != 0 {
		t.Fatalf("expected epoch 0, got %d", epoch)
	}

	// Bump 10 times and verify monotonic increase.
	for i := range 10 {
		resp = initProducerID(t, cl, "txid-exhaust", pid, epoch, 60000)
		if resp.ErrorCode != 0 {
			t.Fatalf("bump %d at epoch %d: %v", i, epoch, kerr.ErrorForCode(resp.ErrorCode))
		}
		newEpoch := resp.ProducerEpoch
		if newEpoch != epoch+1 {
			t.Fatalf("bump %d: expected epoch %d, got %d", i, epoch+1, newEpoch)
		}
		pid = resp.ProducerID
		epoch = newEpoch
	}
	if epoch != 10 {
		t.Fatalf("expected epoch 10 after 10 bumps, got %d", epoch)
	}
}

// TestProduceDuplicateReturnsOriginalOffset verifies that when a
// duplicate idempotent batch is detected (same PID, epoch, sequence),
// the produce response returns the ORIGINAL base offset, not 0.
// This is critical for kgo's produce callback correctness.
func TestProduceDuplicateReturnsOriginalOffset(t *testing.T) {
	t.Parallel()
	topic := "t-dup-offset"

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	// Pin Produce to v11 (topic names, not IDs) so our raw requests work.
	v := kversion.Stable()
	v.SetMaxKeyVersion(0, 11) // Produce key = 0
	cl := newPlainClient(t, c, kgo.MaxVersions(v))
	ctx := context.Background()

	// Produce padding records to move partition 0 past offset 0.
	producer := newPlainClient(t, c, kgo.DefaultProduceTopic(topic), kgo.MaxVersions(v))
	for range 20 {
		r := kgo.StringRecord("pad")
		r.Topic = topic
		r.Partition = 0
		produceSync(t, producer, r)
	}

	// Get an idempotent (non-txn) producer ID.
	initReq := kmsg.NewInitProducerIDRequest()
	initReq.TransactionTimeoutMillis = 0
	initReq.ProducerID = -1
	initReq.ProducerEpoch = -1
	initResp, err := initReq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	if initResp.ErrorCode != 0 {
		t.Fatalf("init: %v", kerr.ErrorForCode(initResp.ErrorCode))
	}
	pid := initResp.ProducerID
	epoch := initResp.ProducerEpoch

	// First produce: sequence 0, should succeed.
	p1 := produceRawV11(t, cl, topic, rawBatch(0, pid, epoch, 0, kvRecord()))
	if p1.ErrorCode != 0 {
		t.Fatalf("produce 1 error: %v", kerr.ErrorForCode(p1.ErrorCode))
	}
	origOffset := p1.BaseOffset
	if origOffset < 20 {
		t.Fatalf("expected offset >= 20 (after padding), got %d", origOffset)
	}

	// Duplicate produce: same PID, epoch, sequence 0.
	p2 := produceRawV11(t, cl, topic, rawBatch(0, pid, epoch, 0, kvRecord()))
	if p2.ErrorCode != 0 {
		t.Fatalf("dup produce error: %v", kerr.ErrorForCode(p2.ErrorCode))
	}
	if p2.BaseOffset != origOffset {
		t.Fatalf("dup produce should return original offset %d, got %d", origOffset, p2.BaseOffset)
	}
}

// TestTxnConcurrentDescribeAndInit verifies that ListTransactions and
// InitProducerID can run concurrently without data races. Run with
// -race to detect.
func TestTxnConcurrentDescribeAndInit(t *testing.T) {
	t.Parallel()

	c := newCluster(t, NumBrokers(1))
	cl := newPlainClient(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()

	var wg sync.WaitGroup
	for range 5 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				req := kmsg.NewInitProducerIDRequest()
				req.TransactionalID = stringp("txid-race-" + strconv.Itoa(rand.Intn(100)))
				req.TransactionTimeoutMillis = 60000
				req.ProducerID = -1
				req.ProducerEpoch = -1
				req.RequestWith(ctx, cl) //nolint:errcheck // fire-and-forget request
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				req := kmsg.NewListTransactionsRequest()
				req.RequestWith(ctx, cl) //nolint:errcheck // fire-and-forget request
			}
		}()
	}
	wg.Wait()
}

// TestTxnAddOffsetsWithoutGroup verifies that AddOffsetsToTxn
// succeeds even when the consumer group does not yet exist. The
// group is created lazily when TxnOffsetCommit arrives.
func TestTxnAddOffsetsWithoutGroup(t *testing.T) {
	t.Parallel()

	c := newCluster(t, NumBrokers(1))
	cl := newPlainClient(t, c)
	ctx := context.Background()

	resp := initProducerID(t, cl, "txid-no-group", -1, -1, 60000)
	if resp.ErrorCode != 0 {
		t.Fatalf("init: %v", kerr.ErrorForCode(resp.ErrorCode))
	}
	pid := resp.ProducerID
	epoch := resp.ProducerEpoch

	// AddOffsetsToTxn for a group that does not exist yet.
	addReq := kmsg.NewAddOffsetsToTxnRequest()
	addReq.TransactionalID = "txid-no-group"
	addReq.ProducerID = pid
	addReq.ProducerEpoch = epoch
	addReq.Group = "nonexistent-group"
	addResp, err := addReq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("add offsets: %v", err)
	}
	if addResp.ErrorCode != 0 {
		t.Fatalf("AddOffsetsToTxn should succeed for non-existent group, got: %v",
			kerr.ErrorForCode(addResp.ErrorCode))
	}

	// TxnOffsetCommit with a non-existent topic/partition should return
	// UNKNOWN_TOPIC_OR_PARTITION for that partition only.
	ocReq := kmsg.NewTxnOffsetCommitRequest()
	ocReq.TransactionalID = "txid-no-group"
	ocReq.Group = "nonexistent-group"
	ocReq.ProducerID = pid
	ocReq.ProducerEpoch = epoch
	ocReq.Generation = -1
	ocT := kmsg.NewTxnOffsetCommitRequestTopic()
	ocT.Topic = "no-such-topic"
	ocP := kmsg.NewTxnOffsetCommitRequestTopicPartition()
	ocP.Partition = 99
	ocP.Offset = 0
	ocT.Partitions = append(ocT.Partitions, ocP)
	ocReq.Topics = append(ocReq.Topics, ocT)
	ocResp, err := ocReq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("txn offset commit: %v", err)
	}
	if len(ocResp.Topics) != 1 || len(ocResp.Topics[0].Partitions) != 1 {
		t.Fatalf("expected 1 topic/1 partition in response, got %d topics", len(ocResp.Topics))
	}
	if ec := ocResp.Topics[0].Partitions[0].ErrorCode; ec != kerr.UnknownTopicOrPartition.Code {
		t.Fatalf("expected UNKNOWN_TOPIC_OR_PARTITION, got: %v", kerr.ErrorForCode(ec))
	}
}

// TestTxnDescribeTransactions verifies that DescribeTransactions
// returns correct state for both ongoing and empty (completed)
// transactions.
func TestTxnDescribeTransactions(t *testing.T) {
	t.Parallel()
	topic := "t-describe-txn"

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c)
	ctx := context.Background()

	txnID := "txid-describe"
	resp := initProducerID(t, cl, txnID, -1, -1, 60000)
	if resp.ErrorCode != 0 {
		t.Fatalf("init: %v", kerr.ErrorForCode(resp.ErrorCode))
	}
	pid := resp.ProducerID
	epoch := resp.ProducerEpoch

	// Before any transaction: state should be Empty.
	descReq := kmsg.NewDescribeTransactionsRequest()
	descReq.TransactionalIDs = []string{txnID}
	descResp, err := descReq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if len(descResp.TransactionStates) != 1 {
		t.Fatalf("expected 1 state, got %d", len(descResp.TransactionStates))
	}
	st := descResp.TransactionStates[0]
	if st.ErrorCode != 0 {
		t.Fatalf("describe error: %v", kerr.ErrorForCode(st.ErrorCode))
	}
	if st.State != "Empty" {
		t.Fatalf("expected Empty state before transaction, got %q", st.State)
	}
	if st.ProducerID != pid {
		t.Fatalf("expected pid %d, got %d", pid, st.ProducerID)
	}

	// Start a transaction by adding a partition.
	addReq := kmsg.NewAddPartitionsToTxnRequest()
	addReq.TransactionalID = txnID
	addReq.ProducerID = pid
	addReq.ProducerEpoch = epoch
	addT := kmsg.NewAddPartitionsToTxnRequestTopic()
	addT.Topic = topic
	addT.Partitions = []int32{0}
	addReq.Topics = append(addReq.Topics, addT)
	if _, err := addReq.RequestWith(ctx, cl); err != nil {
		t.Fatalf("add partitions: %v", err)
	}

	// During transaction: state should be Ongoing with the partition.
	descResp, err = descReq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("describe during txn: %v", err)
	}
	st = descResp.TransactionStates[0]
	if st.ErrorCode != 0 {
		t.Fatalf("describe error: %v", kerr.ErrorForCode(st.ErrorCode))
	}
	if st.State != "Ongoing" {
		t.Fatalf("expected Ongoing state during transaction, got %q", st.State)
	}
	if len(st.Topics) == 0 {
		t.Fatal("expected at least one topic in ongoing transaction")
	}
	found := false
	for _, dt := range st.Topics {
		if dt.Topic == topic {
			for _, p := range dt.Partitions {
				if p == 0 {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatalf("partition 0 of topic %s not in transaction describe", topic)
	}

	// Commit the transaction.
	endReq := kmsg.NewEndTxnRequest()
	endReq.TransactionalID = txnID
	endReq.ProducerID = pid
	endReq.ProducerEpoch = epoch
	endReq.Commit = true
	endResp, err := endReq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("end txn: %v", err)
	}
	if endResp.ErrorCode != 0 {
		t.Fatalf("end txn error: %v", kerr.ErrorForCode(endResp.ErrorCode))
	}

	// After commit: state should be Empty again, no partitions.
	descResp, err = descReq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("describe after commit: %v", err)
	}
	st = descResp.TransactionStates[0]
	if st.ErrorCode != 0 {
		t.Fatalf("describe error: %v", kerr.ErrorForCode(st.ErrorCode))
	}
	if st.State != "Empty" {
		t.Fatalf("expected Empty state after commit, got %q", st.State)
	}
	if len(st.Topics) != 0 {
		t.Fatalf("expected no topics after commit, got %d", len(st.Topics))
	}
}

// TestProduceSyncUnlinger verifies that ProduceSync does not wait for the full
// linger duration before completing. With a 10s linger, ProduceSync should
// still return quickly because it unlingers partitions after enqueuing records.
func TestProduceSyncUnlinger(t *testing.T) {
	t.Parallel()
	topic := "produce-sync-unlinger"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))

	producer := newPlainClient(t, c,
		kgo.DefaultProduceTopic(topic),
		kgo.ProducerLinger(10*time.Second),
	)

	// Produce one record and flush to load topic metadata and
	// establish connections. Without this, the first ProduceSync
	// would buffer to the unknown-topic path and not benefit from
	// the unlinger optimization.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	producer.Produce(ctx, kgo.StringRecord("warmup"), nil)
	if err := producer.Flush(ctx); err != nil {
		t.Fatalf("warmup flush failed: %v", err)
	}

	start := time.Now()
	results := producer.ProduceSync(ctx, kgo.StringRecord("v1"), kgo.StringRecord("v2"), kgo.StringRecord("v3"))
	elapsed := time.Since(start)

	if err := results.FirstErr(); err != nil {
		t.Fatalf("ProduceSync failed: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// With the unlinger fix, ProduceSync should complete well within 5s
	// despite the 10s linger. Without the fix, it would block for 10s.
	if elapsed > 5*time.Second {
		t.Fatalf("ProduceSync took %v, expected well under 5s with unlinger", elapsed)
	}

	// Verify all records are consumable (warmup + 3 = 4 records total).
	consumer := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	records := consumeN(t, consumer, 4, 5*time.Second)
	if len(records) != 4 {
		t.Fatalf("expected 4 consumed records, got %d", len(records))
	}
}

// TestTxnInitProducerIDMaxTimeout verifies that InitProducerID with a
// timeout exceeding transaction.max.timeout.ms returns
// INVALID_TRANSACTION_TIMEOUT.
func TestTxnInitProducerIDMaxTimeout(t *testing.T) {
	t.Parallel()

	c := newCluster(t, NumBrokers(1), BrokerConfigs(map[string]string{
		"transaction.max.timeout.ms": "5000",
	}))
	cl := newPlainClient(t, c)

	// Timeout within limit - should succeed.
	resp := initProducerID(t, cl, "txid-timeout-ok", -1, -1, 5000)
	if resp.ErrorCode != 0 {
		t.Fatalf("expected success for timeout <= max, got: %v", kerr.ErrorForCode(resp.ErrorCode))
	}

	// Timeout exceeding limit - should fail.
	resp2 := initProducerID(t, cl, "txid-timeout-bad", -1, -1, 5001)
	if resp2.ErrorCode != kerr.InvalidTransactionTimeout.Code {
		t.Fatalf("expected INVALID_TRANSACTION_TIMEOUT for timeout > max, got: %v", kerr.ErrorForCode(resp2.ErrorCode))
	}
}

// TestProduceControlBatchRejected verifies that client-sent control batches
// are rejected with INVALID_RECORD.
func TestProduceControlBatchRejected(t *testing.T) {
	t.Parallel()
	topic := "t-control-batch"

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	v := kversion.Stable()
	v.SetMaxKeyVersion(0, 11)
	cl := newPlainClient(t, c, kgo.MaxVersions(v))

	// Attributes 0x0030 is transactional plus the control bit.
	rec := kmsg.Record{Key: []byte{0, 0, 0, 1}, Value: []byte{}}
	errCode := produceRawV11(t, cl, topic, rawBatch(0x0030, 1, 0, -1, rec)).ErrorCode
	if errCode != kerr.InvalidRecord.Code {
		t.Fatalf("expected INVALID_RECORD for control batch, got: %v", kerr.ErrorForCode(errCode))
	}
}

// TestTxnNonTransactionalProduceDuringTx verifies that a
// non-transactional produce during an active transaction returns
// INVALID_TXN_STATE.
func TestTxnNonTransactionalProduceDuringTx(t *testing.T) {
	t.Parallel()
	topic := "t-non-txn-during-tx"

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	v := kversion.Stable()
	v.SetMaxKeyVersion(0, 11)
	cl := newPlainClient(t, c, kgo.MaxVersions(v))
	ctx := context.Background()

	// Init a transactional producer.
	resp := initProducerID(t, cl, "txid-non-txn-during", -1, -1, 60000)
	if resp.ErrorCode != 0 {
		t.Fatalf("init: %v", kerr.ErrorForCode(resp.ErrorCode))
	}
	pid := resp.ProducerID
	epoch := resp.ProducerEpoch

	// Start a transaction by adding a partition.
	addReq := kmsg.NewAddPartitionsToTxnRequest()
	addReq.TransactionalID = "txid-non-txn-during"
	addReq.ProducerID = pid
	addReq.ProducerEpoch = epoch
	addT := kmsg.NewAddPartitionsToTxnRequestTopic()
	addT.Topic = topic
	addT.Partitions = []int32{0}
	addReq.Topics = append(addReq.Topics, addT)
	if _, err := addReq.RequestWith(ctx, cl); err != nil {
		t.Fatalf("add partitions: %v", err)
	}

	// Produce a NON-transactional batch (attributes 0) using the same
	// producer ID.
	errCode := produceRawV11(t, cl, topic, rawBatch(0, pid, epoch, 0, kvRecord())).ErrorCode
	if errCode != kerr.InvalidTxnState.Code {
		t.Fatalf("expected INVALID_TXN_STATE for non-txn produce during tx, got: %v", kerr.ErrorForCode(errCode))
	}
}

// TestTxnProduceUnknownProducerIDPre360 verifies that below KIP-360
// (InitProducerID v3, Kafka 2.5), a transactional append continuing its
// sequence into a log that has never seen the producer is rejected with
// UNKNOWN_PRODUCER_ID. At 2.5 and above the broker seeds state from any
// first sequence and the same append is accepted.
func TestProduceUnknownProducerIDPre360(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		txn     bool
		initMax int16
		want    int16
	}{
		{"txn-pre-360", true, 2, kerr.UnknownProducerID.Code},
		{"txn-post-360", true, 4, 0},
		{"idempotent-pre-360", false, 2, kerr.UnknownProducerID.Code},
		{"idempotent-post-360", false, 4, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			topic := "t-unknown-pid-" + test.name
			txid := "txid-unknown-pid-" + test.name

			v := kversion.Stable()
			v.SetMaxKeyVersion(0, 11) // Produce v11: partitions are added explicitly
			v.SetMaxKeyVersion(22, test.initMax)
			c := newCluster(t, NumBrokers(1), SeedTopics(1, topic), MaxVersions(v))
			cl := newPlainClient(t, c, kgo.MaxVersions(v))
			ctx := context.Background()

			var initResp *kmsg.InitProducerIDResponse
			var attrs int16
			if test.txn {
				initResp = initProducerID(t, cl, txid, -1, -1, 60000)
				attrs = 0x0010 // transactional
			} else {
				initReq := kmsg.NewPtrInitProducerIDRequest()
				initReq.ProducerID = -1
				initReq.ProducerEpoch = -1
				var err error
				if initResp, err = initReq.RequestWith(ctx, cl); err != nil {
					t.Fatalf("init: %v", err)
				}
			}
			if initResp.ErrorCode != 0 {
				t.Fatalf("init: %v", kerr.ErrorForCode(initResp.ErrorCode))
			}

			if test.txn {
				addReq := kmsg.NewAddPartitionsToTxnRequest()
				addReq.TransactionalID = txid
				addReq.ProducerID = initResp.ProducerID
				addReq.ProducerEpoch = initResp.ProducerEpoch
				addT := kmsg.NewAddPartitionsToTxnRequestTopic()
				addT.Topic = topic
				addT.Partitions = []int32{0}
				addReq.Topics = append(addReq.Topics, addT)
				if _, err := addReq.RequestWith(ctx, cl); err != nil {
					t.Fatalf("add partitions: %v", err)
				}
			}

			// Continue the sequence at 7 into a log that has never
			// seen this producer, as a producer whose topic was
			// deleted and recreated under it does.
			batch := rawBatch(attrs, initResp.ProducerID, initResp.ProducerEpoch, 7, kvRecord())
			if got := produceRawV11(t, cl, topic, batch).ErrorCode; got != test.want {
				t.Fatalf("got %v, want %v", kerr.ErrorForCode(got), kerr.ErrorForCode(test.want))
			}
		})
	}
}

// idempotentProduceRaw sends a one record idempotent produce for the given
// producer ID, epoch, and first sequence, and returns the partition's error
// code.
func idempotentProduceRaw(t *testing.T, c *Cluster, cl *kgo.Client, topic string, pid int64, epoch int16, firstSeq int32) int16 {
	t.Helper()
	req := kmsg.NewPtrProduceRequest()
	req.Acks = -1
	req.TimeoutMillis = 5000
	rt := kmsg.NewProduceRequestTopic()
	rt.Topic = topic
	rt.TopicID = c.TopicInfo(topic).TopicID
	rp := kmsg.NewProduceRequestTopicPartition()
	rp.Partition = 0
	rp.Records = rawBatch(0, pid, epoch, firstSeq, kvRecord())
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	return resp.Topics[0].Partitions[0].ErrorCode
}

// initIdempotentPID returns a fresh idempotent producer ID and epoch.
func initIdempotentPID(t *testing.T, cl *kgo.Client) (int64, int16) {
	t.Helper()
	req := kmsg.NewPtrInitProducerIDRequest()
	req.ProducerID = -1
	req.ProducerEpoch = -1
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("init: %v", err)
	}
	if resp.ErrorCode != 0 {
		t.Fatalf("init: %v", kerr.ErrorForCode(resp.ErrorCode))
	}
	return resp.ProducerID, resp.ProducerEpoch
}

// TestProduceNeverWrittenPartitionFirstSeq verifies KAFKA-15591. A producer
// the broker has no state for sends a nonzero first sequence. On an uncapped
// cluster, a partition whose log never held a record answers
// OUT_OF_ORDER_SEQUENCE_NUMBER. A partition that held records and was then
// emptied by DeleteRecords accepts the append. A cluster capped at 4.2 accepts
// both, since no released Kafka carries the check.
func TestProduceNeverWrittenPartitionFirstSeq(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		capped  bool // cap the cluster at 4.2, which lacks the check
		written bool // write a record and delete it back to the end first
		want    int16
	}{
		{"never-written", false, false, kerr.OutOfOrderSequenceNumber.Code},
		{"never-written-capped-4-2", true, false, 0},
		{"emptied-by-delete-records", false, true, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			topic := "t-never-written-" + test.name

			opts := []Opt{NumBrokers(1), SeedTopics(1, topic)}
			if test.capped {
				opts = append(opts, MaxVersions(kversion.V4_2_0()))
			}
			c := newCluster(t, opts...)
			cl := newPlainClient(t, c)

			if test.written {
				pid, epoch := initIdempotentPID(t, cl)
				if code := idempotentProduceRaw(t, c, cl, topic, pid, epoch, 0); code != 0 {
					t.Fatalf("seeding produce: %v", kerr.ErrorForCode(code))
				}
				// Delete every record: -1 truncates to the
				// high watermark.
				if err := c.DeleteRecords(topic, 0, -1); err != nil {
					t.Fatalf("delete records: %v", err)
				}
			}

			// Send a first sequence of 7 as a producer we have
			// no state for. A client does this when its topic
			// was deleted and recreated under it.
			pid, epoch := initIdempotentPID(t, cl)
			got := idempotentProduceRaw(t, c, cl, topic, pid, epoch, 7)
			if got != test.want {
				t.Fatalf("got %v, want %v", kerr.ErrorForCode(got), kerr.ErrorForCode(test.want))
			}
		})
	}
}

// produceErrLogger records the errors a client logs. A test uses it to see
// the error code the broker answered a produce with.
type produceErrLogger struct {
	mu   sync.Mutex
	errs []error
}

func (*produceErrLogger) Level() kgo.LogLevel { return kgo.LogLevelInfo }

func (l *produceErrLogger) Log(_ kgo.LogLevel, _ string, keyvals ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for i := 0; i+1 < len(keyvals); i += 2 {
		if k, _ := keyvals[i].(string); k != "err" {
			continue
		}
		if err, ok := keyvals[i+1].(error); ok {
			l.errs = append(l.errs, err)
		}
	}
}

func (l *produceErrLogger) saw(target error) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.ContainsFunc(l.errs, func(err error) bool { return errors.Is(err, target) })
}

// TestProduceRecreatedTopicFirstSeq deletes and recreates a topic under an
// idempotent client producing by name. On an uncapped cluster the broker
// answers the client's continued sequence with OUT_OF_ORDER_SEQUENCE_NUMBER.
// The client reloads its producer ID and refreshes metadata before resending;
// the refresh reports the new topic ID, so the client fails the records with
// UNKNOWN_TOPIC_ID rather than resending into the recreated topic, which
// stays empty. A cluster capped at 4.2 accepts the continued sequence: the
// client had no signal, and the three records land.
func TestProduceRecreatedTopicFirstSeq(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name    string
		capped  bool // cap the cluster at 4.2, which lacks the check
		wantOOO bool
		wantErr error
		wantEnd int64
	}{
		{"reject", false, true, kerr.UnknownTopicID, 0},
		{"capped-4-2", true, false, nil, 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			topic := "t-recreate-seq-" + test.name

			opts := []Opt{NumBrokers(1), SeedTopics(1, topic)}
			if test.capped {
				opts = append(opts, MaxVersions(kversion.V4_2_0()))
			}
			c := newCluster(t, opts...)

			// Produce v13 addresses topics by ID (KIP-516). The
			// client keeps the deleted topic's ID, so the broker
			// answers UNKNOWN_TOPIC_ID before it looks at any
			// sequence. Cap the client at v12. It then addresses
			// the recreated topic by name, and its sequence
			// reaches the producer state check.
			v := kversion.Stable()
			v.SetMaxKeyVersion(0, 12)
			logger := new(produceErrLogger)
			cl := newPlainClient(t, c,
				kgo.MaxVersions(v),
				// After the rejection the client waits for a
				// metadata refresh before it resends. The
				// default minimum age holds that refresh for
				// 5 seconds.
				kgo.MetadataMinAge(100*time.Millisecond),
				kgo.WithLogger(logger),
			)

			produceNStrings(t, cl, topic, 3) // sequences 0 through 2
			recreateTopic(t, c, topic)

			// Record the first sequence of every batch the client
			// sends from here on.
			var (
				seqMu sync.Mutex
				seqs  []int32
			)
			c.ControlKey(int16(kmsg.Produce), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
				c.KeepControl()
				req := kreq.(*kmsg.ProduceRequest)
				seqMu.Lock()
				defer seqMu.Unlock()
				for _, rt := range req.Topics {
					for _, rp := range rt.Partitions {
						var b kmsg.RecordBatch
						if err := b.ReadFrom(rp.Records); err == nil {
							seqs = append(seqs, b.FirstSequence)
						}
					}
				}
				return nil, nil, false
			})

			var records []*kgo.Record
			for i := range 3 {
				r := kgo.StringRecord("post-" + strconv.Itoa(i))
				r.Topic = topic
				records = append(records, r)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := cl.ProduceSync(ctx, records...).FirstErr(); !errors.Is(err, test.wantErr) {
				t.Fatalf("produce after the recreation returned %v, want %v", err, test.wantErr)
			}

			if got := logger.saw(kerr.OutOfOrderSequenceNumber); got != test.wantOOO {
				t.Errorf("saw OUT_OF_ORDER_SEQUENCE_NUMBER: got %v, want %v", got, test.wantOOO)
			}

			seqMu.Lock()
			sent := slices.Clone(seqs)
			seqMu.Unlock()
			var sawNonzero, sawRestart bool
			for _, seq := range sent {
				switch {
				case seq != 0:
					sawNonzero = true
				case sawNonzero:
					sawRestart = true
				}
			}
			if !sawNonzero {
				t.Errorf("client never continued its sequence into the recreated topic: sent %v", sent)
			}
			if sawRestart {
				t.Errorf("client restarted sequences into the recreated topic (sent %v)", sent)
			}

			adm := kadm.NewClient(cl)
			ends, err := adm.ListEndOffsets(ctx, topic)
			if err != nil {
				t.Fatalf("list end offsets: %v", err)
			}
			end, ok := ends.Lookup(topic, 0)
			if !ok || end.Err != nil {
				t.Fatalf("no end offset for %s: ok=%v err=%v", topic, ok, end.Err)
			}
			if end.Offset != test.wantEnd {
				t.Fatalf("recreated topic ends at %d, want %d", end.Offset, test.wantEnd)
			}
			if test.wantEnd == 0 {
				return
			}

			consumer := newPlainClient(t, c,
				kgo.ConsumeTopics(topic),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
				kgo.FetchMaxWait(250*time.Millisecond),
			)
			var got []string
			for _, r := range consumeN(t, consumer, 3, 10*time.Second) {
				got = append(got, string(r.Value))
			}
			want := []string{"post-0", "post-1", "post-2"}
			if !slices.Equal(got, want) {
				t.Fatalf("consumed %v, want %v", got, want)
			}
		})
	}
}

// TestClassicIncompatibleProtocolRejected verifies that a member whose
// protocols are not supported by all existing members is rejected with
// INCONSISTENT_GROUP_PROTOCOL.
func TestClassicIncompatibleProtocolRejected(t *testing.T) {
	t.Parallel()
	topic := "t-incompat-proto"
	group := "g-incompat-proto"

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c)
	ctx := context.Background()

	// Member A joins with protocols ["range"].
	joinA := kmsg.NewJoinGroupRequest()
	joinA.Group = group
	joinA.SessionTimeoutMillis = 30000
	joinA.RebalanceTimeoutMillis = 10000
	joinA.ProtocolType = "consumer"
	pRange := kmsg.NewJoinGroupRequestProtocol()
	pRange.Name = "range"
	pRange.Metadata = []byte{0, 0, 0, 0, 0, 0, 0, 0}
	joinA.Protocols = append(joinA.Protocols, pRange)
	respA, err := joinA.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("A join: %v", err)
	}
	// First join with no member ID gets MEMBER_ID_REQUIRED (v4+).
	if respA.ErrorCode != kerr.MemberIDRequired.Code {
		t.Fatalf("A join: expected MEMBER_ID_REQUIRED, got %v", kerr.ErrorForCode(respA.ErrorCode))
	}
	joinA.MemberID = respA.MemberID

	// Complete A's join.
	respA2, err := joinA.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("A rejoin: %v", err)
	}
	if respA2.ErrorCode != 0 {
		t.Fatalf("A rejoin error: %v", kerr.ErrorForCode(respA2.ErrorCode))
	}

	// Sync A so group is stable.
	syncA := kmsg.NewSyncGroupRequest()
	syncA.Group = group
	syncA.MemberID = joinA.MemberID
	syncA.Generation = respA2.Generation
	syncA.ProtocolType = kmsg.StringPtr("consumer")
	syncA.Protocol = kmsg.StringPtr("range")
	sa := kmsg.NewSyncGroupRequestGroupAssignment()
	sa.MemberID = joinA.MemberID
	sa.MemberAssignment = []byte{0, 0, 0, 0, 0, 0, 0, 0}
	syncA.GroupAssignment = append(syncA.GroupAssignment, sa)
	if _, err := syncA.RequestWith(ctx, cl); err != nil {
		t.Fatalf("A sync: %v", err)
	}

	// Member B joins with ONLY ["roundrobin"] - incompatible with A's ["range"].
	joinB := kmsg.NewJoinGroupRequest()
	joinB.Group = group
	joinB.SessionTimeoutMillis = 30000
	joinB.RebalanceTimeoutMillis = 10000
	joinB.ProtocolType = "consumer"
	pRR := kmsg.NewJoinGroupRequestProtocol()
	pRR.Name = "roundrobin"
	pRR.Metadata = []byte{0, 0, 0, 0, 0, 0, 0, 0}
	joinB.Protocols = append(joinB.Protocols, pRR)
	respB, err := joinB.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("B join: %v", err)
	}
	if respB.ErrorCode != kerr.InconsistentGroupProtocol.Code {
		t.Fatalf("expected INCONSISTENT_GROUP_PROTOCOL for incompatible member, got %v", kerr.ErrorForCode(respB.ErrorCode))
	}
}

// TestClassicPendingSyncTimeout verifies that members who receive
// JoinGroup responses but don't send SyncGroup within the rebalance
// timeout are removed and a new rebalance is triggered.
func TestClassicPendingSyncTimeout(t *testing.T) {
	t.Parallel()
	topic := "t-pending-sync"
	group := "g-pending-sync"

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c)
	ctx := context.Background()

	// Member A joins with a short rebalance timeout.
	joinA := kmsg.NewJoinGroupRequest()
	joinA.Group = group
	joinA.SessionTimeoutMillis = 30000
	joinA.RebalanceTimeoutMillis = 500 // short
	joinA.ProtocolType = "consumer"
	p := kmsg.NewJoinGroupRequestProtocol()
	p.Name = "range"
	p.Metadata = []byte{0, 0, 0, 0, 0, 0, 0, 0}
	joinA.Protocols = append(joinA.Protocols, p)
	respA, err := joinA.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("A join: %v", err)
	}
	if respA.ErrorCode != kerr.MemberIDRequired.Code {
		t.Fatalf("A join: expected MEMBER_ID_REQUIRED, got %v", kerr.ErrorForCode(respA.ErrorCode))
	}
	joinA.MemberID = respA.MemberID
	respA2, err := joinA.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("A rejoin: %v", err)
	}
	if respA2.ErrorCode != 0 {
		t.Fatalf("A rejoin error: %v", kerr.ErrorForCode(respA2.ErrorCode))
	}

	// Don't send SyncGroup. Wait for the pending sync timeout to fire.
	time.Sleep(800 * time.Millisecond)

	// The member should have been removed: the group is empty or gone.
	if dg := c.GroupInfo(group); dg != nil && len(dg.Members) > 0 {
		t.Fatalf("expected 0 members after pending sync timeout, got %d (state=%s)", len(dg.Members), dg.State)
	}
}

// TestClassicProtocolVoting verifies that protocol selection uses
// Kafka-style voting: each member votes for their most-preferred
// protocol that is universally supported.
func TestClassicProtocolVoting(t *testing.T) {
	t.Parallel()
	topic := "t-proto-vote"
	group := "g-proto-vote"

	c := newCluster(t, NumBrokers(1), SeedTopics(2, topic))
	ctx := context.Background()

	makeJoinReq := func(protos []string) *kmsg.JoinGroupRequest {
		req := kmsg.NewPtrJoinGroupRequest()
		req.Group = group
		req.SessionTimeoutMillis = 30000
		req.RebalanceTimeoutMillis = 250
		req.ProtocolType = "consumer"
		for _, name := range protos {
			p := kmsg.NewJoinGroupRequestProtocol()
			p.Name = name
			p.Metadata = []byte{0, 0, 0, 0, 0, 0, 0, 0}
			req.Protocols = append(req.Protocols, p)
		}
		return req
	}

	// Both members support "range" and "sticky", but in different preference order.
	// A prefers sticky, B prefers range. Both support both.
	// Result should be: sticky gets 1 vote (A), range gets 1 vote (B).
	// Map iteration is non-deterministic for ties, but at least both are candidates.
	clA := newPlainClient(t, c)
	clB := newPlainClient(t, c)

	// A joins with [sticky, range].
	joinA := makeJoinReq([]string{"sticky", "range"})
	respA, err := joinA.RequestWith(ctx, clA)
	if err != nil {
		t.Fatalf("A join: %v", err)
	}
	if respA.ErrorCode != kerr.MemberIDRequired.Code {
		t.Fatalf("A join: expected MEMBER_ID_REQUIRED, got %v", kerr.ErrorForCode(respA.ErrorCode))
	}
	joinA.MemberID = respA.MemberID

	// B joins with [range, sticky] - range is preferred.
	joinB := makeJoinReq([]string{"range", "sticky"})
	respB, err := joinB.RequestWith(ctx, clB)
	if err != nil {
		t.Fatalf("B join: %v", err)
	}
	if respB.ErrorCode != kerr.MemberIDRequired.Code {
		t.Fatalf("B join: expected MEMBER_ID_REQUIRED, got %v", kerr.ErrorForCode(respB.ErrorCode))
	}
	joinB.MemberID = respB.MemberID

	// Complete both joins concurrently (both must be in join for the
	// rebalance to complete).
	type joinResult struct {
		resp *kmsg.JoinGroupResponse
		err  error
	}
	chA := make(chan joinResult, 1)
	chB := make(chan joinResult, 1)
	go func() {
		r, e := joinA.RequestWith(ctx, clA)
		chA <- joinResult{r, e}
	}()
	go func() {
		r, e := joinB.RequestWith(ctx, clB)
		chB <- joinResult{r, e}
	}()

	rA := <-chA
	rB := <-chB
	if rA.err != nil {
		t.Fatalf("A rejoin: %v", rA.err)
	}
	if rB.err != nil {
		t.Fatalf("B rejoin: %v", rB.err)
	}
	if rA.resp.ErrorCode != 0 {
		t.Fatalf("A rejoin error: %v", kerr.ErrorForCode(rA.resp.ErrorCode))
	}
	if rB.resp.ErrorCode != 0 {
		t.Fatalf("B rejoin error: %v", kerr.ErrorForCode(rB.resp.ErrorCode))
	}

	// The selected protocol should be one of the candidates.
	proto := ""
	if rA.resp.Protocol != nil {
		proto = *rA.resp.Protocol
	}
	if proto != "range" && proto != "sticky" {
		t.Fatalf("expected protocol 'range' or 'sticky', got %q", proto)
	}
}

// TestFetchSessionEviction verifies that when the per-broker session
// cache is full, the oldest session is evicted. The evicted client
// gets FETCH_SESSION_ID_NOT_FOUND on its next incremental fetch and
// must re-establish a session to continue consuming.
func TestFetchSessionEviction(t *testing.T) {
	t.Parallel()
	topic := "t-session-evict"

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic), BrokerConfigs(map[string]string{
		"max.incremental.fetch.session.cache.slots": "3",
	}))
	v := kversion.Stable()
	v.SetMaxKeyVersion(1, 11)
	cl := newPlainClient(t, c, kgo.MaxVersions(v))
	ctx := context.Background()

	// Produce a record so fetches have data.
	r := kgo.StringRecord("evict-test")
	r.Topic = topic
	r.Partition = 0
	produceSync(t, cl, r)

	mkFetch := func(sessionID, epoch int32) *kmsg.FetchRequest {
		return fetchRequest(sessionID, epoch, topic, pp{0, 0})
	}

	// Create session A and do an incremental fetch to confirm it works.
	respA, err := mkFetch(0, 0).RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("create session A: %v", err)
	}
	sidA := respA.SessionID

	resp, err := mkFetch(sidA, 1).RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("incremental A: %v", err)
	}
	if resp.ErrorCode != 0 {
		t.Fatalf("incremental A error: %v", kerr.ErrorForCode(resp.ErrorCode))
	}

	// Fill the remaining 2 slots with sessions B and C.
	for range 2 {
		resp, err := mkFetch(0, 0).RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		if resp.ErrorCode != 0 {
			t.Fatalf("create session: %v", kerr.ErrorForCode(resp.ErrorCode))
		}
	}

	// Create session D - this should evict session A (oldest).
	resp, err = mkFetch(0, 0).RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp.ErrorCode != 0 {
		t.Fatalf("create session D: %v", kerr.ErrorForCode(resp.ErrorCode))
	}

	// Session A's next incremental fetch should get FETCH_SESSION_ID_NOT_FOUND.
	resp, err = mkFetch(sidA, 2).RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp.ErrorCode != kerr.FetchSessionIDNotFound.Code {
		t.Fatalf("expected FETCH_SESSION_ID_NOT_FOUND for evicted session, got: %v", kerr.ErrorForCode(resp.ErrorCode))
	}

	// The evicted client can re-establish a session and continue consuming.
	resp, err = mkFetch(0, 0).RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp.ErrorCode != 0 {
		t.Fatalf("re-establish error: %v", kerr.ErrorForCode(resp.ErrorCode))
	}
	if len(resp.Topics) == 0 || len(resp.Topics[0].Partitions) == 0 {
		t.Fatal("expected data in re-established session response")
	}
	if len(resp.Topics[0].Partitions[0].RecordBatches) == 0 {
		t.Fatal("expected records in re-established session")
	}
}

// fetchRequest builds a kmsg.FetchRequest for the given topic/partitions.
func fetchRequest(sessionID, sessionEpoch int32, topic string, partitions ...struct {
	p      int32
	offset int64
},
) *kmsg.FetchRequest {
	req := kmsg.NewPtrFetchRequest()
	req.Version = 11
	req.MaxWaitMillis = 100
	req.MinBytes = 1
	req.MaxBytes = 1 << 20
	req.SessionID = sessionID
	req.SessionEpoch = sessionEpoch
	if len(partitions) > 0 {
		ft := kmsg.NewFetchRequestTopic()
		ft.Topic = topic
		for _, p := range partitions {
			fp := kmsg.NewFetchRequestTopicPartition()
			fp.Partition = p.p
			fp.FetchOffset = p.offset
			fp.PartitionMaxBytes = 1 << 20
			fp.CurrentLeaderEpoch = -1
			ft.Partitions = append(ft.Partitions, fp)
		}
		req.Topics = append(req.Topics, ft)
	}
	return req
}

type pp struct {
	p      int32
	offset int64
}

func TestIncrementalFetchOmitsUnchanged(t *testing.T) {
	t.Parallel()
	topic := "t-incr-fetch"

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	v := kversion.Stable()
	v.SetMaxKeyVersion(1, 11)
	cl := newPlainClient(t, c, kgo.MaxVersions(v))
	ctx := context.Background()

	// Produce 5 records.
	produceNStrings(t, cl, topic, 5)

	// Step 1: epoch=0 fetch creates session, returns all records.
	req := fetchRequest(0, 0, topic, pp{0, 0})
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if resp.ErrorCode != 0 {
		t.Fatalf("fetch error: %v", kerr.ErrorForCode(resp.ErrorCode))
	}
	sessionID := resp.SessionID
	if sessionID == 0 {
		t.Fatal("expected non-zero session ID")
	}
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("expected 1 topic with 1 partition, got %d topics", len(resp.Topics))
	}
	if len(resp.Topics[0].Partitions[0].RecordBatches) == 0 {
		t.Fatal("expected records in initial fetch")
	}

	// Step 2: epoch=1 incremental, advance offset to 5 (caught up).
	// No new data - should return 0 partitions.
	req = fetchRequest(sessionID, 1, topic, pp{0, 5})
	resp, err = req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if resp.ErrorCode != 0 {
		t.Fatalf("fetch error: %v", kerr.ErrorForCode(resp.ErrorCode))
	}
	totalPartitions := 0
	for _, rt := range resp.Topics {
		totalPartitions += len(rt.Partitions)
	}
	if totalPartitions != 0 {
		t.Fatalf("expected 0 partitions in unchanged incremental, got %d", totalPartitions)
	}

	// Step 3: Produce 5 more records.
	produceNStrings(t, cl, topic, 5)

	// Step 4: epoch=2 incremental (session has offset=5, new records at 5-9).
	// HWM changed and there are records - partition should be included.
	req = fetchRequest(sessionID, 2, topic)
	resp, err = req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if resp.ErrorCode != 0 {
		t.Fatalf("fetch error: %v", kerr.ErrorForCode(resp.ErrorCode))
	}
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("expected 1 topic/1 partition, got %d topics", len(resp.Topics))
	}
	if len(resp.Topics[0].Partitions[0].RecordBatches) == 0 {
		t.Fatal("expected records in incremental after produce")
	}

	// Step 5: epoch=3 incremental, advance offset past all records.
	// Should return 0 partitions (caught up, nothing changed).
	req = fetchRequest(sessionID, 3, topic, pp{0, 10})
	resp, err = req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if resp.ErrorCode != 0 {
		t.Fatalf("fetch error: %v", kerr.ErrorForCode(resp.ErrorCode))
	}
	totalPartitions = 0
	for _, rt := range resp.Topics {
		totalPartitions += len(rt.Partitions)
	}
	if totalPartitions != 0 {
		t.Fatalf("expected 0 partitions when caught up, got %d", totalPartitions)
	}
}

func TestIncrementalFetchIncludesErrors(t *testing.T) {
	t.Parallel()
	topic := "t-incr-err"

	// Only 1 partition exists. We'll fetch p0 (exists) and p99 (does not).
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	v := kversion.Stable()
	v.SetMaxKeyVersion(1, 11)
	cl := newPlainClient(t, c, kgo.MaxVersions(v))
	ctx := context.Background()

	produceNStrings(t, cl, topic, 1)

	// Step 1: epoch=0 creates session with p0 and p99.
	// p0 returns data, p99 returns UnknownTopicOrPartition.
	req := fetchRequest(0, 0, topic, pp{0, 0}, pp{99, 0})
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	sessionID := resp.SessionID

	// Step 2: epoch=1 incremental, advance p0 offset to 1 (caught up).
	// p0 should be filtered (unchanged HWM, no records).
	// p99 should be included (error partitions always included since
	// cached HWM is set to -1 on error).
	req = fetchRequest(sessionID, 1, topic, pp{0, 1})
	resp, err = req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	var sawError bool
	var sawP0 bool
	for _, rt := range resp.Topics {
		for _, rp := range rt.Partitions {
			if rp.Partition == 99 && rp.ErrorCode != 0 {
				sawError = true
			}
			if rp.Partition == 0 {
				sawP0 = true
			}
		}
	}
	if !sawError {
		t.Fatal("expected error partition p99 in incremental response")
	}
	if sawP0 {
		t.Fatal("p0 should have been filtered from incremental response")
	}
}

func TestIncrementalFetchEndToEnd(t *testing.T) {
	t.Parallel()
	topic := "t-incr-e2e"

	c := newCluster(t, NumBrokers(1), SeedTopics(3, topic))
	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)

	// Produce to each partition in sequence.
	for p := range int32(3) {
		r := kgo.StringRecord("val-" + strconv.Itoa(int(p)))
		r.Topic = topic
		r.Partition = p
		produceSync(t, cl, r)
	}

	// Consume all 3 records through incremental sessions.
	records := consumeN(t, cl, 3, 10*time.Second)
	if len(records) != 3 {
		t.Fatalf("expected 3 records, got %d", len(records))
	}

	// Verify all partitions represented.
	seen := make(map[int32]bool)
	for _, r := range records {
		seen[r.Partition] = true
	}
	for p := range int32(3) {
		if !seen[p] {
			t.Fatalf("missing record from partition %d", p)
		}
	}
}

func TestAbortedTxnIndexOverlap(t *testing.T) {
	t.Parallel()
	topic := "t-abort-idx"

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	v := kversion.Stable()
	v.SetMaxKeyVersion(1, 11)
	cl := newPlainClient(t, c, kgo.MaxVersions(v))
	ctx := context.Background()

	// Produce a transactional batch, then abort. This creates records
	// at offsets 0-4 and an abort marker at offset 5.
	txnCl := newPlainClient(t, c,
		kgo.TransactionalID("abort-idx-txn"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.MaxVersions(v),
	)
	if err := txnCl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	for i := range 5 {
		r := kgo.StringRecord("txn-" + strconv.Itoa(i))
		r.Topic = topic
		r.Partition = 0
		produceSync(t, txnCl, r)
	}
	if err := txnCl.AbortBufferedRecords(ctx); err != nil {
		t.Fatal(err)
	}
	if err := txnCl.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatal(err)
	}

	// Produce 5 non-txn records (offsets 6-10).
	for i := range 5 {
		r := kgo.StringRecord("plain-" + strconv.Itoa(i))
		r.Topic = topic
		r.Partition = 0
		produceSync(t, cl, r)
	}

	// Fetch from offset 3 (mid-aborted-transaction) with read_committed.
	// The aborted transaction started at offset 0, abort marker at offset 5.
	// The fetch should include this in AbortedTransactions even though
	// firstOffset (0) is before fetchOffset (3).
	req := kmsg.NewPtrFetchRequest()
	req.Version = 11
	req.MaxWaitMillis = 100
	req.MinBytes = 1
	req.MaxBytes = 1 << 20
	req.SessionEpoch = -1
	req.IsolationLevel = 1 // read_committed
	ft := kmsg.NewFetchRequestTopic()
	ft.Topic = topic
	fp := kmsg.NewFetchRequestTopicPartition()
	fp.Partition = 0
	fp.FetchOffset = 3
	fp.PartitionMaxBytes = 1 << 20
	fp.CurrentLeaderEpoch = -1
	ft.Partitions = append(ft.Partitions, fp)
	req.Topics = append(req.Topics, ft)

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("expected 1 topic/1 partition, got %d topics", len(resp.Topics))
	}
	rp := resp.Topics[0].Partitions[0]
	if rp.ErrorCode != 0 {
		t.Fatalf("partition error: %v", kerr.ErrorForCode(rp.ErrorCode))
	}

	// Verify AbortedTransactions includes the overlapping transaction.
	var found bool
	for _, at := range rp.AbortedTransactions {
		if at.FirstOffset == 0 {
			found = true
		}
	}
	if !found {
		t.Fatalf("expected AbortedTransactions to include txn starting at offset 0, got %v", rp.AbortedTransactions)
	}
}

// TestCompactControlBatch verifies control batch handling: abort markers are
// removed when no data remains for the PID, commit markers are kept when the
// PID has surviving data.
func TestCompactControlBatch(t *testing.T) {
	t.Parallel()
	topic := "compact-ctrl"
	c := newCluster(t, NumBrokers(1))

	cl := newPlainClient(t, c)
	if err := c.CreateTopic(topic, 1, map[string]string{"cleanup.policy": "compact"}); err != nil {
		t.Fatal(err)
	}

	// Aborted txn - its data and control batch should be removed.
	abortCl := newPlainClient(t, c,
		kgo.TransactionalID("compact-tx-abort"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err := abortCl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	produceSync(t, abortCl, &kgo.Record{Topic: topic, Partition: 0, Key: []byte("aborted"), Value: []byte("gone")})
	if err := abortCl.AbortBufferedRecords(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := abortCl.EndTransaction(context.Background(), kgo.TryAbort); err != nil {
		t.Fatal(err)
	}

	// Committed txn - its data and control batch should survive.
	commitCl := newPlainClient(t, c,
		kgo.TransactionalID("compact-tx-commit"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err := commitCl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	produceSync(t, commitCl, &kgo.Record{Topic: topic, Partition: 0, Key: []byte("committed"), Value: []byte("kept")})
	if err := commitCl.EndTransaction(context.Background(), kgo.TryCommit); err != nil {
		t.Fatal(err)
	}

	// Active segment sentinel.
	produceSync(t, cl, &kgo.Record{Topic: topic, Key: []byte("sentinel"), Value: []byte("val")})

	c.Compact()

	// read_committed consumer: verifies commit control batch is intact
	// (without it, committed data would be invisible).
	consumer := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	records := consumeN(t, consumer, 2, 5*time.Second)
	got := make(map[string]string)
	for _, r := range records {
		got[string(r.Key)] = string(r.Value)
	}
	if got["committed"] != "kept" {
		t.Fatalf("committed txn record should survive, got: %v", got)
	}
	if got["sentinel"] != "val" {
		t.Fatalf("sentinel should survive, got: %v", got)
	}
}

// TestCompactBackgroundTicker verifies that the background compaction ticker
// runs automatically for compact topics.
func TestCompactBackgroundTicker(t *testing.T) {
	t.Parallel()
	topic := "compact-ticker"
	backoff := "50"
	c := newCluster(t, NumBrokers(1), BrokerConfigs(map[string]string{"log.cleaner.backoff.ms": backoff}))

	cl := newPlainClient(t, c)
	if err := c.CreateTopic(topic, 1, map[string]string{"cleanup.policy": "compact"}); err != nil {
		t.Fatal(err)
	}

	// Produce duplicate keys plus active segment.
	produceSync(t, cl, &kgo.Record{Topic: topic, Key: []byte("x"), Value: []byte("old")})
	produceSync(t, cl, &kgo.Record{Topic: topic, Key: []byte("x"), Value: []byte("new")})
	produceSync(t, cl, &kgo.Record{Topic: topic, Key: []byte("y"), Value: []byte("only")})

	// Wait for the ticker to fire and compact.
	time.Sleep(200 * time.Millisecond)

	consumer := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	records := consumeN(t, consumer, 2, 5*time.Second)
	got := make(map[string]string)
	for _, r := range records {
		got[string(r.Key)] = string(r.Value)
	}
	if got["x"] != "new" || got["y"] != "only" {
		t.Fatalf("expected compaction to run via ticker, got: %v", got)
	}
}

// TestStaticMemberClassicFencing verifies that a second classic group
// client with the same instanceID fences the first (the first gets
// FENCED_INSTANCE_ID).
func TestStaticMemberClassicFencing(t *testing.T) {
	t.Parallel()
	topic := "static-classic-fence"
	group := "static-classic-fence-group"
	instanceID := "fence-instance-1"

	c := newCluster(t, NumBrokers(1), SeedTopics(2, topic))
	producer := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, 20)

	// First client.
	cl1 := newPlainClient(t, c,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.InstanceID(instanceID),
	)
	consumeN(t, cl1, 20, 10*time.Second)
	waitStable(t, c, group, 1)

	// Second client with the same instanceID - should fence the first.
	newPlainClient(t, c,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.InstanceID(instanceID),
	)

	// The group should stabilize with 1 member (cl2 replaced cl1).
	dg := waitStable(t, c, group, 1)
	found := false
	for _, m := range dg.Members {
		if m.InstanceID != nil && *m.InstanceID == instanceID {
			found = true
		}
	}
	if !found {
		t.Fatalf("instanceID %q not found after fencing", instanceID)
	}
}

// TestStaticMemberClassicLeaveByInstance verifies that a static member
// can be removed from a classic group by sending a LeaveGroup request
// with the instanceID.
func TestStaticMemberClassicLeaveByInstance(t *testing.T) {
	t.Parallel()
	topic := "static-classic-leave-inst"
	group := "static-classic-leave-inst-group"
	instanceID := "leave-instance-1"

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	producer := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, 10)

	cl := newPlainClient(t, c,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.InstanceID(instanceID),
	)
	consumeN(t, cl, 10, 10*time.Second)

	waitStable(t, c, group, 1)

	// Send a raw LeaveGroup with instanceID (no memberID).
	raw := newPlainClient(t, c)
	leaveReq := kmsg.NewPtrLeaveGroupRequest()
	leaveReq.Group = group
	leaveReq.Version = 3
	lm := kmsg.NewLeaveGroupRequestMember()
	lm.InstanceID = &instanceID
	leaveReq.Members = append(leaveReq.Members, lm)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	leaveResp, err := leaveReq.RequestWith(ctx, raw)
	if err != nil {
		t.Fatalf("leave request failed: %v", err)
	}
	if leaveResp.ErrorCode != 0 {
		t.Fatalf("leave top-level error: %v", kerr.ErrorForCode(leaveResp.ErrorCode))
	}
	for _, m := range leaveResp.Members {
		if m.ErrorCode != 0 {
			t.Fatalf("leave member error: %v", kerr.ErrorForCode(m.ErrorCode))
		}
	}
}

// Test848FetchOffsetsStaleEpochRetry verifies that the kgo client retries
// OffsetFetch when the server returns STALE_MEMBER_EPOCH at the group
// level. This can happen when the member epoch changes between the
// heartbeat that assigned partitions and the subsequent OffsetFetch.
func Test848FetchOffsetsStaleEpochRetry(t *testing.T) {
	t.Parallel()
	topic := "t-stale-epoch"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))

	pCl := newPlainClient(t, c)
	for i := 0; i < 10; i++ {
		produceSync(t, pCl, &kgo.Record{Topic: topic, Value: []byte("v")})
	}

	// Intercept the first OffsetFetch and return STALE_MEMBER_EPOCH at
	// the group level. The client should force a heartbeat, update its
	// epoch, and retry successfully.
	var staleOnce sync.Once
	c.ControlKey(int16(kmsg.OffsetFetch), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		var fail bool
		staleOnce.Do(func() { fail = true })
		if !fail {
			return nil, nil, false
		}
		c.KeepControl()
		req := kreq.(*kmsg.OffsetFetchRequest)
		resp := req.ResponseKind().(*kmsg.OffsetFetchResponse)
		if req.Version >= 8 {
			sg := kmsg.NewOffsetFetchResponseGroup()
			sg.ErrorCode = kerr.StaleMemberEpoch.Code
			if len(req.Groups) > 0 {
				sg.Group = req.Groups[0].Group
			}
			resp.Groups = append(resp.Groups, sg)
		} else {
			resp.ErrorCode = kerr.StaleMemberEpoch.Code
		}
		return resp, nil, true
	})

	cl := newClient848(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup("test-stale-epoch"),
		kgo.FetchMaxWait(250*time.Millisecond),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var total int
	for total < 10 {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("timed out after consuming %d/10 records", total)
		}
		total += fetches.NumRecords()
	}
}

// TestFetchOffsetsUnstableRetryCancel verifies that if the kgo client is
// in the 1s wait between OffsetFetch retries (because the broker returned
// UNSTABLE_OFFSET_COMMIT) and the fetchOffsets ctx is canceled by a
// rebalance, the client does NOT surface the retryable error to the user
// as a fake fetch error. fetchOffsets is shared between classic and 848
// group management, so this race affects both; the test exercises it via
// 848 because that is where TestTxnEtl/sticky/848 first surfaced it.
//
// Regression test for: fetchOffsets used to fall through from the retry
// wait into the "non-retryable partition error" injection path whenever
// ctx.Done() fired during the wait, even though the underlying error was
// retryable. The user's next PollFetches then surfaced a spurious
// UNSTABLE_OFFSET_COMMIT.
func TestFetchOffsetsUnstableRetryCancel(t *testing.T) {
	t.Parallel()
	topic := "t-unstable-retry-cancel"
	group := "test-unstable-retry-cancel"
	c := newCluster(t, NumBrokers(1), SeedTopics(4, topic))

	// Produce one record to every partition so cl1 will have records to
	// consume after the rebalance completes (which is how the test exits).
	pCl := newPlainClient(t, c)
	for p := int32(0); p < 4; p++ {
		produceSync(t, pCl, &kgo.Record{Topic: topic, Partition: p, Value: []byte("v")})
	}
	pCl.Close()

	// Intercept the FIRST OffsetFetch and return UNSTABLE_OFFSET_COMMIT
	// for every requested partition. The kgo client treats this as
	// retryable and enters a 1s sleep before retrying.
	var firstOnce sync.Once
	fetchSeen := make(chan struct{}, 1)
	c.ControlKey(int16(kmsg.OffsetFetch), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		var fail bool
		firstOnce.Do(func() { fail = true })
		if !fail {
			return nil, nil, false
		}
		c.KeepControl()
		req := kreq.(*kmsg.OffsetFetchRequest)
		resp := req.ResponseKind().(*kmsg.OffsetFetchResponse)
		if len(req.Groups) == 0 {
			return nil, nil, false
		}
		sg := kmsg.NewOffsetFetchResponseGroup()
		sg.Group = req.Groups[0].Group
		for _, rt := range req.Groups[0].Topics {
			st := kmsg.NewOffsetFetchResponseGroupTopic()
			st.Topic = rt.Topic
			st.TopicID = rt.TopicID
			for _, p := range rt.Partitions {
				sp := kmsg.NewOffsetFetchResponseGroupTopicPartition()
				sp.Partition = p
				sp.ErrorCode = kerr.UnstableOffsetCommit.Code
				st.Partitions = append(st.Partitions, sp)
			}
			sg.Topics = append(sg.Topics, st)
		}
		resp.Groups = append(resp.Groups, sg)
		select {
		case fetchSeen <- struct{}{}:
		default:
		}
		return resp, nil, true
	})

	cl1 := newClient848(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)

	// Wait for the broker to send UNSTABLE_OFFSET_COMMIT. After that cl1
	// is processing the response and about to enter the 1s retry sleep;
	// a short pause makes the rebalance land inside the sleep window.
	select {
	case <-fetchSeen:
	case <-time.After(5 * time.Second):
		t.Fatal("did not see OffsetFetch within 5s")
	}
	time.Sleep(200 * time.Millisecond)

	// Joining a second consumer in the same group forces the server to
	// push a new assignment to cl1. cl1's heartbeat exits and cancels
	// the in-flight fetchOffsets retry ctx. With the bug, fetchOffsets
	// falls through and adds a fake UNSTABLE_OFFSET_COMMIT to cl1's
	// fakeReadyForDraining queue, which the user observes on the next
	// PollFetches. With the fix, no fake error is added.
	newClient848(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)

	// Poll cl1 for a few seconds. With the bug, the fake UNSTABLE
	// error sits in fakeReadyForDraining and surfaces on the first
	// poll. With the fix, nothing of the sort is queued, so the
	// poll either returns records (rebalance reconciled) or times
	// out on its short context. We only care that UNSTABLE is never
	// surfaced; records are nice but not required.
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		fetches := cl1.PollFetches(ctx)
		cancel()
		for _, fe := range fetches.Errors() {
			if errors.Is(fe.Err, kerr.UnstableOffsetCommit) {
				t.Fatalf("regression: PollFetches surfaced UNSTABLE_OFFSET_COMMIT: %v", fe.Err)
			}
		}
	}
}

func TestElectLeaders(t *testing.T) {
	t.Parallel()
	topic := "t-elect-leaders"

	c := newCluster(t, NumBrokers(3), SeedTopics(3, topic))
	cl := newPlainClient(t, c)
	ctx := context.Background()

	// Record original leaders.
	meta, err := kmsg.NewPtrMetadataRequest().RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	var mt *kmsg.MetadataResponseTopic
	for i := range meta.Topics {
		if meta.Topics[i].Topic != nil && *meta.Topics[i].Topic == topic {
			mt = &meta.Topics[i]
			break
		}
	}
	if mt == nil {
		t.Fatal("topic not in metadata")
	}
	origLeaders := make(map[int32]int32, len(mt.Partitions))
	origEpochs := make(map[int32]int32, len(mt.Partitions))
	for _, p := range mt.Partitions {
		origLeaders[p.Partition] = p.Leader
		origEpochs[p.Partition] = p.LeaderEpoch
	}

	// Elect leaders for partitions 0 and 1 only.
	electReq := kmsg.NewPtrElectLeadersRequest()
	et := kmsg.NewElectLeadersRequestTopic()
	et.Topic = topic
	et.Partitions = []int32{0, 1}
	electReq.Topics = append(electReq.Topics, et)
	electResp, err := electReq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	for _, rt := range electResp.Topics {
		for _, rp := range rt.Partitions {
			if rp.ErrorCode != 0 {
				t.Fatalf("partition %d error: %v", rp.Partition, kerr.ErrorForCode(rp.ErrorCode))
			}
		}
	}

	// Verify leaders rotated and epochs bumped for 0 and 1.
	meta2, err := kmsg.NewPtrMetadataRequest().RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	for i := range meta2.Topics {
		if meta2.Topics[i].Topic == nil || *meta2.Topics[i].Topic != topic {
			continue
		}
		for _, p := range meta2.Topics[i].Partitions {
			switch p.Partition {
			case 0, 1:
				if p.Leader == origLeaders[p.Partition] {
					t.Errorf("partition %d: leader did not rotate (still %d)", p.Partition, p.Leader)
				}
				if p.LeaderEpoch <= origEpochs[p.Partition] {
					t.Errorf("partition %d: epoch did not bump (%d <= %d)", p.Partition, p.LeaderEpoch, origEpochs[p.Partition])
				}
			case 2:
				if p.Leader != origLeaders[2] {
					t.Errorf("partition 2: leader should not have changed (was %d, now %d)", origLeaders[2], p.Leader)
				}
			}
		}
	}

	// Elect with unknown partition.
	electReq2 := kmsg.NewPtrElectLeadersRequest()
	et2 := kmsg.NewElectLeadersRequestTopic()
	et2.Topic = topic
	et2.Partitions = []int32{99}
	electReq2.Topics = append(electReq2.Topics, et2)
	electResp2, err := electReq2.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	for _, rt := range electResp2.Topics {
		for _, rp := range rt.Partitions {
			if rp.ErrorCode != kerr.UnknownTopicOrPartition.Code {
				t.Fatalf("expected UnknownTopicOrPartition for p99, got %v", kerr.ErrorForCode(rp.ErrorCode))
			}
		}
	}

	// Elect with nil topics (all partitions).
	electReq3 := kmsg.NewPtrElectLeadersRequest()
	electResp3, err := electReq3.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	var totalElected int
	for _, rt := range electResp3.Topics {
		for _, rp := range rt.Partitions {
			if rp.ErrorCode != 0 {
				t.Fatalf("elect all: partition error: %v", kerr.ErrorForCode(rp.ErrorCode))
			}
			totalElected++
		}
	}
	if totalElected < 3 {
		t.Fatalf("expected at least 3 partitions elected, got %d", totalElected)
	}
}

func TestIncrementalAlterConfigAppendSubtract(t *testing.T) {
	t.Parallel()

	c := newCluster(t, NumBrokers(1), SeedTopics(1, "t-incr-cfg"))
	cl := newPlainClient(t, c)
	ctx := context.Background()

	sp := func(s string) *string { return &s }

	mkReq := func(rtype kmsg.ConfigResourceType, name string, configs ...kmsg.IncrementalAlterConfigsRequestResourceConfig) *kmsg.IncrementalAlterConfigsRequest {
		req := kmsg.NewPtrIncrementalAlterConfigsRequest()
		rr := kmsg.NewIncrementalAlterConfigsRequestResource()
		rr.ResourceType = rtype
		rr.ResourceName = name
		rr.Configs = configs
		req.Resources = append(req.Resources, rr)
		return req
	}

	mkCfg := func(name string, val *string, op kmsg.IncrementalAlterConfigOp) kmsg.IncrementalAlterConfigsRequestResourceConfig {
		rc := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
		rc.Name = name
		rc.Value = val
		rc.Op = op
		return rc
	}

	getConfig := func(rtype kmsg.ConfigResourceType, name, key string) *string {
		t.Helper()
		req := kmsg.NewPtrDescribeConfigsRequest()
		rr := kmsg.NewDescribeConfigsRequestResource()
		rr.ResourceType = rtype
		rr.ResourceName = name
		rr.ConfigNames = []string{key}
		req.Resources = append(req.Resources, rr)
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range resp.Resources {
			for _, c := range r.Configs {
				if c.Name == key {
					return c.Value
				}
			}
		}
		return nil
	}

	// APPEND to broker list config (cleanup.policy on a topic).
	resp, err := mkReq(kmsg.ConfigResourceTypeTopic, "t-incr-cfg",
		mkCfg("cleanup.policy", sp("compact"), kmsg.IncrementalAlterConfigOpSet),
	).RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Resources[0].ErrorCode != 0 {
		t.Fatalf("set cleanup.policy: %v", kerr.ErrorForCode(resp.Resources[0].ErrorCode))
	}

	// APPEND "delete" to it.
	resp, err = mkReq(kmsg.ConfigResourceTypeTopic, "t-incr-cfg",
		mkCfg("cleanup.policy", sp("delete"), kmsg.IncrementalAlterConfigOpAppend),
	).RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Resources[0].ErrorCode != 0 {
		t.Fatalf("append cleanup.policy: %v", kerr.ErrorForCode(resp.Resources[0].ErrorCode))
	}

	val := getConfig(kmsg.ConfigResourceTypeTopic, "t-incr-cfg", "cleanup.policy")
	if val == nil || *val != "compact,delete" {
		got := "<nil>"
		if val != nil {
			got = *val
		}
		t.Fatalf("expected 'compact,delete', got %q", got)
	}

	// SUBTRACT "compact" from it.
	resp, err = mkReq(kmsg.ConfigResourceTypeTopic, "t-incr-cfg",
		mkCfg("cleanup.policy", sp("compact"), kmsg.IncrementalAlterConfigOpSubtract),
	).RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Resources[0].ErrorCode != 0 {
		t.Fatalf("subtract cleanup.policy: %v", kerr.ErrorForCode(resp.Resources[0].ErrorCode))
	}

	val = getConfig(kmsg.ConfigResourceTypeTopic, "t-incr-cfg", "cleanup.policy")
	if val == nil || *val != "delete" {
		got := "<nil>"
		if val != nil {
			got = *val
		}
		t.Fatalf("expected 'delete', got %q", got)
	}

	// APPEND on a non-list config should fail.
	resp, err = mkReq(kmsg.ConfigResourceTypeTopic, "t-incr-cfg",
		mkCfg("retention.ms", sp("1000"), kmsg.IncrementalAlterConfigOpAppend),
	).RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Resources[0].ErrorCode != kerr.InvalidRequest.Code {
		t.Fatalf("expected InvalidRequest for APPEND on non-list config, got %v", kerr.ErrorForCode(resp.Resources[0].ErrorCode))
	}

	// APPEND on broker list config.
	resp, err = mkReq(kmsg.ConfigResourceTypeBroker, "0",
		mkCfg("cleanup.policy", sp("compact"), kmsg.IncrementalAlterConfigOpAppend),
	).RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Resources[0].ErrorCode != 0 {
		t.Fatalf("broker append: %v", kerr.ErrorForCode(resp.Resources[0].ErrorCode))
	}
	val = getConfig(kmsg.ConfigResourceTypeBroker, "0", "cleanup.policy")
	if val == nil || *val != "compact" {
		got := "<nil>"
		if val != nil {
			got = *val
		}
		t.Fatalf("expected broker config 'compact', got %q", got)
	}
}

func TestApiVersionsSupportedFeatures(t *testing.T) {
	t.Parallel()

	c := newCluster(t, NumBrokers(1))
	cl := newPlainClient(t, c)
	ctx := context.Background()

	req := kmsg.NewPtrApiVersionsRequest()
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}

	findFeature := func(features []kmsg.ApiVersionsResponseSupportedFeature, name string) *kmsg.ApiVersionsResponseSupportedFeature {
		for i := range features {
			if features[i].Name == name {
				return &features[i]
			}
		}
		return nil
	}
	findFinalized := func(features []kmsg.ApiVersionsResponseFinalizedFeature, name string) *kmsg.ApiVersionsResponseFinalizedFeature {
		for i := range features {
			if features[i].Name == name {
				return &features[i]
			}
		}
		return nil
	}

	// Default cluster has all keys, should have both features.
	txnFeature := findFeature(resp.SupportedFeatures, "transaction.version")
	if txnFeature == nil {
		t.Fatal("expected transaction.version in SupportedFeatures")
	}
	if txnFeature.MaxVersion != 2 {
		t.Fatalf("expected transaction.version max=2, got %d", txnFeature.MaxVersion)
	}

	groupFeature := findFeature(resp.SupportedFeatures, "group.version")
	if groupFeature == nil {
		t.Fatal("expected group.version in SupportedFeatures")
	}
	if groupFeature.MaxVersion != 1 {
		t.Fatalf("expected group.version max=1, got %d", groupFeature.MaxVersion)
	}

	txnFinalized := findFinalized(resp.FinalizedFeatures, "transaction.version")
	if txnFinalized == nil {
		t.Fatal("expected transaction.version in FinalizedFeatures")
	}
	groupFinalized := findFinalized(resp.FinalizedFeatures, "group.version")
	if groupFinalized == nil {
		t.Fatal("expected group.version in FinalizedFeatures")
	}

	// With Produce capped below v12, transaction.version should be absent.
	v := kversion.Stable()
	v.SetMaxKeyVersion(0, 11) // Produce max v11
	c2 := newCluster(t, NumBrokers(1), MaxVersions(v))
	cl2 := newPlainClient(t, c2)
	resp2, err := req.RequestWith(ctx, cl2)
	if err != nil {
		t.Fatal(err)
	}
	if findFeature(resp2.SupportedFeatures, "transaction.version") != nil {
		t.Fatal("transaction.version should be absent when Produce < v12")
	}
	// group.version should still be present.
	if findFeature(resp2.SupportedFeatures, "group.version") == nil {
		t.Fatal("group.version should still be present")
	}
}

// TestOffsetExpiration verifies that committed offsets for a simple group
// (no protocolType) expire after offsets.retention.minutes and that the
// empty group is auto-deleted (KIP-211).
func TestOffsetExpiration(t *testing.T) {
	t.Parallel()
	topic := "offset-expire"
	group := "offset-expire-group"

	// 50ms retention, 50ms check interval for fast expiration.
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic),
		BrokerConfigs(map[string]string{
			"group.consumer.heartbeat.interval.ms": "100",
			"offset.retention.ms":                  "50",
			"offsets.retention.check.interval.ms":  "50",
		}))

	cl := newPlainClient(t, c)
	adm := kadm.NewClient(cl)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Commit offsets to a simple group (no consumer group join).
	offsets := kadm.Offsets{}
	offsets.Add(kadm.Offset{Topic: topic, Partition: 0, At: 5})
	_, err := adm.CommitOffsets(ctx, group, offsets)
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// Verify offsets exist immediately after commit.
	if _, ok := groupCommits(c, group)[topic][0]; !ok {
		t.Fatal("expected committed offset")
	}

	// Wait for expiration check to fire.
	time.Sleep(300 * time.Millisecond)

	// Group should be auto-deleted (offsets expired + empty group).
	if c.GroupInfo(group) != nil {
		t.Fatal("expected group to be auto-deleted after offset expiration")
	}
}

// TestOffsetExpirationActiveGroup verifies that offsets do NOT expire while
// a consumer group is active (Stable state), but do expire after the group
// becomes empty.
func TestOffsetExpirationActiveGroup(t *testing.T) {
	t.Parallel()
	topic := "offset-expire-active"
	group := "offset-expire-active-group"

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic),
		BrokerConfigs(map[string]string{
			"group.consumer.heartbeat.interval.ms": "100",
			"offset.retention.ms":                  "50",
			"offsets.retention.check.interval.ms":  "50",
		}))

	producer := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, 10)

	// Join a consumer group and consume records.
	consumer := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.DisableAutoCommit(),
	)
	_ = consumeN(t, consumer, 10, 10*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Explicitly commit offsets while the group is active.
	if err := consumer.CommitUncommittedOffsets(ctx); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// Wait for several expiration checks - offsets must NOT expire while active.
	time.Sleep(300 * time.Millisecond)

	if _, ok := groupCommits(c, group)[topic][0]; !ok {
		t.Fatal("offsets should NOT expire while group is active")
	}

	// Leave group - now the group becomes empty.
	consumer.Close()

	// Wait for expiration check after empty.
	time.Sleep(300 * time.Millisecond)

	// Group should be auto-deleted (offsets expired + empty group).
	if c.GroupInfo(group) != nil {
		t.Fatal("expected group to be auto-deleted")
	}
}

// TestOffsetCommitUnknownTopicID verifies that committing with an unknown
// TopicID returns UNKNOWN_TOPIC_ID.
func TestOffsetCommitUnknownTopicID(t *testing.T) {
	t.Parallel()
	topic := "commit-unknown-id"
	group := "commit-unknown-id-group"

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	raw := newClient848(t, c)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Build a v10 OffsetCommit with a fabricated (unknown) TopicID.
	req := kmsg.NewOffsetCommitRequest()
	req.Version = 10
	req.Group = group
	req.Generation = -1
	rt := kmsg.NewOffsetCommitRequestTopic()
	rt.TopicID = [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	rp := kmsg.NewOffsetCommitRequestTopicPartition()
	rp.Partition = 0
	rp.Offset = 5
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)

	resp, err := req.RequestWith(ctx, raw)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}

	// The entire response should have UNKNOWN_TOPIC_ID errors.
	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 topic in response, got %d", len(resp.Topics))
	}
	for _, tp := range resp.Topics {
		for _, pp := range tp.Partitions {
			if pp.ErrorCode != kerr.UnknownTopicID.Code {
				t.Errorf("expected UNKNOWN_TOPIC_ID (%d), got error code %d",
					kerr.UnknownTopicID.Code, pp.ErrorCode)
			}
		}
	}
}

// waitShareGroupEmpty waits until the share group has no members. This is
// needed after cl.Close() because the leave heartbeat may not have been
// processed yet.
func waitShareGroupEmpty(t *testing.T, c *Cluster, group string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := c.WaitGroupInfo(ctx, group, func(g *GroupInfo) bool {
		return g == nil || len(g.Members) == 0
	})
	if err != nil {
		t.Fatalf("share group %s never emptied: %v", group, err)
	}
}

// TestShareGroupForgottenTopics verifies that when a share group reassignment
// removes all partitions from a source, the client still sends a ShareFetch
// with ForgottenTopicsData to that source so the broker can clean up its
// share session state.
func TestShareGroupForgottenTopics(t *testing.T) {
	t.Parallel()
	topic := "share-forgotten"
	group := "share-test-forgotten"

	c := newCluster(t, SeedTopics(1, topic), BrokerConfigs(map[string]string{
		"group.share.heartbeat.interval.ms": "100",
	}))

	admin := newPlainClient(t, c,
		kgo.DefaultProduceTopic(topic),
	)

	c.SetGroupConfigs(group, map[string]string{"share.auto.offset.reset": "earliest"})

	// Produce records.
	const total = 5
	for i := range total {
		admin.Produce(context.Background(), kgo.StringRecord(strconv.Itoa(i)), func(_ *kgo.Record, err error) {
			if err != nil {
				t.Errorf("produce: %v", err)
			}
		})
	}
	if err := admin.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// gotForgotten is closed when the ShareFetch observer sees
	// ForgottenTopicsData in a request.
	gotForgotten := make(chan struct{})
	var forgottenOnce sync.Once
	var shareFetchCount atomic.Int64

	// Observe ShareFetch (key 78) for ForgottenTopicsData.
	c.ControlKey(78, func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		req := kreq.(*kmsg.ShareFetchRequest)
		n := shareFetchCount.Add(1)
		t.Logf("ShareFetch #%d: epoch=%d topics=%d forgotten=%d", n, req.ShareSessionEpoch, len(req.Topics), len(req.ForgottenTopicsData))
		if len(req.ForgottenTopicsData) > 0 {
			forgottenOnce.Do(func() { close(gotForgotten) })
		}
		return nil, nil, false // pass through
	})

	// Share consumer with fast heartbeat.
	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ShareGroup(group),
	)

	// Consume all records.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var got int
	for got < total {
		fetches := cl.PollFetches(ctx)
		got += len(fetches.Records())
		if ctx.Err() != nil {
			break
		}
	}
	if got != total {
		t.Fatalf("expected %d records, got %d", total, got)
	}

	// Intercept the next heartbeat to return an empty assignment,
	// which removes all share cursors. With the fix, the source
	// is still visited on the next poll to send ForgottenTopicsData.
	var heartbeatCount atomic.Int64
	c.ControlKey(76, func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		n := heartbeatCount.Add(1)
		req := kreq.(*kmsg.ShareGroupHeartbeatRequest)
		t.Logf("heartbeat #%d: member=%s epoch=%d", n, req.MemberID, req.MemberEpoch)
		resp := req.ResponseKind().(*kmsg.ShareGroupHeartbeatResponse)
		resp.MemberID = &req.MemberID
		resp.MemberEpoch = req.MemberEpoch
		resp.HeartbeatIntervalMillis = 100
		resp.Assignment = &kmsg.ShareGroupHeartbeatResponseAssignment{}
		return resp, nil, true
	})

	// Poll until we observe ForgottenTopicsData in a ShareFetch.
	deadline := time.After(10 * time.Second)
	var pollCount int
	for {
		select {
		case <-gotForgotten:
			t.Logf("success after %d polls, %d ShareFetch reqs, %d heartbeats",
				pollCount, shareFetchCount.Load(), heartbeatCount.Load())
			return
		case <-deadline:
			t.Fatalf("timed out after %d polls, %d ShareFetch reqs, %d heartbeats",
				pollCount, shareFetchCount.Load(), heartbeatCount.Load())
		default:
		}
		pollCount++
		pollCtx, pollCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		fetches := cl.PollFetches(pollCtx)
		pollCancel()
		var nrecs int
		for _, e := range fetches.Errors() {
			if e.Err != context.DeadlineExceeded {
				t.Logf("poll %d: error: %v", pollCount, e.Err)
			}
		}
		nrecs = len(fetches.Records())
		if nrecs > 0 {
			t.Logf("poll %d: got %d records (unexpected after consume phase)", pollCount, nrecs)
		}
	}
}

// TestShareGroupAckRequeue verifies that when ShareAcknowledge returns a
// retryable per-partition error, the client re-queues the acks and delivers
// them on the next ShareFetch (piggybacked). This tests fix for the gap
// where piggybacked or standalone ack errors were silently dropped.
func TestShareGroupAckRequeue(t *testing.T) {
	t.Parallel()

	topic, group := "share-ack-requeue", "share-test-ack-requeue"
	c := newCluster(t, SeedTopics(1, topic))

	admin := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))

	c.SetGroupConfigs(group, map[string]string{"share.auto.offset.reset": "earliest"})
	produceNStrings(t, admin, topic, 10)

	cl1 := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ShareGroup(group),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Poll all records and mark them as accepted.
	var got1 int
	for got1 < 10 {
		fetches := cl1.PollFetches(ctx)
		for _, r := range fetches.Records() {
			r.Ack(kgo.AckAccept)
			got1++
		}
		if ctx.Err() != nil {
			break
		}
	}
	if got1 != 10 {
		t.Fatalf("consumer 1: expected 10 records, got %d", got1)
	}

	// Intercept the first request carrying ack batches (either
	// standalone ShareAcknowledge or piggybacked on ShareFetch --
	// kgo's source loop may piggyback before FlushAcks fires) and
	// return a retryable error. The client should re-queue the acks.
	var intercepted atomic.Bool

	injectRetryableAckErr := func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		if !intercepted.CompareAndSwap(false, true) {
			return nil, nil, false
		}
		switch req := kreq.(type) {
		case *kmsg.ShareAcknowledgeRequest:
			resp := req.ResponseKind().(*kmsg.ShareAcknowledgeResponse)
			for _, rt := range req.Topics {
				respTopic := kmsg.NewShareAcknowledgeResponseTopic()
				respTopic.TopicID = rt.TopicID
				for _, rp := range rt.Partitions {
					respPart := kmsg.NewShareAcknowledgeResponseTopicPartition()
					respPart.Partition = rp.Partition
					respPart.ErrorCode = kerr.RequestTimedOut.Code
					respTopic.Partitions = append(respTopic.Partitions, respPart)
				}
				resp.Topics = append(resp.Topics, respTopic)
			}
			return resp, nil, true
		case *kmsg.ShareFetchRequest:
			hasAcks := false
			for _, rt := range req.Topics {
				for _, rp := range rt.Partitions {
					if len(rp.AcknowledgementBatches) > 0 {
						hasAcks = true
						break
					}
				}
			}
			if !hasAcks {
				intercepted.Store(false) // not an ack request, retry
				return nil, nil, false
			}
			resp := req.ResponseKind().(*kmsg.ShareFetchResponse)
			for _, rt := range req.Topics {
				respTopic := kmsg.NewShareFetchResponseTopic()
				respTopic.TopicID = rt.TopicID
				for _, rp := range rt.Partitions {
					respPart := kmsg.NewShareFetchResponseTopicPartition()
					respPart.Partition = rp.Partition
					respPart.AcknowledgeErrorCode = kerr.RequestTimedOut.Code
					respTopic.Partitions = append(respTopic.Partitions, respPart)
				}
				resp.Topics = append(resp.Topics, respTopic)
			}
			return resp, nil, true
		}
		intercepted.Store(false)
		return nil, nil, false
	}
	c.ControlKey(int16(kmsg.ShareAcknowledge), injectRetryableAckErr)
	c.ControlKey(int16(kmsg.ShareFetch), injectRetryableAckErr)

	// FlushAcks triggers a standalone ShareAcknowledge or the acks
	// piggyback on the next ShareFetch -- either way the control
	// intercepts and returns a retryable error, causing requeue.
	if err := cl1.FlushAcks(ctx); err != nil {
		t.Fatalf("FlushAcks should return nil for retryable ack errors: %v", err)
	}
	if !intercepted.Load() {
		t.Fatal("no ack request was intercepted")
	}

	// FlushAcks already waited for the re-queued acks to be delivered
	// (the control was one-shot, so the second send succeeded). cl1
	// is fully drained at this point.

	// Consumer 2: should see 0 records since all acks were re-queued and
	// delivered via piggybacked ShareFetch. With FetchMaxWait(50ms), any
	// redelivery would arrive within a few cycles.
	cl2 := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ShareGroup(group),
		kgo.FetchMaxWait(50*time.Millisecond),
	)

	ctx2, cancel2 := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel2()
	var got2 int
	for ctx2.Err() == nil {
		fetches := cl2.PollFetches(ctx2)
		got2 += len(fetches.Records())
	}
	if got2 > 0 {
		t.Errorf("consumer 2: expected 0 records after re-queued acks, got %d", got2)
	}
}

// TestShareGroupFencedLeaderEpochMove verifies that FENCED_LEADER_EPOCH
// with a CurrentLeader hint in a ShareFetch partition response triggers
// the same proactive cursor move as NOT_LEADER_FOR_PARTITION. A producer
// goroutine runs concurrently with the consumer so -race can catch data
// races in the move + ack + heartbeat paths.
func TestShareGroupFencedLeaderEpochMove(t *testing.T) {
	t.Parallel()
	topic := "share-fenced-epoch"
	c := newCluster(t,
		NumBrokers(2),
		SeedTopics(1, topic),
	)

	origLeader := c.LeaderFor(topic, 0)
	newLeader := int32(1)
	if origLeader == 1 {
		newLeader = 0
	}

	// Producer.
	pcl := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))

	// Share consumer.
	scl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ShareGroup("share-fenced-epoch-test"),
	)

	// Empty poll to establish the group (SPSO defaults to "latest").
	emptyCtx, emptyCancel := context.WithTimeout(context.Background(), time.Second)
	scl.PollFetches(emptyCtx)
	emptyCancel()

	// Produce initial records and consume them to establish a session.
	for i := range 5 {
		r := &kgo.Record{Value: []byte(strconv.Itoa(i))}
		if err := pcl.ProduceSync(context.Background(), r).FirstErr(); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
	ctx1, cancel1 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel1()
	for ctx1.Err() == nil {
		fetches := scl.PollFetches(ctx1)
		recs := fetches.Records()
		if len(recs) == 0 {
			continue
		}
		for _, r := range recs {
			r.Ack(kgo.AckAccept)
		}
		if err := scl.FlushAcks(ctx1); err != nil {
			t.Fatalf("commit acks: %v", err)
		}
		break
	}
	if ctx1.Err() != nil {
		t.Fatal("timed out consuming initial records")
	}

	// Move the partition to the other broker so the original broker
	// will return partition errors on the next ShareFetch.
	if err := c.MoveTopicPartition(topic, 0, newLeader); err != nil {
		t.Fatalf("move: %v", err)
	}

	// Intercept the first ShareFetch to the OLD leader after the move
	// and replace its partition error with FENCED_LEADER_EPOCH +
	// CurrentLeader. Without the fix, the client wouldn't recognize
	// this error as a leader hint and would surface it instead of
	// moving.
	ti := c.TopicInfo(topic)
	var injected atomic.Bool
	c.ControlKey(78, func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		req := kreq.(*kmsg.ShareFetchRequest)
		for _, rt := range req.Topics {
			if rt.TopicID != ti.TopicID {
				continue
			}
			for _, rp := range rt.Partitions {
				if rp.Partition != 0 {
					continue
				}
				if !injected.CompareAndSwap(false, true) {
					return nil, nil, false
				}
				resp := req.ResponseKind().(*kmsg.ShareFetchResponse)
				st := kmsg.NewShareFetchResponseTopic()
				st.TopicID = ti.TopicID
				sp := kmsg.NewShareFetchResponseTopicPartition()
				sp.Partition = 0
				sp.ErrorCode = kerr.FencedLeaderEpoch.Code
				sp.CurrentLeader.LeaderID = newLeader
				sp.CurrentLeader.LeaderEpoch = 1
				st.Partitions = append(st.Partitions, sp)
				resp.Topics = append(resp.Topics, st)
				return resp, nil, true
			}
		}
		return nil, nil, false
	})

	// Start a concurrent producer that feeds records while the
	// consumer is dealing with the fenced leader epoch and cursor
	// move. This exercises the move + heartbeat + ack paths under
	// -race (internal goroutines: per-source fetch workers, heartbeat
	// loop, metadata updater).
	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	var produceDone sync.WaitGroup
	produceDone.Add(1)
	go func() {
		defer produceDone.Done()
		for i := range 20 {
			r := &kgo.Record{Value: []byte(strconv.Itoa(i + 100))}
			if err := pcl.ProduceSync(ctx2, r).FirstErr(); err != nil {
				return
			}
		}
	}()

	// Single consumer goroutine: poll, ack, commit in a loop until
	// we've consumed enough records from the new leader.
	var got int
	for got < 20 && ctx2.Err() == nil {
		fetches := scl.PollFetches(ctx2)
		recs := fetches.Records()
		if len(recs) == 0 {
			continue
		}
		for _, r := range recs {
			r.Ack(kgo.AckAccept)
		}
		got += len(recs)
		_ = scl.FlushAcks(ctx2)
	}
	produceDone.Wait()

	if got < 20 {
		t.Fatalf("expected at least 20 records after fenced leader epoch move, got %d", got)
	}
	if !injected.Load() {
		t.Fatal("FENCED_LEADER_EPOCH was never injected")
	}
}

// TestShareGroupFetchCascade verifies that when a full-fetch slot fails
// (broker returns a transport error), the slot cascades to the next
// source rather than returning empty to the user. Subtests exercise
// different MaxConcurrentFetches values against 3 brokers.
func TestShareGroupFetchCascade(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name          string
		maxConcurrent int
		// failPartitions are partitions whose brokers always fail.
		// With 5 partitions on 5 brokers (1:1), failing a
		// partition fails exactly that broker.
		failPartitions []int32
	}{
		// 1 slot, 1 failing broker.
		{"one_slot", 1, []int32{0}},

		// 2 slots, 2 failing brokers. By pigeonhole (2 slots,
		// 3 healthy sources out of 5), at least one slot starts
		// on a failing broker and must cascade.
		{"two_slots", 2, []int32{0, 1}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			testShareGroupFetchCascade(t, tt.maxConcurrent, tt.failPartitions)
		})
	}
}

func testShareGroupFetchCascade(t *testing.T, maxConcurrent int, failPartitions []int32) {
	topic := "share-cascade-" + strconv.Itoa(maxConcurrent)
	group := "share-test-cascade-" + strconv.Itoa(maxConcurrent)

	// 5 partitions on 5 brokers: 1 partition per broker, so each
	// broker is a source. Failing a partition fails exactly one broker.
	c := newCluster(t, NumBrokers(5), SeedTopics(5, topic), BrokerConfigs(map[string]string{
		"group.share.heartbeat.interval.ms": "100",
	}))

	admin := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))

	c.SetGroupConfigs(group, map[string]string{"share.auto.offset.reset": "earliest"})
	produceNStrings(t, admin, topic, 60)

	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ShareGroup(group),
		kgo.MaxConcurrentFetches(maxConcurrent),
		kgo.RetryBackoffFn(func(int) time.Duration { return 50 * time.Millisecond }),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Consume a few records first to ensure all partitions are
	// assigned and sessions established on all brokers.
	var warmup int
	for warmup < 3 && ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		for _, r := range fetches.Records() {
			r.Ack(kgo.AckAccept)
			warmup++
		}
	}
	if warmup < 3 {
		t.Fatalf("warmup: expected at least 3, got %d", warmup)
	}
	if err := cl.FlushAcks(ctx); err != nil {
		t.Fatalf("warmup commit: %v", err)
	}

	// Now inject failures. Sessions are already established so
	// the cascade can find healthy brokers.
	failSet := make(map[int32]bool)
	for _, p := range failPartitions {
		failSet[p] = true
	}

	var intercepted atomic.Int64
	c.ControlKey(78, func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		req := kreq.(*kmsg.ShareFetchRequest)

		hasFailing := false
		for _, rt := range req.Topics {
			for _, p := range rt.Partitions {
				if failSet[p.Partition] {
					hasFailing = true
				}
			}
		}
		if !hasFailing {
			return nil, nil, false
		}

		intercepted.Add(1)
		return nil, errors.New("injected broker failure"), true
	})

	// Produce fresh records after warmup so there's data to fetch.
	produceNStrings(t, admin, topic, 60)

	// We should get records from healthy partitions despite failing
	// brokers, because the cascade promotes the next source.
	var got int
	for got < 5 && ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		for _, r := range fetches.Records() {
			r.Ack(kgo.AckAccept)
			got++
		}
	}
	if got < 5 {
		t.Fatalf("expected at least 5 records via cascade (maxConcurrent=%d, failing=%v), got %d (intercepted %d)",
			maxConcurrent, failPartitions, got, intercepted.Load())
	}
	t.Logf("got %d records, intercepted %d fetches to failing partitions", got, intercepted.Load())
}

// TestUnreleasedInstanceIDCapFires verifies that kgo's KIP-848 manage loop
// caps consecutive UnreleasedInstanceID errors at 3 on initialJoin, then
// surfaces the error to the user as an *ErrGroupSession wrapping
// UnreleasedInstanceID.
func TestUnreleasedInstanceIDCapFires(t *testing.T) {
	t.Parallel()
	topic := "t848-unreleased-cap"
	group := "g848-unreleased-cap"

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))

	initialJoinAttempts := c.Fault(Fault{
		Keys:  []kmsg.Key{kmsg.ConsumerGroupHeartbeat},
		Err:   kerr.UnreleasedInstanceID,
		Count: -1,
		When:  isInitialJoin,
	})

	consumer := newClient848(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.InstanceID("test-cap-instance"),
	)

	// Poll with a timeout long enough for the 3 retries + cap fire +
	// manageFailWait to inject the error. The default retry backoff is
	// ~250ms, ~500ms, ~1s for the 3 retries, plus the 4th attempt that
	// triggers the cap. 10s is generous.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var sawErr bool
	for !sawErr && ctx.Err() == nil {
		fs := consumer.PollFetches(ctx)
		fs.EachError(func(_ string, _ int32, err error) {
			var gs *kgo.ErrGroupSession
			if errors.As(err, &gs) && errors.Is(gs.Err, kerr.UnreleasedInstanceID) {
				sawErr = true
			}
		})
	}
	if !sawErr {
		t.Fatal("expected *ErrGroupSession wrapping UnreleasedInstanceID, but it was never surfaced")
	}

	attempts := initialJoinAttempts.Hits()
	if attempts < 4 {
		t.Fatalf("expected >= 4 initialJoin attempts (3 retries + cap fire), got %d", attempts)
	}
	t.Logf("initialJoin attempts seen by ControlKey: %d", attempts)
}

// TestUnreleasedInstanceIDRaceResolves verifies that when the
// UnreleasedInstanceID error resolves within the retry budget (< 3), the
// consumer successfully joins the group and consumes records with no error
// visible to the user.
func TestUnreleasedInstanceIDRaceResolves(t *testing.T) {
	t.Parallel()
	topic := "t848-unreleased-race"
	group := "g848-unreleased-race"
	nRecords := 10

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	producer := newClient848(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, nRecords)

	// The first two joins fail; the third reaches the real handler.
	initialJoinAttempts := c.Fault(Fault{
		Keys:  []kmsg.Key{kmsg.ConsumerGroupHeartbeat},
		Err:   kerr.UnreleasedInstanceID,
		Count: 2,
		When:  isInitialJoin,
	})

	consumer := newClient848(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.InstanceID("test-race-instance"),
	)

	// The consumer should recover and consume all records. Use a generous
	// timeout to allow for the 2 backoff cycles before the successful join.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var records []*kgo.Record
	for len(records) < nRecords && ctx.Err() == nil {
		fs := consumer.PollFetches(ctx)
		fs.EachError(func(_ string, _ int32, err error) {
			var gs *kgo.ErrGroupSession
			if errors.As(err, &gs) && errors.Is(gs.Err, kerr.UnreleasedInstanceID) {
				t.Fatalf("UnreleasedInstanceID should NOT have been surfaced, but got: %v", err)
			}
		})
		fs.EachRecord(func(r *kgo.Record) {
			records = append(records, r)
		})
	}
	if len(records) != nRecords {
		t.Fatalf("expected %d records, got %d", nRecords, len(records))
	}
	t.Logf("consumed %d records after %d initialJoin attempts", len(records), initialJoinAttempts.Hits())
}

// TestConsumeRecordHeaders is a regression test for the per-batch
// RecordHeader slab in processRecordBatch: records with differing header
// counts in the same batch must each round-trip with their own headers.
// A bug in the slab sub-slice cursor would cross-contaminate headers
// between neighboring records.
func TestConsumeRecordHeaders(t *testing.T) {
	t.Parallel()
	topic := "t-record-headers"

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	producer := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
	consumer := newPlainClient(t, c,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {0: kgo.NewOffset().At(0)},
		}),
	)

	// Mix 0-header and multi-header records so the hot path exercises
	// both the h==nil branch and the sub-slice branch. Tag each header
	// with its record index so cross-record aliasing surfaces as a
	// content mismatch, not a silent pass.
	const nRecords = 5
	recs := make([]*kgo.Record, nRecords)
	for i := range recs {
		hdrs := make([]kgo.RecordHeader, i)
		for j := range hdrs {
			hdrs[j] = kgo.RecordHeader{
				Key:   fmt.Sprintf("r%d-k%d", i, j),
				Value: []byte(fmt.Sprintf("r%d-v%d", i, j)),
			}
		}
		recs[i] = &kgo.Record{
			Value:   []byte(strconv.Itoa(i)),
			Headers: hdrs,
		}
	}
	produceSync(t, producer, recs...)

	got := consumeN(t, consumer, nRecords, 5*time.Second)

	for i, r := range got {
		if string(r.Value) != strconv.Itoa(i) {
			t.Fatalf("record %d: value %q != %q", i, r.Value, strconv.Itoa(i))
		}
		if len(r.Headers) != i {
			t.Fatalf("record %d: header count %d != %d", i, len(r.Headers), i)
		}
		for j, h := range r.Headers {
			wantK := fmt.Sprintf("r%d-k%d", i, j)
			wantV := fmt.Sprintf("r%d-v%d", i, j)
			if h.Key != wantK || string(h.Value) != wantV {
				t.Fatalf("record %d header %d: got (%q,%q) want (%q,%q)",
					i, j, h.Key, h.Value, wantK, wantV)
			}
		}
	}
}

// testRecordPool is a deterministically-reusing PoolRecords. The first
// slice we see is cached and handed back on every subsequent GetRecords
// whose size fits -- so the []Record memory for batch 1 becomes the
// same memory for batch 2. This reproduces the pool-reuse aliasing
// scenario that motivates the lastPolledSlabs identity check.
type testRecordPool struct {
	mu   sync.Mutex
	recs []kgo.Record
}

func (p *testRecordPool) GetRecords(n int) []kgo.Record {
	p.mu.Lock()
	defer p.mu.Unlock()
	if cap(p.recs) >= n {
		r := p.recs[:n]
		p.recs = nil
		return r
	}
	return make([]kgo.Record, 0, n)
}

func (p *testRecordPool) PutRecords(r []kgo.Record) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if cap(r) > cap(p.recs) {
		p.recs = r
	}
}

// TestShareGroupRecyclePoolAliasing guards against the pool-reuse
// aliasing bug where calling Record.Recycle without Ack, combined with
// a PoolRecords that hands the same []Record back on the next fetch,
// would cause finalizePreviousPoll to silently auto-accept the NEW
// batch's records via aliased *Record pointers -- overriding the
// user's subsequent explicit Ack. The fix materializes the minimum
// ack state (slab + state pointer + offset) in sc.lastPolled so
// finalize never dereferences the aliased *Record, eliminating both
// the logical aliasing AND the underlying r.Context read/write data
// race.
//
// Correct behavior observed on the wire:
//
//   - Batch 1 offsets: AckAccept (finalize auto-accepts records the
//     user neither Acked nor Rejected, per the next-poll contract).
//   - Batch 2 offsets: AckReject (user's explicit intent preserved).
//
// Buggy behavior would show AckAccept for batch 2 offsets.
func TestShareGroupRecyclePoolAliasing(t *testing.T) {
	t.Parallel()
	topic, group := "share-pool-reuse", "share-pool-reuse-g"
	c := newCluster(t, SeedTopics(1, topic))

	admin := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
	c.SetGroupConfigs(group, map[string]string{"share.auto.offset.reset": "earliest"})

	pool := &testRecordPool{}
	consumer := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ShareGroup(group),
		kgo.WithPools(pool),
		kgo.FetchMaxWait(100*time.Millisecond),
	)

	// Observe per-offset ack types on the wire (from both standalone
	// ShareAcknowledge and piggybacked ShareFetch ack batches). Pass
	// through so the cluster handles them normally.
	type ackObs struct {
		offset int64
		at     int8
	}
	var (
		ackMu    sync.Mutex
		observed []ackObs
	)
	captureBatches := func(bs []kmsg.ShareAcknowledgeRequestTopicPartitionAcknowledgementBatch) {
		ackMu.Lock()
		defer ackMu.Unlock()
		for _, b := range bs {
			if len(b.AcknowledgeTypes) == 1 {
				for off := b.FirstOffset; off <= b.LastOffset; off++ {
					observed = append(observed, ackObs{offset: off, at: b.AcknowledgeTypes[0]})
				}
			} else {
				for i, at := range b.AcknowledgeTypes {
					observed = append(observed, ackObs{offset: b.FirstOffset + int64(i), at: at})
				}
			}
		}
	}
	c.ControlKey(int16(kmsg.ShareAcknowledge), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		req := kreq.(*kmsg.ShareAcknowledgeRequest)
		for _, rt := range req.Topics {
			for _, rp := range rt.Partitions {
				captureBatches(rp.AcknowledgementBatches)
			}
		}
		c.KeepControl()
		return nil, nil, false
	})
	c.ControlKey(int16(kmsg.ShareFetch), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		req := kreq.(*kmsg.ShareFetchRequest)
		for _, rt := range req.Topics {
			for _, rp := range rt.Partitions {
				if len(rp.AcknowledgementBatches) > 0 {
					// ShareFetch and ShareAcknowledge share the same
					// AcknowledgementBatch wire shape; reinterpret.
					reshaped := make([]kmsg.ShareAcknowledgeRequestTopicPartitionAcknowledgementBatch, len(rp.AcknowledgementBatches))
					for i, b := range rp.AcknowledgementBatches {
						reshaped[i] = kmsg.ShareAcknowledgeRequestTopicPartitionAcknowledgementBatch{
							FirstOffset:      b.FirstOffset,
							LastOffset:       b.LastOffset,
							AcknowledgeTypes: b.AcknowledgeTypes,
						}
					}
					captureBatches(reshaped)
				}
			}
		}
		c.KeepControl()
		return nil, nil, false
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Batch 1 (offsets 0..2): produce, poll, Recycle all without Acking.
	produceNStrings(t, admin, topic, 3)
	var batch1 []*kgo.Record
	for len(batch1) < 3 {
		fs := consumer.PollFetches(ctx)
		fs.EachRecord(func(r *kgo.Record) { batch1 = append(batch1, r) })
	}
	batch1Addrs := []*kgo.Record{batch1[0], batch1[1], batch1[2]}
	for _, r := range batch1 {
		r.Recycle()
	}

	// Batch 2 (offsets 3..5): produce and let the source loop fetch
	// into the re-used pooled slice, aliasing batch 1's memory.
	produceNStrings(t, admin, topic, 3)
	time.Sleep(500 * time.Millisecond)

	var batch2 []*kgo.Record
	for len(batch2) < 3 {
		fs := consumer.PollFetches(ctx)
		fs.EachRecord(func(r *kgo.Record) { batch2 = append(batch2, r) })
	}

	// Sanity: confirm the pool reused the slice so batch 2's
	// *Record pointers coincide with batch 1's (proving we are
	// exercising the aliasing path).
	aliased := 0
	for _, r := range batch2 {
		for _, ba := range batch1Addrs {
			if r == ba {
				aliased++
				break
			}
		}
	}
	if aliased != 3 {
		t.Fatalf("pool did not reuse memory; aliased=%d want 3 (bug repro setup failed)", aliased)
	}

	// Explicit Reject of batch 2.
	for _, r := range batch2 {
		r.Ack(kgo.AckReject)
	}
	if err := consumer.FlushAcks(ctx); err != nil {
		t.Fatalf("FlushAcks: %v", err)
	}

	ackMu.Lock()
	defer ackMu.Unlock()
	byOffset := make(map[int64]kgo.AckStatus, len(observed))
	for _, o := range observed {
		byOffset[o.offset] = kgo.AckStatus(o.at)
	}
	// Batch 1 (offsets 0..2) must appear as Accept (auto-accepted
	// by finalizePreviousPoll per the documented contract).
	for _, off := range []int64{0, 1, 2} {
		if got := byOffset[off]; got != kgo.AckAccept {
			t.Errorf("batch 1 offset %d: got ack=%v, want AckAccept (finalize auto-accept)", off, got)
		}
	}
	// Batch 2 (offsets 3..5) must appear as Reject. The bug would
	// manifest here as AckAccept, meaning finalize silently
	// auto-accepted via the aliased pointer and the user's
	// explicit Ack(AckReject) was dropped.
	for _, off := range []int64{3, 4, 5} {
		if got := byOffset[off]; got != kgo.AckReject {
			t.Errorf("batch 2 offset %d: got ack=%v, want AckReject (pool-reuse aliasing regression?)", off, got)
		}
	}
}

// TestCommitFatalMemberErrorTriggersRejoin verifies that a classic
// (non-848) group member rejoins immediately when an OffsetCommit
// returns an error meaning the broker no longer recognizes the member.
// Without the fix, the consumer would zombie along (consuming but
// unable to commit) until the heartbeat loop noticed the dead session
// up to a heartbeat interval later.
func TestCommitFatalMemberErrorTriggersRejoin(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		err  *kerr.Error
	}{
		{"UnknownMemberID", kerr.UnknownMemberID},
		{"IllegalGeneration", kerr.IllegalGeneration},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			topic := "t-commit-fatal-" + test.name
			group := "g-commit-fatal-" + test.name
			const nRecords = 10

			c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
			produceNStrings(t, newPlainClient(t, c), topic, nRecords)

			cl := newPlainClient(t, c,
				kgo.ConsumeTopics(topic),
				kgo.ConsumerGroup(group),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
				kgo.FetchMaxWait(250*time.Millisecond),
				kgo.DisableAutoCommit(),
			)
			consumeN(t, cl, nRecords, 10*time.Second)

			// Count JoinGroups to observe the rejoin; the group is
			// stable now, so any join from here on is the rejoin.
			joins := c.Fault(Fault{Keys: []kmsg.Key{kmsg.JoinGroup}, Observe: true, Count: -1})
			c.Fault(Fault{Keys: []kmsg.Key{kmsg.OffsetCommit}, Err: test.err})

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			cl.CommitUncommittedOffsets(ctx) // fails with the injected error; the fix reacts to it

			if err := joins.Wait(ctx, 1); err != nil {
				t.Fatal("timed out waiting for a rejoin after a fatal commit error")
			}

			// The member must fully recover: new records are consumable
			// and committable after the rejoin.
			produceNStrings(t, newPlainClient(t, c), topic, nRecords)
			if got := consumeN(t, cl, nRecords, 15*time.Second); len(got) != nRecords {
				t.Fatalf("expected %d records after recovery, got %d", nRecords, len(got))
			}
			if err := cl.CommitUncommittedOffsets(context.Background()); err != nil {
				t.Fatalf("commit after recovery: %v", err)
			}
		})
	}
}

// compactRec is one produced record. An empty key produces a null key, and
// the tombstoneVal value produces a nil value, which is also how a surviving
// tombstone reads back in want.
type compactRec struct{ key, val string }

const tombstoneVal = "<tombstone>"

// TestCompact drives log compaction. Every batch is one ProduceSync, so a
// batch of several records lands on the wire as one record batch; the last
// batch is the active segment, which compaction never touches.
func TestCompact(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		configs   map[string]string
		batches   [][]compactRec
		again     [][]compactRec // produced after the first Compact, then compacted again
		at        kgo.Offset
		want      map[string]string
		wantFirst string // key=value of the first record read, for a case that pins the order
	}{
		{
			// Superseded keys and the null key drop; the active segment stays.
			name:    "dedup",
			batches: [][]compactRec{{{"a", "1"}}, {{"", "no-key"}}, {{"b", "2"}}, {{"a", "3"}}, {{"c", "4"}}, {{"b", "5"}}},
			at:      kgo.NewOffset().AtStart(),
			want:    map[string]string{"a": "3", "b": "5", "c": "4"},
		},
		{
			// A topic of one batch is all active segment: compaction is a no-op.
			name:    "active-segment",
			batches: [][]compactRec{{{"only", "one"}}},
			at:      kgo.NewOffset().AtStart(),
			want:    map[string]string{"only": "one"},
		},
		{
			// delete.retention.ms 0 expires the tombstone, so both k records go.
			name:    "tombstone-expired",
			configs: map[string]string{"delete.retention.ms": "0"},
			batches: [][]compactRec{{{"k", "v"}}, {{"k", tombstoneVal}}, {{"other", "x"}}},
			at:      kgo.NewOffset().AtStart(),
			want:    map[string]string{"other": "x"},
		},
		{
			// The default delete.retention.ms is 24h, so the tombstone stays.
			name:    "tombstone-retained",
			batches: [][]compactRec{{{"k", "v"}}, {{"k", tombstoneVal}}, {{"other", "x"}}},
			at:      kgo.NewOffset().AtStart(),
			want:    map[string]string{"k": tombstoneVal, "other": "x"},
		},
		{
			// Compaction leaves a hole at offset 0. A fetch of offset 0 has
			// to skip forward to the next batch it still has.
			name:      "offset-gaps",
			batches:   [][]compactRec{{{"a", "old"}}, {{"a", "new"}}, {{"b", "val"}}},
			at:        kgo.NewOffset().At(0),
			want:      map[string]string{"a": "new", "b": "val"},
			wantFirst: "a=new",
		},
		{
			// Only some records of a batch survive, so the batch is rebuilt:
			// re-encoded, re-CRCed, offset deltas redone.
			name:    "multi-record-batch",
			batches: [][]compactRec{{{"a", "old"}, {"b", "keep"}, {"c", "old"}}, {{"a", "new"}, {"c", "new"}}, {{"d", "active"}}},
			at:      kgo.NewOffset().AtStart(),
			want:    map[string]string{"a": "new", "b": "keep", "c": "new", "d": "active"},
		},
		{
			// The second pass has to decode the batches the first pass wrote.
			name:    "double-compaction",
			batches: [][]compactRec{{{"a", "v1"}}, {{"a", "v2"}}, {{"b", "v1"}}},
			again:   [][]compactRec{{{"a", "v3"}}, {{"c", "v1"}}},
			at:      kgo.NewOffset().AtStart(),
			want:    map[string]string{"a": "v3", "b": "v1", "c": "v1"},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			topic := "compact-" + tc.name
			c := newCluster(t, NumBrokers(1))
			cl := newPlainClient(t, c, kgo.RecordPartitioner(kgo.ManualPartitioner()))

			configs := map[string]string{"cleanup.policy": "compact"}
			maps.Copy(configs, tc.configs)
			if err := c.CreateTopic(topic, 1, configs); err != nil {
				t.Fatal(err)
			}

			produce := func(batches [][]compactRec) {
				for _, batch := range batches {
					var rs []*kgo.Record
					for _, cr := range batch {
						r := &kgo.Record{Topic: topic, Partition: 0, Value: []byte(cr.val)}
						if cr.key != "" {
							r.Key = []byte(cr.key)
						}
						if cr.val == tombstoneVal {
							r.Value = nil
						}
						rs = append(rs, r)
					}
					produceSync(t, cl, rs...)
				}
			}
			produce(tc.batches)
			c.Compact()
			if tc.again != nil {
				produce(tc.again)
				c.Compact()
			}

			consumer := newPlainClient(t, c,
				kgo.ConsumeTopics(topic),
				kgo.ConsumeResetOffset(tc.at),
			)
			records := consumeN(t, consumer, len(tc.want), 5*time.Second)
			got := make(map[string]string)
			for _, r := range records {
				v := string(r.Value)
				if r.Value == nil {
					v = tombstoneVal
				}
				got[string(r.Key)] = v
			}
			for k, wv := range tc.want {
				if got[k] != wv {
					t.Fatalf("key %q: want %q, got %q (all: %v)", k, wv, got[k], got)
				}
			}
			if tc.wantFirst != "" {
				if first := string(records[0].Key) + "=" + string(records[0].Value); first != tc.wantFirst {
					t.Fatalf("first record %s, want %s", first, tc.wantFirst)
				}
			}
		})
	}
}

// TestRetention drives retention: log.cleaner.backoff.ms drives the ticker,
// and ApplyRetention forces a pass without waiting for it.
func TestRetention(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name         string
		brokerCfg    map[string]string
		topicCfg     map[string]string
		first        []string
		sleep        time.Duration
		rest         []string // produced after the sleep, so retention leaves them
		apply        bool     // false leaves the pass to the cleaner ticker
		wantStart    int64    // minimum logStartOffset
		wantSurvivor string   // "" skips the read back
	}{
		{
			name:         "time",
			topicCfg:     map[string]string{"retention.ms": "100"},
			first:        []string{"old1", "old2"},
			sleep:        150 * time.Millisecond,
			rest:         []string{"new"},
			apply:        true,
			wantStart:    2,
			wantSurvivor: "new",
		},
		{
			// A single record batch is about 70 bytes, so 100 leaves the last.
			name:         "bytes",
			topicCfg:     map[string]string{"retention.bytes": "100"},
			first:        []string{"a", "b", "c"},
			apply:        true,
			wantStart:    2,
			wantSurvivor: "c",
		},
		{
			name:      "ticker",
			brokerCfg: map[string]string{"log.cleaner.backoff.ms": "50"},
			topicCfg:  map[string]string{"retention.ms": "1"},
			first:     []string{"old", "keep"},
			sleep:     200 * time.Millisecond,
			wantStart: 1,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			topic := "retention-" + tc.name
			var opts []Opt
			if tc.brokerCfg != nil {
				opts = append(opts, BrokerConfigs(tc.brokerCfg))
			}
			c := newCluster(t, append(opts, NumBrokers(1))...)
			cl := newPlainClient(t, c)
			if err := c.CreateTopic(topic, 1, tc.topicCfg); err != nil {
				t.Fatal(err)
			}

			for _, v := range tc.first {
				produceSync(t, cl, &kgo.Record{Topic: topic, Value: []byte(v)})
			}
			time.Sleep(tc.sleep)
			for _, v := range tc.rest {
				produceSync(t, cl, &kgo.Record{Topic: topic, Value: []byte(v)})
			}
			if tc.apply {
				c.ApplyRetention()
			}

			if pi := c.PartitionInfo(topic, 0); pi.LogStartOffset < tc.wantStart {
				t.Fatalf("expected logStartOffset >= %d after retention, got %d", tc.wantStart, pi.LogStartOffset)
			}
			if tc.wantSurvivor == "" {
				return
			}
			consumer := newPlainClient(t, c,
				kgo.ConsumeTopics(topic),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			)
			records := consumeN(t, consumer, 1, 5*time.Second)
			if string(records[0].Value) != tc.wantSurvivor {
				t.Fatalf("expected %q, got %q", tc.wantSurvivor, string(records[0].Value))
			}
		})
	}
}

// offsetCommitVersions crosses a cluster that speaks OffsetCommit and
// OffsetFetch v10, which identify a topic by ID, with one capped at v9,
// which identifies it by name.
var offsetCommitVersions = []struct {
	name string
	opts []Opt
}{
	{"v10", nil},
	{"v9", func() []Opt {
		v := kversion.Stable()
		v.SetMaxKeyVersion(8, 9) // OffsetCommit
		v.SetMaxKeyVersion(9, 9) // OffsetFetch
		return []Opt{MaxVersions(v)}
	}()},
}

// TestOffsetCommitTopicID commits an offset and reads it back through each
// API that can do so, against both a v10 and a v9 broker.
func TestOffsetCommitTopicID(t *testing.T) {
	t.Parallel()
	for _, api := range []struct {
		name string
		run  func(t *testing.T, c *Cluster, producer *kgo.Client, topic, group string)
	}{
		{"kadm", func(t *testing.T, c *Cluster, producer *kgo.Client, topic, group string) {
			adm := kadm.NewClient(producer)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			// Commit, read back, then commit again over the top.
			for _, at := range []int64{5, 10} {
				offsets := kadm.Offsets{}
				offsets.Add(kadm.Offset{Topic: topic, Partition: 0, At: at, LeaderEpoch: -1})
				rs, err := adm.CommitOffsets(ctx, group, offsets)
				if err != nil {
					t.Fatalf("commit offsets at %d failed: %v", at, err)
				}
				if err := rs.Error(); err != nil {
					t.Fatalf("commit offsets at %d response error: %v", at, err)
				}
				fetched, err := adm.FetchOffsets(ctx, group)
				if err != nil {
					t.Fatalf("fetch offsets after %d failed: %v", at, err)
				}
				o, ok := fetched.Lookup(topic, 0)
				if !ok {
					t.Fatalf("no committed offset found after commit at %d", at)
				}
				if o.At != at {
					t.Errorf("expected committed offset %d, got %d", at, o.At)
				}
			}
		}},

		{"kadm-by-id", func(t *testing.T, c *Cluster, producer *kgo.Client, topic, group string) {
			adm := kadm.NewClient(producer)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			meta, err := adm.Metadata(ctx, topic)
			if err != nil {
				t.Fatalf("metadata failed: %v", err)
			}
			td, ok := meta.Topics[topic]
			if !ok {
				t.Fatalf("topic %q not in metadata", topic)
			}
			topicID := td.ID

			os := kadm.OffsetsByID{}
			os.Add(kadm.Offset{TopicID: topicID, Partition: 0, At: 7, LeaderEpoch: -1})
			rs, err := adm.CommitOffsetsByID(ctx, group, os)
			if err != nil {
				t.Fatalf("commit by ID failed: %v", err)
			}
			if err := rs.Error(); err != nil {
				t.Fatalf("commit by ID response error: %v", err)
			}
			ro, ok := rs.Lookup(topicID, 0)
			if !ok {
				t.Fatal("no response for committed topic ID")
			}
			if ro.Err != nil {
				t.Fatalf("commit by ID partition error: %v", ro.Err)
			}

			fetched, err := adm.FetchOffsetsByID(ctx, group)
			if err != nil {
				t.Fatalf("fetch by ID failed: %v", err)
			}
			fo, ok := fetched.Lookup(topicID, 0)
			if !ok {
				t.Fatal("no fetched offset for topic ID")
			}
			if fo.At != 7 {
				t.Errorf("expected offset 7, got %d", fo.At)
			}
			if fo.TopicID != topicID {
				t.Errorf("expected TopicID %v, got %v", topicID, fo.TopicID)
			}
		}},

		{"848-consumer", func(t *testing.T, c *Cluster, producer *kgo.Client, topic, group string) {
			consumer := newClient848(t, c,
				kgo.ConsumeTopics(topic),
				kgo.ConsumerGroup(group),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
				kgo.DisableAutoCommit(),
			)
			records := consumeN(t, consumer, 10, 10*time.Second)
			if len(records) != 10 {
				t.Fatalf("expected 10 records, got %d", len(records))
			}

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := consumer.CommitUncommittedOffsets(ctx); err != nil {
				t.Fatalf("commit err: %v", err)
			}
			consumer.Close()

			adm := kadm.NewClient(newClient848(t, c))
			fetched, err := adm.FetchOffsets(ctx, group)
			if err != nil {
				t.Fatalf("fetch offsets failed: %v", err)
			}
			o, ok := fetched.Lookup(topic, 0)
			if !ok {
				t.Fatal("no committed offset found after 848 commit")
			}
			if o.At != 10 {
				t.Errorf("expected committed offset 10, got %d", o.At)
			}
		}},
	} {
		for _, v := range offsetCommitVersions {
			t.Run(api.name+"-"+v.name, func(t *testing.T) {
				t.Parallel()
				topic := "commit-topicid-" + api.name + "-" + v.name
				group := topic + "-group"
				c := newCluster(t, append([]Opt{NumBrokers(1), SeedTopics(1, topic)}, v.opts...)...)
				producer := newClient848(t, c, kgo.DefaultProduceTopic(topic))
				produceNStrings(t, producer, topic, 10)
				api.run(t, c, producer, topic, group)
			})
		}
	}
}

// TestShareGroupDescribe verifies that ShareGroupDescribe returns correct
// group state, epoch, and member information for an active share group.
func TestShareGroupDescribe(t *testing.T) {
	t.Parallel()
	topic := "sg-describe"
	group := "sg-describe-group"
	c, admin := shareAdminTopic(t, topic, group, 10)

	cl := newShareConsumer(t, c, topic, group)
	pollShareOnce(t, cl, 5*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := kmsg.NewPtrShareGroupDescribeRequest()
	req.GroupIDs = []string{group}
	resp, err := req.RequestWith(ctx, admin)
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if len(resp.Groups) != 1 {
		t.Fatalf("expected 1 group, got %d", len(resp.Groups))
	}
	g := resp.Groups[0]
	if err := kerr.ErrorForCode(g.ErrorCode); err != nil {
		t.Fatalf("describe error: %v", err)
	}
	if g.GroupState != "Stable" {
		t.Errorf("expected state Stable, got %q", g.GroupState)
	}
	if g.GroupEpoch <= 0 {
		t.Errorf("expected epoch > 0, got %d", g.GroupEpoch)
	}
	if len(g.Members) != 1 {
		t.Fatalf("expected 1 member, got %d", len(g.Members))
	}
	m := g.Members[0]
	if m.MemberID == "" {
		t.Error("member ID empty")
	}
	if !slices.Contains(m.SubscribedTopicNames, topic) {
		t.Errorf("topic %q not in subscribed %v", topic, m.SubscribedTopicNames)
	}
	if len(m.Assignment.TopicPartitions) == 0 {
		t.Error("no assignment")
	}
}

// TestShareGroupDescribeEmpty verifies describe on a group with no active
// members returns "Empty" state, and on a nonexistent group returns
// GROUP_ID_NOT_FOUND.
func TestShareGroupDescribeEmpty(t *testing.T) {
	t.Parallel()
	topic := "sg-describe-empty"
	group := "sg-describe-empty-group"
	c, admin := shareAdminTopic(t, topic, group, 5)

	// Join, consume, leave: creates the group then empties it.
	cl := newShareConsumer(t, c, topic, group)
	pollShareOnce(t, cl, 10*time.Second)
	cl.Close()
	waitShareGroupEmpty(t, c, group)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	req := kmsg.NewPtrShareGroupDescribeRequest()
	req.GroupIDs = []string{group, "nonexistent-sg"}
	resp, err := req.RequestWith(ctx, admin)
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if len(resp.Groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(resp.Groups))
	}

	// Look up groups by GroupID rather than relying on response order.
	groupsByID := make(map[string]kmsg.ShareGroupDescribeResponseGroup)
	for _, g := range resp.Groups {
		groupsByID[g.GroupID] = g
	}

	g0 := groupsByID[group]
	if err := kerr.ErrorForCode(g0.ErrorCode); err != nil {
		t.Fatalf("empty group error: %v", err)
	}
	if g0.GroupState != "Empty" {
		t.Errorf("expected Empty, got %q", g0.GroupState)
	}
	if len(g0.Members) != 0 {
		t.Errorf("expected 0 members, got %d", len(g0.Members))
	}

	g1 := groupsByID["nonexistent-sg"]
	if g1.ErrorCode != kerr.GroupIDNotFound.Code {
		t.Errorf("expected GROUP_ID_NOT_FOUND for nonexistent group, got %d", g1.ErrorCode)
	}
}

// TestDescribeShareGroupOffsetsNotFound verifies that describing offsets
// for a nonexistent group returns GROUP_ID_NOT_FOUND.
func TestDescribeShareGroupOffsetsNotFound(t *testing.T) {
	t.Parallel()
	c := newCluster(t)
	cl := newPlainClient(t, c)

	resp := describeShareOffsets(t, cl, "nonexistent-share-group", "t")
	// Java returns no group-level error for nonexistent groups --
	// the response simply has no partition data (absence, not error).
	if resp.Groups[0].ErrorCode != 0 {
		t.Errorf("expected no group-level error, got %d", resp.Groups[0].ErrorCode)
	}
}

// TestShareGroupOffsetsAdmin drives the share group offset admin RPCs. Every
// case consumes and accepts all 20 records first, so the SPSO sits at 20 and
// the group is empty when the RPC goes out.
func TestShareGroupOffsetsAdmin(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		waitEmpty bool
		act       func(t *testing.T, c *Cluster, admin *kgo.Client, topic, group string)
	}{
		{
			name: "describe",
			act: func(t *testing.T, c *Cluster, admin *kgo.Client, topic, group string) {
				resp := describeShareOffsets(t, admin, group, topic)
				if len(resp.Groups) != 1 {
					t.Fatalf("expected 1 group, got %d", len(resp.Groups))
				}
				g := resp.Groups[0]
				if err := kerr.ErrorForCode(g.ErrorCode); err != nil {
					t.Fatalf("describe offsets error: %v", err)
				}
				if len(g.Topics) != 1 || len(g.Topics[0].Partitions) != 1 {
					t.Fatal("unexpected response structure")
				}
				p := g.Topics[0].Partitions[0]
				if p.Partition != 0 {
					t.Errorf("expected partition 0, got %d", p.Partition)
				}
				// After accepting all 20 records, SPSO should be 20.
				if p.StartOffset != 20 {
					t.Errorf("expected StartOffset 20, got %d", p.StartOffset)
				}
				// Lag = HWM - SPSO = 20 - 20 = 0.
				if p.Lag != 0 {
					t.Errorf("expected Lag 0, got %d", p.Lag)
				}
			},
		},
		{
			name:      "alter",
			waitEmpty: true,
			act: func(t *testing.T, c *Cluster, admin *kgo.Client, topic, group string) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				// Alter SPSO to 10: the next consumer should get records 10-19.
				req := kmsg.NewPtrAlterShareGroupOffsetsRequest()
				req.GroupID = group
				rt := kmsg.NewAlterShareGroupOffsetsRequestTopic()
				rt.Topic = topic
				rp := kmsg.NewAlterShareGroupOffsetsRequestTopicPartition()
				rp.Partition = 0
				rp.StartOffset = 10
				rt.Partitions = append(rt.Partitions, rp)
				req.Topics = append(req.Topics, rt)

				resp, err := req.RequestWith(ctx, admin)
				if err != nil {
					t.Fatalf("alter offsets: %v", err)
				}
				if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
					t.Fatalf("alter error: %v", err)
				}
				if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
					t.Fatal("unexpected response structure")
				}
				if pp := resp.Topics[0].Partitions[0]; pp.ErrorCode != 0 {
					t.Fatalf("partition error: %v", kerr.ErrorForCode(pp.ErrorCode))
				}

				cl2 := newShareConsumer(t, c, topic, group)
				var got2 int
				var minOffset int64 = -1
				ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel2()
				for got2 < 10 {
					fs := cl2.PollFetches(ctx2)
					for _, r := range fs.Records() {
						if minOffset == -1 || r.Offset < minOffset {
							minOffset = r.Offset
						}
						r.Ack(kgo.AckAccept)
						got2++
					}
					if ctx2.Err() != nil {
						break
					}
				}
				if got2 < 10 {
					t.Fatalf("expected 10 records after alter, got %d", got2)
				}
				if minOffset != 10 {
					t.Errorf("expected min offset 10, got %d", minOffset)
				}
			},
		},
		{
			name:      "delete",
			waitEmpty: true,
			act: func(t *testing.T, c *Cluster, admin *kgo.Client, topic, group string) {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()

				dreq := kmsg.NewPtrDeleteShareGroupOffsetsRequest()
				dreq.GroupID = group
				dt := kmsg.NewDeleteShareGroupOffsetsRequestTopic()
				dt.Topic = topic
				dreq.Topics = append(dreq.Topics, dt)

				dresp, err := dreq.RequestWith(ctx, admin)
				if err != nil {
					t.Fatalf("delete offsets: %v", err)
				}
				if err := kerr.ErrorForCode(dresp.ErrorCode); err != nil {
					t.Fatalf("delete error: %v", err)
				}

				p := describeShareOffsets(t, admin, group, topic).Groups[0].Topics[0].Partitions[0]
				if p.StartOffset != -1 {
					t.Errorf("expected StartOffset -1 after delete, got %d", p.StartOffset)
				}

				// The partition state is gone, so a new consumer
				// re-initializes from share.auto.offset.reset and sees all 20.
				cl2 := newShareConsumer(t, c, topic, group)
				var got2 int
				ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel2()
				for got2 < 20 {
					fs := cl2.PollFetches(ctx2)
					for _, r := range fs.Records() {
						r.Ack(kgo.AckAccept)
						got2++
					}
					if ctx2.Err() != nil {
						break
					}
				}
				if got2 < 20 {
					t.Fatalf("expected 20 records after delete+re-consume, got %d", got2)
				}
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			topic := "sg-" + tc.name + "-offsets"
			group := topic + "-group"
			c, admin := shareAdminTopic(t, topic, group, 20)

			cl := newShareConsumer(t, c, topic, group)
			drainShareAccept(t, cl, 20, 10*time.Second)
			cl.Close()
			if tc.waitEmpty {
				waitShareGroupEmpty(t, c, group)
			}
			tc.act(t, c, admin, topic, group)
		})
	}
}

// TestShareGroupOffsetsAdminNonEmpty verifies that the offset admin RPCs
// refuse a share group that still has a member.
func TestShareGroupOffsetsAdminNonEmpty(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name string
		send func(t *testing.T, admin *kgo.Client, topic, group string) int16
	}{
		{"alter", func(t *testing.T, admin *kgo.Client, topic, group string) int16 {
			req := kmsg.NewPtrAlterShareGroupOffsetsRequest()
			req.GroupID = group
			rt := kmsg.NewAlterShareGroupOffsetsRequestTopic()
			rt.Topic = topic
			rp := kmsg.NewAlterShareGroupOffsetsRequestTopicPartition()
			rp.Partition = 0
			rp.StartOffset = 0
			rt.Partitions = append(rt.Partitions, rp)
			req.Topics = append(req.Topics, rt)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := req.RequestWith(ctx, admin)
			if err != nil {
				t.Fatalf("alter offsets: %v", err)
			}
			return resp.ErrorCode
		}},
		{"delete", func(t *testing.T, admin *kgo.Client, topic, group string) int16 {
			req := kmsg.NewPtrDeleteShareGroupOffsetsRequest()
			req.GroupID = group
			dt := kmsg.NewDeleteShareGroupOffsetsRequestTopic()
			dt.Topic = topic
			req.Topics = append(req.Topics, dt)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := req.RequestWith(ctx, admin)
			if err != nil {
				t.Fatalf("delete offsets: %v", err)
			}
			return resp.ErrorCode
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			topic := "sg-" + tc.name + "-nonempty"
			group := topic + "-group"
			c, admin := shareAdminTopic(t, topic, group, 5)

			cl := newShareConsumer(t, c, topic, group)
			pollShareOnce(t, cl, 5*time.Second)

			if code := tc.send(t, admin, topic, group); code != kerr.NonEmptyGroup.Code {
				t.Errorf("expected NON_EMPTY_GROUP (%d), got %d", kerr.NonEmptyGroup.Code, code)
			}
		})
	}
}

// Test848TopicCreatedAfterJoin verifies that a regex subscribed consumer
// picks up a topic created after it already joined. The minute long
// MetadataMaxAge row keeps a periodic metadata refresh from racing the
// heartbeat: the heartbeat then always delivers the assignment before the
// client learns of the topic itself, so the server has to keep including
// the assignment until the client confirms it.
func Test848TopicCreatedAfterJoin(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name       string
		metaMaxAge time.Duration
	}{
		{"periodic-meta", 100 * time.Millisecond},
		{"no-periodic-meta", time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			prefix := "t848-" + tc.name + "-"
			existingTopic := prefix + "existing"
			newTopic := prefix + "new"
			group := "g848-" + tc.name
			nRecords := 10

			c := newCluster(t, NumBrokers(1), SeedTopics(1, existingTopic))
			producer := newClient848(t, c)
			produceNStrings(t, producer, existingTopic, nRecords)

			consumer := newClient848(t, c,
				kgo.ConsumeRegex(),
				kgo.ConsumeTopics(prefix+".*"),
				kgo.ConsumerGroup(group),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
				kgo.MetadataMinAge(50*time.Millisecond),
				kgo.MetadataMaxAge(tc.metaMaxAge),
				kgo.FetchMaxWait(250*time.Millisecond),
			)

			records := consumeN(t, consumer, nRecords, 10*time.Second)
			if len(records) != nRecords {
				t.Fatalf("expected %d records from existing topic, got %d", nRecords, len(records))
			}

			if err := c.CreateTopic(newTopic, 1, nil); err != nil {
				t.Fatalf("create topic failed: %v", err)
			}
			for i := range nRecords {
				r := kgo.StringRecord("new-" + strconv.Itoa(i))
				r.Topic = newTopic
				produceSync(t, producer, r)
			}

			records = consumeN(t, consumer, nRecords, 15*time.Second)
			newTopicCount := 0
			for _, r := range records {
				if r.Topic == newTopic {
					newTopicCount++
				}
			}
			if newTopicCount != nRecords {
				t.Fatalf("expected %d records from new topic, got %d", nRecords, newTopicCount)
			}
		})
	}
}

// TestStaticMemberRecovery verifies that a static member reclaims its slot
// after it goes away: a classic member after its session times out, and an
// 848 member after both a static leave at epoch -2 and a session timeout.
func TestStaticMemberRecovery(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name           string
		use848         bool
		brokerCfg      map[string]string
		firstOpts      []kgo.Opt // extra options for the member that goes away
		gone           time.Duration
		wantInstanceID bool
		wantAssigned   int // 0 skips the check
	}{
		{
			name:      "classic-session-timeout",
			brokerCfg: map[string]string{"group.min.session.timeout.ms": "100"},
			firstOpts: []kgo.Opt{
				kgo.SessionTimeout(500 * time.Millisecond),
				kgo.HeartbeatInterval(100 * time.Millisecond), // must be < session timeout
			},
			gone:           700 * time.Millisecond,
			wantInstanceID: true,
		},
		{
			// Closing a client that carries an instanceID sends epoch -2.
			name:         "848-static-leave",
			use848:       true,
			gone:         500 * time.Millisecond,
			wantAssigned: 2,
		},
		{
			name:         "848-session-timeout",
			use848:       true,
			brokerCfg:    map[string]string{"group.consumer.session.timeout.ms": "500"},
			gone:         700 * time.Millisecond,
			wantAssigned: 2,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			topic := "static-" + tc.name
			group := topic + "-group"
			instanceID := topic + "-inst"

			opts := []Opt{NumBrokers(1), SeedTopics(2, topic)}
			if tc.brokerCfg != nil {
				opts = append(opts, BrokerConfigs(tc.brokerCfg))
			}
			c := newCluster(t, opts...)

			newMember := func(extra ...kgo.Opt) *kgo.Client {
				base := []kgo.Opt{
					kgo.ConsumeTopics(topic),
					kgo.ConsumerGroup(group),
					kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
					kgo.FetchMaxWait(250 * time.Millisecond),
					kgo.InstanceID(instanceID),
				}
				if tc.use848 {
					return newClient848(t, c, append(base, extra...)...)
				}
				return newPlainClient(t, c, append(base, extra...)...)
			}

			producer := newPlainClient(t, c)
			produceNStrings(t, producer, topic, 20)

			cl1 := newMember(tc.firstOpts...)
			consumeN(t, cl1, 20, 10*time.Second)
			waitStable(t, c, group, 1)

			// A static member does not send a leave on close, except
			// under 848 where it leaves at epoch -2.
			cl1.Close()
			time.Sleep(tc.gone)

			newMember()
			dg := waitStable(t, c, group, 1)

			if tc.wantInstanceID {
				found := false
				for _, m := range dg.Members {
					if m.InstanceID != nil && *m.InstanceID == instanceID {
						found = true
					}
				}
				if !found {
					t.Fatalf("instanceID %q not found in group members after rejoin", instanceID)
				}
			}
			if tc.wantAssigned > 0 && dg.NumAssigned() != tc.wantAssigned {
				t.Fatalf("expected %d partitions assigned after rejoin, got %d", tc.wantAssigned, dg.NumAssigned())
			}
		})
	}
}

// endTxnStep is one EndTxn request, always sent at the epoch InitProducerID
// handed back, which is what a retry after a lost response looks like.
type endTxnStep struct {
	commit   bool
	wantCode int16
	// wantEpochUp asserts the response carries an epoch above the one we
	// sent; wantEpochSame asserts it matches what the first step got back.
	wantEpochUp   bool
	wantEpochSame bool
}

// TestTxnEndTxnRetry walks EndTxn through the retries a producer makes when
// it loses a response, at TV1 (v4, no epoch bump) and TV2 (v5, KIP-890,
// which bumps the epoch).
func TestTxnEndTxnRetry(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name  string
		tv1   bool // pin EndTxn to v4
		topic string
		steps []endTxnStep
	}{
		{
			// TV1 is idempotent in the same direction and rejects the
			// other one.
			name:  "tv1-retry",
			tv1:   true,
			topic: "t-endtxn-retry",
			steps: []endTxnStep{
				{commit: true},
				{commit: true},
				{commit: false, wantCode: kerr.InvalidTxnState.Code},
			},
		},
		{
			// A commit at v5 bumps the epoch. A retry still carries the
			// old epoch, so the wrong direction has to be rejected and
			// the right one has to replay the bumped epoch.
			name:  "tv2-retry-mismatched-direction",
			topic: "t-endtxn-v5-mismatch",
			steps: []endTxnStep{
				{commit: true},
				{commit: false, wantCode: kerr.InvalidTxnState.Code},
				{commit: true, wantEpochSame: true},
			},
		},
		{
			// Aborting a transaction with no produce and no offsets is
			// allowed at v5, and bumps the epoch.
			name:  "tv2-empty-abort",
			steps: []endTxnStep{{commit: false, wantEpochUp: true}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			txid := "txid-" + tc.name

			opts := []Opt{NumBrokers(1)}
			if tc.topic != "" {
				opts = append(opts, SeedTopics(1, tc.topic))
			}
			c := newCluster(t, opts...)

			// kgo overrides req.Version with the negotiated max, so
			// pinning TV1 takes MaxVersions rather than req.Version.
			var clOpts []kgo.Opt
			if tc.tv1 {
				v := kversion.Stable()
				v.SetMaxKeyVersion(26, 4) // EndTxn key = 26
				clOpts = append(clOpts, kgo.MaxVersions(v))
			}
			cl := newPlainClient(t, c, clOpts...)
			ctx := context.Background()

			initResp := initProducerID(t, cl, txid, -1, -1, 60000)
			if initResp.ErrorCode != 0 {
				t.Fatalf("init: %v", kerr.ErrorForCode(initResp.ErrorCode))
			}
			pid, epoch := initResp.ProducerID, initResp.ProducerEpoch

			if tc.topic != "" {
				addReq := kmsg.NewAddPartitionsToTxnRequest()
				addReq.TransactionalID = txid
				addReq.ProducerID = pid
				addReq.ProducerEpoch = epoch
				addT := kmsg.NewAddPartitionsToTxnRequestTopic()
				addT.Topic = tc.topic
				addT.Partitions = []int32{0}
				addReq.Topics = append(addReq.Topics, addT)
				if _, err := addReq.RequestWith(ctx, cl); err != nil {
					t.Fatalf("add partitions: %v", err)
				}
			}

			var firstEpoch int16
			for i, step := range tc.steps {
				endReq := kmsg.NewEndTxnRequest()
				endReq.TransactionalID = txid
				endReq.ProducerID = pid
				endReq.ProducerEpoch = epoch
				endReq.Commit = step.commit
				resp, err := endReq.RequestWith(ctx, cl)
				if err != nil {
					t.Fatalf("step %d (commit=%v): %v", i, step.commit, err)
				}
				if resp.ErrorCode != step.wantCode {
					t.Fatalf("step %d (commit=%v): want %v, got %v", i, step.commit,
						kerr.ErrorForCode(step.wantCode), kerr.ErrorForCode(resp.ErrorCode))
				}
				if i == 0 {
					firstEpoch = resp.ProducerEpoch
				}
				if step.wantEpochUp && resp.ProducerEpoch <= epoch {
					t.Fatalf("step %d: expected epoch > %d, got %d", i, epoch, resp.ProducerEpoch)
				}
				if step.wantEpochSame && resp.ProducerEpoch != firstEpoch {
					t.Fatalf("step %d: expected epoch %d, got %d", i, firstEpoch, resp.ProducerEpoch)
				}
			}
		})
	}
}
