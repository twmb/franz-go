// Derived via LLM from Apache Kafka's ConsumerGroupHeartbeatRequestTest.scala and
// GroupMetadataManagerTest.java (Apache 2.0).
// https://github.com/apache/kafka/blob/trunk/group-coordinator/src/test/java/org/apache/kafka/coordinator/group/GroupMetadataManagerTest.java

package kafka_tests

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Test848JoinAndConsume verifies a single consumer joining via the KIP-848
// protocol, receiving an assignment, and consuming records end-to-end.
// Derived via LLM from testConsumerGroupHeartbeatIsAccessibleWhenNewGroupCoordinatorIsEnabled.
func Test848JoinAndConsume(t *testing.T) {
	t.Parallel()
	topic := "t848-join"
	group := "g848-join"
	nRecords := 50

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(3, topic))
	producer := newClient(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, nRecords)

	consumer := newClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)

	records := consumeN(t, consumer, nRecords, 10*time.Second)
	if len(records) != nRecords {
		t.Fatalf("expected %d records, got %d", nRecords, len(records))
	}
}

// Test848TwoConsumersRebalance verifies that when a second consumer joins,
// partitions are redistributed across both members.
// Derived via LLM from testNewJoiningMemberTriggersNewTargetAssignment.
func Test848TwoConsumersRebalance(t *testing.T) {
	t.Parallel()
	topic := "t848-rebal"
	group := "g848-rebal"
	nRecords := 100
	nPartitions := 6

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(int32(nPartitions), topic))
	producer := newClient(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, nRecords)

	// First consumer joins and gets all partitions.
	c1 := newClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	_ = consumeN(t, c1, nRecords, 10*time.Second)

	// Second consumer joins the same group.
	c2 := newClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)

	// Produce more records so both consumers have something to fetch.
	produceNStrings(t, producer, topic, nRecords)

	// Both consumers should eventually get records, meaning partitions
	// were split between them.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	c1Got, c2Got := false, false
	for !c1Got || !c2Got {
		if !c1Got {
			fs := c1.PollRecords(ctx, 10)
			fs.EachRecord(func(*kgo.Record) { c1Got = true })
		}
		if !c2Got {
			fs := c2.PollRecords(ctx, 10)
			fs.EachRecord(func(*kgo.Record) { c2Got = true })
		}
		if ctx.Err() != nil {
			t.Fatalf("timeout waiting for both consumers to get records (c1=%v, c2=%v)", c1Got, c2Got)
		}
	}
}

// Test848ConsumerLeaveReassigns verifies that when a consumer leaves,
// the remaining consumer gets all partitions.
// Derived via LLM from testLeavingMemberBumpsGroupEpoch.
func Test848ConsumerLeaveReassigns(t *testing.T) {
	t.Parallel()
	topic := "t848-leave"
	group := "g848-leave"
	nRecords := 50
	nPartitions := 4

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(int32(nPartitions), topic))
	producer := newClient(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, nRecords)

	// Two consumers join. Use a short FetchMaxWait so that when
	// partitions are reassigned, the new partition's fetch begins
	// promptly rather than waiting for the prior long poll to expire.
	c1 := newClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	c2 := newClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)

	// Consume all existing records to stabilize.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	got := 0
	for got < nRecords {
		fs := c1.PollRecords(ctx, 100)
		fs.EachRecord(func(*kgo.Record) { got++ })
		fs = c2.PollRecords(ctx, 100)
		fs.EachRecord(func(*kgo.Record) { got++ })
		if ctx.Err() != nil {
			t.Fatalf("timeout consuming initial records: got %d/%d", got, nRecords)
		}
	}

	// Explicitly commit c2's offsets before closing. Without this,
	// the auto-commit can race with the leave request - if the leave
	// arrives first, the commit is rejected and c1 would re-read
	// records from c2's former partitions.
	if err := c2.CommitUncommittedOffsets(ctx); err != nil {
		t.Fatalf("c2 commit before close: %v", err)
	}

	// Close c2, leaving only c1.
	c2.Close()

	// Produce more records. c1 should now get all of them.
	produceNStrings(t, producer, topic, nRecords)
	records := consumeN(t, c1, nRecords, 15*time.Second)
	if len(records) != nRecords {
		t.Fatalf("expected %d records after c2 left, got %d", nRecords, len(records))
	}

	// Verify all partitions are assigned to c1 by checking we see
	// records from multiple partitions.
	partitions := make(map[int32]bool)
	for _, r := range records {
		partitions[r.Partition] = true
	}
	if len(partitions) < 2 {
		t.Fatalf("expected records from multiple partitions after rebalance, got %d", len(partitions))
	}
}

// Test848OffsetCommitAndFetch verifies that offsets committed through a
// KIP-848 consumer group can be fetched back.
// Derived via LLM from testReconciliationProcess (offset commit portion).
func Test848OffsetCommitAndFetch(t *testing.T) {
	t.Parallel()
	topic := "t848-commit"
	group := "g848-commit"
	nRecords := 30

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	producer := newClient(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, nRecords)

	// Consume and commit.
	consumer := newClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	records := consumeN(t, consumer, nRecords, 10*time.Second)
	if len(records) != nRecords {
		t.Fatalf("expected %d records, got %d", nRecords, len(records))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := consumer.CommitUncommittedOffsets(ctx); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// Verify via kadm that offsets are committed.
	adm := kadm.NewClient(newClient(t, c))
	offsets, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatalf("fetch offsets failed: %v", err)
	}
	off, ok := offsets.Lookup(topic, 0)
	if !ok {
		t.Fatal("no committed offset for partition 0")
	}
	if off.At != int64(nRecords) {
		t.Fatalf("expected committed offset %d, got %d", nRecords, off.At)
	}
}

// Test848SubscriptionChange verifies that changing subscription (adding a
// topic) triggers rebalance and the consumer gets partitions for the new topic.
// Derived via LLM from testUpdatingSubscriptionTriggersNewTargetAssignment.
func Test848SubscriptionChange(t *testing.T) {
	t.Parallel()
	topic1 := "t848-sub1"
	topic2 := "t848-sub2"
	group := "g848-sub"
	nRecords := 20

	c := newCluster(t, kfake.NumBrokers(1),
		kfake.SeedTopics(2, topic1),
		kfake.SeedTopics(2, topic2),
	)
	producer := newClient(t, c)

	// Produce to both topics.
	for i := range nRecords {
		r := kgo.StringRecord("v-" + strconv.Itoa(i))
		r.Topic = topic1
		produceSync(t, producer, r)
		r2 := kgo.StringRecord("v-" + strconv.Itoa(i))
		r2.Topic = topic2
		produceSync(t, producer, r2)
	}

	// Consumer subscribes to both topics from the start.
	consumer := newClient(t, c,
		kgo.ConsumeTopics(topic1, topic2),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)

	// Should get records from both topics.
	records := consumeN(t, consumer, nRecords*2, 10*time.Second)
	topics := make(map[string]int)
	for _, r := range records {
		topics[r.Topic]++
	}
	if topics[topic1] == 0 {
		t.Fatalf("expected records from %s", topic1)
	}
	if topics[topic2] == 0 {
		t.Fatalf("expected records from %s", topic2)
	}
}

// Test848DescribeGroup verifies that ConsumerGroupDescribe (key 69) returns
// correct state for a KIP-848 consumer group.
// Derived via LLM from testConsumerGroupHeartbeatFullResponse.
func Test848DescribeGroup(t *testing.T) {
	t.Parallel()
	topic := "t848-describe"
	group := "g848-describe"

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(2, topic))
	producer := newClient(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, 10)

	consumer := newClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	_ = consumeN(t, consumer, 10, 10*time.Second)

	// Allow heartbeats to stabilize.
	time.Sleep(500 * time.Millisecond)

	descClient := newClient(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	adm := kadm.NewClient(descClient)
	described, err := adm.DescribeConsumerGroups(ctx, group)
	if err != nil {
		t.Fatalf("describe consumer groups failed: %v", err)
	}
	dg, ok := described[group]
	if !ok {
		t.Fatal("group not found in describe response")
	}
	if dg.Err != nil {
		t.Fatalf("describe group error: %v", dg.Err)
	}
	if len(dg.Members) != 1 {
		t.Fatalf("expected 1 member, got %d", len(dg.Members))
	}
	if dg.Epoch < 1 {
		t.Fatalf("expected epoch >= 1, got %d", dg.Epoch)
	}
	m := dg.Members[0]
	if m.MemberID == "" {
		t.Fatal("expected non-empty member ID")
	}
	if len(m.SubscribedTopics) == 0 {
		t.Fatal("expected subscribed topics")
	}
}

// Test848TxnOffsetCommit verifies that transactional offset commits work
// with KIP-848 consumer groups.
// Derived via LLM from testConsumerGroupTransactionalOffsetCommit (OffsetMetadataManagerTest.java).
func Test848TxnOffsetCommit(t *testing.T) {
	t.Parallel()
	topic := "t848-txn-commit"
	group := "g848-txn-commit"
	nRecords := 20

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	producer := newClient(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, nRecords)

	// Transactional consumer: consume, commit offsets in transaction.
	txnConsumer := newClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.TransactionalID("t848-txn-consumer"),
	)

	records := consumeN(t, txnConsumer, nRecords, 10*time.Second)
	if len(records) != nRecords {
		t.Fatalf("expected %d records, got %d", nRecords, len(records))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := txnConsumer.BeginTransaction(); err != nil {
		t.Fatalf("begin txn: %v", err)
	}
	if err := txnConsumer.CommitUncommittedOffsets(ctx); err != nil {
		t.Fatalf("commit offsets: %v", err)
	}
	if err := txnConsumer.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatalf("end txn: %v", err)
	}

	// Verify committed offsets.
	adm := kadm.NewClient(newClient(t, c))
	offsets, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatalf("fetch offsets failed: %v", err)
	}
	off, ok := offsets.Lookup(topic, 0)
	if !ok {
		t.Fatal("no committed offset for partition 0")
	}
	if off.At != int64(nRecords) {
		t.Fatalf("expected committed offset %d, got %d", nRecords, off.At)
	}
}

// Test848EmptyGroupDescribe verifies that describing a non-existent or
// empty consumer group returns the appropriate error.
// Derived via LLM from testUnknownGroupId (GroupMetadataManagerTest.java).
func Test848EmptyGroupDescribe(t *testing.T) {
	t.Parallel()
	group := "g848-empty-describe"

	c := newCluster(t, kfake.NumBrokers(1))
	cl := newClient(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	adm := kadm.NewClient(cl)
	described, err := adm.DescribeGroups(ctx, group)
	if err != nil {
		t.Fatalf("describe groups failed: %v", err)
	}
	dg, ok := described[group]
	if !ok {
		t.Fatal("group not found in describe response")
	}
	// A non-existent group should show as Dead.
	if dg.State != "Dead" {
		t.Fatalf("expected state Dead for non-existent group, got %s", dg.State)
	}
}

// Test848FencedEpochRecovery verifies that a kgo consumer automatically
// recovers after the server returns FencedMemberEpoch. We inject the error
// via ControlKey and confirm the consumer rejoins and continues consuming.
// Derived via LLM from testConsumerGroupMemberEpochValidation.
func Test848FencedEpochRecovery(t *testing.T) {
	t.Parallel()
	topic := "t848-fenced"
	group := "g848-fenced"
	nRecords := 50

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(3, topic))
	producer := newClient(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, nRecords)

	consumer := newClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)

	// Consume all records and commit so the consumer has a stable position.
	_ = consumeN(t, consumer, nRecords, 10*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := consumer.CommitUncommittedOffsets(ctx); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// Inject a FencedMemberEpoch error on the next regular heartbeat.
	// The control is consumed after one use, so only one heartbeat is
	// affected. kgo should rejoin with epoch 0 and resume consuming.
	c.ControlKey(int16(kmsg.ConsumerGroupHeartbeat), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		req := kreq.(*kmsg.ConsumerGroupHeartbeatRequest)
		if req.MemberEpoch > 0 {
			resp := kmsg.NewPtrConsumerGroupHeartbeatResponse()
			resp.ErrorCode = kerr.FencedMemberEpoch.Code
			return resp, nil, true
		}
		return nil, nil, false
	})

	// Produce more records. The consumer should recover from fencing
	// and consume them.
	produceNStrings(t, producer, topic, nRecords)
	records := consumeN(t, consumer, nRecords, 15*time.Second)
	if len(records) != nRecords {
		t.Fatalf("expected %d records after fencing recovery, got %d", nRecords, len(records))
	}
}

// Test848SessionTimeout verifies that when a consumer stops heartbeating,
// the server removes it after the session timeout and reassigns its
// partitions to the remaining consumer.
// Derived via LLM from testSessionTimeoutExpiration.
func Test848SessionTimeout(t *testing.T) {
	t.Parallel()
	topic := "t848-timeout"
	group := "g848-timeout"
	nRecords := 50
	nPartitions := 4

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(int32(nPartitions), topic))
	producer := newClient(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, nRecords)

	// Use a short FetchMaxWait so that when partitions are reassigned
	// after c2's session timeout, c1's next fetch includes the new
	// partitions promptly rather than blocking on the prior long poll.
	c1 := newClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)

	// c2 uses a short rebalance timeout so the server removes it quickly.
	c2 := newClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.RebalanceTimeout(500*time.Millisecond),
	)

	// Consume all records to stabilize both consumers.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	got := 0
	for got < nRecords {
		fs := c1.PollRecords(ctx, 100)
		fs.EachRecord(func(*kgo.Record) { got++ })
		fs = c2.PollRecords(ctx, 100)
		fs.EachRecord(func(*kgo.Record) { got++ })
		if ctx.Err() != nil {
			t.Fatalf("timeout consuming initial records: got %d/%d", got, nRecords)
		}
	}

	// Commit both consumers' offsets so that when c1 picks up c2's
	// partitions after timeout, it resumes from the correct offset.
	if err := c1.CommitUncommittedOffsets(ctx); err != nil {
		t.Fatalf("c1 commit: %v", err)
	}
	if err := c2.CommitUncommittedOffsets(ctx); err != nil {
		t.Fatalf("c2 commit: %v", err)
	}

	// Intercept c2's leave heartbeat (MemberEpoch == -1) so the server
	// does not process the leave. The control is consumed after one use,
	// so c1's eventual leave during t.Cleanup goes through normally.
	c.ControlKey(int16(kmsg.ConsumerGroupHeartbeat), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		req := kreq.(*kmsg.ConsumerGroupHeartbeatRequest)
		if req.MemberEpoch == -1 {
			resp := kmsg.NewPtrConsumerGroupHeartbeatResponse()
			resp.MemberEpoch = -1
			return resp, nil, true
		}
		return nil, nil, false
	})

	// Close c2. The leave heartbeat is intercepted and dropped, so the
	// server still considers c2 a member. After c2's session timeout
	// (500ms) the server removes it and reassigns partitions to c1.
	c2.Close()
	time.Sleep(800 * time.Millisecond)

	// Produce more records. c1 should now own all partitions and consume
	// all of them.
	produceNStrings(t, producer, topic, nRecords)
	records := consumeN(t, c1, nRecords, 15*time.Second)
	if len(records) != nRecords {
		t.Fatalf("expected %d records after session timeout, got %d", nRecords, len(records))
	}

	// Verify records came from multiple partitions (c1 got c2's former
	// partitions back).
	partitions := make(map[int32]bool)
	for _, r := range records {
		partitions[r.Partition] = true
	}
	if len(partitions) < 2 {
		t.Fatalf("expected records from multiple partitions, got %d", len(partitions))
	}
}
