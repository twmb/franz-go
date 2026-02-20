package kfake_test

import (
	"context"
	"errors"
	"hash/crc32"
	"math/rand"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

func newCluster(t *testing.T, opts ...kfake.Opt) *kfake.Cluster {
	t.Helper()
	c, err := kfake.NewCluster(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

// newClient848 creates a kgo client with the KIP-848 context opt-in enabled.
func newClient848(t *testing.T, c *kfake.Cluster, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	ctx := context.WithValue(context.Background(), "opt_in_kafka_next_gen_balancer_beta", true)
	opts = append([]kgo.Opt{kgo.SeedBrokers(c.ListenAddrs()...), kgo.WithContext(ctx)}, opts...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func produceSync(t *testing.T, cl *kgo.Client, records ...*kgo.Record) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatalf("produce failed: %v", err)
	}
}

func produceNStrings(t *testing.T, cl *kgo.Client, topic string, n int) {
	t.Helper()
	var records []*kgo.Record
	for i := range n {
		r := kgo.StringRecord("value-" + strconv.Itoa(i))
		r.Topic = topic
		r.Key = []byte("key-" + strconv.Itoa(i))
		records = append(records, r)
	}
	produceSync(t, cl, records...)
}

func consumeN(t *testing.T, cl *kgo.Client, n int, timeout time.Duration) []*kgo.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var records []*kgo.Record
	for len(records) < n {
		fs := cl.PollFetches(ctx)
		if errs := fs.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err == context.DeadlineExceeded || e.Err == context.Canceled {
					t.Fatalf("timeout consuming records: got %d/%d", len(records), n)
				}
			}
			t.Fatalf("consume errors: %v", errs)
		}
		fs.EachRecord(func(r *kgo.Record) {
			records = append(records, r)
		})
	}
	return records
}

func waitForStableGroup(t *testing.T, adm *kadm.Client, group string, nMembers int, timeout time.Duration) kadm.DescribedConsumerGroup {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		described, err := adm.DescribeConsumerGroups(ctx, group)
		if err != nil {
			t.Fatalf("describe failed: %v", err)
		}
		dg := described[group]
		if dg.State == "Stable" && len(dg.Members) == nMembers {
			return dg
		}
		if ctx.Err() != nil {
			t.Fatalf("timeout waiting for stable group %q with %d members (state=%s, members=%d)", group, nMembers, dg.State, len(dg.Members))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// newGroupConsumer creates a kgo client configured for group consuming with
// sensible test defaults: ConsumeTopics, ConsumerGroup, AtStart reset,
// and 250ms FetchMaxWait. Additional opts are appended after the defaults.
func newGroupConsumer(t *testing.T, c *kfake.Cluster, topic, group string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	base := []kgo.Opt{
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250 * time.Millisecond),
	}
	return newClient848(t, c, append(base, opts...)...)
}

// poll1FromEachClient polls each client until every one has received at least
// one record, or the timeout expires.
func poll1FromEachClient(t *testing.T, timeout time.Duration, clients ...*kgo.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	remaining := make(map[int]*kgo.Client, len(clients))
	for i, cl := range clients {
		remaining[i] = cl
	}
	for len(remaining) > 0 {
		for i, cl := range remaining {
			fs := cl.PollRecords(ctx, 10)
			if fs.NumRecords() > 0 {
				delete(remaining, i)
			}
		}
		if ctx.Err() != nil {
			t.Fatalf("timeout waiting for all clients to get records: %d/%d remaining", len(remaining), len(clients))
		}
	}
}

func totalAssignedPartitions(dg kadm.DescribedConsumerGroup) int {
	n := 0
	for _, m := range dg.Members {
		for _, parts := range m.Assignment {
			n += len(parts)
		}
	}
	return n
}

// Test848RegexSubscription verifies that server-side regex subscription
// matches the correct topics and excludes non-matching topics.
func Test848RegexSubscription(t *testing.T) {
	t.Parallel()
	matchA := "t848-rx-aaa"
	matchB := "t848-rx-bbb"
	noMatch := "t848-other"
	group := "g848-regex"
	nRecords := 10

	c := newCluster(t, kfake.NumBrokers(1),
		kfake.SeedTopics(2, matchA),
		kfake.SeedTopics(2, matchB),
		kfake.SeedTopics(2, noMatch),
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

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
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

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
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

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
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

	c := newCluster(t, kfake.NumBrokers(1),
		kfake.SeedTopics(3, topicA),
		kfake.SeedTopics(6, topicB),
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

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))

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
		if string(r.Value) == "" {
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

	c := newCluster(t, kfake.NumBrokers(1),
		kfake.SeedTopics(1, topicA),
		kfake.SeedTopics(1, topicB),
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

// Test848TopicCreatedAfterJoin verifies that when a new topic is created
// after a regex-subscribed consumer has already joined, the consumer
// picks up the new topic on the next metadata refresh.
func Test848TopicCreatedAfterJoin(t *testing.T) {
	t.Parallel()
	existingTopic := "t848-dynamic-existing"
	newTopic := "t848-dynamic-new"
	group := "g848-dynamic"
	nRecords := 10

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, existingTopic))
	producer := newClient848(t, c)
	produceNStrings(t, producer, existingTopic, nRecords)

	// Consumer subscribes with regex matching both existing and future topics.
	consumer := newClient848(t, c,
		kgo.ConsumeRegex(),
		kgo.ConsumeTopics("t848-dynamic-.*"),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.MetadataMinAge(50*time.Millisecond),
		kgo.MetadataMaxAge(100*time.Millisecond),
		kgo.FetchMaxWait(250*time.Millisecond),
	)

	// Consume from existing topic first.
	records := consumeN(t, consumer, nRecords, 10*time.Second)
	if len(records) != nRecords {
		t.Fatalf("expected %d records from existing topic, got %d", nRecords, len(records))
	}

	// Create the new topic and produce to it.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	adm := kadm.NewClient(newClient848(t, c))
	_, err := adm.CreateTopics(ctx, 1, 1, nil, newTopic)
	if err != nil {
		t.Fatalf("create topic failed: %v", err)
	}
	for i := range nRecords {
		r := kgo.StringRecord("new-" + strconv.Itoa(i))
		r.Topic = newTopic
		produceSync(t, producer, r)
	}

	// Consumer should pick up the new topic via metadata refresh.
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
}

// Test848TopicCreatedAfterJoinNoPeriodicMeta is the same scenario as
// Test848TopicCreatedAfterJoin but with MetadataMaxAge set to 1 minute
// so no periodic metadata refresh races with the heartbeat. This makes
// the bug deterministic: the heartbeat always delivers the assignment
// before the client discovers the new topic via metadata, exercising
// the resend path where the server must keep including the assignment
// until the client confirms it.
func Test848TopicCreatedAfterJoinNoPeriodicMeta(t *testing.T) {
	t.Parallel()
	existingTopic := "t848-nopermeta-existing"
	newTopic := "t848-nopermeta-new"
	group := "g848-nopermeta"
	nRecords := 10

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, existingTopic))
	producer := newClient848(t, c)
	produceNStrings(t, producer, existingTopic, nRecords)

	// MetadataMaxAge=1min prevents periodic metadata from discovering
	// the new topic before the heartbeat delivers the assignment.
	consumer := newClient848(t, c,
		kgo.ConsumeRegex(),
		kgo.ConsumeTopics("t848-nopermeta-.*"),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.MetadataMinAge(50*time.Millisecond),
		kgo.MetadataMaxAge(time.Minute),
		kgo.FetchMaxWait(250*time.Millisecond),
	)

	records := consumeN(t, consumer, nRecords, 10*time.Second)
	if len(records) != nRecords {
		t.Fatalf("expected %d records from existing topic, got %d", nRecords, len(records))
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	adm := kadm.NewClient(newClient848(t, c))
	_, err := adm.CreateTopics(ctx, 1, 1, nil, newTopic)
	if err != nil {
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

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(int32(nPartitions), topic))
	producer := newClient848(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, 60)

	// Two consumers using the range balancer.
	c1 := newGroupConsumer(t, c, topic, group, kgo.Balancers(kgo.RangeBalancer()))
	c2 := newGroupConsumer(t, c, topic, group, kgo.Balancers(kgo.RangeBalancer()))

	// Wait for the group to stabilize with 2 members and verify
	// the assignment via DescribeConsumerGroups.
	adm := kadm.NewClient(newClient848(t, c))
	dg := waitForStableGroup(t, adm, group, 2, 10*time.Second)

	// Verify each member's assignment is contiguous.
	for _, m := range dg.Members {
		for topicName, parts := range m.Assignment {
			var ps []int32
			for p := range parts {
				ps = append(ps, p)
			}
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

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, "t"))

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

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	producer := newClient848(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, 10)

	// Classic group consumer.
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
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
	_, err = adm.CommitOffsets(ctx, group, offsets)
	if err != nil {
		t.Fatalf("admin commit failed: %v", err)
	}

	// Verify committed offsets.
	fetched, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatalf("fetch offsets failed: %v", err)
	}
	o, ok := fetched.Lookup(topic, 0)
	if !ok {
		t.Fatal("no committed offset found after commit-after-leave")
	}
	if o.At != 10 {
		t.Errorf("expected committed offset 10, got %d", o.At)
	}
}

// TestOffsetCommitAfterLeave848 verifies that an admin-style OffsetCommit
// (empty memberID, negative epoch) is accepted on an empty KIP-848 consumer
// group after the member has left.
func TestOffsetCommitAfterLeave848(t *testing.T) {
	t.Parallel()
	topic := "commit-after-leave-848"
	group := "commit-after-leave-848-group"

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
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

	// Admin-style commit with v9 OffsetCommit: empty memberID, generation -1.
	commitReq := kmsg.NewOffsetCommitRequest()
	commitReq.Version = 9
	commitReq.Group = group
	commitReq.Generation = -1
	rt := kmsg.NewOffsetCommitRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewOffsetCommitRequestTopicPartition()
	rp.Partition = 0
	rp.Offset = 10
	rt.Partitions = append(rt.Partitions, rp)
	commitReq.Topics = append(commitReq.Topics, rt)

	raw := newClient848(t, c)
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

	// Verify committed offsets.
	adm := kadm.NewClient(raw)
	fetched, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatalf("fetch offsets failed: %v", err)
	}
	o, ok := fetched.Lookup(topic, 0)
	if !ok {
		t.Fatal("no committed offset found after commit-after-leave")
	}
	if o.At != 10 {
		t.Errorf("expected committed offset 10, got %d", o.At)
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

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(int32(nPartitions), topic))
	producer := newClient848(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, nRecords)

	adm := kadm.NewClient(newClient848(t, c))

	c1 := newGroupConsumer(t, c, topic, group)
	c2 := newGroupConsumer(t, c, topic, group)

	// Wait for stable 2-member group.
	dg := waitForStableGroup(t, adm, group, 2, 10*time.Second)
	if total := totalAssignedPartitions(dg); total != nPartitions {
		t.Fatalf("expected %d total partitions, got %d", nPartitions, total)
	}

	// Verify no partition overlap.
	seen := make(map[string]string) // "topic/partition" -> memberID
	for _, m := range dg.Members {
		for topicName, parts := range m.Assignment {
			for p := range parts {
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
	dg = waitForStableGroup(t, adm, group, 1, 10*time.Second)
	if total := totalAssignedPartitions(dg); total != nPartitions {
		t.Fatalf("expected %d partitions after c2 leave, got %d", nPartitions, total)
	}

	// Produce more and verify c1 consumes from all partitions.
	produceNStrings(t, producer, topic, nRecords)
	records := consumeN(t, c1, nRecords, 10*time.Second)
	partitions := make(map[int32]bool)
	for _, r := range records {
		partitions[r.Partition] = true
	}
	if len(partitions) != nPartitions {
		t.Errorf("expected records from all %d partitions, got %d", nPartitions, len(partitions))
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

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(int32(nPartitions), topic))
	producer := newClient848(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, nRecords)

	adm := kadm.NewClient(newClient848(t, c))

	nocommit := kgo.DisableAutoCommit()
	c1 := newGroupConsumer(t, c, topic, group, nocommit)
	c2 := newGroupConsumer(t, c, topic, group, nocommit)

	waitForStableGroup(t, adm, group, 2, 10*time.Second)

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
	waitForStableGroup(t, adm, group, 3, 15*time.Second)

	consumeCtx, consumeCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer consumeCancel()
	got = 0
	for got < nRecords {
		for _, cl := range []*kgo.Client{c1, c2, c3} {
			fs := cl.PollRecords(consumeCtx, 50)
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
		kfake.NumBrokers(1),
		kfake.SeedTopics(int32(nPartitions), topic),
		kfake.BrokerConfigs(map[string]string{
			// Long session timeout so it doesn't interfere.
			"group.consumer.session.timeout.ms": "30000",
		}),
	)

	// Plain client for raw requests and admin.
	raw, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()
	adm := kadm.NewClient(raw)

	// A joins via kgo (automatic heartbeating).
	a := newGroupConsumer(t, c, topic, group)
	produceNStrings(t, a, topic, 30)
	waitForStableGroup(t, adm, group, 1, 5*time.Second)

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
		described, err := adm.DescribeConsumerGroups(ctx, group)
		if err != nil {
			t.Fatalf("describe: %v", err)
		}
		if described[group].State == "Stable" && len(described[group].Members) == 2 {
			break
		}
		if i == 19 {
			t.Fatalf("timeout waiting for stable 2-member group (state=%s, members=%d)", described[group].State, len(described[group].Members))
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
	for {
		described, err := adm.DescribeConsumerGroups(deadline, group)
		if err != nil {
			t.Fatalf("describe: %v", err)
		}
		if described[group].State != "Stable" {
			break
		}
		if deadline.Err() != nil {
			t.Fatal("timeout waiting for rebalance to start")
		}
		time.Sleep(50 * time.Millisecond)
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

	described, err := adm.DescribeConsumerGroups(ctx, group)
	if err != nil {
		t.Fatalf("describe after timeout: %v", err)
	}
	dg := described[group]
	for _, m := range dg.Members {
		if m.MemberID == bMemberID {
			t.Fatalf("member B (%s) should have been fenced by rebalance timeout, but is still in group (state=%s)", bMemberID, dg.State)
		}
	}
}

// newPlainClient creates a kgo client without 848 opt-in.
func newPlainClient(t *testing.T, c *kfake.Cluster, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	opts = append([]kgo.Opt{kgo.SeedBrokers(c.ListenAddrs()...)}, opts...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
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

	c := newCluster(t, kfake.NumBrokers(1))
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

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
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

// TestTxnEndTxnTV1Retry verifies that retrying EndTxn at the same
// epoch returns success when the retry matches (commit-after-commit,
// abort-after-abort) and INVALID_TXN_STATE when it doesn't.
// Uses MaxVersions to pin EndTxn to v4 (TV1) to avoid KIP-890
// epoch bumping - kgo always overrides the request version to the
// negotiated max, so setting req.Version is not sufficient.
func TestTxnEndTxnTV1Retry(t *testing.T) {
	t.Parallel()
	topic := "t-endtxn-retry"

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	// Pin EndTxn (key 26) to v4 so we get TV1 behavior.
	// kgo overrides req.Version to the negotiated max, so we
	// must use MaxVersions to cap it.
	v := kversion.Stable()
	v.SetMaxKeyVersion(26, 4) // EndTxn key = 26
	cl := newPlainClient(t, c, kgo.MaxVersions(v))
	ctx := context.Background()

	// Init producer.
	initResp := initProducerID(t, cl, "txid-retry", -1, -1, 60000)
	if initResp.ErrorCode != 0 {
		t.Fatalf("init: %v", kerr.ErrorForCode(initResp.ErrorCode))
	}
	pid := initResp.ProducerID
	epoch := initResp.ProducerEpoch

	// Add a partition to start a transaction.
	addReq := kmsg.NewAddPartitionsToTxnRequest()
	addReq.TransactionalID = "txid-retry"
	addReq.ProducerID = pid
	addReq.ProducerEpoch = epoch
	addT := kmsg.NewAddPartitionsToTxnRequestTopic()
	addT.Topic = topic
	addT.Partitions = []int32{0}
	addReq.Topics = append(addReq.Topics, addT)
	if _, err := addReq.RequestWith(ctx, cl); err != nil {
		t.Fatalf("add partitions: %v", err)
	}

	// EndTxn commit at v4 (TV1 - no epoch bump).
	endReq := kmsg.NewEndTxnRequest()
	endReq.Version = 4
	endReq.TransactionalID = "txid-retry"
	endReq.ProducerID = pid
	endReq.ProducerEpoch = epoch
	endReq.Commit = true
	endResp, err := endReq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("end commit: %v", err)
	}
	if endResp.ErrorCode != 0 {
		t.Fatalf("end commit error: %v", kerr.ErrorForCode(endResp.ErrorCode))
	}

	// Retry commit at same epoch - should succeed (TV1 idempotency).
	endResp2, err := endReq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("retry commit: %v", err)
	}
	if endResp2.ErrorCode != 0 {
		t.Fatalf("retry commit should succeed, got: %v", kerr.ErrorForCode(endResp2.ErrorCode))
	}

	// Retry abort at same epoch - should fail (commit != abort).
	endReq.Commit = false
	endResp3, err := endReq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("retry abort: %v", err)
	}
	if endResp3.ErrorCode != kerr.InvalidTxnState.Code {
		t.Fatalf("retry abort-after-commit should be INVALID_TXN_STATE, got: %v", kerr.ErrorForCode(endResp3.ErrorCode))
	}
}

// TestTxnEndTxnTV2EmptyAbort verifies that aborting an empty
// transaction (no produce, no offsets) succeeds for TV2 (v5+).
func TestTxnEndTxnTV2EmptyAbort(t *testing.T) {
	t.Parallel()

	c := newCluster(t, kfake.NumBrokers(1))
	cl := newPlainClient(t, c)
	ctx := context.Background()

	// Init producer.
	initResp := initProducerID(t, cl, "txid-empty-abort", -1, -1, 60000)
	if initResp.ErrorCode != 0 {
		t.Fatalf("init: %v", kerr.ErrorForCode(initResp.ErrorCode))
	}

	// EndTxn abort when no transaction is active. kfake advertises
	// EndTxn v5 (KIP-890), so kgo sends v5 by default.
	endReq := kmsg.NewEndTxnRequest()
	endReq.TransactionalID = "txid-empty-abort"
	endReq.ProducerID = initResp.ProducerID
	endReq.ProducerEpoch = initResp.ProducerEpoch
	endReq.Commit = false
	endResp, err := endReq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("empty abort: %v", err)
	}
	if endResp.ErrorCode != 0 {
		t.Fatalf("empty abort should succeed for TV2, got: %v", kerr.ErrorForCode(endResp.ErrorCode))
	}
}

// TestTxnEpochBumpMonotonic verifies that repeated InitProducerID
// calls with the current epoch bump monotonically.
func TestTxnEpochBumpMonotonic(t *testing.T) {
	t.Parallel()

	c := newCluster(t, kfake.NumBrokers(1))
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

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
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

	// Build a produce request with specific PID/epoch/sequence.
	buildProduce := func() *kmsg.ProduceRequest {
		rec := kmsg.Record{Key: []byte("k"), Value: []byte("v")}
		rec.Length = int32(len(rec.AppendTo(nil)) - 1)
		now := time.Now().UnixMilli()
		batch := kmsg.RecordBatch{
			PartitionLeaderEpoch: -1,
			Magic:                2,
			LastOffsetDelta:      0,
			FirstTimestamp:       now,
			MaxTimestamp:         now,
			ProducerID:           pid,
			ProducerEpoch:        epoch,
			FirstSequence:        0,
			NumRecords:           1,
			Records:              rec.AppendTo(nil),
		}
		raw := batch.AppendTo(nil)
		batch.Length = int32(len(raw) - 12)
		raw = batch.AppendTo(nil)
		batch.CRC = int32(crc32.Checksum(raw[21:], crc32.MakeTable(crc32.Castagnoli)))

		req := kmsg.NewProduceRequest()
		req.Version = 11 // use topic names, not topic IDs
		req.Acks = -1
		req.TimeoutMillis = 5000
		rt := kmsg.NewProduceRequestTopic()
		rt.Topic = topic
		rp := kmsg.NewProduceRequestTopicPartition()
		rp.Partition = 0
		rp.Records = batch.AppendTo(nil)
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)
		return &req
	}

	// First produce: sequence 0, should succeed.
	resp1, err := buildProduce().RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("produce 1: %v", err)
	}
	p1 := resp1.Topics[0].Partitions[0]
	if p1.ErrorCode != 0 {
		t.Fatalf("produce 1 error: %v", kerr.ErrorForCode(p1.ErrorCode))
	}
	origOffset := p1.BaseOffset
	if origOffset < 20 {
		t.Fatalf("expected offset >= 20 (after padding), got %d", origOffset)
	}

	// Duplicate produce: same PID, epoch, sequence 0.
	resp2, err := buildProduce().RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("produce 2 (dup): %v", err)
	}
	p2 := resp2.Topics[0].Partitions[0]
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

	c := newCluster(t, kfake.NumBrokers(1))
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
				req.RequestWith(ctx, cl) //nolint:errcheck
			}
		}()
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				req := kmsg.NewListTransactionsRequest()
				req.RequestWith(ctx, cl) //nolint:errcheck
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

	c := newCluster(t, kfake.NumBrokers(1))
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

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
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
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))

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

// TestTxnEndTxnTV2RetryMismatchedDirection verifies that retrying an
// EndTxn v5+ with the wrong direction (e.g. abort after a committed
// transaction) returns INVALID_TXN_STATE.
func TestTxnEndTxnTV2RetryMismatchedDirection(t *testing.T) {
	t.Parallel()
	topic := "t-endtxn-v5-mismatch"

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	cl := newPlainClient(t, c)
	ctx := context.Background()

	resp := initProducerID(t, cl, "txid-v5-mismatch", -1, -1, 60000)
	if resp.ErrorCode != 0 {
		t.Fatalf("init: %v", kerr.ErrorForCode(resp.ErrorCode))
	}
	pid := resp.ProducerID
	epoch := resp.ProducerEpoch

	// Start and commit a transaction.
	addReq := kmsg.NewAddPartitionsToTxnRequest()
	addReq.TransactionalID = "txid-v5-mismatch"
	addReq.ProducerID = pid
	addReq.ProducerEpoch = epoch
	addT := kmsg.NewAddPartitionsToTxnRequestTopic()
	addT.Topic = topic
	addT.Partitions = []int32{0}
	addReq.Topics = append(addReq.Topics, addT)
	if _, err := addReq.RequestWith(ctx, cl); err != nil {
		t.Fatalf("add partitions: %v", err)
	}

	endReq := kmsg.NewEndTxnRequest()
	endReq.TransactionalID = "txid-v5-mismatch"
	endReq.ProducerID = pid
	endReq.ProducerEpoch = epoch
	endReq.Commit = true
	endResp, err := endReq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("end commit: %v", err)
	}
	if endResp.ErrorCode != 0 {
		t.Fatalf("end commit error: %v", kerr.ErrorForCode(endResp.ErrorCode))
	}
	// v5+ bumps epoch on commit.
	newEpoch := endResp.ProducerEpoch

	// Retry with the OLD epoch but ABORT direction - should fail.
	endReq.ProducerEpoch = epoch // old epoch, server has newEpoch
	endReq.Commit = false        // wrong direction
	endResp2, err := endReq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("retry abort: %v", err)
	}
	if endResp2.ErrorCode != kerr.InvalidTxnState.Code {
		t.Fatalf("expected INVALID_TXN_STATE for mismatched retry, got: %v", kerr.ErrorForCode(endResp2.ErrorCode))
	}

	// Retry with old epoch and COMMIT direction - should succeed.
	endReq.Commit = true
	endResp3, err := endReq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("retry commit: %v", err)
	}
	if endResp3.ErrorCode != 0 {
		t.Fatalf("matching retry should succeed, got: %v", kerr.ErrorForCode(endResp3.ErrorCode))
	}
	if endResp3.ProducerEpoch != newEpoch {
		t.Fatalf("expected epoch %d in retry response, got %d", newEpoch, endResp3.ProducerEpoch)
	}
}

// TestTxnEndTxnTV2EmptyAbortBumpsEpoch verifies that aborting an empty
// transaction at v5+ bumps the epoch.
func TestTxnEndTxnTV2EmptyAbortBumpsEpoch(t *testing.T) {
	t.Parallel()

	c := newCluster(t, kfake.NumBrokers(1))
	cl := newPlainClient(t, c)
	ctx := context.Background()

	resp := initProducerID(t, cl, "txid-empty-bump", -1, -1, 60000)
	if resp.ErrorCode != 0 {
		t.Fatalf("init: %v", kerr.ErrorForCode(resp.ErrorCode))
	}
	origEpoch := resp.ProducerEpoch

	endReq := kmsg.NewEndTxnRequest()
	endReq.TransactionalID = "txid-empty-bump"
	endReq.ProducerID = resp.ProducerID
	endReq.ProducerEpoch = origEpoch
	endReq.Commit = false
	endResp, err := endReq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("empty abort: %v", err)
	}
	if endResp.ErrorCode != 0 {
		t.Fatalf("empty abort error: %v", kerr.ErrorForCode(endResp.ErrorCode))
	}
	if endResp.ProducerEpoch <= origEpoch {
		t.Fatalf("expected epoch > %d after empty abort, got %d", origEpoch, endResp.ProducerEpoch)
	}
}

// TestTxnInitProducerIDMaxTimeout verifies that InitProducerID with a
// timeout exceeding transaction.max.timeout.ms returns
// INVALID_TRANSACTION_TIMEOUT.
func TestTxnInitProducerIDMaxTimeout(t *testing.T) {
	t.Parallel()

	c := newCluster(t, kfake.NumBrokers(1), kfake.BrokerConfigs(map[string]string{
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

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	v := kversion.Stable()
	v.SetMaxKeyVersion(0, 11)
	cl := newPlainClient(t, c, kgo.MaxVersions(v))
	ctx := context.Background()

	// Build a batch with the control bit (0x0020) set.
	rec := kmsg.Record{Key: []byte{0, 0, 0, 1}, Value: []byte{}}
	rec.Length = int32(len(rec.AppendTo(nil)) - 1)
	now := time.Now().UnixMilli()
	batch := kmsg.RecordBatch{
		PartitionLeaderEpoch: -1,
		Magic:                2,
		Attributes:           int16(0x0030), // transactional + control
		LastOffsetDelta:      0,
		FirstTimestamp:       now,
		MaxTimestamp:         now,
		ProducerID:           1,
		ProducerEpoch:        0,
		FirstSequence:        -1,
		NumRecords:           1,
		Records:              rec.AppendTo(nil),
	}
	raw := batch.AppendTo(nil)
	batch.Length = int32(len(raw) - 12)
	raw = batch.AppendTo(nil)
	batch.CRC = int32(crc32.Checksum(raw[21:], crc32.MakeTable(crc32.Castagnoli)))

	req := kmsg.NewProduceRequest()
	req.Version = 11
	req.Acks = -1
	req.TimeoutMillis = 5000
	rt := kmsg.NewProduceRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewProduceRequestTopicPartition()
	rp.Partition = 0
	rp.Records = batch.AppendTo(nil)
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	errCode := resp.Topics[0].Partitions[0].ErrorCode
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

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
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

	// Produce a NON-transactional batch using the same producer ID.
	rec := kmsg.Record{Key: []byte("k"), Value: []byte("v")}
	rec.Length = int32(len(rec.AppendTo(nil)) - 1)
	now := time.Now().UnixMilli()
	batch := kmsg.RecordBatch{
		PartitionLeaderEpoch: -1,
		Magic:                2,
		Attributes:           0, // non-transactional
		LastOffsetDelta:      0,
		FirstTimestamp:       now,
		MaxTimestamp:         now,
		ProducerID:           pid,
		ProducerEpoch:        epoch,
		FirstSequence:        0,
		NumRecords:           1,
		Records:              rec.AppendTo(nil),
	}
	raw := batch.AppendTo(nil)
	batch.Length = int32(len(raw) - 12)
	raw = batch.AppendTo(nil)
	batch.CRC = int32(crc32.Checksum(raw[21:], crc32.MakeTable(crc32.Castagnoli)))

	produceReq := kmsg.NewProduceRequest()
	produceReq.Version = 11
	produceReq.Acks = -1
	produceReq.TimeoutMillis = 5000
	rt := kmsg.NewProduceRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewProduceRequestTopicPartition()
	rp.Partition = 0
	rp.Records = batch.AppendTo(nil)
	rt.Partitions = append(rt.Partitions, rp)
	produceReq.Topics = append(produceReq.Topics, rt)

	produceResp, err := produceReq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	errCode := produceResp.Topics[0].Partitions[0].ErrorCode
	if errCode != kerr.InvalidTxnState.Code {
		t.Fatalf("expected INVALID_TXN_STATE for non-txn produce during tx, got: %v", kerr.ErrorForCode(errCode))
	}
}

// TestClassicIncompatibleProtocolRejected verifies that a member whose
// protocols are not supported by all existing members is rejected with
// INCONSISTENT_GROUP_PROTOCOL.
func TestClassicIncompatibleProtocolRejected(t *testing.T) {
	t.Parallel()
	topic := "t-incompat-proto"
	group := "g-incompat-proto"

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
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

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
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

	// The member should have been removed. Verify by describing
	// the group - it should be empty or dead.
	adm := kadm.NewClient(cl)
	described, err := adm.DescribeGroups(ctx, group)
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	dg := described[group]
	if len(dg.Members) > 0 {
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

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(2, topic))
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

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic), kfake.BrokerConfigs(map[string]string{
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
}) *kmsg.FetchRequest {
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

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
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
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
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

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(3, topic))
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

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
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

func stringp(s string) *string { return &s }

// TestCompactBasic verifies key deduplication, null-key dropping, and
// the active segment invariant (single batch is never compacted).
func TestCompactBasic(t *testing.T) {
	t.Parallel()
	compact := "compact"
	c := newCluster(t, kfake.NumBrokers(1))

	cl := newPlainClient(t, c)
	adm := kadm.NewClient(cl)

	// Dedup topic: produce a=1, null, b=2, a=3, c=4, b=5.
	// After compaction: a=3, c=4, b=5 (null-key and superseded dropped).
	topic := "compact-basic"
	_, err := adm.CreateTopic(context.Background(), 1, 1, map[string]*string{
		"cleanup.policy": &compact,
	}, topic)
	if err != nil {
		t.Fatal(err)
	}
	produceSync(t, cl, &kgo.Record{Topic: topic, Key: []byte("a"), Value: []byte("1")})
	produceSync(t, cl, &kgo.Record{Topic: topic, Value: []byte("no-key")}) // null key
	produceSync(t, cl, &kgo.Record{Topic: topic, Key: []byte("b"), Value: []byte("2")})
	produceSync(t, cl, &kgo.Record{Topic: topic, Key: []byte("a"), Value: []byte("3")})
	produceSync(t, cl, &kgo.Record{Topic: topic, Key: []byte("c"), Value: []byte("4")})
	produceSync(t, cl, &kgo.Record{Topic: topic, Key: []byte("b"), Value: []byte("5")})

	c.Compact()

	consumer := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	records := consumeN(t, consumer, 3, 5*time.Second)
	got := make(map[string]string)
	for _, r := range records {
		got[string(r.Key)] = string(r.Value)
	}
	if got["a"] != "3" || got["b"] != "5" || got["c"] != "4" {
		t.Fatalf("unexpected records after compaction: %v", got)
	}

	// Active segment: a single-batch topic should be a no-op.
	topicSingle := "compact-single"
	_, err = adm.CreateTopic(context.Background(), 1, 1, map[string]*string{
		"cleanup.policy": &compact,
	}, topicSingle)
	if err != nil {
		t.Fatal(err)
	}
	produceSync(t, cl, &kgo.Record{Topic: topicSingle, Key: []byte("only"), Value: []byte("one")})

	c.Compact()

	consumer2 := newPlainClient(t, c,
		kgo.ConsumeTopics(topicSingle),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	records2 := consumeN(t, consumer2, 1, 5*time.Second)
	if string(records2[0].Key) != "only" || string(records2[0].Value) != "one" {
		t.Fatalf("active segment record should survive compaction")
	}
}

// TestCompactTombstone verifies that tombstones (nil value) are removed
// after delete.retention.ms expires.
func TestCompactTombstone(t *testing.T) {
	t.Parallel()
	topic := "compact-tombstone"
	compact := "compact"
	zero := "0"
	c := newCluster(t, kfake.NumBrokers(1))

	cl := newPlainClient(t, c)
	adm := kadm.NewClient(cl)
	_, err := adm.CreateTopic(context.Background(), 1, 1, map[string]*string{
		"cleanup.policy":      &compact,
		"delete.retention.ms": &zero,
	}, topic)
	if err != nil {
		t.Fatal(err)
	}

	// Produce a keyed record, then a tombstone for it, then a dummy to
	// ensure the tombstone is not in the active segment.
	produceSync(t, cl, &kgo.Record{Topic: topic, Key: []byte("k"), Value: []byte("v")})
	produceSync(t, cl, &kgo.Record{Topic: topic, Key: []byte("k"), Value: nil})
	produceSync(t, cl, &kgo.Record{Topic: topic, Key: []byte("other"), Value: []byte("x")})

	c.Compact()

	// Only "other" should survive - "k" was superseded by tombstone,
	// and the tombstone itself is expired.
	consumer := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	records := consumeN(t, consumer, 1, 5*time.Second)
	if string(records[0].Key) != "other" {
		t.Fatalf("expected only 'other' to survive, got key=%s", string(records[0].Key))
	}
}

// TestCompactOffsetGaps verifies that after compaction creates gaps in offsets,
// fetching from a compacted offset skips to the next available batch.
func TestCompactOffsetGaps(t *testing.T) {
	t.Parallel()
	topic := "compact-gaps"
	compact := "compact"
	c := newCluster(t, kfake.NumBrokers(1))

	cl := newPlainClient(t, c)
	adm := kadm.NewClient(cl)
	_, err := adm.CreateTopic(context.Background(), 1, 1, map[string]*string{
		"cleanup.policy": &compact,
	}, topic)
	if err != nil {
		t.Fatal(err)
	}

	// Produce: a=old (offset 0), a=new (offset 1), b=val (offset 2, active).
	// After compaction, offset 0 is gone. Fetching from offset 0 should
	// skip to the next available batch.
	produceSync(t, cl, &kgo.Record{Topic: topic, Key: []byte("a"), Value: []byte("old")})
	produceSync(t, cl, &kgo.Record{Topic: topic, Key: []byte("a"), Value: []byte("new")})
	produceSync(t, cl, &kgo.Record{Topic: topic, Key: []byte("b"), Value: []byte("val")})

	c.Compact()

	// Consume from offset 0 - should get "a=new" (from cleaned range)
	// and "b=val" (active segment).
	consumer := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().At(0)),
	)
	records := consumeN(t, consumer, 2, 5*time.Second)
	if string(records[0].Key) != "a" || string(records[0].Value) != "new" {
		t.Fatalf("expected a=new, got %s=%s", string(records[0].Key), string(records[0].Value))
	}
}

// TestCompactControlBatch verifies control batch handling: abort markers are
// removed when no data remains for the PID, commit markers are kept when the
// PID has surviving data.
func TestCompactControlBatch(t *testing.T) {
	t.Parallel()
	topic := "compact-ctrl"
	compact := "compact"
	c := newCluster(t, kfake.NumBrokers(1))

	cl := newPlainClient(t, c)
	adm := kadm.NewClient(cl)
	_, err := adm.CreateTopic(context.Background(), 1, 1, map[string]*string{
		"cleanup.policy": &compact,
	}, topic)
	if err != nil {
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
	compact := "compact"
	backoff := "50"
	c := newCluster(t, kfake.NumBrokers(1), kfake.BrokerConfigs(map[string]string{"log.cleaner.backoff.ms": backoff}))

	cl := newPlainClient(t, c)
	adm := kadm.NewClient(cl)
	_, err := adm.CreateTopic(context.Background(), 1, 1, map[string]*string{
		"cleanup.policy": &compact,
	}, topic)
	if err != nil {
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

// TestCompactMultiRecordBatch verifies that compaction correctly rebuilds
// batches when only some records within a multi-record batch survive.
// This exercises the rebuildBatch path (re-encoding, CRC, offset deltas).
func TestCompactMultiRecordBatch(t *testing.T) {
	t.Parallel()
	topic := "compact-multi"
	compact := "compact"
	c := newCluster(t, kfake.NumBrokers(1))

	cl := newPlainClient(t, c,
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	adm := kadm.NewClient(cl)
	_, err := adm.CreateTopic(context.Background(), 1, 1, map[string]*string{
		"cleanup.policy": &compact,
	}, topic)
	if err != nil {
		t.Fatal(err)
	}

	// Produce batch 1: three records in one batch (a=old, b=keep, c=old).
	// ProduceSync with multiple records to the same partition batches them.
	produceSync(t, cl,
		&kgo.Record{Topic: topic, Partition: 0, Key: []byte("a"), Value: []byte("old")},
		&kgo.Record{Topic: topic, Partition: 0, Key: []byte("b"), Value: []byte("keep")},
		&kgo.Record{Topic: topic, Partition: 0, Key: []byte("c"), Value: []byte("old")},
	)
	// Batch 2: supersede a and c.
	produceSync(t, cl,
		&kgo.Record{Topic: topic, Partition: 0, Key: []byte("a"), Value: []byte("new")},
		&kgo.Record{Topic: topic, Partition: 0, Key: []byte("c"), Value: []byte("new")},
	)
	// Batch 3: active segment.
	produceSync(t, cl, &kgo.Record{Topic: topic, Partition: 0, Key: []byte("d"), Value: []byte("active")})

	c.Compact()

	// From batch 1, only b=keep should survive (a and c superseded).
	// Batch 2: a=new and c=new survive (latest for their keys).
	// Batch 3: d=active (active segment, untouched).
	consumer := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	records := consumeN(t, consumer, 4, 5*time.Second)
	got := make(map[string]string)
	for _, r := range records {
		got[string(r.Key)] = string(r.Value)
	}
	want := map[string]string{"a": "new", "b": "keep", "c": "new", "d": "active"}
	for k, wv := range want {
		if got[k] != wv {
			t.Fatalf("key %q: want %q, got %q (all: %v)", k, wv, got[k], got)
		}
	}
}

// TestCompactTombstoneRetained verifies that a tombstone within
// delete.retention.ms is kept during compaction (not prematurely removed).
func TestCompactTombstoneRetained(t *testing.T) {
	t.Parallel()
	topic := "compact-tombstone-retained"
	compact := "compact"
	// Default delete.retention.ms is 24h - tombstone should survive.
	c := newCluster(t, kfake.NumBrokers(1))

	cl := newPlainClient(t, c)
	adm := kadm.NewClient(cl)
	_, err := adm.CreateTopic(context.Background(), 1, 1, map[string]*string{
		"cleanup.policy": &compact,
	}, topic)
	if err != nil {
		t.Fatal(err)
	}

	// Produce a record, then a tombstone for it, then an active segment.
	produceSync(t, cl, &kgo.Record{Topic: topic, Key: []byte("k"), Value: []byte("v")})
	produceSync(t, cl, &kgo.Record{Topic: topic, Key: []byte("k"), Value: nil})
	produceSync(t, cl, &kgo.Record{Topic: topic, Key: []byte("other"), Value: []byte("x")})

	c.Compact()

	// The data record is superseded by the tombstone, but the tombstone
	// itself should survive (within 24h retention). Plus the active segment.
	consumer := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	records := consumeN(t, consumer, 2, 5*time.Second)
	got := make(map[string]string)
	for _, r := range records {
		if r.Value == nil {
			got[string(r.Key)] = "<tombstone>"
		} else {
			got[string(r.Key)] = string(r.Value)
		}
	}
	if got["k"] != "<tombstone>" {
		t.Fatalf("expected tombstone for key 'k' to survive, got: %v", got)
	}
	if got["other"] != "x" {
		t.Fatalf("expected 'other' in active segment, got: %v", got)
	}
}

// TestCompactDoubleCompaction verifies that compacting twice is safe -
// the rebuilt batches from the first compaction can be re-decoded and
// re-processed by the second.
func TestCompactDoubleCompaction(t *testing.T) {
	t.Parallel()
	topic := "compact-double"
	compact := "compact"
	c := newCluster(t, kfake.NumBrokers(1))

	cl := newPlainClient(t, c)
	adm := kadm.NewClient(cl)
	_, err := adm.CreateTopic(context.Background(), 1, 1, map[string]*string{
		"cleanup.policy": &compact,
	}, topic)
	if err != nil {
		t.Fatal(err)
	}

	// Round 1: produce duplicates, compact.
	produceSync(t, cl, &kgo.Record{Topic: topic, Key: []byte("a"), Value: []byte("v1")})
	produceSync(t, cl, &kgo.Record{Topic: topic, Key: []byte("a"), Value: []byte("v2")})
	produceSync(t, cl, &kgo.Record{Topic: topic, Key: []byte("b"), Value: []byte("v1")})

	c.Compact()

	// Round 2: produce more duplicates that supersede round 1 survivors, compact again.
	produceSync(t, cl, &kgo.Record{Topic: topic, Key: []byte("a"), Value: []byte("v3")})
	produceSync(t, cl, &kgo.Record{Topic: topic, Key: []byte("c"), Value: []byte("v1")})

	c.Compact()

	// After two compactions: a=v3 (latest), b=v1, c=v1 (active segment).
	consumer := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	records := consumeN(t, consumer, 3, 5*time.Second)
	got := make(map[string]string)
	for _, r := range records {
		got[string(r.Key)] = string(r.Value)
	}
	want := map[string]string{"a": "v3", "b": "v1", "c": "v1"}
	for k, wv := range want {
		if got[k] != wv {
			t.Fatalf("key %q: want %q, got %q (all: %v)", k, wv, got[k], got)
		}
	}
}

// TestRetentionTime verifies that retention.ms=1 causes old batches to be
// removed after ApplyRetention.
func TestRetentionTime(t *testing.T) {
	t.Parallel()
	topic := "retention-time"
	retMs := "100"
	c := newCluster(t, kfake.NumBrokers(1))

	cl := newPlainClient(t, c)
	adm := kadm.NewClient(cl)
	_, err := adm.CreateTopic(context.Background(), 1, 1, map[string]*string{
		"retention.ms": &retMs,
	}, topic)
	if err != nil {
		t.Fatal(err)
	}

	// Produce old records.
	produceSync(t, cl, &kgo.Record{Topic: topic, Value: []byte("old1")})
	produceSync(t, cl, &kgo.Record{Topic: topic, Value: []byte("old2")})

	// Let them expire.
	time.Sleep(150 * time.Millisecond)

	// Produce a new record (will not be expired).
	produceSync(t, cl, &kgo.Record{Topic: topic, Value: []byte("new")})

	c.ApplyRetention()

	// logStartOffset should have advanced past the old records.
	pi := c.PartitionInfo(topic, 0)
	if pi.LogStartOffset < 2 {
		t.Fatalf("expected logStartOffset >= 2 after retention, got %d", pi.LogStartOffset)
	}

	// Consuming from start should only get the new record.
	consumer := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	records := consumeN(t, consumer, 1, 5*time.Second)
	if string(records[0].Value) != "new" {
		t.Fatalf("expected 'new', got %q", string(records[0].Value))
	}
}

// TestRetentionBytes verifies that retention.bytes removes oldest batches
// when the partition exceeds the size limit.
func TestRetentionBytes(t *testing.T) {
	t.Parallel()
	topic := "retention-bytes"
	// A single-record batch is ~70 bytes. Set retention to 100 so only
	// the last batch survives out of three.
	retBytes := "100"
	c := newCluster(t, kfake.NumBrokers(1))

	cl := newPlainClient(t, c)
	adm := kadm.NewClient(cl)
	_, err := adm.CreateTopic(context.Background(), 1, 1, map[string]*string{
		"retention.bytes": &retBytes,
	}, topic)
	if err != nil {
		t.Fatal(err)
	}

	produceSync(t, cl, &kgo.Record{Topic: topic, Value: []byte("a")})
	produceSync(t, cl, &kgo.Record{Topic: topic, Value: []byte("b")})
	produceSync(t, cl, &kgo.Record{Topic: topic, Value: []byte("c")})

	c.ApplyRetention()

	pi := c.PartitionInfo(topic, 0)
	if pi.LogStartOffset < 2 {
		t.Fatalf("expected logStartOffset >= 2 after retention, got %d", pi.LogStartOffset)
	}

	// Consuming from start should only get the last record.
	consumer := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	records := consumeN(t, consumer, 1, 5*time.Second)
	if string(records[0].Value) != "c" {
		t.Fatalf("expected 'c', got %q", string(records[0].Value))
	}
}

// TestRetentionTicker verifies that the background ticker applies retention
// automatically when retention.ms and log.cleaner.backoff.ms are set.
// waitForStableClassicGroup polls DescribeGroups until the classic group
// is Stable with the expected member count.
func waitForStableClassicGroup(t *testing.T, adm *kadm.Client, group string, nMembers int, timeout time.Duration) kadm.DescribedGroup {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		described, err := adm.DescribeGroups(ctx, group)
		if err != nil {
			t.Fatalf("describe failed: %v", err)
		}
		dg := described[group]
		if dg.State == "Stable" && len(dg.Members) == nMembers {
			return dg
		}
		if ctx.Err() != nil {
			t.Fatalf("timeout waiting for stable classic group %q with %d members (state=%s, members=%d)", group, nMembers, dg.State, len(dg.Members))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// TestStaticMemberClassicRejoin verifies that a static member can rejoin
// a classic group using its instanceID. The instanceID is preserved across
// the session timeout so the new client can reclaim the slot.
func TestStaticMemberClassicRejoin(t *testing.T) {
	t.Parallel()
	topic := "static-classic-rejoin"
	group := "static-classic-rejoin-group"
	instanceID := "static-instance-1"

	c := newCluster(t, kfake.NumBrokers(1),
		kfake.SeedTopics(2, topic),
		kfake.BrokerConfigs(map[string]string{"group.min.session.timeout.ms": "100"}),
	)
	producer := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, 20)

	// First client with instanceID. Use a short session timeout so
	// the server removes the member quickly after close.
	cl1, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.InstanceID(instanceID),
		kgo.SessionTimeout(500*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	consumeN(t, cl1, 20, 10*time.Second)

	adm := kadm.NewClient(newPlainClient(t, c))
	waitForStableClassicGroup(t, adm, group, 1, 10*time.Second)

	// Close first client (static member - does not send leave).
	cl1.Close()

	// Wait for session timeout to expire the member.
	time.Sleep(700 * time.Millisecond)

	// Second client with the same instanceID should rejoin.
	cl2, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.InstanceID(instanceID),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl2.Close()

	dg := waitForStableClassicGroup(t, adm, group, 1, 10*time.Second)
	// Verify the member has the instanceID.
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

// TestStaticMemberClassicFencing verifies that a second classic group
// client with the same instanceID fences the first (the first gets
// FENCED_INSTANCE_ID).
func TestStaticMemberClassicFencing(t *testing.T) {
	t.Parallel()
	topic := "static-classic-fence"
	group := "static-classic-fence-group"
	instanceID := "fence-instance-1"

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(2, topic))
	producer := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, 20)

	// First client.
	cl1, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.InstanceID(instanceID),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl1.Close()
	consumeN(t, cl1, 20, 10*time.Second)
	adm := kadm.NewClient(newPlainClient(t, c))
	waitForStableClassicGroup(t, adm, group, 1, 10*time.Second)

	// Second client with the same instanceID - should fence the first.
	cl2, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.InstanceID(instanceID),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl2.Close()

	// The group should stabilize with 1 member (cl2 replaced cl1).
	dg := waitForStableClassicGroup(t, adm, group, 1, 10*time.Second)
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

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	producer := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, 10)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.InstanceID(instanceID),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	consumeN(t, cl, 10, 10*time.Second)

	adm := kadm.NewClient(newPlainClient(t, c))
	waitForStableClassicGroup(t, adm, group, 1, 10*time.Second)

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

// TestStaticMember848Leave verifies that a static member in an 848 group
// can send epoch -2 (static leave), then rejoin and get an assignment.
func TestStaticMember848Leave(t *testing.T) {
	t.Parallel()
	topic := "static-848-leave"
	group := "static-848-leave-group"
	instanceID := "static-848-instance-1"

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(2, topic))
	producer := newClient848(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, 20)

	// Consumer with instanceID.
	cl1 := newClient848(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.InstanceID(instanceID),
	)
	consumeN(t, cl1, 20, 10*time.Second)
	adm := kadm.NewClient(newClient848(t, c))
	waitForStableGroup(t, adm, group, 1, 10*time.Second)

	// Close client - with instanceID, 848 sends epoch -2.
	cl1.Close()

	// Wait a bit for the leave to be processed.
	time.Sleep(500 * time.Millisecond)

	// Rejoin with a new client using the same instanceID.
	cl2 := newClient848(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.InstanceID(instanceID),
	)
	_ = cl2

	dg := waitForStableGroup(t, adm, group, 1, 10*time.Second)
	if totalAssignedPartitions(dg) != 2 {
		t.Fatalf("expected 2 partitions assigned after rejoin, got %d", totalAssignedPartitions(dg))
	}
}

// TestStaticMember848SessionTimeout verifies that a static 848 member
// that times out can rejoin and reclaim its assignment slot.
func TestStaticMember848SessionTimeout(t *testing.T) {
	t.Parallel()
	topic := "static-848-timeout"
	group := "static-848-timeout-group"
	instanceID := "static-848-timeout-inst"

	c := newCluster(t, kfake.NumBrokers(1),
		kfake.SeedTopics(2, topic),
		kfake.BrokerConfigs(map[string]string{
			"group.consumer.session.timeout.ms": "500",
		}),
	)
	producer := newClient848(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, 20)

	cl1 := newClient848(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.InstanceID(instanceID),
	)
	consumeN(t, cl1, 20, 10*time.Second)
	adm := kadm.NewClient(newClient848(t, c))
	waitForStableGroup(t, adm, group, 1, 10*time.Second)

	// Force-close the client so it cannot heartbeat - triggers session timeout.
	cl1.Close()

	// Wait for the session timeout to expire (500ms configured above).
	time.Sleep(700 * time.Millisecond)

	// Rejoin with same instanceID.
	cl2 := newClient848(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.InstanceID(instanceID),
	)
	_ = cl2

	dg := waitForStableGroup(t, adm, group, 1, 10*time.Second)
	if totalAssignedPartitions(dg) != 2 {
		t.Fatalf("expected 2 partitions assigned after timeout rejoin, got %d", totalAssignedPartitions(dg))
	}
}

func TestRetentionTicker(t *testing.T) {
	t.Parallel()
	topic := "retention-ticker"
	retMs := "1"
	backoff := "50"
	c := newCluster(t, kfake.NumBrokers(1), kfake.BrokerConfigs(map[string]string{"log.cleaner.backoff.ms": backoff}))

	cl := newPlainClient(t, c)
	adm := kadm.NewClient(cl)
	_, err := adm.CreateTopic(context.Background(), 1, 1, map[string]*string{
		"retention.ms": &retMs,
	}, topic)
	if err != nil {
		t.Fatal(err)
	}

	produceSync(t, cl, &kgo.Record{Topic: topic, Value: []byte("old")})
	produceSync(t, cl, &kgo.Record{Topic: topic, Value: []byte("keep")})

	// Wait for the ticker to fire and apply retention.
	time.Sleep(200 * time.Millisecond)

	pi := c.PartitionInfo(topic, 0)
	if pi.LogStartOffset < 1 {
		t.Fatalf("expected logStartOffset >= 1 after ticker-driven retention, got %d", pi.LogStartOffset)
	}
}

// Test848FetchOffsetsStaleEpochRetry verifies that the kgo client retries
// OffsetFetch when the server returns STALE_MEMBER_EPOCH at the group
// level. This can happen when the member epoch changes between the
// heartbeat that assigned partitions and the subsequent OffsetFetch.
func Test848FetchOffsetsStaleEpochRetry(t *testing.T) {
	t.Parallel()
	topic := "t-stale-epoch"
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))

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

func TestElectLeaders(t *testing.T) {
	t.Parallel()
	topic := "t-elect-leaders"

	c := newCluster(t, kfake.NumBrokers(3), kfake.SeedTopics(3, topic))
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

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, "t-incr-cfg"))
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

	c := newCluster(t, kfake.NumBrokers(1))
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
	c2 := newCluster(t, kfake.NumBrokers(1), kfake.MaxVersions(v))
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
