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

// Test848ThreeConsumersFairDistribution verifies that partitions are evenly
// distributed across three consumers. Consumers are staggered to allow
// the group to stabilize between joins.
func Test848ThreeConsumersFairDistribution(t *testing.T) {
	t.Parallel()
	topic := "t848-fair"
	group := "g848-fair"
	nPartitions := 9

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(int32(nPartitions), topic))
	producer := newClient848(t, c, kgo.DefaultProduceTopic(topic))

	makeConsumer := func() *kgo.Client {
		return newClient848(t, c,
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup(group),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		)
	}

	// Stagger consumers to let the group stabilize between joins.
	produceNStrings(t, producer, topic, 30)
	c1 := makeConsumer()
	_ = consumeN(t, c1, 30, 10*time.Second)

	c2 := makeConsumer()
	produceNStrings(t, producer, topic, 30)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	got := 0
	for got < 30 {
		for _, cl := range []*kgo.Client{c1, c2} {
			fs := cl.PollRecords(ctx, 100)
			fs.EachRecord(func(*kgo.Record) { got++ })
			if got >= 30 {
				break
			}
		}
		if got < 30 && ctx.Err() != nil {
			t.Fatalf("timeout waiting for c1+c2: got %d/30", got)
		}
	}

	c3 := makeConsumer()
	produceNStrings(t, producer, topic, 90)

	// All three consumers should collectively consume the 90 records.
	ctx2, cancel2 := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel2()
	got = 0
	for got < 90 {
		for _, cl := range []*kgo.Client{c1, c2, c3} {
			fs := cl.PollRecords(ctx2, 100)
			fs.EachRecord(func(*kgo.Record) { got++ })
			if got >= 90 {
				break
			}
		}
		if got < 90 && ctx2.Err() != nil {
			t.Fatalf("timeout waiting for c1+c2+c3: got %d/90", got)
		}
	}
}

// Test848AllMembersLeave verifies that when all members leave a consumer
// group, the group transitions to empty and a new joiner gets the full
// assignment across all partitions.
func Test848AllMembersLeave(t *testing.T) {
	t.Parallel()
	topic := "t848-allleave"
	group := "g848-allleave"
	nRecords := 20

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(3, topic))
	producer := newClient848(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, nRecords)

	// Two consumers join the group and consume all records.
	makeConsumer := func() *kgo.Client {
		return newClient848(t, c,
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup(group),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		)
	}
	c1 := makeConsumer()
	c2 := makeConsumer()

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
			t.Fatalf("timeout consuming initial records: got %d/%d", got, nRecords)
		}
	}

	// Both consumers leave.
	c1.Close()
	c2.Close()

	// Produce more records.
	moreRecords := 10
	produceNStrings(t, producer, topic, moreRecords)

	// A new consumer should get the full assignment (all 3 partitions).
	c3 := makeConsumer()
	records := consumeN(t, c3, moreRecords, 10*time.Second)
	if len(records) < moreRecords {
		t.Fatalf("expected at least %d records from new consumer, got %d", moreRecords, len(records))
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
	pollCtx, pollCancel := context.WithTimeout(ctx, time.Second)
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
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
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

func stringp(s string) *string { return &s }
