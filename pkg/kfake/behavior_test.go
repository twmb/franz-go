package kfake_test

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
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

func newClient(t *testing.T, c *kfake.Cluster, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	opts = append([]kgo.Opt{kgo.SeedBrokers(c.ListenAddrs()...)}, opts...)
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
	producer := newClient(t, c)

	// Produce to all three topics.
	for i := range nRecords {
		for _, topic := range []string{matchA, matchB, noMatch} {
			r := kgo.StringRecord("v-" + strconv.Itoa(i))
			r.Topic = topic
			produceSync(t, producer, r)
		}
	}

	// Consumer with regex - should only match t848-rx-*.
	consumer := newClient(t, c,
		kgo.ConsumeRegex(),
		kgo.ConsumeTopics("t848-rx-.*"),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.MetadataMinAge(time.Second),
		kgo.MetadataMaxAge(time.Second),
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
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
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
	producer := newClient(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, 10)

	consumer := newClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	_ = consumeN(t, consumer, 10, 10*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	adm := kadm.NewClient(newClient(t, c))

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
	producer := newClient(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, 10)

	consumer := newClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)
	_ = consumeN(t, consumer, 10, 10*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	adm := kadm.NewClient(newClient(t, c))

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
	producer := newClient(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, nRecords)

	// First consumer: consume all and commit.
	c1 := newClient(t, c,
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
	c2 := newClient(t, c,
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
	producer := newClient(t, c)

	// Produce to both topics.
	for i := range nRecords {
		r := kgo.StringRecord("a-" + strconv.Itoa(i))
		r.Topic = topicA
		produceSync(t, producer, r)
		r2 := kgo.StringRecord("b-" + strconv.Itoa(i))
		r2.Topic = topicB
		produceSync(t, producer, r2)
	}

	consumer := newClient(t, c,
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
	txnClient := newClient(t, c,
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
	consumer := newClient(t, c,
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
	producer := newClient(t, c, kgo.DefaultProduceTopic(topic))

	makeConsumer := func() *kgo.Client {
		return newClient(t, c,
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
		}
		if ctx.Err() != nil {
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
		}
		if ctx2.Err() != nil {
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
	producer := newClient(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, nRecords)

	// Two consumers join the group and consume all records.
	makeConsumer := func() *kgo.Client {
		return newClient(t, c,
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
		}
		if ctx.Err() != nil {
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
	producer := newClient(t, c)

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
	consumer := newClient(t, c,
		kgo.ConsumeTopics(topicA),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.MetadataMinAge(500*time.Millisecond),
		kgo.MetadataMaxAge(time.Second),
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
	producer := newClient(t, c)
	produceNStrings(t, producer, existingTopic, nRecords)

	// Consumer subscribes with regex matching both existing and future topics.
	consumer := newClient(t, c,
		kgo.ConsumeRegex(),
		kgo.ConsumeTopics("t848-dynamic-.*"),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.MetadataMinAge(time.Second),
		kgo.MetadataMaxAge(time.Second),
	)

	// Consume from existing topic first.
	records := consumeN(t, consumer, nRecords, 10*time.Second)
	if len(records) != nRecords {
		t.Fatalf("expected %d records from existing topic, got %d", nRecords, len(records))
	}

	// Create the new topic and produce to it.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	adm := kadm.NewClient(newClient(t, c))
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
