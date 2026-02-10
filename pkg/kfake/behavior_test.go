package kfake_test

import (
	"context"
	"errors"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
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
