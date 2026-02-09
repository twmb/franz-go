// Derived via LLM from Apache Kafka's PlaintextConsumerCommitTest.java (Apache 2.0).
// https://github.com/apache/kafka/blob/trunk/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/consumer/PlaintextConsumerCommitTest.java

package kafka_tests

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestAutoCommitOnClose verifies that offsets are committed when a group
// consumer explicitly commits before closing.
func TestAutoCommitOnClose(t *testing.T) {
	t.Parallel()
	topic := "commit-auto-close"
	group := "commit-auto-close-group"
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))

	// Produce some records.
	producer := newClient(t, c, kgo.DefaultProduceTopic(topic))
	for i := range 5 {
		produceSync(t, producer, kgo.StringRecord("v-"+strconv.Itoa(i)))
	}

	// Create a group consumer, consume all records, explicitly commit, then close.
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var consumed int
	for consumed < 5 {
		fs := cl.PollFetches(ctx)
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("consume errors: %v", errs)
		}
		consumed += fs.NumRecords()
	}

	// Explicitly commit.
	if err := cl.CommitUncommittedOffsets(ctx); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// Close the consumer.
	cl.Close()

	// Verify the committed offset using a new admin client.
	admCl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer admCl.Close()
	adm := kadm.NewClient(admCl)

	fetched, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatalf("fetch offsets failed: %v", err)
	}
	o, ok := fetched.Lookup(topic, 0)
	if !ok {
		t.Fatal("no committed offset found after close")
	}
	if o.At != 5 {
		t.Errorf("expected committed offset 5, got %d", o.At)
	}
}

// TestCommitMetadata verifies that offset commit metadata round-trips correctly.
func TestCommitMetadata(t *testing.T) {
	t.Parallel()
	topic := "commit-metadata"
	group := "commit-metadata-group"
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))

	adm := newAdminClient(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	metadata := "my-custom-metadata"
	offsets := kadm.Offsets{}
	offsets.Add(kadm.Offset{
		Topic:     topic,
		Partition: 0,
		At:        5,
		Metadata:  metadata,
	})
	_, err := adm.CommitOffsets(ctx, group, offsets)
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	fetched, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatalf("fetch offsets failed: %v", err)
	}
	o, ok := fetched.Lookup(topic, 0)
	if !ok {
		t.Fatal("committed offset not found")
	}
	if o.Metadata != metadata {
		t.Errorf("expected metadata %q, got %q", metadata, o.Metadata)
	}
}

// TestNoCommittedOffsets verifies that fetching offsets for a group with no
// commits returns no data (or an error indicating the group doesn't exist).
func TestNoCommittedOffsets(t *testing.T) {
	t.Parallel()
	topic := "commit-none"
	group := "commit-none-group"
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))

	adm := newAdminClient(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fetched, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		// GROUP_ID_NOT_FOUND is expected - the group doesn't exist.
		return
	}
	_, ok := fetched.Lookup(topic, 0)
	if ok {
		t.Error("expected no committed offsets for empty group")
	}
}

// TestAsyncCommit verifies that async commit (via a separate goroutine)
// eventually commits offsets.
func TestAsyncCommit(t *testing.T) {
	t.Parallel()
	topic := "commit-async"
	group := "commit-async-group"
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))

	producer := newClient(t, c, kgo.DefaultProduceTopic(topic))
	for i := range 5 {
		produceSync(t, producer, kgo.StringRecord("v-"+strconv.Itoa(i)))
	}

	// Consume all records with group consumer.
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var consumed int
	for consumed < 5 {
		fs := cl.PollFetches(ctx)
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("consume errors: %v", errs)
		}
		consumed += fs.NumRecords()
	}

	// Async commit: commit in a goroutine and wait for it.
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := cl.CommitUncommittedOffsets(ctx); err != nil {
			t.Errorf("async commit failed: %v", err)
		}
	}()
	wg.Wait()

	// Verify committed offset.
	adm := kadm.NewClient(cl)
	fetched, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatalf("fetch offsets failed: %v", err)
	}
	o, ok := fetched.Lookup(topic, 0)
	if !ok {
		t.Fatal("committed offset not found")
	}
	if o.At != 5 {
		t.Errorf("expected committed offset 5, got %d", o.At)
	}
}

// TestCommitSpecifiedOffsets verifies committing explicit offset values.
func TestCommitSpecifiedOffsets(t *testing.T) {
	t.Parallel()
	topic := "commit-specified"
	group := "commit-specified-group"
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))

	adm := newAdminClient(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Commit a specific offset.
	offsets := kadm.Offsets{}
	offsets.Add(kadm.Offset{Topic: topic, Partition: 0, At: 3})
	_, err := adm.CommitOffsets(ctx, group, offsets)
	if err != nil {
		t.Fatalf("commit 3 failed: %v", err)
	}

	fetched, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatalf("fetch offsets failed: %v", err)
	}
	o, ok := fetched.Lookup(topic, 0)
	if !ok {
		t.Fatal("committed offset not found")
	}
	if o.At != 3 {
		t.Errorf("expected offset 3, got %d", o.At)
	}

	// Update to a different offset.
	offsets2 := kadm.Offsets{}
	offsets2.Add(kadm.Offset{Topic: topic, Partition: 0, At: 7})
	_, err = adm.CommitOffsets(ctx, group, offsets2)
	if err != nil {
		t.Fatalf("commit 7 failed: %v", err)
	}

	fetched, err = adm.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatalf("fetch offsets failed: %v", err)
	}
	o, ok = fetched.Lookup(topic, 0)
	if !ok {
		t.Fatal("committed offset not found")
	}
	if o.At != 7 {
		t.Errorf("expected offset 7, got %d", o.At)
	}
}

// TestPositionAndCommit verifies the interaction between consuming position
// and committed offsets.
func TestPositionAndCommit(t *testing.T) {
	t.Parallel()
	topic := "commit-position"
	group := "commit-position-group"
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))

	producer := newClient(t, c, kgo.DefaultProduceTopic(topic))
	for i := range 10 {
		produceSync(t, producer, kgo.StringRecord("v-"+strconv.Itoa(i)))
	}

	// Create a group consumer.
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Consume some records (not all).
	var consumed int
	for consumed < 5 {
		fs := cl.PollFetches(ctx)
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("consume errors: %v", errs)
		}
		consumed += fs.NumRecords()
	}

	// Commit what we've consumed.
	if err := cl.CommitUncommittedOffsets(ctx); err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// Verify committed offset matches what we consumed.
	adm := kadm.NewClient(cl)
	fetched, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatalf("fetch offsets failed: %v", err)
	}
	o, ok := fetched.Lookup(topic, 0)
	if !ok {
		t.Fatal("committed offset not found")
	}
	if o.At < 5 {
		t.Errorf("expected committed offset >= 5, got %d", o.At)
	}
}
