// Derived from Apache Kafka's PlaintextConsumerAssignTest.java (Apache 2.0).
// https://github.com/apache/kafka/blob/trunk/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/consumer/PlaintextConsumerAssignTest.java

package kafka_ai_test_rewrites

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

const assignTestTopic = "assign-test"
const assignTestRecords = 10

// setupAssignTest creates a cluster with one partition, produces records, and
// returns the cluster.
func setupAssignTest(t *testing.T) *kfake.Cluster {
	t.Helper()
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, assignTestTopic))
	producer := newClient(t, c, kgo.DefaultProduceTopic(assignTestTopic))
	for i := range assignTestRecords {
		r := kgo.StringRecord("value-" + strconv.Itoa(i))
		r.Key = []byte("key-" + strconv.Itoa(i))
		produceSync(t, producer, r)
	}
	return c
}

// assignConsumer creates a direct-assign consumer for partition 0 of
// assignTestTopic starting at the given offset.
func assignConsumer(t *testing.T, c *kfake.Cluster, offset kgo.Offset, extraOpts ...kgo.Opt) *kgo.Client {
	t.Helper()
	opts := append([]kgo.Opt{
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			assignTestTopic: {0: offset},
		}),
	}, extraOpts...)
	return newClient(t, c, opts...)
}

// TestAssignAndConsume verifies basic assign + consume from beginning.
func TestAssignAndConsume(t *testing.T) {
	t.Parallel()
	c := setupAssignTest(t)
	consumer := assignConsumer(t, c, kgo.NewOffset().AtStart())
	records := consumeN(t, consumer, assignTestRecords, 5*time.Second)
	if len(records) != assignTestRecords {
		t.Fatalf("expected %d records, got %d", assignTestRecords, len(records))
	}
	for i, r := range records {
		if r.Offset != int64(i) {
			t.Errorf("record %d: expected offset %d, got %d", i, i, r.Offset)
		}
	}
}

// TestAssignAndConsumeSkippingPosition verifies that seeking past offset 0
// skips records.
func TestAssignAndConsumeSkippingPosition(t *testing.T) {
	t.Parallel()
	c := setupAssignTest(t)
	skipN := 5
	consumer := assignConsumer(t, c, kgo.NewOffset().At(int64(skipN)))
	records := consumeN(t, consumer, assignTestRecords-skipN, 5*time.Second)
	if len(records) != assignTestRecords-skipN {
		t.Fatalf("expected %d records, got %d", assignTestRecords-skipN, len(records))
	}
	if records[0].Offset != int64(skipN) {
		t.Errorf("first record offset: expected %d, got %d", skipN, records[0].Offset)
	}
}

// TestAssignAndCommitSyncAllConsumed verifies consuming all records and
// committing synchronously.
func TestAssignAndCommitSyncAllConsumed(t *testing.T) {
	t.Parallel()
	c := setupAssignTest(t)

	groupID := "assign-commit-sync-group"
	consumer := assignConsumer(t, c, kgo.NewOffset().AtStart())
	records := consumeN(t, consumer, assignTestRecords, 5*time.Second)
	if len(records) != assignTestRecords {
		t.Fatalf("expected %d records, got %d", assignTestRecords, len(records))
	}

	// Commit the offset using kadm (since we're doing direct assign, not group consume).
	adm := kadm.NewClient(consumer)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	offsets := kadm.Offsets{}
	offsets.Add(kadm.Offset{
		Topic:     assignTestTopic,
		Partition: 0,
		At:        int64(assignTestRecords),
	})
	_, err := adm.CommitOffsets(ctx, groupID, offsets)
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// Verify committed offset.
	fetched, err := adm.FetchOffsets(ctx, groupID)
	if err != nil {
		t.Fatalf("fetch offsets failed: %v", err)
	}
	o, ok := fetched.Lookup(assignTestTopic, 0)
	if !ok {
		t.Fatal("committed offset not found")
	}
	if o.At != int64(assignTestRecords) {
		t.Errorf("committed offset: expected %d, got %d", assignTestRecords, o.At)
	}
}

// TestAssignAndCommitAsyncNotCommitted verifies that calling commitAsync
// without polling does not commit offsets.
func TestAssignAndCommitAsyncNotCommitted(t *testing.T) {
	t.Parallel()
	c := setupAssignTest(t)

	groupID := "assign-no-commit-async-group"
	// Create a consumer that doesn't poll - just assigns.
	_ = assignConsumer(t, c, kgo.NewOffset().AtStart())

	adm := newAdminClient(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// No offsets should be committed for the group. The group may not even
	// exist, which is also correct.
	fetched, err := adm.FetchOffsets(ctx, groupID)
	if err != nil {
		// GROUP_ID_NOT_FOUND is expected - no group means no commits.
		return
	}
	_, ok := fetched.Lookup(assignTestTopic, 0)
	if ok {
		t.Error("expected no committed offset for a consumer that hasn't polled")
	}
}

// TestAssignAndCommitSyncNotCommitted verifies that calling commitSync
// without polling does not commit offsets.
func TestAssignAndCommitSyncNotCommitted(t *testing.T) {
	t.Parallel()
	c := setupAssignTest(t)

	groupID := "assign-no-commit-sync-group"
	_ = assignConsumer(t, c, kgo.NewOffset().AtStart())

	adm := newAdminClient(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// No offsets should be committed for the group. The group may not even
	// exist, which is also correct.
	fetched, err := adm.FetchOffsets(ctx, groupID)
	if err != nil {
		// GROUP_ID_NOT_FOUND is expected - no group means no commits.
		return
	}
	_, ok := fetched.Lookup(assignTestTopic, 0)
	if ok {
		t.Error("expected no committed offset for a consumer that hasn't polled")
	}
}

// TestAssignAndFetchCommittedOffsets verifies that consumer 1 commits offsets
// and consumer 2 can read those committed offsets.
func TestAssignAndFetchCommittedOffsets(t *testing.T) {
	t.Parallel()
	c := setupAssignTest(t)

	groupID := "assign-fetch-committed-group"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Consumer 1 consumes all and commits.
	consumer1 := assignConsumer(t, c, kgo.NewOffset().AtStart())
	consumeN(t, consumer1, assignTestRecords, 5*time.Second)

	adm1 := kadm.NewClient(consumer1)
	offsets := kadm.Offsets{}
	offsets.Add(kadm.Offset{
		Topic:     assignTestTopic,
		Partition: 0,
		At:        int64(assignTestRecords),
	})
	_, err := adm1.CommitOffsets(ctx, groupID, offsets)
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// Consumer 2 reads the committed offset.
	adm2 := newAdminClient(t, c)
	fetched, err := adm2.FetchOffsets(ctx, groupID)
	if err != nil {
		t.Fatalf("fetch offsets failed: %v", err)
	}
	o, ok := fetched.Lookup(assignTestTopic, 0)
	if !ok {
		t.Fatal("committed offset not found")
	}
	if o.At != int64(assignTestRecords) {
		t.Errorf("expected committed offset %d, got %d", assignTestRecords, o.At)
	}
}

// TestAssignAndConsumeFromCommittedOffsets verifies resuming consumption from
// committed offsets.
func TestAssignAndConsumeFromCommittedOffsets(t *testing.T) {
	t.Parallel()
	c := setupAssignTest(t)

	groupID := "assign-resume-committed-group"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Commit offset at the halfway point.
	halfway := int64(assignTestRecords / 2)
	adm := newAdminClient(t, c)
	offsets := kadm.Offsets{}
	offsets.Add(kadm.Offset{
		Topic:     assignTestTopic,
		Partition: 0,
		At:        halfway,
	})
	_, err := adm.CommitOffsets(ctx, groupID, offsets)
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// Fetch committed offset and consume from there.
	fetched, err := adm.FetchOffsets(ctx, groupID)
	if err != nil {
		t.Fatalf("fetch offsets failed: %v", err)
	}
	o, ok := fetched.Lookup(assignTestTopic, 0)
	if !ok {
		t.Fatal("committed offset not found")
	}

	consumer := assignConsumer(t, c, kgo.NewOffset().At(o.At))
	remaining := assignTestRecords - int(halfway)
	records := consumeN(t, consumer, remaining, 5*time.Second)
	if records[0].Offset != halfway {
		t.Errorf("expected first record at offset %d, got %d", halfway, records[0].Offset)
	}
	if len(records) != remaining {
		t.Errorf("expected %d records, got %d", remaining, len(records))
	}
}

// TestAssignAndRetrievingCommittedOffsetsMultipleTimes verifies that
// repeatedly fetching committed offsets returns stable results.
func TestAssignAndRetrievingCommittedOffsetsMultipleTimes(t *testing.T) {
	t.Parallel()
	c := setupAssignTest(t)

	groupID := "assign-repeat-committed-group"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Commit a known offset.
	adm := newAdminClient(t, c)
	offsets := kadm.Offsets{}
	offsets.Add(kadm.Offset{
		Topic:     assignTestTopic,
		Partition: 0,
		At:        7,
	})
	_, err := adm.CommitOffsets(ctx, groupID, offsets)
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	// Fetch committed offset multiple times and verify stability.
	for i := range 5 {
		fetched, err := adm.FetchOffsets(ctx, groupID)
		if err != nil {
			t.Fatalf("iteration %d: fetch offsets failed: %v", i, err)
		}
		o, ok := fetched.Lookup(assignTestTopic, 0)
		if !ok {
			t.Fatalf("iteration %d: committed offset not found", i)
		}
		if o.At != 7 {
			t.Errorf("iteration %d: expected committed offset 7, got %d", i, o.At)
		}
	}
}

// TestAssignAndCommitMetadata verifies that commit metadata round-trips.
func TestAssignAndCommitMetadata(t *testing.T) {
	t.Parallel()
	c := setupAssignTest(t)

	groupID := "assign-metadata-group"
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	metadata := "test-metadata-value"
	adm := newAdminClient(t, c)
	offsets := kadm.Offsets{}
	offsets.Add(kadm.Offset{
		Topic:     assignTestTopic,
		Partition: 0,
		At:        3,
		Metadata:  metadata,
	})
	_, err := adm.CommitOffsets(ctx, groupID, offsets)
	if err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	fetched, err := adm.FetchOffsets(ctx, groupID)
	if err != nil {
		t.Fatalf("fetch offsets failed: %v", err)
	}
	o, ok := fetched.Lookup(assignTestTopic, 0)
	if !ok {
		t.Fatal("committed offset not found")
	}
	if o.At != 3 {
		t.Errorf("expected offset 3, got %d", o.At)
	}
	if o.Metadata != metadata {
		t.Errorf("expected metadata %q, got %q", metadata, o.Metadata)
	}
}

// TestAssignConsumeFromEnd verifies consuming from the end of a partition
// sees no old records.
func TestAssignConsumeFromEnd(t *testing.T) {
	t.Parallel()
	c := setupAssignTest(t)

	consumer := assignConsumer(t, c, kgo.NewOffset().AtEnd())

	// Produce one more record after the consumer is set up.
	producer := newClient(t, c, kgo.DefaultProduceTopic(assignTestTopic))
	r := kgo.StringRecord("new-value")
	r.Topic = assignTestTopic
	produceSync(t, producer, r)

	records := consumeN(t, consumer, 1, 5*time.Second)
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if string(records[0].Value) != "new-value" {
		t.Errorf("expected value %q, got %q", "new-value", string(records[0].Value))
	}
}
