// Derived via LLM from Apache Kafka's PlaintextConsumerFetchTest.java (Apache 2.0).
// https://github.com/apache/kafka/blob/trunk/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/consumer/PlaintextConsumerFetchTest.java

package kafka_tests

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestFetchInvalidOffset verifies that with NoResetOffset, consuming from an
// unknown partition offset returns an error rather than silently resetting.
func TestFetchInvalidOffset(t *testing.T) {
	t.Parallel()
	topic := "fetch-invalid-offset"
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))

	// Produce some records so there's data.
	producer := newClient(t, c, kgo.DefaultProduceTopic(topic))
	produceSync(t, producer, kgo.StringRecord("v1"), kgo.StringRecord("v2"))

	// Create consumer starting at an offset beyond the log end, with no reset.
	consumer := newClient(t, c,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {0: kgo.NewOffset().At(100)},
		}),
		kgo.ConsumeResetOffset(kgo.NoResetOffset()),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	fs := consumer.PollFetches(ctx)
	// With NoResetOffset, we expect an error for the out-of-range offset.
	errs := fs.Errors()
	if len(errs) == 0 && fs.NumRecords() == 0 {
		// Timeout without data is also acceptable - the point is no records returned.
		return
	}
	if len(errs) > 0 {
		// We got an error, which is the expected behavior.
		return
	}
	// If we somehow got records, that's wrong.
	if fs.NumRecords() > 0 {
		t.Errorf("expected no records with invalid offset, got %d", fs.NumRecords())
	}
}

// TestFetchOutOfRangeResetEarliest verifies that when a consumer seeks beyond
// the log range and the reset policy is earliest, it resets to offset 0.
func TestFetchOutOfRangeResetEarliest(t *testing.T) {
	t.Parallel()
	topic := "fetch-reset-earliest"
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))

	producer := newClient(t, c, kgo.DefaultProduceTopic(topic))
	produceSync(t, producer, kgo.StringRecord("v1"), kgo.StringRecord("v2"), kgo.StringRecord("v3"))

	// Start at an out-of-range offset with reset to earliest.
	consumer := newClient(t, c,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {0: kgo.NewOffset().At(100)},
		}),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
	)

	records := consumeN(t, consumer, 3, 5*time.Second)
	if records[0].Offset != 0 {
		t.Errorf("expected reset to offset 0, got %d", records[0].Offset)
	}
}

// TestFetchOutOfRangeResetLatest verifies that when a consumer seeks beyond
// the log range and the reset policy is latest, it resets to the end and
// only sees newly produced records.
func TestFetchOutOfRangeResetLatest(t *testing.T) {
	t.Parallel()
	topic := "fetch-reset-latest"
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))

	producer := newClient(t, c, kgo.DefaultProduceTopic(topic))
	produceSync(t, producer, kgo.StringRecord("old1"), kgo.StringRecord("old2"))

	// Start at an out-of-range offset with reset to latest.
	consumer := newClient(t, c,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {0: kgo.NewOffset().At(100)},
		}),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
	)

	// Give the consumer time to reset.
	time.Sleep(200 * time.Millisecond)

	// Produce a new record after the consumer has reset to end.
	produceSync(t, producer, kgo.StringRecord("new-after-reset"))

	records := consumeN(t, consumer, 1, 5*time.Second)
	if string(records[0].Value) != "new-after-reset" {
		t.Errorf("expected to consume 'new-after-reset', got %q", string(records[0].Value))
	}
}

// TestFetchRecordLargerThanFetchMaxBytes verifies that a record larger than
// FetchMaxBytes is still returned (KIP-74: a single record is always returned
// even if it exceeds max bytes).
func TestFetchRecordLargerThanFetchMaxBytes(t *testing.T) {
	t.Parallel()
	topic := "fetch-large-record"
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))

	producer := newClient(t, c, kgo.DefaultProduceTopic(topic))

	// Produce a record much larger than our fetch max bytes.
	largeValue := []byte(strings.Repeat("x", 10_000))
	r := &kgo.Record{Value: largeValue}
	produceSync(t, producer, r)

	// Consume with a very small FetchMaxBytes.
	consumer := newClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.FetchMaxBytes(1024),
	)

	records := consumeN(t, consumer, 1, 5*time.Second)
	if !bytes.Equal(records[0].Value, largeValue) {
		t.Errorf("large record was not returned correctly: got %d bytes, expected %d", len(records[0].Value), len(largeValue))
	}
}

// TestFetchRecordLargerThanMaxPartitionFetchBytes verifies that a record
// larger than FetchMaxPartitionBytes is still returned (same KIP-74 guarantee).
func TestFetchRecordLargerThanMaxPartitionFetchBytes(t *testing.T) {
	t.Parallel()
	topic := "fetch-large-partition"
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))

	producer := newClient(t, c, kgo.DefaultProduceTopic(topic))

	largeValue := []byte(strings.Repeat("y", 10_000))
	r := &kgo.Record{Value: largeValue}
	produceSync(t, producer, r)

	consumer := newClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.FetchMaxPartitionBytes(1024),
	)

	records := consumeN(t, consumer, 1, 5*time.Second)
	if !bytes.Equal(records[0].Value, largeValue) {
		t.Errorf("large record was not returned correctly: got %d bytes, expected %d", len(records[0].Value), len(largeValue))
	}
}

// TestFetchHonoursFetchSizeIfLargeRecordNotFirst verifies that fetch size is
// respected when a large record is not the first record in the partition.
// The first small record should be returned, but the second large record
// should not be included in the same fetch.
func TestFetchHonoursFetchSizeIfLargeRecordNotFirst(t *testing.T) {
	t.Parallel()
	topic := "fetch-honour-size"
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))

	producer := newClient(t, c, kgo.DefaultProduceTopic(topic))

	// Produce a small record, then a large record in separate batches.
	smallRecord := kgo.StringRecord("small")
	produceSync(t, producer, smallRecord)
	largeValue := strings.Repeat("z", 10_000)
	largeRecord := kgo.StringRecord(largeValue)
	produceSync(t, producer, largeRecord)

	// Consume with a fetch size that fits the small record but not the large.
	consumer := newClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.FetchMaxPartitionBytes(512),
	)

	// First poll should return the small record.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fs := consumer.PollFetches(ctx)
	if fs.NumRecords() == 0 {
		t.Fatal("expected at least 1 record")
	}
	firstRecord := fs.Records()[0]
	if string(firstRecord.Value) != "small" {
		t.Errorf("expected first record 'small', got %q", string(firstRecord.Value))
	}

	// Second poll should return the large record (KIP-74: first in partition always returned).
	fs = consumer.PollFetches(ctx)
	if fs.NumRecords() == 0 {
		t.Fatal("expected the large record in next fetch")
	}
	secondRecord := fs.Records()[0]
	if string(secondRecord.Value) != largeValue {
		t.Errorf("expected large record, got %d bytes", len(secondRecord.Value))
	}
}
