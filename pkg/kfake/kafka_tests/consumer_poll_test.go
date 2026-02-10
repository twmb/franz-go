// Derived via LLM from Apache Kafka's PlaintextConsumerPollTest.java (Apache 2.0).
// https://github.com/apache/kafka/blob/trunk/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/consumer/PlaintextConsumerPollTest.java

package kafka_tests

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestMaxPollRecords verifies that PollRecords respects the maxPollRecords
// limit.
func TestMaxPollRecords(t *testing.T) {
	t.Parallel()
	topic := "poll-max-records"
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))

	producer := newClient848(t, c, kgo.DefaultProduceTopic(topic))
	totalRecords := 20
	for i := range totalRecords {
		produceSync(t, producer, kgo.StringRecord("v-"+strconv.Itoa(i)))
	}

	consumer := newClient848(t, c,
		kgo.ConsumeTopics(topic),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	maxPoll := 5
	var totalConsumed int
	for totalConsumed < totalRecords {
		fs := consumer.PollRecords(ctx, maxPoll)
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("poll errors: %v", errs)
		}
		n := fs.NumRecords()
		if n > maxPoll {
			t.Errorf("PollRecords returned %d records, exceeding maxPollRecords %d", n, maxPoll)
		}
		totalConsumed += n
	}
	if totalConsumed != totalRecords {
		t.Errorf("expected to consume %d records total, got %d", totalRecords, totalConsumed)
	}
}

// TestPollEventuallyReturnsRecordsWithZeroTimeout verifies that polling with a
// very short (effectively zero) timeout eventually returns all data.
func TestPollEventuallyReturnsRecordsWithZeroTimeout(t *testing.T) {
	t.Parallel()
	topic := "poll-zero-timeout"
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))

	producer := newClient848(t, c, kgo.DefaultProduceTopic(topic))
	totalRecords := 10
	for i := range totalRecords {
		produceSync(t, producer, kgo.StringRecord("v-"+strconv.Itoa(i)))
	}

	consumer := newClient848(t, c,
		kgo.ConsumeTopics(topic),
	)

	deadline := time.Now().Add(10 * time.Second)
	var totalConsumed int
	for totalConsumed < totalRecords && time.Now().Before(deadline) {
		// Use a very short timeout to simulate poll(0).
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
		fs := consumer.PollFetches(ctx)
		cancel()
		totalConsumed += fs.NumRecords()
	}
	if totalConsumed < totalRecords {
		t.Errorf("expected to eventually consume %d records, got %d", totalRecords, totalConsumed)
	}
}

// TestPerPartitionLagWithMaxPollRecords verifies that when maxPollRecords is
// set, not all records are returned at once, and multiple polls are needed to
// drain the partition - effectively testing records-lag behavior.
func TestPerPartitionLagWithMaxPollRecords(t *testing.T) {
	t.Parallel()
	topic := "poll-lag"
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))

	producer := newClient848(t, c, kgo.DefaultProduceTopic(topic))
	totalRecords := 15
	for i := range totalRecords {
		produceSync(t, producer, kgo.StringRecord("v-"+strconv.Itoa(i)))
	}

	consumer := newClient848(t, c,
		kgo.ConsumeTopics(topic),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	maxPoll := 3
	var totalConsumed int
	var polls int
	for totalConsumed < totalRecords {
		fs := consumer.PollRecords(ctx, maxPoll)
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("poll errors: %v", errs)
		}
		n := fs.NumRecords()
		if n > maxPoll {
			t.Errorf("poll %d: got %d records > maxPoll %d", polls, n, maxPoll)
		}
		totalConsumed += n
		polls++

		// Calculate remaining "lag".
		lag := totalRecords - totalConsumed
		if lag > 0 && n == 0 {
			t.Fatal("no progress made despite remaining lag")
		}
	}
	// With 15 records and maxPoll=3, we need at least 5 polls.
	if polls < totalRecords/maxPoll {
		t.Errorf("expected at least %d polls, got %d", totalRecords/maxPoll, polls)
	}
}
