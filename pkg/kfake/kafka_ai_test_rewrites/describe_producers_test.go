// Derived from Apache Kafka's DescribeProducersWithBrokerIdTest.java (Apache 2.0).
// https://github.com/apache/kafka/blob/trunk/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/admin/DescribeProducersWithBrokerIdTest.java

package kafka_ai_test_rewrites

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestDescribeProducersDefaultRoutesToLeader verifies that DescribeProducers
// routes to the partition leader and reports active producers.
func TestDescribeProducersDefaultRoutesToLeader(t *testing.T) {
	t.Parallel()
	topic := "describe-producers-leader"
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create a transactional producer with an active (uncommitted) transaction.
	txnID := "describe-producers-txn"
	producer := newClient(t, c,
		kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID(txnID),
	)
	if err := producer.BeginTransaction(); err != nil {
		t.Fatalf("begin txn: %v", err)
	}
	if err := producer.ProduceSync(ctx, kgo.StringRecord("v1")).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}

	// Describe producers on the partition.
	adm := newAdminClient(t, c)
	ts := make(kadm.TopicsSet)
	ts.Add(topic, 0)
	described, err := adm.DescribeProducers(ctx, ts)
	if err != nil {
		t.Fatalf("DescribeProducers failed: %v", err)
	}

	topicProducers, ok := described[topic]
	if !ok {
		t.Fatal("topic not found in DescribeProducers response")
	}
	partProducers, ok := topicProducers.Partitions[0]
	if !ok {
		t.Fatal("partition 0 not found in DescribeProducers response")
	}
	if partProducers.Err != nil {
		t.Fatalf("DescribeProducers partition error: %v", partProducers.Err)
	}
	if len(partProducers.ActiveProducers) == 0 {
		t.Error("expected at least one active producer, got none")
	}

	// Clean up: commit the transaction.
	if err := producer.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatalf("commit: %v", err)
	}
}

// TestDescribeProducersAfterCommit verifies that after committing a
// transaction, the producer is no longer listed as active.
func TestDescribeProducersAfterCommit(t *testing.T) {
	t.Parallel()
	topic := "describe-producers-committed"
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	txnID := "describe-producers-committed-txn"
	producer := newClient(t, c,
		kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID(txnID),
	)

	// Produce and commit.
	if err := producer.BeginTransaction(); err != nil {
		t.Fatalf("begin txn: %v", err)
	}
	if err := producer.ProduceSync(ctx, kgo.StringRecord("v1")).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
	if err := producer.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// After commit, no active producers should be listed.
	adm := newAdminClient(t, c)
	ts := make(kadm.TopicsSet)
	ts.Add(topic, 0)
	described, err := adm.DescribeProducers(ctx, ts)
	if err != nil {
		t.Fatalf("DescribeProducers failed: %v", err)
	}

	topicProducers, ok := described[topic]
	if !ok {
		t.Fatal("topic not found in response")
	}
	partProducers, ok := topicProducers.Partitions[0]
	if !ok {
		t.Fatal("partition 0 not found in response")
	}
	if partProducers.Err != nil {
		t.Fatalf("partition error: %v", partProducers.Err)
	}
	if len(partProducers.ActiveProducers) != 0 {
		t.Errorf("expected no active producers after commit, got %d", len(partProducers.ActiveProducers))
	}
}
