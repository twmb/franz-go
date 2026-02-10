// Derived via LLM from Apache Kafka's TransactionsWithMaxInFlightOneTest.java (Apache 2.0).
// https://github.com/apache/kafka/blob/trunk/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/producer/TransactionsWithMaxInFlightOneTest.java

package kafka_tests

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestTransactionalProducerSingleBrokerMaxInFlightOne tests the transactional
// producer with MaxProduceRequestsInflightPerBroker=1. It produces an aborted
// transaction and a committed transaction, then verifies that a
// read_committed consumer sees only the committed records.
func TestTransactionalProducerSingleBrokerMaxInFlightOne(t *testing.T) {
	t.Parallel()
	topic := "txn-inflight-one"
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	txnProducer := newClient848(t, c,
		kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID("txn-inflight-test"),
		kgo.MaxProduceRequestsInflightPerBroker(1),
	)

	// Transaction 1: produce and abort.
	if err := txnProducer.BeginTransaction(); err != nil {
		t.Fatalf("begin txn 1: %v", err)
	}
	for i := range 3 {
		r := kgo.StringRecord("aborted-" + strconv.Itoa(i))
		if err := txnProducer.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatalf("produce aborted record %d: %v", i, err)
		}
	}
	if err := txnProducer.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatalf("abort txn 1: %v", err)
	}

	// Transaction 2: produce and commit.
	if err := txnProducer.BeginTransaction(); err != nil {
		t.Fatalf("begin txn 2: %v", err)
	}
	for i := range 3 {
		r := kgo.StringRecord("committed-" + strconv.Itoa(i))
		if err := txnProducer.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatalf("produce committed record %d: %v", i, err)
		}
	}
	if err := txnProducer.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatalf("commit txn 2: %v", err)
	}

	// Verify read_committed consumer only sees committed records.
	consumer := newClient848(t, c,
		kgo.ConsumeTopics(topic),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	records := consumeN(t, consumer, 3, 5*time.Second)
	if len(records) != 3 {
		t.Fatalf("expected 3 committed records, got %d", len(records))
	}
	for _, r := range records {
		v := string(r.Value)
		if len(v) >= 7 && v[:7] == "aborted" {
			t.Errorf("read_committed consumer saw aborted record: %s", v)
		}
	}

	// Verify read_uncommitted consumer sees all 6 data records (aborted + committed).
	uncommittedConsumer := newClient848(t, c,
		kgo.ConsumeTopics(topic),
		kgo.FetchIsolationLevel(kgo.ReadUncommitted()),
	)
	allRecords := consumeN(t, uncommittedConsumer, 6, 5*time.Second)
	if len(allRecords) != 6 {
		t.Fatalf("expected 6 total records with read_uncommitted, got %d", len(allRecords))
	}
}
