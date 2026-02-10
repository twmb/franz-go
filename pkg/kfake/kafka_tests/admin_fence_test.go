// Derived via LLM from Apache Kafka's AdminFenceProducersTest.java (Apache 2.0).
// https://github.com/apache/kafka/blob/trunk/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/admin/AdminFenceProducersTest.java

package kafka_tests

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// fenceProducer fences a transactional producer by calling InitProducerID
// with the same transactional ID from a different client, which bumps the epoch
// and fences the original producer.
func fenceProducer(t *testing.T, c *kfake.Cluster, txnID string) {
	t.Helper()
	cl := newClient848(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := kmsg.NewInitProducerIDRequest()
	req.TransactionalID = &txnID
	req.TransactionTimeoutMillis = 60000
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("fence InitProducerID failed: %v", err)
	}
	if resp.ErrorCode != 0 {
		t.Fatalf("fence InitProducerID error code: %d", resp.ErrorCode)
	}
}

// TestFenceAfterProducerCommit verifies that fencing a transactional producer
// after it has committed a transaction causes subsequent operations to fail.
func TestFenceAfterProducerCommit(t *testing.T) {
	t.Parallel()
	topic := "fence-after-commit"
	txnID := "fence-after-commit-txn"
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Create transactional producer and produce + commit.
	producer := newClient848(t, c,
		kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID(txnID),
	)

	if err := producer.BeginTransaction(); err != nil {
		t.Fatalf("begin txn: %v", err)
	}
	for i := range 3 {
		if err := producer.ProduceSync(ctx, kgo.StringRecord("pre-fence-"+strconv.Itoa(i))).FirstErr(); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}
	if err := producer.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatalf("commit: %v", err)
	}

	// Fence the producer by calling InitProducerID from another client.
	fenceProducer(t, c, txnID)

	// Use a short context for post-fence operations.
	fenceCtx, fenceCancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer fenceCancel()

	// The fenced producer should eventually fail somewhere in the
	// begin/produce/commit cycle.
	var fenceErr error
	if fenceErr = producer.BeginTransaction(); fenceErr != nil {
		return // Failed at begin - correct.
	}
	if fenceErr = producer.ProduceSync(fenceCtx, kgo.StringRecord("post-fence")).FirstErr(); fenceErr != nil {
		// Failed at produce - correct. Try to abort to clean up.
		_ = producer.EndTransaction(fenceCtx, kgo.TryAbort)
		return
	}
	fenceErr = producer.EndTransaction(fenceCtx, kgo.TryCommit)
	if fenceErr == nil {
		t.Error("expected fenced producer to eventually fail, but all operations succeeded")
	}
}

// TestFenceBeforeProducerCommit verifies that fencing a transactional producer
// before it commits causes the commit to fail.
func TestFenceBeforeProducerCommit(t *testing.T) {
	t.Parallel()
	topic := "fence-before-commit"
	txnID := "fence-before-commit-txn"
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	producer := newClient848(t, c,
		kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID(txnID),
	)

	if err := producer.BeginTransaction(); err != nil {
		t.Fatalf("begin txn: %v", err)
	}
	for i := range 3 {
		if err := producer.ProduceSync(ctx, kgo.StringRecord("to-fence-"+strconv.Itoa(i))).FirstErr(); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}

	// Fence the producer before it commits.
	fenceProducer(t, c, txnID)

	// The original producer's commit should fail.
	err := producer.EndTransaction(ctx, kgo.TryCommit)
	if err == nil {
		t.Error("expected fenced producer commit to fail")
	}
}
