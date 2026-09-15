package kfake

// Regression tests for transaction coordinator churn and fencing during
// EndTransaction. Tests whose fatal message starts with BUG REPRODUCED fail
// against the pre-fix client; the control tests document neighboring
// behavior that already holds and must keep holding.

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

// A transaction coordinator answers InitProducerID with
// CONCURRENT_TRANSACTIONS whenever it is still completing a transaction for
// the transactional ID: most notably, taking over a crashed incarnation's
// ongoing transaction ALWAYS returns it at least once, because the broker
// fences and aborts the old transaction and tells the new producer to retry
// (TransactionCoordinator sendRetriableErrorCallback). The client must retry
// in place; surfacing the error makes BeginTransaction report a routine
// recovery as "producer ID has a fatal, unrecoverable error".
func TestAuditTxnInitPidConcurrentTransactions(t *testing.T) {
	t.Parallel()
	c := newCluster(t, NumBrokers(1), SeedTopics(1, "audit-init-ct"))

	injected := c.Fault(Fault{Keys: []kmsg.Key{kmsg.InitProducerID}, Err: kerr.ConcurrentTransactions})

	cl := newPlainClient(t, c, kgo.TransactionalID("audit-init-ct"))
	if err := cl.BeginTransaction(); err != nil {
		t.Fatalf("BUG REPRODUCED: BeginTransaction failed after one transient CONCURRENT_TRANSACTIONS (injected %d): %v", injected.Hits(), err)
	}
	if injected.Hits() == 0 {
		t.Fatal("control was not consumed; the test did not exercise the injection")
	}
	if err := cl.EndTransaction(context.Background(), kgo.TryAbort); err != nil {
		t.Fatalf("EndTransaction: %v", err)
	}
}

// The sibling of the test above: a retriable broker code that the
// coordinator wrapper does not retry (it retries only the three
// coordinator-move codes) and that is not CONCURRENT_TRANSACTIONS surfaces
// as a producer-id load failure. maybeRecoverProducerID must treat a
// kerr-retriable load failure like its transport-error sibling - flag the
// ID for reload and let Begin proceed - rather than reporting a fatal,
// unrecoverable producer state. NOT_ENOUGH_REPLICAS models the coordinator
// failing to write to __transaction_state.
func TestAuditTxnInitPidRetriableLoadFailure(t *testing.T) {
	t.Parallel()
	c := newCluster(t, NumBrokers(1), SeedTopics(1, "audit-init-retriable"))

	injected := c.Fault(Fault{Keys: []kmsg.Key{kmsg.InitProducerID}, Err: kerr.NotEnoughReplicas})

	cl := newPlainClient(t, c, kgo.TransactionalID("audit-init-retriable"))
	if err := cl.BeginTransaction(); err != nil {
		t.Fatalf("BUG REPRODUCED: BeginTransaction failed on a transient retriable InitProducerID error (injected %d): %v", injected.Hits(), err)
	}
	if injected.Hits() == 0 {
		t.Fatal("control was not consumed; the test did not exercise the injection")
	}
	// The producer ID is flagged for reload; the first produce re-runs
	// InitProducerID (the control is consumed, so it now succeeds) and
	// the transaction must work end to end.
	r := &kgo.Record{Topic: "audit-init-retriable", Value: []byte("v")}
	if err := cl.ProduceSync(context.Background(), r).FirstErr(); err != nil {
		t.Fatalf("ProduceSync after reload: %v", err)
	}
	if err := cl.EndTransaction(context.Background(), kgo.TryCommit); err != nil {
		t.Fatalf("EndTransaction: %v", err)
	}
}

// failAllProduces fails every partition of the topic in the next produce
// request with MESSAGE_TOO_LARGE. The batch is terminal and the producer ID
// survives, as with a broker-side append rejection.
func failAllProduces(c *Cluster, topic string) {
	c.Fault(Fault{
		Keys:  []kmsg.Key{kmsg.Produce},
		Topic: topic,
		Err:   kerr.MessageTooLarge,
		Count: 1,
	})
}

// observeEndTxns installs an observing fault that counts EndTxn requests
// without altering them.
func observeEndTxns(c *Cluster) *FaultHandle {
	return c.Fault(Fault{Keys: []kmsg.Key{kmsg.EndTxn}, Observe: true, Count: -1})
}

// Under KIP-890 part 2 (transaction.version=2), partitions join the
// transaction implicitly via produce requests, and the client marks a
// partition as added only on a SUCCESSFUL produce response. If every produce
// in the transaction failed, the client must still issue EndTxn(abort): the
// broker registers the partition during produce verification BEFORE the
// append, so a failed append can leave a server-side ongoing transaction
// that only the transaction timeout would otherwise clear, and under TV2
// every transaction must end with an epoch bump before the next begins.
func TestAuditTxnV2AbortAfterFailedProduces(t *testing.T) {
	t.Parallel()
	c := newCluster(t, NumBrokers(1), SeedTopics(1, "audit-v2-abort"))

	endTxns := observeEndTxns(c)
	failAllProduces(c, "audit-v2-abort")

	cl := newPlainClient(t, c, kgo.TransactionalID("audit-v2-abort"))
	if err := cl.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	r := &kgo.Record{Topic: "audit-v2-abort", Value: []byte("v")}
	if err := cl.ProduceSync(context.Background(), r).FirstErr(); err == nil {
		t.Fatal("expected the injected produce failure, got success")
	}
	if err := cl.EndTransaction(context.Background(), kgo.TryAbort); err != nil {
		t.Fatalf("EndTransaction: %v", err)
	}
	if endTxns.Hits() == 0 {
		t.Fatal("BUG REPRODUCED: EndTransaction(TryAbort) issued no EndTxn after failed transactional produces under KIP-890p2; the broker-side transaction is left ongoing until the transaction timeout")
	}
}

// TV1 control for the test above: pre-890p2 clients add partitions via an
// explicit AddPartitionsToTxn before producing and mark them added at that
// point, so a failed produce still ends with an EndTxn(abort). This must
// keep holding; the missing abort is a TV2-only regression.
func TestAuditTxnV1AbortAfterFailedProducesControl(t *testing.T) {
	t.Parallel()
	c := newCluster(t, NumBrokers(1), SeedTopics(1, "audit-v1-abort"))

	// The client does not opt into KIP-890p2 against a
	// transaction.version=0 cluster.
	downgradeToTxnV0(t, c)

	endTxns := observeEndTxns(c)
	failAllProduces(c, "audit-v1-abort")

	cl := newPlainClient(t, c, kgo.TransactionalID("audit-v1-abort"))
	if err := cl.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	r := &kgo.Record{Topic: "audit-v1-abort", Value: []byte("v")}
	if err := cl.ProduceSync(context.Background(), r).FirstErr(); err == nil {
		t.Fatal("expected the injected produce failure, got success")
	}
	if err := cl.EndTransaction(context.Background(), kgo.TryAbort); err != nil {
		t.Fatalf("EndTransaction: %v", err)
	}
	if endTxns.Hits() == 0 {
		t.Fatal("TV1 control regressed: EndTransaction(TryAbort) issued no EndTxn after failed produces on a pre-890p2 cluster")
	}
}

// A client whose MaxVersions cap is below the KIP-890p2 wire versions must
// not opt into the implicit-add produce path, even when the cluster has
// transaction.version=2 finalized. Opting in skips AddPartitionsToTxn while
// version negotiation still puts produce at v11 or lower on the wire, and
// the broker rejects every batch with INVALID_TXN_STATE because the
// partition was never added to the transaction.
func TestAuditTxnV2MaxVersionsPinnedProduce(t *testing.T) {
	t.Parallel()
	c := newCluster(t, NumBrokers(1), SeedTopics(1, "audit-v2-pin"))

	cl := newPlainClient(t, c,
		kgo.TransactionalID("audit-v2-pin"),
		kgo.MaxVersions(kversion.V3_7_0()),
	)
	if err := cl.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	r := &kgo.Record{Topic: "audit-v2-pin", Value: []byte("v")}
	if err := cl.ProduceSync(context.Background(), r).FirstErr(); err != nil {
		t.Fatalf("BUG REPRODUCED: transactional produce failed for a MaxVersions-pinned client against a transaction.version=2 cluster: %v", err)
	}
	if err := cl.EndTransaction(context.Background(), kgo.TryCommit); err != nil {
		t.Fatalf("EndTransaction: %v", err)
	}
}

// Coordinator-churn sound guard: an EndTxn that lands on a moved coordinator
// gets NOT_COORDINATOR; the coordinator-request wrapper must delete the
// cached coordinator, re-look it up, and retry, so a single NOT_COORDINATOR
// never fails a commit. This holds today; keep it holding.
func TestAuditTxnEndTxnNotCoordinatorRetried(t *testing.T) {
	t.Parallel()
	c := newCluster(t, NumBrokers(1), SeedTopics(1, "audit-endtxn-nc"))

	cl := newPlainClient(t, c, kgo.TransactionalID("audit-endtxn-nc"))
	if err := cl.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	r := &kgo.Record{Topic: "audit-endtxn-nc", Value: []byte("v")}
	if err := cl.ProduceSync(context.Background(), r).FirstErr(); err != nil {
		t.Fatalf("ProduceSync: %v", err)
	}

	injected := c.Fault(Fault{Keys: []kmsg.Key{kmsg.EndTxn}, Err: kerr.NotCoordinator})

	if err := cl.EndTransaction(context.Background(), kgo.TryCommit); err != nil {
		t.Fatalf("EndTransaction did not survive one NOT_COORDINATOR (injected %d): %v", injected.Hits(), err)
	}
	if injected.Hits() != 1 {
		t.Fatalf("expected exactly one injected NOT_COORDINATOR, got %d", injected.Hits())
	}
}
