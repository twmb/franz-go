package kfake

// Round 4 of the FRANZ_AUDIT.md program: transaction coordinator churn and
// fencing during EndTransaction (txn-churn.md). Every test here asserts the
// CORRECT behavior; tests whose fatal message starts with BUG REPRODUCED
// fail against the pre-fix client and flip to passing with the txn-churn
// fixes. The control tests document neighboring behavior that already holds
// and must keep holding.

import (
	"context"
	"sync/atomic"
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

	var injected atomic.Int32
	c.ControlKey(int16(kmsg.InitProducerID), func(req kmsg.Request) (kmsg.Response, error, bool) {
		injected.Add(1)
		resp := req.ResponseKind().(*kmsg.InitProducerIDResponse)
		resp.ErrorCode = kerr.ConcurrentTransactions.Code
		return resp, nil, true
	})

	cl := newPlainClient(t, c, kgo.TransactionalID("audit-init-ct"))
	if err := cl.BeginTransaction(); err != nil {
		t.Fatalf("BUG REPRODUCED: BeginTransaction failed after one transient CONCURRENT_TRANSACTIONS (injected %d): %v", injected.Load(), err)
	}
	if injected.Load() == 0 {
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

	var injected atomic.Int32
	c.ControlKey(int16(kmsg.InitProducerID), func(req kmsg.Request) (kmsg.Response, error, bool) {
		injected.Add(1)
		resp := req.ResponseKind().(*kmsg.InitProducerIDResponse)
		resp.ErrorCode = kerr.NotEnoughReplicas.Code
		return resp, nil, true
	})

	cl := newPlainClient(t, c, kgo.TransactionalID("audit-init-retriable"))
	if err := cl.BeginTransaction(); err != nil {
		t.Fatalf("BUG REPRODUCED: BeginTransaction failed on a transient retriable InitProducerID error (injected %d): %v", injected.Load(), err)
	}
	if injected.Load() == 0 {
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

// failAllProduces installs a control that fails every partition of the next
// produce request with MESSAGE_TOO_LARGE: terminal for the batch, but not a
// producer-id-failing error, mirroring a broker-side append rejection.
func failAllProduces(c *Cluster) {
	c.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
		preq := req.(*kmsg.ProduceRequest)
		resp := preq.ResponseKind().(*kmsg.ProduceResponse)
		for _, rt := range preq.Topics {
			st := kmsg.NewProduceResponseTopic()
			st.Topic = rt.Topic
			st.TopicID = rt.TopicID
			for _, rp := range rt.Partitions {
				sp := kmsg.NewProduceResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = kerr.MessageTooLarge.Code
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})
}

// observeEndTxns installs a keep-forever control that counts EndTxn requests
// without altering them.
func observeEndTxns(c *Cluster) *atomic.Int32 {
	var n atomic.Int32
	c.ControlKey(int16(kmsg.EndTxn), func(kmsg.Request) (kmsg.Response, error, bool) {
		n.Add(1)
		c.KeepControl()
		return nil, nil, false
	})
	return &n
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
	failAllProduces(c)

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
	if endTxns.Load() == 0 {
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

	// Downgrade the cluster to transaction.version=0 so the client does
	// not opt into KIP-890p2.
	admin := newPlainClient(t, c)
	req := kmsg.NewPtrUpdateFeaturesRequest()
	fu := kmsg.NewUpdateFeaturesRequestFeatureUpdate()
	fu.Feature = "transaction.version"
	fu.MaxVersionLevel = 0
	fu.UpgradeType = 2 // safe downgrade
	req.FeatureUpdates = append(req.FeatureUpdates, fu)
	resp, err := req.RequestWith(context.Background(), admin)
	if err != nil {
		t.Fatalf("UpdateFeatures: %v", err)
	}
	if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
		t.Fatalf("UpdateFeatures: %v", err)
	}
	for _, res := range resp.Results {
		if err := kerr.ErrorForCode(res.ErrorCode); err != nil {
			t.Fatalf("UpdateFeatures %s: %v", res.Feature, err)
		}
	}

	endTxns := observeEndTxns(c)
	failAllProduces(c)

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
	if endTxns.Load() == 0 {
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

	var injected atomic.Int32
	c.ControlKey(int16(kmsg.EndTxn), func(req kmsg.Request) (kmsg.Response, error, bool) {
		injected.Add(1)
		resp := req.ResponseKind().(*kmsg.EndTxnResponse)
		resp.ErrorCode = kerr.NotCoordinator.Code
		return resp, nil, true
	})

	if err := cl.EndTransaction(context.Background(), kgo.TryCommit); err != nil {
		t.Fatalf("EndTransaction did not survive one NOT_COORDINATOR (injected %d): %v", injected.Load(), err)
	}
	if injected.Load() != 1 {
		t.Fatalf("expected exactly one injected NOT_COORDINATOR, got %d", injected.Load())
	}
}
