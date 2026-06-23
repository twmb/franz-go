package kfake

// Round 3 of the FRANZ_AUDIT.md program: subsystem sweep of pkg/kgo/sink.go
// (sink-sweep.md). Tests whose fatal message starts with BUG REPRODUCED fail
// against the pre-fix client and flip to passing with the sink-sweep fixes;
// the control test documents neighboring behavior that already holds and
// must keep holding.

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// downgradeToTxnV0 sets the cluster's transaction.version feature to 0 so
// clients use the pre-KIP-890p2 flow: an explicit AddPartitionsToTxn before
// producing, marking addedToTxn at add time.
func downgradeToTxnV0(t *testing.T, c *Cluster) {
	t.Helper()
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
}

// produceCh produces one record and returns the channel its promise err
// lands on.
func produceCh(ctx context.Context, cl *kgo.Client, topic, val string) <-chan error {
	ch := make(chan error, 1)
	cl.Produce(ctx, &kgo.Record{Topic: topic, Value: []byte(val)}, func(_ *kgo.Record, err error) { ch <- err })
	return ch
}

// warmTxnTopics runs one throwaway transaction producing to every topic so
// the producer's metadata knows them all: later produces then buffer
// synchronously, letting a single Flush put batches for several topics into
// one produce request deterministically. The client must use ManualFlushing.
func warmTxnTopics(t *testing.T, cl *kgo.Client, topics ...string) {
	t.Helper()
	ctx := context.Background()
	if err := cl.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction(warm-up): %v", err)
	}
	var chs []<-chan error
	for _, topic := range topics {
		chs = append(chs, produceCh(ctx, cl, topic, "warm"))
	}
	if err := cl.Flush(ctx); err != nil {
		t.Fatalf("Flush(warm-up): %v", err)
	}
	for _, ch := range chs {
		if err := <-ch; err != nil {
			t.Fatalf("warm-up produce: %v", err)
		}
	}
	if err := cl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatalf("EndTransaction(warm-up): %v", err)
	}
}

// failNextAddPartitions installs a one-shot control failing every partition
// of the next AddPartitionsToTxn with the given error code, recording the
// topics the request carried.
func failNextAddPartitions(c *Cluster, code int16) *atomic.Value {
	var topics atomic.Value
	c.ControlKey(int16(kmsg.AddPartitionsToTxn), func(req kmsg.Request) (kmsg.Response, error, bool) {
		areq := req.(*kmsg.AddPartitionsToTxnRequest)
		var ts []string
		resp := areq.ResponseKind().(*kmsg.AddPartitionsToTxnResponse)
		for _, rt := range areq.Topics {
			ts = append(ts, rt.Topic)
			st := kmsg.NewAddPartitionsToTxnResponseTopic()
			st.Topic = rt.Topic
			for _, p := range rt.Partitions {
				sp := kmsg.NewAddPartitionsToTxnResponseTopicPartition()
				sp.Partition = p
				sp.ErrorCode = code
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		topics.Store(ts)
		return resp, nil, true
	})
	return &topics
}

// Pre-KIP-890p2, a partition joins the transaction via an explicit
// AddPartitionsToTxn the FIRST time one of its batches drains; later batches
// for that partition ride produce requests with no add. A failed add must
// not un-mark partitions that joined via an EARLIER acked add: doing so
// makes EndTransaction's anyAdded walk think no partition joined, skip
// EndTxn entirely, and return nil from a commit -- while the broker still
// holds an ongoing transaction with previously appended batches that the
// transaction timeout then aborts. Records whose promises succeeded, in a
// commit that returned nil, would be silently discarded.
func TestAuditTxnV1AddPartitionsFatalKeepsEarlierAdds(t *testing.T) {
	t.Parallel()
	const (
		topicP = "audit-v1-keep-p"
		topicQ = "audit-v1-keep-q"
	)
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topicP, topicQ))
	downgradeToTxnV0(t, c)

	cl := newPlainClient(t, c,
		kgo.TransactionalID("audit-v1-keep"),
		kgo.ManualFlushing(),
	)
	ctx := context.Background()

	warmTxnTopics(t, cl, topicP, topicQ)
	endTxns := observeEndTxns(c)

	// P joins the transaction (acked AddPartitionsToTxn) and gets a batch
	// appended; its promise succeeds.
	if err := cl.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	r1 := produceCh(ctx, cl, topicP, "r1")
	if err := cl.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := <-r1; err != nil {
		t.Fatalf("first produce: %v", err)
	}

	// Fail the next AddPartitionsToTxn -- which contains only Q, P being
	// already added -- with a fatal per-partition auth error. One flush
	// cycle producing to both P and Q rides a single produce request, so
	// the failure handling sees P's batch alongside Q's.
	addTopics := failNextAddPartitions(c, kerr.TopicAuthorizationFailed.Code)

	r2, r3 := produceCh(ctx, cl, topicP, "r2"), produceCh(ctx, cl, topicQ, "r3")
	if err := cl.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	for _, ch := range []<-chan error{r2, r3} {
		if err := <-ch; err == nil {
			t.Fatal("expected the produces after the injected AddPartitionsToTxn failure to fail, got success")
		}
	}
	got, _ := addTopics.Load().([]string)
	if len(got) != 1 || got[0] != topicQ {
		t.Fatalf("test premise broken: expected the failing AddPartitionsToTxn to contain only %q (P already added), got %v", topicQ, got)
	}

	// P joined this transaction and holds an appended batch whose promise
	// succeeded; the commit must not report success.
	err := cl.EndTransaction(ctx, kgo.TryCommit)
	if err == nil {
		t.Fatalf("BUG REPRODUCED: EndTransaction(TryCommit) returned nil after a fatal AddPartitionsToTxn error; %s[0] was added and produced earlier in this transaction (its promise succeeded), so the commit must fail rather than silently abandon the appended batch to the transaction-timeout abort (EndTxns issued: %d)", topicP, endTxns.Load())
	}
	if !errors.Is(err, kerr.OperationNotAttempted) {
		t.Fatalf("EndTransaction(TryCommit) = %v; want kerr.OperationNotAttempted", err)
	}
}

// Control for the test above: a RETRIABLE per-partition error on the add
// strips the batch, requeues it, and the next drain cycle re-adds and
// produces it successfully -- all within one Flush. This held before the
// sink-sweep fixes and must keep holding with the txnReq membership guard.
func TestAuditTxnV1AddPartitionsRetriableStripControl(t *testing.T) {
	t.Parallel()
	const (
		topicP = "audit-v1-strip-p"
		topicQ = "audit-v1-strip-q"
	)
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topicP, topicQ))
	downgradeToTxnV0(t, c)

	cl := newPlainClient(t, c,
		kgo.TransactionalID("audit-v1-strip"),
		kgo.ManualFlushing(),
	)
	ctx := context.Background()

	warmTxnTopics(t, cl, topicP, topicQ)

	if err := cl.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	r1 := produceCh(ctx, cl, topicP, "r1")
	if err := cl.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := <-r1; err != nil {
		t.Fatalf("first produce: %v", err)
	}

	addTopics := failNextAddPartitions(c, kerr.ConcurrentTransactions.Code)

	r2, r3 := produceCh(ctx, cl, topicP, "r2"), produceCh(ctx, cl, topicQ, "r3")
	if err := cl.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	for _, ch := range []<-chan error{r2, r3} {
		if err := <-ch; err != nil {
			t.Fatalf("produce after a stripped retriable add error must succeed end to end, got: %v", err)
		}
	}
	if addTopics.Load() == nil {
		t.Fatal("control was not consumed; the test did not exercise the injection")
	}
	if err := cl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatalf("EndTransaction: %v", err)
	}
}

// EndTransaction documents that a commit attempted while the producer ID has
// an error returns OperationNotAttempted and the caller should retry with
// TryAbort (GroupTransactSession.End does this retry internally). The retry
// must actually issue an EndTxn abort: the broker has an ongoing transaction
// holding the earlier appended batch, which otherwise stalls read_committed
// consumers on the LSO until the transaction timeout aborts it.
func TestAuditTxnAbortRetryAfterOperationNotAttempted(t *testing.T) {
	t.Parallel()
	const topic = "audit-abort-retry"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	endTxns := observeEndTxns(c)

	cl := newPlainClient(t, c, kgo.TransactionalID("audit-abort-retry"))
	ctx := context.Background()

	if err := cl.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	// First produce succeeds: the partition is marked added to the
	// transaction on the successful response.
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("r1")}).FirstErr(); err != nil {
		t.Fatalf("first produce: %v", err)
	}

	// Fail the next produce with INVALID_PRODUCER_ID_MAPPING: an
	// OOOSN-family error that fails the producer ID for transactional
	// producers and is neither retriable nor recoverable under KIP-890p2,
	// so the abort below cannot take the recovered-early-return and must
	// reach the broker via EndTxn.
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
				sp.ErrorCode = kerr.InvalidProducerIDMapping.Code
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("r2")}).FirstErr(); err == nil {
		t.Fatal("expected the injected produce failure, got success")
	}

	if err := cl.EndTransaction(ctx, kgo.TryCommit); !errors.Is(err, kerr.OperationNotAttempted) {
		t.Fatalf("EndTransaction(TryCommit) = %v; want kerr.OperationNotAttempted", err)
	}
	if err := cl.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatalf("EndTransaction(TryAbort) = %v", err)
	}
	if endTxns.Load() == 0 {
		t.Fatal("BUG REPRODUCED: the documented TryAbort retry after OperationNotAttempted issued no EndTxn; the commit attempt consumed the transaction state (inTxn, addedToTxn), so the abort silently no-opped and the broker-side transaction stays ongoing until the transaction timeout")
	}
}

type produceBatchWrittenHook struct{ n atomic.Int32 }

func (h *produceBatchWrittenHook) OnProduceBatchWritten(_ kgo.BrokerMetadata, _ string, _ int32, _ kgo.ProduceBatchMetrics) {
	h.n.Add(1)
}

// A buggy broker can reply with the same topic entry twice in one produce
// response. The first entry finishes the batch; the duplicate takes the
// "erroneously replied" arm, which must not erase the finished batch's
// metrics: the deferred metrics hook would then silently skip
// OnProduceBatchWritten for a successfully produced batch.
func TestAuditProduceDuplicateResponseEntryKeepsHook(t *testing.T) {
	t.Parallel()
	const topic = "audit-dup-resp"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))

	hook := new(produceBatchWrittenHook)
	cl := newPlainClient(t, c, kgo.WithHooks(hook))

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
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st, st) // duplicated entry: buggy-broker shape
		}
		return resp, nil, true
	})

	if err := cl.ProduceSync(context.Background(), &kgo.Record{Topic: topic, Value: []byte("v")}).FirstErr(); err != nil {
		t.Fatalf("ProduceSync: %v", err)
	}
	// The hook fires on its own goroutine after the response is handled.
	deadline := time.Now().Add(2 * time.Second)
	for hook.n.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := hook.n.Load(); got != 1 {
		t.Fatalf("BUG REPRODUCED: OnProduceBatchWritten fired %d times for one successfully produced batch; a duplicated produce response topic entry deleted the batch's metrics before the deferred hook ran", got)
	}
}
