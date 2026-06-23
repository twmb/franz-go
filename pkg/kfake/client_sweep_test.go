package kfake

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Regression tests from the client.go audit sweep (round 10). Each
// TestAudit* below fails before its corresponding kgo fix.

// Seed brokers have negative internal node IDs (math.MinInt32+i). Before the
// fix, brokerOrErr returned the not-found error for every negative ID before
// reaching its seed lookup, so requests through SeedBrokers() handles always
// failed with "unknown broker" - the seed-resolving branch had been dead
// since it was introduced.
func TestAuditSeedBrokerRequest(t *testing.T) {
	t.Parallel()
	c := newCluster(t, NumBrokers(1))
	cl := newPlainClient(t, c)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	seeds := cl.SeedBrokers()
	if len(seeds) != 1 {
		t.Fatalf("expected 1 seed broker, got %d", len(seeds))
	}
	req := kmsg.NewPtrMetadataRequest()
	req.Topics = []kmsg.MetadataRequestTopic{}
	if _, err := seeds[0].Request(ctx, req); err != nil {
		t.Errorf("seed broker request failed: %v", err)
	}

	// Unknown negative IDs must still fail fast: -1 is the unknown
	// controller / coordinator sentinel and can never be a seed.
	if _, err := cl.Broker(-1).Request(ctx, req); err == nil {
		t.Errorf("expected request to unknown broker -1 to fail")
	}
}

// A single-transaction AddPartitionsToTxn with VerifyOnly set must go out at
// v4+: v3 has no VerifyOnly field (and no Transactions array), so the broker
// would treat the request as a real partition add - a silent mutation where
// the caller asked for a read-only verification. Before the fix, the pin
// condition's first disjunct (len <= 1) subsumed the VerifyOnly check and
// pinned every single-transaction request to v3.
func TestAuditAddPartitionsVerifyOnlyNotPinnedToV3(t *testing.T) {
	t.Parallel()
	c := newCluster(t, NumBrokers(1), SeedTopics(1, "t"))

	type capture struct {
		version    int16
		verifyOnly bool
	}
	captured := make(chan capture, 1)
	c.ControlKey(int16(kmsg.AddPartitionsToTxn), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		req := kreq.(*kmsg.AddPartitionsToTxnRequest)
		got := capture{version: req.Version}
		if len(req.Transactions) == 1 {
			got.verifyOnly = req.Transactions[0].VerifyOnly
		}
		select {
		case captured <- got:
		default:
		}
		return nil, nil, false // let kfake handle the request normally
	})

	cl := newPlainClient(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := kmsg.NewPtrAddPartitionsToTxnRequest()
	txn := kmsg.NewAddPartitionsToTxnRequestTransaction()
	txn.TransactionalID = "txid"
	txn.ProducerID = 1
	txn.ProducerEpoch = 0
	txn.VerifyOnly = true
	rt := kmsg.NewAddPartitionsToTxnRequestTransactionTopic()
	rt.Topic = "t"
	rt.Partitions = []int32{0}
	txn.Topics = append(txn.Topics, rt)
	req.Transactions = append(req.Transactions, txn)

	// The response content does not matter (kfake has no VerifyOnly
	// semantics); we assert on what reached the wire.
	cl.Request(ctx, req) //nolint:errcheck // response intentionally ignored

	select {
	case got := <-captured:
		if got.version < 4 {
			t.Errorf("VerifyOnly AddPartitionsToTxn went out at v%d; v3 and below cannot express VerifyOnly and the broker would perform a real add", got.version)
		}
		if got.version >= 4 && !got.verifyOnly {
			t.Errorf("VerifyOnly flag was lost from the v%d request", got.version)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("no AddPartitionsToTxn request reached the broker")
	}
}

// On Close, the client must deliver exactly one PushTelemetry with
// Terminating=true (KIP-714) before connections die. Before the fix, two
// defects each independently broke this: close() selected on the
// just-canceled client context and instantly killed the metrics context that
// the in-flight terminating push was using, and pushMetrics' terminating
// flag was a shadowed variable so the push loops never observed termination
// (a surviving terminating push would have been followed by a duplicate).
func TestAuditCloseSendsTerminatingTelemetryPush(t *testing.T) {
	t.Parallel()
	c := newCluster(t, NumBrokers(1), SeedTopics(1, "t"))

	subscribed := make(chan struct{}, 1)
	c.ControlKey(int16(kmsg.GetTelemetrySubscriptions), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		req := kreq.(*kmsg.GetTelemetrySubscriptionsRequest)
		resp := req.ResponseKind().(*kmsg.GetTelemetrySubscriptionsResponse)
		resp.ClientInstanceID = [16]byte{1}
		resp.SubscriptionID = 1
		resp.AcceptedCompressionTypes = []int8{0} // none
		resp.PushIntervalMillis = 60_000          // no periodic push interferes
		resp.TelemetryMaxBytes = 1 << 20
		resp.DeltaTemporality = true
		resp.RequestedMetrics = []string{""} // all metrics
		select {
		case subscribed <- struct{}{}:
		default:
		}
		return resp, nil, true
	})

	var termPushes atomic.Int32
	gotTerm := make(chan struct{}, 8)
	c.ControlKey(int16(kmsg.PushTelemetry), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		req := kreq.(*kmsg.PushTelemetryRequest)
		if req.Terminating {
			termPushes.Add(1)
			select {
			case gotTerm <- struct{}{}:
			default:
			}
		}
		return nil, nil, false
	})

	cl := newPlainClient(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Metrics are only observed on produce/fetch; one produce triggers
	// firstObserve, which unparks the telemetry loop.
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "t", Value: []byte("v")}).FirstErr(); err != nil {
		t.Fatalf("produce failed: %v", err)
	}

	select {
	case <-subscribed:
	case <-time.After(5 * time.Second):
		t.Fatal("client never fetched its telemetry subscription")
	}
	// Let the push loop park in its push-interval wait.
	time.Sleep(50 * time.Millisecond)

	cl.Close()

	select {
	case <-gotTerm:
	case <-time.After(2 * time.Second):
		t.Fatal("no terminating telemetry push was delivered during Close")
	}
	if n := termPushes.Load(); n != 1 {
		t.Errorf("got %d terminating pushes, want exactly 1", n)
	}
}

// Concurrent callers collapse onto one in-flight broker-metadata fetch.
// Before the fix, the fetch ran on the initiating caller's context, so that
// caller canceling failed every piggybacked waiter with context.Canceled
// even though their own contexts were live.
func TestAuditPiggybackedBrokerMetadataNotPoisonedByCancel(t *testing.T) {
	t.Parallel()
	c := newCluster(t, NumBrokers(1))

	reqArrived := make(chan struct{}, 1)
	release := make(chan struct{})
	c.ControlKey(int16(kmsg.Metadata), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		req := kreq.(*kmsg.MetadataRequest)
		if req.Topics == nil || len(req.Topics) != 0 {
			// Not the broker-only fetch; stay armed for it.
			c.KeepControl()
			return nil, nil, false
		}
		select {
		case reqArrived <- struct{}{}:
		default:
		}
		<-release // stall the fetch so both callers are in flight
		return nil, nil, false
	})

	cl := newPlainClient(t, c)

	// Both goroutines issue an admin request on a fresh client: resolving
	// the controller forces fetchBrokerMetadata. G1 initiates the fetch
	// and is canceled; G2 piggybacks and must not inherit G1's cancel.
	mkreq := func(topic string) *kmsg.CreateTopicsRequest {
		req := kmsg.NewPtrCreateTopicsRequest()
		rt := kmsg.NewCreateTopicsRequestTopic()
		rt.Topic = topic
		rt.NumPartitions = 1
		rt.ReplicationFactor = 1
		req.Topics = append(req.Topics, rt)
		return req
	}

	ctx1, cancel1 := context.WithCancel(context.Background())
	defer cancel1()
	g1done := make(chan error, 1)
	go func() {
		_, err := cl.Request(ctx1, mkreq("g1"))
		g1done <- err
	}()

	select {
	case <-reqArrived:
	case <-time.After(5 * time.Second):
		t.Fatal("broker metadata fetch never started")
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()
	g2done := make(chan error, 1)
	go func() {
		_, err := cl.Request(ctx2, mkreq("g2"))
		g2done <- err
	}()

	// Give G2 time to park on the collapsed fetch, then cancel G1.
	time.Sleep(50 * time.Millisecond)
	cancel1()
	if err := <-g1done; err == nil {
		t.Errorf("expected the canceled caller to fail")
	}
	time.Sleep(10 * time.Millisecond)
	close(release)

	if err := <-g2done; err != nil {
		if errors.Is(err, context.Canceled) {
			t.Fatalf("piggybacked caller was poisoned by the first caller's cancel: %v", err)
		}
		t.Fatalf("piggybacked caller failed: %v", err)
	}
}
