package kfake

import (
	"context"
	"fmt"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

// Regression tests from the broker.go audit sweep (round 11). Each
// TestAudit* below fails before its corresponding kgo fix.

func saslPlainOpts(c *Cluster) []kgo.Opt {
	return []kgo.Opt{
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.SASL(plain.Auth{User: "admin", Pass: "admin"}.AsMechanism()),
	}
}

// countHandshakes installs a persistent observer control that counts every
// SASLHandshake the cluster receives.
func countHandshakes(c *Cluster) *atomic.Int32 {
	var n atomic.Int32
	c.ControlKey(17, func(kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		n.Add(1)
		return nil, nil, false
	})
	return &n
}

// discoverBroker returns a Broker handle for the cluster's single broker,
// discovered via a metadata request over the seed connection. All subsequent
// test requests go through this handle so they share one connection whose
// sasl session the test controls.
func discoverBroker(t *testing.T, cl *kgo.Client) *kgo.Broker {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := kmsg.NewPtrMetadataRequest()
	resp, err := cl.Request(ctx, req)
	if err != nil {
		t.Fatalf("metadata discovery failed: %v", err)
	}
	brokers := resp.(*kmsg.MetadataResponse).Brokers
	if len(brokers) != 1 {
		t.Fatalf("expected 1 broker, got %d", len(brokers))
	}
	return cl.Broker(int(brokers[0].NodeID))
}

// reauthOrderHook tracks, per broker node, how many written requests are
// still awaiting their response, and records a violation if a SASLHandshake
// is ever written while any response is outstanding: a reauth read would race
// the connection's response reader byte-by-byte. Writes and reads for one
// connection are serial on their respective goroutines, so the count is exact
// for tests that drive a single connection per node.
type reauthOrderHook struct {
	mu          sync.Mutex
	outstanding map[int32]int
	violations  []string
}

func newReauthOrderHook() *reauthOrderHook {
	return &reauthOrderHook{outstanding: make(map[int32]int)}
}

func (h *reauthOrderHook) OnBrokerWrite(meta kgo.BrokerMetadata, key int16, _ int, _, _ time.Duration, err error) {
	if err != nil {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	switch key {
	case 17: // SASLHandshake: must only be written on a quiet connection
		if n := h.outstanding[meta.NodeID]; n > 0 {
			h.violations = append(h.violations,
				fmt.Sprintf("sasl handshake written to node %d with %d responses outstanding", meta.NodeID, n))
		}
	case 36: // SASLAuthenticate: part of the (re)auth flow, not counted
	default:
		h.outstanding[meta.NodeID]++
	}
}

func (h *reauthOrderHook) OnBrokerRead(meta kgo.BrokerMetadata, key int16, _ int, _, _ time.Duration, _ error) {
	if key == 17 || key == 36 {
		return
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.outstanding[meta.NodeID] > 0 {
		h.outstanding[meta.NodeID]--
	}
}

func (h *reauthOrderHook) OnBrokerDisconnect(meta kgo.BrokerMetadata, _ net.Conn) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.outstanding[meta.NodeID] = 0
}

func (h *reauthOrderHook) takeViolations() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.violations
}

// A sasl reauthentication (KIP-368) reads the handshake response on the
// broker's request-handling goroutine. Before the fix, it did so even while a
// pipelined response was in flight, racing handleResps byte-by-byte on the
// shared connection: both readers got fragments of each other's responses and
// every in-flight request failed. The fix parks requests for the connection
// while its pipeline drains (sustained traffic would otherwise never leave
// the connection quiet), reauthenticates once the last in-flight response is
// read, and then replays the parked requests in order.
//
// Flow: request A is stalled server-side past the session expiry; request B
// arrives after expiry while A's response is in flight. Pre-fix, B's handleReq
// writes the reauth handshake immediately - while A is outstanding - which the
// reauthOrderHook flags deterministically (the byte-level corruption itself
// depends on kernel reader-wakeup ordering and only sometimes fires). With
// the fix, B parks, the reauth runs only after A's response drains, and B is
// replayed after it: by the time B completes, exactly one reauth handshake
// has happened and no handshake ever overlapped an outstanding response. A
// postpone-only approach also fails here: B would complete with no reauth
// having happened at all.
func TestAuditSaslReauthPipelinedNoCorruption(t *testing.T) {
	t.Parallel()
	c := newCluster(t,
		NumBrokers(1),
		EnableSASL(),
		Superuser("PLAIN", "admin", "admin"),
		// Lifetime 2s; the client's minimum pessimism is 1s, so the
		// client schedules reauth ~1s after authenticating.
		BrokerConfigs(map[string]string{"connections.max.reauth.ms": "2000"}),
	)
	handshakes := countHandshakes(c)

	// Stall the response to a metadata request for the marker topic. The
	// control is left installed (unhandled returns keep it); only the
	// marker request matches.
	const stallTopic = "stallme"
	c.ControlKey(3, func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		req := kreq.(*kmsg.MetadataRequest)
		if len(req.Topics) == 1 && req.Topics[0].Topic != nil && *req.Topics[0].Topic == stallTopic {
			c.SleepControl(func() { time.Sleep(1500 * time.Millisecond) })
		}
		return nil, nil, false
	})

	hook := newReauthOrderHook()
	cl := newPlainClient(t, c, append(saslPlainOpts(c), kgo.WithHooks(hook))...)
	br := discoverBroker(t, cl)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Warm the broker connection; its sasl session starts now and the
	// client considers it expired ~1s from now.
	if _, err := br.Request(ctx, kmsg.NewPtrMetadataRequest()); err != nil {
		t.Fatalf("warm request failed: %v", err)
	}
	base := handshakes.Load()

	// A: stalled server-side for 1.5s; its response is in flight across
	// the expiry boundary.
	errA := make(chan error, 1)
	go func() {
		req := kmsg.NewPtrMetadataRequest()
		reqTopic := kmsg.NewMetadataRequestTopic()
		topic := stallTopic
		reqTopic.Topic = &topic
		req.Topics = append(req.Topics, reqTopic)
		_, err := br.Request(ctx, req)
		errA <- err
	}()

	// B: crosses the expiry while A's response is pending; it must park,
	// and the reauth+replay happens once A's response drains.
	time.Sleep(1200 * time.Millisecond)
	errB := make(chan error, 1)
	go func() {
		_, err := br.Request(ctx, kmsg.NewPtrMetadataRequest())
		errB <- err
	}()

	for name, ch := range map[string]chan error{"A (stalled)": errA, "B (crossed expiry)": errB} {
		select {
		case err := <-ch:
			if err != nil {
				t.Errorf("request %s failed: %v", name, err)
			}
		case <-time.After(8 * time.Second):
			t.Errorf("request %s did not complete", name)
		}
	}

	// No reauth handshake may ever overlap an outstanding response: this
	// catches the original in-place reauth deterministically (B's
	// handshake was written while A was in flight, whether or not the
	// kernel interleaved the response bytes).
	for _, v := range hook.takeViolations() {
		t.Errorf("read-ownership violation: %s", v)
	}

	// B completing means the parked request was replayed, which happens
	// only after the drain-triggered reauthentication: exactly one reauth
	// handshake must exist by now. A postpone-only behavior fails here
	// with zero reauths (B would have been issued on the old session and
	// nothing afterward reauthenticated).
	if got := handshakes.Load(); got != base+1 {
		t.Errorf("expected exactly one reauthentication handshake once the parked request completed: handshakes went %d -> %d", base, got)
	}
}

// acks=0 produce connections run the discard goroutine, which owns all reads
// on the connection. Before the fix, a produce request crossing the session
// expiry reauthenticated in place: the discard goroutine consumed the
// handshake response bytes, the reauth read timed out, and the connection
// died (the produce then succeeded only via a slow retry on a fresh
// connection). After the fix, loadConnection recreates an expired discard
// connection up front, so the produce proceeds immediately on a freshly
// authenticated connection.
func TestAuditSaslReauthAcks0DiscardConn(t *testing.T) {
	t.Parallel()
	c := newCluster(t,
		NumBrokers(1),
		EnableSASL(),
		Superuser("PLAIN", "admin", "admin"),
		SeedTopics(1, "t0"),
		BrokerConfigs(map[string]string{"connections.max.reauth.ms": "2000"}),
	)

	opts := append(saslPlainOpts(c),
		kgo.RequiredAcks(kgo.NoAck()),
		kgo.DisableIdempotentWrite(),
		kgo.DefaultProduceTopic("t0"),
		// Bound the pre-fix failure mode: the reauth read that loses
		// its response to the discard goroutine fails at this read
		// timeout (and the produce then retries on a new connection).
		kgo.RequestTimeoutOverhead(2*time.Second),
	)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// First produce: opens the produce connection (sasl session starts;
	// client-side expiry ~1s from now) and spawns the discard goroutine.
	if err := cl.ProduceSync(ctx, kgo.StringRecord("r1")).FirstErr(); err != nil {
		t.Fatalf("first produce failed: %v", err)
	}

	// Cross the expiry, then produce again. Post-fix this recreates the
	// connection and completes in milliseconds; pre-fix it burns the 2s
	// read timeout before recovering on a retry.
	time.Sleep(1300 * time.Millisecond)
	start := time.Now()
	if err := cl.ProduceSync(ctx, kgo.StringRecord("r2")).FirstErr(); err != nil {
		t.Errorf("second produce failed: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 1500*time.Millisecond {
		t.Errorf("second produce took %v; reauthentication raced the discard goroutine (expected immediate completion on a recreated connection)", elapsed)
	}
}

// A broker replying UNSUPPORTED_VERSION to ApiVersions with a KIP-511 version
// hint that is not strictly lower than what we sent kept the client in the
// downgrade-retry loop forever - and connection init runs on no request
// context, so the initiating request hung until client close. The fix only
// accepts a strictly-lower non-negative hint and fails the connection
// otherwise.
func TestAuditApiVersionsDowngradeLoopTerminates(t *testing.T) {
	t.Parallel()
	c := newCluster(t, NumBrokers(1))

	// Always reply UNSUPPORTED_VERSION advertising max version 4 -- the
	// same version the client starts with, so a downgrade never happens.
	c.ControlKey(18, func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		req := kreq.(*kmsg.ApiVersionsRequest)
		resp := req.ResponseKind().(*kmsg.ApiVersionsResponse)
		resp.Version = 0
		resp.ErrorCode = 35 // UNSUPPORTED_VERSION
		key := kmsg.NewApiVersionsResponseApiKey()
		key.ApiKey = 18
		key.MinVersion = 0
		key.MaxVersion = 4
		resp.ApiKeys = append(resp.ApiKeys, key)
		return resp, nil, true
	})

	cl := newPlainClient(t, c)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		_, err := cl.Request(ctx, kmsg.NewPtrMetadataRequest())
		errCh <- err
	}()
	select {
	case err := <-errCh:
		if err == nil {
			t.Error("expected the request to fail against a broker that never accepts our ApiVersions version")
		}
	case <-time.After(8 * time.Second):
		t.Fatal("ApiVersions downgrade loop did not terminate")
	}
}

// A reauthentication whose SaslAuthenticate response carries no session
// lifetime (re-auth was disabled, e.g. by a dynamic broker config change)
// must clear the connection's expiry. Before the fix the old, already-passed
// expiry stuck around and every subsequent request on the connection re-ran
// the full handshake+authenticate flow first.
func TestAuditSaslReauthLifetimeClearedWhenDisabled(t *testing.T) {
	t.Parallel()
	c := newCluster(t,
		NumBrokers(1),
		EnableSASL(),
		Superuser("PLAIN", "admin", "admin"),
		// Lifetime 1.5s => client expiry ~0.5s after authenticating
		// (1s minimum pessimism).
		BrokerConfigs(map[string]string{"connections.max.reauth.ms": "1500"}),
	)
	handshakes := countHandshakes(c)

	cl := newPlainClient(t, c, saslPlainOpts(c)...)
	br := discoverBroker(t, cl)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Warm the broker connection; its session carries the 1.5s lifetime.
	if _, err := br.Request(ctx, kmsg.NewPtrMetadataRequest()); err != nil {
		t.Fatalf("warm request failed: %v", err)
	}

	// Disable reauthentication broker-side.
	alter := kmsg.NewPtrIncrementalAlterConfigsRequest()
	res := kmsg.NewIncrementalAlterConfigsRequestResource()
	res.ResourceType = kmsg.ConfigResourceTypeBroker
	cfg := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	cfg.Name = "connections.max.reauth.ms"
	val := "0"
	cfg.Value = &val
	res.Configs = append(res.Configs, cfg)
	alter.Resources = append(alter.Resources, res)
	alterResp, err := br.Request(ctx, alter)
	if err != nil {
		t.Fatalf("alter configs failed: %v", err)
	}
	if ec := alterResp.(*kmsg.IncrementalAlterConfigsResponse).Resources[0].ErrorCode; ec != 0 {
		t.Fatalf("alter configs error code %d", ec)
	}

	// Cross the expiry; this request reauthenticates and its authenticate
	// response now carries no lifetime.
	time.Sleep(700 * time.Millisecond)
	if _, err := br.Request(ctx, kmsg.NewPtrMetadataRequest()); err != nil {
		t.Fatalf("reauth-triggering request failed: %v", err)
	}
	base := handshakes.Load()

	// Subsequent requests must not re-handshake: the expiry was cleared.
	for i := 0; i < 3; i++ {
		if _, err := br.Request(ctx, kmsg.NewPtrMetadataRequest()); err != nil {
			t.Fatalf("request %d failed: %v", i, err)
		}
	}
	if got := handshakes.Load(); got != base {
		t.Errorf("connection kept reauthenticating after the broker stopped requiring it: handshakes went %d -> %d across 3 plain requests", base, got)
	}
}
