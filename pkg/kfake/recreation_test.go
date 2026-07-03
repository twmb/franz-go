package kfake

import (
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

// capLogger captures log lines so tests can assert on internal transitions
// that have no other observable API (e.g. the recreation gate).
type capLogger struct {
	mu  sync.Mutex
	buf strings.Builder
}

func (*capLogger) Level() kgo.LogLevel { return kgo.LogLevelInfo }

func (lg *capLogger) Log(_ kgo.LogLevel, msg string, keyvals ...any) {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	fmt.Fprintf(&lg.buf, "%s %v\n", msg, keyvals)
}

func (lg *capLogger) count(substr string) int {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	return strings.Count(lg.buf.String(), substr)
}

const (
	logGateArmed    = "topic recreation handling armed"
	logGateDisarmed = "topic recreation handling disarmed"
)

// waitForLog forces metadata refreshes until the log line has been seen at
// least n times.
func waitForLog(t *testing.T, cl *kgo.Client, lg *capLogger, substr string, n int) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if lg.count(substr) >= n {
			return
		}
		cl.ForceMetadataRefresh()
		time.Sleep(25 * time.Millisecond)
	}
	lg.mu.Lock()
	tail := lg.buf.String()
	lg.mu.Unlock()
	if len(tail) > 4000 {
		tail = tail[len(tail)-4000:]
	}
	t.Fatalf("timed out waiting for %dx %q in logs; log tail:\n%s", n, substr, tail)
}

// The recreation gate requires every broker with negotiated versions to
// support fetch v13. It must disarm when a below-v13 broker joins the
// cluster and is contacted, and re-arm when that broker leaves.
func TestRecreationGateTransitions(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(2), SeedTopics(1, topic))
	lg := new(capLogger)
	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.WithLogger(lg),
	)

	produceN(t, c, topic, 3)
	collectRecords(t, cl, 3, 5*time.Second)
	waitForLog(t, cl, lg, logGateArmed, 1)

	// Cap fetch to v12 on the node we are about to add. The control is
	// registered before the node exists so its very first ApiVersions
	// negotiation is already capped.
	c.ControlKey(18, func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if c.CurrentNode() != 2 {
			return nil, nil, false
		}
		kresp, err := c.handleApiVersions(kreq)
		if err != nil {
			return nil, err, true
		}
		resp := kresp.(*kmsg.ApiVersionsResponse)
		for i := range resp.ApiKeys {
			if resp.ApiKeys[i].ApiKey == 1 && resp.ApiKeys[i].MaxVersion > 12 {
				resp.ApiKeys[i].MaxVersion = 12
			}
		}
		return resp, nil, true
	})

	node, _, err := c.AddNode(-1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if node != 2 {
		t.Fatalf("expected added node 2, got %d", node)
	}
	// Adding a node shuffles leadership randomly; force our partition onto
	// the new node so the consumer must contact (and negotiate with) it.
	if err := c.MoveTopicPartition(topic, 0, node); err != nil {
		t.Fatal(err)
	}

	produceN(t, c, topic, 3)
	collectRecords(t, cl, 3, 5*time.Second)
	waitForLog(t, cl, lg, logGateDisarmed, 1)

	// Removing the node reshuffles leadership onto the remaining v13
	// brokers; with the v12 broker gone the gate must re-arm.
	if err := c.RemoveNode(node); err != nil {
		t.Fatal(err)
	}
	waitForLog(t, cl, lg, logGateArmed, 2)

	produceN(t, c, topic, 3)
	collectRecords(t, cl, 3, 5*time.Second)
}

// The gate must never arm when fetch v13 cannot be negotiated, whether the
// client or the cluster is what caps the version.
func TestRecreationGateNeverArms(t *testing.T) {
	t.Parallel()

	const topic = "t"
	for _, tc := range []struct {
		name        string
		clusterOpts []Opt
		clientOpts  []kgo.Opt
	}{
		{name: "client-capped", clientOpts: []kgo.Opt{kgo.MaxVersions(kversion.V3_0_0())}},
		{name: "cluster-pinned", clusterOpts: []Opt{MaxVersions(kversion.V3_0_0())}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			c := newCluster(t, append([]Opt{SeedTopics(1, topic)}, tc.clusterOpts...)...)
			lg := new(capLogger)
			cl := newPlainClient(t, c, append([]kgo.Opt{
				kgo.ConsumeTopics(topic),
				kgo.FetchMaxWait(250 * time.Millisecond),
				kgo.WithLogger(lg),
			}, tc.clientOpts...)...)

			produceN(t, c, topic, 3)
			collectRecords(t, cl, 3, 5*time.Second)

			// Consuming succeeded, so metadata (with negotiated
			// versions) has been evaluated at least once.
			cl.ForceMetadataRefresh()
			time.Sleep(100 * time.Millisecond)
			if n := lg.count(logGateArmed); n > 0 {
				t.Errorf("gate armed %d times; want never", n)
			}
		})
	}
}
