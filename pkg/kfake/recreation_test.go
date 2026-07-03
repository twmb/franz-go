package kfake

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
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

const logSwap = "topic recreation detected"

// recreateTopic deletes and immediately recreates a topic; the new
// incarnation has a fresh topic ID.
func recreateTopic(t *testing.T, cl *kgo.Client, topic string, partitions int32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	del := kmsg.NewPtrDeleteTopicsRequest()
	del.TopicNames = []string{topic}
	dt := kmsg.NewDeleteTopicsRequestTopic()
	dt.Topic = kmsg.StringPtr(topic)
	del.Topics = append(del.Topics, dt)
	delResp, err := del.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("delete topic: %v", err)
	}
	if ec := delResp.Topics[0].ErrorCode; ec != 0 {
		t.Fatalf("delete topic: %v", kerr.ErrorForCode(ec))
	}

	create := kmsg.NewPtrCreateTopicsRequest()
	ct := kmsg.NewCreateTopicsRequestTopic()
	ct.Topic = topic
	ct.NumPartitions = partitions
	ct.ReplicationFactor = 1
	create.Topics = append(create.Topics, ct)
	createResp, err := create.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}
	if ec := createResp.Topics[0].ErrorCode; ec != 0 {
		t.Fatalf("create topic: %v", kerr.ErrorForCode(ec))
	}
}

// produceVals produces the given values to the given partition.
func produceVals(t *testing.T, c *Cluster, topic string, partition int32, vals ...string) {
	t.Helper()
	cl := newPlainClient(t, c, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for _, v := range vals {
		r := &kgo.Record{Topic: topic, Partition: partition, Value: []byte(v)}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatalf("produce %q: %v", v, err)
		}
	}
}

// collectVals polls until the wanted values (as a set) have all arrived, and
// fails on any unexpected value (e.g. re-read old-incarnation records).
func collectVals(t *testing.T, cl *kgo.Client, want ...string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	need := make(map[string]bool, len(want))
	for _, v := range want {
		need[v] = true
	}
	for ctx.Err() == nil && len(need) > 0 {
		fetches := cl.PollFetches(ctx)
		fetches.EachRecord(func(r *kgo.Record) {
			v := string(r.Value)
			if !need[v] {
				t.Errorf("unexpected record value %q", v)
				return
			}
			delete(need, v)
		})
	}
	if len(need) > 0 {
		t.Fatalf("missing records: %v", need)
	}
}

// A mid-consume recreation with the same leader and epoch 0 => epoch 0 (no
// topicPartitionData change at all) must still be detected, adopt the new
// ID, and reset per ConsumeResetOffset.
func TestRecreationConsumerSwap(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	lg := new(capLogger)
	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.WithLogger(lg),
	)

	produceVals(t, c, topic, 0, "v0", "v1", "v2")
	collectVals(t, cl, "v0", "v1", "v2")

	recreateTopic(t, cl, topic, 1)
	produceVals(t, c, topic, 0, "n0", "n1", "n2")
	collectVals(t, cl, "n0", "n1", "n2")

	if lg.count(logSwap) == 0 {
		t.Error("expected a recreation swap log line")
	}
}

// Same as above, but the new incarnation's partition moves to a different
// leader before the consumer detects the recreation.
func TestRecreationConsumerSwapLeaderChange(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(2), SeedTopics(1, topic))
	lg := new(capLogger)
	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.WithLogger(lg),
	)

	produceVals(t, c, topic, 0, "v0", "v1", "v2")
	collectVals(t, cl, "v0", "v1", "v2")

	oldLeader := c.LeaderFor(topic, 0)
	recreateTopic(t, cl, topic, 1)
	if err := c.MoveTopicPartition(topic, 0, 1-oldLeader); err != nil {
		t.Fatal(err)
	}
	produceVals(t, c, topic, 0, "n0", "n1", "n2")
	collectVals(t, cl, "n0", "n1", "n2")

	if lg.count(logSwap) == 0 {
		t.Error("expected a recreation swap log line")
	}
}

// Recreation with MORE partitions: existing cursors swap, the added
// partitions are picked up as new assignments.
func TestRecreationConsumerSwapGrow(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)

	produceVals(t, c, topic, 0, "v0")
	collectVals(t, cl, "v0")

	recreateTopic(t, cl, topic, 2)
	produceVals(t, c, topic, 0, "n0")
	produceVals(t, c, topic, 1, "n1")
	collectVals(t, cl, "n0", "n1")
}

// Recreation with FEWER partitions: surviving partitions swap and continue;
// the vanished partition's cursor stalls loudly (bounded UNKNOWN_TOPIC_ID
// bubbling), never silently reading anything.
func TestRecreationConsumerSwapShrink(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(2, topic))
	lg := new(capLogger)
	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.WithLogger(lg),
	)

	produceVals(t, c, topic, 0, "v0")
	produceVals(t, c, topic, 1, "v1")
	collectVals(t, cl, "v0", "v1")

	recreateTopic(t, cl, topic, 1)
	produceVals(t, c, topic, 0, "n0")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var gotN0, gotErr bool
	for ctx.Err() == nil && (!gotN0 || !gotErr) {
		fetches := cl.PollFetches(ctx)
		fetches.EachRecord(func(r *kgo.Record) {
			if string(r.Value) == "n0" {
				gotN0 = true
			} else {
				t.Errorf("unexpected record value %q", string(r.Value))
			}
		})
		fetches.EachError(func(_ string, p int32, err error) {
			if p == 1 && errors.Is(err, kerr.UnknownTopicID) {
				gotErr = true
			}
		})
	}
	if !gotN0 || !gotErr {
		lg.mu.Lock()
		tail := lg.buf.String()
		lg.mu.Unlock()
		if len(tail) > 6000 {
			tail = tail[len(tail)-6000:]
		}
		t.Fatalf("wanted new partition-0 record and a partition-1 UnknownTopicID error, got record=%v err=%v; log tail:\n%s", gotN0, gotErr, tail)
	}
}

// Regex consumers ride the same merge swap when the recreation happens
// faster than the missing-topic purge.
func TestRecreationConsumerSwapRegex(t *testing.T) {
	t.Parallel()

	const topic = "rt"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c,
		kgo.ConsumeRegex(),
		kgo.ConsumeTopics("rt.*"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)

	produceVals(t, c, topic, 0, "v0")
	collectVals(t, cl, "v0")

	recreateTopic(t, cl, topic, 1)
	produceVals(t, c, topic, 0, "n0")
	collectVals(t, cl, "n0")
}

// A paused partition generates no fetches and thus no corroboration: the
// swap must defer until unpause (the position is frozen meanwhile, so this
// is safe), then complete.
func TestRecreationConsumerSwapPaused(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	lg := new(capLogger)
	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.WithLogger(lg),
	)

	produceVals(t, c, topic, 0, "v0")
	collectVals(t, cl, "v0")

	cl.PauseFetchTopics(topic)
	recreateTopic(t, cl, topic, 1)
	produceVals(t, c, topic, 0, "n0")

	// A few forced refreshes while paused: the merge sees the ID change
	// but must not swap without corroboration.
	for range 5 {
		cl.ForceMetadataRefresh()
		time.Sleep(25 * time.Millisecond)
	}
	if n := lg.count(logSwap); n != 0 {
		t.Fatalf("swap happened %d times while paused; want deferral", n)
	}

	cl.ResumeFetchTopics(topic)
	collectVals(t, cl, "n0")
	if lg.count(logSwap) == 0 {
		t.Error("expected a recreation swap log line after unpause")
	}
}

// A classic consumer group keeps consuming across a recreation.
func TestRecreationGroupClassic(t *testing.T) {
	t.Parallel()

	const topic, group = "t", "g"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c,
		kgo.MaxVersions(kversion.V3_7_0()),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)

	produceVals(t, c, topic, 0, "v0", "v1", "v2")
	collectVals(t, cl, "v0", "v1", "v2")

	recreateTopic(t, cl, topic, 1)
	produceVals(t, c, topic, 0, "n0", "n1", "n2")
	collectVals(t, cl, "n0", "n1", "n2")
}

// A KIP-848 group keeps consuming across a recreation: adopting the new ID
// into id2t is also what resolves the 848 assignment of the new incarnation
// (pre-change, the assignment spun unresolved).
func TestRecreationGroup848(t *testing.T) {
	t.Parallel()

	const topic, group = "t", "g848"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)

	produceVals(t, c, topic, 0, "v0", "v1", "v2")
	collectVals(t, cl, "v0", "v1", "v2")

	recreateTopic(t, cl, topic, 1)
	produceVals(t, c, topic, 0, "n0", "n1", "n2")
	collectVals(t, cl, "n0", "n1", "n2")
}

// Below the gate (by-name fetch), recreation behavior is UNCHANGED: no
// adoption, no reset. In this offset geometry (old position == new log end)
// the consumer silently sees nothing, which is today's documented stall.
func TestRecreationDisarmedUnchanged(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic), MaxVersions(kversion.V3_0_0()))
	lg := new(capLogger)
	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.WithLogger(lg),
	)

	produceVals(t, c, topic, 0, "v0", "v1", "v2")
	collectVals(t, cl, "v0", "v1", "v2")

	recreateTopic(t, cl, topic, 1)
	produceVals(t, c, topic, 0, "n0", "n1", "n2")

	verifyZeroRecords(t, cl, 500*time.Millisecond)
	if n := lg.count(logSwap); n != 0 {
		t.Errorf("swap happened %d times below the gate; want unchanged behavior", n)
	}
}
