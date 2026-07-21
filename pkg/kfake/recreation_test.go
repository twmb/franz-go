package kfake

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
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
	lvl kgo.LogLevel // defaults to info
}

func (lg *capLogger) Level() kgo.LogLevel {
	if lg.lvl == kgo.LogLevelNone {
		return kgo.LogLevelInfo
	}
	return lg.lvl
}

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
	deleteTopic(t, cl, topic)
	createTopic(t, cl, topic, partitions)
}

func deleteTopic(t *testing.T, cl *kgo.Client, topic string) {
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
}

func createTopic(t *testing.T, cl *kgo.Client, topic string, partitions int32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

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

// A recreated topic restarts consumption from ITS beginning, regardless of
// ConsumeResetOffset: a subscription is a point in time and everything
// after, and everything in the replacement topic arrived after that point.
// An at-end reset policy must not skip the new topic's records.
func TestRecreationResetsToStart(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	lg := new(capLogger)
	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.WithLogger(lg),
	)
	admin := newPlainClient(t, c)

	// The at-end policy applies to the initial subscription: records
	// produced after it are consumed.
	verifyZeroRecords(t, cl, 300*time.Millisecond)
	produceVals(t, c, topic, 0, "v0", "v1")
	collectVals(t, cl, "v0", "v1")

	// Recreate and produce into the new incarnation BEFORE the client
	// swaps: an at-end reset would skip n0/n1; starting from the new
	// topic's beginning delivers them.
	recreateTopic(t, admin, topic, 1)
	produceVals(t, c, topic, 0, "n0", "n1")
	collectVals(t, cl, "n0", "n1")
	if lg.count(logSwap) == 0 {
		t.Error("expected a recreation swap log line")
	}
}

// deleteRecordsTo advances a partition's log start offset.
func deleteRecordsTo(t *testing.T, cl *kgo.Client, topic string, partition int32, offset int64) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := kmsg.NewPtrDeleteRecordsRequest()
	rt := kmsg.NewDeleteRecordsRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewDeleteRecordsRequestTopicPartition()
	rp.Partition = partition
	rp.Offset = offset
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("delete records: %v", err)
	}
	if ec := resp.Topics[0].Partitions[0].ErrorCode; ec != 0 {
		t.Fatalf("delete records: %v", kerr.ErrorForCode(ec))
	}
}

// A recreation restart that has not consumed yet stays pinned to the
// topic's earliest offset: if the new topic truncates between the restart
// resolving and the first fetch, the resulting out-of-range re-resolves the
// earliest offset (here 5) rather than falling back to ConsumeResetOffset
// (at-end here, which would skip the topic's whole remainder).
func TestRecreationRestartThenTruncate(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic), MaxVersions(kversion.V3_0_0()))
	lg := new(capLogger)
	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.WithLogger(lg),
	)
	admin := newPlainClient(t, c)

	verifyZeroRecords(t, cl, 300*time.Millisecond)
	produceVals(t, c, topic, 0, "v0", "v1")
	collectVals(t, cl, "v0", "v1")

	// Recreate; the below-the-gate adoption works from metadata alone, so
	// the swap lands while paused.
	cl.PauseFetchTopics(topic)
	time.Sleep(300 * time.Millisecond)
	recreateTopic(t, admin, topic, 1)
	waitForLog(t, cl, lg, logSwap, 1)

	// Position the restart at offset 0 of the empty new topic.
	cl.ResumeFetchTopics(topic)
	verifyZeroRecords(t, cl, 300*time.Millisecond)

	// Truncate under the unconsumed restart, then resume: the fetch at 0
	// is out of range and must re-resolve to the earliest offset (5).
	cl.PauseFetchTopics(topic)
	time.Sleep(300 * time.Millisecond)
	produceVals(t, c, topic, 0, "n0", "n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9")
	deleteRecordsTo(t, admin, topic, 0, 5)
	cl.ResumeFetchTopics(topic)

	collectVals(t, cl, "n5", "n6", "n7", "n8", "n9")
	verifyZeroRecords(t, cl, 300*time.Millisecond)
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
	// Pausing does not stop a fetch already in flight; the deletion wakes
	// it and its UNKNOWN_TOPIC_ID rejection would corroborate an early
	// (safe, but not deferred) swap. Let it resolve before recreating so
	// the pause deterministically accrues no corroboration.
	time.Sleep(300 * time.Millisecond)
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

const logTierC = "topic recreation inferred from a persistent leader epoch rewind"

// Tier C (2.1-2.7: leader epochs in metadata, no topic IDs): a recreation
// whose old incarnation had epoch > 0 shows as a persistent epoch rewind,
// which the merge treats as a recreation once it survives the consecutive
// rewind bound: the consumer resets per policy with an honest notification.
func TestRecreationTierCConsumer(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(2), SeedTopics(1, topic), MaxVersions(kversion.V2_7_0()))
	lg := new(capLogger)
	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.WithLogger(lg),
	)

	// Bump the live incarnation's epoch above zero so its replacement is
	// visible as a rewind (epoch-0 recreations are invisible in this tier).
	oldLeader := c.LeaderFor(topic, 0)
	if err := c.MoveTopicPartition(topic, 0, 1-oldLeader); err != nil {
		t.Fatal(err)
	}

	produceVals(t, c, topic, 0, "v0", "v1", "v2")
	collectVals(t, cl, "v0", "v1", "v2")

	recreateTopic(t, cl, topic, 1)
	waitForLog(t, cl, lg, logTierC, 1)
	if lg.count(logTierCAmbiguous) == 0 {
		t.Error("a small rewind is ambiguous (recreation or lost epoch history) and must say so")
	}

	produceVals(t, c, topic, 0, "n0", "n1", "n2")
	collectVals(t, cl, "n0", "n1", "n2")
}

const (
	logTierCAmbiguous = "or epoch history was lost after unclean elections"
	logTierCFresh     = "persistent leader epoch rewind onto a fresh lineage"
)

// A persistent rewind from well above onto a nearly virgin lineage (new
// epoch <= 2, consumed >= 3 higher) is treated as a certain recreation and
// restarts from the new topic's beginning. The discriminator here is
// event-time stamping: the new incarnation's records carry timestamps far
// older than anything consumed, so the ambiguous nearest-timestamp reset
// would resolve past them to the log end and never deliver them.
func TestRecreationTierCFreshLineage(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(2), SeedTopics(1, topic), MaxVersions(kversion.V2_7_0()))
	lg := new(capLogger)
	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.WithLogger(lg),
	)
	admin := newPlainClient(t, c)

	// Three moves: consumed records carry leader epoch 3, so the rewind
	// onto the new incarnation's epoch 0 lands >= 3 below what we consumed.
	cur := c.LeaderFor(topic, 0)
	for range 3 {
		cur = 1 - cur
		if err := c.MoveTopicPartition(topic, 0, cur); err != nil {
			t.Fatal(err)
		}
	}
	produceVals(t, c, topic, 0, "v0", "v1")
	collectVals(t, cl, "v0", "v1")

	recreateTopic(t, admin, topic, 1)

	// Republish history into the new incarnation: event-time stamps far
	// before anything the consumer saw.
	pcl := newPlainClient(t, c, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	old := time.Now().Add(-24 * time.Hour)
	for _, v := range []string{"h0", "h1"} {
		r := &kgo.Record{Topic: topic, Partition: 0, Value: []byte(v), Timestamp: old}
		if err := pcl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatalf("produce %q: %v", v, err)
		}
	}

	waitForLog(t, cl, lg, logTierCFresh, 1)
	collectVals(t, cl, "h0", "h1")
}

// Tier C producer: the persistent rewind restarts produce sequences, and
// buffered records ride onto the new incarnation.
func TestRecreationTierCProducer(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(2), SeedTopics(1, topic), MaxVersions(kversion.V2_7_0()))
	lg := new(capLogger)
	cl := newPlainClient(t, c,
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.WithLogger(lg),
	)
	admin := newPlainClient(t, c)

	// Two moves: the old incarnation ends at epoch 2, so the new
	// incarnation stays visibly below it even after its own move.
	oldLeader := c.LeaderFor(topic, 0)
	if err := c.MoveTopicPartition(topic, 0, 1-oldLeader); err != nil {
		t.Fatal(err)
	}
	if err := c.MoveTopicPartition(topic, 0, oldLeader); err != nil {
		t.Fatal(err)
	}

	produceSync(t, cl, topic, "p0")
	recreateTopic(t, admin, topic, 1)
	// Force the new incarnation's leader away from where the client
	// believes the partition lives: produce attempts fail NOT_LEADER
	// until the rewind bound trips and the swap adopts the new state.
	if err := c.MoveTopicPartition(topic, 0, 1-oldLeader); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	cl.Produce(context.Background(), &kgo.Record{Topic: topic, Partition: 0, Value: []byte("p1")}, func(_ *kgo.Record, err error) {
		done <- err
	})
	waitForLog(t, cl, lg, logTierC, 1)
	if err := <-done; err != nil {
		t.Fatalf("produce across the inferred recreation did not heal: %v", err)
	}

	consumeExactly(t, c, topic, "p1")
}

// A churn test: recreate the topic many times under continuous produce and
// group-consume load, then assert the recreation invariants end to end: the
// client never wedges, no surfaced sequence errors, produce failures are
// only the honest unknown-topic classes, and no value is ever delivered
// twice (dup-impossibility under churn; values lost to a deletion are
// definitional, so at-most-once is the assertable half).
func TestRecreationChurn(t *testing.T) {
	t.Parallel()

	const (
		topic     = "t"
		group     = "gchurn"
		recreates = 8
		waveSize  = 10
	)
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	prod := newPlainClient(t, c, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	cons := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.AutoCommitInterval(100*time.Millisecond),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	admin := newPlainClient(t, c)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Producer: waves of async produces; every promise outcome recorded.
	// Failures are tolerated only for the honest unknown-topic classes a
	// recreation can surface; anything else (a sequence error especially)
	// fails the test.
	allowedProduceErr := func(err error) bool {
		return errors.Is(err, kerr.UnknownTopicID) ||
			errors.Is(err, kerr.UnknownTopicOrPartition) ||
			strings.Contains(err.Error(), "metadata update is missing a partition")
	}
	var (
		produceMu   sync.Mutex
		produced    = make(map[string]bool) // value => promise succeeded
		badProduce  []error
		stopProduce = make(chan struct{})
		produceDone = make(chan struct{})
	)
	go func() {
		defer close(produceDone)
		var i int
		for {
			select {
			case <-stopProduce:
				return
			default:
			}
			var wg sync.WaitGroup
			for range waveSize {
				v := fmt.Sprintf("v%05d", i)
				i++
				wg.Add(1)
				prod.Produce(ctx, &kgo.Record{Topic: topic, Partition: 0, Value: []byte(v)}, func(r *kgo.Record, err error) {
					defer wg.Done()
					produceMu.Lock()
					defer produceMu.Unlock()
					if err == nil {
						produced[string(r.Value)] = true
					} else if !allowedProduceErr(err) && ctx.Err() == nil {
						badProduce = append(badProduce, fmt.Errorf("%s: %w", r.Value, err))
					}
				})
			}
			wg.Wait()
		}
	}()

	// Consumer: count every delivered value.
	var (
		consumeMu   sync.Mutex
		consumed    = make(map[string]int)
		badConsume  []error
		consumeDone = make(chan struct{})
	)
	go func() {
		defer close(consumeDone)
		for ctx.Err() == nil {
			fetches := cons.PollFetches(ctx)
			fetches.EachRecord(func(r *kgo.Record) {
				consumeMu.Lock()
				consumed[string(r.Value)]++
				consumeMu.Unlock()
			})
			fetches.EachError(func(_ string, _ int32, err error) {
				switch {
				case errors.Is(err, kerr.UnknownTopicID),
					errors.Is(err, kerr.UnknownTopicOrPartition),
					errors.Is(err, context.Canceled),
					errors.Is(err, context.DeadlineExceeded):
				default:
					consumeMu.Lock()
					badConsume = append(badConsume, err)
					consumeMu.Unlock()
				}
			})
			consumeMu.Lock()
			done := consumed["terminal"] > 0
			consumeMu.Unlock()
			if done {
				return
			}
		}
	}()

	// Churn.
	for range recreates {
		time.Sleep(200 * time.Millisecond)
		recreateTopic(t, admin, topic, 1)
	}
	close(stopProduce)
	<-produceDone

	// Liveness: a terminal produce after the final recreation must heal
	// and be consumed.
	termCtx, termCancel := context.WithTimeout(ctx, 15*time.Second)
	defer termCancel()
	if err := prod.ProduceSync(termCtx, &kgo.Record{Topic: topic, Partition: 0, Value: []byte("terminal")}).FirstErr(); err != nil {
		t.Fatalf("terminal produce did not heal after churn: %v", err)
	}
	select {
	case <-consumeDone:
	case <-ctx.Done():
		t.Fatal("consumer did not reach the terminal value after churn")
	}

	produceMu.Lock()
	defer produceMu.Unlock()
	consumeMu.Lock()
	defer consumeMu.Unlock()

	for _, err := range badProduce {
		t.Errorf("disallowed produce error under churn: %v", err)
	}
	for _, err := range badConsume {
		t.Errorf("disallowed consume error under churn: %v", err)
	}
	if got := consumed["terminal"]; got != 1 {
		t.Errorf("terminal value consumed %d times; want exactly 1", got)
	}
	var totalConsumed int
	for v, n := range consumed {
		totalConsumed += n
		if n > 1 {
			t.Errorf("value %q delivered %d times; recreation handling may never duplicate", v, n)
		}
		if v != "terminal" && !produced[v] {
			// Consumed but its promise failed or never fired: possible
			// only for a value whose ack raced the deletion; it must
			// still be at-most-once (checked above), and it must at
			// least be OURS.
			if !strings.HasPrefix(v, "v") {
				t.Errorf("consumed a value we never produced: %q", v)
			}
		}
	}
	if len(produced) == 0 || totalConsumed == 0 {
		t.Errorf("churn produced/consumed nothing (produced %d, consumed %d); the test exercised nothing", len(produced), totalConsumed)
	}
}

const logEpochGuard = "fetched records carry a leader epoch below what we already consumed"

// The record-batch epoch guard closes Tier B/C's bounded silent window: a
// by-name fetch that lands inside a recreated topic's new incarnation
// carries batch epochs below what we already consumed, and delivery is
// withheld until the merge classifies. Without it, the fetch would silently
// misread records at the stale position (skipping the new incarnation's
// earlier records).
func TestRecreationTierBEpochGuard(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(2), SeedTopics(1, topic), MaxVersions(kversion.V3_0_0()))
	lg := new(capLogger)
	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.WithLogger(lg),
	)
	admin := newPlainClient(t, c)

	// Two moves: consumed records carry leader epoch 2.
	oldLeader := c.LeaderFor(topic, 0)
	if err := c.MoveTopicPartition(topic, 0, 1-oldLeader); err != nil {
		t.Fatal(err)
	}
	if err := c.MoveTopicPartition(topic, 0, oldLeader); err != nil {
		t.Fatal(err)
	}
	produceVals(t, c, topic, 0, "v0", "v1", "v2")
	collectVals(t, cl, "v0", "v1", "v2")

	// Recreate so the next by-name fetch reads new-incarnation records at
	// the stale position: same final leader and same leader epoch as the
	// consumer knows (so nothing fences the fetch), with the records
	// appended while the epoch was still lower.
	cl.PauseFetchTopics(topic)
	time.Sleep(300 * time.Millisecond)
	recreateTopic(t, admin, topic, 1)
	if err := c.MoveTopicPartition(topic, 0, oldLeader); err != nil { // epoch 1
		t.Fatal(err)
	}
	produceVals(t, c, topic, 0, "n0", "n1", "n2", "n3", "n4")         // batches at epoch 1
	if err := c.MoveTopicPartition(topic, 0, oldLeader); err != nil { // self-move: epoch 2, fence passes
		t.Fatal(err)
	}
	cl.ResumeFetchTopics(topic)

	// Every new-incarnation record arrives exactly once: the guard
	// withheld the misread and the swap reset to the start.
	waitForLog(t, cl, lg, logEpochGuard, 1)
	waitForLog(t, cl, lg, logSwap, 1)
	collectVals(t, cl, "n0", "n1", "n2", "n3", "n4")
}

// A partition that never located a first offset never switches
// incarnations on metadata alone - an early switch would let a racing
// old-incarnation committed offset be applied to a partition whose fetches
// then succeed under the new, valid ID. Offset resolution is blocked here
// so the cursor stays unpositioned: with no position there are no fetches,
// so no rejection, and the swap waits. Unblocking positions the cursor, the
// first fetch goes out against the dead ID, and that rejection lands the
// swap.
func TestRecreationUnpositionedWaitsForRejection(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	admin := newPlainClient(t, c)
	produceVals(t, c, topic, 0, "v0", "v1")

	// Answer every offset list with retriable NOT_LEADER until unblocked.
	var blocking atomic.Bool
	blocking.Store(true)
	c.ControlKey(2, func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if !blocking.Load() {
			return nil, nil, false
		}
		req := kreq.(*kmsg.ListOffsetsRequest)
		resp := req.ResponseKind().(*kmsg.ListOffsetsResponse)
		for _, rt := range req.Topics {
			st := kmsg.NewListOffsetsResponseTopic()
			st.Topic = rt.Topic
			for _, rp := range rt.Partitions {
				sp := kmsg.NewListOffsetsResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = kerr.NotLeaderForPartition.Code
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})

	lg := new(capLogger)
	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.WithLogger(lg),
	)
	time.Sleep(300 * time.Millisecond) // let the blocked load attempts begin

	recreateTopic(t, admin, topic, 1)
	for range 4 {
		cl.ForceMetadataRefresh()
		time.Sleep(150 * time.Millisecond)
	}
	if got := lg.count(logSwap); got != 0 {
		t.Fatalf("an unpositioned partition swapped on metadata alone: %d swaps", got)
	}

	produceVals(t, c, topic, 0, "n0", "n1")
	blocking.Store(false)
	waitForLog(t, cl, lg, logSwap, 1)
	collectVals(t, cl, "n0", "n1")
}

// A suspected recreation's confirming metadata update follows in the quick
// cadence rather than waiting out a full MetadataMinAge. The quiet shape: a
// paused consumer produces no fetch errors and nothing corroborates, so the
// periodic refresh is the discovery, and the confirmation used to sit out a
// whole min age behind it.
func TestRecreationConfirmQuickly(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic), MaxVersions(kversion.V3_0_0()))
	lg := new(capLogger)
	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.MetadataMaxAge(4*time.Second),
		kgo.MetadataMinAge(4*time.Second),
		kgo.WithLogger(lg),
	)
	admin := newPlainClient(t, c)

	produceVals(t, c, topic, 0, "v0", "v1")
	collectVals(t, cl, "v0", "v1")

	cl.PauseFetchTopics(topic)
	time.Sleep(300 * time.Millisecond)
	recreateTopic(t, admin, topic, 1)
	start := time.Now()

	// Passive wait: the periodic refresh discovers, the quick round
	// confirms. No forced refreshes; forcing would mask what is measured.
	deadline := time.Now().Add(12 * time.Second)
	for lg.count(logSwap) == 0 && time.Now().Before(deadline) {
		time.Sleep(25 * time.Millisecond)
	}
	if lg.count(logSwap) == 0 {
		t.Fatal("the periodic refresh never confirmed the recreation")
	}
	// Discovery <= one max age (4s) + a quick round (~250ms); without
	// the quick cadence, confirmation adds a full extra min age (4s).
	if elapsed := time.Since(start); elapsed > 5500*time.Millisecond {
		t.Fatalf("swap took %v; want one periodic discovery plus one quick confirmation round, not a full extra min age", elapsed)
	}

	cl.ResumeFetchTopics(topic)
	produceVals(t, c, topic, 0, "n0", "n1")
	collectVals(t, cl, "n0", "n1")
}

// A change BACK to a previously held topic ID is never a fresh recreation
// (IDs are random and never reused): it is stale metadata or split brain.
// Below the fetch-by-ID gate, the two-consecutive-updates rule would
// otherwise adopt a persistently stale broker's view and churn the
// position; a prior ID instead requires a broker to reject the ID we
// currently hold, which a by-name fetch never produces - so the client
// stays on the incarnation it already adopted.
func TestRecreationPriorIDNeedsEvidence(t *testing.T) {
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
	admin := newPlainClient(t, c)

	// Capture a full pre-recreation metadata response, to replay later as
	// a stale broker's view reporting the topic's original ID.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	metaReq := kmsg.NewPtrMetadataRequest()
	mt := kmsg.NewMetadataRequestTopic()
	mt.Topic = kmsg.StringPtr(topic)
	metaReq.Topics = append(metaReq.Topics, mt)
	stale, err := metaReq.RequestWith(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}

	produceVals(t, c, topic, 0, "v0", "v1")
	collectVals(t, cl, "v0", "v1")

	recreateTopic(t, admin, topic, 1)
	waitForLog(t, cl, lg, logSwap, 1)

	// Every metadata answer now replays the pre-recreation view: two
	// consecutive agreeing updates, which would adopt any fresh ID.
	c.ControlKey(3, func(kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		return stale, nil, true
	})
	for range 4 {
		cl.ForceMetadataRefresh()
		time.Sleep(150 * time.Millisecond)
	}
	if got := lg.count(logSwap); got != 1 {
		t.Fatalf("stale replays swapped the consumer back: %d swaps, want 1", got)
	}

	// Still consuming the real, current incarnation.
	produceVals(t, c, topic, 0, "n0", "n1")
	collectVals(t, cl, "n0", "n1")
}

const logGuardDeliver = "delivering records whose leader epoch regressed below what we already consumed"

// The epoch guard's escape hatch, and its pacing. Below 2.8 a recreation
// whose new incarnation catches up to the consumed epoch is invisible to
// metadata (same name, no IDs, no rewind): classification never answers,
// and after five paced withholds the records are delivered loudly rather
// than stalling forever. The withholds are paced by a per-cursor backoff -
// unpaced, the bound would burn out at round-trip speed, long before any
// classifying metadata update could land.
func TestRecreationEpochGuardBound(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(2), SeedTopics(1, topic), MaxVersions(kversion.V2_7_0()))
	lg := new(capLogger)
	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.WithLogger(lg),
	)
	admin := newPlainClient(t, c)

	// Two moves: consumed records carry leader epoch 2.
	oldLeader := c.LeaderFor(topic, 0)
	if err := c.MoveTopicPartition(topic, 0, 1-oldLeader); err != nil {
		t.Fatal(err)
	}
	if err := c.MoveTopicPartition(topic, 0, oldLeader); err != nil {
		t.Fatal(err)
	}
	produceVals(t, c, topic, 0, "v0", "v1", "v2")
	collectVals(t, cl, "v0", "v1", "v2")

	// Recreate masked: records below the consumed epoch at the stale
	// position, but the partition's final epoch and leader match what the
	// consumer knows, so metadata never shows anything actionable.
	cl.PauseFetchTopics(topic)
	time.Sleep(300 * time.Millisecond)
	recreateTopic(t, admin, topic, 1)
	if err := c.MoveTopicPartition(topic, 0, oldLeader); err != nil { // epoch 1
		t.Fatal(err)
	}
	produceVals(t, c, topic, 0, "n0", "n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9")
	if err := c.MoveTopicPartition(topic, 0, oldLeader); err != nil { // epoch 2: no rewind visible
		t.Fatal(err)
	}
	start := time.Now()
	cl.ResumeFetchTopics(topic)

	// Passively wait for the loud delivery (no forced refreshes: the
	// pacing under test must not be masked by extra metadata traffic).
	deadline := time.Now().Add(15 * time.Second)
	for lg.count(logGuardDeliver) == 0 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if lg.count(logGuardDeliver) != 1 {
		t.Fatalf("got %d loud deliveries, want 1", lg.count(logGuardDeliver))
	}
	if got := lg.count(logEpochGuard); got != 5 {
		t.Errorf("got %d withholds before the loud delivery, want 5", got)
	}
	if elapsed := time.Since(start); elapsed < time.Second {
		t.Errorf("withhold bound burned out in %v; want >= 1s of pacing", elapsed)
	}
	if lg.count(logSwap) != 0 || lg.count(logTierC) != 0 {
		t.Error("nothing should have classified this masked recreation")
	}

	// The records at and past the stale position deliver exactly once.
	collectVals(t, cl, "n3", "n4", "n5", "n6", "n7", "n8", "n9")
	verifyZeroRecords(t, cl, 300*time.Millisecond)
}

// OFFSET_OUT_OF_RANGE below the gate defers its reset for one metadata
// classification round: a recreation takes the labeled swap with a SINGLE
// reset, rather than a plain reset that the later swap would repeat,
// re-delivering records.
func TestRecreationTierBOORClassified(t *testing.T) {
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
	admin := newPlainClient(t, c)

	produceVals(t, c, topic, 0, "v0", "v1", "v2", "v3", "v4")
	collectVals(t, cl, "v0", "v1", "v2", "v3", "v4")

	// The new incarnation's log is shorter than the stale position: the
	// next by-name fetch is out of range.
	cl.PauseFetchTopics(topic)
	time.Sleep(300 * time.Millisecond)
	recreateTopic(t, admin, topic, 1)
	produceVals(t, c, topic, 0, "n0", "n1")
	cl.ResumeFetchTopics(topic)

	// Exactly once: a doubled reset would deliver n0/n1 twice and fail
	// collectVals on the duplicate.
	collectVals(t, cl, "n0", "n1")
	verifyZeroRecords(t, cl, 500*time.Millisecond)
	if lg.count(logSwap) == 0 {
		t.Error("expected the out-of-range classification to land the labeled swap")
	}
}

const logOORAmbiguous = "out-of-range classification: the log now ends below our consumed position"

// The masked case below topic IDs: the new incarnation's leader epoch caught
// up to the old one, so metadata never shows a rewind and nothing can prove
// a recreation. The out-of-range deferral then probes OffsetForLeaderEpoch:
// our consumed epoch now ends below our position, which is named honestly
// (substantial truncation, or a recreation - indistinguishable without
// topic IDs), and group commits are fenced and reseeded either way, closing
// the stale-commit window on this path too.
func TestRecreationOORProbeMaskedShrink(t *testing.T) {
	t.Parallel()

	const topic, group = "t", "goor"
	c := newCluster(t, NumBrokers(2), SeedTopics(1, topic), MaxVersions(kversion.V2_7_0()))
	lg := new(capLogger)
	cl := newPlainClient(t, c,
		kgo.MaxVersions(kversion.V2_7_0()),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.AutoCommitInterval(100*time.Millisecond),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.WithLogger(lg),
	)
	admin := newPlainClient(t, c)

	// Old incarnation at epoch 2, position 5.
	leader := c.LeaderFor(topic, 0)
	if err := c.MoveTopicPartition(topic, 0, leader); err != nil { // self-move: epoch 1
		t.Fatal(err)
	}
	if err := c.MoveTopicPartition(topic, 0, leader); err != nil { // self-move: epoch 2
		t.Fatal(err)
	}
	produceVals(t, c, topic, 0, "v0", "v1", "v2", "v3", "v4")
	collectVals(t, cl, "v0", "v1", "v2", "v3", "v4")
	pollCtx, pollCancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	cl.PollFetches(pollCtx)
	pollCancel()
	waitCommitted(t, admin, group, topic, 0, 5)

	// Recreate with the SAME leader and the SAME epoch (two self-moves),
	// but a shorter log: metadata shows nothing unusual, and the next
	// fetch is out of range above the log end.
	cl.PauseFetchTopics(topic)
	time.Sleep(300 * time.Millisecond)
	recreateTopic(t, admin, topic, 1)
	if err := c.MoveTopicPartition(topic, 0, leader); err != nil { // epoch 1
		t.Fatal(err)
	}
	if err := c.MoveTopicPartition(topic, 0, leader); err != nil { // epoch 2
		t.Fatal(err)
	}
	produceVals(t, c, topic, 0, "n0", "n1")
	cl.ResumeFetchTopics(topic)

	// The probe names the ambiguity honestly and the reset seeds a prompt
	// recommit over the stale stored offset 5.
	collectVals(t, cl, "n0", "n1")
	verifyZeroRecords(t, cl, 500*time.Millisecond)
	if lg.count(logOORAmbiguous) == 0 {
		t.Error("expected the out-of-range probe's honest truncation-or-recreation classification")
	}
	if lg.count(logSeedCommit) == 0 {
		t.Error("expected the probe's reset to seed a prompt recommit")
	}
	pollCtx, pollCancel = context.WithTimeout(context.Background(), 300*time.Millisecond)
	cl.PollFetches(pollCtx)
	pollCancel()
	waitCommitted(t, admin, group, topic, 0, 2)
}

// The same out-of-range deferral where nothing corroborates a recreation
// (no IDs, and epoch 0 => epoch 0 so no rewind and no conclusive probe
// answer) resolves to the policy reset with no recreation claim: the probe
// may name the truncation-or-recreation ambiguity, but never fabricates a
// recreation classification.
func TestRecreationTierDOORPlainReset(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic), MaxVersions(kversion.V2_7_0()))
	lg := new(capLogger)
	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.WithLogger(lg),
	)
	admin := newPlainClient(t, c)

	produceVals(t, c, topic, 0, "v0", "v1", "v2", "v3", "v4")
	collectVals(t, cl, "v0", "v1", "v2", "v3", "v4")

	cl.PauseFetchTopics(topic)
	time.Sleep(300 * time.Millisecond)
	recreateTopic(t, admin, topic, 1)
	produceVals(t, c, topic, 0, "n0", "n1")
	cl.ResumeFetchTopics(topic)

	collectVals(t, cl, "n0", "n1")
	verifyZeroRecords(t, cl, 500*time.Millisecond)
	if n := lg.count(logSwap) + lg.count(logTierC); n != 0 {
		t.Errorf("saw %d recreation classifications; want a plain policy reset (nothing corroborates)", n)
	}
}

// Below ID-ful metadata (2.7 and earlier: no topic IDs anywhere), no signal
// exists and recreation behavior is UNCHANGED: no adoption, no reset. In
// this offset geometry (old position == new log end) the consumer silently
// sees nothing, which is today's documented behavior.
func TestRecreationDisarmedUnchanged(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic), MaxVersions(kversion.V2_7_0()))
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

// newOffsetAdmin returns a client for observing stored commits. OffsetFetch
// is pinned to v9: the by-name wire reads what is stored under the name
// regardless of incarnation (the v10+ wire carries topic IDs, which the
// admin's own cache may hold stale across a recreation).
func newOffsetAdmin(t *testing.T, c *Cluster) *kgo.Client {
	t.Helper()
	maxv := kversion.Stable()
	maxv.SetMaxKeyVersion(9, 9) // 9 == offset fetch
	return newPlainClient(t, c, kgo.MaxVersions(maxv))
}

// fetchCommitted returns the committed offset for a group's topic partition,
// or -1 if none.
func fetchCommitted(t *testing.T, cl *kgo.Client, group, topic string, partition int32) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := kmsg.NewPtrOffsetFetchRequest()
	req.Group = group
	rt := kmsg.NewOffsetFetchRequestTopic()
	rt.Topic = topic
	rt.Partitions = []int32{partition}
	req.Topics = append(req.Topics, rt)
	rg := kmsg.NewOffsetFetchRequestGroup()
	rg.Group = group
	rgt := kmsg.NewOffsetFetchRequestGroupTopic()
	rgt.Topic = topic
	rgt.Partitions = []int32{partition}
	rg.Topics = append(rg.Topics, rgt)
	req.Groups = append(req.Groups, rg)

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("offset fetch: %v", err)
	}
	if len(resp.Groups) > 0 {
		for _, rt := range resp.Groups[0].Topics {
			if rt.Topic != topic {
				continue
			}
			for _, rp := range rt.Partitions {
				if rp.Partition == partition {
					return rp.Offset
				}
			}
		}
		return -1
	}
	for _, rt := range resp.Topics {
		if rt.Topic != topic {
			continue
		}
		for _, rp := range rt.Partitions {
			if rp.Partition == partition {
				return rp.Offset
			}
		}
	}
	return -1
}

// waitCommitted polls until the group's committed offset equals want.
func waitCommitted(t *testing.T, cl *kgo.Client, group, topic string, partition int32, want int64) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	var got int64 = -2
	for time.Now().Before(deadline) {
		if got = fetchCommitted(t, cl, group, topic, partition); got == want {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for committed offset %d, last saw %d", want, got)
}

const logSeedCommit = "committing the reset position for a recreated topic partition"

// The <=v9 poison: a commit stored under a recreated name silently
// mispositions the next member to fetch it, and on a quiet topic nothing
// would ever overwrite it (nothing is committable until records are polled).
// The commit fence + seeded recommit must promptly overwrite the stored
// commit with the recreation reset position.
func TestRecreationCommitFenceSeed(t *testing.T) {
	t.Parallel()

	const topic, group = "t", "g"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	lg := new(capLogger)
	cl := newPlainClient(t, c,
		kgo.MaxVersions(kversion.V3_7_0()),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.AutoCommitInterval(100*time.Millisecond),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.WithLogger(lg),
	)
	admin := newOffsetAdmin(t, c)

	produceVals(t, c, topic, 0, "v0", "v1")
	collectVals(t, cl, "v0", "v1")
	// Promote head (default autocommit lags one poll) and wait for the
	// pre-recreation commit to land: this is the future poison.
	pollCtx, pollCancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	cl.PollFetches(pollCtx)
	pollCancel()
	waitCommitted(t, admin, group, topic, 0, 2)

	// Recreate and produce NOTHING: the new incarnation is quiet, so
	// nothing will ever be polled and re-committed. Only the seeded
	// recommit can overwrite the stored 2 with the reset position 0.
	recreateTopic(t, admin, topic, 1)
	waitCommitted(t, admin, group, topic, 0, 0)
	if lg.count(logSeedCommit) == 0 {
		t.Error("expected a seeded recommit log line")
	}

	// The live consumer then consumes the new incarnation from the reset
	// position: nothing lost, nothing duplicated.
	produceVals(t, c, topic, 0, "n0", "n1", "n2")
	collectVals(t, cl, "n0", "n1", "n2")
	pollCtx, pollCancel = context.WithTimeout(context.Background(), 300*time.Millisecond)
	cl.PollFetches(pollCtx)
	pollCancel()
	waitCommitted(t, admin, group, topic, 0, 3)
	cl.Close()

	// A next member inherits the seeded lineage (3), not the poison (2):
	// at 2 it would re-consume n2; at the pre-seed poison it would skip
	// n0/n1 for a fresh group. It must consume nothing.
	cl2 := newPlainClient(t, c,
		kgo.MaxVersions(kversion.V3_7_0()),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	verifyZeroRecords(t, cl2, 500*time.Millisecond)
}

// With autocommit disabled, the swap seeds the reset position but does NOT
// commit: the user's next commit carries it.
func TestRecreationCommitFenceSeedManual(t *testing.T) {
	t.Parallel()

	const topic, group = "t", "gm"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c,
		kgo.MaxVersions(kversion.V3_7_0()),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	admin := newOffsetAdmin(t, c)

	produceVals(t, c, topic, 0, "v0", "v1")
	collectVals(t, cl, "v0", "v1")
	if err := cl.CommitUncommittedOffsets(context.Background()); err != nil {
		t.Fatalf("manual commit: %v", err)
	}
	waitCommitted(t, admin, group, topic, 0, 2)

	recreateTopic(t, admin, topic, 1)

	// The seed lands without a commit; wait for it via the uncommitted
	// view flipping to the reset position (the fence hides the stale 2,
	// the seed then exposes 0).
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if un := cl.UncommittedOffsets(); un != nil {
			if ps, ok := un[topic]; ok {
				if eo, ok := ps[0]; ok && eo.Offset == 0 {
					break
				}
			}
		}
		time.Sleep(25 * time.Millisecond)
	}

	// Broker still stores the stale 2 until the user commits.
	if got := fetchCommitted(t, admin, group, topic, 0); got != 2 {
		t.Fatalf("stored commit changed to %d without a user commit; want the stale 2", got)
	}
	if err := cl.CommitUncommittedOffsets(context.Background()); err != nil {
		t.Fatalf("manual commit: %v", err)
	}
	waitCommitted(t, admin, group, topic, 0, 0)
}

const logProduceSwap = "topic recreation detected, adopting the new topic ID for producing"

// produceSync produces one record on the given client and requires success.
func produceSync(t *testing.T, cl *kgo.Client, topic, val string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r := &kgo.Record{Topic: topic, Partition: 0, Value: []byte(val)}
	if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
		t.Fatalf("produce %q: %v", val, err)
	}
}

// consumeExactly asserts the topic's full contents (from the start) are
// exactly the wanted values: nothing lost, nothing duplicated.
func consumeExactly(t *testing.T, c *Cluster, topic string, want ...string) {
	t.Helper()
	cons := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	collectVals(t, cons, want...)
	verifyZeroRecords(t, cons, 300*time.Millisecond)
}

// An idempotent producer heals across a recreation with no surfaced error,
// no producer ID or epoch change, and a sequence chain restarted at zero:
// dup-impossible at v13, no OOOSN surfaced.
func TestRecreationProduceHeal(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	lg := new(capLogger)
	cl := newPlainClient(t, c,
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.WithLogger(lg),
	)
	admin := newPlainClient(t, c)

	// Record every produce attempt's epoch and first sequence as written
	// on the wire.
	type attempt struct {
		epoch int16
		seq   int32
	}
	var attemptsMu sync.Mutex
	var attempts []attempt
	c.ControlKey(0, func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		preq := kreq.(*kmsg.ProduceRequest)
		attemptsMu.Lock()
		defer attemptsMu.Unlock()
		for i := range preq.Topics {
			for j := range preq.Topics[i].Partitions {
				var b kmsg.RecordBatch
				if err := b.ReadFrom(preq.Topics[i].Partitions[j].Records); err == nil {
					attempts = append(attempts, attempt{b.ProducerEpoch, b.FirstSequence})
				}
			}
		}
		return nil, nil, false
	})

	for _, v := range []string{"v0", "v1", "v2"} {
		produceSync(t, cl, topic, v)
	}
	recreateTopic(t, admin, topic, 1)
	for _, v := range []string{"n0", "n1", "n2"} {
		produceSync(t, cl, topic, v)
	}

	if n := lg.count("failing the producer ID"); n != 0 {
		t.Errorf("producer ID was failed %d times; want a heal with no ID reload", n)
	}
	if n := lg.count(logProduceSwap); n == 0 {
		t.Error("expected a produce swap log line")
	}

	// The last three attempts are the healed chain: sequences restart at
	// zero, epoch unchanged from the very first attempt.
	attemptsMu.Lock()
	defer attemptsMu.Unlock()
	if len(attempts) < 6 {
		t.Fatalf("saw %d produce attempts, want at least 6", len(attempts))
	}
	epoch := attempts[0].epoch
	for i, a := range attempts {
		if a.epoch != epoch {
			t.Errorf("attempt %d used epoch %d; want the initial epoch %d for every attempt", i, a.epoch, epoch)
		}
	}
	last3 := attempts[len(attempts)-3:]
	for i, want := range []int32{0, 1, 2} {
		if last3[i].seq != want {
			t.Errorf("healed attempt %d has sequence %d; want %d (chain restarted at zero)", i, last3[i].seq, want)
		}
	}

	consumeExactly(t, c, topic, "n0", "n1", "n2")
}

// A recreation that lands while a produce request is in flight cannot
// duplicate: the in-flight request addressed the dead incarnation's ID and
// is rejected before reaching any log, and the retry heals into the new
// incarnation exactly once.
func TestRecreationProduceInflight(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	admin := newPlainClient(t, c)

	produceSync(t, cl, topic, "v0")

	// Hold the next produce in flight while the topic is recreated under
	// it, then let the broker process it against the post-recreation state.
	recreated := make(chan struct{})
	var held bool
	c.ControlKey(0, func(kmsg.Request) (kmsg.Response, error, bool) {
		if held {
			return nil, nil, false
		}
		held = true
		c.SleepControl(func() { <-recreated })
		return nil, nil, false
	})

	done := make(chan error, 1)
	cl.Produce(context.Background(), &kgo.Record{Topic: topic, Partition: 0, Value: []byte("h0")}, func(_ *kgo.Record, err error) {
		done <- err
	})
	recreateTopic(t, admin, topic, 1)
	close(recreated)

	if err := <-done; err != nil {
		t.Fatalf("in-flight produce did not heal: %v", err)
	}
	produceSync(t, cl, topic, "n1")

	consumeExactly(t, c, topic, "h0", "n1")
}

// Below v13 the produce wire addresses topics by name, and a batch whose
// by-name outcome was never resolved may already sit in a recreated topic's
// new incarnation: re-producing it could not be deduplicated, so the swap
// must fail the partition's buffered records loudly, then continue cleanly.
func TestRecreationProduceUnsureByName(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	maxv := kversion.Stable()
	maxv.SetMaxKeyVersion(0, 12) // produce by name; fetch stays v13+ so the gate arms
	lg := new(capLogger)
	cl := newPlainClient(t, c,
		kgo.MaxVersions(maxv),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.WithLogger(lg),
	)
	admin := newPlainClient(t, c)

	produceSync(t, cl, topic, "p0")

	// Time out every produce until the topic is deleted: the outcome of
	// anything in flight becomes unknowable. After deletion, attempts flow
	// to the broker again (and fail as unknown, corroborating the swap).
	var timeouts atomic.Int32
	var deleted atomic.Bool
	c.ControlKey(0, func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if deleted.Load() {
			return nil, nil, false
		}
		preq := kreq.(*kmsg.ProduceRequest)
		resp := preq.ResponseKind().(*kmsg.ProduceResponse)
		for i := range preq.Topics {
			rt := &preq.Topics[i]
			st := kmsg.NewProduceResponseTopic()
			st.Topic = rt.Topic
			st.TopicID = rt.TopicID
			for _, rp := range rt.Partitions {
				sp := kmsg.NewProduceResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = kerr.RequestTimedOut.Code
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		timeouts.Add(1)
		return resp, nil, true
	})

	done := make(chan error, 1)
	cl.Produce(context.Background(), &kgo.Record{Topic: topic, Partition: 0, Value: []byte("u0")}, func(_ *kgo.Record, err error) {
		done <- err
	})

	// At least one attempt must have received the timed-out response
	// before the recreation, marking its by-name outcome unknowable.
	deadline := time.Now().Add(5 * time.Second)
	for timeouts.Load() == 0 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if timeouts.Load() == 0 {
		t.Fatal("no produce attempt was timed out")
	}

	deleteTopic(t, admin, topic)
	deleted.Store(true)
	// A couple of merges during the deletion gap corroborate via the
	// missing-partition load error (kept under the unknown-fail limit).
	for range 2 {
		cl.ForceMetadataRefresh()
		time.Sleep(25 * time.Millisecond)
	}
	createTopic(t, admin, topic, 1)

	// The swap lands on the next metadata update; force them rather than
	// waiting out the client's min-age cadence.
	var err error
	failDeadline := time.Now().Add(5 * time.Second)
wait:
	for {
		select {
		case err = <-done:
			break wait
		default:
			if time.Now().After(failDeadline) {
				t.Fatal("timed out waiting for the unsure by-name batch to fail")
			}
			cl.ForceMetadataRefresh()
			time.Sleep(25 * time.Millisecond)
		}
	}
	if err == nil {
		t.Fatal("unsure by-name batch was produced across the recreation; want a loud failure")
	}
	if !strings.Contains(err.Error(), "deleted and recreated") {
		t.Fatalf("unsure by-name batch failed with %v; want the recreation unsure-batch error", err)
	}
	if n := lg.count(logProduceSwap); n == 0 {
		t.Error("expected a produce swap log line")
	}

	// The failure is scoped to what was buffered: new produces continue
	// cleanly on the new incarnation.
	produceSync(t, cl, topic, "p1")
	consumeExactly(t, c, topic, "p1")
}

// consumeCommitted asserts a topic's full read-committed contents are
// exactly the wanted values.
func consumeCommitted(t *testing.T, c *Cluster, topic string, want ...string) {
	t.Helper()
	cons := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	if len(want) > 0 {
		collectVals(t, cons, want...)
	}
	verifyZeroRecords(t, cons, 300*time.Millisecond)
}

// txnProduceSync produces one record inside the current transaction and
// returns the promise error.
func txnProduceSync(t *testing.T, cl *kgo.Client, topic, val string) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r := &kgo.Record{Topic: topic, Partition: 0, Value: []byte(val)}
	return cl.ProduceSync(ctx, r).FirstErr()
}

// A transaction whose topic is recreated mid-transaction fails with an
// abortable error (never a silent partial commit); aborting recovers, and
// the next transaction produces cleanly to the new incarnation. Modern path:
// KIP-890p2, produce v13 by ID.
func TestRecreationTxnAborts(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	txcl := newPlainClient(t, c,
		kgo.TransactionalID("tx-recreate"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	admin := newPlainClient(t, c)

	// A first transaction commits normally.
	if err := txcl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := txnProduceSync(t, txcl, topic, "a0"); err != nil {
		t.Fatal(err)
	}
	if err := txcl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}

	// The second transaction spans the recreation: its produce fails
	// abortable, commit is refused, abort recovers.
	if err := txcl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := txnProduceSync(t, txcl, topic, "b0"); err != nil {
		t.Fatal(err)
	}
	recreateTopic(t, admin, topic, 1)
	if err := txnProduceSync(t, txcl, topic, "b1"); !errors.Is(err, kerr.TransactionAbortable) {
		t.Fatalf("produce across recreation got %v; want an abortable transaction error", err)
	}
	err := txcl.EndTransaction(ctx, kgo.TryCommit)
	if !errors.Is(err, kerr.OperationNotAttempted) {
		t.Fatalf("commit got %v; want a refusal wrapping OperationNotAttempted", err)
	}
	if !errors.Is(err, kerr.TransactionAbortable) {
		t.Fatalf("commit refusal %v does not carry the abortable recreation reason", err)
	}
	if err := txcl.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatalf("abort after recreation: %v", err)
	}

	// The next transaction is clean on the new incarnation.
	if err := txcl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := txnProduceSync(t, txcl, topic, "c0"); err != nil {
		t.Fatal(err)
	}
	if err := txcl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}

	consumeCommitted(t, c, topic, "c0")
}

// Same shape, pre-KIP-890p2 (produce v11 by name, EndTxn v4): the
// post-recreation write lands silently in the new incarnation, the acked
// offset regression poisons the transaction, and recovery works because the
// recreation sentinel is recognized in the pre-890p2 recovery arm (raw
// TransactionAbortable is not recoverable there).
func TestRecreationTxnAbortsPre890p2(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	lg := new(capLogger)
	txcl := newPlainClient(t, c,
		kgo.MaxVersions(kversion.V3_7_0()),
		kgo.TransactionalID("tx-recreate-pre890p2"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.WithLogger(lg),
	)
	admin := newPlainClient(t, c)

	if err := txcl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := txnProduceSync(t, txcl, topic, "b0"); err != nil {
		t.Fatal(err)
	}
	recreateTopic(t, admin, topic, 1)
	// By-name produce silently lands in the new incarnation; the accepted
	// offset regression is what poisons the transaction. The promise
	// itself is not an error (the write was accepted).
	if err := txnProduceSync(t, txcl, topic, "b1"); err != nil && !errors.Is(err, kerr.TransactionAbortable) {
		t.Fatalf("produce across recreation got %v; want success (silent by-name landing) or the abortable poison", err)
	}
	err := txcl.EndTransaction(ctx, kgo.TryCommit)
	if err == nil {
		t.Fatal("commit across a recreation succeeded; want a refusal")
	}
	if !errors.Is(err, kerr.TransactionAbortable) {
		t.Fatalf("commit refusal %v does not carry the abortable recreation reason", err)
	}
	if err := txcl.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatalf("abort after recreation (pre-890p2 recovery): %v", err)
	}

	// The offset-regression refresh drives the swap asynchronously; wait
	// for it so the next transaction deterministically starts on the new
	// incarnation (a swap landing mid-transaction poisons that
	// transaction too, by design).
	waitForLog(t, txcl, lg, logSwap, 1)

	if err := txcl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := txnProduceSync(t, txcl, topic, "c0"); err != nil {
		t.Fatal(err)
	}
	if err := txcl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}

	consumeCommitted(t, c, topic, "c0")
}

// Shape 2: a transaction produces to a topic, the topic is recreated, and
// the transaction never touches it again -- no response exists to inspect at
// any produce version. Only the commit-time verification can catch it.
func TestRecreationTxnShape2(t *testing.T) {
	t.Parallel()

	const foo, bar = "foo", "bar"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, foo, bar))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	lg := new(capLogger)
	txcl := newPlainClient(t, c,
		kgo.TransactionalID("tx-shape2"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		// The commit-time verification amortizes against recent metadata
		// passes; the smallest min age keeps this test on the fetch path.
		kgo.MetadataMinAge(10*time.Millisecond),
		kgo.WithLogger(lg),
	)
	admin := newPlainClient(t, c)

	if err := txcl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := txnProduceSync(t, txcl, foo, "f0"); err != nil {
		t.Fatal(err)
	}
	if err := txnProduceSync(t, txcl, bar, "r0"); err != nil {
		t.Fatal(err)
	}

	// Recreate foo; the transaction never touches foo again, so no
	// response can corroborate anything before the commit.
	recreateTopic(t, admin, foo, 1)

	err := txcl.EndTransaction(ctx, kgo.TryCommit)
	if err == nil {
		t.Fatal("commit of a transaction that wrote to a recreated topic succeeded; want the verification refusal")
	}
	if !errors.Is(err, kerr.TransactionAbortable) {
		t.Fatalf("verification refusal %v does not carry the abortable recreation reason", err)
	}
	if !strings.Contains(err.Error(), "deleted and recreated") {
		t.Fatalf("verification refusal %v does not name the recreation", err)
	}
	if err := txcl.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatalf("abort after verification refusal: %v", err)
	}

	// The verification corroborated the swap; wait for it to land so the
	// next transaction deterministically starts on the new incarnation.
	// (Either swap wording can fire: with the restored addedToTxn still
	// set the merge poisons the already-failed transaction, without it
	// the swap lands alone.)
	waitForLog(t, txcl, lg, logSwap, 1)

	if err := txcl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := txnProduceSync(t, txcl, foo, "f1"); err != nil {
		t.Fatal(err)
	}
	if err := txnProduceSync(t, txcl, bar, "r1"); err != nil {
		t.Fatal(err)
	}
	if err := txcl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}

	consumeCommitted(t, c, foo, "f1")
	consumeCommitted(t, c, bar, "r1")
}

const logTxnObserved = "topic recreation observed with an active transaction exposed to it"

// The metadata merge fails an exposed transaction on the FIRST observation
// of a recreated produced-to topic - no corroboration wait - which is what
// lets commit-time verification trust recent metadata passes instead of
// fetching per commit. The commit here is refused via the poisoned producer
// ID with no verification fetch of its own (the forced refresh is fresh).
func TestRecreationTxnPoisonOnObservation(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	lg := new(capLogger)
	txcl := newPlainClient(t, c,
		kgo.TransactionalID("tx-observe"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.WithLogger(lg),
	)
	admin := newPlainClient(t, c)

	if err := txcl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := txnProduceSync(t, txcl, topic, "a0"); err != nil {
		t.Fatal(err)
	}
	recreateTopic(t, admin, topic, 1)
	waitForLog(t, txcl, lg, logTxnObserved, 1)

	err := txcl.EndTransaction(ctx, kgo.TryCommit)
	if !errors.Is(err, kerr.OperationNotAttempted) || !errors.Is(err, kerr.TransactionAbortable) {
		t.Fatalf("commit got %v; want a refusal carrying the abortable recreation poison", err)
	}
	if err := txcl.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatalf("abort after observation poison: %v", err)
	}

	// The next transaction's produce is addressed to the dead incarnation
	// (the swap waits for wire corroboration), fails abortable, and the
	// corroborating rejection lands the swap; the transaction after that
	// is clean.
	if err := txcl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := txnProduceSync(t, txcl, topic, "b0"); !errors.Is(err, kerr.TransactionAbortable) {
		t.Fatalf("produce against the stale incarnation got %v; want the abortable poison", err)
	}
	if err := txcl.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatal(err)
	}
	waitForLog(t, txcl, lg, logSwap, 1)
	if err := txcl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := txnProduceSync(t, txcl, topic, "c0"); err != nil {
		t.Fatal(err)
	}
	if err := txcl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}

	consumeCommitted(t, c, topic, "c0")
}

// The integration-suite transaction shape, run at pinned broker versions: a
// committed transaction, a recreate, then bounded abort-retry rounds until
// a transaction commits into the new incarnation, with read-committed
// consumption seeing exactly the committed value. This shape caught two
// real bugs the targeted tests missed: a recreation swap clobbering the
// pending epoch-bump sequence reset (OUT_OF_ORDER_SEQUENCE_NUMBER, or a
// fatal fence on a real broker), and kfake's transactional-id re-init not
// aborting the open transaction (a later commit swallowed the prior
// epoch's writes into its range).
func testShapeAt(t *testing.T, name string, vs *kversion.Versions) {
	t.Run(name, func(t *testing.T) {
		t.Parallel()
		const topic = "t"
		opts := []Opt{NumBrokers(1), SeedTopics(1, topic)}
		if vs != nil {
			opts = append(opts, MaxVersions(vs))
		}
		c := newCluster(t, opts...)
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		lg := &capLogger{lvl: kgo.LogLevelDebug}
		cl := newPlainClient(t, c,
			kgo.DefaultProduceTopic(topic),
			kgo.TransactionalID("tx-shape"),
			kgo.WithLogger(lg),
		)
		defer func() {
			if t.Failed() {
				lg.mu.Lock()
				s := lg.buf.String()
				lg.mu.Unlock()
				t.Logf("client log:\n%s", s)
			}
		}()
		admin := newPlainClient(t, c)

		if err := cl.BeginTransaction(); err != nil {
			t.Fatal(err)
		}
		if err := cl.ProduceSync(ctx, kgo.StringRecord("t0")).FirstErr(); err != nil {
			t.Fatal(err)
		}
		if err := cl.EndTransaction(ctx, kgo.TryCommit); err != nil {
			t.Fatal(err)
		}

		recreateTopic(t, admin, topic, 1)

		loud := func(err error) bool {
			// UNKNOWN_PRODUCER_ID: below 2.5, a recreated topic's log
			// never saw this producer and rejects a continued chain.
			return errors.Is(err, kerr.TransactionAbortable) || errors.Is(err, kerr.OperationNotAttempted) ||
				errors.Is(err, kerr.UnknownProducerID)
		}
		var committed string
		for round := 0; ; round++ {
			if round > 8 || ctx.Err() != nil {
				t.Fatal("no commit within budget")
			}
			if err := cl.BeginTransaction(); err != nil {
				t.Fatalf("begin round %d: %v", round, err)
			}
			val := fmt.Sprintf("post%d", round)
			perr := cl.ProduceSync(ctx, kgo.StringRecord(val)).FirstErr()
			var cerr error
			if perr == nil {
				cerr = cl.EndTransaction(ctx, kgo.TryCommit)
			}
			if perr == nil && cerr == nil {
				committed = val
				break
			}
			t.Logf("round %d: produce err %v; commit err %v", round, perr, cerr)
			for _, err := range []error{perr, cerr} {
				if err != nil && !loud(err) {
					t.Fatalf("round %d failed outside loud classes: %v", round, err)
				}
			}
			if err := cl.EndTransaction(ctx, kgo.TryAbort); err != nil {
				t.Fatalf("abort round %d: %v", round, err)
			}
		}
		t.Logf("committed %q", committed)
		consumeCommitted(t, c, topic, committed)
	})
}

func TestRecreationTxnFreshStartShape(t *testing.T) {
	t.Parallel()
	testShapeAt(t, "latest", nil)
	testShapeAt(t, "v3_8", kversion.V3_8_0())
	testShapeAt(t, "v3_0", kversion.V3_0_0())
	testShapeAt(t, "v2_1", kversion.V2_1_0())
	testShapeAt(t, "v0_11", kversion.V0_11_0())
}

// A recreation that lands inside the unconfirmed-EndTxn window: the
// documented TryAbort retry heals without any commit-time verification (the
// prior attempt's fate is sealed), and the following transactions converge
// onto the new incarnation with an abortable poison, never silence.
func TestRecreationTxnUnconfirmedInterplay(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	txcl := newPlainClient(t, c,
		kgo.TransactionalID("tx-unconfirmed-recreate"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	admin := newPlainClient(t, c)

	// One-shot control: the first EndTxn fails with UNKNOWN_SERVER_ERROR
	// without kfake processing it -- the commit outcome is unconfirmed.
	c.ControlKey(int16(kmsg.EndTxn), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		req := kreq.(*kmsg.EndTxnRequest)
		resp := req.ResponseKind().(*kmsg.EndTxnResponse)
		resp.ErrorCode = kerr.UnknownServerError.Code
		return resp, nil, true
	})

	if err := txcl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := txnProduceSync(t, txcl, topic, "a0"); err != nil {
		t.Fatal(err)
	}
	if err := txcl.EndTransaction(ctx, kgo.TryCommit); err == nil {
		t.Fatal("expected the controlled EndTxn to leave the commit unconfirmed")
	}

	// The topic is recreated inside the unconfirmed window; the abort
	// retry must still heal.
	recreateTopic(t, admin, topic, 1)
	if err := txcl.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatalf("unconfirmed abort retry: %v", err)
	}

	// The next transaction produces against the stale incarnation and is
	// poisoned abortable; the one after that is clean.
	if err := txcl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := txnProduceSync(t, txcl, topic, "b0"); !errors.Is(err, kerr.TransactionAbortable) {
		t.Fatalf("produce against the stale incarnation got %v; want the abortable poison", err)
	}
	if err := txcl.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatal(err)
	}
	if err := txcl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := txnProduceSync(t, txcl, topic, "c0"); err != nil {
		t.Fatal(err)
	}
	if err := txcl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}

	consumeCommitted(t, c, topic, "c0")
}

const logShareSwap = "topic recreation detected, adopting the new topic ID for share consuming"

// A share consumer swaps across a recreation: consumption continues on the
// new incarnation (fresh broker-side share state), and acknowledgments of
// records acquired from the dead incarnation are invalidated loudly rather
// than re-addressed -- an ack under the new topic ID could acknowledge an
// unrelated record at the same offset.
func TestRecreationShareSwap(t *testing.T) {
	t.Parallel()

	const topic, group = "t", "sg"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	admin := newPlainClient(t, c)
	setShareAutoOffsetReset(t, admin, group)

	var ackMu sync.Mutex
	var ackErrs []error
	lg := new(capLogger)
	cl := newShareConsumer(t, c, topic, group,
		kgo.ShareAckCallback(func(_ *kgo.Client, results kgo.ShareAckResults) {
			ackMu.Lock()
			defer ackMu.Unlock()
			for _, r := range results {
				if r.Err != nil {
					ackErrs = append(ackErrs, r.Err)
				}
			}
		}),
		kgo.WithLogger(lg),
	)

	// Acquire the old incarnation's records without acknowledging them
	// (they all arrive in one poll, and we do not poll again until after
	// the swap, so the implicit ack never fires for them).
	produceVals(t, c, topic, 0, "v0", "v1", "v2")
	rs := collectRecords(t, cl, 3, 5*time.Second)

	recreateTopic(t, admin, topic, 1)
	waitForLog(t, cl, lg, logShareSwap, 1)

	// Acknowledging the dead incarnation's records is invalidated loudly.
	cl.MarkAcks(kgo.AckAccept, rs...)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.FlushAcks(ctx); err != nil {
		t.Fatalf("flush acks: %v", err)
	}
	ackMu.Lock()
	var sawRecreation bool
	for _, err := range ackErrs {
		if errors.Is(err, kerr.UnknownTopicID) && strings.Contains(err.Error(), "recreated") {
			sawRecreation = true
		}
	}
	ackMu.Unlock()
	if !sawRecreation {
		t.Fatalf("acks of prior-incarnation records were not invalidated with the recreation error; callback errors: %v", ackErrs)
	}

	// The new incarnation starts fresh share state: consumption continues.
	produceVals(t, c, topic, 0, "n0", "n1")
	collectVals(t, cl, "n0", "n1")
}

// Tier B (IDs in metadata, by-name fetch wire, e.g. 2.8-3.0): nothing on the
// wire can corroborate, so the consumer adopts a recreation once two
// consecutive metadata updates agree on the new ID, then resets per policy.
func TestRecreationTierBConsumer(t *testing.T) {
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
	// The first forced update observes the new ID (pending); the retry
	// loop drives the second consecutive observation, which adopts.
	waitForLog(t, cl, lg, logSwap, 1)

	produceVals(t, c, topic, 0, "n0", "n1", "n2")
	collectVals(t, cl, "n0", "n1", "n2")
}

// Tier B producer: with nothing buffered or in flight, the swap adopts on
// two consecutive metadata updates and the next produce continues on the
// new incarnation with a fresh sequence chain.
func TestRecreationTierBProducer(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic), MaxVersions(kversion.V3_0_0()))
	lg := new(capLogger)
	cl := newPlainClient(t, c,
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.WithLogger(lg),
	)
	admin := newPlainClient(t, c)

	produceSync(t, cl, topic, "p0")
	recreateTopic(t, admin, topic, 1)
	waitForLog(t, cl, lg, logProduceSwap, 1)
	produceSync(t, cl, topic, "p1")

	consumeExactly(t, c, topic, "p1")
}

// Tier B transactional: the metadata-fact adoption poisons a transaction
// with state tied to the dead incarnation, abort recovers (pre-890p2
// recovery arm), and the next transaction commits cleanly.
func TestRecreationTierBTxn(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic), MaxVersions(kversion.V3_0_0()))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	lg := new(capLogger)
	txcl := newPlainClient(t, c,
		kgo.TransactionalID("tx-tierb"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.WithLogger(lg),
	)
	admin := newPlainClient(t, c)

	if err := txcl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := txnProduceSync(t, txcl, topic, "a0"); err != nil {
		t.Fatal(err)
	}
	recreateTopic(t, admin, topic, 1)
	waitForLog(t, txcl, lg, logSwap, 1)

	err := txcl.EndTransaction(ctx, kgo.TryCommit)
	if err == nil {
		t.Fatal("commit across a recreation succeeded; want a refusal")
	}
	if !errors.Is(err, kerr.TransactionAbortable) {
		t.Fatalf("commit refusal %v does not carry the abortable recreation reason", err)
	}
	if err := txcl.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatalf("abort after recreation: %v", err)
	}

	if err := txcl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := txnProduceSync(t, txcl, topic, "c0"); err != nil {
		t.Fatal(err)
	}
	if err := txcl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}

	consumeCommitted(t, c, topic, "c0")
}

// Below ID-ful metadata, produce behavior across a recreation is unchanged
// (by-name produce continues into the new incarnation): default-on handling
// must not change what old clusters see.
func TestRecreationProduceDisarmedUnchanged(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic), MaxVersions(kversion.V2_7_0()))
	lg := new(capLogger)
	cl := newPlainClient(t, c,
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.WithLogger(lg),
	)
	admin := newPlainClient(t, c)

	produceSync(t, cl, topic, "v0")
	recreateTopic(t, admin, topic, 1)
	produceSync(t, cl, topic, "n0")

	if n := lg.count(logProduceSwap); n != 0 {
		t.Errorf("swap happened %d times below the gate; want unchanged behavior", n)
	}
	consumeExactly(t, c, topic, "n0")
}
