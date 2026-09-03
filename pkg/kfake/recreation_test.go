package kfake

import (
	"context"
	"errors"
	"fmt"
	"slices"
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

// capLogger captures log lines so tests can assert on transitions that have
// no other observable API, such as a recreation swap.
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

// tail returns the last n bytes of the log, or all of it for n <= 0.
func (lg *capLogger) tail(n int) string {
	lg.mu.Lock()
	defer lg.mu.Unlock()
	s := lg.buf.String()
	if n > 0 && len(s) > n {
		s = s[len(s)-n:]
	}
	return s
}

// waitForLog forces metadata refreshes until the log line has been seen at
// least n times.
func waitForLog(t *testing.T, cl *kgo.Client, lg *capLogger, substr string, n int) {
	t.Helper()
	waitForLogWith(t, lg, substr, n, cl.ForceMetadataRefresh)
}

// waitForLogQuiet waits for the log line without forcing refreshes, for a
// line that a metadata refresh landing first would preempt, or that the
// client's own paced retries must reach on their own.
func waitForLogQuiet(t *testing.T, lg *capLogger, substr string, n int) {
	t.Helper()
	waitForLogWith(t, lg, substr, n, func() { time.Sleep(25 * time.Millisecond) })
}

func waitForLogWith(t *testing.T, lg *capLogger, substr string, n int, between func()) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if lg.count(substr) >= n {
			return
		}
		between()
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %dx %q in logs; log tail:\n%s", n, substr, lg.tail(4000))
}

const logSwap = "topic recreation detected"

// waitHits fails the test unless the faults answer n requests in time.
func waitHits(t *testing.T, h *FaultHandle, n int) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := h.Wait(ctx, n); err != nil {
		t.Fatalf("faults answered %d requests, want %d: %v", h.Hits(), n, err)
	}
}

// pauseAndDrain pauses fetching topic and waits out a fetch already in
// flight (the clients' 250ms FetchMaxWait), so that nothing on the wire can
// land the next observation.
func pauseAndDrain(cl *kgo.Client, topic string) {
	cl.PauseFetchTopics(topic)
	time.Sleep(300 * time.Millisecond)
}

// dumpLogOnFailure logs the captured client log if the test failed.
func dumpLogOnFailure(t *testing.T, lg *capLogger) {
	if t.Failed() {
		t.Logf("client log:\n%s", lg.tail(0))
	}
}

// opt848 opts a client into the KIP-848 group protocol, which kgo keeps
// behind a context value until brokers stabilize.
func opt848() kgo.Opt {
	return kgo.WithContext(context.WithValue(context.Background(), "opt_in_kafka_next_gen_balancer_beta", true))
}

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

// A recreation that changes nothing but the topic ID (same leader, epoch 0
// both times) is still detected and adopted.
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

// staleMetadata captures the topics' current metadata response, to replay
// later as a lagging broker's view.
func staleMetadata(t *testing.T, cl *kgo.Client, topics ...string) *kmsg.MetadataResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := kmsg.NewPtrMetadataRequest()
	for _, topic := range topics {
		mt := kmsg.NewMetadataRequestTopic()
		mt.Topic = kmsg.StringPtr(topic)
		req.Topics = append(req.Topics, mt)
	}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// newOffsetAdmin returns a client for observing stored commits, with
// OffsetFetch pinned to v9: the by-name wire reads what is stored under the
// name regardless of incarnation, whereas v10+ carries topic IDs the admin's
// own cache may hold stale across a recreation.
func newOffsetAdmin(t *testing.T, c *Cluster) *kgo.Client {
	t.Helper()
	maxv := kversion.Stable()
	maxv.SetMaxKeyVersion(int16(kmsg.OffsetFetch), 9)
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

// Recreation with fewer partitions: survivors swap and continue, and the
// vanished partition surfaces UNKNOWN_TOPIC_ID rather than reading anything.
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
		t.Fatalf("wanted new partition-0 record and a partition-1 UnknownTopicID error, got record=%v err=%v; log tail:\n%s", gotN0, gotErr, lg.tail(6000))
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

// A recreated topic restarts from its own beginning regardless of
// ConsumeResetOffset: an at-end policy must not skip the new topic's records.
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

	// Recreate and produce into the new incarnation before the client
	// swaps, with fetches paused so that only the metadata observation
	// can land the swap: an at-end restart would then skip n0/n1;
	// starting from the new topic's beginning delivers them.
	pauseAndDrain(cl, topic)
	recreateTopic(t, admin, topic, 1)
	produceVals(t, c, topic, 0, "n0", "n1")
	waitForLog(t, cl, lg, logSwap, 1)
	cl.ResumeFetchTopics(topic)
	collectVals(t, cl, "n0", "n1")
}

// A recreation restart that has not consumed yet stays pinned to the
// earliest offset: if the new topic truncates before the first fetch, the
// out of range re-resolves to earliest (5 here) rather than falling back to
// ConsumeResetOffset (at-end here, which would skip the rest of the topic).
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

	// Recreate; the swap lands from metadata alone while paused.
	pauseAndDrain(cl, topic)
	recreateTopic(t, admin, topic, 1)
	waitForLog(t, cl, lg, logSwap, 1)

	// Position the restart at offset 0 of the empty new topic.
	cl.ResumeFetchTopics(topic)
	verifyZeroRecords(t, cl, 300*time.Millisecond)

	// Truncate under the unconsumed restart, then resume: the fetch at 0
	// is out of range and must re-resolve to the earliest offset (5).
	pauseAndDrain(cl, topic)
	produceVals(t, c, topic, 0, "n0", "n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9")
	deleteRecordsTo(t, admin, topic, 0, 5)
	cl.ResumeFetchTopics(topic)

	collectVals(t, cl, "n5", "n6", "n7", "n8", "n9")
	verifyZeroRecords(t, cl, 300*time.Millisecond)
}

// A paused partition fetches nothing, so no broker can reject the ID it
// holds: the swap lands from metadata alone, and resuming consumes the new
// incarnation from its beginning.
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

	// Drain before recreating so that no rejection can be what swaps us.
	pauseAndDrain(cl, topic)
	recreateTopic(t, cl, topic, 1)
	produceVals(t, c, topic, 0, "n0")
	waitForLog(t, cl, lg, logSwap, 1)

	cl.ResumeFetchTopics(topic)
	collectVals(t, cl, "n0")
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
// into id2t is what resolves the 848 assignment of the new incarnation.
func TestRecreationGroup848(t *testing.T) {
	t.Parallel()

	const topic, group = "t", "g848"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c,
		opt848(),
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

// A KIP-848 assignment names the new incarnation by its topic ID, and
// adopting that ID into id2t is what resolves it: unresolved, the member
// keeps its current name-based assignment and never picks up partitions the
// new incarnation added.
func TestRecreationGroup848Grow(t *testing.T) {
	t.Parallel()

	const topic, group = "t", "g848grow"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic),
		BrokerConfigs(map[string]string{"group.consumer.heartbeat.interval.ms": "100"}), // unresolved IDs re-resolve per heartbeat
	)
	cl := newPlainClient(t, c,
		opt848(),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
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

// Recreate the topic many times under continuous produce and group-consume
// load. The client must not wedge, no sequence error may surface, produce
// failures must be unknown-topic errors only, and no value may be delivered
// twice. Values lost to a deletion are expected, so at-most-once is the
// assertable half.
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
	// Failures are tolerated only for the unknown-topic classes a
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

// A by-name fetch (below v13) that lands inside a recreated topic carries
// batch epochs below what we already consumed. The guard withholds
// delivery until the merge decides; without it the fetch would misread
// records at the stale position and skip the new incarnation's earlier ones.
func TestRecreationEpochGuard(t *testing.T) {
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
	pauseAndDrain(cl, topic)
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
	// withheld the misread and the swap reset to the start. The guard
	// wait forces no refresh, since a refresh landing before the fetch
	// response swaps on the new ID alone and leaves the guard nothing to
	// withhold.
	waitForLogQuiet(t, lg, logEpochGuard, 1)
	waitForLog(t, cl, lg, logSwap, 1)
	collectVals(t, cl, "n0", "n1", "n2", "n3", "n4")
}

// A never seen ID swaps at once, except on a partition with no position
// yet: swapped that early, a racing old-incarnation committed offset would
// be applied to the new topic and fetch successfully under the new ID.
// Offset resolution is blocked so the cursor stays unpositioned and fetches
// nothing; unblocking positions it, and the swap follows.
func TestRecreationUnpositionedWaitsForRejection(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	admin := newPlainClient(t, c)
	produceVals(t, c, topic, 0, "v0", "v1")

	// Every offset list answers a retryable NOT_LEADER, so the cursor
	// never positions.
	block := c.Fault(Fault{
		Keys:  []kmsg.Key{kmsg.ListOffsets},
		Topic: topic,
		Err:   kerr.NotLeaderForPartition,
		Count: -1,
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
	block.Remove()
	waitForLog(t, cl, lg, logSwap, 1)
	collectVals(t, cl, "n0", "n1")
}

// A previously held topic ID reported by metadata is a lagging broker's
// view, since IDs are never reused. The immediate swap would otherwise adopt
// a persistently stale broker's view; a prior ID is refused instead, so the
// client stays put.
func TestRecreationPriorIDRefused(t *testing.T) {
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
	stale := staleMetadata(t, admin, topic)

	produceVals(t, c, topic, 0, "v0", "v1")
	collectVals(t, cl, "v0", "v1")

	recreateTopic(t, admin, topic, 1)
	waitForLog(t, cl, lg, logSwap, 1)

	// Every metadata answer now replays the pre-recreation view, which
	// would adopt any never seen ID at once.
	c.ControlKey(int16(kmsg.Metadata), func(kmsg.Request) (kmsg.Response, error, bool) {
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

// A leader that has not yet learned a recreation rejects the new ID with
// UNKNOWN_TOPIC_ID, and a lagging broker's metadata still reports the old ID
// meanwhile. Neither moves the client back: a rejection says only that one
// broker's view differs, and a prior ID is refused short of the grace.
// Consumption resumes on the new incarnation once the leader catches up. A
// co-partition on the source keeps the rejected response from being
// all-stripped, so nothing but the rejection pacing paces the refetch.
func TestRecreationLaggingLeaderConsumer(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic, "u"))
	lg := new(capLogger)
	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic, "u"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.WithLogger(lg),
	)
	admin := newPlainClient(t, c)
	stale := staleMetadata(t, admin, topic)

	produceVals(t, c, topic, 0, "v0", "v1")
	collectVals(t, cl, "v0", "v1")

	// Adopt the new ID from metadata alone, then play a lagging leader:
	// the first fetches by the new ID are rejected, and every metadata
	// answer meanwhile replays the old ID.
	pauseAndDrain(cl, topic)
	recreateTopic(t, admin, topic, 1)
	waitForLog(t, cl, lg, logSwap, 1)

	// Reject the new ID; the co-partition is answered normally.
	const rejections = 3
	reject := c.Fault(Fault{
		Keys:    []kmsg.Key{kmsg.Fetch},
		TopicID: c.TopicInfo(topic).TopicID,
		Err:     kerr.UnknownTopicID,
		Count:   rejections,
	})
	c.ControlKey(int16(kmsg.Metadata), func(kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if reject.Hits() >= rejections {
			return nil, nil, false
		}
		return stale, nil, true
	})
	start := time.Now()
	cl.ResumeFetchTopics(topic)
	waitHits(t, reject, rejections)
	// Rejected fetches are paced so the grace measures metadata rounds.
	if elapsed := time.Since(start); elapsed < 500*time.Millisecond {
		t.Errorf("%d rejections took %v; want the refetches paced", rejections, elapsed)
	}

	produceVals(t, c, topic, 0, "n0", "n1")
	collectVals(t, cl, "n0", "n1")
	if got := lg.count(logSwap); got != 1 {
		t.Fatalf("a lagging leader's rejections swapped the consumer back: %d swaps, want 1", got)
	}
}

// The self-healing side: a broker lagging by more than one recreation
// reports an ID we never held, so we adopt it, and no broker knows it. Once
// the fetches by it have been rejected for the whole grace, the real ID that
// metadata keeps reporting, which we held before, is taken back, and the
// restart from the beginning re-delivers.
func TestRecreationStaleIDRecoveryConsumer(t *testing.T) {
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
	admin := newPlainClient(t, c)
	fake := staleMetadata(t, admin, topic)
	fake.Topics[0].TopicID[0]++ // an ID no broker has ever had

	produceVals(t, c, topic, 0, "v0", "v1")
	collectVals(t, cl, "v0", "v1")

	var replay atomic.Bool
	replay.Store(true)
	c.ControlKey(int16(kmsg.Metadata), func(kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if !replay.Load() {
			return nil, nil, false
		}
		return fake, nil, true
	})
	waitForLog(t, cl, lg, logSwap, 1)
	replay.Store(false)

	// The rejections drive the refreshes; nothing is forced.
	waitForLogQuiet(t, lg, logSwap, 2)
	produceVals(t, c, topic, 0, "n0", "n1")
	collectVals(t, cl, "v0", "v1", "n0", "n1")
}

const logGuardDeliver = "delivering records whose leader epoch regressed below what we already consumed"

// The epoch guard's bound and its pacing. When metadata keeps reporting the
// ID we hold (here a replayed pre-recreation view, the shape a rolled back
// log also has), the guard never gets an answer, and after five paced
// withholds the records are delivered loudly rather than stalling forever.
// Unpaced, the bound would burn out at round-trip speed, before any
// metadata update could land. A co-partition on the same source keeps the
// withheld response from being all-stripped, so nothing but the guard
// pacing paces the refetch.
func TestRecreationEpochGuardBound(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(2), SeedTopics(1, topic, "u"), MaxVersions(kversion.V3_0_0()))
	lg := new(capLogger)
	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic, "u"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.WithLogger(lg),
	)
	admin := newPlainClient(t, c)

	// Two moves: consumed records carry leader epoch 2. The co-partition
	// lands on the same broker.
	oldLeader := c.LeaderFor(topic, 0)
	if err := c.MoveTopicPartition(topic, 0, 1-oldLeader); err != nil {
		t.Fatal(err)
	}
	if err := c.MoveTopicPartition(topic, 0, oldLeader); err != nil {
		t.Fatal(err)
	}
	if err := c.MoveTopicPartition("u", 0, oldLeader); err != nil {
		t.Fatal(err)
	}
	produceVals(t, c, topic, 0, "v0", "v1", "v2")
	collectVals(t, cl, "v0", "v1", "v2")

	// Capture the pre-recreation view to replay: the merge then keeps
	// seeing the ID it holds and never swaps.
	stale := staleMetadata(t, admin, topic, "u")

	// Recreate so the by-name fetch reads records below the consumed
	// epoch at the stale position, with the partition's final epoch and
	// leader matching what the consumer knows so nothing fences the
	// fetch.
	pauseAndDrain(cl, topic)
	recreateTopic(t, admin, topic, 1)
	if err := c.MoveTopicPartition(topic, 0, oldLeader); err != nil { // epoch 1
		t.Fatal(err)
	}
	produceVals(t, c, topic, 0, "n0", "n1", "n2", "n3", "n4", "n5", "n6", "n7", "n8", "n9")
	if err := c.MoveTopicPartition(topic, 0, oldLeader); err != nil { // epoch 2: matches the stale view
		t.Fatal(err)
	}
	c.ControlKey(int16(kmsg.Metadata), func(kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		return stale, nil, true
	})
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
	if lg.count(logSwap) != 0 {
		t.Error("nothing should have swapped on this masked recreation")
	}

	// The records at and past the stale position deliver exactly once.
	collectVals(t, cl, "n3", "n4", "n5", "n6", "n7", "n8", "n9")
	verifyZeroRecords(t, cl, 300*time.Millisecond)
}

// An OFFSET_OUT_OF_RANGE from a by-name fetch resets only after the metadata
// update it triggers, and a swap in that update replaces the pending reset
// with its own restart: a recreation takes a single reset rather than a
// plain reset the later swap would repeat, re-delivering records.
func TestRecreationOutOfRangeSingleReset(t *testing.T) {
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
	pauseAndDrain(cl, topic)
	recreateTopic(t, admin, topic, 1)
	produceVals(t, c, topic, 0, "n0", "n1")
	var lists atomic.Int32
	c.ControlKey(int16(kmsg.ListOffsets), func(kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		lists.Add(1)
		return nil, nil, false
	})
	cl.ResumeFetchTopics(topic)

	// Exactly once, from a single reset: the plain reset waits on the
	// metadata update, whose swap replaces it with the restart, the one
	// offset list. A plain reset first would list twice and could
	// deliver n0/n1 twice.
	collectVals(t, cl, "n0", "n1")
	verifyZeroRecords(t, cl, 500*time.Millisecond)
	if lg.count(logSwap) == 0 {
		t.Error("expected the out of range's metadata update to land the swap")
	}
	if n := lists.Load(); n != 1 {
		t.Errorf("saw %d offset lists across the recreation; want exactly 1, the swap's restart", n)
	}
}

// Below topic IDs (2.7 and earlier) nothing detects a recreation: an out
// of range position takes the plain policy reset with no recreation claim,
// as it always did.
func TestRecreationOutOfRangePlainReset(t *testing.T) {
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

	pauseAndDrain(cl, topic)
	recreateTopic(t, admin, topic, 1)
	produceVals(t, c, topic, 0, "n0", "n1")
	cl.ResumeFetchTopics(topic)

	collectVals(t, cl, "n0", "n1")
	verifyZeroRecords(t, cl, 500*time.Millisecond)
	if n := lg.count(logSwap); n != 0 {
		t.Errorf("saw %d recreation swaps; want a plain policy reset (nothing detects a recreation below topic IDs)", n)
	}
}

// Below ID-ful metadata (2.7 and earlier: no topic IDs anywhere), no signal
// exists and recreation behavior is UNCHANGED: no adoption, no reset. In
// this offset geometry (old position == new log end) the consumer silently
// sees nothing, which is today's documented behavior.
func TestRecreationNoIDsUnchanged(t *testing.T) {
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
		t.Errorf("swap happened %d times below ID-ful metadata; want unchanged behavior", n)
	}
}

const logSeedCommit = "committing the reset position for a recreated topic partition"

// A commit stored under a recreated name mispositions the next member to
// fetch it, and on a quiet topic nothing would ever overwrite it, since
// nothing is committable until records are polled. The fence and seeded
// recommit must promptly overwrite it with the reset position, under both
// group protocols.
func TestRecreationCommitFenceSeed(t *testing.T) {
	t.Parallel()
	testCommitFenceSeedAt(t, "classic", []kgo.Opt{kgo.MaxVersions(kversion.V3_7_0())})
	testCommitFenceSeedAt(t, "848", []kgo.Opt{opt848()})
}

func testCommitFenceSeedAt(t *testing.T, name string, protocol []kgo.Opt) {
	t.Run(name, func(t *testing.T) {
		t.Parallel()

		const topic, group = "t", "g"
		c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
		lg := new(capLogger)
		cl := newPlainClient(t, c, append([]kgo.Opt{
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup(group),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.AutoCommitInterval(100 * time.Millisecond),
			kgo.FetchMaxWait(250 * time.Millisecond),
			kgo.WithLogger(lg),
		}, protocol...)...)
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
		cl2 := newPlainClient(t, c, append([]kgo.Opt{
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup(group),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.FetchMaxWait(250 * time.Millisecond),
		}, protocol...)...)
		verifyZeroRecords(t, cl2, 500*time.Millisecond)
	})
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

// While a swapped partition's reset has not resolved, its fenced entry is
// invisible to the commit views: UncommittedOffsets must not hand you the
// {-1, -1} sentinel to commit. Offset resolution is blocked so the fence
// outlives the check.
func TestRecreationFencedHiddenFromCommitViews(t *testing.T) {
	t.Parallel()

	const topic, group = "t", "gfence"
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
	pollCtx, pollCancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	cl.PollFetches(pollCtx)
	pollCancel()
	waitCommitted(t, admin, group, topic, 0, 2)

	// Block the reset so it cannot resolve and the fence stays up: every
	// offset list answers a retryable NOT_LEADER.
	block := c.Fault(Fault{
		Keys:  []kmsg.Key{kmsg.ListOffsets},
		Topic: topic,
		Err:   kerr.NotLeaderForPartition,
		Count: -1,
	})

	recreateTopic(t, admin, topic, 1)
	waitForLog(t, cl, lg, logSwap, 1)

	// The fence is up and cannot lift; sample the views for a while.
	deadline := time.Now().Add(500 * time.Millisecond)
	for time.Now().Before(deadline) {
		for name, view := range map[string]map[string]map[int32]kgo.EpochOffset{
			"UncommittedOffsets": cl.UncommittedOffsets(),
			"CommittedOffsets":   cl.CommittedOffsets(),
		} {
			if eo, ok := view[topic][0]; ok && eo.Offset < 0 {
				t.Fatalf("%s exposed the fence sentinel %+v for a swapped partition", name, eo)
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	block.Remove()
	waitCommitted(t, admin, group, topic, 0, 0)
	produceVals(t, c, topic, 0, "n0")
	collectVals(t, cl, "n0")
}

const logProduceSwap = "topic recreation detected, adopting the new topic ID for producing"

// An idempotent producer heals across a recreation with no surfaced error,
// no producer ID or epoch change, and a sequence chain restarted at zero.
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
	c.ControlKey(int16(kmsg.Produce), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
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

// A recreation that lands while a produce is in flight cannot duplicate:
// the request addressed the dead incarnation's ID and is rejected before
// reaching any log, and the retry heals into the new incarnation once.
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
	var held atomic.Bool
	c.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		if held.Swap(true) {
			return nil, nil, false
		}
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

// The producer side of a lagging leader: produces by the new ID are rejected
// with UNKNOWN_TOPIC_ID while metadata replays the old ID. The producer
// retries on the new ID rather than swapping back, and the record lands
// once the leader catches up.
func TestRecreationLaggingLeaderProducer(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	lg := new(capLogger)
	cl := newPlainClient(t, c,
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.WithLogger(lg),
	)
	admin := newPlainClient(t, c)
	stale := staleMetadata(t, admin, topic)

	produceSync(t, cl, topic, "p0")
	recreateTopic(t, admin, topic, 1)
	waitForLog(t, cl, lg, logProduceSwap, 1)

	const rejections = 2
	reject := c.Fault(Fault{
		Keys:    []kmsg.Key{kmsg.Produce},
		TopicID: c.TopicInfo(topic).TopicID,
		Err:     kerr.UnknownTopicID,
		Count:   rejections,
	})
	c.ControlKey(int16(kmsg.Metadata), func(kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if reject.Hits() >= rejections {
			return nil, nil, false
		}
		return stale, nil, true
	})

	produceSync(t, cl, topic, "p1")
	if got := reject.Hits(); got != rejections {
		t.Errorf("the leader rejected %d produces; want %d", got, rejections)
	}
	if got := lg.count(logProduceSwap); got != 1 {
		t.Errorf("a lagging leader's rejections swapped the producer back: %d swaps, want 1", got)
	}
	consumeExactly(t, c, topic, "p1")
}

// The producer side of the self-healing: produces by the never held ID are
// rejected, and after the grace the real ID is taken back and the record
// lands.
func TestRecreationStaleIDRecoveryProducer(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	lg := new(capLogger)
	cl := newPlainClient(t, c,
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.UnknownTopicRetries(10), // outlast the grace
		kgo.RetryBackoffFn(func(int) time.Duration { return 50 * time.Millisecond }),
		kgo.WithLogger(lg),
	)
	admin := newPlainClient(t, c)
	fake := staleMetadata(t, admin, topic)
	fake.Topics[0].TopicID[0]++

	produceSync(t, cl, topic, "p0")

	var replay atomic.Bool
	replay.Store(true)
	c.ControlKey(int16(kmsg.Metadata), func(kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if !replay.Load() {
			return nil, nil, false
		}
		return fake, nil, true
	})
	waitForLog(t, cl, lg, logProduceSwap, 1)
	replay.Store(false)

	produceSync(t, cl, topic, "p1")
	if got := lg.count(logProduceSwap); got != 2 {
		t.Errorf("saw %d produce swaps; want 2, the stale adoption and the recovery", got)
	}
	consumeExactly(t, c, topic, "p0", "p1")
}

// A by-name produce (below v13) that continues the sequence chain into the
// new incarnation is accepted by a 2.5+ broker with no error, so the only
// signal is the ack: its base offset went backwards. That regression drives
// the swap, and because the broker took our chain into the new log, the
// swap keeps the chain rather than restarting it at zero.
func TestRecreationProduceOffsetRegression(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	maxv := kversion.Stable()
	maxv.SetMaxKeyVersion(int16(kmsg.Produce), 12) // by name
	lg := new(capLogger)
	cl := newPlainClient(t, c,
		kgo.MaxVersions(maxv),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.WithLogger(lg),
	)
	admin := newPlainClient(t, c)

	var seqsMu sync.Mutex
	var seqs []int32
	c.ControlKey(int16(kmsg.Produce), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		preq := kreq.(*kmsg.ProduceRequest)
		seqsMu.Lock()
		defer seqsMu.Unlock()
		for i := range preq.Topics {
			for j := range preq.Topics[i].Partitions {
				var b kmsg.RecordBatch
				if err := b.ReadFrom(preq.Topics[i].Partitions[j].Records); err == nil {
					seqs = append(seqs, b.FirstSequence)
				}
			}
		}
		return nil, nil, false
	})

	produceAt := func(val string, want int64) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		r := &kgo.Record{Topic: topic, Partition: 0, Value: []byte(val)}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatalf("produce %q: %v", val, err)
		}
		if r.Offset != want {
			t.Fatalf("produce %q acked at offset %d; want %d", val, r.Offset, want)
		}
	}
	produceAt("v0", 0)
	produceAt("v1", 1)

	// Nothing refreshes metadata between the recreate and the produce, so
	// n0 goes by name with the continued chain and lands at 0, below the
	// acked 2.
	recreateTopic(t, admin, topic, 1)
	produceAt("n0", 0)
	waitForLog(t, cl, lg, logProduceSwap, 1)
	if n := lg.count("restarting_sequences false"); n != 1 {
		t.Errorf("saw %d swaps keeping the chain; want 1 (the acked offset regression proved the chain landed in the new log)", n)
	}
	if n := lg.count("failing the producer ID"); n != 0 {
		t.Errorf("the producer ID was failed %d times; want the chain untouched", n)
	}

	// The chain continues where it landed: the next produce carries the
	// next sequence and lands at the next offset.
	produceAt("n1", 1)
	seqsMu.Lock()
	got := append([]int32(nil), seqs...)
	seqsMu.Unlock()
	if want := []int32{0, 1, 2, 3}; !slices.Equal(got, want) {
		t.Errorf("wire sequences %v; want %v (one unbroken chain across the swap)", got, want)
	}
	consumeExactly(t, c, topic, "n0", "n1")
}

// Below v13 produce addresses topics by name, and a batch whose outcome was
// never resolved may already sit in the new incarnation. Re-producing it
// could not be deduplicated, so the swap fails the partition's buffered
// records loudly and then continues cleanly.
func TestRecreationProduceUnsureByName(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	maxv := kversion.Stable()
	maxv.SetMaxKeyVersion(int16(kmsg.Produce), 12) // by name
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
	// to the broker again (and fail as unknown).
	timeouts := c.Fault(Fault{
		Keys:  []kmsg.Key{kmsg.Produce},
		Topic: topic,
		Err:   kerr.RequestTimedOut,
		Count: -1,
	})

	done := make(chan error, 1)
	cl.Produce(context.Background(), &kgo.Record{Topic: topic, Partition: 0, Value: []byte("u0")}, func(_ *kgo.Record, err error) {
		done <- err
	})

	// At least one attempt must have received the timed-out response
	// before the recreation, marking its by-name outcome unknowable.
	waitHits(t, timeouts, 1)

	deleteTopic(t, admin, topic)
	timeouts.Remove()
	// A couple of merges observe the deletion gap (kept under the
	// unknown-fail limit) before the create lands the swap.
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

// A by-name produce held in flight across a recreation lands in the new
// incarnation continuing the old sequence chain. kfake accepts that (the
// 2.5+ leniency), so a control plays a strict broker and answers
// OUT_OF_ORDER_SEQUENCE_NUMBER. The request predates the swap, so the error
// is reclassified as recreation rather than data loss: the batch retries on
// the reset chain with the same epoch, and nothing is failed.
func TestRecreationProduceInflightByName(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	maxv := kversion.Stable()
	maxv.SetMaxKeyVersion(int16(kmsg.Produce), 12) // by name
	lg := &capLogger{lvl: kgo.LogLevelDebug}
	cl := newPlainClient(t, c,
		kgo.MaxVersions(maxv),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.MetadataMinAge(100*time.Millisecond), // the reclassified retry waits on a non-urgent metadata update
		kgo.WithLogger(lg),
	)
	defer dumpLogOnFailure(t, lg)
	admin := newPlainClient(t, c)

	produceSync(t, cl, topic, "p0")

	// Record every attempt from here on: the held request and its retry.
	// Hold the first request in flight across the recreation, then reject
	// it as a strict broker would.
	type attempt struct {
		epoch int16
		seq   int32
	}
	var attemptsMu sync.Mutex
	var attempts []attempt
	recreated := make(chan struct{})
	var held atomic.Bool
	c.ControlKey(int16(kmsg.Produce), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		preq := kreq.(*kmsg.ProduceRequest)
		for i := range preq.Topics {
			for j := range preq.Topics[i].Partitions {
				var b kmsg.RecordBatch
				if err := b.ReadFrom(preq.Topics[i].Partitions[j].Records); err == nil {
					attemptsMu.Lock()
					attempts = append(attempts, attempt{b.ProducerEpoch, b.FirstSequence})
					attemptsMu.Unlock()
				}
			}
		}
		if held.Swap(true) {
			return nil, nil, false
		}
		c.SleepControl(func() { <-recreated })
		resp := preq.ResponseKind().(*kmsg.ProduceResponse)
		for i := range preq.Topics {
			rt := &preq.Topics[i]
			st := kmsg.NewProduceResponseTopic()
			st.Topic = rt.Topic
			for _, rp := range rt.Partitions {
				sp := kmsg.NewProduceResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = kerr.OutOfOrderSequenceNumber.Code
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})

	done := make(chan error, 1)
	cl.Produce(context.Background(), &kgo.Record{Topic: topic, Partition: 0, Value: []byte("h0")}, func(_ *kgo.Record, err error) {
		done <- err
	})
	for !held.Load() {
		time.Sleep(10 * time.Millisecond)
	}

	// A couple of merges observe the deletion gap; the create then lands
	// the swap under the held request.
	deleteTopic(t, admin, topic)
	for range 2 {
		cl.ForceMetadataRefresh()
		time.Sleep(25 * time.Millisecond)
	}
	createTopic(t, admin, topic, 1)
	waitForLog(t, cl, lg, logProduceSwap, 1)
	close(recreated)

	if err := <-done; err != nil {
		t.Fatalf("in-flight by-name produce did not heal: %v", err)
	}
	if n := lg.count("failing the producer ID"); n != 0 {
		t.Errorf("producer ID was failed %d times; want the sequence error reclassified as recreation", n)
	}
	// Snapshot under the lock: the control takes it on every produce, and
	// we produce again below.
	attemptsMu.Lock()
	got := append([]attempt(nil), attempts...)
	attemptsMu.Unlock()
	if len(got) < 2 {
		t.Fatalf("saw %d produce attempts, want the held request and its retry", len(got))
	}
	for i, a := range got {
		if a.epoch != got[0].epoch {
			t.Errorf("attempt %d used epoch %d; want the initial epoch %d", i, a.epoch, got[0].epoch)
		}
	}
	if last := got[len(got)-1]; last.seq != 0 {
		t.Errorf("healed attempt has sequence %d; want 0 (chain restarted)", last.seq)
	}

	produceSync(t, cl, topic, "n1")
	consumeExactly(t, c, topic, "h0", "n1")
}

// A transaction whose topic is recreated mid-transaction fails with an
// abortable error, never a silent partial commit; aborting recovers, and the
// next transaction produces cleanly to the new incarnation. KIP-890p2,
// produce v13 by ID.
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

// Same shape before KIP-890p2 (produce v11 by name, EndTxn v4): the
// post-recreation write lands silently in the new incarnation, the acked
// offset regression poisons the transaction, and recovery works because the
// recreation sentinel is recognized in the pre-890p2 recovery arm, where raw
// TransactionAbortable is not recoverable.
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

// A transaction produces to a topic, the topic is recreated, and the
// transaction never touches it again: no response exists to inspect at any
// produce version. Only the commit-time verification can catch it.
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
	// produce response can reveal it before the commit.
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

	// The verification's metadata fetch lands the swap; wait for it so the
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

// The merge fails an exposed transaction on the first observation of a
// recreated produced-to topic, which is what lets commit-time verification
// trust recent metadata passes. The commit is refused through the poisoned
// producer ID with no verification fetch of its own, and at a version with
// by-name produce the abort recovers through the pre-890p2 recovery arm.
func testTxnPoisonAt(t *testing.T, name string, vs *kversion.Versions) {
	t.Run(name, func(t *testing.T) {
		t.Parallel()

		const topic = "t"
		opts := []Opt{NumBrokers(1), SeedTopics(1, topic)}
		if vs != nil {
			opts = append(opts, MaxVersions(vs))
		}
		c := newCluster(t, opts...)
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

		// The same merge swapped the partition onto the new incarnation, so
		// the next transaction is clean.
		if n := lg.count(logSwap); n != 1 {
			t.Fatalf("saw %d swaps; want the observing merge to swap once", n)
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
	})
}

func TestRecreationTxnPoisonOnObservation(t *testing.T) {
	t.Parallel()
	testTxnPoisonAt(t, "latest", nil)
	testTxnPoisonAt(t, "v3_0", kversion.V3_0_0())
}

// The real-broker transaction shape at pinned versions: a committed
// transaction, a recreate, then bounded abort-and-retry rounds until a
// transaction commits into the new incarnation, which read-committed
// consumption sees exactly. This shape caught two bugs the targeted tests
// missed: a swap clobbering a pending epoch-bump sequence reset, and kfake
// not aborting the open transaction on a transactional id re-init.
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
		defer dumpLogOnFailure(t, lg)
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

// An idempotent producer's chain continues into a recreated topic at every
// broker version. Below 2.5 the new log has no state for the producer and
// rejects the nonzero first sequence with UNKNOWN_PRODUCER_ID; with no
// topic ID to name the recreation, the producer resets its ID and the
// record still lands.
func testIdempotentShapeAt(t *testing.T, name string, vs *kversion.Versions) {
	t.Run(name, func(t *testing.T) {
		t.Parallel()
		const topic = "t"
		opts := []Opt{NumBrokers(1), SeedTopics(1, topic)}
		if vs != nil {
			opts = append(opts, MaxVersions(vs))
		}
		c := newCluster(t, opts...)
		lg := new(capLogger)
		cl := newPlainClient(t, c,
			kgo.RecordPartitioner(kgo.ManualPartitioner()),
			kgo.MetadataMinAge(100*time.Millisecond), // a reset's retry waits on a non-urgent metadata update
			kgo.WithLogger(lg),
		)
		admin := newPlainClient(t, c)

		produceSync(t, cl, topic, "p0")
		recreateTopic(t, admin, topic, 1)
		produceSync(t, cl, topic, "p1")
		consumeExactly(t, c, topic, "p1")

		var pre25 bool
		if vs != nil {
			v, _ := vs.LookupMaxKeyVersion(int16(kmsg.InitProducerID))
			pre25 = v < 3
		}
		resets := lg.count("resetting all sequence numbers")
		if pre25 && resets == 0 {
			t.Error("pre-2.5 broker did not reject the continued chain, or the producer did not reset")
		} else if !pre25 && resets != 0 {
			t.Errorf("producer ID was reset %d times; want the chain to continue on 2.5+", resets)
		}
	})
}

func TestRecreationIdempotentShape(t *testing.T) {
	t.Parallel()
	testIdempotentShapeAt(t, "latest", nil)
	testIdempotentShapeAt(t, "v3_0", kversion.V3_0_0())
	testIdempotentShapeAt(t, "v2_1", kversion.V2_1_0())
	testIdempotentShapeAt(t, "v0_11", kversion.V0_11_0())
}

// A recreation inside the unconfirmed-EndTxn window: the TryAbort retry
// heals without commit-time verification, since the prior attempt's fate is
// sealed, and the following transactions converge onto the new incarnation
// through an abortable poison, never silently.
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

	// The first EndTxn fails with UNKNOWN_SERVER_ERROR before kfake
	// processes it, so the commit outcome is unconfirmed.
	c.Fault(Fault{
		Keys: []kmsg.Key{kmsg.EndTxn},
		Err:  kerr.UnknownServerError,
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
// new incarnation's fresh share state, and acknowledgments of records
// acquired from the dead incarnation fail loudly rather than being
// re-addressed, since an ack under the new ID could acknowledge an unrelated
// record at the same offset.
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

// With topic IDs in metadata but a by-name fetch wire (2.8 through 3.0),
// nothing on the wire can reject a stale ID; the consumer adopts a
// recreation from the metadata observation alone.
func TestRecreationMetadataIDConsumer(t *testing.T) {
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
	waitForLog(t, cl, lg, logSwap, 1)

	produceVals(t, c, topic, 0, "n0", "n1", "n2")
	collectVals(t, cl, "n0", "n1", "n2")
}

// Same, producing: with nothing buffered or in flight, the swap adopts on
// the metadata observation and the next produce continues on the new
// incarnation with a fresh sequence chain.
func TestRecreationMetadataIDProducer(t *testing.T) {
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

// Below ID-ful metadata, produce across a recreation is unchanged: by-name
// produce continues into the new incarnation.
func TestRecreationProduceNoIDsUnchanged(t *testing.T) {
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
		t.Errorf("swap happened %d times below ID-ful metadata; want unchanged behavior", n)
	}
	consumeExactly(t, c, topic, "n0")
}
