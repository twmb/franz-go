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
// line that a forced refresh landing first would preempt, or that the
// client must reach on its own, one retry per metadata update.
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

// The per topic restart line; the per partition detail is at debug.
const logRestartTopic = "restarting partitions from the recreated topic's beginning"

// Below fetch v13 the response carries no topic ID; polled fetches carry the
// ID metadata gave the cursor instead.
func TestRecreationFetchTopicIDByName(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic), MaxVersions(kversion.V3_0_0()))
	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	produceVals(t, c, topic, 0, "v0")

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	fs := cl.PollFetches(ctx)
	if fs.NumRecords() == 0 {
		t.Fatal("no records")
	}
	want := c.TopicInfo(topic).TopicID
	fs.EachTopic(func(ft kgo.FetchTopic) {
		if ft.TopicID != want {
			t.Errorf("fetch topic ID %x, want %x", ft.TopicID, want)
		}
	})
}

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

// opt848 opts a client into the KIP-848 group protocol, which kgo keeps
// behind a context value until brokers stabilize.
func opt848() kgo.Opt {
	return kgo.WithContext(context.WithValue(context.Background(), "opt_in_kafka_next_gen_balancer_beta", true))
}

// recreateTopic deletes and immediately recreates a topic; the new
// topic has a fresh topic ID.
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
// fails on any unexpected value (e.g. re-read old-topic records).
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
// name regardless of recreation, whereas v10+ carries topic IDs the admin's
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

// Same as above, but the new topic's partition moves to a different
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
		kgo.MetadataMinAge(100*time.Millisecond),
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

	// Recreate and produce into the new topic before the client
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
// topic from its beginning.
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
// into id2t is what resolves the 848 assignment of the new topic.
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

// A KIP-848 assignment names the new topic by its topic ID, and
// adopting that ID into id2t is what resolves it: unresolved, the member
// keeps its current name-based assignment and never picks up partitions the
// new topic added.
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

// A never seen ID swaps at once, except on a partition with no position
// yet: swapped that early, a racing old-topic committed offset would
// be applied to the new topic and fetch successfully under the new ID.
// Offset resolution is blocked so the cursor never gets an offset and fetches
// nothing; unblocking positions it, and the swap follows.
// A partition whose first offset is still loading swaps like any other:
// the pending load is dropped and the new topic's start is loaded instead.
func TestRecreationSwapWhileLoading(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	admin := newPlainClient(t, c)
	produceVals(t, c, topic, 0, "v0", "v1")

	// Every offset list answers a retryable NOT_LEADER, so the cursor
	// never gets an offset.
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
	waitForLog(t, cl, lg, logSwap, 1)

	produceVals(t, c, topic, 0, "n0", "n1")
	block.Remove()
	collectVals(t, cl, "n0", "n1")
}

// A partition we do not consume is left alone by the recreation: it is not
// restarted, not consumed, and the metadata update does not retry for it.
func TestRecreationUnconsumedPartitionAdopts(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(2, topic))
	admin := newPlainClient(t, c)

	lg := &capLogger{lvl: kgo.LogLevelDebug}
	cl := newPlainClient(t, c,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart()}}),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.WithLogger(lg),
	)
	produceVals(t, c, topic, 0, "v0")
	produceVals(t, c, topic, 1, "x0")
	collectVals(t, cl, "v0")

	recreateTopic(t, admin, topic, 2)
	produceVals(t, c, topic, 0, "n0")
	produceVals(t, c, topic, 1, "x1")
	collectVals(t, cl, "n0")

	if got := lg.count(logSwap); got != 1 {
		t.Errorf("expected the recreation detected once, got %d", got)
	}
	if got := lg.count("restarting the partition"); got != 1 {
		t.Errorf("expected 1 restart (partition 0), got %d", got)
	}
	if got := lg.count(logRestartTopic); got != 1 {
		t.Errorf("expected one restart line for the topic, got %d", got)
	}
	if got := lg.count("metadata update had inner errors"); got != 0 {
		t.Errorf("the metadata update retried %d times after the swap:\n%s", got, lg.tail(2000))
	}
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
		kgo.RetryBackoffFn(func(int) time.Duration { return 250 * time.Millisecond }),
		kgo.MetadataMinAge(250*time.Millisecond),
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

	// Still consuming the real, current topic.
	produceVals(t, c, topic, 0, "n0", "n1")
	collectVals(t, cl, "n0", "n1")
}

// A leader that has not yet learned a recreation rejects the new ID with
// UNKNOWN_TOPIC_ID, and a lagging broker's metadata still reports the old ID
// meanwhile. Neither moves the client back: a rejection says only that one
// broker's view differs, and a prior ID is refused short of the grace.
// Consumption resumes on the new topic once the leader catches up. A
// co-partition on the source keeps the rejected response from being
// all-stripped, so nothing but the rejection's wait for a metadata update
// spaces the refetches.
func TestRecreationLaggingLeaderConsumer(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic, "u"))
	lg := new(capLogger)
	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic, "u"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.RetryBackoffFn(func(int) time.Duration { return 250 * time.Millisecond }),
		kgo.MetadataMinAge(300*time.Millisecond),
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
	// Each rejected fetch waits for a metadata update, so the limit counts
	// metadata rounds.
	if elapsed := time.Since(start); elapsed < 500*time.Millisecond {
		t.Errorf("%d rejections took %v; want the refetches spaced by a metadata refresh", rejections, elapsed)
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
		kgo.RetryBackoffFn(func(int) time.Duration { return 250 * time.Millisecond }),
		kgo.MetadataMinAge(250*time.Millisecond),
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

	// The new topic's log is shorter than the stale position: the
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

	// Pause so that the first fetch after the recreation sees the new
	// log at its final length: a fetch at offset 3 against the empty new
	// topic would be out of range and reset.
	pauseAndDrain(cl, topic)
	recreateTopic(t, cl, topic, 1)
	produceVals(t, c, topic, 0, "n0", "n1", "n2")
	cl.ResumeFetchTopics(topic)

	verifyZeroRecords(t, cl, 500*time.Millisecond)
	if n := lg.count(logSwap); n != 0 {
		t.Errorf("swap happened %d times below ID-ful metadata; want unchanged behavior", n)
	}
}

// The group's uncommitted offsets for the old topic are dropped at the
// swap, so a commit after the swap never carries them; the broker deletes
// the old commits with the topic. On a quiet new topic nothing is committed
// until records arrive, and a member joining then starts from the reset
// policy. Pinned under both group protocols and with manual commits.
func TestRecreationNoStaleCommit(t *testing.T) {
	t.Parallel()
	testNoStaleCommitAt(t, "classic", []kgo.Opt{kgo.MaxVersions(kversion.V3_7_0())})
	testNoStaleCommitAt(t, "848", []kgo.Opt{opt848()})
	testNoStaleCommitAt(t, "manual", []kgo.Opt{kgo.MaxVersions(kversion.V3_7_0()), kgo.DisableAutoCommit()})
}

func testNoStaleCommitAt(t *testing.T, name string, protocol []kgo.Opt) {
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
		commit := func() {
			if err := cl.CommitUncommittedOffsets(context.Background()); err != nil {
				t.Fatalf("commit: %v", err)
			}
		}

		produceVals(t, c, topic, 0, "v0", "v1")
		collectVals(t, cl, "v0", "v1")
		pollCtx, pollCancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		cl.PollFetches(pollCtx)
		pollCancel()
		commit()
		waitCommitted(t, admin, group, topic, 0, 2)

		// Recreate and produce nothing. The broker deleted the stored 2
		// with the topic, and the swap dropped our uncommitted 2, so
		// commits over several intervals must not bring it back.
		recreateTopic(t, admin, topic, 1)
		waitForLog(t, cl, lg, logSwap, 1)
		time.Sleep(400 * time.Millisecond)
		commit()
		if got := fetchCommitted(t, admin, group, topic, 0); got != -1 {
			t.Fatalf("stored commit is %d after the swap; want none", got)
		}

		// The live consumer restarts from the new topic's beginning.
		produceVals(t, c, topic, 0, "n0", "n1", "n2")
		collectVals(t, cl, "n0", "n1", "n2")
		pollCtx, pollCancel = context.WithTimeout(context.Background(), 300*time.Millisecond)
		cl.PollFetches(pollCtx)
		pollCancel()
		commit()
		waitCommitted(t, admin, group, topic, 0, 3)
		cl.Close()

		// A member joining now starts from the commit of the new topic
		// and consumes nothing.
		cl2 := newPlainClient(t, c, append([]kgo.Opt{
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup(group),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.FetchMaxWait(250 * time.Millisecond),
		}, protocol...)...)
		verifyZeroRecords(t, cl2, 500*time.Millisecond)
	})
}

const logProduceSwap = "topic recreation detected, adopting the new topic ID for producing"

const logProduceRecreatedOOOSN = "first produce to a recreated partition was rejected as out of order"

const logTxnObserved = "topic recreation observed with an active transaction exposed to it"

const logShareSwap = "topic recreation detected, adopting the new topic ID for share consuming"

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

// commitOffset commits one offset for a group with no members, by name.
func commitOffset(t *testing.T, cl *kgo.Client, group, topic string, partition int32, offset int64, epoch int32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := kmsg.NewPtrOffsetCommitRequest()
	req.Group = group
	req.Generation = -1
	rp := kmsg.NewOffsetCommitRequestTopicPartition()
	rp.Partition = partition
	rp.Offset = offset
	rp.LeaderEpoch = epoch
	rt := kmsg.NewOffsetCommitRequestTopic()
	rt.Topic = topic
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("offset commit: %v", err)
	}
	if err := kerr.ErrorForCode(resp.Topics[0].Partitions[0].ErrorCode); err != nil {
		t.Fatalf("offset commit: %v", err)
	}
}

// The coordinator answering UNKNOWN_TOPIC_ID to an offset fetch starts the
// partition from the beginning under the ID we asked with, with no wait or
// retry: the coordinator's broker has no commit under an ID it does not
// know. Here the topic exists, so the fetch by that ID is served.
func TestRecreationOffsetFetchUnknownID(t *testing.T) {
	t.Parallel()

	const topic, group = "t", "gunknown"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	c.Fault(Fault{Keys: []kmsg.Key{kmsg.OffsetFetch}, Topic: topic, Err: kerr.UnknownTopicID})
	produceVals(t, c, topic, 0, "v0")

	lg := &capLogger{lvl: kgo.LogLevelDebug}
	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.WithLogger(lg),
	)
	collectVals(t, cl, "v0")
	if got := lg.count("restarting the partition"); got != 0 {
		t.Fatalf("expected no restart, got %d", got)
	}
}

// An offset fetch sent under the old topic ID and answered after this
// client adopted the new one: the coordinator answers a retryable error
// first, metadata reports the recreation during the retry backoff, and the
// retry is answered with the old topic's commit, as a coordinator that has
// not learned of the recreation would. Below OffsetFetch v10 the commit
// carries no ID; it is stamped with the ID the request was sent under, the
// fetch with that ID is rejected, and the metadata update the rejection
// triggers restarts the partition from the new topic's beginning.
func TestRecreationOffsetFetchAcrossSwap(t *testing.T) {
	t.Parallel()

	const topic, group = "t", "gacross"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic), MaxVersions(kversion.V3_7_0()))
	admin := newPlainClient(t, c)
	stale := staleMetadata(t, admin, topic)
	oldID := stale.Topics[0].TopicID

	recreateTopic(t, admin, topic, 1)
	produceVals(t, c, topic, 0, "n0", "n1")

	lg := &capLogger{lvl: kgo.LogLevelDebug}

	// Metadata replays the pre-recreation view until the offset fetch
	// arrives, so the request is sent under the old ID.
	var fresh atomic.Bool
	c.ControlKey(int16(kmsg.Metadata), func(kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if fresh.Load() {
			return nil, nil, false
		}
		return stale, nil, true
	})
	c.ControlKey(int16(kmsg.OffsetFetch), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		fresh.Store(true)
		req := kreq.(*kmsg.OffsetFetchRequest)
		resp := req.ResponseKind().(*kmsg.OffsetFetchResponse)
		rg := kmsg.NewOffsetFetchResponseGroup()
		rg.Group = group
		if lg.count(logSwap) == 0 {
			rg.ErrorCode = kerr.CoordinatorLoadInProgress.Code
			resp.Groups = append(resp.Groups, rg)
			return resp, nil, true
		}
		rt := kmsg.NewOffsetFetchResponseGroupTopic()
		rt.Topic = topic
		rt.TopicID = oldID
		rp := kmsg.NewOffsetFetchResponseGroupTopicPartition()
		rp.Partition = 0
		rp.Offset = 2
		rp.LeaderEpoch = -1
		rt.Partitions = append(rt.Partitions, rp)
		rg.Topics = append(rg.Topics, rt)
		resp.Groups = append(resp.Groups, rg)
		return resp, nil, true
	})

	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.MetadataMinAge(100*time.Millisecond),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.WithLogger(lg),
	)
	waitForLog(t, cl, lg, logSwap, 1)
	collectVals(t, cl, "n0", "n1")
	if got := lg.count("restarting the partition"); got != 1 {
		t.Errorf("expected one restart, got %d", got)
	}
}

// Below OffsetFetch v10 the coordinator matches by name, so a commit for the
// old topic can be handed to a member of the new one. A committed offset past
// the new log's end, validated by epoch against the new log, looks like data
// loss. The member here holds the old topic ID because its metadata came
// from a stale broker; the refresh before believing the data loss reports
// the recreation, and the partition restarts from the new topic's beginning
// instead of reporting data loss.
func TestRecreationCommittedOffsetRevalidates(t *testing.T) {
	t.Parallel()

	const topic, group = "t", "grevalidate"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic), MaxVersions(kversion.V3_0_0()))
	admin := newPlainClient(t, c, kgo.MaxVersions(kversion.V3_0_0()))
	stale := staleMetadata(t, admin, topic)

	recreateTopic(t, admin, topic, 1)
	produceVals(t, c, topic, 0, "n0")
	commitOffset(t, admin, group, topic, 0, 5, 0) // past the new log's end

	// Metadata replays the pre-recreation view until the member's first
	// epoch validation, which is the one that finds the data loss.
	var fresh atomic.Bool
	c.ControlKey(int16(kmsg.Metadata), func(kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if fresh.Load() {
			return nil, nil, false
		}
		return stale, nil, true
	})
	c.ControlKey(int16(kmsg.OffsetForLeaderEpoch), func(kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		fresh.Store(true)
		return nil, nil, false
	})

	lg := &capLogger{lvl: kgo.LogLevelDebug}
	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.WithLogger(lg),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			t.Fatalf("poll errors: %v", errs)
		}
		var got []string
		fetches.EachRecord(func(r *kgo.Record) { got = append(got, string(r.Value)) })
		if len(got) > 0 {
			if got[0] != "n0" {
				t.Fatalf("expected n0 first, got %v", got)
			}
			break
		}
	}
	if got := lg.count("revalidating offsets after apparent data loss"); got != 1 {
		t.Errorf("expected one revalidation, got %d", got)
	}
	if got := lg.count("restarting the partition"); got != 1 {
		t.Errorf("expected one restart, got %d", got)
	}
}

// A partition the recreated topic added starts from the beginning even with
// ConsumeResetOffset at the end: everything in the new topic arrived after
// the subscription. Direct consumer and KIP-848 group.
func TestRecreationConsumerSwapGrowAtEnd(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	verifyZeroRecords(t, cl, 300*time.Millisecond)
	produceVals(t, c, topic, 0, "v0")
	collectVals(t, cl, "v0")

	recreateTopic(t, cl, topic, 2)
	produceVals(t, c, topic, 0, "n0")
	produceVals(t, c, topic, 1, "n1")
	collectVals(t, cl, "n0", "n1")
}

func TestRecreationGroup848GrowAtEnd(t *testing.T) {
	t.Parallel()

	const topic, group = "t", "g848growend"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic),
		BrokerConfigs(map[string]string{"group.consumer.heartbeat.interval.ms": "100"}),
	)
	cl := newPlainClient(t, c,
		opt848(),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	verifyZeroRecords(t, cl, 300*time.Millisecond)
	produceVals(t, c, topic, 0, "v0")
	collectVals(t, cl, "v0")

	recreateTopic(t, cl, topic, 2)
	produceVals(t, c, topic, 0, "n0")
	produceVals(t, c, topic, 1, "n1")
	collectVals(t, cl, "n0", "n1")
}

// An idempotent producer continues across a recreation with no surfaced
// error, and every record lands once. Kafka up to 4.4 accepts whatever
// sequence number a producer with no state on the log sends first, so the
// chain continues under the same epoch. Kafka 4.5 (KAFKA-15591) rejects the
// first batch as out of order on a partition that has never held a record;
// the producer bumps its epoch and restarts its sequence numbers at zero.
// Either way, every record lands once.
func testProduceHealAt(t *testing.T, name string, vs *kversion.Versions) {
	t.Run(name, func(t *testing.T) {
		t.Parallel()

		const topic = "t"
		opts := []Opt{NumBrokers(1), SeedTopics(1, topic)}
		if vs != nil {
			opts = append(opts, MaxVersions(vs))
		}
		c := newCluster(t, opts...)
		lg := &capLogger{lvl: kgo.LogLevelDebug}
		var dataLoss atomic.Int32
		cl := newPlainClient(t, c,
			kgo.RecordPartitioner(kgo.ManualPartitioner()),
			kgo.MetadataMinAge(100*time.Millisecond),
			kgo.ProducerOnDataLossDetected(func(string, int32) { dataLoss.Add(1) }),
			kgo.WithLogger(lg),
		)
		admin := newPlainClient(t, c)

		// Record every produce attempt's epoch and first sequence as
		// written on the wire.
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

		if n := lg.count(logProduceSwap); n != 1 {
			t.Errorf("saw %d produce swaps; want 1", n)
		}

		attemptsMu.Lock()
		defer attemptsMu.Unlock()
		if len(attempts) < 6 {
			t.Fatalf("saw %d produce attempts, want at least 6", len(attempts))
		}

		// At 4.5 the first batch after the recreation is rejected and
		// the chain restarts at zero under the next epoch. Below that,
		// the chain continues under the same epoch.
		wantSeqs := []int32{3, 4, 5}
		wantEpoch := attempts[0].epoch
		if vs == nil {
			if n := lg.count(logProduceRecreatedOOOSN); n != 1 {
				t.Errorf("saw %d sequence resets; want 1", n)
			}
			wantSeqs = []int32{0, 1, 2}
			wantEpoch = attempts[0].epoch + 1
		} else if n := lg.count("failing the producer ID"); n != 0 {
			t.Errorf("the producer ID was failed %d times; want none", n)
		}
		if n := dataLoss.Load(); n != 0 {
			t.Errorf("the data loss hook was called %d times; want none", n)
		}

		last3 := attempts[len(attempts)-3:]
		for i, want := range wantSeqs {
			if last3[i].seq != want {
				t.Errorf("attempt %d after the recreation has sequence %d; want %d", i, last3[i].seq, want)
			}
			if last3[i].epoch != wantEpoch {
				t.Errorf("attempt %d after the recreation has epoch %d; want %d", i, last3[i].epoch, wantEpoch)
			}
		}

		consumeExactly(t, c, topic, "n0", "n1", "n2")
	})
}

func TestRecreationProduceHeal(t *testing.T) {
	t.Parallel()
	testProduceHealAt(t, "v4_5", nil)
	testProduceHealAt(t, "v4_2", kversion.V4_2_0())
}

// The rejected first produce into a recreated partition is not data loss, so
// StopProducerOnDataLossDetected does not stop the producer.
func TestRecreationProduceStopOnDataLoss(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	lg := &capLogger{lvl: kgo.LogLevelDebug}
	cl := newPlainClient(t, c,
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.MetadataMinAge(100*time.Millisecond),
		kgo.StopProducerOnDataLossDetected(),
		kgo.WithLogger(lg),
	)
	admin := newPlainClient(t, c)

	produceSync(t, cl, topic, "v0")
	recreateTopic(t, admin, topic, 1)
	produceSync(t, cl, topic, "n0")

	if n := lg.count(logProduceRecreatedOOOSN); n != 1 {
		t.Errorf("saw %d sequence resets; want 1", n)
	}
	consumeExactly(t, c, topic, "n0")
}

// A recreation that lands while a produce is in flight cannot duplicate:
// the request addressed the old topic's ID and is rejected before reaching
// any log, and the retry lands once in the new topic.
func TestRecreationProduceInflight(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c,
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.MetadataMinAge(100*time.Millisecond),
	)
	admin := newPlainClient(t, c)

	produceSync(t, cl, topic, "v0")

	// Hold the next produce in flight while the topic is recreated under
	// it, then let the broker process it against the new topic.
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
		t.Fatalf("in-flight produce did not land after the recreation: %v", err)
	}
	produceSync(t, cl, topic, "n1")

	consumeExactly(t, c, topic, "h0", "n1")
}

// The producer side of a lagging leader: produces by the new ID are rejected
// with UNKNOWN_TOPIC_ID while metadata replays the old ID. The producer
// retries under the new ID rather than swapping back, and the record lands
// once the leader catches up.
func TestRecreationLaggingLeaderProducer(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	lg := new(capLogger)
	cl := newPlainClient(t, c,
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.MetadataMinAge(100*time.Millisecond),
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
// rejected, and after recreationRejectionLimit of them the real ID, which
// metadata keeps reporting, is taken back and the record lands.
func TestRecreationStaleIDRecoveryProducer(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	lg := new(capLogger)
	cl := newPlainClient(t, c,
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.UnknownTopicRetries(10), // outlast the rejections
		kgo.RetryBackoffFn(func(int) time.Duration { return 50 * time.Millisecond }),
		kgo.MetadataMinAge(100*time.Millisecond),
		kgo.WithLogger(lg),
	)
	admin := newPlainClient(t, c)
	fake := staleMetadata(t, admin, topic)
	fake.Topics[0].TopicID[0]++ // an ID no broker has ever had

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

// With topic IDs in metadata but a by-name produce wire (2.8 through 4.0),
// the producer adopts the recreation from metadata alone and the next
// produce continues into the new topic after the sequence error and reset.
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

// Below metadata with topic IDs, produce across a recreation is unchanged:
// the by-name produce continues into the new topic after the sequence error
// and reset.
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
		t.Errorf("swap happened %d times below metadata with topic IDs; want unchanged behavior", n)
	}
	consumeExactly(t, c, topic, "n0")
}

// A transaction whose topic is recreated mid-transaction fails with an
// abortable error, never a silent partial commit; aborting recovers, and the
// next transaction produces cleanly to the new topic. Produce by ID, under
// KIP-890 part 2.
func TestRecreationTxnAborts(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	txcl := newPlainClient(t, c,
		kgo.TransactionalID("tx-recreate"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.MetadataMinAge(100*time.Millisecond),
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

	// The next transaction is clean on the new topic.
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

// A transaction that begins after the swap produces into a partition whose
// log has no producer state for us, and a 4.5 broker rejects that first
// produce as out of order. The recBuf remembers the recreation, so the
// transaction fails abortable instead of fatally: the commit is refused,
// the abort recovers, and the next transaction is clean.
//
// The cluster is at transaction.version 0, which is how a 4.5 broker looks
// before its cluster finalizes the feature. Under KIP-890 part 2 the commit
// of every transaction bumps our epoch and resets our sequence numbers, so
// the first produce of the next transaction is at sequence zero and the
// broker accepts it.
func TestRecreationTxnAfterSwap(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	downgradeToTxnV0(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	lg := new(capLogger)
	txcl := newPlainClient(t, c,
		kgo.TransactionalID("tx-after-swap"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.MetadataMinAge(100*time.Millisecond),
		kgo.WithLogger(lg),
	)
	admin := newPlainClient(t, c)

	// A first transaction commits before the recreation.
	if err := txcl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := txnProduceSync(t, txcl, topic, "a0"); err != nil {
		t.Fatal(err)
	}
	if err := txcl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}

	// The swap lands between transactions, with nothing exposed to it.
	recreateTopic(t, admin, topic, 1)
	waitForLog(t, txcl, lg, logProduceSwap, 1)
	if n := lg.count(logTxnObserved); n != 0 {
		t.Fatalf("saw %d transaction observations; want none, no transaction was exposed", n)
	}

	// The next transaction's first produce is the one the broker rejects.
	if err := txcl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := txnProduceSync(t, txcl, topic, "b0"); !errors.Is(err, kerr.TransactionAbortable) {
		t.Fatalf("first produce after the swap got %v; want an abortable transaction error", err)
	}
	err := txcl.EndTransaction(ctx, kgo.TryCommit)
	if !errors.Is(err, kerr.OperationNotAttempted) || !errors.Is(err, kerr.TransactionAbortable) {
		t.Fatalf("commit got %v; want a refusal carrying the abortable recreation error", err)
	}
	if err := txcl.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatalf("abort after the swap: %v", err)
	}

	// The next transaction is clean on the new topic.
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

// The merge fails an exposed transaction on the first observation of a
// recreated topic it produced to. The commit is refused through the failed
// producer ID, and the abort recovers: at the latest version through the
// KIP-890 part 2 arm, at 3.0 through the pre-part-2 arm with the abort sent
// on the wire first.
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
			t.Fatalf("commit got %v; want a refusal carrying the abortable recreation error", err)
		}
		if err := txcl.EndTransaction(ctx, kgo.TryAbort); err != nil {
			t.Fatalf("abort after the observation: %v", err)
		}

		// The same merge swapped the partition onto the new topic, so
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

// A recreated topic's partitions start at leader epoch 0, below the old
// topic's once it has ever changed leader. The merge must take the new ID
// before it compares leader epochs, or every partition takes the epoch
// rewind path and keeps producing under an ID the topic no longer has,
// which leaves a transaction that wrote to the deleted topic free to
// commit.
func TestRecreationTxnPoisonAfterLeaderEpochBump(t *testing.T) {
	t.Parallel()

	const topic = "t"
	c := newCluster(t, NumBrokers(2), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	lg := new(capLogger)
	txcl := newPlainClient(t, c,
		kgo.TransactionalID("tx-epoch"),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.WithLogger(lg),
	)
	admin := newPlainClient(t, c)

	// Raise the old topic's leader epoch above the 0 a recreated topic
	// starts at.
	for range 2 {
		leader := c.LeaderFor(topic, 0)
		if err := c.MoveTopicPartition(topic, 0, 1-leader); err != nil {
			t.Fatal(err)
		}
	}

	if err := txcl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := txnProduceSync(t, txcl, topic, "a0"); err != nil {
		t.Fatal(err)
	}
	recreateTopic(t, admin, topic, 1)
	waitForLog(t, txcl, lg, logTxnObserved, 1)

	err := txcl.EndTransaction(ctx, kgo.TryCommit)
	if !errors.Is(err, kerr.TransactionAbortable) {
		t.Fatalf("commit got %v; want the abortable recreation error", err)
	}
	if err := txcl.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatalf("abort after the observation: %v", err)
	}
}

// A share consumer swaps across a recreation: consumption continues on the
// new topic's share state, and acknowledgments of records acquired from the
// old topic are sent under the old ID and fail with the broker's
// UNKNOWN_TOPIC_ID.
func TestRecreationShareSwap(t *testing.T) {
	t.Parallel()

	const topic, group = "t", "sg"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	admin := newPlainClient(t, c)
	setShareAutoOffsetReset(t, admin, group)

	var ackMu sync.Mutex
	var ackResults kgo.ShareAckResults
	lg := new(capLogger)
	cl := newShareConsumer(t, c, topic, group,
		kgo.ShareAckCallback(func(_ *kgo.Client, results kgo.ShareAckResults) {
			ackMu.Lock()
			defer ackMu.Unlock()
			ackResults = append(ackResults, results...)
		}),
		kgo.WithLogger(lg),
	)

	// Acquire the old topic's records without acknowledging them: they
	// arrive in one poll, and we do not poll again until after the swap,
	// so the implicit ack never fires for them.
	produceVals(t, c, topic, 0, "v0", "v1", "v2")
	rs := collectRecords(t, cl, 3, 5*time.Second)

	recreateTopic(t, admin, topic, 1)
	waitForLog(t, cl, lg, logShareSwap, 1)

	// The acks of the old topic's records go to the broker under the old
	// ID, which it no longer knows. The flush returns once the callback
	// has seen the broker's answer, so nothing stays pending.
	cl.MarkAcks(kgo.AckAccept, rs...)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.FlushAcks(ctx); err != nil {
		t.Fatalf("flush acks: %v", err)
	}
	ackMu.Lock()
	var sawUnknownID bool
	for _, r := range ackResults {
		if r.Topic == topic && r.Partition == 0 && errors.Is(r.Err, kerr.UnknownTopicID) {
			sawUnknownID = true
		}
	}
	ackMu.Unlock()
	if !sawUnknownID {
		t.Fatalf("acks of old-topic records did not fail with UNKNOWN_TOPIC_ID; callback results: %v", ackResults)
	}

	// The new topic starts fresh share state: consumption continues.
	produceVals(t, c, topic, 0, "n0", "n1")
	collectVals(t, cl, "n0", "n1")
}
