package kfake

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Round 9 audit repros (share-churn.md). These tests stage broker churn
// against the share consumer's fetch path:
//
//   - TestShareFetchLeaderMoveNoHintHeals: share-churn.md 3.1 - a leader
//     move whose NOT_LEADER responses carry no CurrentLeader hint (the
//     leaderless-window shape; also every error code the broker never
//     hints for) must not surface retriable errors to poll, and must
//     trigger a metadata refresh so the cursor migrates. Pre-fix both
//     halves fail: NotLeaderForPartition reaches Fetches.Errors() and
//     nothing consumes until the periodic refresh.
//   - TestShareFetchLeaderMoveHintHeals: control - the existing
//     CurrentLeader hint path migrates without metadata; passes pre-fix
//     and post-fix.
//   - TestShareFetchTransportErrorTriggersMetadata: share-churn.md 3.2 -
//     a dead broker (connection killed on every ShareFetch) must trigger
//     a metadata refresh from the fetch backoff path, like the classic
//     consumer's source backoff does.
//   - TestShareFetchTopLevelErrorBackoff: share-churn.md 3.3 - persistent
//     top-level ShareFetch errors must back off rather than hot-loop at
//     round-trip pace.
//   - TestShareAssignmentNegativePartitionNoPanic: share-churn.md 3.4 -
//     a negative partition number in a heartbeat assignment must be
//     skipped, not panic the manage goroutine.

// shareChurnConsumer returns a share consumer with metadata ages tuned so
// that (a) the periodic refresh cannot accidentally heal a stalled cursor
// within a test's observation window (max age 2m) and (b) the
// triggered-refresh path is not debounced into oblivion (min age 100ms).
func shareChurnConsumer(t *testing.T, c *Cluster, topic, group string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	opts = append([]kgo.Opt{
		kgo.MetadataMinAge(100 * time.Millisecond),
		kgo.MetadataMaxAge(2 * time.Minute),
	}, opts...)
	return newShareConsumer(t, c, topic, group, opts...)
}

// pollShareUntil polls until want records arrive or the deadline passes,
// returning the records seen and whether any polled error matched errIs.
func pollShareUntil(t *testing.T, cl *kgo.Client, want int, timeout time.Duration, errIs *kerr.Error) (got int, sawErr bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for ctx.Err() == nil && got < want {
		fetches := cl.PollFetches(ctx)
		got += len(fetches.Records())
		for _, fe := range fetches.Errors() {
			if errIs != nil && errors.Is(fe.Err, errIs) {
				sawErr = true
			}
		}
	}
	return got, sawErr
}

// TestShareFetchLeaderMoveNoHintHeals moves a partition's leadership and
// makes the old leader answer ShareFetch with NOT_LEADER and no
// CurrentLeader hint - what a real broker sends while the new leader is
// unknown (KafkaApis populates the hint only when getCurrentLeader knows
// one), and the only shape available for error codes that never carry
// hints (UNKNOWN_TOPIC_ID, KAFKA_STORAGE_ERROR, ...).
//
// The share consumer must (a) not surface the retriable error to poll
// (classic strips these; Java's share consumer swallows + requests
// metadata) and (b) heal via a triggered metadata refresh that migrates
// the cursor to the new leader. Pre-fix, NotLeaderForPartition surfaces
// on every poll and the partition stalls until the periodic refresh.
func TestShareFetchLeaderMoveNoHintHeals(t *testing.T) {
	t.Parallel()

	const (
		topic = "share-churn-nohint"
		group = "share-churn-nohint-g"
		pre   = 3
		post  = 3
	)

	c := newCluster(t,
		NumBrokers(2),
		SeedTopics(1, topic),
		BrokerConfigs(map[string]string{
			"group.share.heartbeat.interval.ms": "100",
		}),
	)
	produceShareN(t, c, topic, group, pre)

	ti := c.TopicInfo(topic)
	if ti == nil {
		t.Fatal("topic info missing")
	}
	oldLeader := c.LeaderFor(topic, 0)
	newLeader := (oldLeader + 1) % 2

	// Once moved is set, every ShareFetch served by the old leader gets
	// a manual NOT_LEADER response with CurrentLeader=-1/-1 (no hint)
	// and no NodeEndpoints. Requests to the new leader pass through.
	var moved atomic.Bool
	c.ControlKey(int16(kmsg.ShareFetch), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if !moved.Load() || c.CurrentNode() != oldLeader {
			return nil, nil, false
		}
		req := kreq.(*kmsg.ShareFetchRequest)
		resp := req.ResponseKind().(*kmsg.ShareFetchResponse)
		resp.AcquisitionLockTimeoutMillis = 30000
		rt := kmsg.NewShareFetchResponseTopic()
		rt.TopicID = ti.TopicID
		rp := kmsg.NewShareFetchResponseTopicPartition()
		rp.Partition = 0
		rp.ErrorCode = kerr.NotLeaderForPartition.Code
		rp.CurrentLeader.LeaderID = -1 // leaderless window: no hint
		rp.CurrentLeader.LeaderEpoch = -1
		rt.Partitions = append(rt.Partitions, rp)
		resp.Topics = append(resp.Topics, rt)
		return resp, nil, true
	})

	cl := shareChurnConsumer(t, c, topic, group)

	if got, _ := pollShareUntil(t, cl, pre, 10*time.Second, nil); got < pre {
		t.Fatalf("pre-move: consumed %d of %d records", got, pre)
	}

	if err := c.MoveTopicPartition(topic, 0, newLeader); err != nil {
		t.Fatal(err)
	}
	moved.Store(true)
	produceN(t, c, topic, post)

	// Post-fix: the stripped NOT_LEADER triggers a metadata refresh, the
	// merge migrates the cursor to the new leader, and the post-move
	// records flow well within the window. Pre-fix: the error surfaces
	// to poll and nothing re-resolves the leader (metadata max age is
	// 2m), so this fails on both assertions.
	got, sawNotLeader := pollShareUntil(t, cl, post, 15*time.Second, kerr.NotLeaderForPartition)
	if sawNotLeader {
		t.Error("BUG REPRODUCED: retriable NotLeaderForPartition surfaced to PollFetches; classic strips it (KeepRetryableFetchErrors is unset)")
	}
	if got < post {
		t.Errorf("BUG REPRODUCED: consumed %d of %d post-move records; share fetch errors never trigger a metadata refresh so the cursor never migrates", got, post)
	}
}

// TestShareFetchLeaderMoveHintHeals is the control for
// TestShareFetchLeaderMoveNoHintHeals: when the old leader's NOT_LEADER
// response carries a CurrentLeader hint (kfake fills it whenever it knows
// the leader, like a settled real broker), the client migrates the cursor
// via applyMoves with no metadata round trip and surfaces nothing. This
// passes pre-fix and post-fix; it pins the moves arm staying first.
func TestShareFetchLeaderMoveHintHeals(t *testing.T) {
	t.Parallel()

	const (
		topic = "share-churn-hint"
		group = "share-churn-hint-g"
		pre   = 3
		post  = 3
	)

	c := newCluster(t,
		NumBrokers(2),
		SeedTopics(1, topic),
		BrokerConfigs(map[string]string{
			"group.share.heartbeat.interval.ms": "100",
		}),
	)
	produceShareN(t, c, topic, group, pre)

	cl := shareChurnConsumer(t, c, topic, group)
	if got, _ := pollShareUntil(t, cl, pre, 10*time.Second, nil); got < pre {
		t.Fatalf("pre-move: consumed %d of %d records", got, pre)
	}

	oldLeader := c.LeaderFor(topic, 0)
	if err := c.MoveTopicPartition(topic, 0, (oldLeader+1)%2); err != nil {
		t.Fatal(err)
	}
	produceN(t, c, topic, post)

	got, sawNotLeader := pollShareUntil(t, cl, post, 15*time.Second, kerr.NotLeaderForPartition)
	if sawNotLeader {
		t.Error("NotLeaderForPartition surfaced to PollFetches despite a CurrentLeader hint")
	}
	if got < post {
		t.Errorf("consumed %d of %d post-move records; CurrentLeader hint migration failed", got, post)
	}
}

// TestShareFetchTransportErrorTriggersMetadata kills the connection of
// every ShareFetch sent to the old leader after a leadership move - the
// observable shape of a dead broker process. No response means no
// CurrentLeader hint is possible; the only heal is a metadata refresh.
// The classic consumer's fetch backoff opportunistically triggers one
// (source.go backoff); the share fetch backoff must do the same.
// Pre-fix the cursor is stranded until the periodic refresh (2m here).
func TestShareFetchTransportErrorTriggersMetadata(t *testing.T) {
	t.Parallel()

	const (
		topic = "share-churn-transport"
		group = "share-churn-transport-g"
		pre   = 3
		post  = 3
	)

	c := newCluster(t,
		NumBrokers(2),
		SeedTopics(1, topic),
		BrokerConfigs(map[string]string{
			"group.share.heartbeat.interval.ms": "100",
		}),
	)
	produceShareN(t, c, topic, group, pre)

	oldLeader := c.LeaderFor(topic, 0)
	newLeader := (oldLeader + 1) % 2

	var moved atomic.Bool
	c.ControlKey(int16(kmsg.ShareFetch), func(kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if !moved.Load() || c.CurrentNode() != oldLeader {
			return nil, nil, false
		}
		// A non-nil control error closes the client connection:
		// the client sees a transport error, as with a dead broker.
		return nil, errors.New("simulated broker death"), true
	})

	cl := shareChurnConsumer(t, c, topic, group)
	if got, _ := pollShareUntil(t, cl, pre, 10*time.Second, nil); got < pre {
		t.Fatalf("pre-move: consumed %d of %d records", got, pre)
	}

	if err := c.MoveTopicPartition(topic, 0, newLeader); err != nil {
		t.Fatal(err)
	}
	moved.Store(true)
	produceN(t, c, topic, post)

	got, _ := pollShareUntil(t, cl, post, 15*time.Second, nil)
	if got < post {
		t.Errorf("BUG REPRODUCED: consumed %d of %d post-move records; the share fetch transport-failure backoff never triggers a metadata refresh so the cursor never leaves the dead broker", got, post)
	}
}

// TestShareFetchTopLevelErrorBackoff returns a top-level error from every
// ShareFetch (the persistent shape: e.g. group ACL revoked mid-run; kfake
// and KafkaApis both answer GROUP_AUTHORIZATION_FAILED top-level) and
// asserts the client backs off between attempts. Transport errors and
// all-errors-stripped responses already back off; pre-fix the top-level
// arm returns straight into the next fetch and hot-loops at round-trip
// pace - in-process that is thousands of requests in the window.
func TestShareFetchTopLevelErrorBackoff(t *testing.T) {
	t.Parallel()

	const (
		topic = "share-churn-toperr"
		group = "share-churn-toperr-g"
	)

	c := newCluster(t,
		NumBrokers(1),
		SeedTopics(1, topic),
		BrokerConfigs(map[string]string{
			"group.share.heartbeat.interval.ms": "100",
		}),
	)
	produceShareN(t, c, topic, group, 1)

	var (
		mu    sync.Mutex
		times []time.Time
	)
	c.ControlKey(int16(kmsg.ShareFetch), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		mu.Lock()
		times = append(times, time.Now())
		mu.Unlock()
		req := kreq.(*kmsg.ShareFetchRequest)
		resp := req.ResponseKind().(*kmsg.ShareFetchResponse)
		resp.AcquisitionLockTimeoutMillis = 30000
		resp.ErrorCode = kerr.GroupAuthorizationFailed.Code
		return resp, nil, true
	})

	cl := shareChurnConsumer(t, c, topic, group)

	// Poll in the background; nothing will ever arrive. The poll paces
	// nothing here: discarded fetches are not buffered, so the fetch
	// loop is self-paced (pre-fix: not at all).
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	go func() {
		for ctx.Err() == nil {
			cl.PollFetches(ctx)
		}
	}()

	// Wait for the first ShareFetch, then measure a 2s window.
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := len(times)
		mu.Unlock()
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no ShareFetch arrived")
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(2 * time.Second)
	mu.Lock()
	n := len(times)
	mu.Unlock()

	// Post-fix pacing: retryBackoff walks 250ms, 500ms, 750ms, 1s with
	// +/-20% jitter, so a 2s window sees ~4-6 attempts. Allow 12 for CI
	// noise. Pre-fix the loop is round-trip paced and lands hundreds to
	// thousands of attempts.
	if n > 12 {
		t.Errorf("BUG REPRODUCED: %d ShareFetch requests within ~2s of the first; top-level errors are retried with no backoff", n)
	}
}

// TestShareAssignmentNegativePartitionNoPanic fabricates a share group
// heartbeat response whose assignment contains a negative partition
// number. The broker owns the assignment and a sane broker never sends
// negatives, but a buggy or hostile one can; the classic/848 assignment
// funnel routes negative indexes to safety (consumer.go partition >= 0
// bounds check) while the share path indexed td.partitions[p] with only
// an upper-bound check. Pre-fix this panics the manage goroutine (and
// the process); post-fix the bogus index is skipped with a warn and the
// valid partition 0 keeps consuming.
func TestShareAssignmentNegativePartitionNoPanic(t *testing.T) {
	t.Parallel()

	const (
		topic = "share-churn-negpart"
		group = "share-churn-negpart-g"
		total = 3
	)

	c := newCluster(t,
		NumBrokers(1),
		SeedTopics(1, topic),
		BrokerConfigs(map[string]string{
			"group.share.heartbeat.interval.ms": "100",
		}),
	)
	produceShareN(t, c, topic, group, total)

	ti := c.TopicInfo(topic)
	if ti == nil {
		t.Fatal("topic info missing")
	}

	// Pass the epoch-0 join through to kfake (real member, real
	// assignment), then hijack exactly one post-join heartbeat to
	// deliver an assignment of partitions [0, -1] at the member's
	// current epoch.
	var injected atomic.Bool
	c.ControlKey(int16(kmsg.ShareGroupHeartbeat), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		req := kreq.(*kmsg.ShareGroupHeartbeatRequest)
		if req.MemberEpoch <= 0 || !injected.CompareAndSwap(false, true) {
			return nil, nil, false
		}
		resp := req.ResponseKind().(*kmsg.ShareGroupHeartbeatResponse)
		resp.MemberEpoch = req.MemberEpoch
		resp.HeartbeatIntervalMillis = 100
		as := kmsg.NewShareGroupHeartbeatResponseAssignment()
		tp := kmsg.NewShareGroupHeartbeatResponseAssignmentTopicPartition()
		tp.TopicID = ti.TopicID
		tp.Partitions = []int32{0, -1}
		as.TopicPartitions = append(as.TopicPartitions, tp)
		resp.Assignment = &as
		return resp, nil, true
	})

	cl := shareChurnConsumer(t, c, topic, group)

	if got, _ := pollShareUntil(t, cl, total, 10*time.Second, nil); got < total {
		t.Fatalf("pre-inject: consumed %d of %d records", got, total)
	}

	// The client clamps heartbeat sleeps to >= 1s, so the first
	// post-join heartbeat (the one we hijack) lands about a second in.
	deadline := time.Now().Add(8 * time.Second)
	for !injected.Load() {
		if time.Now().After(deadline) {
			t.Fatal("never injected the negative-partition assignment")
		}
		time.Sleep(20 * time.Millisecond)
	}

	// Pre-fix, processing the [0, -1] assignment panics the manage
	// goroutine (index out of range), crashing the process. Post-fix
	// the -1 is skipped and partition 0 keeps consuming.
	produceN(t, c, topic, total)
	if got, _ := pollShareUntil(t, cl, total, 10*time.Second, nil); got < total {
		t.Errorf("consumed %d of %d records after negative-partition assignment", got, total)
	}
}

// TestShareFetchPiggybackAckForgetNoLeak reproduces the share-session leak
// that hung TestShareGroupETL. The broker adds EVERY partition listed in a
// ShareFetch's topics to its share session, whether the partition is there to
// fetch or only to carry piggybacked acks. The client, however, tracked only
// fetched partitions in sessionParts, and it computes its forget set as
// sessionParts minus the want-set. So a partition listed only to piggyback an
// ack -- one revoked, paused, or migrated after its records were drained --
// was added to the broker session but never recorded by the client, hence
// never forgotten. The broker then kept re-acquiring and redelivering its
// records forever while the client discarded them ("broker returned partition
// ... we did not ask for") and spun the fetch loop until the test timed out.
//
// The minimal deterministic trigger staged here:
//   - partition 0 holds a record; partition 1 stays empty so the consumer
//     keeps issuing ShareFetches that piggybacked acks can ride (empty
//     responses are not buffered, so this needs no polling),
//   - poll once and hold partition 0's record unacked,
//   - pause partition 0 so the client forgets it (no acks drained yet),
//   - ack the held record: the ack rides a ShareFetch as a piggyback-only
//     re-add of partition 0, which the broker puts back in the session.
//
// We watch the client's actual ShareFetch requests. Post-fix, after the
// piggyback re-add the client tracks partition 0 and forgets it again (a
// fresh ForgottenTopicsData entry). Pre-fix it never does -- the leak.
func TestShareFetchPiggybackAckForgetNoLeak(t *testing.T) {
	t.Parallel()

	const (
		topic = "share-piggyback-leak"
		group = "share-piggyback-leak-g"
	)

	c := newCluster(t,
		NumBrokers(1),
		SeedTopics(2, topic),
		BrokerConfigs(map[string]string{
			"group.share.heartbeat.interval.ms": "100",
		}),
	)

	ti := c.TopicInfo(topic)
	if ti == nil {
		t.Fatal("topic info missing")
	}

	// Seed exactly one record on partition 0; partition 1 stays empty.
	pcl := newPlainClient(t, c, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	setShareAutoOffsetReset(t, pcl, group)
	if err := pcl.ProduceSync(context.Background(),
		&kgo.Record{Topic: topic, Partition: 0, Value: []byte("v")}).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}

	// Observe the client's ShareFetch requests (pass-through, never
	// answering). p0ForgetCount counts ForgottenTopicsData entries for
	// partition 0; sawPiggybackAfterAck records the piggyback-only re-add.
	var (
		acked                atomic.Bool
		p0ForgetCount        atomic.Int32
		sawPiggybackAfterAck atomic.Bool
	)
	c.ControlKey(int16(kmsg.ShareFetch), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		req := kreq.(*kmsg.ShareFetchRequest)
		for i := range req.Topics {
			rt := &req.Topics[i]
			if rt.TopicID != ti.TopicID {
				continue
			}
			for j := range rt.Partitions {
				rp := &rt.Partitions[j]
				if rp.Partition == 0 && len(rp.AcknowledgementBatches) > 0 && acked.Load() {
					sawPiggybackAfterAck.Store(true)
				}
			}
		}
		for i := range req.ForgottenTopicsData {
			ft := &req.ForgottenTopicsData[i]
			if ft.TopicID != ti.TopicID {
				continue
			}
			for _, p := range ft.Partitions {
				if p == 0 {
					p0ForgetCount.Add(1)
				}
			}
		}
		return nil, nil, false
	})

	cl := newShareConsumer(t, c, topic, group, kgo.FetchMaxWait(100*time.Millisecond))

	// Poll exactly once and hold partition 0's record unacked. Polling
	// again would auto-accept it (the share consumer accepts un-acked
	// records on the next poll), draining the ack before we want it.
	// Partition 1 is empty, so the consumer keeps long-polling it without
	// us draining anything.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var held []*kgo.Record
	for ctx.Err() == nil && len(held) == 0 {
		held = cl.PollFetches(ctx).Records()
	}
	if len(held) != 1 || held[0].Partition != 0 {
		t.Fatalf("wanted 1 record on partition 0, got %d records", len(held))
	}

	// Pause partition 0. It is in sessionParts and no longer wanted, so the
	// client forgets it on the next ShareFetch (no acks drained yet). Wait
	// for that forget so the later ack is a clean piggyback-only re-add.
	cl.PauseFetchPartitions(map[string][]int32{topic: {0}})
	waitFor(t, 5*time.Second, "partition 0 was never forgotten after pause",
		func() bool { return p0ForgetCount.Load() >= 1 })
	forgetsBeforeAck := p0ForgetCount.Load()

	// Ack the held record. The ack rides a ShareFetch as a piggyback-only
	// entry for partition 0, which the broker re-adds to its session.
	acked.Store(true)
	for _, r := range held {
		r.Ack(kgo.AckAccept)
	}

	// Sanity: confirm the leak trigger actually fired (true for both the
	// buggy and fixed client -- the fix changes only what happens next).
	waitFor(t, 5*time.Second, "partition 0's ack never rode a ShareFetch as a piggyback re-add", sawPiggybackAfterAck.Load)

	// The fix: now that partition 0 is back in the broker session, the
	// client must forget it again (it is tracked in sessionParts and still
	// unwanted). Pre-fix it is untracked, so no further forget is ever
	// sent and the partition leaks.
	waitFor(t, 5*time.Second,
		"BUG REPRODUCED: a piggyback-only ack re-added partition 0 to the broker share session and the client never forgot it (sessionParts omitted piggyback-only partitions); the broker would redeliver its records forever",
		func() bool { return p0ForgetCount.Load() > forgetsBeforeAck })
}

// waitFor polls cond until it returns true or the timeout elapses, failing
// with msg on timeout.
func waitFor(t *testing.T, timeout time.Duration, msg string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		if cond() {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal(msg)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
