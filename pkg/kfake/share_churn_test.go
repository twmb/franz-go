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
