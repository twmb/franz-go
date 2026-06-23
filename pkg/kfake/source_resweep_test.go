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

// Regression test from the source.go ledger re-sweep (round 23). Fails before
// the kgo fix in loadEpochsForBrokerLoad.

// undefinedNextOFLE installs a control that answers exactly one
// OffsetForLeaderEpoch request with the KIP-320 UNDEFINED sentinel
// {error:NONE, leaderEpoch:-1, endOffset:-1}. A real broker returns this when
// its leader-epoch cache has no record of the requested epoch (empty/truncated
// cache, unclean election, or an epoch newer than the log), so it is a
// conformant response, not a hostile one. It closes fired when it answers.
func undefinedNextOFLE(c *Cluster) (fired chan struct{}) {
	fired = make(chan struct{})
	var once atomic.Bool
	c.ControlKey(int16(kmsg.OffsetForLeaderEpoch), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if once.Swap(true) {
			return nil, nil, false // later validations are served for real
		}
		req := kreq.(*kmsg.OffsetForLeaderEpochRequest)
		resp := req.ResponseKind().(*kmsg.OffsetForLeaderEpochResponse)
		for _, rt := range req.Topics {
			st := kmsg.NewOffsetForLeaderEpochResponseTopic()
			st.Topic = rt.Topic
			for _, rp := range rt.Partitions {
				sp := kmsg.NewOffsetForLeaderEpochResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = 0
				sp.LeaderEpoch = -1 // UNDEFINED_EPOCH
				sp.EndOffset = -1   // UNDEFINED_EPOCH_OFFSET
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		close(fired)
		return resp, nil, true
	})
	return fired
}

// TestAuditOFLEUndefinedEpochOffsetNotDataLoss verifies that an
// OffsetForLeaderEpoch validation answered with the KIP-320 UNDEFINED sentinel
// (endOffset -1, leaderEpoch -1) is NOT reported as data loss. Pre-fix,
// loadEpochsForBrokerLoad compared endOffset (-1) against the validating
// offset, saw `-1 < offset`, and surfaced a spurious ErrDataLoss while pinning
// the cursor at offset -1. Post-fix the sentinel routes to the normal
// OFFSET_OUT_OF_RANGE reset path with no false data-loss error, and the
// consumer keeps consuming.
func TestAuditOFLEUndefinedEpochOffsetNotDataLoss(t *testing.T) {
	t.Parallel()

	const (
		topic   = "ofle-undefined"
		initial = 5
		extra   = 3
	)

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	produceN(t, c, topic, initial)

	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableFetchSessions(),
		kgo.FetchMaxWait(100*time.Millisecond),
	)

	// Consume everything so the cursor sits at the log end with a
	// non-negative last consumed epoch: the client only issues an
	// OffsetForLeaderEpoch validation on the next fence when it has a real
	// epoch to validate against.
	collectRecords(t, cl, initial, 8*time.Second)

	// Fence the next fetch (queues the validation at offset 5) and answer
	// that validation with the UNDEFINED sentinel.
	fired := undefinedNextOFLE(c)
	fenceNextFetch(c, topic)

	select {
	case <-fired:
	case <-time.After(8 * time.Second):
		t.Fatal("timed out waiting for the epoch validation to be issued")
	}

	// Produce more records past the reset so we can confirm the consumer
	// recovers rather than stranding.
	produceN(t, c, topic, extra)

	// Poll past the validation. Any ErrDataLoss is the bug. We also require
	// at least one record at offset >= initial to prove the cursor reset and
	// resumed instead of stalling.
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	var resumed bool
	for ctx.Err() == nil && !resumed {
		fetches := cl.PollFetches(ctx)
		fetches.EachError(func(_ string, _ int32, err error) {
			var dl *kgo.ErrDataLoss
			if errors.As(err, &dl) {
				t.Fatalf("BUG REPRODUCED: spurious ErrDataLoss on UNDEFINED_EPOCH_OFFSET (endOffset -1): %v", err)
			}
		})
		fetches.EachRecord(func(r *kgo.Record) {
			if r.Offset >= initial {
				resumed = true
			}
		})
	}
	if !resumed {
		t.Fatal("consumer did not resume after the UNDEFINED_EPOCH_OFFSET validation")
	}
}

// TestAuditFetchTopLevelErrorBackoff returns an unexpected top-level error from
// every classic Fetch (the persistent shape: a non-conformant or future broker
// answering a code kgo's fetch-session arms do not recognize) and asserts the
// client backs off between attempts. The session-related top-level codes all
// self-heal in one round-trip, but the `default` arm has no such bounded heal;
// pre-fix it reset the session and returned straight into the next fetch,
// hot-looping at round-trip pace - in-process that is hundreds to thousands of
// requests in the window. Transport errors and all-errors-stripped responses
// already back off. Sibling of the share path's
// TestShareFetchTopLevelErrorBackoff (share-churn round).
func TestAuditFetchTopLevelErrorBackoff(t *testing.T) {
	t.Parallel()

	const topic = "fetch-toperr"

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	produceN(t, c, topic, 1)

	var (
		mu    sync.Mutex
		times []time.Time
	)
	c.ControlKey(int16(kmsg.Fetch), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		req := kreq.(*kmsg.FetchRequest)
		mu.Lock()
		times = append(times, time.Now())
		mu.Unlock()
		resp := req.ResponseKind().(*kmsg.FetchResponse)
		// An unexpected top-level code (not a fetch-session code) routes to
		// the default arm.
		resp.ErrorCode = kerr.UnknownServerError.Code
		return resp, nil, true
	})

	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(100*time.Millisecond),
	)

	// Poll in the background; nothing ever arrives (every fetch errors at the
	// top level). Discarded fetches are not buffered, so the fetch loop is
	// self-paced (pre-fix: not at all).
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	go func() {
		for ctx.Err() == nil {
			cl.PollFetches(ctx)
		}
	}()

	// Wait for the first Fetch, then measure a 2s window.
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := len(times)
		mu.Unlock()
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("no Fetch arrived")
		}
		time.Sleep(10 * time.Millisecond)
	}
	time.Sleep(2 * time.Second)
	mu.Lock()
	n := len(times)
	mu.Unlock()

	// Post-fix pacing: retryBackoff walks 250ms, 500ms, 750ms, 1s with
	// jitter, so a 2s window sees ~4-6 attempts. Allow 12 for CI noise.
	// Pre-fix the loop is round-trip paced and lands hundreds to thousands.
	if n > 12 {
		t.Errorf("BUG REPRODUCED: %d Fetch requests within ~2s of the first; unexpected top-level errors are retried with no backoff", n)
	}
}
