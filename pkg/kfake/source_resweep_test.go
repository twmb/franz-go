package kfake

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Regression test: fails before the kgo fix in loadEpochsForBrokerLoad.

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

// TestAuditOFLEUndefinedEpochOffsetResetsInBounds verifies that an
// OffsetForLeaderEpoch validation answered with the KIP-320 UNDEFINED sentinel
// (endOffset -1, leaderEpoch -1) never pins the cursor at offset -1. Pre-fix,
// loadEpochsForBrokerLoad compared endOffset (-1) against the validating
// offset, saw `-1 < offset`, and reset the cursor to -1, reporting that as
// where it had consumed to. The sentinel now routes to the same reset an out
// of range fetch after consuming takes: we cannot locate where the log
// diverged, so ConsumeResetOffset picks a real offset, we report the loss from
// the offset we were at, and the consumer keeps consuming.
func TestAuditOFLEUndefinedEpochOffsetResetsInBounds(t *testing.T) {
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
	consumeN(t, cl, initial, 8*time.Second)

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
			if dl, ok := errors.AsType[*kgo.ErrDataLoss](err); ok {
				if dl.ResetTo < 0 {
					t.Fatalf("BUG REPRODUCED: reset to a negative offset on UNDEFINED_EPOCH_OFFSET (endOffset -1): %v", err)
				}
				if dl.ConsumedTo != initial {
					t.Fatalf("data loss reports consuming to %d, want %d", dl.ConsumedTo, initial)
				}
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

// TestUndefinedEpochStays requires a cursor to stay where it was when an
// OffsetForLeaderEpoch validation cannot answer and there is nothing to reset
// from: NoResetOffset, or nothing consumed yet. That reset is issued from the
// validation rather than from a fetch, so it never passes the fetch path's
// checks and has to repeat them. Without them the reset runs the policy: a
// bare NoResetOffset is an end offset, which skips every record between where
// we were and the end, and AtStart re-reads the log. A committed offset
// carries an epoch, so the validation runs before the first record is read;
// there is nothing to recover to and nothing lost to report, and if the offset
// is out of the log the next fetch says so.
func TestUndefinedEpochStays(t *testing.T) {
	t.Parallel()

	const (
		topic = "ooor-undef"
		nrecs = 8
		at    = 5
	)

	for _, test := range []struct {
		name     string
		consumed bool
		opts     []kgo.Opt
	}{
		{name: "no reset after consuming", consumed: true, opts: []kgo.Opt{
			kgo.ConsumeTopics(topic),
			kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
			kgo.ConsumeResetOffset(kgo.NoResetOffset()),
		}},
		// An exact offset with an epoch is validated before the first
		// fetch, which is the shape of a group consumer resuming from a
		// commit.
		{name: "nothing consumed", opts: []kgo.Opt{
			kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().At(at).WithEpoch(0)}}),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
			produceN(t, c, topic, nrecs)

			if !test.consumed {
				undefinedNextOFLE(c)
			}
			cl := newPlainClient(t, c, append([]kgo.Opt{
				kgo.DisableFetchSessions(),
				kgo.FetchMaxWait(100 * time.Millisecond),
			}, test.opts...)...)
			if test.consumed {
				// Stop the cursor at 5 with the log end three above
				// it, so resuming at the end is visibly different
				// from staying put.
				consumeThenPause(t, cl, topic, at)
				fired := undefinedNextOFLE(c)
				fenceNextFetch(c, topic)
				cl.ResumeFetchTopics(topic)
				waitCh(t, fired, "the epoch validation was never issued")
			}

			dl, got := pollDataLoss(cl, 4*time.Second)
			if dl != nil {
				t.Errorf("reported data loss: %v", dl)
			}
			if len(got) != nrecs-at {
				t.Fatalf("consumed %d records after the validation, want %d", len(got), nrecs-at)
			}
			for i, r := range got {
				if want := int64(at + i); r.Offset != want {
					t.Errorf("record %d is offset %d, want %d", i, r.Offset, want)
				}
			}
		})
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

	// The observer goes in first: the fault below answers every fetch, so
	// only a fault ahead of it counts them.
	attempts := c.Fault(Fault{Keys: []kmsg.Key{kmsg.Fetch}, TopLevel: true, Observe: true, Count: -1})
	// An unexpected top-level code (not a fetch-session code) routes to the
	// default arm.
	c.Fault(Fault{
		Keys:     []kmsg.Key{kmsg.Fetch},
		TopLevel: true,
		Err:      kerr.UnknownServerError,
		Count:    -1,
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
	if err := attempts.Wait(ctx, 1); err != nil {
		t.Fatal("no Fetch arrived")
	}
	first := attempts.Hits()
	time.Sleep(2 * time.Second)
	n := attempts.Hits() - first

	// Post-fix pacing: retryBackoff walks 250ms, 500ms, 750ms, 1s with
	// jitter, so a 2s window sees ~4-6 attempts. Allow 12 for CI noise.
	// Pre-fix the loop is round-trip paced and lands hundreds to thousands.
	if n > 12 {
		t.Errorf("BUG REPRODUCED: %d Fetch requests within ~2s of the first; unexpected top-level errors are retried with no backoff", n)
	}
}
