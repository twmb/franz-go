package kfake

import (
	"context"
	"errors"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestOffsetForLeaderEpochEndsTheNewestEpochAtTheHighWatermark asks for the end
// offset of the epoch every batch in the log carries, after the leader epoch
// has been bumped with nothing produced since.
//
// That epoch has not ended -- no batch carries a later one -- so its end is
// the high watermark, which is the same answer the current-epoch branch of the
// handler already gives. Answering -1 instead says something quite different:
// EndOffset < 0 is KIP-320's UNDEFINED_EPOCH_OFFSET, "the broker has no record
// of the epoch you asked about", which is false of a log that is full of
// batches at it.
//
// A client is entitled to act on that answer, and kgo does: it cannot locate
// where the log diverged, so it resets the partition through
// ConsumeResetOffset. For the common `AtEnd` reset that means skipping every
// record between the cursor and the log end -- see
// TestALeaderChangeDoesNotSkipRecordsUnderAtEndReset below.
func TestOffsetForLeaderEpochEndsTheNewestEpochAtTheHighWatermark(t *testing.T) {
	t.Parallel()
	const topic = "ofle-newest-epoch"
	const records = 5
	// One broker, so the shuffle cannot move the leader and the only thing
	// that changes is the epoch: the question is purely about the answer for
	// an epoch the log still holds.
	c := newCoverCluster(t, NumBrokers(1), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	cl := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
	for range records {
		if err := cl.ProduceSync(ctx, kgo.StringRecord("v")).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	// Bump the leader epoch with nothing produced after it, which is what a
	// client sees after an election.
	c.ShufflePartitionLeaders()

	adm := kadm.NewClient(cl)
	var req kadm.OffsetForLeaderEpochRequest
	req.Add(topic, 0, 0) // the epoch every batch in the log carries
	resp, err := adm.OffsetForLeaderEpoch(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	r, ok := resp[topic][0]
	if !ok {
		t.Fatal("no response for the partition")
	}
	if r.Err != nil {
		t.Fatalf("the request was refused: %v", r.Err)
	}
	if r.EndOffset < 0 {
		t.Fatalf("EndOffset is %d, which is KIP-320's UNDEFINED_EPOCH_OFFSET -- \"no record of that epoch\" -- for an epoch that every one of the %d batches in the log carries; want the high watermark %d",
			r.EndOffset, records, records)
	}
	if r.EndOffset != records {
		t.Errorf("EndOffset is %d, want the high watermark %d: the requested epoch is the newest one in the log, so it has not ended", r.EndOffset, records)
	}
}

// TestALeaderChangeDoesNotSkipRecordsUnderAtEndReset is the consequence of the
// above for a consumer, and the shape this was found through.
//
// A consumer that starts at the end and resets to the end is the common
// "latest" configuration. A leader election must not make it skip records it
// has not consumed: nothing was lost, so there is nothing to reset past.
//
// The skip itself is a race -- it needs the reset to resolve AtEnd after the
// next records have landed -- so what this asserts deterministically is that
// no fetch reports ErrDataLoss, which is the client acting on the -1. Run
// against a tree without the fix this fails on every run, with the skip itself
// appearing in roughly one run in thirty.
func TestALeaderChangeDoesNotSkipRecordsUnderAtEndReset(t *testing.T) {
	t.Parallel()
	const (
		parts = 4
		half  = 12
	)
	topic := "leaderepoch-noskip"
	group := "g-" + topic
	c := newCoverCluster(t, NumBrokers(3), SeedTopics(parts, topic))

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	pcl := newPlainClient(t, c, kgo.DefaultProduceTopic(topic), kgo.RequiredAcks(kgo.AllISRAcks()))
	produce := func(lo, hi int) {
		for i := lo; i < hi; i++ {
			r := kgo.StringRecord(strconv.Itoa(i))
			r.Key = []byte("k" + strconv.Itoa(i))
			if err := pcl.ProduceSync(ctx, r).FirstErr(); err != nil {
				t.Fatalf("producing %d: %v", i, err)
			}
		}
	}

	var (
		mu       sync.Mutex
		seen     = map[string]int{}
		dataLoss []error
	)
	ccl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtEnd()),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.DisableAutoCommit(),
	)

	poll := func(want int, budget time.Duration) {
		deadline := time.Now().Add(budget)
		for time.Now().Before(deadline) {
			mu.Lock()
			have := len(seen)
			mu.Unlock()
			if have >= want {
				return
			}
			pctx, pcancel := context.WithTimeout(ctx, time.Second)
			fs := ccl.PollFetches(pctx)
			pcancel()
			fs.EachError(func(_ string, _ int32, err error) {
				var dl *kgo.ErrDataLoss
				if errors.As(err, &dl) {
					mu.Lock()
					dataLoss = append(dataLoss, err)
					mu.Unlock()
				}
			})
			fs.EachRecord(func(r *kgo.Record) {
				mu.Lock()
				seen[string(r.Value)]++
				mu.Unlock()
			})
			if err := ccl.CommitUncommittedOffsets(ctx); err != nil && ctx.Err() == nil {
				t.Logf("commit: %v", err)
			}
		}
	}

	// Join and settle at the end of the log BEFORE producing, so "start at
	// the end" cannot be what skips the first half.
	poll(0, 3*time.Second)
	produce(0, half)
	poll(half, 30*time.Second)

	// Every partition gets a new leader and a bumped epoch: a rolling
	// restart, from a client's point of view.
	c.ShufflePartitionLeaders()

	produce(half, 2*half)
	poll(2*half, 40*time.Second)

	mu.Lock()
	defer mu.Unlock()
	if len(dataLoss) > 0 {
		t.Errorf("%d fetch(es) reported data loss after a leader change that lost nothing: %v", len(dataLoss), dataLoss[0])
	}
	var missing []string
	for i := range 2 * half {
		if seen[strconv.Itoa(i)] == 0 {
			missing = append(missing, strconv.Itoa(i))
		}
	}
	sort.Strings(missing)
	if len(missing) > 0 {
		t.Errorf("%d of %d record(s) never arrived after the leader change: %v", len(missing), 2*half, missing)
	}
}
