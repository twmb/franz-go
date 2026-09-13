package kfake

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// pollNoDataLoss polls for the duration and fails on any ErrDataLoss, returning
// every record it saw.
func pollNoDataLoss(t *testing.T, cl *kgo.Client, timeout time.Duration) []*kgo.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var recs []*kgo.Record
	for ctx.Err() == nil {
		fs := cl.PollFetches(ctx)
		fs.EachError(func(_ string, _ int32, err error) {
			var dl *kgo.ErrDataLoss
			if errors.As(err, &dl) {
				t.Errorf("reported data loss: %v", dl)
			}
		})
		fs.EachRecord(func(r *kgo.Record) { recs = append(recs, r) })
	}
	return recs
}

// TestOutOfRangeNothingConsumedUsesPolicy requires an out of range seek on a
// partition we have never consumed to follow ConsumeResetOffset, whose default
// starts at the beginning. The default reset looks back from the last record
// consumed, and there is none here, so the lookback has to fall away and leave
// the offset's position: a lookback resolved against the clock instead would
// match nothing in an older log and skip every record still in it.
func TestOutOfRangeNothingConsumedUsesPolicy(t *testing.T) {
	t.Parallel()

	const (
		topic = "ooor-fresh-policy"
		nrecs = 10
	)

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	// An hour old, so a thirty second lookback matches nothing.
	produceSpaced(t, c, topic, nrecs, time.Now().Add(-time.Hour), time.Second)

	cl := newPlainClient(t, c,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().At(500)}}),
		kgo.DisableFetchSessions(),
		kgo.FetchMaxWait(100*time.Millisecond),
	)

	got := collectRecords(t, cl, nrecs, 8*time.Second)
	if got[0].Offset != 0 {
		t.Fatalf("resumed at offset %d, want 0: the log start", got[0].Offset)
	}
}

// TestUndefinedEpochNoResetStays requires NoResetOffset to keep a cursor where
// it was when an OffsetForLeaderEpoch validation cannot answer. That reset is
// issued from the validation rather than from a fetch, so it never passes the
// fetch path's NoResetOffset check and has to repeat it; without that, the
// reset runs the policy, and a bare NoResetOffset is an end offset, which skips
// every record between where we were and the end.
func TestUndefinedEpochNoResetStays(t *testing.T) {
	t.Parallel()

	const (
		topic  = "ooor-undef-noreset"
		nrecs  = 8
		nfirst = 5
	)

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	produceN(t, c, topic, nrecs)

	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
		kgo.ConsumeResetOffset(kgo.NoResetOffset()),
		kgo.DisableFetchSessions(),
		kgo.FetchMaxWait(100*time.Millisecond),
	)

	// Stop the cursor below the log end: one poll takes five of the eight
	// and pausing drops the rest, so the end is three above where we are
	// and resuming at the end is visibly different from staying put.
	fs := cl.PollRecords(context.Background(), nfirst)
	if err := fs.Err(); err != nil {
		t.Fatal(err)
	}
	if n := fs.NumRecords(); n != nfirst {
		t.Fatalf("polled %d records, want %d", n, nfirst)
	}
	cl.PauseFetchTopics(topic)
	verifyZeroRecords(t, cl, time.Second)

	fired := undefinedNextOFLE(c)
	fenceNextFetch(c, topic)
	cl.ResumeFetchTopics(topic)
	select {
	case <-fired:
	case <-time.After(8 * time.Second):
		t.Fatal("timed out waiting for the epoch validation to be issued")
	}

	got := pollNoDataLoss(t, cl, 4*time.Second)
	if len(got) != nrecs-nfirst {
		t.Fatalf("consumed %d records after the validation, want %d", len(got), nrecs-nfirst)
	}
	for i, r := range got {
		if want := int64(nfirst + i); r.Offset != want {
			t.Errorf("record %d is offset %d, want %d", i, r.Offset, want)
		}
	}
}

// TestUndefinedEpochNothingConsumedBounds requires a partition we have never
// consumed to bound its offset within the log rather than run the reset policy
// when an OffsetForLeaderEpoch validation cannot answer. A committed offset
// carries an epoch, so the validation runs before the first record is read:
// there is nothing to recover to, and nothing was lost to report.
func TestUndefinedEpochNothingConsumedBounds(t *testing.T) {
	t.Parallel()

	const (
		topic = "ooor-undef-fresh"
		nrecs = 10
		at    = 5
	)

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	produceN(t, c, topic, nrecs)

	undefinedNextOFLE(c)

	// An exact offset with an epoch is validated before the first fetch,
	// which is the shape a group consumer resuming from a commit has.
	cl := newPlainClient(t, c,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {0: kgo.NewOffset().At(at).WithEpoch(0)},
		}),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableFetchSessions(),
		kgo.FetchMaxWait(100*time.Millisecond),
	)

	// AtStart is the reset policy, so running it resumes at offset 0.
	// Bounding keeps the offset we asked for, which is in the log.
	got := pollNoDataLoss(t, cl, 4*time.Second)
	if len(got) == 0 {
		t.Fatal("consumed nothing after the validation")
	}
	if got[0].Offset != at {
		t.Fatalf("resumed at offset %d, want %d", got[0].Offset, at)
	}
}

// TestOutOfRangeBelowStartIgnoresPolicy requires a cursor that fell below the
// log start to resume at the start whatever the reset policy says. The start is
// exact there: every record that still exists is one we never consumed, so
// there is nothing for a policy to decide, and AtEnd would skip all of it.
func TestOutOfRangeBelowStartIgnoresPolicy(t *testing.T) {
	t.Parallel()

	const (
		topic  = "ooor-belowstart-policy"
		nrecs  = 10
		nfirst = 5
		delTo  = 7
	)

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	produceN(t, c, topic, nrecs)

	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.DisableFetchSessions(),
		kgo.FetchMaxWait(100*time.Millisecond),
	)

	fs := cl.PollRecords(context.Background(), nfirst)
	if err := fs.Err(); err != nil {
		t.Fatal(err)
	}
	if n := fs.NumRecords(); n != nfirst {
		t.Fatalf("polled %d records, want %d", n, nfirst)
	}

	cl.PauseFetchTopics(topic)
	verifyZeroRecords(t, cl, time.Second)
	deleteRecordsTo(t, c, topic, delTo)
	cl.ResumeFetchTopics(topic)

	got := collectRecords(t, cl, nrecs-delTo, 8*time.Second)
	for i, r := range got {
		if want := int64(delTo + i); r.Offset != want {
			t.Errorf("record %d is offset %d, want %d", i, r.Offset, want)
		}
	}
}
