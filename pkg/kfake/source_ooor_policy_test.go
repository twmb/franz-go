package kfake

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

const (
	oorNrecs   = 10
	oorConsume = 8
	oorApart   = 10 * time.Second
)

// produceSpaced produces n records to the topic, each its own batch, stamped
// apart from each other starting at base.
func produceSpaced(t *testing.T, c *Cluster, topic string, n int, base time.Time, apart time.Duration) {
	t.Helper()
	cl := newPlainClient(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := range n {
		r := &kgo.Record{Topic: topic, Value: fmt.Appendf(nil, "v%d", i), Timestamp: base.Add(time.Duration(i) * apart)}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
}

// oorPastEnd answers exactly one fetch at the given offset with
// OFFSET_OUT_OF_RANGE and unset watermarks, which is what Kafka sends. Every
// other fetch passes through. The returned flag reports whether it answered.
func oorPastEnd(c *Cluster, topic string, at int64) *atomic.Bool {
	ti := c.TopicInfo(topic)
	fired := new(atomic.Bool)
	c.ControlKey(int16(kmsg.Fetch), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		req := kreq.(*kmsg.FetchRequest)
		if len(req.Topics) != 1 || len(req.Topics[0].Partitions) != 1 ||
			req.Topics[0].Partitions[0].FetchOffset != at || fired.Swap(true) {
			return nil, nil, false
		}
		resp := req.ResponseKind().(*kmsg.FetchResponse)
		rt := kmsg.NewFetchResponseTopic()
		rt.Topic = topic
		rt.TopicID = ti.TopicID
		rp := kmsg.NewFetchResponseTopicPartition()
		rp.Partition = 0
		rp.ErrorCode = kerr.OffsetOutOfRange.Code
		rp.HighWatermark = -1
		rp.LastStableOffset = -1
		rp.LogStartOffset = -1
		rt.Partitions = append(rt.Partitions, rp)
		resp.Topics = append(resp.Topics, rt)
		return resp, nil, true
	})
	return fired
}

// consumedPastEnd produces ten records ten seconds apart, consumes all of
// them, and arranges for the next fetch to be answered out of range. The
// cursor is then at the log end carrying the last record's timestamp, which is
// what a broker that lost its tail looks like from here.
func consumedPastEnd(t *testing.T, topic string, opts ...kgo.Opt) (*Cluster, *kgo.Client, *atomic.Bool) {
	t.Helper()
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	produceSpaced(t, c, topic, oorNrecs, time.Now().Add(-time.Hour), oorApart)
	fired := oorPastEnd(c, topic, oorNrecs)
	cl := newPlainClient(t, c, append([]kgo.Opt{
		kgo.ConsumeTopics(topic),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
		kgo.DisableFetchSessions(),
		kgo.FetchMaxWait(100 * time.Millisecond),
	}, opts...)...)
	collectRecords(t, cl, oorNrecs, 8*time.Second)
	return c, cl, fired
}

// consumedInRange produces ten records ten seconds apart, consumes eight of
// them, drops the rest, and arranges for the next fetch to be answered out of
// range. The cursor is then inside the log with the end above it, which is
// where resuming at the end and resuming where we were differ.
func consumedInRange(t *testing.T, topic string, opts ...kgo.Opt) (*Cluster, *kgo.Client, *atomic.Bool) {
	t.Helper()
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	produceSpaced(t, c, topic, oorNrecs, time.Now().Add(-time.Hour), oorApart)
	fired := oorPastEnd(c, topic, oorConsume)
	cl := newPlainClient(t, c, append([]kgo.Opt{
		kgo.ConsumeTopics(topic),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
		kgo.DisableFetchSessions(),
		kgo.FetchMaxWait(100 * time.Millisecond),
	}, opts...)...)

	// One poll takes eight of the ten records and leaves the rest buffered.
	// The source does not fetch again while it holds a buffer, so nothing is
	// in flight for the rest of the setup.
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	fs := cl.PollRecords(ctx, oorConsume)
	if err := fs.Err(); err != nil {
		t.Fatal(err)
	}
	if n := fs.NumRecords(); n != oorConsume {
		t.Fatalf("polled %d records, want %d", n, oorConsume)
	}

	// Pausing drops the buffered remainder, leaving the cursor at oorConsume
	// with the log end at oorNrecs.
	cl.PauseFetchTopics(topic)
	verifyZeroRecords(t, cl, time.Second)
	cl.ResumeFetchTopics(topic)
	return c, cl, fired
}

// deleteRecordsTo moves the log start offset of the topic's only partition.
func deleteRecordsTo(t *testing.T, c *Cluster, topic string, to int64) {
	t.Helper()
	cl := newPlainClient(t, c)
	req := kmsg.NewPtrDeleteRecordsRequest()
	rt := kmsg.NewDeleteRecordsRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewDeleteRecordsRequestTopicPartition()
	rp.Partition = 0
	rp.Offset = to
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if code := resp.Topics[0].Partitions[0].ErrorCode; code != 0 {
		t.Fatalf("delete records: %v", kerr.ErrorForCode(code))
	}
}

// pollReset polls until the reset reports data loss, returning it along with
// any records that arrived in the same polls.
func pollReset(t *testing.T, cl *kgo.Client, timeout time.Duration) (*kgo.ErrDataLoss, []*kgo.Record) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var (
		dl   *kgo.ErrDataLoss
		recs []*kgo.Record
	)
	for ctx.Err() == nil && dl == nil {
		fs := cl.PollFetches(ctx)
		fs.EachError(func(_ string, _ int32, err error) {
			var e *kgo.ErrDataLoss
			if errors.As(err, &e) {
				dl = e
			}
		})
		fs.EachRecord(func(r *kgo.Record) { recs = append(recs, r) })
	}
	if dl == nil {
		t.Fatal("the reset never reported data loss")
	}
	return dl, recs
}

// TestLookbackResetRewinds drops a consumer sitting at the log end out of
// range and requires the default lookback to resume three records back: the
// records are stamped ten seconds apart, so thirty seconds before the last one
// we read is the record three before it.
func TestLookbackResetRewinds(t *testing.T) {
	t.Parallel()

	const want = 6 // oorNrecs-1 is the last record read, 30s back over 10s spacing

	_, cl, fired := consumedPastEnd(t, "ooor-lookback")

	dl, recs := pollReset(t, cl, 8*time.Second)
	if !fired.Load() {
		t.Fatal("the fetch at the cursor was never answered out of range")
	}
	if dl.ResetTo != want {
		t.Fatalf("reset to offset %d, want %d", dl.ResetTo, want)
	}
	if len(recs) == 0 {
		recs = collectRecords(t, cl, 1, 8*time.Second)
	}
	if recs[0].Offset != want {
		t.Fatalf("resumed at offset %d, want %d", recs[0].Offset, want)
	}
}

// TestOutOfRangeResetAtStart requires an AtStart reset offset to resume at the
// log start when the broker lost data at a point we cannot locate.
func TestOutOfRangeResetAtStart(t *testing.T) {
	t.Parallel()

	_, cl, fired := consumedPastEnd(t, "ooor-atstart", kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))

	dl, recs := pollReset(t, cl, 8*time.Second)
	if !fired.Load() {
		t.Fatal("the fetch at the cursor was never answered out of range")
	}
	if dl.ResetTo != 0 {
		t.Fatalf("reset to offset %d, want 0", dl.ResetTo)
	}
	if len(recs) == 0 {
		recs = collectRecords(t, cl, 1, 8*time.Second)
	}
	if recs[0].Offset != 0 {
		t.Fatalf("resumed at offset %d, want 0", recs[0].Offset)
	}
}

// TestOutOfRangeResetAtEnd requires an AtEnd reset offset to resume at the log
// end, skipping whatever the broker lost. The cursor is below the end here, so
// the end is not where we already were. Nothing is left to read at the end, so
// we produce once the reset has landed and require that record.
func TestOutOfRangeResetAtEnd(t *testing.T) {
	t.Parallel()

	const topic = "ooor-atend"

	c, cl, fired := consumedInRange(t, topic, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()))

	dl, _ := pollReset(t, cl, 8*time.Second)
	if !fired.Load() {
		t.Fatal("the fetch at the cursor was never answered out of range")
	}
	if dl.ResetTo != oorNrecs {
		t.Fatalf("reset to offset %d, want %d", dl.ResetTo, oorNrecs)
	}

	// The data loss means the reset resolved, so the end it listed cannot
	// move under it: what we produce now lands at that end.
	produceN(t, c, topic, 3)
	got := collectRecords(t, cl, 1, 8*time.Second)
	if got[0].Offset != oorNrecs {
		t.Fatalf("resumed at offset %d, want %d", got[0].Offset, oorNrecs)
	}
}

// TestOutOfRangeResetRelative requires a relative reset offset to keep its
// relative part. The log end is the offset the cursor could not read, so an
// unadjusted AtEnd would look identical to keeping our offset; three before
// the end is neither, and the records between are still read.
func TestOutOfRangeResetRelative(t *testing.T) {
	t.Parallel()

	const topic = "ooor-relative"

	_, cl, fired := consumedInRange(t, topic, kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd().Relative(-3)))

	dl, _ := pollReset(t, cl, 8*time.Second)
	if !fired.Load() {
		t.Fatal("the fetch at the cursor was never answered out of range")
	}
	want := int64(oorNrecs - 3)
	if dl.ResetTo != want {
		t.Fatalf("reset to offset %d, want %d", dl.ResetTo, want)
	}
	got := collectRecords(t, cl, 1, 8*time.Second)
	if got[0].Offset != want {
		t.Fatalf("resumed at offset %d, want %d", got[0].Offset, want)
	}
}

// TestOutOfRangeUnlocatableDataLoss requires the reset the client cannot
// locate to report where we were through ErrDataLoss.
func TestOutOfRangeUnlocatableDataLoss(t *testing.T) {
	t.Parallel()

	_, cl, fired := consumedPastEnd(t, "ooor-dataloss")

	dl, _ := pollReset(t, cl, 8*time.Second)
	if !fired.Load() {
		t.Fatal("the fetch at the cursor was never answered out of range")
	}
	if dl.ConsumedTo != oorNrecs {
		t.Fatalf("data loss reports consuming to %d, want %d", dl.ConsumedTo, oorNrecs)
	}
	if dl.ResetTo >= dl.ConsumedTo {
		t.Fatalf("data loss reset to %d, want below %d", dl.ResetTo, dl.ConsumedTo)
	}
}

// TestOutOfRangeNoResetErrors requires NoResetOffset to surface the error for
// an out of range seek rather than bounding the offset within the log.
func TestOutOfRangeNoResetErrors(t *testing.T) {
	t.Parallel()

	// A start offset that opts into NoResetOffset carries that into the
	// reset offset, which is what a group consumer using AtCommitted as its
	// start offset relies on.
	for _, test := range []struct {
		name string
		opt  kgo.Opt
	}{
		{"reset offset", kgo.ConsumeResetOffset(kgo.NoResetOffset())},
		{"start offset", kgo.ConsumeStartOffset(kgo.NoResetOffset())},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			const topic = "ooor-noreset"

			c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
			produceN(t, c, topic, oorNrecs)

			cl := newPlainClient(t, c,
				kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().At(500)}}),
				test.opt,
				kgo.DisableFetchSessions(),
				kgo.FetchMaxWait(100*time.Millisecond),
			)

			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			var got error
			for ctx.Err() == nil && got == nil {
				fs := cl.PollFetches(ctx)
				fs.EachError(func(_ string, _ int32, err error) {
					if errors.Is(err, kerr.OffsetOutOfRange) {
						got = err
					}
				})
				if n := len(fs.Records()); n > 0 {
					t.Fatalf("consumed %d records, want none: the seek was bounded rather than kept", n)
				}
			}
			if got == nil {
				t.Fatal("the out of range seek never surfaced an error")
			}
		})
	}
}
