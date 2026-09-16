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
	"github.com/twmb/franz-go/pkg/kversion"
)

// produceSpaced produces n records to the topic, each its own batch, stamped
// apart from each other starting at base.
func produceSpaced(t *testing.T, c *Cluster, topic string, n int, base time.Time, apart time.Duration) {
	t.Helper()
	batches := make([][]int64, n)
	for i := range batches {
		batches[i] = []int64{base.Add(time.Duration(i) * apart).UnixMilli()}
	}
	produceBatches(t, c, topic, batches)
}

// oorAt answers one fetch at the given offset with OFFSET_OUT_OF_RANGE, which
// is what a broker that lost data sends. Every other fetch passes through.
func oorAt(c *Cluster, topic string, at int64) *FaultHandle {
	return c.Fault(Fault{
		Keys:       []kmsg.Key{kmsg.Fetch},
		Topic:      topic,
		Partitions: []int32{0},
		Err:        kerr.OffsetOutOfRange,
		When: func(kreq kmsg.Request) bool {
			for _, rt := range kreq.(*kmsg.FetchRequest).Topics {
				for _, rp := range rt.Partitions {
					if rp.FetchOffset == at {
						return true
					}
				}
			}
			return false
		},
	})
}

// pollUntil polls until a fetch error satisfies match or the timeout
// expires, returning whether one did and every record that arrived.
func pollUntil(cl *kgo.Client, timeout time.Duration, match func(error) bool) (matched bool, recs []*kgo.Record) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for ctx.Err() == nil && !matched {
		fs := cl.PollFetches(ctx)
		fs.EachError(func(_ string, _ int32, err error) { matched = matched || match(err) })
		fs.EachRecord(func(r *kgo.Record) { recs = append(recs, r) })
	}
	return matched, recs
}

// pollDataLoss polls until a fetch reports data loss or the timeout expires,
// returning that loss and every record that arrived.
func pollDataLoss(cl *kgo.Client, timeout time.Duration) (dl *kgo.ErrDataLoss, recs []*kgo.Record) {
	_, recs = pollUntil(cl, timeout, func(err error) bool {
		got, ok := errors.AsType[*kgo.ErrDataLoss](err)
		if ok {
			dl = got
		}
		return ok
	})
	return dl, recs
}

// consumeThenPause polls n records and pauses the topic, which drops whatever
// the poll left buffered. The cursor is then at n carrying record n-1's
// timestamp, and nothing is in flight until the topic is resumed.
func consumeThenPause(t *testing.T, cl *kgo.Client, topic string, n int) {
	t.Helper()
	fs := cl.PollRecords(context.Background(), n)
	if err := fs.Err(); err != nil {
		t.Fatal(err)
	}
	if got := fs.NumRecords(); got != n {
		t.Fatalf("polled %d records, want %d", got, n)
	}
	cl.PauseFetchTopics(topic)
	verifyZeroRecords(t, cl, time.Second)
}

// consumedThenOOR consumes nconsume of the topic's nrecs records and answers
// the next fetch out of range, which is what a broker that lost data looks
// like from here. Consuming fewer than every record leaves the log end above
// the cursor, which is where resuming at the end and resuming where we were
// differ.
func consumedThenOOR(t *testing.T, c *Cluster, topic string, nrecs, nconsume int, opts ...kgo.Opt) (*kgo.Client, *FaultHandle) {
	t.Helper()
	h := oorAt(c, topic, int64(nconsume))
	cl := newPlainClient(t, c, append([]kgo.Opt{
		kgo.ConsumeTopics(topic),
		kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
		kgo.DisableFetchSessions(),
		kgo.FetchMaxWait(100 * time.Millisecond),
	}, opts...)...)
	if nconsume == nrecs {
		consumeN(t, cl, nconsume, 8*time.Second)
	} else {
		consumeThenPause(t, cl, topic, nconsume)
		cl.ResumeFetchTopics(topic)
	}
	return cl, h
}

// TestOutOfRangeBelowStart drops a consumer below the log start and requires
// it to resume at the log start whatever ConsumeResetOffset says. The start is
// exact there: every record that still exists is one we never consumed, so
// there is nothing for a policy to decide, and AtEnd would skip all of it. The
// surviving records are stamped older than the last one consumed - a producer
// with a fast clock, then a normal one - which is what broke the old reset:
// listing by the last consumed millisecond matched nothing, so we listed the
// end and skipped every record still in the log.
func TestOutOfRangeBelowStart(t *testing.T) {
	t.Parallel()

	const (
		topic  = "ooor-below-start"
		nrecs  = 10
		nfirst = 5
		delTo  = 7
	)

	for _, test := range []struct {
		name string
		opt  kgo.Opt
	}{
		{name: "default rewind"},
		{name: "at end", opt: kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd())},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))

			// Every record is its own batch. The first five are
			// stamped an hour ahead of the rest, so a reset by the
			// fifth record's timestamp finds nothing once only the
			// later records remain.
			now := time.Now()
			batches := make([][]int64, nrecs)
			for i := range batches {
				ts := now
				if i < nfirst {
					ts = now.Add(time.Hour)
				}
				batches[i] = []int64{ts.UnixMilli()}
			}
			produceBatches(t, c, topic, batches)

			opts := []kgo.Opt{
				kgo.ConsumeTopics(topic),
				kgo.ConsumeStartOffset(kgo.NewOffset().AtStart()),
				kgo.FetchMaxWait(100 * time.Millisecond),
			}
			if test.opt != nil {
				opts = append(opts, test.opt)
			}
			cl := newPlainClient(t, c, opts...)

			consumeThenPause(t, cl, topic, nfirst)

			// Delete through offset 7: the cursor is now below the
			// log start, and every record that survives is one we
			// have not consumed.
			if err := c.DeleteRecords(topic, 0, delTo); err != nil {
				t.Fatalf("delete records: %v", err)
			}
			cl.ResumeFetchTopics(topic)

			got := consumeN(t, cl, nrecs-delTo, 8*time.Second)
			for i, r := range got {
				if want := int64(delTo + i); r.Offset != want {
					t.Errorf("record %d is offset %d, want %d", i, r.Offset, want)
				}
			}
		})
	}
}

// TestOutOfRangeNothingConsumed seeks a partition we have never consumed past
// the log end and requires ConsumeResetOffset to decide what happens, which is
// what it has always meant there. The default rewinds from the last record
// consumed, and there is none, so it resumes at the log start. A start offset
// set alone does not reach here, so AtEnd resolves to the log start too.
// NoResetOffset surfaces the error rather than resolving,
// whether set as the reset offset or carried from the start offset, which is
// what a group consumer using AtCommitted as its start offset relies on.
func TestOutOfRangeNothingConsumed(t *testing.T) {
	t.Parallel()

	const (
		topic = "ooor-fresh"
		nrecs = 10
	)

	for _, test := range []struct {
		name string
		opt  kgo.Opt
		want int64 // -1: the seek errors rather than resolving
	}{
		{name: "default rewind", want: 0},
		{name: "start offset at end", opt: kgo.ConsumeStartOffset(kgo.NewOffset().AtEnd()), want: 0},
		{name: "reset offset none", opt: kgo.ConsumeResetOffset(kgo.NoResetOffset()), want: -1},
		{name: "start offset none", opt: kgo.ConsumeStartOffset(kgo.NoResetOffset()), want: -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
			// An hour old.
			produceSpaced(t, c, topic, nrecs, time.Now().Add(-time.Hour), time.Second)
			listed := c.Fault(Fault{Keys: []kmsg.Key{kmsg.ListOffsets}, Observe: true, Count: -1})

			opts := []kgo.Opt{
				kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().At(500)}}),
				kgo.DisableFetchSessions(),
				kgo.FetchMaxWait(100 * time.Millisecond),
			}
			if test.opt != nil {
				opts = append(opts, test.opt)
			}
			cl := newPlainClient(t, c, opts...)

			if test.want < 0 {
				errored, recs := pollUntil(cl, 8*time.Second, func(err error) bool { return errors.Is(err, kerr.OffsetOutOfRange) })
				if len(recs) > 0 {
					t.Fatalf("consumed %d records, want none: the seek was resolved rather than kept", len(recs))
				}
				if !errored {
					t.Fatal("the out of range seek never surfaced an error")
				}
				return
			}

			// The reset lists the log before it resumes. Producing
			// after that list lands past the end it saw, so the at
			// end row has a record to read.
			ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
			defer cancel()
			if err := listed.Wait(ctx, 1); err != nil {
				t.Fatal("the reset never listed offsets")
			}
			produceN(t, c, topic, 1)
			if got := consumeN(t, cl, 1, 8*time.Second); got[0].Offset != test.want {
				t.Fatalf("resumed at offset %d, want %d", got[0].Offset, test.want)
			}
		})
	}
}

// TestOutOfRangeReset drops a consumer out of range after it has consumed, at
// a point the client cannot locate, and requires ConsumeResetOffset to decide
// where it resumes. The default rewinds a minute from the last record
// consumed: ten seconds apart that is six records back, two minutes apart it
// stays on the record itself. A LookbackOffset keeps the last thirty seconds
// of the log instead, ahead of where we were if need be.
func TestOutOfRangeReset(t *testing.T) {
	t.Parallel()

	const (
		topic = "ooor-reset"
		nrecs = 10
	)

	for _, test := range []struct {
		name     string
		nconsume int
		apart    time.Duration
		opt      kgo.Opt
		want     int64
	}{
		{name: "rewind", nconsume: nrecs, apart: 10 * time.Second, want: 3},
		{name: "rewind stays on the last record", nconsume: nrecs, apart: 2 * time.Minute, want: nrecs - 1},
		{name: "lookback keeps the tail", nconsume: 4, apart: 10 * time.Second, opt: kgo.ConsumeResetOffset(kgo.LookbackOffset(30 * time.Second)), want: 6},
		{name: "rewind ignores relative", nconsume: nrecs, apart: 10 * time.Second, opt: kgo.ConsumeResetOffset(kgo.RewindOffset(time.Minute).Relative(-1)), want: 3},
		{name: "at start", nconsume: nrecs, apart: 10 * time.Second, opt: kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), want: 0},
		{name: "at end", nconsume: 8, apart: 10 * time.Second, opt: kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()), want: nrecs},
		{name: "relative to the end", nconsume: 8, apart: 10 * time.Second, opt: kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd().Relative(-3)), want: nrecs - 3},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
			produceSpaced(t, c, topic, nrecs, time.Now().Add(-time.Hour), test.apart)

			var opts []kgo.Opt
			if test.opt != nil {
				opts = append(opts, test.opt)
			}
			cl, h := consumedThenOOR(t, c, topic, nrecs, test.nconsume, opts...)

			dl, recs := pollDataLoss(cl, 8*time.Second)
			if h.Hits() == 0 {
				t.Fatal("the fetch at the cursor was never answered out of range")
			}
			if dl == nil {
				t.Fatal("the reset never reported data loss")
			}
			if dl.ConsumedTo != int64(test.nconsume) {
				t.Fatalf("data loss reports consuming to %d, want %d", dl.ConsumedTo, test.nconsume)
			}
			if dl.ResetTo != test.want {
				t.Fatalf("reset to offset %d, want %d", dl.ResetTo, test.want)
			}

			// A reset to the log end has nothing left to read, so
			// we produce once the reset has landed. The data loss
			// means the reset resolved, so the end it listed
			// cannot move under it, and what we produce lands
			// past every other row's answer.
			produceN(t, c, topic, 3)
			if len(recs) == 0 {
				recs = consumeN(t, cl, 1, 8*time.Second)
			}
			if recs[0].Offset != test.want {
				t.Fatalf("resumed at offset %d, want %d", recs[0].Offset, test.want)
			}
		})
	}
}

// TestOutOfRangeInRangeNeverSkipsForward answers one fetch with
// OFFSET_OUT_OF_RANGE while the cursor sits inside an intact log, with the log
// end above it, and requires the reset to resume where the cursor was. A
// by-time answer ahead of us would skip the records in between; no by-time
// answer at all used to take the log end and skip the rest of the log.
// Everything from our offset on is unread. The log did shrink under us at
// some point, so the reset still reports the loss, from and to our offset.
func TestOutOfRangeInRangeNeverSkipsForward(t *testing.T) {
	t.Parallel()

	const (
		topic    = "ooor-in-range"
		nrecs    = 12
		nconsume = 10
	)

	for _, test := range []struct {
		name   string
		byTime int64
		want   int64
	}{
		{name: "ahead", byTime: 11, want: nconsume},
		{name: "none", byTime: -1, want: nconsume},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
			epoch := c.PartitionInfo(topic, 0).Epoch

			// Timestamps one second apart, so the last consumed
			// record is the only one at its timestamp.
			produceSpaced(t, c, topic, nrecs, time.Now().Add(-time.Hour), time.Second)

			// The reset lists the start, the end, and the last
			// consumed millisecond. We answer the by-time list; the
			// start (0) and the end (12) are served for real.
			var listed atomic.Bool
			c.ControlKey(int16(kmsg.ListOffsets), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
				c.KeepControl()
				req := kreq.(*kmsg.ListOffsetsRequest)
				var byTime bool
				for _, rt := range req.Topics {
					for _, rp := range rt.Partitions {
						byTime = byTime || rp.Timestamp != -1 && rp.Timestamp != -2
					}
				}
				if !byTime {
					return nil, nil, false
				}
				listed.Store(true)
				resp := req.ResponseKind().(*kmsg.ListOffsetsResponse)
				for _, rt := range req.Topics {
					st := kmsg.NewListOffsetsResponseTopic()
					st.Topic = rt.Topic
					for _, rp := range rt.Partitions {
						sp := kmsg.NewListOffsetsResponseTopicPartition()
						sp.Partition = rp.Partition
						sp.ErrorCode = 0
						sp.Timestamp = -1
						sp.Offset = test.byTime
						sp.LeaderEpoch = epoch
						st.Partitions = append(st.Partitions, sp)
					}
					resp.Topics = append(resp.Topics, st)
				}
				return resp, nil, true
			})

			// The default reset lists by time, which is what this
			// test injects an answer for. AtStart would resume at
			// the log start and never list by time at all.
			cl, h := consumedThenOOR(t, c, topic, nrecs, nconsume)

			dl, recs := pollDataLoss(cl, 8*time.Second)
			if h.Hits() == 0 {
				t.Fatal("the fetch at the cursor was never answered out of range")
			}
			if !listed.Load() {
				t.Fatal("the reset never listed by the last consumed millisecond")
			}
			if dl == nil || dl.ConsumedTo != nconsume || dl.ResetTo != test.want {
				t.Fatalf("data loss %v, want consumed to %d and reset to %d", dl, nconsume, test.want)
			}
			if len(recs) == 0 {
				recs = consumeN(t, cl, 1, 8*time.Second)
			}
			if recs[0].Offset != test.want {
				t.Fatalf("resumed at offset %d, want %d", recs[0].Offset, test.want)
			}
		})
	}
}

// TestLookbackStartOffset stamps ten records a minute apart across the last
// ten minutes and consumes with a five minute LookbackOffset. The client lists
// the newest record's timestamp and starts at the first record five minutes
// before it, record 4. Set only as the reset offset, the lookback becomes the
// start offset too. Below ListOffsets v7 the client looks back from the
// current time instead, which a moment after producing falls between records
// 4 and 5.
func TestLookbackStartOffset(t *testing.T) {
	t.Parallel()

	const (
		topic = "lookback-start"
		nrecs = 10
	)

	v6 := kversion.Stable()
	v6.SetMaxKeyVersion(int16(kmsg.ListOffsets), 6)

	for _, test := range []struct {
		name string
		opts []kgo.Opt
		want int
	}{
		{name: "start offset", opts: []kgo.Opt{kgo.ConsumeStartOffset(kgo.LookbackOffset(5 * time.Minute))}, want: 4},
		{name: "reset offset alone", opts: []kgo.Opt{kgo.ConsumeResetOffset(kgo.LookbackOffset(5 * time.Minute))}, want: 4},
		{name: "relative and epoch ignored", opts: []kgo.Opt{kgo.ConsumeStartOffset(kgo.LookbackOffset(5 * time.Minute).Relative(-2).WithEpoch(3))}, want: 4},
		{name: "below ListOffsets v7", opts: []kgo.Opt{kgo.ConsumeStartOffset(kgo.LookbackOffset(5 * time.Minute)), kgo.MaxVersions(v6)}, want: 5},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
			produceSpaced(t, c, topic, nrecs, time.Now().Add(-10*time.Minute+30*time.Second), time.Minute)

			cl := newPlainClient(t, c, append([]kgo.Opt{kgo.ConsumeTopics(topic), kgo.FetchMaxWait(100 * time.Millisecond)}, test.opts...)...)
			got := consumeN(t, cl, nrecs-test.want, 8*time.Second)
			for i, r := range got {
				if w := int64(test.want + i); r.Offset != w {
					t.Errorf("record %d is offset %d, want %d", i, r.Offset, w)
				}
			}
		})
	}
}
