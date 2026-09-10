package kfake

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestOutOfRangeBelowStartResetsToStart drops a consumer below the log start
// and requires it to resume at the log start. The surviving records are
// stamped older than the last one consumed - a producer with a fast clock,
// then a normal one - which is what broke the old reset: listing by the last
// consumed millisecond matched nothing, so we listed the end and skipped
// every record still in the log.
func TestOutOfRangeBelowStartResetsToStart(t *testing.T) {
	t.Parallel()

	const (
		topic  = "ooor-below-start"
		nrecs  = 10
		nfirst = 5
		delTo  = 7
	)

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Every record is its own batch. The first five are stamped an hour
	// ahead of the rest, so a reset by the fifth record's timestamp finds
	// nothing once only the later records remain.
	pcl := newPlainClient(t, c)
	ahead := time.Now().Add(time.Hour)
	now := time.Now()
	for i := range nrecs {
		r := &kgo.Record{Topic: topic, Value: fmt.Appendf(nil, "v%d", i), Timestamp: ahead}
		if i >= nfirst {
			r.Timestamp = now
		}
		if err := pcl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(100*time.Millisecond),
	)

	// One poll takes the first five and leaves the rest buffered. The
	// source does not fetch again while it holds a buffer, so nothing is
	// in flight for the rest of the setup.
	fs := cl.PollRecords(ctx, nfirst)
	if err := fs.Err(); err != nil {
		t.Fatal(err)
	}
	if n := fs.NumRecords(); n != nfirst {
		t.Fatalf("polled %d records, want %d", n, nfirst)
	}

	// Pausing drops the buffered remainder, leaving the cursor at offset 5
	// with the fifth record's timestamp.
	cl.PauseFetchTopics(topic)
	verifyZeroRecords(t, cl, time.Second)

	// Delete through offset 7: the cursor is now below the log start, and
	// every record that survives is one we have not consumed.
	req := kmsg.NewPtrDeleteRecordsRequest()
	rt := kmsg.NewDeleteRecordsRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewDeleteRecordsRequestTopicPartition()
	rp.Partition = 0
	rp.Offset = delTo
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, pcl)
	if err != nil {
		t.Fatal(err)
	}
	if code := resp.Topics[0].Partitions[0].ErrorCode; code != 0 {
		t.Fatalf("delete records: %v", kerr.ErrorForCode(code))
	}

	cl.ResumeFetchTopics(topic)

	got := collectRecords(t, cl, nrecs-delTo, 8*time.Second)
	for i, r := range got {
		if want := int64(delTo + i); r.Offset != want {
			t.Errorf("record %d is offset %d, want %d", i, r.Offset, want)
		}
	}
}

// TestOutOfRangePastEndResetsByTime answers one fetch past the end of an
// intact log with OFFSET_OUT_OF_RANGE, as a broker that lost its tail would,
// and requires the consumer to resume at the last record it consumed, found
// by timestamp. Resuming at the log start would re-read the log; resuming at
// the end would skip anything written after the loss.
func TestOutOfRangePastEndResetsByTime(t *testing.T) {
	t.Parallel()

	const (
		topic = "ooor-past-end"
		nrecs = 10
	)

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	ti := c.TopicInfo(topic)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Timestamps one second apart, so the last consumed record is the
	// only one at its timestamp.
	pcl := newPlainClient(t, c)
	base := time.Now().Add(-time.Hour)
	for i := range nrecs {
		r := &kgo.Record{Topic: topic, Value: fmt.Appendf(nil, "v%d", i), Timestamp: base.Add(time.Duration(i) * time.Second)}
		if err := pcl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	// The first fetch at offset 10 is answered out of range with the
	// watermarks unset, which is what Kafka sends. Every other fetch
	// passes through.
	var fired atomic.Bool
	c.ControlKey(int16(kmsg.Fetch), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		req := kreq.(*kmsg.FetchRequest)
		if len(req.Topics) != 1 || len(req.Topics[0].Partitions) != 1 ||
			req.Topics[0].Partitions[0].FetchOffset != nrecs || fired.Swap(true) {
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

	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableFetchSessions(),
		kgo.FetchMaxWait(100*time.Millisecond),
	)
	collectRecords(t, cl, nrecs, 8*time.Second)

	// The reset lists by the last consumed timestamp and finds record 9,
	// which we read a second time.
	got := collectRecords(t, cl, 1, 8*time.Second)
	if got[0].Offset != nrecs-1 {
		t.Fatalf("resumed at offset %d, want %d", got[0].Offset, nrecs-1)
	}
}

// TestOutOfRangeInRangeNeverSkipsForward answers one fetch with
// OFFSET_OUT_OF_RANGE while the cursor sits inside an intact log, with the log
// end above it, and requires the reset to resume where the cursor was. A
// by-time answer ahead of us would skip the records in between; no by-time
// answer at all used to take the log end and skip the rest of the log.
// Everything from our offset on is unread.
func TestOutOfRangeInRangeNeverSkipsForward(t *testing.T) {
	t.Parallel()

	const (
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

			topic := "ooor-in-range-" + test.name

			c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
			ti := c.TopicInfo(topic)
			epoch := c.PartitionInfo(topic, 0).Epoch

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			// Timestamps one second apart, so the last consumed record is
			// the only one at its timestamp.
			pcl := newPlainClient(t, c)
			base := time.Now().Add(-time.Hour)
			for i := range nrecs {
				r := &kgo.Record{Topic: topic, Value: fmt.Appendf(nil, "v%d", i), Timestamp: base.Add(time.Duration(i) * time.Second)}
				if err := pcl.ProduceSync(ctx, r).FirstErr(); err != nil {
					t.Fatal(err)
				}
			}

			// The fetch at offset 10 is answered out of range once, with
			// the watermarks unset, which is what Kafka sends.
			var fired atomic.Bool
			c.ControlKey(int16(kmsg.Fetch), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
				c.KeepControl()
				req := kreq.(*kmsg.FetchRequest)
				if len(req.Topics) != 1 || len(req.Topics[0].Partitions) != 1 ||
					req.Topics[0].Partitions[0].FetchOffset != nconsume || fired.Swap(true) {
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

			// The reset lists the start, the end, and the last consumed
			// millisecond. We answer the by-time list; the start (0) and
			// the end (12) are served for real.
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

			cl := newPlainClient(t, c,
				kgo.ConsumeTopics(topic),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
				kgo.DisableFetchSessions(),
				kgo.FetchMaxWait(100*time.Millisecond),
			)

			// One poll takes ten of the twelve records and leaves the rest
			// buffered. The source does not fetch again while it holds a
			// buffer, so nothing is in flight for the rest of the setup.
			fs := cl.PollRecords(ctx, nconsume)
			if err := fs.Err(); err != nil {
				t.Fatal(err)
			}
			if n := fs.NumRecords(); n != nconsume {
				t.Fatalf("polled %d records, want %d", n, nconsume)
			}

			// Pausing drops the buffered remainder, leaving the cursor at
			// offset 10 with the tenth record's timestamp and the log end
			// at 12.
			cl.PauseFetchTopics(topic)
			verifyZeroRecords(t, cl, time.Second)

			cl.ResumeFetchTopics(topic)

			got := collectRecords(t, cl, 1, 8*time.Second)
			if !fired.Load() {
				t.Fatal("the fetch at the cursor was never answered out of range")
			}
			if !listed.Load() {
				t.Fatal("the reset never listed by the last consumed millisecond")
			}
			if got[0].Offset != test.want {
				t.Fatalf("resumed at offset %d, want %d", got[0].Offset, test.want)
			}
		})
	}
}
