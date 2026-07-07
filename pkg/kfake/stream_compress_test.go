package kfake

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// These tests exercise kgo.StreamingCompression end to end. kfake validates
// every produced batch's CRC, batch length, magic, compression type, and
// idempotent sequences, and the consume side decompresses and re-parses every
// record, so passing runs prove the merged wire bytes are well formed and
// sequence-correct, not merely accepted.

type streamCodec struct {
	name       string
	c          kgo.CompressionCodec
	streamable bool
}

func streamCodecs() []streamCodec {
	return []streamCodec{
		{"gzip", kgo.GzipCompression(), true},
		{"zstd", kgo.ZstdCompression(), true},
		{"lz4", kgo.Lz4Compression(), true},
		{"snappy", kgo.SnappyCompression(), false}, // one-shot block codec: option is a no-op
		{"none", kgo.NoCompression(), false},       // option is a no-op
	}
}

// streamWorkload deterministically generates n records with mixed sizes,
// headers, and nil values, ~5x compressible.
func streamWorkload(n int) (recs []*kgo.Record, expected map[string][]byte) {
	rng := rand.New(rand.NewSource(2))
	base := mixValue(2048, 0.15)
	expected = make(map[string][]byte, n)
	recs = make([]*kgo.Record, n)
	for i := 0; i < n; i++ {
		key := fmt.Sprintf("k-%06d", i)
		var val []byte
		switch i % 5 {
		case 0:
			val = base[:16]
		case 1:
			val = base[:512]
		case 2:
			val = base[:2048]
		case 3:
			val = base[rng.Intn(1024) : 1024+rng.Intn(1024)]
		case 4:
			val = nil
		}
		expected[key] = val
		r := &kgo.Record{Key: []byte(key), Value: val}
		if i%7 == 0 {
			r.Headers = []kgo.RecordHeader{{Key: "h1", Value: []byte("v1")}, {Key: "h2", Value: base[:32]}}
		}
		recs[i] = r
	}
	return recs, expected
}

// consumeExactly consumes from topic until exactly n records are seen,
// verifying values byte-for-byte against expected and that no extra records
// arrive. Returns the number of records that carried headers.
func consumeExactly(t *testing.T, addrs []string, topic string, n int, expected map[string][]byte) int {
	t.Helper()
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(addrs...),
		kgo.ConsumeTopics(topic),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	got := make(map[string][]byte, n)
	total, headered := 0, 0
	perPart := map[int32][2]int64{} // partition -> {count, maxOffset}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	for total < n {
		fs := consumer.PollFetches(ctx)
		if err := fs.Err0(); err != nil {
			// Diagnostic: where is each partition vs the broker's end offset?
			req := kmsg.NewPtrListOffsetsRequest()
			rt := kmsg.NewListOffsetsRequestTopic()
			rt.Topic = topic
			for p := int32(0); p < 4; p++ {
				rp := kmsg.NewListOffsetsRequestTopicPartition()
				rp.Partition = p
				rp.Timestamp = -1
				rt.Partitions = append(rt.Partitions, rp)
			}
			req.Topics = append(req.Topics, rt)
			for p, st := range perPart {
				t.Logf("DIAG p=%d consumed=%d maxOffsetSeen=%d", p, st[0], st[1])
			}
			if kresp, kerr2 := consumer.Request(context.Background(), req); kerr2 != nil {
				t.Logf("DIAG listoffsets request error: %v", kerr2)
			} else {
				for _, rt := range kresp.(*kmsg.ListOffsetsResponse).Topics {
					for _, rp := range rt.Partitions {
						t.Logf("DIAG p=%d brokerEndOffset=%d err=%d", rp.Partition, rp.Offset, rp.ErrorCode)
						st, seen := perPart[rp.Partition]
						if rp.ErrorCode != 0 || rp.Offset == 0 || (seen && st[1]+1 >= rp.Offset) {
							continue
						}
						// Stuck: raw-fetch at the next unconsumed offset and
						// dump what the broker serves there.
						at := int64(0)
						if seen {
							at = st[1] + 1
						}
						freq := kmsg.NewPtrFetchRequest()
						freq.MaxWaitMillis = 100
						freq.MaxBytes = 50 << 20
						ft := kmsg.NewFetchRequestTopic()
						ft.Topic = topic
						fpr := kmsg.NewFetchRequestTopicPartition()
						fpr.Partition = rp.Partition
						fpr.FetchOffset = at
						fpr.PartitionMaxBytes = 1 << 20
						ft.Partitions = append(ft.Partitions, fpr)
						freq.Topics = append(freq.Topics, ft)
						fresp, ferr := consumer.Request(context.Background(), freq)
						if ferr != nil {
							t.Logf("DIAG p=%d rawfetch@%d error: %v", rp.Partition, at, ferr)
							continue
						}
						for _, frt := range fresp.(*kmsg.FetchResponse).Topics {
							for _, frp := range frt.Partitions {
								raw := frp.RecordBatches
								t.Logf("DIAG p=%d rawfetch@%d err=%d hwm=%d rawbytes=%d", frp.Partition, at, frp.ErrorCode, frp.HighWatermark, len(raw))
								for len(raw) > 12 {
									var b kmsg.RecordBatch
									if err := b.ReadFrom(raw); err != nil {
										t.Logf("DIAG   batch parse err after header: %v (rem=%d)", err, len(raw))
										break
									}
									whole := 12 + int(b.Length)
									trunc := whole > len(raw)
									t.Logf("DIAG   batch first=%d nrec=%d lod=%d len=%d attrs=%x truncated=%v", b.FirstOffset, b.NumRecords, b.LastOffsetDelta, b.Length, b.Attributes, trunc)
									if trunc {
										break
									}
									raw = raw[whole:]
								}
							}
						}
					}
				}
			}
			t.Fatalf("poll error with %d/%d consumed: %v", total, n, err)
		}
		fs.EachRecord(func(r *kgo.Record) {
			total++
			got[string(r.Key)] = r.Value
			if len(r.Headers) > 0 {
				headered++
			}
			st := perPart[r.Partition]
			st[0]++
			if r.Offset > st[1] {
				st[1] = r.Offset
			}
			perPart[r.Partition] = st
		})
	}
	// One more short poll: nothing else should arrive (no duplicates).
	extraCtx, extraCancel := context.WithTimeout(context.Background(), 400*time.Millisecond)
	defer extraCancel()
	extra := 0
	consumer.PollFetches(extraCtx).EachRecord(func(*kgo.Record) { extra++ })
	if extra != 0 {
		t.Fatalf("consumed %d duplicate/extra records after the expected %d", extra, n)
	}
	if len(got) != n {
		t.Fatalf("consumed %d records but only %d unique keys (duplicates?)", total, len(got))
	}
	for k, want := range expected {
		gotv, ok := got[k]
		if !ok {
			t.Fatalf("missing key %s", k)
		}
		if !bytes.Equal(gotv, want) {
			t.Fatalf("key %s: value mismatch (got %d bytes, want %d)", k, len(gotv), len(want))
		}
	}
	return headered
}

// TestStreamCompressRoundTrip produces with and without StreamingCompression
// across all codecs, verifies every record byte-for-byte, and asserts the
// density win: with manual flushing (so batches roll purely on the size
// bound), streaming codecs must pack materially more records per produce
// request than the same codec without the option.
func TestStreamCompressRoundTrip(t *testing.T) {
	t.Parallel()

	var results sync.Map // "codec/mode" -> rec/req

	t.Run("matrix", func(t *testing.T) {
		for _, codec := range streamCodecs() {
			for _, stream := range []bool{false, true} {
				mode := "legacy"
				if stream {
					mode = "stream"
				}
				t.Run(codec.name+"/"+mode, func(t *testing.T) {
					t.Parallel()
					c, err := NewCluster(NumBrokers(1), SeedTopics(2, "rt"))
					if err != nil {
						t.Fatal(err)
					}
					defer c.Close()

					var produceReqs atomic.Int64
					opts := []kgo.Opt{
						kgo.SeedBrokers(c.ListenAddrs()...),
						kgo.DefaultProduceTopic("rt"),
						kgo.ProducerBatchCompression(codec.c),
						kgo.MaxBufferedRecords(200_000),
						// Manual flushing so batches roll purely on the size
						// bound rather than drain cadence.
						kgo.ManualFlushing(),
						kgo.WithHooks(produceReqCounter{&produceReqs}),
					}
					if stream {
						opts = append(opts, kgo.StreamingCompression())
					}
					cl, err := kgo.NewClient(opts...)
					if err != nil {
						t.Fatal(err)
					}
					defer cl.Close()

					const n = 40_000
					recs, expected := streamWorkload(n)
					var wg sync.WaitGroup
					wg.Add(n)
					var failed atomic.Int64
					for _, r := range recs {
						cl.Produce(context.Background(), r, func(_ *kgo.Record, err error) {
							if err != nil {
								failed.Add(1)
							}
							wg.Done()
						})
					}
					if err := cl.Flush(context.Background()); err != nil {
						t.Fatalf("flush: %v", err)
					}
					wg.Wait()
					if f := failed.Load(); f > 0 {
						t.Fatalf("%d produces failed", f)
					}

					recPerReq := float64(n) / float64(produceReqs.Load())
					results.Store(codec.name+"/"+mode, recPerReq)
					t.Logf("%s/%s: %d records in %d produce requests (%.0f rec/req)", codec.name, mode, n, produceReqs.Load(), recPerReq)

					headered := consumeExactly(t, c.ListenAddrs(), "rt", n, expected)
					if want := n / 7; headered < want {
						t.Errorf("saw %d headered records, want >= %d", headered, want)
					}
				})
			}
		}
	})

	loadf := func(name string) float64 {
		v, ok := results.Load(name)
		if !ok {
			t.Fatalf("missing result for %s", name)
		}
		return v.(float64)
	}
	for _, codec := range streamCodecs() {
		legacy, stream := loadf(codec.name+"/legacy"), loadf(codec.name+"/stream")
		if codec.streamable {
			if stream < 2.5*legacy {
				t.Errorf("%s: streaming packed %.0f rec/req, want >= 2.5x legacy's %.0f", codec.name, stream, legacy)
			}
		} else if stream < 0.7*legacy || stream > 1.5*legacy {
			t.Errorf("%s: option should be a no-op, got stream %.0f vs legacy %.0f rec/req", codec.name, stream, legacy)
		}
	}
}

// TestStreamCompressRetry fails the first produce request with a retriable
// error AFTER merged batches were formed (manual flushing builds the backlog,
// so the flush drain merges before the first send). The retry must resend the
// merged batches verbatim: kfake's idempotent sequence validation rejects any
// gap or reorder, and consuming verifies exactly-once delivery.
func TestStreamCompressRetry(t *testing.T) {
	t.Parallel()
	c, err := NewCluster(NumBrokers(1), SeedTopics(2, "rt"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var failed atomic.Int64
	c.ControlKey(0, func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		// Single-shot: fail every partition of the first produce request.
		failed.Add(1)
		req := kreq.(*kmsg.ProduceRequest)
		resp := req.ResponseKind().(*kmsg.ProduceResponse)
		for _, t := range req.Topics {
			st := kmsg.NewProduceResponseTopic()
			st.Topic = t.Topic
			st.TopicID = t.TopicID
			for _, p := range t.Partitions {
				sp := kmsg.NewProduceResponseTopicPartition()
				sp.Partition = p.Partition
				sp.ErrorCode = kerr.NotLeaderForPartition.Code
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic("rt"),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
		kgo.StreamingCompression(),
		kgo.MaxBufferedRecords(200_000),
		kgo.ManualFlushing(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	const n = 20_000
	recs, expected := streamWorkload(n)
	var wg sync.WaitGroup
	wg.Add(n)
	var perr atomic.Int64
	for _, r := range recs {
		cl.Produce(context.Background(), r, func(_ *kgo.Record, err error) {
			if err != nil {
				perr.Add(1)
			}
			wg.Done()
		})
	}
	if err := cl.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	wg.Wait()
	if e := perr.Load(); e > 0 {
		t.Fatalf("%d produces failed", e)
	}
	if failed.Load() == 0 {
		t.Fatal("control never fired; retry path not exercised")
	}
	consumeExactly(t, c.ListenAddrs(), "rt", n, expected)
}

// slowProduce installs a persistent control that delays every produce request,
// so a backlog (and merging) builds while requests are in flight.
func slowProduce(c *Cluster, d time.Duration) {
	c.ControlKey(0, func(kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		c.SleepControl(func() { time.Sleep(d) })
		return nil, nil, false
	})
}

// TestStreamCompressPurge purges the topic while merged and merging batches
// are buffered and in flight against a slow broker: every produce promise must
// fire exactly once (success or purge error), with no hang and no double
// promise (the waitgroup would panic on a double Done).
func TestStreamCompressPurge(t *testing.T) {
	t.Parallel()
	c, err := NewCluster(NumBrokers(1), SeedTopics(2, "rt"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	slowProduce(c, 15*time.Millisecond)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic("rt"),
		kgo.ProducerBatchCompression(kgo.GzipCompression()),
		kgo.StreamingCompression(),
		kgo.MaxBufferedRecords(200_000),
		kgo.ProducerLinger(time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	const n = 10_000
	recs, _ := streamWorkload(n)
	var wg sync.WaitGroup
	wg.Add(n)
	var oks, errs atomic.Int64
	for _, r := range recs {
		cl.Produce(context.Background(), r, func(_ *kgo.Record, err error) {
			if err != nil {
				errs.Add(1)
			} else {
				oks.Add(1)
			}
			wg.Done()
		})
	}
	time.Sleep(60 * time.Millisecond) // let some requests fly and merges happen
	cl.PurgeTopicsFromClient("rt")

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("hang: %d/%d promises fired (ok=%d err=%d)", oks.Load()+errs.Load(), n, oks.Load(), errs.Load())
	}
	if got := oks.Load() + errs.Load(); got != n {
		t.Fatalf("promises fired %d times, want %d", got, n)
	}
	t.Logf("purge: %d delivered, %d failed (all promised exactly once)", oks.Load(), errs.Load())
}

// TestStreamCompressClose closes the client while merged and merging batches
// are buffered and in flight against a slow broker: every promise must fire
// exactly once and Close must return.
func TestStreamCompressClose(t *testing.T) {
	t.Parallel()
	c, err := NewCluster(NumBrokers(1), SeedTopics(2, "rt"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	slowProduce(c, 15*time.Millisecond)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic("rt"),
		kgo.ProducerBatchCompression(kgo.ZstdCompression()),
		kgo.StreamingCompression(),
		kgo.MaxBufferedRecords(200_000),
		kgo.ProducerLinger(time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}

	const n = 10_000
	recs, _ := streamWorkload(n)
	var wg sync.WaitGroup
	wg.Add(n)
	var oks, errs atomic.Int64
	var badErr atomic.Value
	for _, r := range recs {
		cl.Produce(context.Background(), r, func(_ *kgo.Record, err error) {
			switch {
			case err == nil:
				oks.Add(1)
			case errors.Is(err, kgo.ErrClientClosed) || strings.Contains(err.Error(), "closed"):
				errs.Add(1)
			default:
				errs.Add(1)
				badErr.Store(err)
			}
			wg.Done()
		})
	}
	time.Sleep(60 * time.Millisecond)

	closed := make(chan struct{})
	go func() { cl.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(30 * time.Second):
		t.Fatal("Close hung")
	}

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatalf("hang: %d/%d promises fired", oks.Load()+errs.Load(), n)
	}
	if got := oks.Load() + errs.Load(); got != n {
		t.Fatalf("promises fired %d times, want %d", got, n)
	}
	if be := badErr.Load(); be != nil {
		t.Logf("note: some records failed with %v (accepted: close-time failures vary)", be)
	}
	t.Logf("close: %d delivered, %d failed (all promised exactly once)", oks.Load(), errs.Load())
}

// TestStreamCompressLiveDrain exercises merging in the LIVE drain path (no
// manual flushing): a slow broker builds a backlog while linger paces
// admission, merges happen between requests, and everything must arrive
// exactly once, byte for byte.
func TestStreamCompressLiveDrain(t *testing.T) {
	t.Parallel()
	c, err := NewCluster(NumBrokers(1), SeedTopics(2, "rt"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	slowProduce(c, 5*time.Millisecond)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic("rt"),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		kgo.StreamingCompression(),
		kgo.MaxBufferedRecords(200_000),
		kgo.ProducerLinger(time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	const n = 20_000
	recs, expected := streamWorkload(n)
	var wg sync.WaitGroup
	wg.Add(n)
	var perr atomic.Int64
	for _, r := range recs {
		cl.Produce(context.Background(), r, func(_ *kgo.Record, err error) {
			if err != nil {
				perr.Add(1)
			}
			wg.Done()
		})
	}
	if err := cl.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
	wg.Wait()
	if e := perr.Load(); e > 0 {
		t.Fatalf("%d produces failed", e)
	}
	consumeExactly(t, c.ListenAddrs(), "rt", n, expected)
}
