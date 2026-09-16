package kfake

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

// These tests drive kgo.StreamingCompression end to end. kfake validates
// every produced batch's CRC, length, magic, compression type, and
// idempotent sequences, and consuming decompresses and re-parses every
// record, so a passing run proves the merged wire bytes rather than merely
// accepting them.

// mixValue builds a value of the given size whose last randFrac is random
// bytes (0.15 is ~5x compressible, 1.0 is incompressible).
func mixValue(size int, randFrac float64) []byte {
	tmpl := []byte(`{"event":"click","user":"user-000000","session":"sess-0000","page":"/home/index","ts":1700000000000}`)
	v := bytes.Repeat(tmpl, size/len(tmpl)+1)[:size-int(float64(size)*randFrac)]
	r := make([]byte, size-len(v))
	rand.New(rand.NewSource(1)).Read(r)
	return append(v, r...)
}

// streamWorkload returns n records with mixed sizes, headers, and nil
// values, keyed so the consumer can verify them.
func streamWorkload(n int) (recs []*kgo.Record, expected map[string][]byte) {
	base := mixValue(2048, 0.15)
	sizes := []int{16, 512, 2048, 1033, 0} // 0 stays a nil value
	expected = make(map[string][]byte, n)
	for i := range n {
		key := fmt.Sprintf("k-%06d", i)
		var val []byte
		if size := sizes[i%5]; size > 0 {
			val = base[:size]
		}
		expected[key] = val
		r := &kgo.Record{Key: []byte(key), Value: val}
		if i%7 == 0 {
			r.Headers = []kgo.RecordHeader{{Key: "h1", Value: []byte("v1")}, {Key: "h2", Value: base[:32]}}
		}
		recs = append(recs, r)
	}
	return recs, expected
}

// produceCounter counts produce requests, the batches written in them, and
// how many of those batches were merged: only a merged batch can hold more
// uncompressed bytes than ProducerBatchMaxBytes allows.
type produceCounter struct{ reqs, batches, merged atomic.Int64 }

func (c *produceCounter) OnBrokerWrite(_ kgo.BrokerMetadata, key int16, _ int, _, _ time.Duration, _ error) {
	if key == 0 {
		c.reqs.Add(1)
	}
}

func (c *produceCounter) OnProduceBatchWritten(_ kgo.BrokerMetadata, _ string, _ int32, m kgo.ProduceBatchMetrics) {
	c.batches.Add(1)
	if m.UncompressedBytes > 1_000_012 { // the default ProducerBatchMaxBytes
		c.merged.Add(1)
	}
}

// slowProduce delays every produce request so a backlog builds while
// requests are in flight.
func slowProduce(d time.Duration) func(*Cluster) {
	return func(c *Cluster) {
		c.ControlKey(0, func(kmsg.Request) (kmsg.Response, error, bool) {
			c.KeepControl()
			c.SleepControl(func() { time.Sleep(d) })
			return nil, nil, false
		})
	}
}

type streamCase struct {
	codec    kgo.CompressionCodec
	stream   bool
	linger   time.Duration     // zero means manual flushing: batches roll on size alone
	control  func(*Cluster)    // installed once the producer has seen a response
	stop     func(*kgo.Client) // purge or close mid produce; only promise accounting is checked
	opts     []kgo.Opt         // extra client options
	cluster  []Opt             // extra cluster options
	workload func(int) ([]*kgo.Record, map[string][]byte)
}

// runStreamCase produces 20k ~5x compressible records to a two partition
// topic, waits for every promise to fire exactly once, and unless the case
// stops the client early, consumes everything back byte for byte. It
// returns the records per batch written and whether any batch was merged.
func runStreamCase(t *testing.T, tc streamCase) (float64, bool) {
	t.Helper()
	c := newCluster(t, append([]Opt{SeedTopics(2, "t")}, tc.cluster...)...)
	var counts produceCounter
	opts := []kgo.Opt{
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic("t"),
		kgo.ProducerBatchCompression(tc.codec),
		kgo.MaxBufferedRecords(200_000),
		kgo.WithHooks(&counts),
	}
	if tc.linger > 0 {
		opts = append(opts, kgo.ProducerLinger(tc.linger))
	} else {
		opts = append(opts, kgo.ManualFlushing())
	}
	if tc.stream {
		opts = append(opts, kgo.StreamingCompression())
	}
	opts = append(opts, tc.opts...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	if tc.stop == nil {
		t.Cleanup(cl.Close)
	}

	var wg sync.WaitGroup
	var failed atomic.Int64
	produce := func(rs ...*kgo.Record) {
		for _, r := range rs {
			wg.Add(1)
			cl.Produce(context.Background(), r, func(_ *kgo.Record, err error) {
				if err != nil {
					failed.Add(1)
				}
				wg.Done()
			})
		}
	}
	flush := func() {
		if err := cl.Flush(context.Background()); err != nil {
			t.Fatalf("flush: %v", err)
		}
		wg.Wait()
	}

	// Merging needs the produce version, which a sink learns from its
	// first response: warm up with one record before counting batches.
	produce(&kgo.Record{Value: []byte("warmup")})
	flush()
	counts.batches.Store(0)
	if tc.control != nil {
		tc.control(c)
	}

	const n = 20_000
	workload := tc.workload
	if workload == nil {
		workload = streamWorkload
	}
	recs, expected := workload(n)
	produce(recs...)
	if tc.stop != nil {
		time.Sleep(60 * time.Millisecond) // let requests fly and merges happen
		tc.stop(cl)
		done := make(chan struct{})
		go func() { wg.Wait(); close(done) }()
		waitCh(t, done, "not every promise fired")
		return 0, false
	}
	flush()
	if f := failed.Load(); f > 0 {
		t.Fatalf("%d produces failed", f)
	}

	consumer := newPlainClient(t, c, kgo.ConsumeTopics("t"), kgo.FetchMaxWait(250*time.Millisecond))
	got := consumeN(t, consumer, n+1, 60*time.Second)
	verifyZeroRecords(t, consumer, 400*time.Millisecond)
	last := map[int32]int{} // keys are produced in index order and must arrive so within a partition
	for _, r := range got {
		if len(r.Key) == 0 {
			continue // the warmup record
		}
		if want, ok := expected[string(r.Key)]; !ok || !bytes.Equal(r.Value, want) {
			t.Fatalf("key %q: unexpected or mismatched value (%d bytes)", r.Key, len(r.Value))
		}
		var idx int
		fmt.Sscanf(string(r.Key), "k-%d", &idx)
		if idx < last[r.Partition] {
			t.Fatalf("key %q arrived after index %d on partition %d", r.Key, last[r.Partition], r.Partition)
		}
		last[r.Partition] = idx
		delete(expected, string(r.Key))
	}
	if len(expected) > 0 {
		t.Fatalf("%d records never consumed", len(expected))
	}
	return float64(n) / float64(counts.batches.Load()), counts.merged.Load() > 0
}

func TestStreamingCompression(t *testing.T) {
	t.Parallel()
	for _, codec := range []struct {
		name    string
		c       kgo.CompressionCodec
		streams bool
	}{
		{"gzip", kgo.GzipCompression(), true},
		{"zstd", kgo.ZstdCompression(), true},
		{"lz4", kgo.Lz4Compression(), true},
		{"snappy", kgo.SnappyCompression(), true},
		{"none", kgo.NoCompression(), false},
	} {
		t.Run(codec.name, func(t *testing.T) {
			t.Parallel()
			legacy, _ := runStreamCase(t, streamCase{codec: codec.c})
			stream, merged := runStreamCase(t, streamCase{codec: codec.c, stream: true})
			t.Logf("%.0f rec/batch legacy, %.0f rec/batch streaming", legacy, stream)
			if merged != codec.streams {
				t.Errorf("merged batches written: %v, want %v", merged, codec.streams)
			}
			if codec.streams && stream < 2.5*legacy {
				t.Errorf("streaming packed %.0f rec/batch, want at least 2.5x legacy's %.0f", stream, legacy)
			}
		})
	}
	for _, tc := range []struct {
		name string
		streamCase
	}{
		{"retry", streamCase{codec: kgo.ZstdCompression(), stream: true, control: func(c *Cluster) {
			// Fail the next produce request, which carries the first merged batches: the retry resends them verbatim.
			c.Fault(Fault{Keys: []kmsg.Key{kmsg.Produce}, Err: kerr.NotLeaderForPartition})
		}}},
		{"live_drain", streamCase{codec: kgo.Lz4Compression(), stream: true, linger: time.Millisecond, control: slowProduce(5 * time.Millisecond)}},
		{"purge", streamCase{codec: kgo.GzipCompression(), stream: true, linger: time.Millisecond, control: slowProduce(15 * time.Millisecond), stop: func(cl *kgo.Client) { cl.PurgeTopicsFromClient("t") }}},
		{"close", streamCase{codec: kgo.ZstdCompression(), stream: true, linger: time.Millisecond, control: slowProduce(15 * time.Millisecond), stop: (*kgo.Client).Close}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			runStreamCase(t, tc.streamCase)
		})
	}
}

// randomWorkload returns n records of random size up to 8KB with random
// keys, zero to three headers, and timestamps spread over a week and out
// of order, so merges cut at every kind of record and every varint width.
func randomWorkload(n int) (recs []*kgo.Record, expected map[string][]byte) {
	rng := rand.New(rand.NewSource(11))
	base := time.Now().Add(-4 * 24 * time.Hour)
	expected = make(map[string][]byte, n)
	for i := range n {
		key := fmt.Sprintf("k-%06d", i)
		val := make([]byte, rng.Intn(8<<10))
		if rng.Intn(2) == 0 {
			rng.Read(val)
		} else {
			copy(val, bytes.Repeat([]byte("abcdefgh"), len(val)/8+1))
		}
		expected[key] = val
		r := &kgo.Record{Key: []byte(key), Value: val, Timestamp: base.Add(time.Duration(rng.Int63n(int64(7 * 24 * time.Hour))))}
		for range rng.Intn(4) {
			r.Headers = append(r.Headers, kgo.RecordHeader{Key: fmt.Sprint(rng.Intn(100)), Value: val[:min(len(val), rng.Intn(64))]})
		}
		recs = append(recs, r)
	}
	return recs, expected
}

// batchLimit fails the test if any batch written exceeds the batch limit
// once compressed.
type batchLimit struct {
	t     *testing.T
	limit int
}

func (b batchLimit) OnProduceBatchWritten(_ kgo.BrokerMetadata, _ string, _ int32, m kgo.ProduceBatchMetrics) {
	if m.CompressedBytes+65 > b.limit { // the v2 batch header and length prefix
		b.t.Errorf("batch of %d compressed bytes exceeds the %d byte limit", m.CompressedBytes, b.limit)
	}
}

// TestStreamingCompressionRandom drives every codec with the random
// workload under a small batch limit, so merges cut often, and checks every
// batch written stays within the limit compressed.
func TestStreamingCompressionRandom(t *testing.T) {
	t.Parallel()
	for _, codec := range []struct {
		name string
		c    kgo.CompressionCodec
	}{
		{"gzip", kgo.GzipCompression()},
		{"snappy", kgo.SnappyCompression()},
		{"lz4", kgo.Lz4Compression()},
		{"zstd", kgo.ZstdCompression()},
	} {
		t.Run(codec.name, func(t *testing.T) {
			t.Parallel()
			const limit = 64 << 10
			runStreamCase(t, streamCase{
				codec:    codec.c,
				stream:   true,
				workload: randomWorkload,
				opts:     []kgo.Opt{kgo.ProducerBatchMaxBytes(limit), kgo.WithHooks(batchLimit{t, limit})},
			})
		})
	}
}

// A broker too old for the configured codec (produce below v7 cannot carry
// zstd) gets no merged batches: the client sends uncompressed, as it does
// without the option.
func TestStreamingCompressionOldBroker(t *testing.T) {
	t.Parallel()
	_, merged := runStreamCase(t, streamCase{
		codec:   kgo.ZstdCompression(),
		stream:  true,
		cluster: []Opt{MaxVersions(kversion.V2_0_0())},
	})
	if merged {
		t.Fatal("merged batches were written to a broker whose produce version cannot carry the codec")
	}
}

// proseWorkload returns n records of 2KB of word salad: compressible, so a
// merge spans several MB of it, but slow to compress at a high level.
func proseWorkload(n int) (recs []*kgo.Record, expected map[string][]byte) {
	rng := rand.New(rand.NewSource(5))
	words := bytes.Fields([]byte("the quick brown fox jumps over the lazy dog while brokers replicate partitions across racks and consumers commit offsets to the coordinator every few seconds with varying latency"))
	for i := range n {
		var val []byte
		for len(val) < 2048 {
			val = append(append(val, words[rng.Intn(len(words))]...), ' ')
			if rng.Intn(9) == 0 {
				val = fmt.Appendf(val, "%d ", rng.Intn(1e6))
			}
		}
		recs = append(recs, &kgo.Record{Key: fmt.Appendf(nil, "k-%06d", i), Value: val[:2048]})
	}
	return recs, nil
}

// TestStreamingCompressionMigration moves the partition to the other
// broker while its backlog is being merged. The sink it moves to drains
// once on arrival, skips the partition because a merge is running, and
// then has nothing to wake it, so unless the merge itself triggers a drain
// on the new sink, the merged batch and everything behind it sit forever.
// Nothing here calls Flush, which would mask that; the promises must fire
// on their own. Prose at the slowest zstd level keeps each merge around
// half a second, so the move lands inside one.
func TestStreamingCompressionMigration(t *testing.T) {
	t.Parallel()
	c := newCluster(t, NumBrokers(2), SeedTopics(1, "t"))
	cl := newPlainClient(t, c,
		kgo.DefaultProduceTopic("t"),
		kgo.ProducerBatchCompression(kgo.ZstdCompression().WithLevel(4)), // zstd.SpeedBestCompression
		kgo.StreamingCompression(),
		kgo.MaxBufferedRecords(200_000),
	)
	produceSync(t, cl, &kgo.Record{Value: []byte("warmup")}) // merging needs the produce version

	const n = 4000
	recs, _ := proseWorkload(n)
	var wg sync.WaitGroup
	var failed atomic.Int64
	wg.Add(n)
	for _, r := range recs {
		cl.Produce(context.Background(), r, func(_ *kgo.Record, err error) {
			if err != nil {
				failed.Add(1)
			}
			wg.Done()
		})
	}
	time.Sleep(50 * time.Millisecond) // into the first merge
	if err := c.MoveTopicPartition("t", 0, 1-c.LeaderFor("t", 0)); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	waitCh(t, done, "promises never fired after the partition moved during a merge")
	if f := failed.Load(); f > 0 {
		t.Fatalf("%d produces failed", f)
	}
}

// BenchmarkProduceStreaming measures produce throughput and records per
// produce request against a blackholed cluster, with and without
// StreamingCompression:
//
//	go test -run xxx -bench BenchmarkProduceStreaming -benchtime=300000x ./
func BenchmarkProduceStreaming(b *testing.B) {
	for _, codec := range []struct {
		name string
		c    kgo.CompressionCodec
	}{
		{"none", kgo.NoCompression()},
		{"gzip", kgo.GzipCompression()},
		{"snappy", kgo.SnappyCompression()},
		{"lz4", kgo.Lz4Compression()},
		{"zstd", kgo.ZstdCompression()},
	} {
		for _, stream := range []bool{false, true} {
			b.Run(fmt.Sprintf("%s/stream=%v", codec.name, stream), func(b *testing.B) {
				c, err := NewCluster(NumBrokers(1), BlackholeProduce(), SeedTopics(4, "t"))
				if err != nil {
					b.Fatal(err)
				}
				defer c.Close()
				var counts produceCounter
				opts := []kgo.Opt{
					kgo.SeedBrokers(c.ListenAddrs()...),
					kgo.DefaultProduceTopic("t"),
					kgo.ProducerBatchCompression(codec.c),
					kgo.MaxBufferedRecords(1 << 20),
					kgo.WithHooks(&counts),
				}
				if stream {
					opts = append(opts, kgo.StreamingCompression())
				}
				cl, err := kgo.NewClient(opts...)
				if err != nil {
					b.Fatal(err)
				}
				defer cl.Close()

				val := mixValue(512, 0.15)
				b.SetBytes(int64(len(val)))
				b.ResetTimer()
				var wg sync.WaitGroup
				wg.Add(b.N)
				for range b.N {
					cl.Produce(context.Background(), &kgo.Record{Value: val}, func(_ *kgo.Record, err error) {
						if err != nil {
							b.Error(err)
						}
						wg.Done()
					})
				}
				wg.Wait()
				b.StopTimer()
				b.ReportMetric(float64(b.N)/float64(counts.reqs.Load()), "rec/req")
				b.ReportMetric(float64(b.N)/float64(counts.batches.Load()), "rec/batch")
			})
		}
	}
}
