package kfake

import (
	"context"
	"math/rand"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// mixValue builds a value of the given size with randFrac incompressible bytes
// (0.15 -> ~5x compressible, 1.0 -> ~1x).
func mixValue(size int, randFrac float64) []byte {
	tmpl := []byte(`{"event":"click","user":"user-000000","session":"sess-0000","page":"/home/index","ts":1700000000000,"props":{"a":"x","b":"y","c":"z"}}`)
	nRand := int(float64(size) * randFrac)
	v := make([]byte, 0, size)
	for len(v) < size-nRand {
		v = append(v, tmpl...)
	}
	v = v[:size-nRand]
	r := make([]byte, nRand)
	rand.New(rand.NewSource(1)).Read(r)
	return append(v, r...)
}

type produceReqCounter struct{ n *atomic.Int64 }

func (c produceReqCounter) OnBrokerWrite(_ kgo.BrokerMetadata, key int16, _ int, _, _ time.Duration, _ error) {
	if key == 0 { // Produce
		c.n.Add(1)
	}
}

// BenchmarkProduceBlackhole measures end-to-end produce throughput and batch
// density (records per produce request) against a blackholing kfake, with and
// without StreamingCompression. Run:
//
//	go test -run xxx -bench BenchmarkProduceBlackhole -benchtime=300000x ./
func BenchmarkProduceBlackhole(b *testing.B) {
	codecs := []struct {
		name string
		c    kgo.CompressionCodec
	}{
		{"none", kgo.NoCompression()},
		{"gzip", kgo.GzipCompression()},
		{"zstd", kgo.ZstdCompression()},
		{"lz4", kgo.Lz4Compression()},
	}
	lingers := []struct {
		name string
		d    time.Duration
	}{
		{"linger0", 0},
		{"linger1ms", time.Millisecond},
	}
	const valSize = 512

	for _, codec := range codecs {
		for _, linger := range lingers {
			for _, stream := range []bool{false, true} {
				mode := "legacy"
				if stream {
					mode = "stream"
				}
				b.Run(codec.name+"/"+linger.name+"/"+mode, func(b *testing.B) {
					c, err := NewCluster(NumBrokers(1), BlackholeProduce(), SeedTopics(4, "bench"))
					if err != nil {
						b.Fatal(err)
					}
					defer c.Close()

					var reqs atomic.Int64
					opts := []kgo.Opt{
						kgo.SeedBrokers(c.ListenAddrs()...),
						kgo.DefaultProduceTopic("bench"),
						kgo.ProducerBatchCompression(codec.c),
						kgo.MaxBufferedRecords(1 << 20),
						kgo.WithHooks(produceReqCounter{&reqs}),
					}
					if linger.d > 0 {
						opts = append(opts, kgo.ProducerLinger(linger.d))
					}
					if stream {
						opts = append(opts, kgo.StreamingCompression())
					}
					cl, err := kgo.NewClient(opts...)
					if err != nil {
						b.Fatal(err)
					}
					defer cl.Close()

					val := mixValue(valSize, 0.15)
					b.SetBytes(int64(valSize))
					b.ResetTimer()

					var wg sync.WaitGroup
					wg.Add(b.N)
					for i := 0; i < b.N; i++ {
						cl.Produce(context.Background(), &kgo.Record{Value: val}, func(_ *kgo.Record, err error) {
							if err != nil {
								b.Error(err)
							}
							wg.Done()
						})
					}
					wg.Wait()
					b.StopTimer()

					if n := reqs.Load(); n > 0 {
						b.ReportMetric(float64(b.N)/float64(n), "rec/req")
					}
				})
			}
		}
	}
}

// BenchmarkProduceBlackholePar is BenchmarkProduceBlackhole with 4x GOMAXPROCS
// producing goroutines.
func BenchmarkProduceBlackholePar(b *testing.B) {
	for _, codec := range []struct {
		name string
		c    kgo.CompressionCodec
	}{
		{"none", kgo.NoCompression()},
		{"gzip", kgo.GzipCompression()},
		{"zstd", kgo.ZstdCompression()},
	} {
		for _, stream := range []bool{false, true} {
			mode := "legacy"
			if stream {
				mode = "stream"
			}
			b.Run(codec.name+"/"+mode, func(b *testing.B) {
				c, err := NewCluster(NumBrokers(1), BlackholeProduce(), SeedTopics(4, "bench"))
				if err != nil {
					b.Fatal(err)
				}
				defer c.Close()

				var reqs atomic.Int64
				opts := []kgo.Opt{
					kgo.SeedBrokers(c.ListenAddrs()...),
					kgo.DefaultProduceTopic("bench"),
					kgo.ProducerBatchCompression(codec.c),
					kgo.MaxBufferedRecords(1 << 20),
					kgo.WithHooks(produceReqCounter{&reqs}),
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
				b.SetBytes(512)
				b.SetParallelism(4)
				b.ResetTimer()

				var wg sync.WaitGroup
				b.RunParallel(func(pb *testing.PB) {
					for pb.Next() {
						wg.Add(1)
						cl.Produce(context.Background(), &kgo.Record{Value: val}, func(_ *kgo.Record, err error) {
							if err != nil {
								b.Error(err)
							}
							wg.Done()
						})
					}
				})
				wg.Wait()
				b.StopTimer()
				if n := reqs.Load(); n > 0 {
					b.ReportMetric(float64(b.N)/float64(n), "rec/req")
				}
			})
		}
	}
}
