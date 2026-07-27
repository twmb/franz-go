package kgo

import (
	"context"
	"math"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const benchProducePartitions = 64

// newSeededProduceClient returns a client with `topics` topics of `partitions`
// partitions each, seeded directly into the producer's topic map so that no
// broker is needed. batchMaxBytes overrides the per-recBuf batch cap directly,
// bypassing the config minimum: a tiny cap rolls batches constantly (and so
// runs the KIP-480 abort-and-repick constantly), while an unreachable cap
// keeps every recBuf at exactly one batch, which is what the benchmarks want
// so that nothing ever drains toward the nonexistent broker.
func newSeededProduceClient(tb testing.TB, topics, partitions int, partitioner Partitioner, batchMaxBytes int32) *Client {
	tb.Helper()

	// A guaranteed-refused seed plus an unreachable metadata age means
	// nothing in these tests ever talks to a broker.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		tb.Fatal(err)
	}
	deadAddr := ln.Addr().String()
	ln.Close()

	opts := []Opt{
		SeedBrokers(deadAddr),
		ManualFlushing(),
		MaxBufferedRecords(math.MaxInt32),
		ProducerLinger(time.Minute),
		MetadataMaxAge(time.Hour),
		MetadataMinAge(time.Hour),
	}
	if partitioner != nil {
		opts = append(opts, RecordPartitioner(partitioner))
	}
	cl, err := NewClient(opts...)
	if err != nil {
		tb.Fatal(err)
	}

	sink := cl.newSink(0)
	data := make(topicsPartitionsData, topics)
	for t := range topics {
		topic := produceTestTopic(t)
		tps := newTopicPartitions()
		tpd := &topicPartitionsData{topic: topic}
		for p := range partitions {
			mp := metadataPartition{
				topic:     topic,
				partition: int32(p),
				sns:       sinkAndSource{sink: sink},
			}
			tp := mp.newPartition(cl, partitionKindProduce)
			tp.records.maxRecordBatchBytes = batchMaxBytes
			tpd.partitions = append(tpd.partitions, tp)
			tpd.writablePartitions = append(tpd.writablePartitions, tp)
		}
		tps.v.Store(tpd)
		data[topic] = tps
	}
	cl.producer.topics.storeData(data)

	return cl
}

func produceTestTopic(n int) string { return "bench-" + strconv.Itoa(n) }

// doPartition holds parts.partsMu across the buffering only for partitioners
// implementing TopicPartitionerOnNewBatch, whose abort-and-repick has to be
// atomic against the partitioner's state; for every other partitioner it drops
// the lock as soon as it has its pick. This produces concurrently through every
// shipped partitioner, with a batch cap small enough that batches roll (and so
// OnNewBatch fires) constantly, and asserts each record is promised exactly
// once with a partition in range.
//
// Under -race this is the guard on that narrowed scope: any partitioner state
// read or written outside partsMu surfaces here.
func TestConcurrentProducePartitioning(t *testing.T) {
	t.Parallel()

	const (
		partitions   = 8
		goroutines   = 8
		perGoroutine = 250
		total        = goroutines * perGoroutine
	)

	for _, test := range []struct {
		name        string
		partitioner Partitioner
		keys        bool
	}{
		// The default partitioner, both of its paths: keyed records
		// hash consistently, keyless records take the sticky byte path.
		{"default", nil, false},
		{"default-keyed", nil, true},

		// TopicPartitionerOnNewBatch implementors: the lock must still
		// span the buffering for these.
		{"sticky", StickyPartitioner(), false},
		{"sticky-key", StickyKeyPartitioner(nil), false},
		{"sticky-key-keyed", StickyKeyPartitioner(nil), true},
		{"least-backup", LeastBackupPartitioner(), false},

		{"round-robin", RoundRobinPartitioner(), false},
		{"uniform-bytes-nonadaptive", UniformBytesPartitioner(512, false, true, nil), true},
		{"basic-consistent", BasicConsistentPartitioner(func(string) func(*Record, int) int {
			return func(_ *Record, n int) int { return n - 1 }
		}), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			cl := newSeededProduceClient(t, 1, partitions, test.partitioner, 512)

			var (
				wg        sync.WaitGroup
				promised  atomic.Int64
				badPart   atomic.Int64
				topic     = produceTestTopic(0)
				ctx       = context.Background()
				value     = make([]byte, 64)
				produceWG sync.WaitGroup
			)
			wg.Add(total)
			promise := func(r *Record, _ error) {
				if r.Partition < 0 || r.Partition >= partitions {
					badPart.Add(1)
				}
				promised.Add(1)
				wg.Done()
			}

			produceWG.Add(goroutines)
			for g := range goroutines {
				go func() {
					defer produceWG.Done()
					for i := range perGoroutine {
						r := &Record{Topic: topic, Value: value}
						if test.keys {
							r.Key = []byte("key-" + strconv.Itoa(g*perGoroutine+i))
						}
						cl.TryProduce(ctx, r, promise)
					}
				}()
			}
			produceWG.Wait()

			// Close fails everything still buffered, so every record's
			// promise is guaranteed to run.
			cl.Close()

			done := make(chan struct{})
			go func() { wg.Wait(); close(done) }()
			select {
			case <-done:
			case <-time.After(30 * time.Second):
				t.Fatalf("timed out with %d/%d records promised", promised.Load(), total)
			}

			if got := promised.Load(); got != total {
				t.Errorf("promised %d records, want %d", got, total)
			}
			if got := badPart.Load(); got != 0 {
				t.Errorf("%d records were promised with an out-of-range partition", got)
			}
		})
	}
}

// benchProduce produces b.N records in parallel, giving each goroutine a topic
// round robin. Every goroutine cycles a fixed set of records rather than
// building one per iteration: we are measuring the produce path's locking, not
// allocation.
//
// These benchmarks are sensitive to anything that lets a record reach the
// (nonexistent) broker or that carries memory pressure between runs. Measure
// one benchmark at one -cpu per process; a -count above 1, or several -cpu
// values in one process, has each run inherit the previous run's buffered
// records and reports garbage.
func benchProduce(b *testing.B, topics int, keyed bool) {
	cl := newSeededProduceClient(b, topics, benchProducePartitions, nil, math.MaxInt32)
	defer cl.Close()

	value := make([]byte, 128)
	ctx := context.Background()

	var goroutine atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		topic := produceTestTopic(int(goroutine.Add(1)-1) % topics)
		// Keyed records hash across every partition, so the records a
		// goroutine produces land in many different recBufs while still
		// sharing the topic's one partsMu. Keyless records take the
		// default partitioner's sticky path, which pins one partition
		// (and so one recBuf) at a time.
		rs := make([]*Record, benchProducePartitions)
		for i := range rs {
			rs[i] = &Record{Topic: topic, Value: value}
			if keyed {
				rs[i].Key = []byte("key-" + strconv.Itoa(i))
			}
		}
		var i int
		for pb.Next() {
			cl.TryProduce(ctx, rs[i], nil)
			i++
			if i == len(rs) {
				i = 0
			}
		}
	})
	b.StopTimer()

	// Every record must have buffered. If any failed - a drain reaching the
	// dead broker and erroring records, say - the numbers above measured
	// the wrong thing.
	cl.producer.mu.Lock()
	buffered := cl.producer.bufferedRecords
	cl.producer.mu.Unlock()
	if buffered != int64(b.N) {
		b.Fatalf("buffered %d records, expected all %d to buffer", buffered, b.N)
	}
}

// All goroutines share one topic and so share that topic's partsMu. Keyed
// records spread over every partition, so partsMu is the only thing every
// goroutine must pass through.
func BenchmarkProduceParallelSharedTopicKeyed(b *testing.B) { benchProduce(b, 1, true) }

// As above, but keyless: the default partitioner is sticky, so every goroutine
// also converges on one partition's recBuf.
func BenchmarkProduceParallelSharedTopic(b *testing.B) { benchProduce(b, 1, false) }

// Each goroutine gets its own topic and so its own partsMu, with the same
// partition count as the shared-topic benchmarks. This is the uncontended
// reference point.
func BenchmarkProduceParallelTopicPerGoroutine(b *testing.B) { benchProduce(b, 8, true) }
