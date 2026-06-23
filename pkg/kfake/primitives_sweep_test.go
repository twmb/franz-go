package kfake

// Round 15 of the FRANZ_AUDIT.md program: primitives sweep over pkg/kgo
// ring.go / pools.go / atomic_maybe_work.go / hooks.go / errors.go /
// compression.go (primitives-sweep.md). The test here covers the
// pools+fetch-processing seam; the compression fixes are unit-tested in
// pkg/kgo/compression_test.go. Tests whose error message starts with BUG
// REPRODUCED fail against the pre-fix client and flip to passing with the
// round's fixes.

import (
	"bytes"
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// countingPools counts Get/Put pairs for the records and decompress-bytes
// pools.
type countingPools struct {
	recGets, recPuts atomic.Int64
	decGets, decPuts atomic.Int64
}

func (p *countingPools) GetRecords(n int) []kgo.Record {
	p.recGets.Add(1)
	return make([]kgo.Record, 0, n)
}
func (p *countingPools) PutRecords([]kgo.Record) { p.recPuts.Add(1) }

func (p *countingPools) GetDecompressBytes(b []byte, _ kgo.CompressionCodecType) []byte {
	p.decGets.Add(1)
	return make([]byte, 0, 2*len(b)+1024)
}
func (p *countingPools) PutDecompressBytes([]byte) { p.decPuts.Add(1) }

// A batch from which no record is kept - an aborted transaction's data
// batch under read_committed, a transaction marker batch, or a batch wholly
// below the requested offset - never runs Record.Recycle, and the pool
// put-back only ran from a batch's last Recycle: the pooled slices taken to
// process such batches were never returned. Abort-heavy read_committed
// workloads leaked every pool Get exactly where fetch volume was highest.
func TestAuditPoolsReleasedNoRecordsKept(t *testing.T) {
	t.Parallel()
	c := newCluster(t, NumBrokers(1), SeedTopics(1, "t"))

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	producer := newPlainClient(t, c,
		kgo.TransactionalID("primitives-sweep-txn"),
		kgo.DefaultProduceTopic("t"),
		kgo.ProducerBatchCompression(kgo.SnappyCompression()),
	)

	// One aborted transaction with a compressed data batch...
	if err := producer.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	aborted := bytes.Repeat([]byte("aborted!"), 128)
	for range 8 {
		producer.Produce(ctx, &kgo.Record{Value: aborted}, nil)
	}
	if err := producer.Flush(ctx); err != nil {
		t.Fatalf("Flush: %v", err)
	}
	if err := producer.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatalf("EndTransaction(abort): %v", err)
	}
	// ...then one committed record so the consumer has something to poll
	// once it processes past the aborted batch.
	if err := producer.BeginTransaction(); err != nil {
		t.Fatalf("BeginTransaction: %v", err)
	}
	if err := producer.ProduceSync(ctx, &kgo.Record{Value: []byte("committed")}).FirstErr(); err != nil {
		t.Fatalf("ProduceSync: %v", err)
	}
	if err := producer.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatalf("EndTransaction(commit): %v", err)
	}

	pools := new(countingPools)
	consumer := newPlainClient(t, c,
		kgo.ConsumeTopics("t"),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.WithPools(pools),
		kgo.FetchMaxWait(250*time.Millisecond),
	)

	recs := collectRecords(t, consumer, 1, 10*time.Second)
	if string(recs[0].Value) != "committed" {
		t.Fatalf("got record %q != exp %q", recs[0].Value, "committed")
	}
	// Recycle the kept batch's records, returning its pooled slices; the
	// no-record-kept batches (the aborted data batch and the two markers)
	// must have released theirs during fetch processing.
	for _, r := range recs {
		r.Recycle()
	}

	if g, p := pools.recGets.Load(), pools.recPuts.Load(); g == 0 || g != p {
		t.Errorf("BUG REPRODUCED: record pool gets %d != puts %d (no-record-kept batches never release their pooled slices)", g, p)
	}
	if g, p := pools.decGets.Load(), pools.decPuts.Load(); g == 0 || g != p {
		t.Errorf("BUG REPRODUCED: decompress pool gets %d != puts %d (no-record-kept batches never release their pooled slices)", g, p)
	}
}
