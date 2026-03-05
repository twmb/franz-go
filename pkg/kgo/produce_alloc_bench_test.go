// Package kgo: benchmarks to reproduce Produce path allocations inside the library.
//
// These benchmarks minimize caller-side allocations so that allocs/op reflect
// franz-go internal allocations:
//   - producer.go: calcNums closure (1 alloc/call), promiseRecord []promisedRec slice
//   - sink.go: newRecordBatch &recBatch{}, append(records, pr) growth
//   - AppendVarintBytes / request buffer growth in encode path
//
// Baseline (upstream, no patches): BenchmarkProduceAllocsMinimalCaller typically
// reports ~5 allocs/op. After the inline-only patch (no closure), expect ~4 allocs/op.
//
// Requires Kafka (KGO_SEEDS or 127.0.0.1:9092). The package init in helpers_test.go
// also requires Kafka; if unavailable, the test binary may panic before the benchmark runs.
//
// Run:
//
//	go test -bench=BenchmarkProduce -benchmem -count=5 ./pkg/kgo/
//
// Stable alloc reporting (fixed iterations):
//
//	go test -bench=BenchmarkProduceAllocsMinimalCaller -benchmem -benchtime=10000x ./pkg/kgo/
//
// Profile allocs:
//
//	go test -bench=BenchmarkProduceAllocsMinimalCaller -benchtime=10000x -memprofile=mem.out ./pkg/kgo/
//	go tool pprof -alloc_objects -top mem.out
package kgo

import (
	"context"
	"testing"
)

const (
	produceAllocBenchTopic = "produce-alloc-bench"
	produceAllocKeyLen     = 36
	produceAllocValueLen   = 256
	produceAllocFlushEvery = 2000
)

// BenchmarkProduceAllocsMinimalCaller measures allocs on the Produce path with
// minimal caller-side allocation: one Record (reused), one shared callback,
// same Key/Value slices every time. Periodic Flush ensures callbacks run so
// that buffering and response handling are exercised. The reported allocs/op
// are predominantly from the library (producer.go produce closure,
// promiseRecord slice, sink.go recBatch, AppendVarintBytes / request buffer).
func BenchmarkProduceAllocsMinimalCaller(b *testing.B) {
	cl, err := newTestClient(
		DefaultProduceTopic(produceAllocBenchTopic),
		AllowAutoTopicCreation(),
		RequiredAcks(LeaderAck()),
		DisableIdempotentWrite(),
		MaxProduceRequestsInflightPerBroker(5),
	)
	if err != nil {
		b.Skipf("Kafka unreachable (need KGO_SEEDS or 127.0.0.1:9092): %v", err)
	}
	defer cl.Close()

	// Single Record and Key/Value buffers reused for every Produce.
	key := make([]byte, produceAllocKeyLen)
	for i := range key {
		key[i] = byte('k')
	}
	value := make([]byte, produceAllocValueLen)
	for i := range value {
		value[i] = byte('v')
	}
	record := &Record{
		Topic: produceAllocBenchTopic,
		Key:   key,
		Value: value,
	}

	// Single callback shared by all Produce calls (no per-call closure alloc).
	var produceCB func(*Record, error)
	produceCB = func(*Record, error) {}

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		cl.Produce(ctx, record, produceCB)
		if (i+1)%produceAllocFlushEvery == 0 {
			_ = cl.Flush(ctx)
		}
	}
	_ = cl.Flush(ctx)
}

// BenchmarkProduceAllocsWithNewRecord is like BenchmarkProduceAllocsMinimalCaller
// but allocates a new Record and new Key/Value slices every iteration. It
// establishes an upper bound: allocs/op will include both library and caller
// allocations. Compare with MinimalCaller to see how much the library contributes.
func BenchmarkProduceAllocsWithNewRecord(b *testing.B) {
	cl, err := newTestClient(
		DefaultProduceTopic(produceAllocBenchTopic),
		AllowAutoTopicCreation(),
		RequiredAcks(LeaderAck()),
		DisableIdempotentWrite(),
		MaxProduceRequestsInflightPerBroker(5),
	)
	if err != nil {
		b.Skipf("Kafka unreachable (need KGO_SEEDS or 127.0.0.1:9092): %v", err)
	}
	defer cl.Close()

	ctx := context.Background()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		key := make([]byte, produceAllocKeyLen)
		value := make([]byte, produceAllocValueLen)
		record := &Record{
			Topic: produceAllocBenchTopic,
			Key:   key,
			Value:  value,
		}
		cl.Produce(ctx, record, func(*Record, error) {})
		if (i+1)%produceAllocFlushEvery == 0 {
			_ = cl.Flush(ctx)
		}
	}
	_ = cl.Flush(ctx)
}
