package kgo

import (
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// mergeHarness returns a recBuf on a streaming sink with a small batch
// limit and buffers n records of ~100 byte values, each record's value
// either repeating a pattern or random bytes.
func mergeHarness(t *testing.T, codec CompressionCodec, limit int32, n int, random bool) (*sink, *recBuf) {
	t.Helper()
	cl, s, r := sinkHarness(t)
	cl.cfg.streamCompression = true
	cl.cfg.compressor, _ = DefaultCompressor(codec)
	r.maxRecordBatchBytes = limit
	rng := rand.New(rand.NewSource(1))
	for i := range n {
		v := bytes.Repeat([]byte("abcdefghij"), 10)
		if random {
			rng.Read(v)
		}
		r.bufferRecord(promisedRec{
			ctx:     context.Background(),
			promise: func(*Record, error) {},
			Record:  &Record{Key: fmt.Append(nil, i), Value: v, Context: context.Background()},
		}, false)
	}
	return s, r
}

// verifyBatch encodes b as a v8 and a v9 produce batch and checks its size
// is within the limit and, for a merged batch, exactly its wire length;
// that kmsg parses it; and that its records decompress to what the legacy
// path would have serialized. It returns the batch's keys.
func verifyBatch(t *testing.T, r *recBuf, b *recBatch) []string {
	t.Helper()
	var keys []string
	for _, version := range []int16{8, 9} { // last non-flexible, first flexible
		wire, _ := seqRecBatch{recBatch: b}.appendTo(nil, version, -1, -1, false, r.cl.cfg.compressor)
		wireLength, _, _ := b.wireLengthForProduceVersion(int32(version))
		if int32(len(wire)) > wireLength || wireLength > r.maxRecordBatchBytes || b.stream != nil && int32(len(wire)) != wireLength {
			t.Fatalf("v%d: encoded %d bytes, wireLength %d, limit %d, merged %v", version, len(wire), wireLength, r.maxRecordBatchBytes, b.stream != nil)
		}
		rd := kbin.Reader{Src: wire}
		raw := rd.NullableBytes()
		if version >= 9 {
			rd = kbin.Reader{Src: wire}
			raw = rd.CompactBytes()
		}
		var kb kmsg.RecordBatch
		if err := kb.ReadFrom(raw); err != nil || len(rd.Src) != 0 {
			t.Fatalf("v%d: batch does not parse: %v (%d trailing bytes)", version, err, len(rd.Src))
		}
		if int(kb.NumRecords) != len(b.records) || int(kb.Length)+12 != len(raw) {
			t.Fatalf("v%d: parsed %d records of length %d, want %d records in %d bytes", version, kb.NumRecords, kb.Length, len(b.records), len(raw))
		}
		got, err := DefaultDecompressor().Decompress(kb.Records, CompressionCodecType(kb.Attributes&7))
		if err != nil {
			t.Fatalf("v%d: records do not decompress: %v", version, err)
		}
		var want []byte
		keys = keys[:0]
		for i, pr := range b.records {
			want = pr.appendTo(want, int32(i))
			keys = append(keys, string(pr.Key))
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("v%d: decompressed records differ from serializing the batch's records", version)
		}
	}
	return keys
}

// Every codec merges a backlog into batches that stay within the limit,
// parse, and decompress to exactly the records they hold, in order. Random
// values exercise the incompressible worst case, where the bound rests on
// the codecs storing raw blocks with tiny overhead.
func TestMergeBacklog(t *testing.T) {
	t.Parallel()
	for _, codec := range []CompressionCodec{GzipCompression(), SnappyCompression(), Lz4Compression(), ZstdCompression()} {
		for _, random := range []bool{false, true} {
			t.Run(fmt.Sprintf("%v/random=%v", codec.codec, random), func(t *testing.T) {
				t.Parallel()
				const n, limit = 500, 4096
				s, r := mergeHarness(t, codec, limit, n, random)
				sources, merged, biggest := len(r.batches), 0, 0
				var keys []string
				for len(r.batches) > 0 {
					s.mergeBacklogs()
					b := r.batches[0]
					keys = append(keys, verifyBatch(t, r, b)...)
					if b.stream != nil {
						merged++
					}
					biggest = max(biggest, len(b.records))
					r.batches = r.batches[1:] // as if the request finished
				}
				for i, key := range keys {
					if key != fmt.Sprint(i) {
						t.Fatalf("record %d has key %s: records lost or reordered", i, key)
					}
				}
				if len(keys) != n || merged == 0 {
					t.Fatalf("saw %d of %d records across %d merged batches", len(keys), n, merged)
				}
				if avg := n / sources; !random && biggest < 3*avg {
					t.Fatalf("biggest merged batch holds %d records, want at least 3x the %d per source", biggest, avg)
				}
			})
		}
	}
}

// renumber must agree with recomputing a record's numbers from scratch at
// every varint width: timestamps spread over days and out of order, and
// offsets past one byte.
func TestRenumber(t *testing.T) {
	t.Parallel()
	_, _, r := sinkHarness(t)
	src, dst := r.newRecordBatch(), r.newRecordBatch()
	rng := rand.New(rand.NewSource(3))
	base := time.Now()
	for i := range 400 {
		ts := base.Add(time.Duration(rng.Int63n(int64(6*24*time.Hour))) - 3*24*time.Hour).Truncate(time.Millisecond)
		pr := promisedRec{Record: &Record{Value: []byte("v"), Timestamp: ts}}
		src.appendRecord(pr, src.calculateRecordNumbers(pr.Record)) // stamps pr for src
		if i%3 == 0 {                                               // move some, so dst positions differ from src positions
			got, want := dst.renumber(pr, int32(i)), dst.calculateRecordNumbers(pr.Record)
			if got != want {
				t.Fatalf("record %d: renumber gave %+v, recomputing gives %+v", i, got, want)
			}
			dst.appendRecord(pr, got)
		}
	}
}

// discardHarness buffers records into exactly two batches under the small
// limit, then raises the limit so a merge would span both.
func discardHarness(t *testing.T, codec CompressionCodec, small, large int32, n int, random bool) (*sink, *recBuf) {
	t.Helper()
	s, r := mergeHarness(t, codec, small, n, random)
	if len(r.batches) != 2 {
		t.Fatalf("buffered %d batches, want 2", len(r.batches))
	}
	r.maxRecordBatchBytes = large
	return s, r
}

// A merge whose sources disappear while it compresses is discarded and the
// surviving sources serialize exactly as before: their records were
// re-stamped for the merged batch and must be restored. Here the first
// source is popped from under the merge while it waits on the second's mu.
// With random values the merge also cuts inside the second source, so the
// discard has a tail to recycle.
func TestMergeBacklogDiscard(t *testing.T) {
	t.Parallel()
	for _, random := range []bool{false, true} {
		t.Run(fmt.Sprintf("random=%v", random), func(t *testing.T) {
			t.Parallel()
			s, r := discardHarness(t, GzipCompression(), 2200, 4096, 36, random)
			before, _ := seqRecBatch{recBatch: r.batches[1]}.appendTo(nil, 8, -1, -1, false, nil)
			last := r.batches[1]
			last.mu.Lock()
			done := make(chan struct{})
			go func() { defer close(done); s.mergeBacklogs() }()
			for merging := false; !merging; runtime.Gosched() {
				r.mu.Lock()
				merging = r.merging
				r.mu.Unlock()
			}
			r.mu.Lock()
			r.batches = r.batches[1:]
			r.mu.Unlock()
			last.mu.Unlock()
			<-done

			after, _ := seqRecBatch{recBatch: r.batches[0]}.appendTo(nil, 8, -1, -1, false, nil)
			if len(r.batches) != 1 || !bytes.Equal(before, after) {
				t.Fatalf("discarded merge left %d batches, source serializes the same: %v", len(r.batches), bytes.Equal(before, after))
			}
			if b := r.batches[0]; b.frozen || b.stream != nil || r.merging {
				t.Fatalf("source frozen %v, stream %v, merging %v after a discarded merge", b.frozen, b.stream != nil, r.merging)
			}
		})
	}
}

// A source swept (records nil) before the merge reads it discards the merge.
func TestMergeSpanSwept(t *testing.T) {
	t.Parallel()
	s, r := discardHarness(t, ZstdCompression(), 2200, 4096, 36, false)
	span := append([]*recBatch(nil), r.batches...)
	span[1].records = nil // as failAllRecords would, under mu
	cc, codec := s.streamCodec()
	m, tail, consumed, ok := r.mergeSpan(span, len(span[0].records), 4096, cc, codec)
	if ok || consumed != 1 || tail != nil {
		t.Fatalf("swept merge returned ok %v, consumed %d, tail %v", ok, consumed, tail != nil)
	}
	m.recycle()
}

// A head record that fits the uncompressed bound but not the compressed one
// (it sits within the codec slack of the limit) merges nothing, and the
// batches are left as they were.
func TestMergeBacklogHeadTooBig(t *testing.T) {
	t.Parallel()
	const limit = 4096
	s, r := mergeHarness(t, Lz4Compression(), limit, 1, false)
	big := &Record{Value: make([]byte, limit-recordBatchOverhead-8), Context: context.Background()}
	r.bufferRecord(promisedRec{ctx: context.Background(), promise: func(*Record, error) {}, Record: big}, false)
	r.batches[0], r.batches[1] = r.batches[1], r.batches[0] // the big record leads
	before, _ := seqRecBatch{recBatch: r.batches[1]}.appendTo(nil, 8, -1, -1, false, nil)
	s.mergeBacklogs()
	after, _ := seqRecBatch{recBatch: r.batches[1]}.appendTo(nil, 8, -1, -1, false, nil)
	if len(r.batches) != 2 || r.batches[0].frozen || r.batches[0].stream != nil || !bytes.Equal(before, after) {
		t.Fatalf("merge of an oversized head left %d batches, head frozen %v", len(r.batches), r.batches[0].frozen)
	}
}
