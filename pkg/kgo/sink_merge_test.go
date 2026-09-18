package kgo

import (
	"bytes"
	"context"
	"errors"
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
// either repeating a pattern or random bytes. Every fifth record has no
// key, every seventh an empty value, every third headers, and timestamps
// spread over days, out of order.
func mergeHarness(t *testing.T, codec CompressionCodec, limit int32, n int, random bool) (*sink, *recBuf, []*Record) {
	t.Helper()
	cl, s, r := sinkHarness(t)
	cl.cfg.streamCompression = true
	cl.cfg.compressor, _ = DefaultCompressor(codec)
	r.maxRecordBatchBytes = limit
	rng := rand.New(rand.NewSource(1))
	base := time.Now()
	var recs []*Record
	for i := range n {
		v := bytes.Repeat([]byte("abcdefghij"), 10)
		if random {
			rng.Read(v)
		}
		rec := &Record{Key: fmt.Append(nil, i), Value: v, Context: context.Background()}
		rec.Timestamp = base.Add(time.Duration(rng.Int63n(int64(6*24*time.Hour))) - 3*24*time.Hour)
		switch {
		case i%5 == 0:
			rec.Key = nil
		case i%7 == 0:
			rec.Value = []byte{}
		}
		if i%3 == 0 {
			rec.Headers = []RecordHeader{{Key: "h", Value: v[:8]}, {Key: "", Value: nil}}
		}
		r.bufferRecord(promisedRec{ctx: context.Background(), promise: func(*Record, error) {}, Record: rec}, false)
		recs = append(recs, rec)
	}
	return s, r, recs
}

// verifyBatch encodes b as a produce batch at every record batch version
// shape (v3, v8, v9, v13), transactional and not, and checks its size is
// within the limit and, for a merged batch, exactly its wire length; that
// kmsg parses it with the right header fields; and that its records
// decompress to what the legacy path would have serialized. It returns the
// batch's records.
func verifyBatch(t *testing.T, r *recBuf, b *recBatch) []*Record {
	t.Helper()
	var recs []*Record
	for _, version := range []int16{3, 8, 9, 13} {
		txn := version%2 == 1
		wire, _ := seqRecBatch{seq: 7, recBatch: b}.appendTo(nil, version, 5, 2, txn, r.cl.cfg.compressor)
		wireLength, _, _ := b.wireLengthForProduceVersion(int32(version))
		if int32(len(wire)) > wireLength || wireLength > r.maxRecordBatchBytes || b.stream != nil && int32(len(wire)) != wireLength {
			t.Fatalf("v%d: encoded %d bytes, wireLength %d, limit %d, merged %v", version, len(wire), wireLength, r.maxRecordBatchBytes, b.stream != nil)
		}
		// A sink that has not seen a produce response sizes with the
		// largest encoding; for a merged batch that must still fit.
		if unknown, _, _ := b.wireLengthForProduceVersion(-1); b.stream != nil && unknown > r.maxRecordBatchBytes {
			t.Fatalf("merged batch sizes to %d bytes at an unknown produce version, limit %d", unknown, r.maxRecordBatchBytes)
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
		if kb.Magic != 2 || int(kb.LastOffsetDelta) != len(b.records)-1 || kb.ProducerID != 5 || kb.ProducerEpoch != 2 || kb.FirstSequence != 7 ||
			kb.Attributes&0x10 != 0 != txn || kb.FirstTimestamp != b.firstTimestamp || kb.MaxTimestamp != b.firstTimestamp+b.maxTimestampDelta {
			t.Fatalf("v%d txn=%v: header fields wrong: %+v", version, txn, kb)
		}
		got, err := DefaultDecompressor().Decompress(kb.Records, CompressionCodecType(kb.Attributes&7))
		if err != nil {
			t.Fatalf("v%d: records do not decompress: %v", version, err)
		}
		var want []byte
		recs = recs[:0]
		for i, pr := range b.records {
			want = pr.appendTo(want, int32(i))
			recs = append(recs, pr.Record)
		}
		if !bytes.Equal(got, want) {
			t.Fatalf("v%d: decompressed records differ from serializing the batch's records", version)
		}
	}
	return recs
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
				s, r, want := mergeHarness(t, codec, limit, n, random)
				sources, merged, biggest := len(r.batches), 0, 0
				var got []*Record
				for len(r.batches) > 0 {
					s.mergeBacklogs()
					b := r.batches[0]
					got = append(got, verifyBatch(t, r, b)...)
					if b.stream != nil {
						merged++
					}
					biggest = max(biggest, len(b.records))
					r.batches = r.batches[1:] // as if the request finished
				}
				for i := range min(len(got), n) {
					if got[i] != want[i] {
						t.Fatalf("record %d is not the record produced %dth: records lost or reordered", i, i)
					}
				}
				if len(got) != n || merged == 0 {
					t.Fatalf("saw %d of %d records across %d merged batches", len(got), n, merged)
				}
				if avg := n / sources; !random && biggest < 3*avg {
					t.Fatalf("biggest merged batch holds %d records, want at least 3x the %d per source", biggest, avg)
				}
			})
		}
	}
}

// MaxDecompressedBatchBytes caps a merged batch's uncompressed bytes, so a
// consumer bounded the same way can decompress it.
func TestMergeBacklogMaxDecompressed(t *testing.T) {
	t.Parallel()
	const n, limit, maxUncompressed = 500, 4096, 2048
	s, r, want := mergeHarness(t, GzipCompression(), limit, n, false)
	r.cl.cfg.maxDecompressedBatchBytes = maxUncompressed
	var got []*Record
	var merged int
	for len(r.batches) > 0 {
		s.mergeBacklogs()
		b := r.batches[0]
		got = append(got, verifyBatch(t, r, b)...)
		if b.stream != nil {
			merged++
			if b.stream.uncompressed > maxUncompressed {
				t.Fatalf("merged batch holds %d uncompressed bytes, max %d", b.stream.uncompressed, maxUncompressed)
			}
		}
		r.batches = r.batches[1:]
	}
	if len(got) != n || merged == 0 {
		t.Fatalf("saw %d of %d records across %d merged batches", len(got), n, merged)
	}
	for i := range got {
		if got[i] != want[i] {
			t.Fatalf("record %d is not the record produced %dth", i, i)
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
		ts := base.Add(time.Duration(rng.Int63n(int64(6000*24*time.Hour))) - 3000*24*time.Hour).Truncate(time.Millisecond)
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
	s, r, _ := mergeHarness(t, codec, small, n, random)
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

// A merged batch stages on a sink that has not yet seen a produce response,
// as after a leader move to a broker this client never produced to.
func TestMergeStagesOnFreshSink(t *testing.T) {
	t.Parallel()
	s, r, _ := mergeHarness(t, GzipCompression(), 4096, 500, false)
	s.mergeBacklogs()
	if r.batches[0].stream == nil {
		t.Fatal("head did not merge")
	}
	fresh := r.cl.newSink(2)
	s.removeRecBuf(r)
	r.mu.Lock()
	r.sink = fresh
	r.mu.Unlock()
	fresh.addRecBuf(r)
	req, _, _ := fresh.createReq(5, 0)
	if len(req.batches.bs) != 1 {
		t.Fatalf("fresh sink staged %d topics, want the merged batch", len(req.batches.bs))
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

// A failure sweep during a merge waits for at most one chunk, not a whole
// source: the merge releases the source while the codec works. The first
// source is a few records and the second is 4MB of half random data at
// zstd's slowest level. The merge stamps each record for the merged batch
// as it copies it, under the source's mu, so the stamps say how far it
// has gotten each time the test takes the mu: a merge holding the source
// across the codec would stamp all of it before the mu is free. Stamps
// are read before the sweep, since finishing a promise resets them. At
// the slowest level a chunk's compression takes milliseconds under the
// race detector, so a descheduled test costs a few chunks.
func TestMergeSweepWaitsOneChunk(t *testing.T) {
	t.Parallel()
	cl, s, r := sinkHarness(t)
	cl.cfg.streamCompression = true
	cl.cfg.compressor, _ = DefaultCompressor(ZstdCompression().WithLevel(4)) // zstd.SpeedBestCompression
	rng := rand.New(rand.NewSource(9))
	words := bytes.Fields([]byte("the quick brown fox jumps over the lazy dog while brokers replicate partitions across racks"))
	base := time.Now()
	buffer := func(n int, ts time.Time) []*Record {
		var recs []*Record
		for range n {
			var v []byte
			for len(v) < 1024 {
				v = append(append(v, words[rng.Intn(len(words))]...), ' ')
			}
			rng.Read(v[512:]) // half random: slow to search, still merges
			rec := &Record{Value: v, Timestamp: ts, Context: context.Background()}
			r.bufferRecord(promisedRec{ctx: context.Background(), promise: func(*Record, error) {}, Record: rec}, false)
			recs = append(recs, rec)
		}
		return recs
	}
	later := base.Add(time.Second)
	r.maxRecordBatchBytes = 8 << 10
	buffer(7, base)            // fills the first source
	second := buffer(1, later) // does not fit: starts the second
	r.maxRecordBatchBytes = 4 << 20
	second = append(second, buffer(3799, later)...)
	if len(r.batches) != 2 || len(r.batches[1].records) != len(second) {
		t.Fatalf("buffered %d batches, want 2 with the second holding %d records", len(r.batches), len(second))
	}
	// In its own batch, every record of the second source has timestamp
	// delta 0; in the merged batch, whose first timestamp is base, 1000.
	src := r.batches[1]
	stamped := func() int {
		var n int
		for _, rec := range second {
			if _, tsDelta := rec.lengthAndTimestampDelta(); tsDelta == 1000 {
				n++
			}
		}
		return n
	}
	done := make(chan struct{})
	go func() { defer close(done); s.mergeBacklogs() }()
	const past = 128
	var n int
	for n <= past {
		src.mu.Lock()
		n = stamped()
		src.mu.Unlock()
		select {
		case <-done:
			t.Fatal("merge finished before reaching the second source")
		default:
			runtime.Gosched()
		}
	}
	const recordsPerChunk = streamChunk / 1024 // values are 1KB
	if n > past+8*recordsPerChunk {
		t.Fatalf("merge stamped %d of %d records before releasing the source", n, len(second))
	}
	r.mu.Lock()
	r.failAllRecords(errors.New("swept"))
	r.mu.Unlock()
	<-done
	if len(r.batches) != 0 {
		t.Fatalf("%d batches survived the sweep", len(r.batches))
	}
}

// A merged batch the sink's version cannot carry fails only once nothing
// is inflight ahead of it: failAllRecords would drop the inflight batches
// too, and the sequence chain with them.
func TestMergeUnsupportedWaitsForInflight(t *testing.T) {
	t.Parallel()
	s, r, _ := mergeHarness(t, ZstdCompression(), 4096, 500, false)
	b0 := r.batches[0]
	stageOne(t, s)
	r.okOnSink = true
	s.mergeBacklogs()
	if r.batches[1].stream == nil {
		t.Fatal("backlog behind the inflight batch did not merge")
	}
	s.produceVersion.Store(5) // as after a move to a broker below zstd's v7
	if req, _, _ := s.createReq(5, 0); len(req.batches.bs) != 0 || r.batches == nil {
		t.Fatal("createReq staged or failed the merged batch behind an inflight batch")
	}
	r.mu.Lock()
	r.cl.finishBatch(b0, 5, 0, 0, nil)
	b0.decInflight()
	r.mu.Unlock()
	if req, _, _ := s.createReq(5, 0); len(req.batches.bs) != 0 || r.batches != nil {
		t.Fatal("createReq did not fail the merged batch once it was the head")
	}
}

// A head record that fits the uncompressed bound but not the compressed one
// (it sits within the codec slack of the limit) merges nothing, and the
// batches are left as they were.
func TestMergeBacklogHeadTooBig(t *testing.T) {
	t.Parallel()
	const limit = 4096
	s, r, _ := mergeHarness(t, Lz4Compression(), limit, 1, false)
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
