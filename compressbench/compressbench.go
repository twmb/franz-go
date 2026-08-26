// Package compressbench is a throwaway experiment (branch compress-density)
// comparing two Kafka record-batch packing strategies:
//
//	A (what franz-go does today): accumulate records until their UNCOMPRESSED
//	  wire size would exceed maxBatch, then compress the batch once. Simple and
//	  exact, but the compressed batch ends up ~1/ratio of the limit -> under-packed.
//
//	B (proposed): stream records through a compressor and cut by COMPRESSED size.
//	  Uses decide-before-write with a worst-case (incompressible) reservation for
//	  the candidate record plus a flush-to-tighten only when nearing the boundary,
//	  so it never overflows, never splits, and never Closes a polluted compressor.
//
// The question this answers: does B's denser packing (fewer, bigger batches ->
// fewer produce requests) beat the compression-ratio tax of the boundary flushes?
package compressbench

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"math/rand"

	"github.com/klauspost/compress/zstd"
)

const (
	batchHeader    = 61 // record batch v2 header bytes (fixed)
	framingReserve = 5  // worst-case flexible (v9+) COMPACT_BYTES uvarint prefix
)

// ---- record wire encoding (mirrors franz-go promisedRec.appendTo) ----

func appendVarint(dst []byte, v int32) []byte {
	uv := uint32(v<<1) ^ uint32(v>>31) // zigzag
	for uv >= 0x80 {
		dst = append(dst, byte(uv)|0x80)
		uv >>= 7
	}
	return append(dst, byte(uv))
}

func appendVarlong(dst []byte, v int64) []byte {
	uv := uint64(v<<1) ^ uint64(v>>63)
	for uv >= 0x80 {
		dst = append(dst, byte(uv)|0x80)
		uv >>= 7
	}
	return append(dst, byte(uv))
}

func appendVarintBytes(dst, b []byte) []byte {
	dst = appendVarint(dst, int32(len(b)))
	return append(dst, b...)
}

// recordWire returns the on-wire bytes of one record (length-prefixed body),
// exactly as franz-go serializes it into the (to-be-compressed) records blob.
func recordWire(key, val []byte, offsetDelta int32) []byte {
	var body []byte
	body = append(body, 0)        // attributes
	body = appendVarlong(body, 0) // timestampDelta
	body = appendVarint(body, offsetDelta)
	body = appendVarintBytes(body, key)
	body = appendVarintBytes(body, val)
	body = appendVarint(body, 0) // header count
	var dst []byte
	dst = appendVarint(dst, int32(len(body)))
	return append(dst, body...)
}

// ---- streaming compressor adapter ----

type stream interface {
	write(p []byte)
	flush()             // sync flush; retains dictionary
	compressedLen() int // valid only right after flush()/close()
	close() []byte      // finalize (emit footer), return full compressed bytes
	reset()             // reset for reuse (as if pooled)
	footerMax() int     // upper bound on bytes close() appends beyond the last flush
}

type gzipStream struct {
	buf *bytes.Buffer
	w   *gzip.Writer
}

func newGzip(level int) func() stream {
	return func() stream {
		buf := new(bytes.Buffer)
		w, _ := gzip.NewWriterLevel(buf, level)
		return &gzipStream{buf, w}
	}
}
func (g *gzipStream) write(p []byte)     { g.w.Write(p) }
func (g *gzipStream) flush()             { g.w.Flush() }
func (g *gzipStream) compressedLen() int { return g.buf.Len() }
func (g *gzipStream) close() []byte      { g.w.Close(); return g.buf.Bytes() }
func (g *gzipStream) reset()             { g.buf.Reset(); g.w.Reset(g.buf) }
func (*gzipStream) footerMax() int       { return 8 } // gzip CRC32 + ISIZE

type zstdStream struct {
	buf *bytes.Buffer
	enc *zstd.Encoder
}

func newZstd(level zstd.EncoderLevel) func() stream {
	opts := []zstd.EOption{
		zstd.WithEncoderLevel(level),
		zstd.WithWindowSize(64 << 10),
		zstd.WithEncoderConcurrency(1),
		zstd.WithZeroFrames(true),
	}
	return func() stream {
		buf := new(bytes.Buffer)
		enc, _ := zstd.NewWriter(buf, opts...)
		return &zstdStream{buf, enc}
	}
}
func (z *zstdStream) write(p []byte)     { z.enc.Write(p) }
func (z *zstdStream) flush()             { z.enc.Flush() }
func (z *zstdStream) compressedLen() int { return z.buf.Len() }
func (z *zstdStream) close() []byte      { z.enc.Close(); return z.buf.Bytes() }
func (z *zstdStream) reset()             { z.buf.Reset(); z.enc.Reset(z.buf) }
func (*zstdStream) footerMax() int       { return 16 } // zstd frame epilogue (generous)

// ---- packing strategies ----

// packA is today's franz-go behavior: bound the batch by uncompressed wire size,
// compress once at seal. onBatch (optional) receives each sealed batch's
// compressed length (excluding header/framing).
func packA(wires [][]byte, maxBatch int, newStream func() stream, onBatch func(int)) (nBatches, totalWire int) {
	s := newStream()
	uncompressed, nInBatch := 0, 0
	seal := func() {
		c := s.close()
		nBatches++
		totalWire += batchHeader + len(c)
		if onBatch != nil {
			onBatch(len(c))
		}
		s.reset()
		uncompressed, nInBatch = 0, 0
	}
	for _, w := range wires {
		if nInBatch > 0 && batchHeader+uncompressed+len(w)+framingReserve > maxBatch {
			seal()
		}
		s.write(w)
		uncompressed += len(w)
		nInBatch++
	}
	if nInBatch > 0 {
		seal()
	}
	return
}

// packB is the proposed streaming, compressed-size-bound packing. It decides
// BEFORE writing each record (so the compressor only ever holds accepted
// records and close() is clean), reserving the candidate's uncompressed size as
// its worst-case compressed contribution, and flushing to tighten the estimate
// only when the worst case would exceed the limit.
func packB(wires [][]byte, maxBatch int, newStream func() stream, onBatch func(int)) (nBatches, totalWire int) {
	s := newStream()
	reserve := batchHeader + s.footerMax() + framingReserve
	checkpoint := 0 // exact compressed length at last flush
	sinceFlush := 0 // uncompressed bytes written since last flush
	nInBatch := 0
	seal := func() {
		c := s.close()
		nBatches++
		totalWire += batchHeader + len(c)
		if onBatch != nil {
			onBatch(len(c))
		}
		s.reset()
		checkpoint, sinceFlush, nInBatch = 0, 0, 0
	}
	for _, w := range wires {
		// Worst case if we accept w: everything since the last flush plus w
		// might be incompressible.
		if reserve+checkpoint+sinceFlush+len(w) > maxBatch && nInBatch > 0 {
			s.flush() // tighten: convert sinceFlush into real compressed bytes
			checkpoint = s.compressedLen()
			sinceFlush = 0
			if reserve+checkpoint+len(w) > maxBatch {
				seal() // even tightened, w won't fit -> cut before writing it
			}
		}
		s.write(w)
		sinceFlush += len(w)
		nInBatch++
	}
	if nInBatch > 0 {
		seal()
	}
	return
}

// packAdaptive is packB with a cheap incompressibility guard: once a boundary
// flush reveals the batch is compressing poorly (< ~10%), it stops flushing and
// falls back to A's uncompressed bound for the rest of the run. That kills the
// gzip-on-incompressible flush tax while keeping B's density on compressible
// data (which is the only place density is available anyway). The guard is
// sticky per run: a uniformly incompressible stream probes once, then behaves
// like A.
func packAdaptive(wires [][]byte, maxBatch int, newStream func() stream, onBatch func(int)) (nBatches, totalWire int) {
	s := newStream()
	reserve := batchHeader + s.footerMax() + framingReserve
	checkpoint, sinceFlush, uncomp, nInBatch := 0, 0, 0, 0
	incompressible := false
	seal := func() {
		c := s.close()
		nBatches++
		totalWire += batchHeader + len(c)
		if onBatch != nil {
			onBatch(len(c))
		}
		s.reset()
		checkpoint, sinceFlush, uncomp, nInBatch = 0, 0, 0, 0
	}
	for _, w := range wires {
		if incompressible {
			if nInBatch > 0 && batchHeader+uncomp+len(w)+framingReserve > maxBatch {
				seal()
			}
			s.write(w)
			uncomp += len(w)
			nInBatch++
			continue
		}
		if reserve+checkpoint+sinceFlush+len(w) > maxBatch && nInBatch > 0 {
			s.flush()
			checkpoint = s.compressedLen()
			sinceFlush = 0
			if checkpoint > (uncomp*9)/10 { // < ~10% compression -> stop trying
				incompressible = true
			}
			if reserve+checkpoint+len(w) > maxBatch {
				seal()
			}
		}
		s.write(w)
		sinceFlush += len(w)
		uncomp += len(w)
		nInBatch++
	}
	if nInBatch > 0 {
		seal()
	}
	return
}

// ---- workload generation ----

// genFn produces one record value given a deterministic rng and record index.
type genFn = func(rng *rand.Rand, i int) []byte

// mixVal builds a value that is a repeated JSON-ish template with randFrac of
// its bytes replaced by incompressible random, giving a tunable compression
// ratio (0.15 -> ~5x, 0.55 -> ~1.8x, 1.0 -> ~1x). This models real payloads
// (logs/JSON with some high-entropy fields), not a pure-redundancy strawman.
func mixVal(size int, randFrac float64) genFn {
	tmpl := []byte(`{"event":"click","user":"user-000000","session":"sess-0000","page":"/home/index","ts":1700000000000,"props":{"a":"x","b":"y","c":"z"}}`)
	nRand := int(float64(size) * randFrac)
	nTmpl := size - nRand
	return func(rng *rand.Rand, _ int) []byte {
		v := make([]byte, 0, size)
		for len(v) < nTmpl {
			v = append(v, tmpl...)
		}
		v = v[:nTmpl]
		for j := 0; j < 4 && nTmpl > 4; j++ { // vary a few bytes so records differ
			v[rng.Intn(nTmpl)] = byte('0' + rng.Intn(10))
		}
		r := make([]byte, nRand)
		rng.Read(r)
		return append(v, r...)
	}
}

func highCompressVal(size int) genFn { return mixVal(size, 0.15) } // ~5x
func medCompressVal(size int) genFn  { return mixVal(size, 0.55) } // ~1.8x
func lowCompressVal(size int) genFn  { return mixVal(size, 1.0) }  // ~1x

// genWires produces nRecords serialized record wires with the given value size
// and compressibility. Keys are small and structured.
func genWires(nRecords, _ int, genVal func(*rand.Rand, int) []byte) [][]byte {
	rng := rand.New(rand.NewSource(1))
	wires := make([][]byte, nRecords)
	for i := range wires {
		key := []byte(fmt.Sprintf("key-%08d", i))
		wires[i] = recordWire(key, genVal(rng, i), int32(i))
	}
	return wires
}
