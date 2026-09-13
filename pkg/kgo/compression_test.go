package kgo

import (
	"bytes"
	"compress/gzip"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// Regression test for #778.
func TestCompressionCodecLZ4WithSpecifiedLevel(t *testing.T) {
	t.Parallel()

	codec := Lz4Compression().WithLevel(512)
	w := lz4.NewWriter(new(bytes.Buffer))
	err := w.Apply(lz4.CompressionLevelOption(lz4.CompressionLevel(codec.level)))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestNewCompressor(t *testing.T) {
	t.Parallel()
	for i, test := range []struct {
		codecs []CompressionCodec
		fail   bool
	}{
		{codecs: []CompressionCodec{{codec: -1}}, fail: true},
		{codecs: []CompressionCodec{{codec: 5}}, fail: true},

		{codecs: []CompressionCodec{{codec: 0}}},
		{codecs: []CompressionCodec{{codec: 1}, {codec: 0}}},
		{codecs: []CompressionCodec{{codec: 2}, {codec: 0}}},
		{codecs: []CompressionCodec{{codec: 3}}},
		{codecs: []CompressionCodec{{codec: 4}}},
		{codecs: []CompressionCodec{{codec: 4}, {codec: 3}}},

		{codecs: []CompressionCodec{{codec: 1, level: 127}}}, // bad gzip level is defaulted fine
		{codecs: []CompressionCodec{{codec: 3, level: 127}}}, // bad lz4 level, same
		{codecs: []CompressionCodec{{codec: 4, level: 127}}}, // bad zstd level, same

		{codecs: []CompressionCodec{
			{codec: 4},
			{codec: 4},
			{codec: 3},
			{codec: 2},
			{codec: 1, level: 1},
		}},
	} {
		_, err := DefaultCompressor(test.codecs...)
		fail := err != nil
		if fail != test.fail {
			t.Errorf("#%d: ok? %v, exp ok? %v", i, !fail, !test.fail)
		}
	}
}

// Regression test: DefaultCompressor's gzip arm adopted the user's level
// only when the validation probe failed (an inverted check). A valid custom level
// was silently ignored, and an invalid level was adopted, making the pool
// hand out typed-nil *gzip.Writers that panicked on first use. This test
// panics pre-fix on the invalid-level arm and fails pre-fix on the
// levels-differ assertion.
func TestGzipCompressionLevels(t *testing.T) {
	t.Parallel()

	// Compressible-but-not-uniform input so that BestSpeed and
	// BestCompression provably emit different bytes.
	in := bytes.Repeat([]byte("abcdefghijklmno pqrs tuvwxy   z0123456789 the quick brown fox "), 2048)

	compress := func(c Compressor) []byte {
		w := new(bytes.Buffer)
		out, codec := c.Compress(w, in)
		if codec != CodecGzip {
			t.Fatalf("got codec %d != exp gzip", codec)
		}
		return bytes.Clone(out)
	}

	c1, err := DefaultCompressor(GzipCompression().WithLevel(gzip.BestSpeed))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	c9, err := DefaultCompressor(GzipCompression().WithLevel(gzip.BestCompression))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	cDefault, err := DefaultCompressor(GzipCompression())
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}
	// WithLevel docs: an invalid level falls back to the default level.
	cBad, err := DefaultCompressor(GzipCompression().WithLevel(127))
	if err != nil {
		t.Fatalf("unexpected err: %v", err)
	}

	out1, out9, outDefault, outBad := compress(c1), compress(c9), compress(cDefault), compress(cBad)

	if bytes.Equal(out1, out9) {
		t.Error("BestSpeed and BestCompression unexpectedly compressed identically; custom levels are being ignored")
	}
	if !bytes.Equal(outBad, outDefault) {
		t.Error("invalid level did not fall back to the default level")
	}

	d := DefaultDecompressor()
	for i, out := range [][]byte{out1, out9, outDefault, outBad} {
		got, err := d.Decompress(out, CodecGzip)
		if err != nil {
			t.Errorf("#%d: unexpected decompress err: %v", i, err)
			continue
		}
		if !bytes.Equal(got, in) {
			t.Errorf("#%d: decompressed != input", i)
		}
	}
}

func TestCompressDecompress(t *testing.T) {
	randStr := func(length int) []byte {
		const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
		b := make([]byte, length)
		for i := range b {
			b[i] = charset[rand.Intn(len(charset))]
		}
		return b
	}

	t.Parallel()
	d := DefaultDecompressor()
	inputs := [][]byte{
		randStr(1 << 2),
		randStr(1 << 5),
		randStr(1 << 8),
	}

	var wg sync.WaitGroup
	for _, flag := range []CompressFlag{
		0, CompressDisableZstd,
	} {
		wg.Add(1)
		go func(flag CompressFlag) {
			defer wg.Done()
			for _, codecs := range [][]CompressionCodec{
				{{codec: 0}},
				{{codec: 1}},
				{{codec: 2}},
				{{codec: 3}},
				{{codec: 4}},
				{{codec: 4}, {codec: 3}},
			} {
				c, _ := DefaultCompressor(codecs...)
				if c == nil {
					if codecs[0].codec == 0 {
						continue
					}
					t.Errorf("unexpected nil compressor from codecs %v", codecs)
				}
				for range 3 {
					wg.Add(1)
					go func() {
						w := byteBuffers.Get().(*bytes.Buffer)
						defer wg.Done()
						defer byteBuffers.Put(w)
						for _, in := range inputs {
							w.Reset()

							got, used := c.Compress(w, in, flag)
							got, err := d.Decompress(got, used)
							if err != nil {
								t.Errorf("unexpected decompress err: %v", err)
								return
							}
							if !bytes.Equal(got, in) {
								t.Errorf("got decompress %s != exp compress in %s", got, in)
							}
						}
					}()
				}
			}
		}(flag)
	}
	wg.Wait()
}

// Tests that a small compressed input expanding past the decompressed-size
// bound is rejected rather than materialized (decompression bomb). The wire
// fetch limits bound only compressed bytes; pre-fix every arm of this test
// succeeded, and in production zstd accepted up to the library-default
// 64 GiB while gzip/lz4 had no bound at all.
func TestDecompressBombBounded(t *testing.T) {
	// Not parallel: overrides maxDecompressedSize.
	old := maxDecompressedSize
	defer func() { maxDecompressedSize = old }()
	maxDecompressedSize = 1 << 20

	huge := make([]byte, 8<<20) // zeros compress tightly under every codec
	for _, codec := range []CompressionCodecType{CodecGzip, CodecSnappy, CodecLz4, CodecZstd} {
		c, err := DefaultCompressor(CompressionCodec{codec: codec})
		if err != nil {
			t.Fatalf("codec %d: unexpected compressor err: %v", codec, err)
		}
		w := new(bytes.Buffer)
		compressed, used := c.Compress(w, huge)
		if used != codec {
			t.Fatalf("codec %d: compressed with %d", codec, used)
		}
		if _, err := DefaultDecompressor().Decompress(compressed, codec); err == nil {
			t.Errorf("codec %d: decompressing an 8MiB-decoded batch under a 1MiB bound unexpectedly succeeded", codec)
		}
	}

	// No false positives: inputs under the bound still round trip.
	small := bytes.Repeat([]byte("abc123 "), 512)
	for _, codec := range []CompressionCodecType{CodecGzip, CodecSnappy, CodecLz4, CodecZstd} {
		c, _ := DefaultCompressor(CompressionCodec{codec: codec})
		w := new(bytes.Buffer)
		compressed, used := c.Compress(w, small)
		got, err := DefaultDecompressor().Decompress(compressed, used)
		if err != nil {
			t.Errorf("codec %d: unexpected decompress err: %v", codec, err)
		} else if !bytes.Equal(got, small) {
			t.Errorf("codec %d: round trip mismatch", codec)
		}
	}

	// Xerial-framed snappy accumulates chunks; the cumulative output must
	// respect the bound even when each chunk individually is under it.
	chunk := s2.EncodeSnappy(nil, make([]byte, 768<<10))
	xer := append([]byte{}, xerialPfx...)
	xer = append(xer, make([]byte, 8)...) // version + compat fields
	for range 2 {
		xer = binary.BigEndian.AppendUint32(xer, uint32(len(chunk)))
		xer = append(xer, chunk...)
	}
	if _, err := DefaultDecompressor().Decompress(xer, CodecSnappy); err == nil {
		t.Error("xerial chunks summing past the bound unexpectedly succeeded")
	}
}

// decompressPool hands out a slice of the configured length and capacity
// filled with 0xAA and records what is put back.
type decompressPool struct {
	len, cap int
	given    []byte
	puts     [][]byte
}

func (p *decompressPool) GetDecompressBytes([]byte, CompressionCodecType) []byte {
	p.given = bytes.Repeat([]byte{0xAA}, p.cap)[:p.len]
	return p.given
}

func (p *decompressPool) PutDecompressBytes(b []byte) { p.puts = append(p.puts, b) }

// TestDecompressPools pins what Decompress promises, for every codec, with
// and without a PoolDecompressBytes. With a pool, output starts at index 0
// of the pool's slice whatever its length, is decoded in place when the
// capacity suffices and is the grown slice otherwise, and Put is never
// called on success since only Recycle may. When decoding fails no record
// will ever Recycle, so the pool's slice is put back at once, zeroed
// through its capacity. With no pool, snappy and xerial return a fresh
// allocation and gzip and lz4 a clone; only zstd returns the decoder's own
// allocation, so that decoder is held and decoded on twice to pin that it
// does not write into an earlier result.
func TestDecompressPools(t *testing.T) {
	t.Parallel()
	inA := bytes.Repeat([]byte("batch A 0123456789 "), 4000)
	inB := bytes.Repeat([]byte("batch B abcdefghij "), 5000)
	inputsA, inputsB := codecInputs(t, inA), codecInputs(t, inB)
	garbage := bytes.Repeat([]byte{0xff, 0x00, 0x13, 0x37}, 8)
	for _, pt := range []struct {
		name     string
		len, cap int // the pool's slice; cap 0 means no pool
		corrupt  bool
	}{
		{name: "nopool"},
		{name: "fits", cap: 2 * len(inA)},
		{name: "fits-with-len", len: 64, cap: 2 * len(inA)},
		{name: "too-small", cap: 16},
		{name: "corrupt", cap: 2 * len(inA), corrupt: true},
	} {
		for i, tc := range inputsA {
			name := pt.name + "/" + tc.name
			pool := &decompressPool{len: pt.len, cap: pt.cap}
			var pools []Pool
			if pt.cap > 0 {
				pools = append(pools, pool)
			}
			d := DefaultDecompressor(pools...)

			if pt.corrupt {
				src := garbage
				if tc.name == "snappy-xerial" {
					src = append(append([]byte{}, xerialPfx...), garbage...)
				}
				if _, err := d.Decompress(src, tc.codec); err == nil {
					t.Errorf("%s: corrupt input unexpectedly decompressed", name)
				} else if len(pool.puts) != 1 || len(pool.puts[0]) != pt.len || &pool.puts[0][:1][0] != &pool.given[:1][0] {
					t.Errorf("%s: pool did not get its slice back after the failure (%d puts)", name, len(pool.puts))
				} else if i := bytes.IndexFunc(pool.puts[0][:pt.cap], func(r rune) bool { return r != 0 }); i >= 0 {
					t.Errorf("%s: byte %d not zeroed before put", name, i)
				}
				continue
			}

			got, err := d.Decompress(tc.src, tc.codec)
			if err != nil {
				t.Errorf("%s: unexpected decompress err: %v", name, err)
				continue
			}
			if !bytes.Equal(got, inA) {
				t.Errorf("%s: decompressed data corrupted (len %d, exp %d)", name, len(got), len(inA))
				continue
			}
			if pt.cap > 0 {
				if len(pool.puts) != 0 {
					t.Errorf("%s: Decompress called Put %d times on success", name, len(pool.puts))
				}
				if inPool := &got[0] == &pool.given[:1][0]; inPool != (pt.cap >= len(inA)) {
					t.Errorf("%s: decoded into the pool's slice: %v, want %v", name, inPool, !inPool)
				}
			} else if tc.codec == CodecZstd {
				zd := d.(*decompressor).unzstdPool.Get().(*zstdDecoder)
				gotA, err := zd.inner.DecodeAll(tc.src, nil)
				if err != nil || !bytes.Equal(gotA, inA) {
					t.Fatalf("%s: first batch: err %v", name, err)
				}
				if gotB, err := zd.inner.DecodeAll(inputsB[i].src, nil); err != nil || !bytes.Equal(gotB, inB) {
					t.Errorf("%s: second batch: err %v", name, err)
				} else if !bytes.Equal(gotA, inA) {
					t.Errorf("%s: first batch modified by a later decode", name)
				}
			}
		}
	}
}

// A zstd frame whose header claims a huge content size must be rejected by
// the decoder's content-size bound. Pre-fix the decoder accepted claims up
// to the library-default 64 GiB and this truncated frame failed with a
// generic EOF error only because no blocks follow the header; a
// non-truncated bomb materialized in full. The frame deliberately carries
// a small window descriptor so the claim is caught by the size bound, not
// the unrelated window-size check.
func TestDecompressZstdHugeClaim(t *testing.T) {
	t.Parallel()
	frame := []byte{0x28, 0xb5, 0x2f, 0xfd} // zstd magic, little endian
	// frame header descriptor 0xc0 (8-byte frame content size, not single
	// segment), then window descriptor 0x00 (1 KiB).
	frame = append(frame, 0xc0, 0x00)
	frame = binary.LittleEndian.AppendUint64(frame, 8<<30)
	_, err := DefaultDecompressor().Decompress(frame, CodecZstd)
	if !errors.Is(err, zstd.ErrDecoderSizeExceeded) {
		t.Errorf("got err %v != exp zstd.ErrDecoderSizeExceeded", err)
	}
}

func BenchmarkCompress(b *testing.B) {
	in := bytes.Repeat([]byte("abcdefghijklmno pqrs tuvwxy   z"), 100)
	for _, codec := range []CompressionCodecType{CodecGzip, CodecSnappy, CodecLz4, CodecZstd} {
		c, _ := DefaultCompressor(CompressionCodec{codec: codec})
		b.Run(fmt.Sprint(codec), func(b *testing.B) {
			var afterSize int
			for i := 0; i < b.N; i++ {
				w := byteBuffers.Get().(*bytes.Buffer)
				w.Reset()
				after, _ := c.Compress(w, in, 99)
				afterSize = len(after)
				byteBuffers.Put(w)
			}
			b.Logf("%d => %d", len(in), afterSize)
		})
	}
}

type codecInput struct {
	name  string
	codec CompressionCodecType
	src   []byte
}

// codecInputs compresses in under every codec, plus xerial framed snappy
// (what the Java client writes) and a streamed zstd frame with no content
// size in its header (what Java's zstd writer produces). The zstd writer
// is flushed midway so the frame header goes out before the size is known.
func codecInputs(tb testing.TB, in []byte) []codecInput {
	tb.Helper()
	inputs := []codecInput{
		{name: "gzip", codec: CodecGzip},
		{name: "snappy", codec: CodecSnappy},
		{name: "lz4", codec: CodecLz4},
		{name: "zstd", codec: CodecZstd},
	}
	for i := range inputs {
		c, err := DefaultCompressor(CompressionCodec{codec: inputs[i].codec})
		if err != nil {
			tb.Fatalf("%s: unexpected compressor err: %v", inputs[i].name, err)
		}
		var used CompressionCodecType
		inputs[i].src, used = c.Compress(new(bytes.Buffer), in)
		if used != inputs[i].codec {
			tb.Fatalf("%s: compressed with %d", inputs[i].name, used)
		}
	}
	var zstream bytes.Buffer
	zw, err := zstd.NewWriter(&zstream)
	if err != nil {
		tb.Fatalf("zstd writer: %v", err)
	}
	if _, err := zw.Write(in[:len(in)/2]); err != nil {
		tb.Fatalf("zstd write: %v", err)
	}
	if err := zw.Flush(); err != nil {
		tb.Fatalf("zstd flush: %v", err)
	}
	if _, err := zw.Write(in[len(in)/2:]); err != nil {
		tb.Fatalf("zstd write: %v", err)
	}
	if err := zw.Close(); err != nil {
		tb.Fatalf("zstd close: %v", err)
	}
	return append(inputs,
		codecInput{name: "snappy-xerial", codec: CodecSnappy, src: xerialFrame(in, 32<<10)},
		codecInput{name: "zstd-stream", codec: CodecZstd, src: zstream.Bytes()},
	)
}

// xerialFrame frames in the way the Java snappy stream does: a header,
// then per chunk a big endian length and a raw snappy block.
func xerialFrame(in []byte, chunkSize int) []byte {
	xer := append([]byte{}, xerialPfx...)
	xer = append(xer, make([]byte, 8)...) // version + compat fields
	for len(in) > 0 {
		n := min(chunkSize, len(in))
		chunk := s2.EncodeSnappy(nil, in[:n])
		xer = binary.BigEndian.AppendUint32(xer, uint32(len(chunk)))
		xer = append(xer, chunk...)
		in = in[n:]
	}
	return xer
}

// benchDecompressPool implements PoolDecompressBytes with a pre-allocated
// buffer, representative of a real pool that avoids per-call allocation.
// Single-goroutine only (the benchmark does not run sub-benchmarks in
// parallel).
type benchDecompressPool struct {
	buf []byte
}

func (p *benchDecompressPool) GetDecompressBytes([]byte, CompressionCodecType) []byte {
	return p.buf[:0]
}

func (*benchDecompressPool) PutDecompressBytes([]byte) {}

func BenchmarkDecompress(b *testing.B) {
	in := bytes.Repeat([]byte("abcdefghijklmno pqrs tuvwxy   z"), 10_000)
	for _, tc := range codecInputs(b, in) {
		pool := &benchDecompressPool{buf: make([]byte, 0, len(in)*2)}
		b.Run(tc.name+"/pool", func(b *testing.B) {
			d := DefaultDecompressor(pool)
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := d.Decompress(tc.src, tc.codec); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run(tc.name+"/nopool", func(b *testing.B) {
			d := DefaultDecompressor()
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				if _, err := d.Decompress(tc.src, tc.codec); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

func Test_xerialDecode(t *testing.T) {
	tests := []struct {
		name            string
		compressedInput string
		want            []byte
		wantErr         bool
	}{
		{
			"Compressed data ok",
			"glNOQVBQWQAAAAABAAAAAQAAAA8NMEhlbGxvLCBXb3JsZCE=",
			[]byte("Hello, World!"),
			false,
		},
		{
			"Compressed data without header",
			"/wYAAHNOYVBwWQERAACChVPDSGVsbG8sIFdvcmxkIQ==",
			nil,
			true,
		},
		{
			"Compressed data less than minimum length, malformed",
			"glNOQVBQWQAAAAABAAAAAQAAAA==",
			nil,
			true,
		},
		{
			"Compressed data not the advertised length",
			"glNOQVBQWQAAAAABAAAAAQAAAA8NMEhlbGxvLCBXb3Js",
			nil,
			true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := base64.StdEncoding.DecodeString(test.compressedInput)
			if err != nil {
				t.Errorf("base64 decode error = %v", err)
				return
			}
			got, err := xerialDecode(nil, data)
			if (err != nil) != test.wantErr {
				t.Errorf("xerialDecode() error = %v, wantErr %v", err, test.wantErr)
				return
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("got decompress %s != exp compress in %s", got, test.want)
			}
		})
	}
}
