package kgo

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io"
	"io/ioutil"
	"sync"
	"sync/atomic"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4"
)

// NOTE: level configuration was removed at some point due to it likely being
// more configuration than necessary; we may add level options as new functions
// down the line. The code below supports levels; zstd levels will need wiring
// in and levels will need validating.

// compressorsMu guards concurrent writes to compressors.
var compressorMu sync.Mutex

// compressors is an atomic storing mappings of codecs to their compressors.
// Since the set of codecs will likely not change after the initial few, we go
// through the effort of making this an atomic.Value.
var compressors atomic.Value

func init() {
	compressors.Store(map[CompressionCodec]*compressor{
		CompressionCodec{}: nil, // required default
	})
}

var tozstd, _ = zstd.NewWriter(nil)

type zstdCompressor struct {
	dst io.Writer // always a *sliceWriter
}

func (z *zstdCompressor) Reset(dst io.Writer) { z.dst = dst }
func (z *zstdCompressor) Write(p []byte) (int, error) {
	sw := z.dst.(*sliceWriter)
	sw.base = tozstd.EncodeAll(p, sw.base[:0])
	return len(p), nil
}
func (z *zstdCompressor) Close() error { return nil }

type snappyCompressor struct {
	dst io.Writer // always a *sliceWriter
}

func (s *snappyCompressor) Reset(dst io.Writer) { s.dst = dst }
func (s *snappyCompressor) Write(p []byte) (int, error) {
	sw := s.dst.(*sliceWriter)
	sw.base = snappy.Encode(sw.base[:0], p)
	return len(p), nil
}
func (s *snappyCompressor) Close() error { return nil }

// codecCompressor a shoddy compression interface. Some compressors can just
// compress a blob of data, others are stream only (gzip, lz4).
//
// This interface is provided to satisfy all compressors; the order of
// calls is always Reset, Write, Close, once each, and the reset is always
// passed a *sliceWriter.
//
// Compressors can use this to provide a streaming interface while still just
// doing one more efficient block compress.
type codecCompressor interface {
	Reset(io.Writer)
	io.Writer
	Close() error
}

// sliceWriter a reusable slice as an io.Writer
type sliceWriter struct{ base []byte }

func (s *sliceWriter) reset()                      { s.base = s.base[:0] }
func (s *sliceWriter) get() []byte                 { return s.base }
func (s *sliceWriter) Write(p []byte) (int, error) { s.base = append(s.base, p...); return len(p), nil }

// zipr owns a slice writer and the compressor that will write to it.
type zipr struct {
	base sliceWriter
	cc   codecCompressor
}

// compressor pairs ziprs and the attributes that specify the zipr kind.
type compressor struct {
	pool  sync.Pool
	attrs int8
}

// compress returns the compressed form of in, or nil on error.
//
// The returned slice will be reused and cannot be saved.
func (z *zipr) compress(in []byte) []byte {
	z.base.reset()
	z.cc.Reset(&z.base)
	if _, err := z.cc.Write(in); err != nil {
		return nil
	}
	if err := z.cc.Close(); err != nil {
		return nil
	}
	return z.base.get()
}

// getZipr returns a zipr for this compressor.
func (c *compressor) getZipr() *zipr {
	return c.pool.Get().(*zipr)
}

// putZipr puts a zipr back to this compressor for reuse.
//
// Any slice returned from the compressor must be done being used.
func (c *compressor) putZipr(z *zipr) {
	c.pool.Put(z)
}

// loadProduceCompressor returns a compressor based on the given codecs and
// produce request version.
//
// The input codecs must have been validated.
func loadProduceCompressor(codecs []CompressionCodec, produceRequestVersion int16) *compressor {
	for _, codec := range codecs {
		if codec.codec == 4 && (produceRequestVersion < 7) {
			continue
		}

		cs := compressors.Load().(map[CompressionCodec]*compressor)
		c, exists := cs[codec]
		if !exists {
			compressorMu.Lock()
			cs = compressors.Load().(map[CompressionCodec]*compressor)
			c, exists = cs[codec]
			if !exists {
				// Codec still does not exist and we own the
				// compressor write lock: we will be the
				// creator for this codec.
				var n func() interface{}

				// level must be valid by here; the gzip writer
				// will panic otherwise.
				level := codec.level
				switch codec.codec {
				case 1:
					n = func() interface{} { cc, _ := gzip.NewWriterLevel(nil, int(level)); return &zipr{cc: cc} }
				case 2:
					n = func() interface{} { return &zipr{cc: new(snappyCompressor)} }
				case 3:
					n = func() interface{} { return &zipr{cc: new(lz4.Writer)} }
				case 4:
					n = func() interface{} { return &zipr{cc: new(zstdCompressor)} }
				}

				// Create the next codec map, copying all the
				// existing codecs.
				next := make(map[CompressionCodec]*compressor, len(cs)+1)
				for codec, c := range cs {
					next[codec] = c
				}
				// Finally, assign our new codec and save the
				// updated map.
				c = &compressor{
					pool:  sync.Pool{New: n},
					attrs: int8(codec.codec),
				}
				next[codec] = c
				compressors.Store(next)
			}
			compressorMu.Unlock()
		}
		return c
	}
	return nil
}

// CompressionCodec configures how records are compressed before being sent.
//
// Records are compressed within individual topics and partitions, inside of a
// RecordBatch. All records in a RecordBatch are compressed into one record
// for that batch.
type CompressionCodec struct {
	codec int8 // 1: gzip, 2: snappy, 3: lz4, 4: zstd
	level int8
}

// NoCompression is the default compression used for messages and can be used
// as a fallback compression option.
func NoCompression() CompressionCodec { return CompressionCodec{0, 0} }

// GzipCompression enables gzip compression with the default compression level.
func GzipCompression() CompressionCodec { return CompressionCodec{1, gzip.DefaultCompression} }

// SnappyCompression enables snappy compression.
func SnappyCompression() CompressionCodec { return CompressionCodec{2, 0} }

// Lz4Compression enables lz4 compression with the fastest compression level.
func Lz4Compression() CompressionCodec { return CompressionCodec{3, 0} }

// ZstdCompression enables zstd compression with the default compression level.
func ZstdCompression() CompressionCodec { return CompressionCodec{4, 0} }

var ungzPool = sync.Pool{
	New: func() interface{} { return new(gzip.Reader) },
}

var unlz4Pool = sync.Pool{
	New: func() interface{} { return new(lz4.Reader) },
}

var unzstd, _ = zstd.NewReader(nil)

var xerialPfx = []byte{130, 83, 78, 65, 80, 80, 89, 0}

func decompress(src []byte, codec byte) ([]byte, error) {
	switch codec {
	case 0:
		return src, nil

	case 1:
		ungz := ungzPool.Get().(*gzip.Reader)
		defer ungzPool.Put(ungz)
		if err := ungz.Reset(bytes.NewReader(src)); err != nil {
			return nil, err
		}
		return ioutil.ReadAll(ungz)

	case 2:
		if len(src) > 16 && bytes.HasPrefix(src, xerialPfx) {
			return xerialDecode(src)
		}
		return snappy.Decode(nil, src)

	case 3:
		unlz4 := unlz4Pool.Get().(*lz4.Reader)
		defer unlz4Pool.Put(unlz4)
		unlz4.Reset(bytes.NewReader(src))
		return ioutil.ReadAll(unlz4)

	case 4:
		return unzstd.DecodeAll(src, nil)

	default:
		return nil, errors.New("unknown compression codec")
	}
}

var errMalformedXerial = errors.New("malformed xerial framing")

func xerialDecode(src []byte) ([]byte, error) {
	// bytes 0-8: xerial header
	// bytes 8-16: xerial version
	// everything after: uint32 chunk size, snappy chunk
	// we come into this function knowing src is at least 16
	src = src[16:]
	var dst, chunk []byte
	var err error
	for len(src) > 0 {
		if len(src) < 4 {
			return nil, errMalformedXerial
		}
		size := int32(binary.BigEndian.Uint32(src))
		src = src[4:]
		if size < 0 || len(src) < int(size) {
			return nil, errMalformedXerial
		}
		if chunk, err = snappy.Decode(chunk[:cap(chunk)], src[:size]); err != nil {
			return nil, err
		}
		src = src[size:]
		dst = append(dst, chunk...)
	}
	return dst, nil
}
