package kgo

import (
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"

	"github.com/golang/snappy"
	"github.com/pierrec/lz4"
)

// compressorsMu guards concurrent writes to compressors.
var compressorMu sync.Mutex

// compressors is an atomic storing mappings of codecs to their compressors.
// Since the set of codecs will likely not change after the initial few, we go
// through the effort of making this an atomic.Value.
var compressors atomic.Value

func init() {
	compressors.Store(map[CompressionCodec]*compressor{
		CompressionCodec{0, 0}: nil,
	})
}

// Not quite sure why, but the snappy writer doesn't write messages the way
// Kafka wants.
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
		if codec.codec == 4 && (produceRequestVersion < 7 || !hasZstd) {
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
					n = func() interface{} { cc, _ := gzip.NewWriterLevel(nil, level); return &zipr{cc: cc} }
				case 2:
					n = func() interface{} { return &zipr{cc: new(snappyCompressor)} }
				case 3:
					n = func() interface{} { return &zipr{cc: &lz4.Writer{Header: lz4.Header{CompressionLevel: level}}} }
				case 4:
					n = func() interface{} { return &zipr{cc: newZstdWriter(level)} }
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
	codec int // 1: gzip, 2: snappy, 3: lz4, 4: zstd
	level int
}

func (c CompressionCodec) validate() error {
	// KIP-390
	var min, max int
	var name string
	switch c.codec {
	case 0:
		min, max, name = 0, 0, "no-compression"
	case 1:
		min, max, name = 0, 9, "gzip"
	case 2:
		min, max, name = 0, 0, "snappy"
	case 3:
		min, max, name = 0, 12, "lz4" // 12 is LZ4HC_CLEVEL_MAX in C
	case 4: // can be negative? but we will not support that for now
		min, max, name = 1, 22, "zstd"
	default:
		return errors.New("unknown compression codec")
	}
	if c.level < min || c.level > max {
		return fmt.Errorf("invalid %s compression level %d (min %d, max %d)", name, c.level, min, max)
	}
	return nil
}

// NoCompression is the default compression used for messages and can be used
// as a fallback compression option.
func NoCompression() CompressionCodec { return CompressionCodec{0, 0} }

// GzipCompression enables gzip compression. Level must be between 0 and 9.
func GzipCompression(level int) CompressionCodec { return CompressionCodec{1, level} }

// SnappyCompression enables snappy compression.
func SnappyCompression() CompressionCodec { return CompressionCodec{2, 0} }

// Lz4Compression enables lz4 compression. Level must be between 0 and 12.
func Lz4Compression(level int) CompressionCodec { return CompressionCodec{3, level} }

// ZstdCompression enables zstd compression. Level must be between 1 and 22.
func ZstdCompression(level int) CompressionCodec { return CompressionCodec{4, level} }
