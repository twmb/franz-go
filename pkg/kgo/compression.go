package kgo

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io/ioutil"
	"runtime"
	"sync"

	"github.com/klauspost/compress/s2"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4/v4"
)

// sliceWriter a reusable slice as an io.Writer
type sliceWriter struct{ inner []byte }

func (s *sliceWriter) Write(p []byte) (int, error) {
	s.inner = append(s.inner, p...)
	return len(p), nil
}

var sliceWriters = sync.Pool{New: func() interface{} { r := make([]byte, 8<<10); return &sliceWriter{inner: r} }}

// CompressionCodec configures how records are compressed before being sent.
//
// Records are compressed within individual topics and partitions, inside of a
// RecordBatch. All records in a RecordBatch are compressed into one record
// for that batch.
type CompressionCodec struct {
	codec int8 // 1: gzip, 2: snappy, 3: lz4, 4: zstd
	level int8
}

// NoCompression is a compression option that avoids compression. This can
// always be used as a fallback compression.
func NoCompression() CompressionCodec { return CompressionCodec{0, 0} }

// GzipCompression enables gzip compression with the default compression level.
func GzipCompression() CompressionCodec { return CompressionCodec{1, gzip.DefaultCompression} }

// SnappyCompression enables snappy compression.
func SnappyCompression() CompressionCodec { return CompressionCodec{2, 0} }

// Lz4Compression enables lz4 compression with the fastest compression level.
func Lz4Compression() CompressionCodec { return CompressionCodec{3, 0} }

// ZstdCompression enables zstd compression with the default compression level.
func ZstdCompression() CompressionCodec { return CompressionCodec{4, 0} }

// WithLevel changes the compression codec's "level", effectively allowing for
// higher or lower compression ratios at the expense of CPU speed.
//
// For the zstd package, the level is a typed int; simply convert the type back
// to an int for this function.
//
// If the level is invalid, compressors just use a default level.
func (c CompressionCodec) WithLevel(level int) CompressionCodec {
	if level > 127 {
		level = 127 // lz4 could theoretically be large, I guess
	}
	c.level = int8(level)
	return c
}

type compressor struct {
	options  []int8
	gzPool   sync.Pool
	lz4Pool  sync.Pool
	zstdPool sync.Pool
}

func newCompressor(codecs ...CompressionCodec) (*compressor, error) {
	if len(codecs) == 0 {
		return nil, nil
	}

	used := make(map[int8]bool) // we keep one type of codec per CompressionCodec
	var keepIdx int
	for _, codec := range codecs {
		if _, exists := used[codec.codec]; exists {
			continue
		}
		used[codec.codec] = true
		codecs[keepIdx] = codec
		keepIdx++
	}
	codecs = codecs[:keepIdx]

	for _, codec := range codecs {
		if codec.codec < 0 || codec.codec > 4 {
			return nil, errors.New("unknown compression codec")
		}
	}

	c := new(compressor)

out:
	for _, codec := range codecs {
		c.options = append(c.options, codec.codec)
		switch codec.codec {
		case 0:
			break out
		case 1:
			level := gzip.DefaultCompression
			if codec.level != 0 {
				if _, err := gzip.NewWriterLevel(nil, int(codec.level)); err != nil {
					level = int(codec.level)
				}
			}
			c.gzPool = sync.Pool{New: func() interface{} { c, _ := gzip.NewWriterLevel(nil, level); return c }}
		case 3:
			level := codec.level
			if level < 0 {
				level = 0 // 0 == lz4.Fast
			}
			fn := func() interface{} { return lz4.NewWriter(new(bytes.Buffer)) }
			w := lz4.NewWriter(new(bytes.Buffer))
			if err := w.Apply(lz4.CompressionLevelOption(lz4.CompressionLevel(level))); err == nil {
				fn = func() interface{} {
					w := lz4.NewWriter(new(bytes.Buffer))
					w.Apply(lz4.CompressionLevelOption(lz4.CompressionLevel(level)))
					return w
				}
			}
			w.Close()
			c.lz4Pool = sync.Pool{New: fn}
		case 4:
			opts := []zstd.EOption{
				zstd.WithWindowSize(64 << 10),
				zstd.WithEncoderConcurrency(1),
				zstd.WithZeroFrames(true),
			}
			fn := func() interface{} {
				zstdEnc, _ := zstd.NewWriter(nil, opts...)
				r := &zstdEncoder{zstdEnc}
				runtime.SetFinalizer(r, func(r *zstdEncoder) { r.inner.Close() })
				return r
			}
			zstdEnc, err := zstd.NewWriter(nil, append(opts, zstd.WithEncoderLevel(zstd.EncoderLevel(codec.level)))...)
			if err == nil {
				zstdEnc.Close()
				opts = append(opts, zstd.WithEncoderLevel(zstd.EncoderLevel(codec.level)))
			}
			c.zstdPool = sync.Pool{New: fn}
		}
	}

	if c.options[0] == 0 {
		return nil, nil // first codec was passthrough
	}

	return c, nil
}

type zstdEncoder struct {
	inner *zstd.Encoder
}

// Compress compresses src to buf, returning buf's inner slice once done or nil
// if an error is encountered.
//
// The writer should be put back to its pool after the returned slice is done
// being used.
func (c *compressor) compress(dst *sliceWriter, src []byte, produceRequestVersion int16) ([]byte, int8) {
	dst.inner = dst.inner[:0]

	var use int8
	for _, option := range c.options {
		if option == 4 && produceRequestVersion < 7 {
			continue
		}
		use = option
		break
	}

	switch use {
	case 0:
		return src, 0
	case 1:
		gz := c.gzPool.Get().(*gzip.Writer)
		defer c.gzPool.Put(gz)
		gz.Reset(dst)
		if _, err := gz.Write(src); err != nil {
			return nil, -1
		}
		if err := gz.Close(); err != nil {
			return nil, -1
		}

	case 2:
		dst.inner = s2.EncodeSnappy(dst.inner[:cap(dst.inner)], src)

	case 3:
		lz := c.lz4Pool.Get().(*lz4.Writer)
		defer c.lz4Pool.Put(lz)
		lz.Reset(dst)
		if _, err := lz.Write(src); err != nil {
			return nil, -1
		}
		if err := lz.Close(); err != nil {
			return nil, -1
		}
	case 4:
		zstdEnc := c.zstdPool.Get().(*zstdEncoder)
		defer c.zstdPool.Put(zstdEnc)
		dst.inner = zstdEnc.inner.EncodeAll(src, dst.inner)
	}

	return dst.inner, use
}

type decompressor struct {
	ungzPool   sync.Pool
	unlz4Pool  sync.Pool
	unzstdPool sync.Pool
}

func newDecompressor() *decompressor {
	d := &decompressor{
		ungzPool: sync.Pool{
			New: func() interface{} { return new(gzip.Reader) },
		},
		unlz4Pool: sync.Pool{
			New: func() interface{} { return lz4.NewReader(nil) },
		},
		unzstdPool: sync.Pool{
			New: func() interface{} {
				zstdDec, _ := zstd.NewReader(nil,
					zstd.WithDecoderLowmem(true),
					zstd.WithDecoderConcurrency(1),
				)
				r := &zstdDecoder{zstdDec}
				runtime.SetFinalizer(r, func(r *zstdDecoder) {
					r.inner.Close()
				})
				return r
			},
		},
	}
	return d
}

type zstdDecoder struct {
	inner *zstd.Decoder
}

func (d *decompressor) decompress(src []byte, codec byte) ([]byte, error) {
	switch codec {
	case 0:
		return src, nil
	case 1:
		ungz := d.ungzPool.Get().(*gzip.Reader)
		defer d.ungzPool.Put(ungz)
		if err := ungz.Reset(bytes.NewReader(src)); err != nil {
			return nil, err
		}
		return ioutil.ReadAll(ungz)
	case 2:
		if len(src) > 16 && bytes.HasPrefix(src, xerialPfx) {
			return xerialDecode(src)
		}
		return s2.Decode(nil, src)
	case 3:
		unlz4 := d.unlz4Pool.Get().(*lz4.Reader)
		defer d.unlz4Pool.Put(unlz4)
		unlz4.Reset(bytes.NewReader(src))
		return ioutil.ReadAll(unlz4)
	case 4:
		unzstd := d.unzstdPool.Get().(*zstdDecoder)
		defer d.unzstdPool.Put(unzstd)
		return unzstd.inner.DecodeAll(src, nil)
	default:
		return nil, errors.New("unknown compression codec")
	}
}

var xerialPfx = []byte{130, 83, 78, 65, 80, 80, 89, 0}

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
		if chunk, err = s2.Decode(chunk[:cap(chunk)], src[:size]); err != nil {
			return nil, err
		}
		src = src[size:]
		dst = append(dst, chunk...)
	}
	return dst, nil
}
