package kgo

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"errors"
	"io/ioutil"
	"sync"

	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4"
)

// NOTE: level configuration was removed at some point due to it likely being
// more configuration than necessary; we may add level options as new functions
// down the line. The code below supports levels; zstd levels will need wiring
// in and levels will need validating.

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
	options []int8
	zstdEnc *zstd.Encoder
	gzPool  sync.Pool
	lz4Pool sync.Pool
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
			level := codec.level
			if _, err := gzip.NewWriterLevel(nil, int(level)); err != nil {
				level = gzip.DefaultCompression
			}
			c.gzPool = sync.Pool{New: func() interface{} { c, _ := gzip.NewWriterLevel(nil, int(level)); return c }}
		case 3:
			level := codec.level
			if level < 0 {
				level = 0
			}
			c.lz4Pool = sync.Pool{New: func() interface{} { w := new(lz4.Writer); w.Header.CompressionLevel = int(level); return w }}
		case 4:
			level := zstd.EncoderLevel(codec.level)
			zstdEnc, err := zstd.NewWriter(nil, zstd.WithEncoderLevel(level))
			if err != nil {
				zstdEnc, _ = zstd.NewWriter(nil)
			}
			c.zstdEnc = zstdEnc
		}
	}

	if c.options[0] == 0 {
		return nil, nil // first codec was passthrough
	}

	return c, nil
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
		dst.inner = snappy.Encode(dst.inner[:cap(dst.inner)], src)

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
		dst.inner = c.zstdEnc.EncodeAll(src, dst.inner)
	}

	return dst.inner, int8(use)
}

func (c *compressor) close() {
	if c != nil && c.zstdEnc != nil {
		c.zstdEnc.Close()
	}
}

type decompressor struct {
	zstdOnce  sync.Once
	zstdDec   *zstd.Decoder
	ungzPool  sync.Pool
	unlz4Pool sync.Pool
}

func newDecompressor() *decompressor {
	d := &decompressor{
		ungzPool: sync.Pool{
			New: func() interface{} { return new(gzip.Reader) },
		},
		unlz4Pool: sync.Pool{
			New: func() interface{} { return new(lz4.Reader) },
		},
	}
	return d
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
		return snappy.Decode(nil, src)
	case 3:
		unlz4 := d.unlz4Pool.Get().(*lz4.Reader)
		defer d.unlz4Pool.Put(unlz4)
		unlz4.Reset(bytes.NewReader(src))
		return ioutil.ReadAll(unlz4)
	case 4:
		d.zstdOnce.Do(func() { d.zstdDec, _ = zstd.NewReader(nil) })
		return d.zstdDec.DecodeAll(src, nil)
	default:
		return nil, errors.New("unknown compression codec")
	}
}

func (d *decompressor) close() {
	if d == nil {
		return
	}
	// We must initialize zstdDec here, otherwise a concurrent decompress
	// may see nil. Alternatively, we could nil check above, but we favor
	// doing less in the hotter code path.
	d.zstdOnce.Do(func() { d.zstdDec, _ = zstd.NewReader(nil) })
	d.zstdDec.Close()
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
		if chunk, err = snappy.Decode(chunk[:cap(chunk)], src[:size]); err != nil {
			return nil, err
		}
		src = src[size:]
		dst = append(dst, chunk...)
	}
	return dst, nil
}
