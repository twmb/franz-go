// Package compress provides Kafka (de)compressors.
package compress

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

type Codec int8 // 1: gzip, 2: snappy, 3: lz4, 4: zstd

type Compressor struct {
	primary   Codec
	secondary Codec
	zstdOnce  sync.Once
	zstdEnc   *zstd.Encoder
	gzPool    sync.Pool
	lz4Pool   sync.Pool
	bufs      sync.Pool
}

func NewCompressor(codecs ...Codec) (*Compressor, error) {
	if len(codecs) == 0 {
		return nil, errors.New("missing compression codec")
	}

	used := make(map[Codec]bool)
	var keepIdx int
	for _, codec := range codecs {
		if _, exists := used[codec]; exists {
			continue
		}
		used[codec] = true
		codecs[keepIdx] = codec
		keepIdx++
	}
	codecs = codecs[:keepIdx]

	if codecs[0] < 0 || codecs[0] > 4 ||
		len(codecs) > 1 && (codecs[1] < 0 || codecs[1] > 4) {
		return nil, errors.New("unknown compression codec")
	}
	if codecs[0] == 0 {
		return nil, nil
	}
	if len(codecs) == 1 {
		codecs = append(codecs, -1) // not using a second codec, see below
	}

	return &Compressor{
		primary:   codecs[0],
		secondary: codecs[1],
		gzPool: sync.Pool{
			New: func() interface{} { return gzip.NewWriter(nil) },
		},
		lz4Pool: sync.Pool{
			New: func() interface{} { return new(lz4.Writer) },
		},
		bufs: sync.Pool{
			New: func() interface{} { return &Buf{make([]byte, 0, 1<<10)} },
		},
	}, nil
}

// GetBuf returns a buffer to use for compressing. Compressing returns the
// contents of this buffer if the compression was valid; PutBuf should only be
// called once the Compress return is not being used anymore.
func (c *Compressor) GetBuf() *Buf { return c.bufs.Get().(*Buf) }

// PutBuf puts a buffer returned from GetBuf back into the compressor's pool.
func (c *Compressor) PutBuf(b *Buf) { c.bufs.Put(b) }

// Buf is a simple writer on a slice.
type Buf struct{ inner []byte }

func (b *Buf) Write(p []byte) (int, error) { b.inner = append(b.inner, p...); return len(p), nil }

// Compress compresses src to buf, returning buf's inner slice once done or nil
// if an error is encountered.
//
// The Buf should be put back to the compressor's pool after the returned slice
// is done being used.
func (c *Compressor) Compress(dst *Buf, src []byte, produceRequestVersion int16) ([]byte, int8) {
	dst.inner = dst.inner[:0]

	use := c.primary
	if use == 4 && produceRequestVersion < 7 {
		use = c.secondary
	}

	switch use {
	case -1: // no secondary decoder
		return nil, -1
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
		c.zstdOnce.Do(func() {
			c.zstdEnc, _ = zstd.NewWriter(nil)
		})
		dst.inner = c.zstdEnc.EncodeAll(src, dst.inner)
	}

	return dst.inner, int8(use)
}

func (c *Compressor) Close() {
	c.zstdOnce.Do(func() {})
	if c.zstdEnc != nil {
		c.zstdEnc.Close()
	}
}

type Decompressor struct {
	zstdOnce  sync.Once
	zstdDec   *zstd.Decoder
	ungzPool  sync.Pool
	unlz4Pool sync.Pool
}

func NewDecompressor() *Decompressor {
	d := &Decompressor{
		ungzPool: sync.Pool{
			New: func() interface{} { return new(gzip.Reader) },
		},
		unlz4Pool: sync.Pool{
			New: func() interface{} { return new(lz4.Reader) },
		},
	}
	return d
}

func (d *Decompressor) Decompress(src []byte, codec Codec) ([]byte, error) {
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
		d.zstdOnce.Do(func() {
			d.zstdDec, _ = zstd.NewReader(nil)
		})
		return d.zstdDec.DecodeAll(src, nil)
	default:
		return nil, errors.New("unknown compression codec")
	}
}

func (d *Decompressor) Close() {
	d.zstdOnce.Do(func() {})
	if d.zstdDec != nil {
		d.zstdDec.Close()
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
		if chunk, err = snappy.Decode(chunk[:cap(chunk)], src[:size]); err != nil {
			return nil, err
		}
		src = src[size:]
		dst = append(dst, chunk...)
	}
	return dst, nil
}
