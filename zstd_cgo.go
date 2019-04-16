// +build cgo

package kgo

import (
	"io"

	"github.com/DataDog/zstd"
)

var hasZstd = true

type zstdCompressor struct {
	dst   io.Writer // always a *sliceWriter
	level int
}

func (z *zstdCompressor) Reset(dst io.Writer) { z.dst = dst }
func (z *zstdCompressor) Write(p []byte) (int, error) {
	var err error
	sw := z.dst.(*sliceWriter)
	sw.base, err = zstd.CompressLevel(sw.base[:0], p, z.level)
	if err != nil {
		return 0, err
	}
	return len(p), nil
}
func (z *zstdCompressor) Close() error { return nil }

func newZstdWriter(level int) codecCompressor {
	return &zstdCompressor{level: level}
}

func unzstd(src []byte) ([]byte, error) {
	return zstd.Decompress(nil, src)
}
