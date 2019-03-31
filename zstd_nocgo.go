// +build !cgo

package kgo

var hasZstd = false

func newZstdWriter(level int) codecCompressor {
	return nil
}
