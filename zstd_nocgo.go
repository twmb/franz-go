// +build !cgo

package kgo

import "errors"

var hasZstd = false

func newZstdWriter(level int) codecCompressor {
	return nil
}

func unzstd(src []byte) ([]byte, error) {
	return nil, errors.New("zstd decoding not supported")
}
