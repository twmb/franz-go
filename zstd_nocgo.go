// +build !cgo

package kgo

// TODO https://github.com/klauspost/compress/tree/master/zstd

import "errors"

var hasZstd = false

func newZstdWriter(level int) codecCompressor {
	return nil
}

func unzstd(src []byte) ([]byte, error) {
	return nil, errors.New("zstd decoding not supported")
}
