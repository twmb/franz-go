// Package kbin contains Kafka primitive reading and writing functions.
package kbin

import (
	"encoding/binary"
	"errors"
	"math/bits"
)

// This file contains primitive type encoding and decoding.
//
// The Reader helper can be used even when content runs out
// or an error is hit; all other number requests will return
// zero so a decode will basically no-op.

var (
	// ErrNotEnoughData is returned when a type could not fully decode
	// from a slice because the slice did not have enough data.
	ErrNotEnoughData = errors.New("response did not contain enough data to be valid")

	// ErrTooMuchData is returned when there is leftover data in a slice.
	ErrTooMuchData = errors.New("response contained too much data to be valid")
)

func AppendBool(dst []byte, v bool) []byte {
	if v {
		return append(dst, 1)
	}
	return append(dst, 0)
}
func AppendInt8(dst []byte, i int8) []byte {
	return append(dst, byte(i))
}
func AppendInt16(dst []byte, i int16) []byte {
	u := uint16(i)
	return append(dst, byte(u>>8), byte(u))
}
func AppendInt32(dst []byte, i int32) []byte {
	return AppendUint32(dst, uint32(i))
}
func AppendInt64(dst []byte, i int64) []byte {
	u := uint64(i)
	return append(dst, byte(u>>56), byte(u>>48), byte(u>>40), byte(u>>32),
		byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}
func AppendUint32(dst []byte, u uint32) []byte {
	return append(dst, byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}

// varintLens could only be length 65, but using 256 allows bounds check
// elimination on lookup.
var varintLens [256]byte

func init() {
	for i := 0; i < len(varintLens[:]); i++ {
		varintLens[i] = byte((i-1)/7) + 1
	}
}

func VarintLen(i int64) int {
	u := uint64(i)<<1 ^ uint64(i>>63)
	return int(varintLens[byte(bits.Len64(u))])
}

// Varlong is a loop unrolled 64 bit varint decoder. The return semantics
// are the same as binary.Varint.
func Varlong(in []byte) (int64, int) {
	var c byte
	var x uint64
	var n int

	if len(in) < 1 {
		goto fail
	}

	c = in[0]
	x = uint64(c & 0x7f)
	n = 1

	if c&0x80 != 0 {
		if len(in) < 2 {
			goto fail
		}
		c = in[1]
		x |= uint64(c&0x7f) << 7
		n = 2

		if c&0x80 != 0 {
			if len(in) < 3 {
				goto fail
			}
			c = in[2]
			x |= uint64(c&0x7f) << 14
			n = 3

			if c&0x80 != 0 {
				if len(in) < 4 {
					goto fail
				}
				c = in[3]
				x |= uint64(c&0x7f) << 21
				n = 4

				if c&0x80 != 0 {
					if len(in) < 5 {
						goto fail
					}
					c = in[4]
					x |= uint64(c&0x7f) << 28
					n = 5

					if c&0x80 != 0 {
						if len(in) < 6 {
							goto fail
						}
						c = in[5]
						x |= uint64(c&0x7f) << 35
						n = 6

						if c&0x80 != 0 {
							if len(in) < 7 {
								goto fail
							}
							c = in[6]
							x |= uint64(c&0x7f) << 42
							n = 7

							if c&0x80 != 0 {
								if len(in) < 8 {
									goto fail
								}
								c = in[7]
								x |= uint64(c&0x7f) << 49
								n = 8

								if c&0x80 != 0 {
									if len(in) < 9 {
										goto fail
									}
									c = in[8]
									x |= uint64(c&0x7f) << 56
									n = 9

									if c&0x80 != 0 {
										if len(in) < 10 {
											goto fail
										}
										c = in[9]
										if c > 1 {
											return 0, -10
										}
										x |= uint64(c) << 63
										n = 10

									}
								}
							}
						}
					}
				}
			}
		}
	}

	return int64((x >> 1) ^ -(x & 1)), n
fail:
	return 0, 0
}

// Varint is a loop unrolled 32 bit varint decoder. The return semantics
// are the same as binary.Varint, with the added benefit that overflows
// in 5 byte encodings are handled rather than left to the user.
func Varint(in []byte) (int32, int) {
	var c byte
	var x uint32
	var n int

	if len(in) < 1 {
		goto fail
	}

	c = in[0]
	x = uint32(c & 0x7f)
	n = 1

	if c&0x80 != 0 {
		if len(in) < 2 {
			goto fail
		}
		c = in[1]
		x |= uint32(c&0x7f) << 7
		n = 2

		if c&0x80 != 0 {
			if len(in) < 3 {
				goto fail
			}
			c = in[2]
			x |= uint32(c&0x7f) << 14
			n = 3

			if c&0x80 != 0 {
				if len(in) < 4 {
					goto fail
				}
				c = in[3]
				x |= uint32(c&0x7f) << 21
				n = 4

				if c&0x80 != 0 {
					if len(in) < 5 {
						goto fail
					}
					c = in[4]
					if c > 0x0f {
						return 0, -5
					}
					x |= uint32(c) << 28
					n = 5

				}
			}
		}
	}

	return int32((x >> 1) ^ -(x & 1)), n
fail:
	return 0, 0
}

func AppendVarint(dst []byte, i int32) []byte {
	u := uint32(i)<<1 ^ uint32(i>>31)
	switch VarintLen(int64(i)) {
	case 5:
		return append(dst,
			byte(u&0x7f|0x80),
			byte((u>>7)&0x7f|0x80),
			byte((u>>14)&0x7f|0x80),
			byte((u>>21)&0x7f|0x80),
			byte(u>>28))
	case 4:
		return append(dst,
			byte(u&0x7f|0x80),
			byte((u>>7)&0x7f|0x80),
			byte((u>>14)&0x7f|0x80),
			byte(u>>21))
	case 3:
		return append(dst,
			byte(u&0x7f|0x80),
			byte((u>>7)&0x7f|0x80),
			byte(u>>14))
	case 2:
		return append(dst,
			byte(u&0x7f|0x80),
			byte(u>>7))
	case 1:
		return append(dst, byte(u))
	}
	return dst
}

func AppendVarlong(dst []byte, i int64) []byte {
	u := uint64(i)<<1 ^ uint64(i>>63)
	for u&0x7f != u {
		dst = append(dst, byte(u&0x7f|0x80))
		u >>= 7
	}
	return append(dst, byte(u))
}

func AppendString(dst []byte, s string) []byte {
	dst = AppendInt16(dst, int16(len(s)))
	return append(dst, s...)
}

func AppendNullableString(dst []byte, s *string) []byte {
	if s == nil {
		return AppendInt16(dst, -1)
	}
	return AppendString(dst, *s)
}

func AppendBytes(dst, b []byte) []byte {
	dst = AppendInt32(dst, int32(len(b)))
	return append(dst, b...)
}

func AppendNullableBytes(dst []byte, b *[]byte) []byte {
	if b == nil {
		return AppendInt32(dst, -1)
	}
	return AppendBytes(dst, *b)
}

func AppendVarintString(dst []byte, s string) []byte {
	dst = AppendVarint(dst, int32(len(s)))
	return append(dst, s...)
}

func AppendVarintBytes(dst []byte, b []byte) []byte {
	if b == nil {
		return AppendVarint(dst, -1)
	}
	dst = AppendVarint(dst, int32(len(b)))
	return append(dst, b...)
}

func AppendNullableArrayLen(dst []byte, l int, isNil bool) []byte {
	if isNil {
		return AppendInt32(dst, -1)
	}
	return AppendInt32(dst, int32(l))
}

func AppendArrayLen(dst []byte, l int) []byte {
	return AppendInt32(dst, int32(l))
}

type Reader struct {
	Src []byte
	bad bool
}

func (b *Reader) Bool() bool {
	if len(b.Src) < 1 {
		b.bad = true
		b.Src = nil
		return false
	}
	t := b.Src[0] != 0 // if '0', false
	b.Src = b.Src[1:]
	return t
}

func (b *Reader) Int8() int8 {
	if len(b.Src) < 1 {
		b.bad = true
		b.Src = nil
		return 0
	}
	r := b.Src[0]
	b.Src = b.Src[1:]
	return int8(r)
}

func (b *Reader) Int16() int16 {
	if len(b.Src) < 2 {
		b.bad = true
		b.Src = nil
		return 0
	}
	r := int16(binary.BigEndian.Uint16(b.Src))
	b.Src = b.Src[2:]
	return r
}

func (b *Reader) Int32() int32 {
	if len(b.Src) < 4 {
		b.bad = true
		b.Src = nil
		return 0
	}
	r := int32(binary.BigEndian.Uint32(b.Src))
	b.Src = b.Src[4:]
	return r
}

func (b *Reader) Int64() int64 {
	if len(b.Src) < 8 {
		b.bad = true
		b.Src = nil
		return 0
	}
	r := int64(binary.BigEndian.Uint64(b.Src))
	b.Src = b.Src[8:]
	return r
}

func (b *Reader) Uint32() uint32 {
	if len(b.Src) < 4 {
		b.bad = true
		b.Src = nil
		return 0
	}
	r := binary.BigEndian.Uint32(b.Src)
	b.Src = b.Src[4:]
	return r
}

func (b *Reader) Varint() int32 {
	val, n := Varint(b.Src)
	if n <= 0 {
		b.bad = true
		b.Src = nil
		return 0
	}
	b.Src = b.Src[n:]
	return int32(val)
}

func (b *Reader) Varlong() int64 {
	val, n := Varlong(b.Src)
	if n <= 0 {
		b.bad = true
		b.Src = nil
		return 0
	}
	b.Src = b.Src[n:]
	return int64(val)
}

func (b *Reader) Span(l int) []byte {
	if len(b.Src) < l || l < 0 {
		b.bad = true
		b.Src = nil
		return nil
	}
	r := b.Src[:l:l]
	b.Src = b.Src[l:]
	return r
}

func (b *Reader) String() string {
	l := b.Int16()
	return string(b.Span(int(l)))
}

func (b *Reader) NullableString() *string {
	l := b.Int16()
	if l < 0 {
		return nil
	}
	s := string(b.Span(int(l)))
	return &s
}

func (b *Reader) Bytes() []byte {
	l := b.Int32()
	return b.Span(int(l))
}

func (b *Reader) NullableBytes() *[]byte {
	l := b.Int32()
	if l < 0 {
		return nil
	}
	r := b.Span(int(l))
	return &r
}

func (b *Reader) ArrayLen() int32 {
	r := b.Int32()
	// The min size of a Kafka type is a byte, so if we do not have
	// at least the array length of bytes left, it is bad.
	if len(b.Src) < int(r) {
		b.bad = true
		b.Src = nil
		return 0
	}
	return r
}

func (b *Reader) VarintBytes() []byte {
	l := b.Varint()
	if l < 0 {
		return nil
	}
	return b.Span(int(l))
}

func (b *Reader) VarintString() string {
	return string(b.VarintBytes())
}

// Complete returns ErrNotEnoughData if the source ran out while decoding,
// or ErrTooMuchData if there is data remaining, or nil.
func (b *Reader) Complete() error {
	if b.bad {
		return ErrNotEnoughData
	}
	if len(b.Src) > 0 {
		return ErrTooMuchData
	}
	return nil
}
