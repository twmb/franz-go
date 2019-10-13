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

// AppendBool appends 1 for true or 0 for false to dst.
func AppendBool(dst []byte, v bool) []byte {
	if v {
		return append(dst, 1)
	}
	return append(dst, 0)
}

// AppendInt8 appends an int8 to dst.
func AppendInt8(dst []byte, i int8) []byte {
	return append(dst, byte(i))
}

// AppendInt16 appends a big endian int16 to dst.
func AppendInt16(dst []byte, i int16) []byte {
	u := uint16(i)
	return append(dst, byte(u>>8), byte(u))
}

// AppendInt32 appends a big endian int32 to dst.
func AppendInt32(dst []byte, i int32) []byte {
	return AppendUint32(dst, uint32(i))
}

// AppendInt64 appends a big endian int64 to dst.
func AppendInt64(dst []byte, i int64) []byte {
	u := uint64(i)
	return append(dst, byte(u>>56), byte(u>>48), byte(u>>40), byte(u>>32),
		byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}

// AppendUint32 appends a big endian uint32 to dst.
func AppendUint32(dst []byte, u uint32) []byte {
	return append(dst, byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}

// uvarintLens could only be length 65, but using 256 allows bounds check
// elimination on lookup.
var uvarintLens [256]byte

func init() {
	for i := 0; i < len(uvarintLens[:]); i++ {
		uvarintLens[i] = byte((i-1)/7) + 1
	}
}

// VarintLen returns how long i would be if it were varint encoded.
func VarintLen(i int64) int {
	u := uint64(i)<<1 ^ uint64(i>>63)
	return UvarintLen(u)
}

// UvarintLen returns how long u would be if it were uvarint encoded.
func UvarintLen(u uint64) int {
	return int(uvarintLens[byte(bits.Len64(u))])
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
	x, n := Uvarint(in)
	return int32((x >> 1) ^ -(x & 1)), n
}

// Uvarint is a loop unrolled 32 bit uvarint decoder. The return semantics
// are the same as binary.Uvarint, with the added benefit that overflows
// in 5 byte encodings are handled rather than left to the user.
func Uvarint(in []byte) (uint32, int) {
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

	return x, n
fail:
	return 0, 0
}

// AppendVarint appends a varint encoded i to dst.
func AppendVarint(dst []byte, i int32) []byte {
	return AppendUvarint(dst, uint32(i)<<1^uint32(i>>31))
}

// AppendUvarint appends a uvarint encoded u to dst.
func AppendUvarint(dst []byte, u uint32) []byte {
	switch UvarintLen(uint64(u)) {
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

// AppendVarlong appends a varint encoded i to dst.
func AppendVarlong(dst []byte, i int64) []byte {
	u := uint64(i)<<1 ^ uint64(i>>63)
	for u&0x7f != u {
		dst = append(dst, byte(u&0x7f|0x80))
		u >>= 7
	}
	return append(dst, byte(u))
}

// AppendString appends a string to dst prefixed with its int16 length.
func AppendString(dst []byte, s string) []byte {
	dst = AppendInt16(dst, int16(len(s)))
	return append(dst, s...)
}

// AppendCompactString appends a string to dst prefixed with its uvarint length
// starting at 1; 0 is reserved for null, which compact strings are not
// (nullable compact ones are!). Thus, the length is the decoded uvarint - 1.
//
// For KIP-482.
func AppendCompactString(dst []byte, s string) []byte {
	dst = AppendUvarint(dst, 1+uint32(len(s)))
	return append(dst, s...)
}

// AppendNullableString appends potentially nil string to dst prefixed with its
// int16 length or int16(-1) if nil.
func AppendNullableString(dst []byte, s *string) []byte {
	if s == nil {
		return AppendInt16(dst, -1)
	}
	return AppendString(dst, *s)
}

// AppendCompactNullableString appends a potentially nil string to dst with its
// uvarint length starting at 1, with 0 indicating null. Thus, the length is
// the decoded uvarint - 1.
//
// For KIP-482.
func AppendCompactNullableString(dst []byte, s *string) []byte {
	if s == nil {
		return AppendUvarint(dst, 0)
	}
	return AppendCompactString(dst, *s)
}

// AppendBytes appends bytes to dst prefixed with its int32 length.
func AppendBytes(dst, b []byte) []byte {
	dst = AppendInt32(dst, int32(len(b)))
	return append(dst, b...)
}

// AppendCompactBytes appends bytes to dst prefixed with a its uvarint length
// starting at 1; 0 is reserved for null, which compact bytes are not (nullable
// compact ones are!). Thus, the length is the decoded uvarint - 1.
//
// For KIP-482.
func AppendCompactBytes(dst, b []byte) []byte {
	dst = AppendUvarint(dst, 1+uint32(len(b)))
	return append(dst, b...)
}

// AppendNullableBytes appends a potentially nil slice to dst prefixed with its
// int32 length or int32(-1) if nil.
func AppendNullableBytes(dst []byte, b []byte) []byte {
	if b == nil {
		return AppendInt32(dst, -1)
	}
	return AppendBytes(dst, b)
}

// AppendCompactNullableBytes appends a potentially nil slice to dst with its
// uvarint length starting at 1, with 0 indicating null. Thus, the length is
// the decoded uvarint - 1.
//
// For KIP-482.
func AppendCompactNullableBytes(dst []byte, b []byte) []byte {
	if b == nil {
		return AppendUvarint(dst, 0)
	}
	return AppendCompactBytes(dst, b)
}

// AppendVarintString appends a string to dst prefixed with its length encoded
// as a varint.
func AppendVarintString(dst []byte, s string) []byte {
	dst = AppendVarint(dst, int32(len(s)))
	return append(dst, s...)
}

// AppendVarintBytes appends a slice to dst prefixed with its length encoded as
// a varint.
func AppendVarintBytes(dst []byte, b []byte) []byte {
	if b == nil {
		return AppendVarint(dst, -1)
	}
	dst = AppendVarint(dst, int32(len(b)))
	return append(dst, b...)
}

// AppendArrayLen appends the length of an array as an int32 to dst.
func AppendArrayLen(dst []byte, l int) []byte {
	return AppendInt32(dst, int32(l))
}

// AppendCompactArrayLen appends the length of an array as a uvarint to dst
// as the length + 1.
//
// For KIP-482.
func AppendCompactArrayLen(dst []byte, l int) []byte {
	return AppendUvarint(dst, 1+uint32(l))
}

// AppendNullableArrayLen appends the length of an array as an int32 to dst,
// or -1 if isNil is true.
func AppendNullableArrayLen(dst []byte, l int, isNil bool) []byte {
	if isNil {
		return AppendInt32(dst, -1)
	}
	return AppendInt32(dst, int32(l))
}

// AppendCompactNullableArrayLen appends the length of an array as a uvarint to
// dst as the length + 1; if isNil is true, this appends 0 as a uvarint.
//
// For KIP-482.
func AppendCompactNullableArrayLen(dst []byte, l int, isNil bool) []byte {
	if isNil {
		return AppendUvarint(dst, 0)
	}
	return AppendUvarint(dst, 1+uint32(l))
}

// Reader is used to decode Kafka messages.
//
// For all functions on Reader, if the reader has been invalidated, functions
// return defaults (false, 0, nil, ""). Use Complete to detect if the reader
// was invalidated or if the reader has remaining data.
type Reader struct {
	Src []byte
	bad bool
}

// Bool returns a bool from the reader.
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

// Int8 returns an int8 from the reader.
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

// Int16 returns an int16 from the reader.
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

// Int32 returns an int32 from the reader.
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

// Int64 returns an int64 from the reader.
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

// Uint32 returns a uint32 from the reader.
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

// Varint returns a varint int32 from the reader.
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

// Uvarint returns a uvarint encoded uint32 from the reader.
func (b *Reader) Uvarint() uint32 {
	val, n := Uvarint(b.Src)
	if n <= 0 {
		b.bad = true
		b.Src = nil
		return 0
	}
	b.Src = b.Src[n:]
	return val
}

// Varlong returns a varlong int64 from the reader.
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

// Span returns l bytes from the reader.
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

// String returns a Kafka string from the reader.
func (b *Reader) String() string {
	l := b.Int16()
	return string(b.Span(int(l)))
}

// CompactString returns a Kafka compact string from the reader.
func (b *Reader) CompactString() string {
	l := int(b.Uvarint()) - 1
	return string(b.Span(l))
}

// NullableString returns a Kafka nullable string from the reader.
func (b *Reader) NullableString() *string {
	l := b.Int16()
	if l < 0 {
		return nil
	}
	s := string(b.Span(int(l)))
	return &s
}

// CompactNullableString returns a Kafka compact nullable string from the
// reader.
func (b *Reader) CompactNullableString() *string {
	l := int(b.Uvarint()) - 1
	if l < 0 {
		return nil
	}
	s := string(b.Span(int(l)))
	return &s
}

// Bytes returns a Kafka byte array from the reader.
//
// This never returns nil.
func (b *Reader) Bytes() []byte {
	l := b.Int32()
	return b.Span(int(l))
}

// CompactBytes returns a Kafka compact byte array from the reader.
//
// This never returns nil.
func (b *Reader) CompactBytes() []byte {
	l := int(b.Uvarint()) - 1
	return b.Span(int(l))
}

// NullableBytes returns a Kafka nullable byte array from the reader, returning
// nil as appropriate.
func (b *Reader) NullableBytes() []byte {
	l := b.Int32()
	if l < 0 {
		return nil
	}
	r := b.Span(int(l))
	return r
}

// CompactNullableBytes returns a Kafka compact nullable byte array from the
// reader, returning nil as appropriate.
func (b *Reader) CompactNullableBytes() []byte {
	l := int(b.Uvarint()) - 1
	if l < 0 {
		return nil
	}
	r := b.Span(int(l))
	return r
}

// ArrayLen returns a Kafka array length from the reader.
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

// VarintArrayLen returns a Kafka array length from the reader.
func (b *Reader) VarintArrayLen() int32 {
	r := b.Varint()
	// The min size of a Kafka type is a byte, so if we do not have
	// at least the array length of bytes left, it is bad.
	if len(b.Src) < int(r) {
		b.bad = true
		b.Src = nil
		return 0
	}
	return r
}

// CompactArrayLen returns a Kafka compact array length from the reader.
func (b *Reader) CompactArrayLen() int32 {
	r := int32(b.Uvarint()) - 1
	// The min size of a Kafka type is a byte, so if we do not have
	// at least the array length of bytes left, it is bad.
	if len(b.Src) < int(r) {
		b.bad = true
		b.Src = nil
		return 0
	}
	return r
}

// VarintBytes returns a Kafka encoded varint array from the reader, returning
// nil as appropriate.
func (b *Reader) VarintBytes() []byte {
	l := b.Varint()
	if l < 0 {
		return nil
	}
	return b.Span(int(l))
}

// VarintString returns a Kafka encoded varint string from the reader.
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

// Ok returns true if the reader is still ok.
func (b *Reader) Ok() bool {
	return !b.bad
}
