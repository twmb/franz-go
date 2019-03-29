// Package kbin contains Kafka primitive reading and writing functions.
package kbin

import (
	"encoding/binary"
	"errors"
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

func AppendVarint(dst []byte, i int32) []byte {
	u := uint32(i)<<1 ^ uint32(i>>31)
	for u&0x7f != u {
		dst = append(dst, byte(u&0x7f|0x80))
		u >>= 7
	}
	return append(dst, byte(u))
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
	dst = AppendVarint(dst, int32(len(b)))
	return append(dst, b...)
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
		return false
	}
	t := b.Src[0] != 0 // if '0', false
	b.Src = b.Src[1:]
	return t
}

func (b *Reader) Int8() int8 {
	if len(b.Src) < 1 {
		b.bad = true
		return 0
	}
	r := b.Src[0]
	b.Src = b.Src[1:]
	return int8(r)
}

func (b *Reader) Int16() int16 {
	if len(b.Src) < 2 {
		b.bad = true
		return 0
	}
	r := int16(binary.BigEndian.Uint16(b.Src))
	b.Src = b.Src[2:]
	return r
}

func (b *Reader) Int32() int32 {
	if len(b.Src) < 4 {
		b.bad = true
		return 0
	}
	r := int32(binary.BigEndian.Uint32(b.Src))
	b.Src = b.Src[4:]
	return r
}

func (b *Reader) Int64() int64 {
	if len(b.Src) < 8 {
		b.bad = true
		return 0
	}
	r := int64(binary.BigEndian.Uint64(b.Src))
	b.Src = b.Src[8:]
	return r
}

func (b *Reader) Uint32() uint32 {
	if len(b.Src) < 4 {
		b.bad = true
		return 0
	}
	r := binary.BigEndian.Uint32(b.Src)
	b.Src = b.Src[4:]
	return r
}

func (b *Reader) Varint() int32 {
	val, n := binary.Varint(b.Src)
	if n <= 0 || n > 5 {
		b.bad = true
		return 0
	}
	b.Src = b.Src[n:]
	return int32(val)
}

func (b *Reader) Varlong() int64 {
	val, n := binary.Varint(b.Src)
	if n <= 0 {
		b.bad = true
		return 0
	}
	b.Src = b.Src[n:]
	return int64(val)
}

func (b *Reader) Span(l int) []byte {
	if len(b.Src) < l || l < 0 {
		b.bad = true
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
	return b.Int32()
}

func (b *Reader) VarintBytes() []byte {
	l := b.Varint()
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
