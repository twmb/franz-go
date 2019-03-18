package kgo

import (
	"encoding/binary"
	"math/bits"
)

// maybeMake is used for preallocating array lengths only if the length is
// non-null (-1).
func maybeMake(l int32, f func()) {
	if l >= 0 {
		f()
	}
}

// atVarintBoundary returns whether n+1 would increase the length of n were it
// varint encoded.
//
// negatives are excluded since we are dealing with positive lengths.
func atVarintBoundary(n int) bool {
	switch n {
	case 63,
		8191,
		1048575,
		134217727,
		17179869183,
		2199023255551,
		281474976710655,
		36028797018963967,
		4611686018427387903:
		return true
	}
	return false
}

// ********** PROTOCOL PRIMITIVE TYPES **********
// http://kafka.apache.org/protocol.html#protocol_types

func appendBool(dst []byte, v bool) []byte {
	if v {
		return append(dst, 1)
	}
	return append(dst, 0)
}
func appendInt8(dst []byte, i int8) []byte {
	return append(dst, byte(i))
}
func appendInt16(dst []byte, i int16) []byte {
	u := uint16(i)
	return append(dst, byte(u>>8), byte(u))
}
func appendInt32(dst []byte, i int32) []byte {
	return appendUint32(dst, uint32(i))
}
func appendInt64(dst []byte, i int64) []byte {
	u := uint64(i)
	return append(dst, byte(u>>56), byte(u>>48), byte(u>>40), byte(u>>32),
		byte(u>>24), byte(u>>16), byte(u>>8), byte(u))
}
func appendUint32(dst []byte, u uint32) []byte {
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

// We only provide varintLen for int64s; the length calculation is the same
// for int32s and it is easy to cast up.
func varintLen(i int64) int {
	u := uint64(i)<<1 ^ uint64(i>>63) // sign extend i so it xor's out
	return int(varintLens[byte(bits.Len64(u))])
}

func appendVarint(dst []byte, i int32) []byte {
	u := uint32(i)<<1 ^ uint32(i>>31)
	for u&0x7f != u {
		dst = append(dst, byte(u&0x7f|0x80))
		u >>= 7
	}
	return append(dst, byte(u))
}

func appendVarintString(dst []byte, s string) []byte {
	dst = appendVarint(dst, int32(len(s)))
	return append(dst, s...)
}

func appendVarlong(dst []byte, i int64) []byte {
	u := uint64(i)<<1 ^ uint64(i>>63)
	for u&0x7f != u {
		dst = append(dst, byte(u&0x7f|0x80))
		u >>= 7
	}
	return append(dst, byte(u))
}

func appendString(dst []byte, s string) []byte {
	dst = appendInt16(dst, int16(len(s)))
	return append(dst, s...)
}

func appendNullableString(dst []byte, s *string) []byte {
	if s == nil {
		return appendInt16(dst, -1)
	}
	return appendString(dst, *s)
}

func appendBytes(dst, b []byte) []byte {
	dst = appendInt32(dst, int32(len(b)))
	return append(dst, b...)
}

func appendStringAsBytes(dst []byte, s string) []byte {
	dst = appendInt32(dst, int32(len(s)))
	return append(dst, s...)
}

func appendArrayLen(dst []byte, l int) []byte {
	return appendInt32(dst, int32(l))
}

// ********** READING **********

type binreader struct {
	bad bool
	src []byte
}

func (b *binreader) has(n int) bool { return len(b.src) >= n }

func (b *binreader) varint() (int32, bool) {
	val, n := binary.Uvarint(b.src)
	if n <= 0 || n > 5 {
		b.bad = true
		return 0, false
	}
	return int32(val), true
}

func (b *binreader) varlong() (int64, bool) {
	val, n := binary.Uvarint(b.src)
	if n <= 0 {
		b.bad = true
		return 0, false
	}
	b.src = b.src[n:]
	return int64(val), true
}

func (b *binreader) rbool() bool {
	if len(b.src) < 1 {
		b.bad = true
		return false
	}
	t := b.src[0] != 0 // if '0', false
	b.src = b.src[1:]
	return t
}

func (b *binreader) rint8() int8 {
	if len(b.src) < 1 {
		b.bad = true
		return 0
	}
	r := b.src[0]
	b.src = b.src[1:]
	return int8(r)
}

func (b *binreader) rint16() int16 {
	if len(b.src) < 2 {
		b.bad = true
		return 0
	}
	r := int16(binary.BigEndian.Uint16(b.src))
	b.src = b.src[2:]
	return r
}

func (b *binreader) rint32() int32 {
	if len(b.src) < 4 {
		b.bad = true
		return 0
	}
	r := int32(binary.BigEndian.Uint32(b.src))
	b.src = b.src[4:]
	return r
}

func (b *binreader) rint64() int64 {
	if len(b.src) < 8 {
		b.bad = true
		return 0
	}
	r := int64(binary.BigEndian.Uint64(b.src))
	b.src = b.src[8:]
	return r
}

func (b *binreader) rstringLen(l int16) string {
	if len(b.src) < int(l) {
		b.bad = true
		return ""
	}
	r := b.src[:l]
	b.src = b.src[l:]
	return string(r)
}

func (b *binreader) rstring() string {
	l := b.rint16()
	return b.rstringLen(l)
}

func (b *binreader) rnullableString() *string {
	l := b.rint16()
	if l < 0 {
		return nil
	}
	s := b.rstringLen(l)
	return &s
}

func (b *binreader) rarrayLen() int32 {
	return b.rint32()
}

func (b *binreader) complete() error {
	if b.bad {
		return errNotEnoughData
	}
	if len(b.src) > 0 {
		return errTooMuchData
	}
	return nil
}
