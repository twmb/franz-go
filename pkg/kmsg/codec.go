package kmsg

import (
	"math"

	"github.com/twmb/franz-go/pkg/kmsg/internal/kbin"
)

// Generated code reads and appends fixed size fields with these. A call is
// much smaller than the inlined kbin function at every field.

//go:noinline
func readBool(b *kbin.Reader) bool { return b.Bool() }

//go:noinline
func readInt8(b *kbin.Reader) int8 { return b.Int8() }

//go:noinline
func readInt16(b *kbin.Reader) int16 { return b.Int16() }

//go:noinline
func readUint16(b *kbin.Reader) uint16 { return b.Uint16() }

//go:noinline
func readInt32(b *kbin.Reader) int32 { return b.Int32() }

//go:noinline
func readInt64(b *kbin.Reader) int64 { return b.Int64() }

//go:noinline
func readUuid(b *kbin.Reader) [16]byte { return b.Uuid() }

//go:noinline
func appendBool(dst []byte, v bool) []byte { return kbin.AppendBool(dst, v) }

//go:noinline
func appendInt8(dst []byte, v int8) []byte { return kbin.AppendInt8(dst, v) }

//go:noinline
func appendInt16(dst []byte, v int16) []byte { return kbin.AppendInt16(dst, v) }

//go:noinline
func appendUint16(dst []byte, v uint16) []byte { return kbin.AppendUint16(dst, v) }

//go:noinline
func appendInt32(dst []byte, v int32) []byte { return kbin.AppendInt32(dst, v) }

//go:noinline
func appendInt64(dst []byte, v int64) []byte { return kbin.AppendInt64(dst, v) }

//go:noinline
func appendUuid(dst []byte, v [16]byte) []byte { return kbin.AppendUuid(dst, v) }

// Generated code reads and appends strings, bytes, and array lengths that can
// be compact or unsafe with the functions below, rather than choosing between
// kbin functions at every field.
//
// Most compact lengths are one byte. The read functions decode that byte
// themselves, since Reader.Uvarint is not inlined. The read functions for
// strings and bytes store into dst; see writeDecodeInto in the generator.

func readString(b *kbin.Reader, dst *string, flexible, unsafe bool) {
	var l int
	if flexible {
		if src := b.Src; len(src) > 0 && src[0] < 0x80 {
			b.Src = src[1:]
			l = int(src[0]) - 1
		} else {
			l = int(b.Uvarint()) - 1
		}
	} else {
		l = int(b.Int16())
	}
	if unsafe {
		*dst = kbin.UnsafeString(b.Span(l))
	} else {
		*dst = string(b.Span(l))
	}
}

func readNullableString(b *kbin.Reader, dst **string, flexible, unsafe bool) {
	var l int
	if flexible {
		if src := b.Src; len(src) > 0 && src[0] < 0x80 {
			b.Src = src[1:]
			l = int(src[0]) - 1
		} else {
			l = int(b.Uvarint()) - 1
		}
	} else {
		l = int(b.Int16())
	}
	if l < 0 {
		*dst = nil
		return
	}
	var s string
	if unsafe {
		s = kbin.UnsafeString(b.Span(l))
	} else {
		s = string(b.Span(l))
	}
	*dst = &s
}

func readVarintString(b *kbin.Reader, dst *string, unsafe bool) {
	if unsafe {
		*dst = kbin.UnsafeString(b.VarintBytes())
	} else {
		*dst = string(b.VarintBytes())
	}
}

func readVarintBytes(b *kbin.Reader, dst *[]byte) { *dst = b.VarintBytes() }

func readBytes(b *kbin.Reader, dst *[]byte, flexible bool) {
	var l int
	if flexible {
		if src := b.Src; len(src) > 0 && src[0] < 0x80 {
			b.Src = src[1:]
			l = int(src[0]) - 1
		} else {
			l = int(b.Uvarint()) - 1
		}
	} else {
		l = int(b.Int32())
	}
	if l == -1 { // see kbin.Reader.Bytes
		*dst = []byte{}
		return
	}
	*dst = b.Span(l)
}

func readNullableBytes(b *kbin.Reader, dst *[]byte, flexible bool) {
	var l int
	if flexible {
		if src := b.Src; len(src) > 0 && src[0] < 0x80 {
			b.Src = src[1:]
			l = int(src[0]) - 1
		} else {
			l = int(b.Uvarint()) - 1
		}
	} else {
		l = int(b.Int32())
	}
	if l < 0 {
		*dst = nil
		return
	}
	*dst = b.Span(l)
}

// The append helpers below refuse a string longer than math.MaxInt16 bytes,
// and bytes or an array longer than math.MaxInt32: the helper appends nothing
// and sets `over`, and the AppendTo that called the helper returns nil. If we
// instead wrote a field too long for a classic length prefix, the length
// would wrap, and a reader would parse the rest of the field as the fields
// after it.
//
// kbin's append functions have the same limits. We repeat the checks here
// because these helpers write the length inline: kbin's compact functions do
// not inline, and calling them adds a call per field.

func appendString(dst []byte, s string, flexible bool, over *bool) []byte {
	if len(s) > math.MaxInt16 {
		*over = true
		return dst
	}
	if flexible {
		dst = kbin.AppendUvarint(dst, 1+uint32(len(s)))
	} else {
		dst = kbin.AppendInt16(dst, int16(len(s)))
	}
	return append(dst, s...)
}

func appendNullableString(dst []byte, s *string, flexible bool, over *bool) []byte {
	if s == nil {
		if flexible {
			return append(dst, 0)
		}
		return kbin.AppendInt16(dst, -1)
	}
	if len(*s) > math.MaxInt16 {
		*over = true
		return dst
	}
	if flexible {
		dst = kbin.AppendUvarint(dst, 1+uint32(len(*s)))
	} else {
		dst = kbin.AppendInt16(dst, int16(len(*s)))
	}
	return append(dst, *s...)
}

func appendBytes(dst, b []byte, flexible bool, over *bool) []byte {
	if len(b) > math.MaxInt32 {
		*over = true
		return dst
	}
	if flexible {
		dst = kbin.AppendUvarint(dst, 1+uint32(len(b)))
	} else {
		dst = kbin.AppendInt32(dst, int32(len(b)))
	}
	return append(dst, b...)
}

func appendNullableBytes(dst, b []byte, flexible bool, over *bool) []byte {
	if b == nil {
		if flexible {
			return append(dst, 0)
		}
		return kbin.AppendInt32(dst, -1)
	}
	if len(b) > math.MaxInt32 {
		*over = true
		return dst
	}
	if flexible {
		dst = kbin.AppendUvarint(dst, 1+uint32(len(b)))
	} else {
		dst = kbin.AppendInt32(dst, int32(len(b)))
	}
	return append(dst, b...)
}

func readArrayLen(b *kbin.Reader, flexible bool) int32 {
	if flexible {
		if src := b.Src; len(src) > 0 && src[0] < 0x80 && int(src[0]) <= len(src) {
			b.Src = src[1:]
			return int32(src[0]) - 1
		}
		return b.CompactArrayLen()
	}
	return b.ArrayLen()
}

func appendArrayLen(dst []byte, n int, flexible bool, over *bool) []byte {
	if n > math.MaxInt32 {
		*over = true
		return dst
	}
	if flexible {
		return kbin.AppendUvarint(dst, 1+uint32(n))
	}
	return kbin.AppendInt32(dst, int32(n))
}

func appendNullableArrayLen(dst []byte, n int, isNil, flexible bool, over *bool) []byte {
	if isNil {
		if flexible {
			return append(dst, 0)
		}
		return kbin.AppendInt32(dst, -1)
	}
	return appendArrayLen(dst, n, flexible, over)
}

func appendVarintArrayLen(dst []byte, n int, over *bool) []byte {
	if n > math.MaxInt32 {
		*over = true
		return dst
	}
	return kbin.AppendVarint(dst, int32(n))
}

func appendVarintString(dst []byte, s string, over *bool) []byte {
	if len(s) > math.MaxInt32 {
		*over = true
		return dst
	}
	dst = kbin.AppendVarint(dst, int32(len(s)))
	return append(dst, s...)
}

func appendVarintBytes(dst, b []byte, over *bool) []byte {
	if b == nil {
		return kbin.AppendVarint(dst, -1)
	}
	if len(b) > math.MaxInt32 {
		*over = true
		return dst
	}
	dst = kbin.AppendVarint(dst, int32(len(b)))
	return append(dst, b...)
}

// Generated code appends a tagged field once to measure it. We write the size
// over those bytes, from lenAt on, and the generated code appends the field
// again after the size. If the field is longer than math.MaxInt32 bytes, we
// set `over`, and the AppendTo returns nil.
func appendTagSize(dst []byte, lenAt int, over *bool) []byte {
	n := len(dst) - lenAt
	if n > math.MaxInt32 {
		*over = true
	}
	return kbin.AppendUvarint(dst[:lenAt], uint32(n))
}
