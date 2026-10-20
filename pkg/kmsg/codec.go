package kmsg

import "github.com/twmb/franz-go/pkg/kmsg/internal/kbin"

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
