package kbin

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"strconv"
	"strings"
	"testing"
	"testing/quick"
)

func TestVarint(t *testing.T) {
	if err := quick.Check(func(x int32) bool {
		var expPut [10]byte
		n := binary.PutVarint(expPut[:], int64(x))

		gotPut := AppendVarint(nil, x)
		if !bytes.Equal(expPut[:n], gotPut) {
			return false
		}
		if len(gotPut) != n {
			return false
		}
		if VarintLen(x) != n {
			return false
		}

		expRead, expN := binary.Varint(expPut[:n])
		gotRead, gotN := Varint(gotPut)

		if expN != gotN || expRead != int64(gotRead) {
			return false
		}

		return true
	}, nil); err != nil {
		t.Error(err)
	}
}

func TestUvarint(t *testing.T) {
	if err := quick.Check(func(u uint32) bool {
		var expPut [10]byte
		n := binary.PutUvarint(expPut[:], uint64(u))

		gotPut := AppendUvarint(nil, u)
		if !bytes.Equal(expPut[:n], gotPut) {
			return false
		}

		expRead, expN := binary.Uvarint(expPut[:n])
		gotRead, gotN := Uvarint(gotPut)

		if expN != gotN || expRead != uint64(gotRead) {
			return false
		}

		return true
	}, nil); err != nil {
		t.Error(err)
	}
}

func TestVarlong(t *testing.T) {
	if err := quick.Check(func(x int64) bool {
		var expPut [10]byte
		n := binary.PutVarint(expPut[:], x)

		gotPut := AppendVarlong(nil, x)
		if !bytes.Equal(expPut[:n], gotPut) {
			return false
		}
		if len(gotPut) != n {
			return false
		}
		if VarlongLen(x) != n {
			return false
		}

		expRead, expN := binary.Varint(expPut[:n])
		gotRead, gotN := Varlong(gotPut)

		if expN != gotN || expRead != gotRead {
			return false
		}

		return true
	}, nil); err != nil {
		t.Error(err)
	}
}

func BenchmarkUvarint(b *testing.B) {
	for _, u := range []uint32{
		0,         // len 1
		128,       // len 2
		16384,     // len 3
		2097152,   // len 4
		268435456, // len 5
	} {
		b.Run(fmt.Sprintf("uvarint_len_%d", UvarintLen(u)), func(b *testing.B) {
			s := AppendUvarint(nil, u)
			for i := 0; i < b.N; i++ {
				Uvarint(s)
			}
		})
	}
}

// We append nothing for a string or an array length over the limit, and we
// append both at the limit.
func TestAppendTooLong(t *testing.T) {
	over := strings.Repeat("a", math.MaxInt16+1)
	atMax := over[1:]
	for name, fn := range map[string]func([]byte, string) []byte{
		"AppendString":                AppendString,
		"AppendCompactString":         AppendCompactString,
		"AppendNullableString":        func(dst []byte, s string) []byte { return AppendNullableString(dst, &s) },
		"AppendCompactNullableString": func(dst []byte, s string) []byte { return AppendCompactNullableString(dst, &s) },
	} {
		if got := fn([]byte{1}, over); len(got) != 1 {
			t.Errorf("%s appended %d bytes for a string over the max, want 0", name, len(got)-1)
		}
		if got := fn([]byte{1}, atMax); len(got) <= 1+len(atMax) {
			t.Errorf("%s appended %d bytes for a string at the max, want the string and its length", name, len(got)-1)
		}
	}

	if strconv.IntSize < 64 {
		return // an int cannot be over math.MaxInt32
	}
	overLen := int64(math.MaxInt32)
	overLen++
	for name, fn := range map[string]func([]byte, int) []byte{
		"AppendArrayLen":                AppendArrayLen,
		"AppendCompactArrayLen":         AppendCompactArrayLen,
		"AppendNullableArrayLen":        func(dst []byte, l int) []byte { return AppendNullableArrayLen(dst, l, false) },
		"AppendCompactNullableArrayLen": func(dst []byte, l int) []byte { return AppendCompactNullableArrayLen(dst, l, false) },
	} {
		if got := fn([]byte{1}, int(overLen)); len(got) != 1 {
			t.Errorf("%s appended %d bytes for a length over the max, want 0", name, len(got)-1)
		}
		if got := fn([]byte{1}, math.MaxInt32); len(got) == 1 {
			t.Errorf("%s appended nothing for a length at the max", name)
		}
	}
}
