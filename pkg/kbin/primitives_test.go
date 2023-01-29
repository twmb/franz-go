package kbin

import (
	"bytes"
	"encoding/binary"
	"fmt"
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
