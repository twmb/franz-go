package compress

import (
	"bytes"
	"sync"
	"testing"
)

func TestNewCompressor(t *testing.T) {
	for i, test := range []struct {
		codecs    []Codec
		secondary bool
		fail      bool
	}{
		{fail: true},
		{codecs: []Codec{-1}, fail: true},
		{codecs: []Codec{5}, fail: true},

		{codecs: []Codec{0}},
		{codecs: []Codec{1, 0}},
		{codecs: []Codec{2, 0}},
		{codecs: []Codec{3}},
		{codecs: []Codec{4}},
		{codecs: []Codec{4, 3}, secondary: true},
		{codecs: []Codec{4, 4, 3}, secondary: true},
	} {
		c, err := NewCompressor(test.codecs...)
		if c != nil {
			c.Close()
		}
		fail := err != nil
		if fail != test.fail {
			t.Errorf("#%d: ok? %v, exp ok? %v", i, !fail, !test.fail)
		}
	}
}

func TestCompressDecompress(t *testing.T) {
	d := NewDecompressor()
	defer d.Close()
	in := []byte("foo")
	for _, produceVersion := range []int16{
		0, 7,
	} {
		for _, codecs := range [][]Codec{
			[]Codec{0},
			[]Codec{1},
			[]Codec{2},
			[]Codec{3},
			[]Codec{4},
			[]Codec{4, 3},
		} {
			c, _ := NewCompressor(codecs...)
			var wg sync.WaitGroup
			for i := 0; i < 5; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					buf := c.GetBuf()
					defer c.PutBuf(buf)
					got, used := c.Compress(buf, in, produceVersion)
					canNil := c.primary == 4 && c.secondary == -1

					if got == nil && !canNil {
						t.Errorf("got nil unexpectedly, codecs: %v", codecs)
						return
					}
					if got == nil {
						return
					}

					got, err := d.Decompress(got, Codec(used))
					if err != nil {
						t.Errorf("unexpected decompress err: %v", err)
						return
					}
					if !bytes.Equal(got, in) {
						t.Errorf("got decompress %s != exp compress in %s", got, in)
					}

				}()
			}
			wg.Wait()

			c.Close()

		}
	}
}

func BenchmarkCompress(b *testing.B) {
	c, _ := NewCompressor(2) // snappy
	defer c.Close()
	in := []byte("foo")
	for i := 0; i < b.N; i++ {
		buf := c.GetBuf()
		c.Compress(buf, in, 0)
		c.PutBuf(buf)
	}
}
