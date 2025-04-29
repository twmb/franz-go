package kgo

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"math/rand"
	"reflect"
	"sync"
	"testing"

	"github.com/pierrec/lz4/v4"
)

// Regression test for #778.
func TestCompressionCodecLZ4WithSpecifiedLevel(t *testing.T) {
	t.Parallel()

	codec := Lz4Compression().WithLevel(512)
	w := lz4.NewWriter(new(bytes.Buffer))
	err := w.Apply(lz4.CompressionLevelOption(lz4.CompressionLevel(codec.level)))
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestNewCompressor(t *testing.T) {
	t.Parallel()
	for i, test := range []struct {
		codecs []CompressionCodec
		fail   bool
	}{
		{codecs: []CompressionCodec{{codec: -1}}, fail: true},
		{codecs: []CompressionCodec{{codec: 5}}, fail: true},

		{codecs: []CompressionCodec{{codec: 0}}},
		{codecs: []CompressionCodec{{codec: 1}, {codec: 0}}},
		{codecs: []CompressionCodec{{codec: 2}, {codec: 0}}},
		{codecs: []CompressionCodec{{codec: 3}}},
		{codecs: []CompressionCodec{{codec: 4}}},
		{codecs: []CompressionCodec{{codec: 4}, {codec: 3}}},

		{codecs: []CompressionCodec{{codec: 1, level: 127}}}, // bad gzip level is defaulted fine
		{codecs: []CompressionCodec{{codec: 3, level: 127}}}, // bad lz4 level, same
		{codecs: []CompressionCodec{{codec: 4, level: 127}}}, // bad zstd level, same

		{codecs: []CompressionCodec{
			{codec: 4},
			{codec: 4},
			{codec: 3},
			{codec: 2},
			{codec: 1, level: 1},
		}},
	} {
		_, err := DefaultCompressor(test.codecs...)
		fail := err != nil
		if fail != test.fail {
			t.Errorf("#%d: ok? %v, exp ok? %v", i, !fail, !test.fail)
		}
	}
}

func TestCompressDecompress(t *testing.T) {
	randStr := func(length int) []byte {
		const charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
		b := make([]byte, length)
		for i := range b {
			b[i] = charset[rand.Intn(len(charset))]
		}
		return b
	}

	t.Parallel()
	d := DefaultDecompressor()
	inputs := [][]byte{
		randStr(1 << 2),
		randStr(1 << 5),
		randStr(1 << 8),
	}

	var wg sync.WaitGroup
	for _, flag := range []CompressFlag{
		0, CompressDisableZstd,
	} {
		wg.Add(1)
		go func(flag CompressFlag) {
			defer wg.Done()
			for _, codecs := range [][]CompressionCodec{
				{{codec: 0}},
				{{codec: 1}},
				{{codec: 2}},
				{{codec: 3}},
				{{codec: 4}},
				{{codec: 4}, {codec: 3}},
			} {
				c, _ := DefaultCompressor(codecs...)
				if c == nil {
					if codecs[0].codec == 0 {
						continue
					}
					t.Errorf("unexpected nil compressor from codecs %v", codecs)
				}
				for i := 0; i < 3; i++ {
					wg.Add(1)
					go func() {
						w := byteBuffers.Get().(*bytes.Buffer)
						defer wg.Done()
						defer byteBuffers.Put(w)
						for _, in := range inputs {
							w.Reset()

							got, used := c.Compress(w, in, flag)
							got, err := d.Decompress(got, used)
							if err != nil {
								t.Errorf("unexpected decompress err: %v", err)
								return
							}
							if !bytes.Equal(got, in) {
								t.Errorf("got decompress %s != exp compress in %s", got, in)
							}
						}
					}()
				}
			}
		}(flag)
	}
	wg.Wait()
}

func BenchmarkCompress(b *testing.B) {
	in := bytes.Repeat([]byte("abcdefghijklmno pqrs tuvwxy   z"), 100)
	for _, codec := range []CompressionCodecType{CodecGzip, CodecSnappy, CodecLz4, CodecZstd} {
		c, _ := DefaultCompressor(CompressionCodec{codec: codec})
		b.Run(fmt.Sprint(codec), func(b *testing.B) {
			var afterSize int
			for i := 0; i < b.N; i++ {
				w := byteBuffers.Get().(*bytes.Buffer)
				w.Reset()
				after, _ := c.Compress(w, in, 99)
				afterSize = len(after)
				byteBuffers.Put(w)
			}
			b.Logf("%d => %d", len(in), afterSize)
		})
	}
}

func BenchmarkDecompress(b *testing.B) {
	in := bytes.Repeat([]byte("abcdefghijklmno pqrs tuvwxy   z"), 100)
	for _, codec := range []CompressionCodecType{CodecGzip, CodecSnappy, CodecLz4, CodecZstd} {
		c, _ := DefaultCompressor(CompressionCodec{codec: codec})
		w := byteBuffers.Get().(*bytes.Buffer)
		w.Reset()
		c.Compress(w, in, 99)

		b.Run(fmt.Sprint(codec), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				d := DefaultDecompressor()
				d.Decompress(w.Bytes(), codec)
			}
		})
		byteBuffers.Put(w)
	}
}

func Test_xerialDecode(t *testing.T) {
	tests := []struct {
		name            string
		compressedInput string
		want            []byte
		wantErr         bool
	}{
		{
			"Compressed data ok",
			"glNOQVBQWQAAAAABAAAAAQAAAA8NMEhlbGxvLCBXb3JsZCE=",
			[]byte("Hello, World!"),
			false,
		},
		{
			"Compressed data without header",
			"/wYAAHNOYVBwWQERAACChVPDSGVsbG8sIFdvcmxkIQ==",
			nil,
			true,
		},
		{
			"Compressed data less than minimum length, malformed",
			"glNOQVBQWQAAAAABAAAAAQAAAA==",
			nil,
			true,
		},
		{
			"Compressed data not the advertised length",
			"glNOQVBQWQAAAAABAAAAAQAAAA8NMEhlbGxvLCBXb3Js",
			nil,
			true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := base64.StdEncoding.DecodeString(test.compressedInput)
			if err != nil {
				t.Errorf("base64 decode error = %v", err)
				return
			}
			got, err := xerialDecode(data)
			if (err != nil) != test.wantErr {
				t.Errorf("xerialDecode() error = %v, wantErr %v", err, test.wantErr)
				return
			}
			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("got decompress %s != exp compress in %s", got, test.want)
			}
		})
	}
}
