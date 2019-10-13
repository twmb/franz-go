package kmsg

import "github.com/twmb/kafka-go/pkg/kbin"

// SkipTags skips tags in a reader.
func SkipTags(b *kbin.Reader) {
	for num := b.Uvarint(); num > 0; num-- {
		_, size := b.Uvarint(), b.Uvarint()
		b.Span(int(size))
	}
}
