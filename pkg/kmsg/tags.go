package kmsg

import "github.com/twmb/kafka-go/pkg/kbin"

// SkipTags skips tags in a reader.
func SkipTags(b *kbin.Reader) {
	num := b.Uvarint()
	if num == 0 {
		return
	}

	for ; num > 0; num-- {
		_ = b.Uvarint() // tag
		size := b.Uvarint()
		b.Span(int(size))
	}
}
