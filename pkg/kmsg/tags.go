package kmsg

import "github.com/twmb/kafka-go/pkg/kbin"

// until a struct has a tag, we skip tags; once we see what tags looks like
// with an example, we will properly design the API.
func skipTags(b *kbin.Reader) {
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
