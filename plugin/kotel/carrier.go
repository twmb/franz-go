package kotel

import "github.com/twmb/franz-go/pkg/kgo"

// RecordCarrier injects and extracts traces from a kgo.Record.
type RecordCarrier struct {
	record *kgo.Record
}

// Get retrieves a single value for a given key.
func (c RecordCarrier) Get(key string) string {
	for _, h := range c.record.Headers {
		if h.Key == key {
			return string(h.Value)
		}
	}
	return ""
}

// Set sets a header.
func (c RecordCarrier) Set(key, val string) {
	// Ensure uniqueness of keys
	for i := 0; i < len(c.record.Headers); i++ {
		if c.record.Headers[i].Key == key {
			c.record.Headers = append(c.record.Headers[:i], c.record.Headers[i+1:]...)
			i--
		}
	}
	c.record.Headers = append(c.record.Headers, kgo.RecordHeader{
		Key:   key,
		Value: []byte(val),
	})
}

// Keys returns a slice of all key identifiers in the carrier.
func (c RecordCarrier) Keys() []string {
	out := make([]string, len(c.record.Headers))
	for i, h := range c.record.Headers {
		out[i] = h.Key
	}
	return out
}

// NewRecordCarrier creates a new RecordCarrier.
func NewRecordCarrier(record *kgo.Record) RecordCarrier {
	return RecordCarrier{record: record}
}
