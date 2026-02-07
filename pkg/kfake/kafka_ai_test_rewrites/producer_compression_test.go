// Derived from Apache Kafka's ProducerCompressionTest.java (Apache 2.0).
// https://github.com/apache/kafka/blob/trunk/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/producer/ProducerCompressionTest.java

package kafka_ai_test_rewrites

import (
	"bytes"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestCompression verifies produce/consume round-trip with each compression
// codec: none, gzip, snappy, lz4. Verifies record keys, values, and headers
// are preserved.
func TestCompression(t *testing.T) {
	t.Parallel()

	codecs := []struct {
		name  string
		codec kgo.CompressionCodec
	}{
		{"none", kgo.NoCompression()},
		{"gzip", kgo.GzipCompression()},
		{"snappy", kgo.SnappyCompression()},
		{"lz4", kgo.Lz4Compression()},
	}

	for _, tc := range codecs {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			topic := "compression-" + tc.name
			c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))

			producer := newClient(t, c,
				kgo.DefaultProduceTopic(topic),
				kgo.ProducerBatchCompression(tc.codec),
			)

			numRecords := 10
			var produced []*kgo.Record
			for i := range numRecords {
				r := &kgo.Record{
					Key:   []byte("key-" + strconv.Itoa(i)),
					Value: []byte("value-" + strconv.Itoa(i)),
					Headers: []kgo.RecordHeader{
						{Key: "h1", Value: []byte("hv-" + strconv.Itoa(i))},
					},
				}
				produced = append(produced, r)
			}
			produceSync(t, producer, produced...)

			consumer := newClient(t, c, kgo.ConsumeTopics(topic))
			records := consumeN(t, consumer, numRecords, 5*time.Second)

			if len(records) != numRecords {
				t.Fatalf("expected %d records, got %d", numRecords, len(records))
			}

			for i, r := range records {
				expKey := []byte("key-" + strconv.Itoa(i))
				expVal := []byte("value-" + strconv.Itoa(i))
				if !bytes.Equal(r.Key, expKey) {
					t.Errorf("record %d: key mismatch: %q != %q", i, r.Key, expKey)
				}
				if !bytes.Equal(r.Value, expVal) {
					t.Errorf("record %d: value mismatch: %q != %q", i, r.Value, expVal)
				}
				if len(r.Headers) != 1 {
					t.Errorf("record %d: expected 1 header, got %d", i, len(r.Headers))
					continue
				}
				expHdr := []byte("hv-" + strconv.Itoa(i))
				if r.Headers[0].Key != "h1" || !bytes.Equal(r.Headers[0].Value, expHdr) {
					t.Errorf("record %d: header mismatch: %v", i, r.Headers[0])
				}
			}
		})
	}
}
