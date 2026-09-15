// Derived via LLM from Apache Kafka's ListOffsetsIntegrationTest.java (Apache 2.0).
// https://github.com/apache/kafka/blob/trunk/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/admin/ListOffsetsIntegrationTest.java

package kafka_tests

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestListMaxTimestamp verifies that ListMaxTimestampOffsets finds the
// record with the highest timestamp, whatever codec it arrived under and
// however it was batched.
func TestListMaxTimestamp(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		codec       kgo.CompressionCodec
		firstMillis int64
		oneBatch    bool
	}{
		{"uncompressed-one-batch", kgo.NoCompression(), 10_000, true},
		{"compressed-one-batch", kgo.GzipCompression(), 20_000, true},
		{"uncompressed-separate-batches", kgo.NoCompression(), 30_000, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			topic := "list-offsets-" + tc.name
			c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))

			producer := newClient848(t, c,
				kgo.DefaultProduceTopic(topic),
				kgo.ProducerBatchCompression(tc.codec),
			)

			var rs []*kgo.Record
			for i := range 3 {
				rs = append(rs, &kgo.Record{
					Value:     []byte("v" + strconv.Itoa(i+1)),
					Timestamp: time.UnixMilli(tc.firstMillis + int64(i)),
				})
			}
			if tc.oneBatch {
				produceSync(t, producer, rs...)
			} else {
				// One ProduceSync per record is one batch per record.
				for _, r := range rs {
					produceSync(t, producer, r)
				}
			}

			adm := newAdminClient(t, c)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			offsets, err := adm.ListMaxTimestampOffsets(ctx, topic)
			if err != nil {
				t.Fatalf("ListMaxTimestampOffsets failed: %v", err)
			}
			o, ok := offsets.Lookup(topic, 0)
			if !ok {
				t.Fatal("partition 0 not found in response")
			}
			// The last record carries the max timestamp, at offset 2.
			if o.Offset != 2 {
				t.Errorf("expected offset 2 (max timestamp record), got %d", o.Offset)
			}
			if want := tc.firstMillis + 2; o.Timestamp != want {
				t.Errorf("expected timestamp %d, got %d", want, o.Timestamp)
			}
		})
	}
}
