package kgo

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// A batch whose records were concurrently taken (failAllRecords) or that is
// failing from a load error serializes as a null-records placeholder. On
// flexible versions (v9+), that placeholder previously skipped the
// per-partition tagged-field terminator, leaving the request one byte short
// per failed batch and desyncing the broker's parse of the entire request.
// Round-trip the hand-rolled encoding through kmsg's generated decoder to
// pin the wire shape.
func TestProduceRequestNullBatchEncoding(t *testing.T) {
	t.Parallel()

	for _, version := range []int16{8, 9} { // last non-flexible, first flexible
		req := &produceRequest{
			version: version,
			acks:    -1,
			timeout: 1000,
		}
		req.batches.add("t", [16]byte{1}, 0, 0, seqRecBatch{0, &recBatch{}}) // records nil: the null placeholder arm
		req.batches.add("t", [16]byte{1}, 0, 1, seqRecBatch{0, &recBatch{}})

		raw := req.AppendTo(nil)

		var decoded kmsg.ProduceRequest
		decoded.SetVersion(version)
		if err := decoded.ReadFrom(raw); err != nil {
			t.Errorf("v%d: hand-rolled produce request does not decode: %v", version, err)
			continue
		}
		if len(decoded.Topics) != 1 || len(decoded.Topics[0].Partitions) != 2 {
			t.Errorf("v%d: decoded %d topics / %v partitions, want 1 topic with 2 partitions", version, len(decoded.Topics), decoded.Topics)
			continue
		}
		for _, p := range decoded.Topics[0].Partitions {
			if p.Records != nil {
				t.Errorf("v%d partition %d: records not null", version, p.Partition)
			}
		}
	}
}
