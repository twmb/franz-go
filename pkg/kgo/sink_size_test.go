package kgo

import (
	"context"
	"testing"
	"time"
)

// A full batch must fit in a produce request at the version it was sized
// for. If it does not, createReq never stages it and nothing fails it, so
// the sink spins draining forever and ProduceSync never returns. Batches are
// at their tightest when the record batch limit is derived from
// BrokerMaxWriteBytes, and whether the last record overshoots depends on its
// size, so sweep record sizes.
func TestFullBatchFitsProduceRequest(t *testing.T) {
	t.Parallel()

	const limit = 1 << 10
	ts := time.Now()
	for _, version := range []int32{-1, 0, 2, 9} {
		for valueSize := 1; valueSize <= 200; valueSize++ {
			_, s, r := sinkHarness(t, ProducerBatchMaxBytes(limit), BrokerMaxWriteBytes(limit))
			s.produceVersion.Store(version)
			r.maxRecordBatchBytes = r.cl.maxRecordBatchBytesForTopic(r.topic)

			value := make([]byte, valueSize)
			for len(r.batches) < 2 {
				r.bufferRecord(promisedRec{
					ctx:     context.Background(),
					promise: func(*Record, error) {},
					Record:  &Record{Value: value, Timestamp: ts, Context: context.Background()},
				}, false)
			}

			head := r.batches[0]
			if size, _, _ := head.wireLengthForProduceVersion(version); size > r.maxRecordBatchBytes {
				t.Errorf("v%d value size %d: batch of %d records sizes to %d, over the %d limit", version, valueSize, len(head.records), size, r.maxRecordBatchBytes)
			}
			if req, _, _ := s.createReq(5, 0); len(req.batches.bs) != 1 {
				t.Errorf("v%d value size %d: full batch of %d records does not fit in a produce request", version, valueSize, len(head.records))
			}
		}
	}
}
