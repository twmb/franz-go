package kgo

import (
	"sync"

	"github.com/twmb/kgo/kbin"
)

// promisedRecord ties a record with the callback that will be called once
// a batch is finally written and receives a response.
type promisedRecord struct {
	promise func(string, *Record, error)
	r       *Record
}

// recordNumbers tracks a few numbers for a record that is buffered.
type recordNumbers struct {
	wireLength     int32
	lengthField    int32
	timestampDelta int32
	offsetDelta    int32
}

// promisedNumberedRecord ties a promisedRecord to its calculated numbers.
type promisedNumberedRecord struct {
	n  recordNumbers
	pr promisedRecord
}

var noPNR promisedNumberedRecord
var emptyRecordsPool = sync.Pool{
	New: func() interface{} {
		return make([]promisedNumberedRecord, 0, 500)
	},
}

// newRecordBatch returns a new record batch for a topic and partition
// containing the given record.
func (bt *brokerToppars) newRecordBatch(firstSeq int32, pr promisedRecord) *recordBatch {
	const recordBatchOverhead = 4 + // NULLABLE_BYTES overhead
		8 + // firstOffset
		4 + // batchLength
		4 + // partitionLeaderEpoch
		1 + // magic
		4 + // crc
		2 + // attributes
		4 + // lastOffsetDelta
		8 + // firstTimestamp
		8 + // maxTimestamp
		8 + // producerID
		2 + // producerEpoch
		4 + // baseSequence
		4 // record array length
	b := &recordBatch{
		firstTimestamp: pr.r.Timestamp.UnixNano() / 1e6,
		records:        emptyRecordsPool.Get().([]promisedNumberedRecord),
		producerID:     bt.br.cl.producerID,
		producerEpoch:  bt.br.cl.producerEpoch,
		baseSequence:   firstSeq,
	}
	pnr := promisedNumberedRecord{
		n:  b.calculateRecordNumbers(pr.r),
		pr: pr,
	}
	b.records = append(b.records, pnr)
	b.wireLength = recordBatchOverhead + pnr.n.wireLength
	return b
}

// appendRecord saves a new record to a batch.
func (b *recordBatch) appendRecord(pr promisedRecord, nums recordNumbers) {
	b.wireLength += nums.wireLength
	b.records = append(b.records, promisedNumberedRecord{
		n:  nums,
		pr: pr,
	})
}

// recordBatch is the type used for buffering records before they are written.
type recordBatch struct {
	tried bool // if this was sent before and is thus now immutable

	wireLength int32 // tracks total size this batch would currently encode as

	attrs          int16
	firstTimestamp int64 // since unix epoch, in millis

	// The following three are used for idempotent message delivery.
	producerID    int64
	producerEpoch int16
	baseSequence  int32

	records []promisedNumberedRecord
}

// calculateRecordNumbers returns the record numbers for a record if it were
// added to the record batch.
//
// No attempt is made to calculate overflows here; that should be done prior.
func (b *recordBatch) calculateRecordNumbers(r *Record) recordNumbers {
	tsMillis := r.Timestamp.UnixNano() / 1e6
	tsDelta := int32(tsMillis - b.firstTimestamp)
	offsetDelta := int32(len(b.records)) // since called before adding record, delta is the current end

	l := 1 + // attributes, int8 unused
		kbin.VarintLen(int64(tsDelta)) +
		kbin.VarintLen(int64(offsetDelta)) +
		kbin.VarintLen(int64(len(r.Key))) +
		len(r.Key) +
		kbin.VarintLen(int64(len(r.Value))) +
		len(r.Value) +
		kbin.VarintLen(int64(len(r.Headers))) // int32 array len headers

	for _, h := range r.Headers {
		l += kbin.VarintLen(int64(len(h.Key))) +
			len(h.Key) +
			kbin.VarintLen(int64(len(h.Value))) +
			len(h.Value)
	}

	return recordNumbers{
		wireLength:     int32(kbin.VarintLen(int64(l)) + l),
		lengthField:    int32(l),
		timestampDelta: tsDelta,
		offsetDelta:    offsetDelta,
	}
}
