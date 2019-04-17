package kgo

import (
	"hash/crc32"
	"sync"

	"github.com/twmb/kgo/kbin"
	"github.com/twmb/kgo/kmsg"
)

// promisedRecord ties a record with the callback that will be called once
// a batch is finally written and receives a response.
type promisedRecord struct {
	promise func(*Record, error)
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
		producerID:     bt.br.cl.producer.id,
		producerEpoch:  bt.br.cl.producer.epoch,
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

// Below here lies our custom encoding of record batches.

var crc32c = crc32.MakeTable(crc32.Castagnoli) // record crc's use Castagnoli table

// produceRequest is a kmsg.Request that is used when we want to
// flush our buffered records.
//
// It is the same as kmsg.ProduceRequest, but with a custom AppendTo.
type produceRequest struct {
	version int16

	acks             int16
	timeout          int32
	topicsPartitions map[string]map[int32]topparBatch

	compression []CompressionCodec
}

func (*produceRequest) Key() int16           { return 0 }
func (*produceRequest) MaxVersion() int16    { return 7 }
func (*produceRequest) MinVersion() int16    { return 3 }
func (p *produceRequest) SetVersion(v int16) { p.version = v }
func (p *produceRequest) GetVersion() int16  { return p.version }
func (p *produceRequest) AppendTo(dst []byte) []byte {
	if p.version >= 3 {
		dst = kbin.AppendNullableString(dst, nil) // TODO transactional ID
	}

	compressor := loadProduceCompressor(p.compression, p.version)

	dst = kbin.AppendInt16(dst, p.acks)
	dst = kbin.AppendInt32(dst, p.timeout)
	dst = kbin.AppendArrayLen(dst, len(p.topicsPartitions))
	for topic, partitions := range p.topicsPartitions {
		dst = kbin.AppendString(dst, topic)
		dst = kbin.AppendArrayLen(dst, len(partitions))
		for partition, batch := range partitions {
			dst = kbin.AppendInt32(dst, partition)
			dst = batch.appendTo(dst, compressor)
		}
	}
	return dst
}

func (p *produceRequest) ResponseKind() kmsg.Response {
	return &kmsg.ProduceResponse{Version: p.version}
}

func (r *recordBatch) appendTo(dst []byte, compressor *compressor) []byte {
	nullableBytesLen := r.wireLength - 4 // NULLABLE_BYTES leading length, minus itself
	nullableBytesLenAt := len(dst)       // in case compression adjusting
	dst = kbin.AppendInt32(dst, nullableBytesLen)

	dst = kbin.AppendInt64(dst, 0) // firstOffset, defined as zero for producing

	batchLen := nullableBytesLen - 8 - 4 // minus baseOffset, minus self
	batchLenAt := len(dst)               // in case compression adjusting
	dst = kbin.AppendInt32(dst, batchLen)

	dst = kbin.AppendInt32(dst, -1) // partitionLeaderEpoch, unused in clients
	dst = kbin.AppendInt8(dst, 2)   // magic, defined as 2 for records v0.11.0.0+

	crcStart := len(dst)           // fill at end
	dst = kbin.AppendInt32(dst, 0) // reserved crc

	attrsAt := len(dst) // in case compression adjusting
	attrs := r.attrs
	dst = kbin.AppendInt16(dst, attrs)
	dst = kbin.AppendInt32(dst, int32(len(r.records)-1)) // lastOffsetDelta
	dst = kbin.AppendInt64(dst, r.firstTimestamp)

	// maxTimestamp is the timestamp of the last record in a batch.
	lastRecord := r.records[len(r.records)-1]
	dst = kbin.AppendInt64(dst, r.firstTimestamp+int64(lastRecord.n.timestampDelta))

	dst = kbin.AppendInt64(dst, r.producerID)
	dst = kbin.AppendInt16(dst, r.producerEpoch)
	dst = kbin.AppendInt32(dst, r.baseSequence)

	dst = kbin.AppendArrayLen(dst, len(r.records))
	recordsAt := len(dst)
	for _, pnr := range r.records {
		dst = pnr.appendTo(dst)
	}

	if compressor != nil {
		toCompress := dst[recordsAt:]
		zipr := compressor.getZipr()
		defer compressor.putZipr(zipr)

		compressed := zipr.compress(toCompress)
		if compressed != nil && // nil would be from an error
			len(compressed) < len(toCompress) {

			// our compressed was shorter: copy over
			copy(dst[recordsAt:], compressed)
			dst = dst[:recordsAt+len(compressed)]

			// update the few record batch fields we already wrote
			savings := int32(len(toCompress) - len(compressed))
			nullableBytesLen -= savings
			batchLen -= savings
			attrs |= int16(compressor.attrs)
			kbin.AppendInt32(dst[:nullableBytesLenAt], nullableBytesLen)
			kbin.AppendInt32(dst[:batchLenAt], batchLen)
			kbin.AppendInt16(dst[:attrsAt], attrs)
		}
	}

	kbin.AppendInt32(dst[:crcStart], int32(crc32.Checksum(dst[crcStart+4:], crc32c)))

	return dst
}

func (pnr promisedNumberedRecord) appendTo(dst []byte) []byte {
	dst = kbin.AppendVarint(dst, pnr.n.lengthField)
	dst = kbin.AppendInt8(dst, 0) // attributes, currently unused
	dst = kbin.AppendVarint(dst, pnr.n.timestampDelta)
	dst = kbin.AppendVarint(dst, pnr.n.offsetDelta)
	dst = kbin.AppendVarintBytes(dst, pnr.pr.r.Key)
	dst = kbin.AppendVarintBytes(dst, pnr.pr.r.Value)
	dst = kbin.AppendVarint(dst, int32(len(pnr.pr.r.Headers)))
	for _, h := range pnr.pr.r.Headers {
		dst = kbin.AppendVarintString(dst, h.Key)
		dst = kbin.AppendVarintBytes(dst, h.Value)
	}
	return dst
}
