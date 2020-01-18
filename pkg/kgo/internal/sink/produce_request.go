package sink

import (
	"hash/crc32"
	"sync/atomic"

	"github.com/twmb/kafka-go/pkg/kbin"
	"github.com/twmb/kafka-go/pkg/kgo/internal/compress"
	"github.com/twmb/kafka-go/pkg/kmsg"
	"github.com/twmb/kafka-go/pkg/krec"
)

// produceRequest is a kmsg.Request that is used when we want to
// flush our buffered records.
//
// It is the same as kmsg.ProduceRequest, but with a custom AppendTo.
type produceRequest struct {
	version int16

	backoffSeq uint32

	txnID   *string
	acks    int16
	timeout int32
	batches reqBatches

	producerID    int64
	producerEpoch int16

	compressor *compress.Compressor
}

type reqBatches map[string]map[int32]*recBatch

func (rbs *reqBatches) addBatch(topic string, part int32, batch *recBatch) {
	if *rbs == nil {
		*rbs = make(reqBatches, 5)
	}
	topicBatches, exists := (*rbs)[topic]
	if !exists {
		topicBatches = make(map[int32]*recBatch, 1)
		(*rbs)[topic] = topicBatches
	}
	topicBatches[part] = batch
}

// This function runs fn on each batch if the batch is the first batch in
// the owning recBuf.
func (rbs reqBatches) onEachFirstBatchWhileBatchOwnerLocked(fn func(*recBatch)) {
	for _, partitions := range rbs {
		for _, batch := range partitions {
			batch.owner.mu.Lock()
			if batch.lockedIsFirstBatch() {
				fn(batch)
			}
			batch.owner.mu.Unlock()
		}
	}
}

func (*produceRequest) Key() int16           { return 0 }
func (*produceRequest) MaxVersion() int16    { return 8 }
func (p *produceRequest) SetVersion(v int16) { p.version = v }
func (p *produceRequest) GetVersion() int16  { return p.version }
func (p *produceRequest) IsFlexible() bool   { return false } // version 8 is not flexible
func (p *produceRequest) AppendTo(dst []byte) []byte {
	if p.version >= 3 {
		dst = kbin.AppendNullableString(dst, p.txnID)
	}

	dst = kbin.AppendInt16(dst, p.acks)
	dst = kbin.AppendInt32(dst, p.timeout)
	dst = kbin.AppendArrayLen(dst, len(p.batches))

	for topic, partitions := range p.batches {
		dst = kbin.AppendString(dst, topic)
		dst = kbin.AppendArrayLen(dst, len(partitions))
		for partition, batch := range partitions {
			dst = kbin.AppendInt32(dst, partition)
			if p.version < 3 {
				dst = batch.appendToAsMessageSet(dst, p.version, p.compressor)
			} else {
				dst = batch.appendTo(
					dst,
					p.version,
					p.compressor,
					p.producerID,
					p.producerEpoch,
					p.txnID != nil,
				)
			}
		}
	}
	return dst
}

func (p *produceRequest) ResponseKind() kmsg.Response {
	return &kmsg.ProduceResponse{Version: p.version}
}

func (r *recBatch) appendTo(
	dst []byte,
	version int16,
	compressor *compress.Compressor,
	producerID int64,
	producerEpoch int16,
	transactional bool,
) []byte {
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
	if transactional {
		r.attrs |= 0x0010 // bit 5 is the "is transactional" bit
	}
	attrs := r.attrs
	dst = kbin.AppendInt16(dst, attrs)
	dst = kbin.AppendInt32(dst, int32(len(r.records)-1)) // lastOffsetDelta
	dst = kbin.AppendInt64(dst, r.firstTimestamp)

	// maxTimestamp is the timestamp of the last record in a batch.
	lastRecord := r.records[len(r.records)-1]
	dst = kbin.AppendInt64(dst, r.firstTimestamp+int64(lastRecord.timestampDelta))

	dst = kbin.AppendInt64(dst, producerID)
	dst = kbin.AppendInt16(dst, producerEpoch)
	dst = kbin.AppendInt32(dst, atomic.LoadInt32(&r.baseSequence)) // read atomically in case of concurrent reset

	dst = kbin.AppendArrayLen(dst, len(r.records))
	recordsAt := len(dst)
	for i, pnr := range r.records {
		dst = pnr.appendTo(dst, int32(i))
	}

	if compressor != nil {
		toCompress := dst[recordsAt:]
		buf := compressor.GetBuf()
		defer compressor.PutBuf(buf)

		compressed, codec := compressor.Compress(buf, toCompress, version)
		if compressed != nil && // nil would be from an error
			len(compressed) < len(toCompress) {

			// our compressed was shorter: copy over
			copy(dst[recordsAt:], compressed)
			dst = dst[:recordsAt+len(compressed)]

			// update the few record batch fields we already wrote
			savings := int32(len(toCompress) - len(compressed))
			nullableBytesLen -= savings
			batchLen -= savings
			attrs |= int16(codec)
			kbin.AppendInt32(dst[:nullableBytesLenAt], nullableBytesLen)
			kbin.AppendInt32(dst[:batchLenAt], batchLen)
			kbin.AppendInt16(dst[:attrsAt], attrs)
		}
	}

	kbin.AppendInt32(dst[:crcStart], int32(crc32.Checksum(dst[crcStart+4:], crc32c)))

	return dst
}

var crc32c = crc32.MakeTable(crc32.Castagnoli) // record crc's use Castagnoli table

func (pnr promisedNumberedRecord) appendTo(dst []byte, offsetDelta int32) []byte {
	dst = kbin.AppendVarint(dst, pnr.lengthField)
	dst = kbin.AppendInt8(dst, 0) // attributes, currently unused
	dst = kbin.AppendVarint(dst, pnr.timestampDelta)
	dst = kbin.AppendVarint(dst, offsetDelta)
	dst = kbin.AppendVarintBytes(dst, pnr.Key)
	dst = kbin.AppendVarintBytes(dst, pnr.Value)
	dst = kbin.AppendVarint(dst, int32(len(pnr.Headers)))
	for _, h := range pnr.Headers {
		dst = kbin.AppendVarintString(dst, h.Key)
		dst = kbin.AppendVarintBytes(dst, h.Value)
	}
	return dst
}

func (r *recBatch) appendToAsMessageSet(dst []byte, version int16, compressor *compress.Compressor) []byte {
	nullableBytesLenAt := len(dst)
	dst = append(dst, 0, 0, 0, 0) // nullable bytes len
	for i, pnr := range r.records {
		dst = appendMessageTo(
			dst,
			uint8(version),
			0,
			int64(i),
			r.firstTimestamp+int64(pnr.timestampDelta),
			pnr.Rec,
		)
	}

	if compressor != nil {
		toCompress := dst[nullableBytesLenAt+4:] // skip nullable bytes leading prefix
		buf := compressor.GetBuf()
		defer compressor.PutBuf(buf)

		compressed, codec := compressor.Compress(buf, toCompress, version)
		inner := &krec.Rec{Value: compressed}
		wrappedLength := messageSet0Length(inner)
		if version == 2 {
			wrappedLength += 8 // timestamp
		}

		if compressed != nil &&
			int(wrappedLength) < len(toCompress) {

			dst = appendMessageTo(
				dst[:nullableBytesLenAt+4],
				uint8(version),
				codec,
				int64(len(r.records)-1),
				r.firstTimestamp,
				inner,
			)
		}
	}

	kbin.AppendInt32(dst[:nullableBytesLenAt], int32(len(dst[nullableBytesLenAt+4:])))
	return dst
}

func appendMessageTo(
	dst []byte,
	version uint8,
	attributes int8,
	offset int64,
	timestamp int64,
	r *krec.Rec,
) []byte {
	magic := version >> 1
	dst = kbin.AppendInt64(dst, offset)
	msgSizeStart := len(dst)
	dst = append(dst, 0, 0, 0, 0)
	crc32Start := len(dst)
	dst = append(dst, 0, 0, 0, 0)
	dst = append(dst, magic)
	dst = append(dst, byte(attributes))
	if magic == 1 {
		dst = kbin.AppendInt64(dst, timestamp)
	}
	dst = kbin.AppendNullableBytes(dst, r.Key)
	dst = kbin.AppendNullableBytes(dst, r.Value)
	kbin.AppendInt32(dst[:crc32Start], int32(crc32.ChecksumIEEE(dst[crc32Start+4:])))
	kbin.AppendInt32(dst[:msgSizeStart], int32(len(dst[msgSizeStart+4:])))
	return dst
}
