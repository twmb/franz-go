package kgo

import (
	"hash/crc32"

	"github.com/twmb/kgo/kbin"
	"github.com/twmb/kgo/kmsg"
)

var crc32c = crc32.MakeTable(crc32.Castagnoli) // record crc's use Castagnoli table

// messageBufferedProduceRequest is a kmsg.Request that is used when we want to
// flush our buffered records.
//
// It is the same as kmsg.ProduceRequest, but with a custom AppendTo.
type messageBufferedProduceRequest struct {
	version int16

	acks    int16
	timeout int32
	data    map[string]map[int32]*recordBatch
}

func (*messageBufferedProduceRequest) Key() int16           { return 0 }
func (*messageBufferedProduceRequest) MaxVersion() int16    { return 7 }
func (*messageBufferedProduceRequest) MinVersion() int16    { return 3 }
func (m *messageBufferedProduceRequest) SetVersion(v int16) { m.version = v }
func (m *messageBufferedProduceRequest) GetVersion() int16  { return m.version }
func (m *messageBufferedProduceRequest) AppendTo(dst []byte) []byte {
	if m.version >= 3 {
		dst = kbin.AppendNullableString(dst, nil) // TODO transactional ID
	}
	dst = kbin.AppendInt16(dst, m.acks)
	dst = kbin.AppendInt32(dst, m.timeout)
	dst = kbin.AppendArrayLen(dst, len(m.data))
	for topic, partitions := range m.data {
		dst = kbin.AppendString(dst, topic)
		dst = kbin.AppendArrayLen(dst, len(partitions))
		for partition, batch := range partitions {
			dst = kbin.AppendInt32(dst, partition)
			dst = batch.appendTo(dst)
		}
	}
	return dst
}

func (m *messageBufferedProduceRequest) ResponseKind() kmsg.Response {
	return &kmsg.ProduceResponse{Version: m.version}
}

func (r *recordBatch) appendTo(dst []byte) []byte {
	dst = kbin.AppendInt32(dst, r.wireLength-4) // NULLABLE_BYTES leading length, minus itself
	dst = kbin.AppendInt64(dst, 0)              // firstOffset, defined as zero for producing

	dst = kbin.AppendInt32(dst, r.wireLength-4-8-4) // minus nullable bytes, minus baseOffset, minus self

	dst = kbin.AppendInt32(dst, -1) // partitionLeaderEpoch, unused in clients
	dst = kbin.AppendInt8(dst, 2)   // magic, defined as 2 for records v0.11.0.0+

	crcStart := len(dst)           // fill at end
	dst = kbin.AppendInt32(dst, 0) // reserved crc

	dst = kbin.AppendInt16(dst, r.attrs)
	dst = kbin.AppendInt32(dst, int32(len(r.records)-1)) // lastOffsetDelta
	dst = kbin.AppendInt64(dst, r.firstTimestamp)

	// maxTimestamp is the timestamp of the last record in a batch.
	lastRecord := r.records[len(r.records)-1]
	dst = kbin.AppendInt64(dst, r.firstTimestamp+int64(lastRecord.n.timestampDelta))

	dst = kbin.AppendInt64(dst, -1) // producerId
	dst = kbin.AppendInt16(dst, -1) // producerEpoch
	dst = kbin.AppendInt32(dst, -1) // baseSequence

	// TODO compression: grab len here, compress, overwrite if shorter
	// and update attr
	dst = kbin.AppendArrayLen(dst, len(r.records))
	for _, pnr := range r.records {
		dst = pnr.appendTo(dst)
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
