package kgo

import (
	"fmt"
	"hash/crc32"
	"math"
	"time"
)

// Record crcs use the castagnoli table
var crc32c = crc32.MakeTable(crc32.Castagnoli)

// promisedRecord ties a record with the callback that will be called once
// a batch is finally written and receives a response.
type promisedRecord struct {
	promise func(string, *Record)
	r       *Record
}

// bufferedRecords is a produceRequest before it turns into a produce request
// on the wire.
type bufferedRecords struct {
	version int16

	// length tracks how long everything that is buffered is, were this
	// written as a produce request.
	length int

	// forceTimer contains an AfterFunc that will force a
	// flush for all buffered messages.
	flushTimer        *time.Timer
	flushTimerRunning bool
	flushSeq          uint64

	// transactionalID  *string  unused and unaccounted for right now
	// acks             int16    tracked in client itself
	// timeout          int32    unused right now

	batches map[string]map[int32]*recordBatch // topic => partition => batch
}

/*
func (b *bufferedRecords) buffer(c *Client, topic string, p promisedRecord) {
	b.mu.Lock()
	defer b.mu.Unlock()

	// TODO check write size
	batch, exists := b.batches[topic]
	if !exists {
	}
}
*/

// reset resets the buffered records to an empty state, resetting its length to
// what the minimum length is for a produce request.
func (b *bufferedRecords) reset() {
	b.batches = make(map[string]map[int32]*recordBatch, 2)
	// TODO if adding transactionalID, increase length here
	b.length = 2 + 4 + 4 // acks + timeout + batches array len
}

// messageBufferedProduce is a messageRequestKind that is used when we want to
// flush our buffered records.
//
// It is the same as messageProduceRequest, but with a custom appendTo.
type messageBufferedProduce struct {
	b       bufferedRecords
	acks    int16
	timeout int32
}

func (*messageBufferedProduce) key() int16           { return 0 }
func (*messageBufferedProduce) maxVersion() int16    { return 7 }
func (*messageBufferedProduce) minVersion() int16    { return 3 }
func (m *messageBufferedProduce) setVersion(v int16) { m.b.version = v }
func (m *messageBufferedProduce) appendTo(dst []byte) []byte {
	return m.b.appendTo(dst, m.acks, m.timeout)
}
func (m *messageBufferedProduce) responseKind() messageResponseKind {
	return &produceResponse{version: m.b.version}
}

func (b *bufferedRecords) appendTo(dst []byte, acks int16, timeout int32) []byte {
	// TODO if adding transactionalID, encode
	dst = appendInt16(dst, acks)
	dst = appendInt32(dst, timeout)
	dst = appendArrayLen(dst, len(b.batches))
	for topic, partitions := range b.batches {
		dst = appendString(dst, topic)
		dst = appendArrayLen(dst, len(partitions))
		for partition, batch := range partitions {
			dst = appendInt32(dst, partition)
			dst = appendInt32(dst, int32(batch.fullLength()))
			dst = batch.appendTo(dst)
		}
	}
	return dst
}

// newRecordBatch returns a new record batch for a topic and partition.
//
// The returned batch holds the given record.
func newRecordBatch(pr promisedRecord) *recordBatch {
	b := &recordBatch{
		firstTimestamp: pr.r.Timestamp.UnixNano() / 1e6,
		records:        make([]numberedRecord, 0, 10),
	}
	nr := numberedRecord{
		n:  b.calculateRecordNumbers(pr.r),
		pr: pr,
	}
	b.records = append(b.records, nr)
	b.curLength = recordBatchOverhead + nr.n.fullLength()
	return b
}

// lengthWithRecord returns how long a record batch would be with the new
// record.
//
// This also returns the new record's numbers which are calculated as a
// byproduct of calculating the extended batch length. These can be saved as
// necessary to avoid recalculation.
func (b *recordBatch) lengthWithRecord(r *Record) (int, recordNumbers) {
	length := b.curLength
	nums := b.calculateRecordNumbers(r)
	return length + nums.fullLength(), nums
}

// appendRecord saves a new record to a batch and updates the batch's full
// length.
func (b *recordBatch) appendRecord(pr promisedRecord, nums recordNumbers) {
	b.curLength += nums.fullLength()
	b.records = append(b.records, numberedRecord{
		n:  nums,
		pr: pr,
	})
}

// recordBatch is the type used for buffering records before they are written.
//
// This is nearly the data type used in actually writing produce requests, but
// we leave out some assumed defaults, and this batches numberedRecords.
//
// We keep track of how long everything inside the batch would encode as to
// aid in when-to-batch.
type recordBatch struct {
	curLength int // tracks total size this batch would currently encode as

	// firstOffset          int64  is defined as zero for producing
	// batchLength          int32  of what follows; calculated on write
	// partitionLeaderEpoch int32  used for Kafka cluster comms: we use -1
	// magic                int8   is defined as 2 for record batches
	// crc                  int32  of what follows; calculated on write

	attrs int16

	// lastOffsetDelta int32  defined as len(records)-1

	firstTimestamp int64 // since unix epoch, in millis

	// maxTimestamp int64  timestamp of the last record in a batch

	// The following three are used for idempotent message delivery
	// following an InitProducerId request.
	// producerId    int64  defined as -1 until we support it
	// producerEpoch int16  defined as -1 until we support it
	// baseSequence  int32  defined as -1 until we support it

	records []numberedRecord
}

// record batches are special NULLABLE_BYTES, meaning they are preceeded with
// an array length (which is int32).
func (r *recordBatch) fullLength() int {
	return r.curLength + 4
}

// calculateRecordNumbers returns the record numbers for a record if it were
// added to the record batch.
//
// Lengths of record fields are validated before this function, and then this
// function ensures that the int64 => int32 for the total record length does
// not overflow.
//
// No attempt is made to validate the timestamp delta if it over/underflows.
func (b *recordBatch) calculateRecordNumbers(r *Record) recordNumbers {
	tsMillis := r.Timestamp.UnixNano() / 1e6
	tsDelta := int32(tsMillis - b.firstTimestamp)
	offsetDelta := int32(len(b.records) - 1)

	l := 1 + // attributes, int8 unused
		varintLen(int64(tsDelta)) +
		varintLen(int64(offsetDelta)) +
		varintLen(int64(len(r.Key))) +
		len(r.Key) +
		varintLen(int64(len(r.Value))) +
		len(r.Value) +
		4 // int32 array len headers

	for _, h := range r.Headers {
		l += varintLen(int64(len(h.Key))) +
			len(h.Key) +
			varintLen(int64(len(h.Value))) +
			len(h.Value)
	}

	return recordNumbers{
		length:         int32(l),
		timestampDelta: tsDelta,
		offsetDelta:    offsetDelta,
	}
}

// appendTo appends a record batch for a produce request. This is separate from
// the messages protocol for ease and efficiency.
func (r *recordBatch) appendTo(dst []byte) []byte {
	dst = appendInt64(dst, 0) // firstOffset, defined as zero for producing

	lengthStart := len(dst)   // fill at end
	dst = appendInt32(dst, 0) // reserved length

	dst = appendInt32(dst, -1) // partitionLeaderEpoch, unused in clients
	dst = appendInt8(dst, 2)   // magic

	crcStart := len(dst)      // fill at end
	dst = appendInt32(dst, 0) // reserved crc

	dst = appendInt16(dst, r.attrs)
	dst = appendInt32(dst, int32(len(r.records)-1)) // lastOffsetDelta
	dst = appendInt64(dst, r.firstTimestamp)

	// maxTimestamp is the timestamp of the last record in a batch.
	dst = appendInt64(dst, r.firstTimestamp+
		int64(r.records[len(r.records)-1].n.timestampDelta))

	dst = appendInt64(dst, -1) // producerId
	dst = appendInt16(dst, -1) // producerEpoch
	dst = appendInt32(dst, -1) // baseSequence

	dst = appendArrayLen(dst, len(r.records))
	for _, nr := range r.records {
		dst = nr.appendTo(dst)
	}

	appendInt32(dst[lengthStart:], int32(len(dst[lengthStart+4:])))
	appendInt32(dst[crcStart:], int32(crc32.Checksum(dst[crcStart+4:], crc32c)))

	return dst
}

// recordBatchOverhead is the constant overhead of a record batch for a topic
// partition.
const recordBatchOverhead = 8 + // firstOffset
	4 + // batchLength
	4 + // partitionLeaderEpoch
	1 + // magic
	4 + // crc
	2 + // attributes
	4 + // lastOffsetDelta
	8 + // firstTimestamp
	8 + // maxTimestamp
	8 + // producerId
	2 + // producerEpoch
	4 + // baseSequence
	4 // record array length

// numberedRecord ties a promisedRecord to its numbers once they're calculated.
type numberedRecord struct {
	n  recordNumbers
	pr promisedRecord
}

// appendTo appends a record to a batch for a produce request. This is separate
// from the messages protocol for ease and efficiency.
func (nr numberedRecord) appendTo(dst []byte) []byte {
	dst = appendVarint(dst, nr.n.length)
	dst = appendInt8(dst, 0) // attributes, currently unused
	dst = appendVarint(dst, nr.n.timestampDelta)
	dst = appendVarint(dst, nr.n.offsetDelta)
	dst = appendVarintString(dst, nr.pr.r.Key)
	dst = appendVarintString(dst, nr.pr.r.Value)
	dst = appendArrayLen(dst, len(nr.pr.r.Headers))
	for _, h := range nr.pr.r.Headers {
		dst = appendVarintString(dst, h.Key)
		dst = appendVarintString(dst, h.Value)
	}
	return dst
}

// checkLengths returns whether any record field is too long for the length to
// be properly represented in a varint.
func (r *Record) checkLengths() error {
	var err error
	chk := func(name, s string) {
		if err != nil {
			return
		}
		if len(s) > math.MaxInt32 {
			err = fmt.Errorf("%s is too long: length %d > max %d", name, len(s), math.MaxInt32)
		}
	}
	chk("record key", r.Key)
	chk("record value", r.Value)
	for _, h := range r.Headers {
		chk("record header key", h.Key)
		chk("record header value", h.Value)
	}
	return err
}

// recordNumbers tracks a few numbers for a record that is buffered so that we
// do not need to recalculate them when records. Most importantly, the length
// field is is needed to ensure we do not write too much.
type recordNumbers struct {
	length         int32
	timestampDelta int32
	offsetDelta    int32
}

// fullLength returns the length of a record as it would be on the wire, which
// includes the length of the record's length field.
func (rn recordNumbers) fullLength() int {
	return varintLen(int64(rn.length)) + int(rn.length)
}

// RecordHeader contains extra information that can be sent with Records.
type RecordHeader struct {
	Key   string
	Value string
}

// Record is a record to write to Kafka.
type Record struct {
	// Key is an optional field that can be used for partition assignment.
	//
	// This is generally used with a hash partitioner to cause all records
	// with the same key to go to the same partition.
	Key string
	// Value is blob of data to write to Kafka.
	Value string

	// Headers are optional key/value pairs that are passed along with
	// records.
	//
	// These are purely for producers and consumers; Kafka does not look at
	// this field and only writes it to disk.
	Headers []RecordHeader

	// Timestamp is the timestamp that will be used for this record.
	//
	// Record batches are always written with "CreateTime", meaning that
	// timestamps are generated by clients rather than brokers.
	//
	// If this field is zero, the timestamp will be set in Produce.
	Timestamp time.Time

	// TimestampType specifies what type of timestamping to use.
	//
	// The default is CreateTime, but another option is LogAppendTime,
	// which ignores any set timestamp when writing a record and instead
	// uses the timestamp Kafka generates when storing the record.
	TimestampType TimestampType

	// Partition is the partition that a record is written to.
	//
	// For producing, this is left unset. If acks are required, this field
	// will be filled in before the produce callback if the produce is
	// successful.
	Partition int32
	// Offset is the offset that a record is written as.
	//
	// For producing, this is left unset. If acks are required, this field
	// will be filled in before the produce callback if the produce is
	// successful.
	Offset int64

	// NOTE: if logAppendTime, timestamp is MaxTimestamp, not first + delta
	// zendesk/ruby-kafka#706
}

// TimestampType signifies what type of timestamp is in a record.
type TimestampType struct {
	t int8
}

// TimestampCreateTime returns the CreateTime timestamp type.
func TimestampCreateTime() TimestampType { return TimestampType{0} }

// TimestampLogAppendTime returns the LogAppendTime timestamp type.
func TimestampLogAppendTime() TimestampType { return TimestampType{1} }

// IsNotAvailable returns if a timestamp type is unavailable for a record.
//
// Records before Kafka 0.11.0.0 did not have timestamp types.
func (t TimestampType) IsNotAvailable() bool { return t.t == -1 }

// IsCreateTime returns if a record's timestamp is from the time the record was
// created (in a client).
func (t TimestampType) IsCreateTime() bool { return t.t == 0 }

// IsLogAppendTime returns if a record's timestamp was generated within a Kafka
// broker.
func (t TimestampType) IsLogAppendTime() bool { return t.t == 1 }
