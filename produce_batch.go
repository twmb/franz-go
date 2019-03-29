package kgo

import (
	"math/bits"
	"sync"
	"time"

	"github.com/twmb/kgo/kerr"
	"github.com/twmb/kgo/kmsg"
)

// TODO KIP-359: if broker LeaderEpoch known, set it in produce request
// and handle response errors

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

// bufferedProduceRequest is a produceRequest before it turns into a produce
// request on the wire.
type bufferedProduceRequest struct {
	br *broker

	mu sync.Mutex

	// wireLength tracks how long everything that is buffered is, were this
	// written as a produce request.
	wireLength int32
	nrecords   int

	// forceTimer contains an AfterFunc that will force a
	// flush for all buffered messages.
	flushTimer        *time.Timer
	flushTimerRunning bool
	flushSeq          uint64

	// transactionalID  *string  unused and unaccounted for right now
	// timeout          int32    unused right now

	batches map[string]map[int32]*recordBatch
}

func (b *bufferedProduceRequest) maybeFlush() {
	if b.nrecords > b.br.cl.cfg.producer.maxBrokerBufdRecs {
		b.flush()
		return
	}
	if b.wireLength > b.br.cl.cfg.producer.brokerBufBytes {
		b.flush()
		return
	}
	if !b.flushTimerRunning {
		b.beginFlushTimer()
	}
}

func (b *bufferedProduceRequest) beginFlushTimer() {
	b.flushTimerRunning = true
	seq := b.flushSeq
	b.flushTimer = time.AfterFunc(b.br.cl.cfg.producer.brokerBufDur, func() {
		b.mu.Lock()
		defer b.mu.Unlock()

		if b.flushSeq == seq {
			b.flush()
		}
	})
}

func (b *bufferedProduceRequest) timerFlush(seq uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.flushSeq == seq {
		b.flush()
		return
	}
}

func (b *bufferedProduceRequest) flush() {
	defer b.reset()

	data := b.batches
	req := &messageBufferedProduceRequest{
		acks:    b.br.cl.cfg.producer.acks.val,
		timeout: int32(time.Second / 1e6), // TODO
		data:    data,
	}
	// TODO doSequencedAsyncPromise
	// broker contains locked list,
	// first on list begins itself
	// when funcs finish, remove self from front, check if still front
	// and begin new
	b.br.do(
		req,
		func(resp kmsg.Response, err error) {
			if err != nil {
				for topic, partitions := range data {
					for _, batch := range partitions {
						for _, record := range batch.records {
							record.pr.promise(topic, record.pr.r, err)
						}
					}
				}
				return
			}

			pr := resp.(*kmsg.ProduceResponse)
			for _, responseTopic := range pr.Responses {
				topic := responseTopic.Topic
				partitions, ok := data[topic]
				if !ok {
					continue // ???
				}
				delete(data, topic)

				for _, responsePartition := range responseTopic.PartitionResponses {
					partition := responsePartition.Partition
					batch, ok := partitions[partition]
					if !ok {
						continue // ???
					}
					delete(partitions, partition)

					err := kerr.ErrorForCode(responsePartition.ErrorCode)
					// TODO:
					// retriable errors
					// duplicate sequence num
					for i, record := range batch.records {
						if err == nil {
							record.pr.r.Offset = int64(i)
							record.pr.r.Partition = partition
							if record.pr.r.TimestampType.IsLogAppendTime() {
								record.pr.r.Timestamp = time.Unix(0, responsePartition.LogAppendTime*1e4)
							}
						}
						record.pr.promise(topic, record.pr.r, err)
					}
				}

				for _, batch := range partitions {
					for _, record := range batch.records {
						record.pr.promise(topic, record.pr.r, errNoResp)
					}
				}
			}
			for topic, partitions := range data {
				for _, batch := range partitions {
					for _, record := range batch.records {
						record.pr.promise(topic, record.pr.r, errNoResp)
					}
				}
			}
		},
	)
}

func (b *bufferedProduceRequest) reset() {
	if b.flushTimerRunning {
		b.flushTimerRunning = false
		b.flushTimer.Stop()
		b.flushSeq++
	}

	b.batches = make(map[string]map[int32]*recordBatch, 1)
	// TODO if adding transactionalID, increase length here
	b.wireLength = 2 + 4 + 4 + // acks + timeout + batches array len
		messageRequestOverhead(b.br.cl.cfg.client.id)
}

func messageRequestOverhead(clientID *string) int32 {
	const base = 4 + 2 + 2 + 4
	if clientID == nil {
		return base + 2
	}
	return base + 2 + int32(len(*clientID))
}

type messageResponseKind interface {
	readFrom([]byte) error
}

func (b *bufferedProduceRequest) buffer(topic string, partition int32, pr promisedRecord) error {
	// TODO check record lengths.

	b.mu.Lock()
	defer b.mu.Unlock()

start:
	partitionBatches, exists := b.batches[topic]
	if !exists {
		newOverhead := 2 + int32(len(topic)) + // new topic string
			4 + // partitions array len
			4 // partition number
		newBatch, _, ok := b.newRecordBatch(topic, pr, newOverhead)
		if !ok {
			return errRecordTooLarge
		}

		// With or without flushing, we are creating this topic for this batch.
		b.wireLength += newOverhead + newBatch.wireLength
		b.nrecords++
		b.batches[topic] = map[int32]*recordBatch{partition: newBatch}
		b.maybeFlush()
		return nil
	}

	recordBatch, exists := partitionBatches[partition]
	if !exists {
		const newOverhead = 4 // new partition number
		newBatch, flushed, ok := b.newRecordBatch(topic, pr, newOverhead)
		if !ok {
			return errRecordTooLarge
		}
		if flushed {
			goto start // recreate the topic now that it is gone
		}

		// We did not flush and we can fit the new batch: keep it.
		b.wireLength += newOverhead + newBatch.wireLength
		b.nrecords++
		partitionBatches[partition] = newBatch
		b.maybeFlush()
		return nil
	}

	recordNums := recordBatch.calculateRecordNumbers(pr.r)
	if recordBatch.wireLength+recordNums.wireLength > b.br.cl.cfg.producer.maxRecordBatchBytes {
		b.flush()
		goto start // would overflow batch, flush and try again
	}
	canFit, flushed := b.tryFitLength(recordNums.wireLength)
	if !canFit {
		return errRecordTooLarge
	}
	if flushed {
		goto start // topic & partitions are gone
	}

	recordBatch.appendRecord(pr, recordNums)
	b.maybeFlush()
	return nil
}

// newRecordBatch tries to create a new recordBatch for a record.
//
// newOverhead is the extra overhead that will come along with creating this
// new batch.
//
// This returns the new batch, if a flush occured, and if the record can be
// kept or not due to size reasons.
func (b *bufferedProduceRequest) newRecordBatch(
	topic string, // only used for the promise
	pr promisedRecord,
	newOverhead int32,
) (*recordBatch, bool, bool) {
	newBatch := newRecordBatch(pr)
	if newBatch.wireLength > b.br.cl.cfg.producer.maxRecordBatchBytes {
		return nil, false, false
	}

	extraLength := newOverhead + newBatch.wireLength
	canFit, flushed := b.tryFitLength(extraLength)
	if !canFit {
		return nil, flushed, false
	}

	return newBatch, flushed, true
}

// Returns if some extra length can be buffered. If no, this tries flushing
// and checks again.
func (b *bufferedProduceRequest) tryFitLength(extraLength int32) (canFit, flushed bool) {
	maxBytes := b.br.cl.cfg.producer.maxBrokerWriteBytes
	if b.wireLength+extraLength > maxBytes {
		b.flush()

		// Even after a flush, the extra overhead could cause
		// an overflow.
		if b.wireLength+extraLength > maxBytes {
			return false, false
		}
		return true, true
	}
	return true, false
}

// newRecordBatch returns a new record batch for a topic and partition
// containing the given record.
func newRecordBatch(pr promisedRecord) *recordBatch {
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
		8 + // producerId
		2 + // producerEpoch
		4 + // baseSequence
		4 // record array length
	b := &recordBatch{
		firstTimestamp: pr.r.Timestamp.UnixNano() / 1e6,
		records:        make([]promisedNumberedRecord, 0, 10),
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
	wireLength int32 // tracks total size this batch would currently encode as

	attrs          int16
	firstTimestamp int64 // since unix epoch, in millis

	// The following three are used for idempotent message delivery
	// following an InitProducerId request.
	// producerId    int64  defined as -1 until we support it
	// producerEpoch int16  defined as -1 until we support it
	// baseSequence  int32  defined as -1 until we support it

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
		varintLen(int64(tsDelta)) +
		varintLen(int64(offsetDelta)) +
		varintLen(int64(len(r.Key))) +
		len(r.Key) +
		varintLen(int64(len(r.Value))) +
		len(r.Value) +
		varintLen(int64(len(r.Headers))) // int32 array len headers

	for _, h := range r.Headers {
		l += varintLen(int64(len(h.Key))) +
			len(h.Key) +
			varintLen(int64(len(h.Value))) +
			len(h.Value)
	}

	return recordNumbers{
		wireLength:     int32(varintLen(int64(l)) + l),
		lengthField:    int32(l),
		timestampDelta: tsDelta,
		offsetDelta:    offsetDelta,
	}
}

// varintLens could only be length 65, but using 256 allows bounds check
// elimination on lookup.
var varintLens [256]byte

func init() {
	for i := 0; i < len(varintLens[:]); i++ {
		varintLens[i] = byte((i-1)/7) + 1
	}
}

func varintLen(i int64) int {
	u := uint64(i)<<1 ^ uint64(i>>63)
	return int(varintLens[byte(bits.Len64(u))])
}
