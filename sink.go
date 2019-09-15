package kgo

import (
	"hash/crc32"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/kgo/kbin"
	"github.com/twmb/kgo/kerr"
	"github.com/twmb/kgo/kmsg"
)

// TODO linger:
// - important to keep broker CPU small if producers are low throughput, but many many producers
// - can have atomic, bytesBuffered
// - inc on bufferRecord
// - dec on createRequest
// - if still enough for batch, go again
// - else linger, if new record pushes past enough for batch, signal the linger to stop
// - on toppar move, inc/dec as necessary, should not affect loop much

type recordSink struct {
	broker *broker // the broker this sink belongs to

	// inflightSem controls the number of concurrent produce requests.  We
	// start with a limit of 1, which covers Kafka v0.11.0.0. On the first
	// response, we check what version was set in the request. If it is at
	// least 4, which 1.0.0 introduced, we upgrade the sem size.
	//
	// Note that both v4 and v5 were introduced with 1.0.0.
	inflightSem         atomic.Value
	handledFirstResp    bool
	produceVersionKnown int32 // atomic bool; 1 is true
	produceVersion      int16 // is set before produceVersionKnown

	// baseWireLength is the minimum wire length of a produce request
	// for a client.
	baseWireLength int32

	drainState   uint32
	backoffTimer *time.Timer // runs if all partition records are in a backoff state

	mu sync.Mutex // guards the two fields below

	recordBuffers []*recordBuffer // contains all partition records for batch building

	// recordBuffersStart is where we will begin in recordBuffers for
	// building a batch. This increments by one every produce request,
	// avoiding starvation for large record batches that cannot fit into
	// the request that is currently being built.
	recordBuffersStart int
}

func newRecordSink(broker *broker) *recordSink {
	const messageRequestOverhead int32 = 4 + // full length
		2 + // key
		2 + // version
		4 + // correlation ID
		2 // client ID len
	const produceRequestOverhead int32 = 2 + // transactional ID len
		2 + // acks
		4 + // timeout
		4 // topics array length

	sink := &recordSink{
		broker:         broker,
		baseWireLength: messageRequestOverhead + produceRequestOverhead,
	}
	sink.inflightSem.Store(make(chan struct{}, 1))

	if broker.client.cfg.producer.txnID != nil {
		sink.baseWireLength += int32(len(*broker.client.cfg.producer.txnID))
	}
	if broker.client.cfg.client.id != nil {
		sink.baseWireLength += int32(len(*broker.client.cfg.client.id))
	}

	return sink
}

// createRequest returns a produceRequest from currently buffered records
// and whether there are more records to create more requests immediately.
func (sink *recordSink) createRequest() (*produceRequest, bool) {
	req := &produceRequest{
		txnID:   sink.broker.client.cfg.producer.txnID,
		acks:    sink.broker.client.cfg.producer.acks.val,
		timeout: sink.broker.client.cfg.client.requestTimeout,
		batches: make(reqBatches, 5),

		compression: sink.broker.client.cfg.producer.compression,
	}

	var (
		wireLength      = sink.baseWireLength
		wireLengthLimit = sink.broker.client.cfg.client.maxBrokerWriteBytes

		soonestDeadline time.Time
		numDeadlined    int
		moreToDrain     bool
		now             = time.Now()
	)

	// Prevent concurrent modification to allPartsRecs.
	sink.mu.Lock()
	defer sink.mu.Unlock()

	// Over every record buffer, check to see if the first batch is not
	// backing off and that it can can fit in our request.
	recordBuffersIdx := sink.recordBuffersStart
	for i := 0; i < len(sink.recordBuffers); i++ {
		recordBuffer := sink.recordBuffers[recordBuffersIdx]
		recordBuffersIdx = (recordBuffersIdx + 1) % len(sink.recordBuffers)

		recordBuffer.mu.Lock()
		if dl := recordBuffer.backoffDeadline; now.Before(dl) {
			recordBuffer.mu.Unlock()
			if soonestDeadline.IsZero() || dl.Before(soonestDeadline) {
				soonestDeadline = dl
			}
			numDeadlined++
			continue
		}

		if len(recordBuffer.batches) == recordBuffer.batchDrainIdx {
			recordBuffer.mu.Unlock()
			continue
		}

		batch := recordBuffer.batches[recordBuffer.batchDrainIdx]
		batchWireLength := 4 + batch.wireLength // partition, batch len

		if sink.produceVersionKnown == 0 {
			v1BatchWireLength := 4 + batch.v1wireLength
			if v1BatchWireLength > batchWireLength {
				batchWireLength = v1BatchWireLength
			}
		} else {
			switch sink.produceVersion {
			case 0, 1:
				batchWireLength = 4 + batch.v0wireLength
			case 2:
				batchWireLength = 4 + batch.v1wireLength
			}
		}

		if _, exists := req.batches[recordBuffer.topicPartition.topic]; !exists {
			batchWireLength += 2 + int32(len(recordBuffer.topicPartition.topic)) + 4 // string len, topic, array len
		}
		if wireLength+batchWireLength > wireLengthLimit {
			recordBuffer.mu.Unlock()
			moreToDrain = true
			continue
		}

		batch.tries++
		recordBuffer.batchDrainIdx++

		moreToDrain = len(recordBuffer.batches) > recordBuffer.batchDrainIdx || moreToDrain
		recordBuffer.mu.Unlock()

		wireLength += batchWireLength
		req.batches.addBatch(
			recordBuffer.topicPartition.topic,
			recordBuffer.topicPartition.partition,
			batch,
		)
	}

	// If we have no more to drain, yet some bufs are backing off, we
	// begin the drain backoff for when the soonest is ready.
	if !moreToDrain && numDeadlined != 0 && soonestDeadline != forever {
		sink.beginDrainBackoff(soonestDeadline.Sub(now))
	}

	sink.recordBuffersStart = (sink.recordBuffersStart + 1) % len(sink.recordBuffers)
	return req, moreToDrain
}

func (sink *recordSink) maybeBeginDraining() {
	if maybeBeginWork(&sink.drainState) {
		go sink.drain()
	}
}

// beginDrainBackoff is called if all record buffers are detected to be in a
// backoff state and no produce request can be built.
func (sink *recordSink) beginDrainBackoff(after time.Duration) {
	sink.backoffTimer = time.AfterFunc(after, sink.maybeBeginDraining)
}

// drain drains buffered records and issues produce requests.
//
// This function is harmless if there are no records that need draining.
// We rely on that to not worry about accidental triggers of this function.
func (sink *recordSink) drain() {
	// Before we begin draining, sleep a tiny bit. This helps when a
	// high volume new sink began draining; rather than immediately
	// eating just one record, we allow it to buffer a bit before we
	// loop draining.
	time.Sleep(time.Millisecond)

	again := true
	for again {
		if sink.backoffTimer != nil {
			sink.backoffTimer.Stop()
			sink.backoffTimer = nil
		}

		sem := sink.inflightSem.Load().(chan struct{})
		sem <- struct{}{}

		var req *produceRequest
		req, again = sink.createRequest()

		if len(req.batches) == 0 { // everything entered backoff
			again = maybeTryFinishWork(&sink.drainState, again)
			<-sem // wont be using that
			continue
		}

		sink.broker.doSequencedAsyncPromise(
			req,
			func(resp kmsg.Response, err error) {
				sink.handleReqResp(req, resp, err)
				<-sem
			},
		)
		again = maybeTryFinishWork(&sink.drainState, again)
	}
}

// requeueUnattemptedReq requeues all batches in req to the recordSink.
// This is done if a retriable network error occured.
func (sink *recordSink) requeueUnattemptedReq(req *produceRequest) {
	// We use a first-level backoff since these network errors are
	// presumed to be exceedingly temporary.
	backoffDeadline := time.Now().Add(sink.broker.client.cfg.client.retryBackoff(1))
	// If we set any backoff state, we maybeBeginDraining. We do this so
	// that the drain loop can set the backoff timer if necessary.
	maybeBeginDraining := false
	req.batches.onEachBatchWhileBatchOwnerLocked(func(batch *recordBatch) {
		if batch.lockedTryCanBackoffInSink(sink) {
			batch.owner.backoffDeadline = backoffDeadline
			maybeBeginDraining = true
		}
	})

	if maybeBeginDraining {
		sink.maybeBeginDraining()
	}
}

// errorAllRecordsInAllRecordBuffersInRequest is called on unrecoverable errors
// while handling produce responses.
//
// These are errors that are so unrecoverable that not only are all records in
// the original response failed (not retried), all record buffers that
// contained those records have all of their other buffered records failed.
//
// For example, auth failures (cannot produce to a topic), or a lack of a
// response (Kafka did not reply to a topic, a violation of the protocol).
//
// The name is extra verbose to be clear as to the intent.
func (sink *recordSink) errorAllRecordsInAllRecordBuffersInRequest(
	req *produceRequest,
	err error,
) {
	for _, partitions := range req.batches {
		sink.errorAllRecordsInAllRecordBuffersInPartitions(
			partitions,
			err,
		)
	}
}

// errorAllRecordsInAllRecordBuffersInPartitions is similar to the extra
// verbosely named function just above; read that documentation.
func (sink *recordSink) errorAllRecordsInAllRecordBuffersInPartitions(
	partitions map[int32]*recordBatch,
	err error,
) {
	for _, batch := range partitions {
		recordBuffer := batch.owner
		recordBuffer.mu.Lock()
		// We error here even if the buffer is now on a different sink;
		// no reason to wait for the same error on a different sink.
		if batch.lockedIsFirstBatchInRecordBuf() {
			recordBuffer.lockedFailAllRecords(err)
		}
		recordBuffer.mu.Unlock()
	}
}

// firstRespCheck is effectively a sink.Once. On the first response, if the
// used request version is at least 4, we upgrade our inflight sem.
//
// Starting on version 4, Kafka allowed five inflight requests while
// maintaining idempotency. Before, only one was allowed.
//
// We go through an atomic because drain can be waiting on the sem (with
// capacity one). We store four here, meaning new drain loops will load the
// higher capacity sem without read/write pointer racing a current loop.
func (sink *recordSink) firstRespCheck(version int16) {
	if !sink.handledFirstResp {
		sink.handledFirstResp = true
		sink.produceVersion = version
		atomic.StoreInt32(&sink.produceVersionKnown, 1)
		if version >= 4 {
			sink.inflightSem.Store(make(chan struct{}, 4))
		}
	}
}

// handleReqClientErr is called when the client errors before receiving a
// produce response.
func (sink *recordSink) handleReqClientErr(req *produceRequest, err error) {
	switch {
	case err == ErrBrokerDead:
		// A dead broker means the broker may have migrated, so we
		// retry to force a metadata reload.
		sink.handleRetryBatches(req.batches)

	case isRetriableBrokerErr(err):
		sink.requeueUnattemptedReq(req)

	default:
		sink.errorAllRecordsInAllRecordBuffersInRequest(req, err)
	}
}

func (sink *recordSink) handleReqResp(req *produceRequest, resp kmsg.Response, err error) {
	sink.firstRespCheck(req.version)

	// If we had an err, it is from the client itself. This is either a
	// retriable conn failure or a total loss (e.g. auth failure).
	if err != nil {
		sink.handleReqClientErr(req, err)
		return
	}

	var reqRetry reqBatches // handled at the end

	pr := resp.(*kmsg.ProduceResponse)
	for _, responseTopic := range pr.Responses {
		topic := responseTopic.Topic
		partitions, ok := req.batches[topic]
		if !ok {
			continue
		}
		delete(req.batches, topic)

		for _, responsePartition := range responseTopic.PartitionResponses {
			partition := responsePartition.Partition
			batch, ok := partitions[partition]
			if !ok {
				continue
			}
			delete(partitions, partition)

			// We only ever operate on the first batch in a record
			// buf. Batches work sequentially; if this is not the
			// first batch then an error happened and this later
			// batch is no longer a part of the sequential chain.
			//
			// If the batch is a success, everything is golden and
			// we do not need to worry about the buffer migrating
			// sinks.
			//
			// If the batch is a failure and needs retrying, the
			// retry function checks for sink migration problems.
			if !batch.isFirstBatchInRecordBuf() {
				continue
			}

			err := kerr.ErrorForCode(responsePartition.ErrorCode)
			finishBatch := func() { sink.broker.client.finishBatch(batch, partition, responsePartition.BaseOffset, err) }
			switch {
			case kerr.IsRetriable(err) &&
				err != kerr.CorruptMessage &&
				batch.tries < sink.broker.client.cfg.client.retries:
				// Retriable: add to retry map.
				reqRetry.addBatch(topic, partition, batch)

			case err == kerr.UnknownProducerID: // 1.0.0+ only
				// If -1, retry: the partition moved between the error being raised
				// in Kafka and the time the response was constructed.
				if responsePartition.LogStartOffset == -1 {
					reqRetry.addBatch(topic, partition, batch)
					continue
				}

				// LogStartOffset <= last acked: data loss.
				//
				// The official client resets the producer ID on non transactional
				// requests. Doing so avoids perma-OOOSN errors, but may not be the
				// best considering it could cause ordering problems.
				//
				// OutOfOrderSequenceNumber could be ambiguous pre 1.0.0, but we
				// will assume it is data loss (and just rely on the default below).
				if responsePartition.LogStartOffset <= batch.owner.lastAckedOffset {
					finishBatch()
					continue
				}
				// Otherwise, the log head rotated; we need to reset seq's and retry.
				batch.owner.resetSequenceNums()
				reqRetry.addBatch(topic, partition, batch)

			default:
				finishBatch()
			}
		}

		if len(partitions) > 0 {
			sink.errorAllRecordsInAllRecordBuffersInPartitions(
				partitions,
				ErrNoResp,
			)
		}
	}
	if len(req.batches) > 0 {
		sink.errorAllRecordsInAllRecordBuffersInRequest(
			req,
			ErrNoResp,
		)
	}

	if len(reqRetry) > 0 {
		sink.handleRetryBatches(reqRetry)
	}
}

func (client *Client) finishBatch(batch *recordBatch, partition int32, baseOffset int64, err error) {
	batch.removeFromRecordBuf()
	if err == nil {
		batch.owner.lastAckedOffset = baseOffset + int64(len(batch.records))
	}
	for i, pnr := range batch.records {
		pnr.Offset = baseOffset + int64(i)
		pnr.Partition = partition
		client.finishRecordPromise(pnr, err)
		batch.records[i] = noPNR
	}
	emptyRecordsPool.Put(&batch.records)
}

var forever = time.Now().Add(100 * 365 * 24 * time.Hour)

func (sink *recordSink) handleRetryBatches(retry reqBatches) {
	retry.onEachBatchWhileBatchOwnerLocked(func(batch *recordBatch) {
		if batch.lockedTryCanBackoffInSink(sink) {
			batch.owner.backoffDeadline = forever // tombstone
		}
	})

	// A metadata update triggers all sinks to maybeBeginDraining.
	sink.broker.client.triggerUpdateMetadata()
}

// addSource adds a new recordBuffer to a sink, unconditionally resetting the
// backoff timer using the buffer's resetBackoffAndMaybeTriggerSinkDrain.
func (sink *recordSink) addSource(add *recordBuffer) {
	sink.mu.Lock()
	add.recordBuffersIdx = len(sink.recordBuffers)
	sink.recordBuffers = append(sink.recordBuffers, add)
	sink.mu.Unlock()

	add.resetBackoffAndMaybeTriggerSinkDrain()
}

// removeSource removes the tracking of a toppar from the recordSink.
func (sink *recordSink) removeSource(rm *recordBuffer) {
	sink.mu.Lock()
	defer sink.mu.Unlock()

	if rm.recordBuffersIdx != len(sink.recordBuffers)-1 {
		sink.recordBuffers[rm.recordBuffersIdx], sink.recordBuffers[len(sink.recordBuffers)-1] =
			sink.recordBuffers[len(sink.recordBuffers)-1], nil

		sink.recordBuffers[rm.recordBuffersIdx].recordBuffersIdx = rm.recordBuffersIdx
	} else {
		sink.recordBuffers[rm.recordBuffersIdx] = nil // do not let this removal hang around
	}

	sink.recordBuffers = sink.recordBuffers[:len(sink.recordBuffers)-1]
	if sink.recordBuffersStart == len(sink.recordBuffers) {
		sink.recordBuffersStart = 0
	}
}

// recordBuffer buffers records to be sent to a topic/partition.
type recordBuffer struct {
	topicPartition *topicPartition

	// lastAckedOffset, present for Kafka 1.0.0+ (v5+), is used for data
	// loss detection on UnknownProducerID errors.
	//
	// This is only modified when processing responses, which is serial,
	// and thus is the only field that can change without the mutex (the
	// topicPartition field never changes).
	lastAckedOffset int64

	mu sync.Mutex // guards r/w access to all fields below

	// sink is who is currently draining us. This can be modified
	// concurrently during a metadata update.
	sink *recordSink
	// recordBuffersIdx is our index into our current sink's recordBuffers
	// field. This exists to aid in removing the buffer from the sink.
	recordBuffersIdx int

	// sequenceNum is used for the baseSequence in each record batch. This
	// is incremented in bufferRecord and can be reset when processing a
	// response.
	sequenceNum int32 // used for baseSequence in a record batch

	// batches is our list of buffered records. Batches are appended as
	// the final batch crosses size thresholds or as drain freezes batches
	// from further modification.
	//
	// Most functions in a sink only operate on a batch if the batch is the
	// first batch in a buffer. Since response processing is serial, if a
	// third in-flight batch had an error, the first would.
	batches []*recordBatch
	// batchDrainIdx is where the next batch will drain from. We only
	// remove from the head of batches when a batch is finished.
	// This is read while buffering and modified in a few places.
	batchDrainIdx int

	// backoffDeadline is used for retries. If a network error occurs,
	// we backoff for a short deadline. Otherwise, we set the backoff
	// to forever and wait for a metadata update. The metadata update
	// always calls maybeBeginDraining.
	backoffDeadline time.Time
}

const recordsOverhead = 4 // NULLABLE_BYTES

func messageSet0Length(r *Record) int32 {
	const length = 0 +
		8 + // offset
		4 + // size
		4 + // crc
		1 + // magic
		1 + // attributes
		4 + // key array bytes len
		4 // value array bytes len
	return length + int32(len(r.Key)) + int32(len(r.Value))
}

func messageSet1Length(r *Record) int32 {
	return messageSet0Length(r) + 8 // timestamp
}

func (recordBuffer *recordBuffer) bufferRecord(pr promisedRecord) {
	recordBuffer.mu.Lock()

	pr.Timestamp = time.Now() // timestamp after locking to ensure sequential

	sink := recordBuffer.sink
	client := sink.broker.client

	newBatch := true
	firstBatch := recordBuffer.batchDrainIdx == len(recordBuffer.batches)

	if !firstBatch {
		batch := recordBuffer.batches[len(recordBuffer.batches)-1]
		recordNumbers := batch.calculateRecordNumbers(pr.Record)

		newBatchLength := batch.wireLength + recordNumbers.wireLength

		// If we do not know the broker version, we may be talking
		// to <0.11.0 and be using message sets. Until we know the
		// broker version, we pessimisitically cut our batch off using
		// the largest record length numbers.
		produceVersionKnown := atomic.LoadInt32(&sink.produceVersionKnown) == 1
		if !produceVersionKnown {
			v1newBatchLength := batch.v1wireLength + messageSet1Length(pr.Record)
			if v1newBatchLength > newBatchLength { // we only check v1 since it is larger than v0
				newBatchLength = v1newBatchLength
			}
		} else {
			// If we do know our broker version and it is indeed
			// an old one, we use the appropriate length.
			switch sink.produceVersion {
			case 0, 1:
				newBatchLength = batch.v0wireLength + messageSet0Length(pr.Record)
			case 2:
				newBatchLength = batch.v1wireLength + messageSet1Length(pr.Record)
			}
		}

		if batch.tries == 0 &&
			newBatchLength <= client.cfg.producer.maxRecordBatchBytes {
			newBatch = false
			batch.appendRecord(pr, recordNumbers)
		}
	}

	if newBatch {
		recordBuffer.batches = append(recordBuffer.batches,
			recordBuffer.newRecordBatch(
				client.producer.id,
				client.producer.epoch,
				pr,
			))
	}
	recordBuffer.sequenceNum++

	recordBuffer.mu.Unlock()

	if firstBatch {
		sink.maybeBeginDraining()
	}
}

func (recordBuffer *recordBuffer) lockedRemoveBatch0() {
	recordBuffer.batches[0] = nil
	recordBuffer.batches = recordBuffer.batches[1:]
	recordBuffer.batchDrainIdx--
}

// bumpTriesAndMaybeFailBatch0 is called during metadata updating.
//
// If the metadata loads an error for the topicPartition that this recordBuffer
// is on, the first batch in the buffer has its try count bumped.
//
// Partition load errors are generally temporary (leader/listener/replica not
// available), and this try bump is not expected to do much. If for some reason
// a partition errors for a long time, this function can drop the first batch
// and move to the next.
//
// This is also called if the entire topic errors, which has similar retriable
// errors.
//
// Effectively, we have some means of slowly discarding a shouldn't-be-but-is
// permanently failing partition.
func (recordBuffer *recordBuffer) bumpTriesAndMaybeFailBatch0(err error) {
	recordBuffer.mu.Lock()
	defer recordBuffer.mu.Unlock()
	if len(recordBuffer.batches) == 0 {
		return
	}
	batch0 := recordBuffer.batches[0]
	batch0.tries++
	client := recordBuffer.sink.broker.client
	if batch0.tries > client.cfg.client.retries {
		recordBuffer.lockedRemoveBatch0()
		for i, pnr := range batch0.records {
			client.finishRecordPromise(pnr, err)
			batch0.records[i] = noPNR
		}
		emptyRecordsPool.Put(&batch0.records)
	}
}

// failAllRecords is called on a non-retriable error to fail all records
// currently buffered in this recordBuffer.
//
// For example, this is called in metadata on invalid auth errors.
func (recordBuffer *recordBuffer) failAllRecords(err error) {
	recordBuffer.mu.Lock()
	defer recordBuffer.mu.Unlock()
	recordBuffer.lockedFailAllRecords(err)
}

// lockedFailAllRecords is the same as above, but already in a lock.
func (recordBuffer *recordBuffer) lockedFailAllRecords(err error) {
	client := recordBuffer.sink.broker.client

	for _, batch := range recordBuffer.batches {
		for i, pnr := range batch.records {
			client.finishRecordPromise(
				pnr,
				err,
			)
			batch.records[i] = noPNR
		}
		emptyRecordsPool.Put(&batch.records)
	}
	recordBuffer.batches = nil
}

// resetBackoffAndMaybeTriggerSinkDrain clears the backoff on a buffer and
// triggers a sink drain if there were any buffered records.
//
// Note that this does not reset the drain index. There could be an inflight
// batch that eventually resets the drain index.
func (recordBuffer *recordBuffer) resetBackoffAndMaybeTriggerSinkDrain() {
	recordBuffer.mu.Lock()
	recordBuffer.backoffDeadline = time.Time{}
	drain := len(recordBuffer.batches) != recordBuffer.batchDrainIdx
	sink := recordBuffer.sink
	recordBuffer.mu.Unlock()

	if drain {
		sink.maybeBeginDraining()
	}
}

// resetSequenceNumbers is called in a special error during produce response
// handling. See above where it is used.
func (recordBuffer *recordBuffer) resetSequenceNums() {
	recordBuffer.lastAckedOffset = -1

	// Guard modification of sequenceNum and batches access.
	recordBuffer.mu.Lock()
	defer recordBuffer.mu.Unlock()

	recordBuffer.sequenceNum = 0
	for _, batch := range recordBuffer.batches {
		batch.baseSequence = recordBuffer.sequenceNum
		recordBuffer.sequenceNum += int32(len(batch.records))
	}
}

// promisedRecord ties a record with the callback that will be called once
// a batch is finally written and receives a response.
type promisedRecord struct {
	promise func(*Record, error)
	*Record
}

// recordNumbers tracks a few numbers for a record that is buffered.
type recordNumbers struct {
	wireLength     int32
	lengthField    int32
	timestampDelta int32
}

// promisedNumberedRecord ties a promisedRecord to its calculated numbers.
type promisedNumberedRecord struct {
	recordNumbers
	promisedRecord
}

var noPNR promisedNumberedRecord
var emptyRecordsPool = sync.Pool{
	New: func() interface{} {
		r := make([]promisedNumberedRecord, 0, 500)
		return &r
	},
}

// newRecordBatch returns a new record batch for a topic and partition
// containing the given record.
func (records *recordBuffer) newRecordBatch(producerID int64, producerEpoch int16, pr promisedRecord) *recordBatch {
	const recordBatchOverhead = recordsOverhead + // NULLABLE_BYTES
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
		owner:          records,
		firstTimestamp: pr.Timestamp.UnixNano() / 1e6,
		records:        (*(emptyRecordsPool.Get().(*[]promisedNumberedRecord)))[:0],
		producerID:     producerID,
		producerEpoch:  producerEpoch,
		baseSequence:   records.sequenceNum,
	}
	pnr := promisedNumberedRecord{
		b.calculateRecordNumbers(pr.Record),
		pr,
	}
	b.records = append(b.records, pnr)
	b.wireLength = recordBatchOverhead + pnr.wireLength
	b.v0wireLength = recordsOverhead + messageSet0Length(pr.Record)
	b.v1wireLength = recordsOverhead + messageSet1Length(pr.Record)
	return b
}

// appendRecord saves a new record to a batch.
func (b *recordBatch) appendRecord(pr promisedRecord, nums recordNumbers) {
	b.wireLength += nums.wireLength
	b.v0wireLength += messageSet0Length(pr.Record)
	b.v1wireLength += messageSet1Length(pr.Record)
	b.records = append(b.records, promisedNumberedRecord{
		nums,
		pr,
	})
}

// recordBatch is the type used for buffering records before they are written.
type recordBatch struct {
	owner *recordBuffer // who owns us

	tries int // if this was sent before and is thus now immutable

	v0wireLength int32 // same as wireLength, but for message set v0
	v1wireLength int32 // same as wireLength, but for message set v1
	wireLength   int32 // tracks total size this batch would currently encode as

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
	}
}

// lockedIsFirstBatchInRecordBuf returns if the batch in an recordBatch is the
// first batch in a records.
//
// This function is necessary because we only want to remove leading batches.
// One batch could be received successfully but the response dies in
// disconnect, then a second batch in flight batch could cause a reconnect and
// send successfully. We do not want to operate on that one since we still
// think the first failed.
func (batch *recordBatch) lockedIsFirstBatchInRecordBuf() bool {
	return len(batch.owner.batches) > 0 && batch.owner.batches[0] == batch
}

// The above, but inside the owning recordBuffer mutex.
func (batch *recordBatch) isFirstBatchInRecordBuf() bool {
	batch.owner.mu.Lock()
	r := batch.lockedIsFirstBatchInRecordBuf()
	batch.owner.mu.Unlock()
	return r
}

// removeFromRecordBuf is called in a successful produce response, incrementing
// past the record buffer's now-known-to-be-in-Kafka-batch.
func (batch *recordBatch) removeFromRecordBuf() {
	recordBuffer := batch.owner
	recordBuffer.mu.Lock()
	if !batch.lockedIsFirstBatchInRecordBuf() {
		panic("removeFromRecordBuf called on non-first batch")
	}
	recordBuffer.lockedRemoveBatch0()
	recordBuffer.mu.Unlock()
}

// lockedTryCanBackoffInSink returns whether a batch can backoff within a sink.
// This returns false if the batch is not the first batch or if the batch's
// owner moved to a different sink.
func (batch *recordBatch) lockedTryCanBackoffInSink(sink *recordSink) bool {
	if !batch.lockedIsFirstBatchInRecordBuf() {
		return false
	}

	// We always reset the drain index. This function is only called as a
	// result of an error that wants to cause the batch to backoff.
	batch.owner.batchDrainIdx = 0

	// If the batch's owner moved sinks due to a concurrent metadata
	// update, then we trigger that sink to maybe start draining due to our
	// drain index reset.
	if batch.owner.sink != sink {
		batch.owner.sink.maybeBeginDraining()
		return false
	}

	// Since the sink is the same and this is the first batch, the caller
	// can backoff.
	return true
}

////////////////////
// produceRequest //
////////////////////

// produceRequest is a kmsg.Request that is used when we want to
// flush our buffered records.
//
// It is the same as kmsg.ProduceRequest, but with a custom AppendTo.
type produceRequest struct {
	version int16

	txnID   *string
	acks    int16
	timeout int32
	batches reqBatches

	compression []CompressionCodec
}

type reqBatches map[string]map[int32]*recordBatch

func (rbs *reqBatches) addBatch(topic string, part int32, batch *recordBatch) {
	if *rbs == nil {
		*rbs = make(reqBatches, 5)
	}
	topicBatches, exists := (*rbs)[topic]
	if !exists {
		topicBatches = make(map[int32]*recordBatch, 1)
		(*rbs)[topic] = topicBatches
	}
	topicBatches[part] = batch
}

func (rbs reqBatches) onEachBatchWhileBatchOwnerLocked(fn func(*recordBatch)) {
	for _, partitions := range rbs {
		for _, batch := range partitions {
			batch.owner.mu.Lock()
			fn(batch)
			batch.owner.mu.Unlock()
		}
	}
}

func (*produceRequest) Key() int16           { return 0 }
func (*produceRequest) MaxVersion() int16    { return 7 }
func (p *produceRequest) SetVersion(v int16) { p.version = v }
func (p *produceRequest) GetVersion() int16  { return p.version }
func (p *produceRequest) AppendTo(dst []byte) []byte {
	if p.version >= 3 {
		dst = kbin.AppendNullableString(dst, p.txnID)
	}

	compressor := loadProduceCompressor(p.compression, p.version)

	dst = kbin.AppendInt16(dst, p.acks)
	dst = kbin.AppendInt32(dst, p.timeout)
	dst = kbin.AppendArrayLen(dst, len(p.batches))

	for topic, partitions := range p.batches {
		dst = kbin.AppendString(dst, topic)
		dst = kbin.AppendArrayLen(dst, len(partitions))
		for partition, batch := range partitions {
			dst = kbin.AppendInt32(dst, partition)
			if p.version < 3 {
				dst = batch.appendToAsMessageSet(dst, uint8(p.version), compressor)
			} else {
				dst = batch.appendTo(dst, compressor)
			}
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
	dst = kbin.AppendInt64(dst, r.firstTimestamp+int64(lastRecord.timestampDelta))

	dst = kbin.AppendInt64(dst, r.producerID)
	dst = kbin.AppendInt16(dst, r.producerEpoch)
	dst = kbin.AppendInt32(dst, r.baseSequence)

	dst = kbin.AppendArrayLen(dst, len(r.records))
	recordsAt := len(dst)
	for i, pnr := range r.records {
		dst = pnr.appendTo(dst, int32(i))
	}

	// TODO swap compressor interface to a writer to avoid one copy here:
	// (1) write record in pieces to compressor directly
	// (2) append compressed data directly to dst
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

func (r *recordBatch) appendToAsMessageSet(dst []byte, version uint8, compressor *compressor) []byte {
	nullableBytesLenAt := len(dst)
	dst = append(dst, 0, 0, 0, 0) // nullable bytes len
	for i, pnr := range r.records {
		dst = appendMessageTo(
			dst,
			version,
			0,
			int64(i),
			r.firstTimestamp+int64(pnr.timestampDelta),
			pnr.Record,
		)
	}

	if compressor != nil {
		toCompress := dst[nullableBytesLenAt+4:] // skip nullable bytes leading prefix
		zipr := compressor.getZipr()
		defer compressor.putZipr(zipr)

		compressed := zipr.compress(toCompress)
		inner := &Record{Value: compressed}
		wrappedLength := messageSet0Length(inner)
		if version == 2 {
			wrappedLength += 8 // timestamp
		}

		if compressed != nil &&
			int(wrappedLength) < len(toCompress) {

			dst = appendMessageTo(
				dst[:nullableBytesLenAt+4],
				version,
				compressor.attrs,
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
	r *Record,
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
