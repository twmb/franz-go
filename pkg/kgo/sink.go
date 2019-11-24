package kgo

import (
	"hash/crc32"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/kafka-go/pkg/kbin"
	"github.com/twmb/kafka-go/pkg/kerr"
	"github.com/twmb/kafka-go/pkg/kmsg"
)

// TODO millis drift (sarama #1455)

type recordSink struct {
	broker *broker // the broker this sink belongs to

	recordTimeout time.Duration // from producer cfg

	// inflightSem controls the number of concurrent produce requests.  We
	// start with a limit of 1, which covers Kafka v0.11.0.0. On the first
	// response, we check what version was set in the request. If it is at
	// least 4, which 1.0.0 introduced, we upgrade the sem size.
	//
	// Note that both v4 and v5 were introduced with 1.0.0.
	inflightSem         atomic.Value
	produceVersionKnown int32 // atomic bool; 1 is true
	produceVersion      int16 // is set before produceVersionKnown

	// baseWireLength is the minimum wire length of a produce request
	// for a client.
	baseWireLength int32

	// drainState is an atomic used for maybeBeginWork.
	drainState uint32

	// backoffSeq is used to prevent pile on failures from unprocessed
	// responses when the first already triggered a backoff. Once the
	// sink backs off, the seq is incremented followed by doBackoff
	// being cleared. No pile on failure will cause an additional
	// backoff; only new failures will.
	backoffSeq uint32
	doBackoff  uint32
	// consecutiveFailures is incremented every backoff and cleared every
	// successful response. Note that for simplicity if we have a
	// successful response following an error response before the error
	// response's backoff occurs, the backoff is not cleared.
	consecutiveFailures uint32

	mu sync.Mutex // guards the fields below

	recBufs      []*recordBuffer // contains all partition records for batch building
	recBufsStart int             // +1 every req to avoid large batch starvation

	aborting bool // set to true if aborting in EndTransaction
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
		recordTimeout:  broker.client.cfg.recordTimeout,
		broker:         broker,
		baseWireLength: messageRequestOverhead + produceRequestOverhead,
	}
	sink.inflightSem.Store(make(chan struct{}, 1))

	if broker.client.cfg.txnID != nil {
		sink.baseWireLength += int32(len(*broker.client.cfg.txnID))
	}
	if broker.client.cfg.id != nil {
		sink.baseWireLength += int32(len(*broker.client.cfg.id))
	}

	return sink
}

// createRequest returns a produceRequest from currently buffered records
// and whether there are more records to create more requests immediately.
func (sink *recordSink) createRequest() (*produceRequest, *kmsg.AddPartitionsToTxnRequest, bool) {
	req := &produceRequest{
		txnID:   sink.broker.client.cfg.txnID,
		acks:    sink.broker.client.cfg.acks.val,
		timeout: int32(sink.broker.client.cfg.requestTimeout.Milliseconds()),
		batches: make(reqBatches, 5),

		compression: sink.broker.client.cfg.compression,
	}

	var (
		wireLength      = sink.baseWireLength
		wireLengthLimit = sink.broker.client.cfg.maxBrokerWriteBytes

		moreToDrain bool

		transactional  = req.txnID != nil
		txnReq         *kmsg.AddPartitionsToTxnRequest
		txnAddedTopics map[string]int // topic => index in txnReq
	)

	// Prevent concurrent modification to allPartsRecs.
	sink.mu.Lock()
	defer sink.mu.Unlock()

	if sink.aborting {
		for _, recBuf := range sink.recBufs {
			recBuf.failAllRecords(ErrAborting)
			return req, nil, false
		}
	}

	// Over every record buffer, check to see if the first batch is not
	// backing off and that it can can fit in our request.
	recBufsIdx := sink.recBufsStart
	for i := 0; i < len(sink.recBufs); i++ {
		recBuf := sink.recBufs[recBufsIdx]
		recBufsIdx = (recBufsIdx + 1) % len(sink.recBufs)

		recBuf.mu.Lock()
		if recBuf.failing || len(recBuf.batches) == recBuf.batchDrainIdx {
			recBuf.mu.Unlock()
			continue
		}

		batch := recBuf.batches[recBuf.batchDrainIdx]
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

		if _, exists := req.batches[recBuf.topic]; !exists {
			batchWireLength += 2 + int32(len(recBuf.topic)) + 4 // string len, topic, array len
		}
		if wireLength+batchWireLength > wireLengthLimit {
			recBuf.mu.Unlock()
			moreToDrain = true
			continue
		}

		batch.tries++
		recBuf.batchDrainIdx++

		recBuf.lockedStopLinger()

		// If lingering is configured, we have more to drain if there
		// is more than one backed up batches. If there is only one, we
		// re-start the batch's linger (unless flushing).
		if recBuf.linger > 0 {
			if len(recBuf.batches) > recBuf.batchDrainIdx+1 {
				moreToDrain = true
			} else if len(recBuf.batches) == recBuf.batchDrainIdx+1 {
				if !recBuf.lockedStartLinger() {
					moreToDrain = true
				}
			}
		} else {
			// No lingering is simple.
			moreToDrain = len(recBuf.batches) > recBuf.batchDrainIdx || moreToDrain
		}

		recBuf.mu.Unlock()

		if transactional && !recBuf.addedToTxn {
			recBuf.addedToTxn = true
			if txnReq == nil {
				txnReq = &kmsg.AddPartitionsToTxnRequest{
					TransactionalID: *recBuf.cl.cfg.txnID,
					ProducerID:      recBuf.cl.producer.id,
					ProducerEpoch:   recBuf.cl.producer.epoch,
				}
			}
			if txnAddedTopics == nil {
				txnAddedTopics = make(map[string]int, 10)
			}
			idx, exists := txnAddedTopics[recBuf.topic]
			if !exists {
				idx = len(txnReq.Topics)
				txnAddedTopics[recBuf.topic] = idx
				txnReq.Topics = append(txnReq.Topics, kmsg.AddPartitionsToTxnRequestTopic{
					Topic: recBuf.topic,
				})
			}
			txnReq.Topics[idx].Partitions = append(txnReq.Topics[idx].Partitions, recBuf.partition)
		}

		wireLength += batchWireLength
		req.batches.addBatch(
			recBuf.topic,
			recBuf.partition,
			batch,
		)
	}

	// We could have lost our only record buffer just before we grabbed the
	// lock above.
	if len(sink.recBufs) > 0 {
		sink.recBufsStart = (sink.recBufsStart + 1) % len(sink.recBufs)
	}
	return req, txnReq, moreToDrain
}

func (sink *recordSink) maybeBeginDraining() {
	if maybeBeginWork(&sink.drainState) {
		go sink.drain()
	}
}

func (sink *recordSink) backoff() {
	tries := int(atomic.AddUint32(&sink.consecutiveFailures, 1))
	after := time.NewTimer(sink.broker.client.cfg.retryBackoff(tries))
	defer after.Stop()
	select {
	case <-after.C:
	case <-sink.broker.client.ctx.Done():
	}
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
		if atomic.SwapUint32(&sink.doBackoff, 0) == 1 {
			sink.broker.client.triggerUpdateMetadata()
			sink.backoff()
			atomic.AddUint32(&sink.backoffSeq, 1)
			atomic.StoreUint32(&sink.doBackoff, 0) // clear any pile on failures before seq inc
		}

		sem := sink.inflightSem.Load().(chan struct{})
		sem <- struct{}{}

		req, txnReq, goAgain := sink.createRequest()
		again = goAgain

		if txnReq != nil {
			sink.doTxnReq(req, txnReq)
		}

		if len(req.batches) == 0 { // everything is failing
			again = maybeTryFinishWork(&sink.drainState, again)
			<-sem // wont be using that
			continue
		}

		req.backoffSeq = sink.backoffSeq
		sink.broker.doSequencedAsyncPromise(
			sink.broker.client.ctx,
			req,
			func(resp kmsg.Response, err error) {
				sink.handleReqResp(req, resp, err)
				<-sem
			},
		)
		again = maybeTryFinishWork(&sink.drainState, again)
	}
}

// doTxnReq issues an AddPartitionsToTxnRequest for a produce request for all
// partitions that need to be added to a transaction.
//
// If the entire request fails, all batches of the produce request are errored.
// Otherwise, all partitions that have errors are removed.
func (sink *recordSink) doTxnReq(req *produceRequest, txnReq *kmsg.AddPartitionsToTxnRequest) {
	kresp, err := sink.broker.client.Request(sink.broker.client.ctx, txnReq)
	if err != nil {
		for _, topic := range txnReq.Topics {
			topicBatches := req.batches[topic.Topic]
			for _, partition := range topic.Partitions {
				batch := topicBatches[partition]
				batch.owner.addedToTxn = false
				sink.broker.client.finishBatch(batch, partition, 0, err)
				delete(topicBatches, partition)
			}
			if len(topicBatches) == 0 {
				delete(req.batches, topic.Topic)
			}
		}
		return
	}

	resp := kresp.(*kmsg.AddPartitionsToTxnResponse)
	for _, topic := range resp.Topics {
		topicBatches := req.batches[topic.Topic]
		for _, partition := range topic.Partitions {
			if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
				batch := topicBatches[partition.Partition]
				batch.owner.addedToTxn = false
				sink.broker.client.finishBatch(batch, partition.Partition, 0, err)
				delete(topicBatches, partition.Partition)
			}
			if len(topicBatches) == 0 {
				delete(req.batches, topic.Topic)
			}
		}
	}
}

// requeueUnattemptedReq resets all drain indices to zero on all buffers
// where the batch is the first in the buffer.
func (sink *recordSink) requeueUnattemptedReq(req *produceRequest) {
	var maybeBeginDraining bool
	req.batches.onEachBatchWhileBatchOwnerLocked(func(batch *recordBatch) {
		if batch.lockedIsFirstBatch() {
			if batch.isTimedOut(sink.recordTimeout) {
				batch.owner.lockedFailBatch0(ErrRecordTimeout)
			}
			maybeBeginDraining = true
			batch.owner.batchDrainIdx = 0
		}
	})
	if maybeBeginDraining {
		if req.backoffSeq == atomic.LoadUint32(&sink.backoffSeq) {
			atomic.StoreUint32(&sink.doBackoff, 1)
		}
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
		recBuf := batch.owner
		recBuf.mu.Lock()
		// We error here even if the buffer is now on a different sink;
		// no reason to wait for the same error on a different sink.
		if batch.lockedIsFirstBatch() {
			recBuf.lockedFailAllRecords(err)
		}
		recBuf.mu.Unlock()
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
	if sink.produceVersionKnown == 0 {
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
		sink.handleRetryBatches(req.batches, true)

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
	atomic.StoreUint32(&sink.consecutiveFailures, 0)

	var reqRetry reqBatches // handled at the end
	// On normal retriable errors, we backoff. We only do not if we detect
	// data loss and data loss is our only error.
	var backoffRetry bool

	pr := resp.(*kmsg.ProduceResponse)
	for _, rTopic := range pr.Topics {
		topic := rTopic.Topic
		partitions, ok := req.batches[topic]
		if !ok {
			continue
		}
		delete(req.batches, topic)

		for _, rPartition := range rTopic.Partitions {
			partition := rPartition.Partition
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

			err := kerr.ErrorForCode(rPartition.ErrorCode)
			finishBatch := func() { sink.broker.client.finishBatch(batch, partition, rPartition.BaseOffset, err) }
			switch {
			case kerr.IsRetriable(err) &&
				err != kerr.CorruptMessage &&
				batch.tries < sink.broker.client.cfg.retries:
				// Retriable: add to retry map.
				backoffRetry = true
				reqRetry.addBatch(topic, partition, batch)

			case err == kerr.OutOfOrderSequenceNumber,
				err == kerr.UnknownProducerID:
				// OOOSN always means data loss 1.0.0+ and is ambiguous prior.
				// We assume the worst and only continue if requested.
				//
				// UnknownProducerID was introduced to allow some form of safe
				// handling, but KIP-360 demonstrated that resetting sequence
				// numbers is fundamentally unsafe, so we treat it like OOOSN.
				if sink.broker.client.cfg.stopOnDataLoss {
					finishBatch()
					continue
				}
				if sink.broker.client.cfg.onDataLoss != nil {
					sink.broker.client.cfg.onDataLoss(topic, partition)
				}
				batch.owner.resetSequenceNums()
				reqRetry.addBatch(topic, partition, batch)

			case err == kerr.DuplicateSequenceNumber: // ignorable, but we should not get
				err = nil
				fallthrough
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
		sink.handleRetryBatches(reqRetry, backoffRetry)
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
		client.finishRecordPromise(pnr.promisedRecord, err)
		batch.records[i] = noPNR
	}
	emptyRecordsPool.Put(&batch.records)
}

// handleRetryBatches sets any first-buf-batch to failing and triggers a
// metadata that will eventually clear the failing state.
//
// If the retry is due to detecting data loss (and only that), then we
// do not need to refresh metadata.
func (sink *recordSink) handleRetryBatches(retry reqBatches, withBackoff bool) {
	var needsMetaUpdate bool
	retry.onEachBatchWhileBatchOwnerLocked(func(batch *recordBatch) {
		if batch.lockedIsFirstBatch() {
			batch.owner.batchDrainIdx = 0
			if batch.isTimedOut(sink.recordTimeout) {
				batch.owner.lockedFailBatch0(ErrRecordTimeout)
			}
			if withBackoff {
				batch.owner.failing = true
				needsMetaUpdate = true
			}
		}
	})

	if needsMetaUpdate {
		sink.broker.client.triggerUpdateMetadata()
	} else if !withBackoff {
		sink.maybeBeginDraining()
	}
}

// addSource adds a new recordBuffer to a sink, unconditionally clearing
// the fail state.
func (sink *recordSink) addSource(add *recordBuffer) {
	sink.mu.Lock()
	add.recBufsIdx = len(sink.recBufs)
	sink.recBufs = append(sink.recBufs, add)
	sink.mu.Unlock()

	add.clearFailing()
}

// removeSource removes the tracking of a toppar from the recordSink.
func (sink *recordSink) removeSource(rm *recordBuffer) {
	sink.mu.Lock()
	defer sink.mu.Unlock()

	if rm.recBufsIdx != len(sink.recBufs)-1 {
		sink.recBufs[rm.recBufsIdx], sink.recBufs[len(sink.recBufs)-1] =
			sink.recBufs[len(sink.recBufs)-1], nil

		sink.recBufs[rm.recBufsIdx].recBufsIdx = rm.recBufsIdx
	} else {
		sink.recBufs[rm.recBufsIdx] = nil // do not let this removal hang around
	}

	sink.recBufs = sink.recBufs[:len(sink.recBufs)-1]
	if sink.recBufsStart == len(sink.recBufs) {
		sink.recBufsStart = 0
	}
}

// recordBuffer buffers records to be sent to a topic/partition.
//
// Special care must be taken to not access the sink before it is actually
// loaded (non-nil).
type recordBuffer struct {
	cl *Client // for config access / producer id

	topic     string
	partition int32

	// lastAckedOffset, present for Kafka 1.0.0+ (v5+), is used for data
	// loss detection on UnknownProducerID errors.
	//
	// This is only modified when processing responses, which is serial,
	// and thus can change without the mutex (the topicPartition field
	// never changes).
	lastAckedOffset int64

	// addedToTxn, for transactions only, signifies whether this partition
	// has been added to the transaction yet or not.
	//
	// This does not need to be under the mu since it is updated either
	// serially in building a req (the first time) or after failing to add
	// the partition to a txn (again serially), or in EndTransaction after
	// all buffered records are flushed.
	addedToTxn bool

	mu sync.Mutex // guards r/w access to all fields below

	// sink is who is currently draining us. This can be modified
	// concurrently during a metadata update.
	sink *recordSink
	// recBufsIdx is our index into our current sink's recBufs field.
	// This exists to aid in removing the buffer from the sink.
	recBufsIdx int

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

	// lingering is a timer that avoids starting maybeBeginDraining until
	// expired, allowing for more records to be buffered in a single batch.
	//
	// Note that if something else starts a drain, if the first batch of
	// this buffer fits into the request, it will be used.
	lingering *time.Timer
	linger    time.Duration // client configuration

	// failing is set when we encounter a partition error.
	// It is always cleared on metadata update.
	failing bool
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

// bufferRecord usually buffers a record, but does not if abortOnNewBatch is
// true and if this function would create a new batch.
//
// This function is careful not to touch the record sink if the sink is nil,
// which it could be on metadata load err. Note that if the sink is ever not
// nil, then the sink will forever not be nil.
func (recBuf *recordBuffer) bufferRecord(pr promisedRecord, abortOnNewBatch bool) bool {
	recBuf.mu.Lock()
	defer recBuf.mu.Unlock()

	pr.Timestamp = time.Now() // timestamp after locking to ensure sequential

	newBatch := true
	firstBatch := recBuf.batchDrainIdx == len(recBuf.batches)

	if !firstBatch {
		batch := recBuf.batches[len(recBuf.batches)-1]
		recordNumbers := batch.calculateRecordNumbers(pr.Record)

		newBatchLength := batch.wireLength + recordNumbers.wireLength

		// If we do not know the broker version, we may be talking
		// to <0.11.0 and be using message sets. Until we know the
		// broker version, we pessimisitically cut our batch off using
		// the largest record length numbers.
		produceVersionKnown := recBuf.sink != nil && atomic.LoadInt32(&recBuf.sink.produceVersionKnown) == 1
		if !produceVersionKnown {
			v1newBatchLength := batch.v1wireLength + messageSet1Length(pr.Record)
			if v1newBatchLength > newBatchLength { // we only check v1 since it is larger than v0
				newBatchLength = v1newBatchLength
			}
		} else {
			// If we do know our broker version and it is indeed
			// an old one, we use the appropriate length.
			switch recBuf.sink.produceVersion {
			case 0, 1:
				newBatchLength = batch.v0wireLength + messageSet0Length(pr.Record)
			case 2:
				newBatchLength = batch.v1wireLength + messageSet1Length(pr.Record)
			}
		}

		if batch.tries == 0 &&
			newBatchLength <= recBuf.cl.cfg.maxRecordBatchBytes {
			newBatch = false
			batch.appendRecord(pr, recordNumbers)
		}
	}

	if newBatch {
		if abortOnNewBatch {
			return false
		}
		recBuf.batches = append(recBuf.batches,
			recBuf.newRecordBatch(
				recBuf.cl.producer.id,
				recBuf.cl.producer.epoch,
				pr,
			))
	}
	recBuf.sequenceNum++

	// Our sink could be nil if our metadata loaded a partition that is
	// erroring.
	if recBuf.sink == nil {
		return true
	}

	if recBuf.linger == 0 {
		if firstBatch {
			recBuf.sink.maybeBeginDraining()
		}
	} else {
		// With linger, if this is a new batch but not the first, we
		// stop lingering and begin draining. The drain loop will
		// restart our linger once this buffer has one batch left.
		if newBatch && !firstBatch ||
			// If this is the first batch, try lingering; if
			// we cannot, we are being flushed and must drain.
			firstBatch && !recBuf.lockedStartLinger() {
			recBuf.lockedStopLinger()
			recBuf.sink.maybeBeginDraining()
		}
	}
	return true
}

// lockedStartLinger begins a linger timer unless the producer is being flushed.
func (recBuf *recordBuffer) lockedStartLinger() bool {
	if atomic.LoadInt32(&recBuf.cl.producer.flushing) == 1 {
		return false
	}
	recBuf.lingering = time.AfterFunc(recBuf.linger, recBuf.sink.maybeBeginDraining)
	return true
}

func (recBuf *recordBuffer) lockedStopLinger() {
	if recBuf.lingering != nil {
		recBuf.lingering.Stop()
		recBuf.lingering = nil
	}
}

func (recBuf *recordBuffer) lockedRemoveBatch0() {
	recBuf.batches[0] = nil
	recBuf.batches = recBuf.batches[1:]
	recBuf.batchDrainIdx--
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
func (recBuf *recordBuffer) bumpTriesAndMaybeFailBatch0(err error) {
	recBuf.mu.Lock()
	defer recBuf.mu.Unlock()
	if len(recBuf.batches) == 0 {
		return
	}
	batch0 := recBuf.batches[0]
	batch0.tries++
	if batch0.tries > recBuf.cl.cfg.retries {
		recBuf.lockedFailBatch0(err)
	}
}

func (recBuf *recordBuffer) lockedFailBatch0(err error) {
	batch0 := recBuf.batches[0]
	recBuf.lockedRemoveBatch0()
	for i, pnr := range batch0.records {
		recBuf.cl.finishRecordPromise(pnr.promisedRecord, err)
		batch0.records[i] = noPNR
	}
	emptyRecordsPool.Put(&batch0.records)
}

// failAllRecords is called on a non-retriable error to fail all records
// currently buffered in this recordBuffer.
//
// For example, this is called in metadata on invalid auth errors.
func (recBuf *recordBuffer) failAllRecords(err error) {
	recBuf.mu.Lock()
	defer recBuf.mu.Unlock()
	recBuf.lockedFailAllRecords(err)
}

// lockedFailAllRecords is the same as above, but already in a lock.
func (recBuf *recordBuffer) lockedFailAllRecords(err error) {
	recBuf.lockedStopLinger()
	for _, batch := range recBuf.batches {
		for i, pnr := range batch.records {
			recBuf.cl.finishRecordPromise(
				pnr.promisedRecord,
				err,
			)
			batch.records[i] = noPNR
		}
		emptyRecordsPool.Put(&batch.records)
	}
	recBuf.batches = nil
}

// clearFailing is called to clear any failing state.
//
// This is called when a buffer is added to a sink (to clear a failing state
// from migrating buffers between sinks) or when a metadata update sees the
// sink is still on the same source.
//
// Note the sink cannot be nil here, since nil sinks correspond to load errors,
// and partitions with load errors do not call clearFailing.
func (recBuf *recordBuffer) clearFailing() {
	recBuf.mu.Lock()
	defer recBuf.mu.Unlock()

	wasFailing := recBuf.failing
	recBuf.failing = false

	if wasFailing && len(recBuf.batches) != recBuf.batchDrainIdx {
		recBuf.sink.maybeBeginDraining()
	}
}

// resetSequenceNumbers is called in a special error during produce response
// handling. See above where it is used.
func (recBuf *recordBuffer) resetSequenceNums() {
	recBuf.lastAckedOffset = -1

	// Guard modification of sequenceNum and batches access.
	recBuf.mu.Lock()
	defer recBuf.mu.Unlock()

	recBuf.sequenceNum = 0
	for _, batch := range recBuf.batches {
		// We store the new sequence atomically because there may be
		// more requests being built and sent concurrently. It is fine
		// that they get the new sequence num, they will fail with
		// OOOSN, but the error will be dropped since they are not the
		// first batch.
		atomic.StoreInt32(&batch.baseSequence, recBuf.sequenceNum)
		recBuf.sequenceNum += int32(len(batch.records))
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
func (recBuf *recordBuffer) newRecordBatch(producerID int64, producerEpoch int16, pr promisedRecord) *recordBatch {
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
		owner:          recBuf,
		firstTimestamp: pr.Timestamp.UnixNano() / 1e6,
		records:        (*(emptyRecordsPool.Get().(*[]promisedNumberedRecord)))[:0],
		producerID:     producerID,
		producerEpoch:  producerEpoch,
		baseSequence:   recBuf.sequenceNum,
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

// calculateRecordNumbers returns the numbers for a record if it were added to
// the record batch. Nothing accounts for overflows; that should be done prior.
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
		kbin.VarintLen(int64(len(r.Headers))) // varint array len headers

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

// lockedIsFirstBatch returns if the batch in an recordBatch is the first batch
// in a records. We only ever want to update batch / buffer logic if the batch
// is the first in the buffer.
func (batch *recordBatch) lockedIsFirstBatch() bool {
	return len(batch.owner.batches) > 0 && batch.owner.batches[0] == batch
}

// The above, but inside the owning recordBuffer mutex.
func (batch *recordBatch) isFirstBatchInRecordBuf() bool {
	batch.owner.mu.Lock()
	r := batch.lockedIsFirstBatch()
	batch.owner.mu.Unlock()
	return r
}

// removeFromRecordBuf is called in a successful produce response, incrementing
// past the record buffer's now-known-to-be-in-Kafka-batch.
func (batch *recordBatch) removeFromRecordBuf() {
	recBuf := batch.owner
	recBuf.mu.Lock()
	recBuf.lockedRemoveBatch0()
	recBuf.mu.Unlock()
}

// isTimedOut, called only on frozen batches, returns whether the first record
// in a batch is past the limit.
func (batch *recordBatch) isTimedOut(limit time.Duration) bool {
	if limit == 0 {
		return false
	}
	return time.Since(batch.records[0].Timestamp) > limit
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

	backoffSeq uint32

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
func (*produceRequest) MaxVersion() int16    { return 8 }
func (p *produceRequest) SetVersion(v int16) { p.version = v }
func (p *produceRequest) GetVersion() int16  { return p.version }
func (p *produceRequest) IsFlexible() bool   { return false } // version 8 is not flexible
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
				dst = batch.appendTo(dst, compressor, p.txnID != nil)
			}
		}
	}
	return dst
}

func (p *produceRequest) ResponseKind() kmsg.Response {
	return &kmsg.ProduceResponse{Version: p.version}
}

func (r *recordBatch) appendTo(dst []byte, compressor *compressor, transactional bool) []byte {
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

	dst = kbin.AppendInt64(dst, r.producerID)
	dst = kbin.AppendInt16(dst, r.producerEpoch)
	dst = kbin.AppendInt32(dst, atomic.LoadInt32(&r.baseSequence)) // read atomically in case of concurrent reset

	dst = kbin.AppendArrayLen(dst, len(r.records))
	recordsAt := len(dst)
	for i, pnr := range r.records {
		dst = pnr.appendTo(dst, int32(i))
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
