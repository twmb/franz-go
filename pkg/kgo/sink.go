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

type sink struct {
	cfg *cfg    // client's cfg for easy access
	b   *broker // the broker this sink belongs to
	cl  *Client // our owning client, for metadata triggering, context, etc.

	// inflightSem controls the number of concurrent produce requests.  We
	// start with a limit of 1, which covers Kafka v0.11.0.0. On the first
	// response, we check what version was set in the request. If it is at
	// least 4, which 1.0.0 introduced, we upgrade the sem size.
	//
	// Note that both v4 and v5 were introduced with 1.0.0.
	inflightSem         atomic.Value
	produceVersionKnown uint32 // atomic bool; 1 is true
	produceVersion      int16  // is set before produceVersionKnown

	// baseWireLength is the minimum wire length of a produce request for a
	// client.
	//
	// This may be a slight overshoot for versions before 3, which do not
	// include the txn id. We do not bother updating the size if we know we
	// are <v3; the length is only used to cutoff creating a request.
	baseWireLength int32

	drainState workLoop

	// backoffSeq is used to prevent pile on failures from unprocessed
	// responses when the first already triggered a backoff. Once the
	// sink backs off, the seq is incremented followed by doBackoff
	// being cleared. No pile on failure will cause an additional
	// backoff; only new failures will.
	backoffSeq uint32
	doBackoff  uint32
	// consecutiveFailures is incremented every backoff and cleared every
	// successful response. For simplicity, if we have a good response
	// following an error response before the error response's backoff
	// occurs, the backoff is not cleared.
	consecutiveFailures uint32

	mu sync.Mutex // guards the fields below

	recBufs      []*recBuf // contains all partition records for batch building
	recBufsStart int       // incremented every req to avoid large batch starvation

	aborting bool // set to true if aborting in EndTransaction
}

func newSink(
	cfg *cfg,
	cl *Client,
	b *broker,
) *sink {
	const messageRequestOverhead int32 = 4 + // full length
		2 + // key
		2 + // version
		4 + // correlation ID
		2 // client ID len
	const produceRequestOverhead int32 = 2 + // transactional ID len
		2 + // acks
		4 + // timeout
		4 // topics array length

	s := &sink{
		cfg: cfg,
		b:   b,
		cl:  cl,

		baseWireLength: messageRequestOverhead + produceRequestOverhead,
	}
	s.inflightSem.Store(make(chan struct{}, 1))

	if cfg.txnID != nil {
		s.baseWireLength += int32(len(*cfg.txnID))
	}
	if cfg.id != nil {
		s.baseWireLength += int32(len(*cfg.id))
	}

	return s
}

// createReq returns a produceRequest from currently buffered records
// and whether there are more records to create more requests immediately.
func (s *sink) createReq() (*produceRequest, *kmsg.AddPartitionsToTxnRequest, bool) {
	req := &produceRequest{
		txnID:   s.cfg.txnID,
		acks:    s.cfg.acks.val,
		timeout: int32(s.cfg.requestTimeout.Milliseconds()),
		batches: make(seqRecBatches, 5),

		compressor: s.cl.compressor,
	}

	var (
		wireLength      = s.baseWireLength
		wireLengthLimit = s.cfg.maxBrokerWriteBytes

		moreToDrain bool

		transactional  = req.txnID != nil
		txnReq         *kmsg.AddPartitionsToTxnRequest
		txnAddedTopics map[string]int // topic => index in txnReq
	)

	s.mu.Lock() // prevent concurrent modification to recBufs
	defer s.mu.Unlock()

	if s.aborting {
		for _, recBuf := range s.recBufs {
			recBuf.failAllRecords(ErrAborting)
			return req, nil, false
		}
	}

	// Over every record buffer, check to see if the first batch is not
	// backing off and that it can can fit in our request.
	recBufsIdx := s.recBufsStart
	for i := 0; i < len(s.recBufs); i++ {
		recBuf := s.recBufs[recBufsIdx]
		recBufsIdx = (recBufsIdx + 1) % len(s.recBufs)

		recBuf.mu.Lock()
		if recBuf.failing || len(recBuf.batches) == recBuf.batchDrainIdx {
			recBuf.mu.Unlock()
			continue
		}

		batch := recBuf.batches[recBuf.batchDrainIdx]
		batchWireLength := 4 + batch.wireLength // partition, batch len

		if atomic.LoadUint32(&s.produceVersionKnown) == 0 {
			v1BatchWireLength := 4 + batch.v1wireLength
			if v1BatchWireLength > batchWireLength {
				batchWireLength = v1BatchWireLength
			}
		} else {
			switch s.produceVersion {
			case 0, 1:
				batchWireLength = 4 + batch.v0wireLength
			case 2:
				batchWireLength = 4 + batch.v1wireLength
			}
		}

		if _, exists := req.batches[recBuf.topic]; !exists {
			batchWireLength += 2 + int32(len(recBuf.topic)) + 4 // string len, topic, partition array len
		}
		if wireLength+batchWireLength > wireLengthLimit {
			recBuf.mu.Unlock()
			moreToDrain = true
			continue
		}

		batch.tries++
		recBuf.batchDrainIdx++

		recBuf.lockedStopLinger()

		// If lingering is configured, there is some logic around
		// whether there is more to drain. If this recbuf has more than
		// one batch ready, then yes, more to drain. Otherwise, we
		// re-linger unless we are flushing.
		if recBuf.cfg.linger > 0 {
			if len(recBuf.batches) > recBuf.batchDrainIdx+1 {
				moreToDrain = true
			} else if len(recBuf.batches) == recBuf.batchDrainIdx+1 {
				if !recBuf.lockedMaybeStartLinger() {
					moreToDrain = true
				}
			}
		} else { // no linger, easier
			moreToDrain = len(recBuf.batches) > recBuf.batchDrainIdx || moreToDrain
		}

		seq := recBuf.seq
		recBuf.seq += int32(len(batch.records))

		recBuf.mu.Unlock()

		if transactional && !recBuf.addedToTxn {
			recBuf.addedToTxn = true
			if txnReq == nil {
				txnReq = &kmsg.AddPartitionsToTxnRequest{
					TransactionalID: *req.txnID,
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
			seq,
			batch,
		)
	}

	// We could have lost our only record buffer just before we grabbed the
	// lock above, so we have to check there are recBufs.
	if len(s.recBufs) > 0 {
		s.recBufsStart = (s.recBufsStart + 1) % len(s.recBufs)
	}
	return req, txnReq, moreToDrain
}

// setAborting sets the sink to abort all records until ClearAborting is called.
func (s *sink) setAborting() {
	s.mu.Lock()
	s.aborting = true
	s.mu.Unlock()
	s.maybeDrain()
}

// clearAborting clears the sink of an aborting state.
func (s *sink) clearAborting() {
	s.mu.Lock()
	s.aborting = false
	s.mu.Unlock()
}

// maybeDrain begins a drain loop on the sink if the sink is not yet draining.
func (s *sink) maybeDrain() {
	if s.drainState.maybeBegin() {
		go s.drain()
	}
}

func (s *sink) backoff() {
	tries := int(atomic.AddUint32(&s.consecutiveFailures, 1))
	after := time.NewTimer(s.cfg.retryBackoff(tries))
	defer after.Stop()
	select {
	case <-after.C:
	case <-s.cl.ctx.Done():
	}
}

// drain drains buffered records and issues produce requests.
//
// This function is harmless if there are no records that need draining.
// We rely on that to not worry about accidental triggers of this function.
func (s *sink) drain() {
	// Before we begin draining, sleep a tiny bit. This helps when a high
	// volume new sink began draining with no linger; rather than
	// immediately eating just one record, we allow it to buffer a bit
	// before we loop draining.
	time.Sleep(time.Millisecond)

	again := true
	for again {
		if atomic.SwapUint32(&s.doBackoff, 0) == 1 {
			s.cl.triggerUpdateMetadata()
			s.backoff()
			atomic.AddUint32(&s.backoffSeq, 1)
			atomic.StoreUint32(&s.doBackoff, 0) // clear any pile on failures before seq inc
		}

		sem := s.inflightSem.Load().(chan struct{})
		sem <- struct{}{}

		var req *produceRequest
		var txnReq *kmsg.AddPartitionsToTxnRequest
		req, txnReq, again = s.createReq()

		// If we created a request with no batches, everything may be
		// failing or lingering. Release the sem and continue.
		if len(req.batches) == 0 {
			again = s.drainState.maybeFinish(again)
			<-sem
			continue
		}

		// At this point, we need our producer ID.
		id, epoch, err := s.cl.producerID()
		if err == nil && txnReq != nil {
			err = s.doTxnReq(req, txnReq, id, epoch)
		}

		// If the producer ID fn or txn req errored, we fail everything.
		// The error is unrecoverable.
		if err != nil {
			s.cl.failProducerID(req.producerID, req.producerEpoch, err)
			for _, partitions := range req.batches {
				for _, batch := range partitions {
					batch.owner.addedToTxn = false
					batch.owner.failAllRecords(err)
				}

			}
			again = s.drainState.maybeFinish(again)
			<-sem
			continue
		}

		// Again we check if there are any batches to send: our txn req
		// could have had some non-fatal partition errors that removed
		// partitions from our req.
		if len(req.batches) == 0 {
			again = s.drainState.maybeFinish(again)
			<-sem
			continue
		}

		// Finally, just before we issue our request, we set the final
		// fields of the struct.
		req.producerID = id
		req.producerEpoch = epoch
		req.backoffSeq = s.backoffSeq

		s.b.doSequencedAsyncPromise(
			s.cl.ctx,
			req,
			func(resp kmsg.Response, err error) {
				s.handleReqResp(req, resp, err)
				<-sem
			},
		)

		again = s.drainState.maybeFinish(again)
	}
}

// doTxnReq issues an AddPartitionsToTxnRequest for a produce request for all
// partitions that need to be added to a transaction.
func (s *sink) doTxnReq(
	req *produceRequest,
	txnReq *kmsg.AddPartitionsToTxnRequest,
	producerID int64,
	producerEpoch int16,
) error {
	txnReq.ProducerID = producerID
	txnReq.ProducerEpoch = producerEpoch
	kresp, err := s.cl.Request(s.cl.ctx, txnReq)

	if err != nil { // if we could not even complete the request, this is fatal.
		return err
	}

	resp := kresp.(*kmsg.AddPartitionsToTxnResponse)
	var retErr error
	var fatal bool
	for _, topic := range resp.Topics {
		topicBatches := req.batches[topic.Topic]
		for _, partition := range topic.Partitions {
			if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
				switch err {
				case kerr.InvalidProducerIDMapping,
					kerr.InvalidProducerEpoch:
					retErr = err // all partitions should have the same fatal error

					fatal = true
				}

				batch := topicBatches[partition.Partition]
				batch.owner.addedToTxn = false
				if !fatal {
					batch.owner.resetBatchDrainIdx()
					delete(topicBatches, partition.Partition)
				}
			}
			if len(topicBatches) == 0 {
				delete(req.batches, topic.Topic)
			}
		}
	}
	return retErr
}

// requeueUnattemptedReq resets all drain indices to zero on all buffers
// where the batch is the first in the buffer.
func (s *sink) requeueUnattemptedReq(req *produceRequest) {
	var maybeDrain bool
	req.batches.onEachFirstBatchWhileBatchOwnerLocked(func(batch seqRecBatch) {
		if batch.isTimedOut(s.cfg.recordTimeout) {
			batch.owner.lockedFailAllRecords(ErrRecordTimeout)
		}
		batch.owner.resetBatchDrainIdx()
		maybeDrain = true
	})
	if maybeDrain {
		// If the sink has not backed off since issuing this request,
		// we ensure here that it will backoff before the next request.
		if req.backoffSeq == atomic.LoadUint32(&s.backoffSeq) {
			atomic.StoreUint32(&s.doBackoff, 1)
		}
		s.maybeDrain()
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
func (s *sink) errorAllRecordsInAllRecordBuffersInRequest(
	req *produceRequest,
	err error,
) {
	for _, partitions := range req.batches {
		s.errorAllRecordsInAllRecordBuffersInPartitions(
			partitions,
			err,
		)
	}
}

// errorAllRecordsInAllRecordBuffersInPartitions is similar to the extra
// verbosely named function just above; read that documentation.
func (s *sink) errorAllRecordsInAllRecordBuffersInPartitions(
	partitions map[int32]seqRecBatch,
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
//
// This logic does mean that we will never use the full potential 5 in flight
// outside of a small window during the store, but some pages in the Kafka
// confluence basically show that more than two in flight has marginal benefit
// anyway (although that may be due to their Java API).
func (s *sink) firstRespCheck(version int16) {
	if s.produceVersionKnown == 0 { // this is the only place this can be checked non-atomically
		s.produceVersion = version
		atomic.StoreUint32(&s.produceVersionKnown, 1)
		if version >= 4 {
			s.inflightSem.Store(make(chan struct{}, 4))
		}
	}
}

// handleReqClientErr is called when the client errors before receiving a
// produce response.
func (s *sink) handleReqClientErr(req *produceRequest, err error) {
	switch {
	case err == ErrBrokerDead:
		// A dead broker means the broker may have migrated, so we
		// retry to force a metadata reload.
		s.handleRetryBatches(req.batches, true)

	case isRetriableBrokerErr(err):
		s.requeueUnattemptedReq(req)

	default:
		s.errorAllRecordsInAllRecordBuffersInRequest(req, err)
	}
}

func (s *sink) handleReqResp(req *produceRequest, resp kmsg.Response, err error) {
	// If we had an err, it is from the client itself. This is either a
	// retriable conn failure or a total loss (e.g. auth failure).
	if err != nil {
		s.handleReqClientErr(req, err)
		return
	}
	s.firstRespCheck(req.version)
	atomic.StoreUint32(&s.consecutiveFailures, 0)

	var reqRetry seqRecBatches // handled at the end
	// On normal retriable errors, we backoff. We only do not if we detect
	// data loss and data loss is our only error.
	var backoffRetry bool

	pr := resp.(*kmsg.ProduceResponse)
	for _, rTopic := range pr.Topics {
		topic := rTopic.Topic
		partitions, ok := req.batches[topic]
		if !ok {
			continue // should not hit this
		}
		delete(req.batches, topic)

		for _, rPartition := range rTopic.Partitions {
			partition := rPartition.Partition
			batch, ok := partitions[partition]
			if !ok {
				continue // should not hit this
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
			// retry function checks for migration problems.
			if !batch.isFirstBatchInRecordBuf() {
				continue
			}

			err := kerr.ErrorForCode(rPartition.ErrorCode)
			switch {
			case kerr.IsRetriable(err) &&
				err != kerr.CorruptMessage &&
				batch.tries < s.cfg.retries:
				// Retriable: add to retry map.
				backoffRetry = true
				reqRetry.addSeqBatch(topic, partition, batch)

			case err == kerr.OutOfOrderSequenceNumber,
				err == kerr.UnknownProducerID:
				// OOOSN always means data loss 1.0.0+ and is ambiguous prior.
				// We assume the worst and only continue if requested.
				//
				// UnknownProducerID was introduced to allow some form of safe
				// handling, but KIP-360 demonstrated that resetting sequence
				// numbers is fundamentally unsafe, so we treat it like OOOSN.
				//
				// 2.5.0 introduced some behavior to potentially safely reset
				// the sequence numbers by bumping an epoch (see KIP-360).
				// For the idempotent producer, the solution is to fail all
				// buffered records and then let the client user reset things
				// with the understanding that they cannot guard against
				// potential dups / reordering at that point. Realistically,
				// that's no better than just a config knob that allows
				// the user to continue (our stopOnDataLoss flag), so
				// we do not try any logic in the idempotent case.
				if s.cfg.stopOnDataLoss || err == kerr.UnknownProducerID && s.cl.producer.idVersion >= 3 && s.cfg.txnID != nil {
					s.cl.failProducerID(req.producerID, req.producerEpoch, err)
					s.cl.finishBatch(batch.recBatch, partition, rPartition.BaseOffset, err)
					continue
				}
				if s.cfg.onDataLoss != nil {
					s.cfg.onDataLoss(topic, partition)
				}
				batch.owner.resetSeq()
				reqRetry.addSeqBatch(topic, partition, batch)

			case err == kerr.DuplicateSequenceNumber: // ignorable, but we should not get
				err = nil
				fallthrough
			default:
				s.cl.finishBatch(batch.recBatch, partition, rPartition.BaseOffset, err)
			}
		}

		if len(partitions) > 0 {
			s.errorAllRecordsInAllRecordBuffersInPartitions(
				partitions,
				ErrNoResp,
			)
		}
	}
	if len(req.batches) > 0 {
		s.errorAllRecordsInAllRecordBuffersInRequest(
			req,
			ErrNoResp,
		)
	}

	if len(reqRetry) > 0 {
		s.handleRetryBatches(reqRetry, backoffRetry)
	}
}

// finishBatch removes a batch from its owning record buffer and finishes all
// records in the batch.
//
// This is safe even if the owning recBuf migrated sinks, since we are
// finishing based off the status of an inflight req from the original sink.
func (cl *Client) finishBatch(batch *recBatch, partition int32, baseOffset int64, err error) {
	batch.removeFromRecordBuf()
	for i, pnr := range batch.records {
		pnr.Offset = baseOffset + int64(i)
		pnr.Partition = partition
		cl.finishRecordPromise(pnr.promisedRec, err)
		batch.records[i] = noPNR
	}
	emptyRecordsPool.Put(&batch.records)
}

// handleRetryBatches sets any first-buf-batch to failing and triggers a
// metadata that will eventually clear the failing state.
//
// If the retry is due to detecting data loss (and only that), then we
// do not need to refresh metadata.
func (s *sink) handleRetryBatches(retry seqRecBatches, withBackoff bool) {
	var needsMetaUpdate bool
	retry.onEachFirstBatchWhileBatchOwnerLocked(func(batch seqRecBatch) {
		if batch.isTimedOut(s.cfg.recordTimeout) {
			batch.owner.lockedFailAllRecords(ErrRecordTimeout)
		}
		batch.owner.resetBatchDrainIdx()
		if withBackoff {
			batch.owner.failing = true
			needsMetaUpdate = true
		}
	})

	if needsMetaUpdate {
		s.cl.triggerUpdateMetadata()
	} else if !withBackoff {
		s.maybeDrain()
	}
}

// addRecBuf adds a new record buffer to be drained to a sink and clears the
// buffer's failing state.
func (s *sink) addRecBuf(add *recBuf) {
	s.mu.Lock()
	add.recBufsIdx = len(s.recBufs)
	s.recBufs = append(s.recBufs, add)
	s.mu.Unlock()

	add.clearFailing()
}

// removeRecBuf removes a record buffer from a sink.
func (s *sink) removeRecBuf(rm *recBuf) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if rm.recBufsIdx != len(s.recBufs)-1 {
		s.recBufs[rm.recBufsIdx], s.recBufs[len(s.recBufs)-1] =
			s.recBufs[len(s.recBufs)-1], nil

		s.recBufs[rm.recBufsIdx].recBufsIdx = rm.recBufsIdx
	} else {
		s.recBufs[rm.recBufsIdx] = nil // do not let this removal hang around
	}

	s.recBufs = s.recBufs[:len(s.recBufs)-1]
	if s.recBufsStart == len(s.recBufs) {
		s.recBufsStart = 0
	}
}

// recBuf is a buffer of records being produced to a partition and (usually)
// being drained by a sink. This is only not drained if the partition has
// a load error and thus does not a have a sink to be drained into.
type recBuf struct {
	cfg *cfg
	cl  *Client // for record finishing

	topic     string
	partition int32

	// addedToTxn, for transactions only, signifies whether this partition
	// has been added to the transaction yet or not.
	//
	// This does not need to be under the mu since it is updated either
	// serially in building a req (the first time) or after failing to add
	// the partition to a txn (again serially), or in EndTransaction after
	// all buffered records are flushed (if the API is used correctly).
	addedToTxn bool

	mu sync.Mutex // guards r/w access to all fields below

	// sink is who is currently draining us. This can be modified
	// concurrently during a metadata update.
	//
	// The first set to a non-nil sink is done without a mutex.
	//
	// Since only metadata updates can change the sink, metadata updates
	// also read this without a mutex.
	sink *sink
	// recBufsIdx is our index into our current sink's recBufs field.
	// This exists to aid in removing the buffer from the sink.
	recBufsIdx int

	// seq is used for the seq in each record batch. This is incremented in
	// bufferRecord and can be reset when processing a response.
	seq int32
	// batch0Seq is the seq of the batch at batchDrainIdx 0. If we reset
	// the drain index, we reset seq with this number. If we successfully
	// finish batch 0, we bump this.
	batch0Seq int32

	// batches is our list of buffered records. Batches are appended as the
	// final batch crosses size thresholds or as drain freezes batches from
	// further modification.
	//
	// Most functions in a sink only operate on a batch if the batch is the
	// first batch in a buffer. This is necessary to ensure that all
	// records are truly finished without error in order.
	batches []*recBatch
	// batchDrainIdx is where the next batch will drain from. We only
	// remove from the head of batches when a batch is finished.
	// This is read while buffering and modified in a few places.
	batchDrainIdx int

	// lingering is a timer that avoids starting maybeDrain until
	// expired, allowing for more records to be buffered in a single batch.
	//
	// Note that if something else starts a drain, if the first batch of
	// this buffer fits into the request, it will be used.
	//
	// This is on recBuf rather than Sink to avoid some complicated
	// interactions of triggering the sink to loop or not. Ideally, with
	// the sticky partition hashers, we will only have a few partitions
	// lingering and that this is on a RecBuf should not matter.
	lingering *time.Timer

	// failing is set when we encounter a temporary partition error during
	// producing, such as UnknownTopicOrPartition (signifying the partition
	// moved to a different broker).
	//
	// It is always cleared on metadata update.
	failing bool
}

func messageSet0Length(r *Record) int32 {
	const length = 4 + // array len
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
func (recBuf *recBuf) bufferRecord(pr promisedRec, abortOnNewBatch bool) bool {
	recBuf.mu.Lock()
	defer recBuf.mu.Unlock()

	// Timestamp after locking to ensure sequential, and truncate to
	// milliseconds to avoid some accumulated rounding error problems
	// (see Shopify/sarama#1455)
	pr.Timestamp = time.Now().Truncate(time.Millisecond)

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
		produceVersionKnown := recBuf.sink != nil && atomic.LoadUint32(&recBuf.sink.produceVersionKnown) == 1
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

		if batch.tries == 0 && newBatchLength <= recBuf.cfg.maxRecordBatchBytes {
			newBatch = false
			batch.appendRecord(pr, recordNumbers)
		}
	}

	if newBatch {
		if abortOnNewBatch {
			return false
		}
		recBuf.batches = append(recBuf.batches, recBuf.newRecordBatch(pr))
	}

	// Our sink could be nil if our metadata loaded a partition that is
	// erroring.
	if recBuf.sink == nil {
		return true
	}

	if recBuf.cfg.linger == 0 {
		if firstBatch {
			recBuf.sink.maybeDrain()
		}
	} else {
		// With linger, if this is a new batch but not the first, we
		// stop lingering and begin draining. The drain loop will
		// restart our linger once this buffer has one batch left.
		if newBatch && !firstBatch ||
			// If this is the first batch, try lingering; if
			// we cannot, we are being flushed and must drain.
			firstBatch && !recBuf.lockedMaybeStartLinger() {
			recBuf.lockedStopLinger()
			recBuf.sink.maybeDrain()
		}
	}
	return true
}

// lockedMaybeStartLinger begins a linger timer unless the producer is being flushed.
func (recBuf *recBuf) lockedMaybeStartLinger() bool {
	// Note that we must use this flushing int32; we cannot just have a
	// flushing field on the recBuf that is toggled on flush.
	//
	// If we did, then a new rec buf could be created and records sent to
	// while we are flushing. We would have to block a much larger chunk
	// of the pipeline, rather than just relying on this int.
	if atomic.LoadInt32(&recBuf.cl.producer.flushing) == 1 {
		return false
	}
	recBuf.lingering = time.AfterFunc(recBuf.cfg.linger, recBuf.sink.maybeDrain)
	return true
}

// lockedStopLinger stops a linger if there is one.
func (recBuf *recBuf) lockedStopLinger() {
	if recBuf.lingering != nil {
		recBuf.lingering.Stop()
		recBuf.lingering = nil
	}
}

// unlinger stops lingering if it is started and begins draining.
func (recBuf *recBuf) unlinger() {
	recBuf.mu.Lock()
	defer recBuf.mu.Unlock()
	recBuf.lockedStopLinger()
	if recBuf.sink != nil {
		recBuf.sink.maybeDrain()
	}
}

// bumpRepeatedLoadErr is provided to bump a buffer's number of consecutive
// load errors during metadata updates.
//
// If the metadata loads an error for the topicPartition that this recBuf
// is on, the first batch in the buffer has its try count bumped.
//
// Partition load errors are generally temporary (leader/listener/replica not
// available), and this try bump is not expected to do much. If for some reason
// a partition errors for a long time, this function drops all buffered
// records.
//
// This is also called if the entire topic errors, which has similar retriable
// errors.
//
// This is really coarse logic with the intent solely being an escape hatch for
// forever failing partitions.
func (recBuf *recBuf) bumpRepeatedLoadErr(err error) {
	recBuf.mu.Lock()
	defer recBuf.mu.Unlock()
	if len(recBuf.batches) == 0 {
		return
	}
	batch0 := recBuf.batches[0]
	batch0.tries++
	if batch0.tries > recBuf.cl.cfg.retries {
		recBuf.lockedFailAllRecords(err)
	}
}

// failAllRecords fails all buffered records with err.
func (recBuf *recBuf) failAllRecords(err error) {
	recBuf.mu.Lock()
	defer recBuf.mu.Unlock()
	recBuf.lockedFailAllRecords(err)
}

// lockedFailAllRecords is the same as above, but already in a lock.
//
// This is used anywhere where we have to fail and remove an entire batch.  It
// is likely we did not even attempt the batch, so we cannot remove it and
// leave the others because our seq num chain would be broken.
func (recBuf *recBuf) lockedFailAllRecords(err error) {
	recBuf.lockedStopLinger()
	for _, batch := range recBuf.batches {
		for i, pnr := range batch.records {
			recBuf.cl.finishRecordPromise(pnr.promisedRec, err)
			batch.records[i] = noPNR
		}
		emptyRecordsPool.Put(&batch.records)
	}
	recBuf.resetBatchDrainIdx()
	recBuf.batches = nil
}

// clearFailing clears a buffer's failing state if it is failing.
//
// This is called when a buffer is added to a sink (to clear a failing state
// from migrating buffers between sinks) or when a metadata update sees the
// sink is still on the same source.
//
// Note the sink cannot be nil here, since nil sinks correspond to load errors,
// and partitions with load errors do not call clearFailing.
func (recBuf *recBuf) clearFailing() {
	recBuf.mu.Lock()
	defer recBuf.mu.Unlock()

	wasFailing := recBuf.failing
	recBuf.failing = false

	if wasFailing && len(recBuf.batches) != recBuf.batchDrainIdx {
		recBuf.sink.maybeDrain()
	}
}

func (recBuf *recBuf) resetBatchDrainIdx() {
	recBuf.seq = recBuf.batch0Seq
	recBuf.batchDrainIdx = 0
}

// resetSeq resets a buffer's seq.
//
// Pre 2.5.0, this function should only be called if it is *acceptable* to
// continue on data loss. The client does this automatically given proper
// configuration.
//
// 2.5.0+, it is safe to call this if the producer ID can be reset (KIP-360).
func (recBuf *recBuf) resetSeq() {
	recBuf.mu.Lock()
	defer recBuf.mu.Unlock()
	recBuf.seq = 0
	recBuf.batch0Seq = 0
}

// promisedRec ties a record with the callback that will be called once
// a batch is finally written and receives a response.
type promisedRec struct {
	promise func(*Record, error)
	*Record
}

// recordNumbers tracks a few numbers for a record that is buffered.
type recordNumbers struct {
	wireLength     int32
	lengthField    int32
	timestampDelta int32
}

// promisedNumberedRecord ties a promised record to its calculated numbers.
type promisedNumberedRecord struct {
	recordNumbers
	promisedRec
}

var noPNR promisedNumberedRecord
var emptyRecordsPool = sync.Pool{
	New: func() interface{} {
		r := make([]promisedNumberedRecord, 0, 100)
		return &r
	},
}

// recBatch is the type used for buffering records before they are written.
type recBatch struct {
	owner *recBuf // who owns us

	tries int // if this was sent before and is thus now immutable

	v0wireLength int32 // same as wireLength, but for message set v0
	v1wireLength int32 // same as wireLength, but for message set v1
	wireLength   int32 // tracks total size this batch would currently encode as

	attrs          int16
	firstTimestamp int64 // since unix epoch, in millis

	records []promisedNumberedRecord
}

type seqRecBatch struct {
	seq int32
	*recBatch
}

// appendRecord saves a new record to a batch.
func (b *recBatch) appendRecord(pr promisedRec, nums recordNumbers) {
	b.wireLength += nums.wireLength
	b.v0wireLength += messageSet0Length(pr.Record)
	b.v1wireLength += messageSet1Length(pr.Record)
	b.records = append(b.records, promisedNumberedRecord{
		nums,
		pr,
	})
}

// newRecordBatch returns a new record batch for a topic and partition
// containing the given record.
func (recBuf *recBuf) newRecordBatch(pr promisedRec) *recBatch {
	const recordBatchOverhead = 4 + // array len
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
		4 + // seq
		4 // record array length
	b := &recBatch{
		owner:          recBuf,
		firstTimestamp: pr.Timestamp.UnixNano() / 1e6,
		records:        (*(emptyRecordsPool.Get().(*[]promisedNumberedRecord)))[:0],
	}
	pnr := promisedNumberedRecord{
		b.calculateRecordNumbers(pr.Record),
		pr,
	}
	b.records = append(b.records, pnr)
	b.wireLength = recordBatchOverhead + pnr.wireLength
	b.v0wireLength = messageSet0Length(pr.Record)
	b.v1wireLength = messageSet1Length(pr.Record)
	return b
}

// calculateRecordNumbers returns the numbers for a record if it were added to
// the record batch. Nothing accounts for overflows; that should be done prior.
func (b *recBatch) calculateRecordNumbers(r *Record) recordNumbers {
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

// lockedIsFirstBatch returns if the batch in a recBatch is the first batch in
// a records. We only ever want to update batch / buffer logic if the batch is
// the first in the buffer.
func (batch *recBatch) lockedIsFirstBatch() bool {
	return len(batch.owner.batches) > 0 && batch.owner.batches[0] == batch
}

// The above, but inside the owning recBuf mutex.
func (batch *recBatch) isFirstBatchInRecordBuf() bool {
	batch.owner.mu.Lock()
	defer batch.owner.mu.Unlock()

	return batch.lockedIsFirstBatch()
}

// removeFromRecordBuf is called in a successful produce response, incrementing
// past the record buffer's now-known-to-be-in-Kafka-batch.
func (batch *recBatch) removeFromRecordBuf() {
	recBuf := batch.owner
	recBuf.mu.Lock()
	defer recBuf.mu.Unlock()

	// This could not be the first batch if the user aborted everything
	// concurrently.
	if batch.lockedIsFirstBatch() {
		recBuf.batch0Seq += int32(len(recBuf.batches[0].records))
		recBuf.batches[0] = nil
		recBuf.batches = recBuf.batches[1:]
		recBuf.batchDrainIdx--
	}
}

// isTimedOut, called only on frozen batches, returns whether the first record
// in a batch is past the limit.
func (batch *recBatch) isTimedOut(limit time.Duration) bool {
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
	batches seqRecBatches

	producerID    int64
	producerEpoch int16

	compressor *compressor
}

type seqRecBatches map[string]map[int32]seqRecBatch

func (rbs *seqRecBatches) addBatch(topic string, part int32, seq int32, batch *recBatch) {
	if *rbs == nil {
		*rbs = make(seqRecBatches, 5)
	}
	topicBatches, exists := (*rbs)[topic]
	if !exists {
		topicBatches = make(map[int32]seqRecBatch, 1)
		(*rbs)[topic] = topicBatches
	}
	topicBatches[part] = seqRecBatch{seq, batch}
}

func (rbs *seqRecBatches) addSeqBatch(topic string, part int32, batch seqRecBatch) {
	if *rbs == nil {
		*rbs = make(seqRecBatches, 5)
	}
	topicBatches, exists := (*rbs)[topic]
	if !exists {
		topicBatches = make(map[int32]seqRecBatch, 1)
		(*rbs)[topic] = topicBatches
	}
	topicBatches[part] = batch
}

func (rbs seqRecBatches) onEachFirstBatchWhileBatchOwnerLocked(fn func(seqRecBatch)) {
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
				dst = batch.appendToAsMessageSet(dst, uint8(p.version), p.compressor)
			} else {
				dst = batch.appendTo(
					dst,
					p.compressor,
					p.producerID,
					p.producerEpoch,
					p.txnID != nil,
					p.version,
				)
			}
		}
	}
	return dst
}

func (p *produceRequest) ResponseKind() kmsg.Response {
	return &kmsg.ProduceResponse{Version: p.version}
}

func (r seqRecBatch) appendTo(
	dst []byte,
	compressor *compressor,
	producerID int64,
	producerEpoch int16,
	transactional bool,
	version int16,
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
	dst = kbin.AppendInt32(dst, r.seq)

	dst = kbin.AppendArrayLen(dst, len(r.records))
	recordsAt := len(dst)
	for i, pnr := range r.records {
		dst = pnr.appendTo(dst, int32(i))
	}

	if compressor != nil {
		toCompress := dst[recordsAt:]
		w := sliceWriters.Get().(*sliceWriter)
		defer sliceWriters.Put(w)

		compressed, codec := compressor.compress(w, toCompress, int16(version))
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

func (r seqRecBatch) appendToAsMessageSet(dst []byte, version uint8, compressor *compressor) []byte {
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
		w := sliceWriters.Get().(*sliceWriter)
		defer sliceWriters.Put(w)

		compressed, codec := compressor.compress(w, toCompress, int16(version))
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
