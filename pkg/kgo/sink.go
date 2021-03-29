package kgo

import (
	"errors"
	"hash/crc32"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type sink struct {
	cl     *Client // our owning client, for cfg, metadata triggering, context, etc.
	nodeID int32   // the node ID of the broker this sink belongs to

	// inflightSem controls the number of concurrent produce requests.  We
	// start with a limit of 1, which covers Kafka v0.11.0.0. On the first
	// response, we check what version was set in the request. If it is at
	// least 4, which 1.0.0 introduced, we upgrade the sem size.
	inflightSem         atomic.Value
	produceVersionKnown uint32 // atomic bool; 1 is true
	produceVersion      int16  // is set before produceVersionKnown

	drainState workLoop

	// seqReqResps, guarded by seqReqRespsMu, contains responses that must
	// be handled sequentially. These responses are handled asyncronously,
	// but sequentially.
	seqReqRespsMu sync.Mutex
	seqReqResps   []seqSinkReqResp

	backoffMu   sync.Mutex // guards the following
	needBackoff bool
	backoffSeq  uint32 // prevents pile on failures

	// consecutiveFailures is incremented every backoff and cleared every
	// successful response. For simplicity, if we have a good response
	// following an error response before the error response's backoff
	// occurs, the backoff is not cleared.
	consecutiveFailures uint32

	mu sync.Mutex // guards the fields below

	recBufs      []*recBuf // contains all partition records for batch building
	recBufsStart int       // incremented every req to avoid large batch starvation
}

type seqSinkReqResp struct {
	req     kmsg.Request
	promise func(kmsg.Response, error)
}

func (cl *Client) newSink(nodeID int32) *sink {
	s := &sink{
		cl:     cl,
		nodeID: nodeID,
	}
	s.inflightSem.Store(make(chan struct{}, 1))
	return s
}

// createReq returns a produceRequest from currently buffered records
// and whether there are more records to create more requests immediately.
func (s *sink) createReq() (*produceRequest, *kmsg.AddPartitionsToTxnRequest, bool) {
	req := &produceRequest{
		txnID:   s.cl.cfg.txnID,
		acks:    s.cl.cfg.acks.val,
		timeout: int32(s.cl.cfg.produceTimeout.Milliseconds()),
		batches: make(seqRecBatches, 5),

		compressor: s.cl.compressor,
	}

	var (
		// We use non-flexible lengths for what follows. These will be
		// strictly larger (unless we are creating a produce request
		// with >16K partitions for a single topic...), so using
		// non-flexible makes calculations simpler.
		wireLength      = s.cl.baseProduceRequestLength()
		wireLengthLimit = s.cl.cfg.maxBrokerWriteBytes

		moreToDrain bool

		noIdempotency = s.cl.cfg.disableIdempotency

		transactional  = req.txnID != nil
		txnReq         *kmsg.AddPartitionsToTxnRequest
		txnAddedTopics map[string]int // topic => index in txnReq
	)

	s.mu.Lock() // prevent concurrent modification to recBufs
	defer s.mu.Unlock()

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
				batchWireLength = 4 + batch.v0wireLength()
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
		if s.cl.cfg.linger > 0 {
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

		if noIdempotency {
			seq = 0
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

// maybeDrain begins a drain loop on the sink if the sink is not yet draining
// and we are not manually flushing.
func (s *sink) maybeDrain() {
	if s.cl.cfg.manualFlushing && atomic.LoadInt32(&s.cl.producer.flushing) == 0 {
		return
	}
	if s.drainState.maybeBegin() {
		go s.drain()
	}
}

func (s *sink) maybeBackoff() {
	s.backoffMu.Lock()
	backoff := s.needBackoff
	s.backoffMu.Unlock()

	if !backoff {
		return
	}
	defer s.clearBackoff()

	s.cl.triggerUpdateMetadata() // as good a time as any

	tries := int(atomic.AddUint32(&s.consecutiveFailures, 1))
	after := time.NewTimer(s.cl.cfg.retryBackoff(tries))
	defer after.Stop()

	select {
	case <-after.C:
	case <-s.cl.ctx.Done():
	}
}

func (s *sink) maybeTriggerBackoff(seq uint32) {
	s.backoffMu.Lock()
	defer s.backoffMu.Unlock()
	if seq == s.backoffSeq {
		s.needBackoff = true
	}
}

func (s *sink) clearBackoff() {
	s.backoffMu.Lock()
	defer s.backoffMu.Unlock()
	s.backoffSeq++
	s.needBackoff = false
}

// drain drains buffered records and issues produce requests.
//
// This function is harmless if there are no records that need draining.
// We rely on that to not worry about accidental triggers of this function.
func (s *sink) drain() {
	// If not lingering, before we begin draining, sleep a tiny bit. This
	// helps when a high volume new sink began draining with no linger;
	// rather than immediately eating just one record, we allow it to
	// buffer a bit before we loop draining.
	if s.cl.cfg.linger == 0 && !s.cl.cfg.manualFlushing {
		time.Sleep(5 * time.Millisecond)
	}

	s.cl.producer.incDrains()
	defer s.cl.producer.decDrains()

	again := true
	for again {
		if s.cl.producer.isAborting() {
			s.drainState.hardFinish()
			return
		}

		s.maybeBackoff()

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
			txnReq.ProducerID = id
			txnReq.ProducerEpoch = epoch
			err = s.doTxnReq(req, txnReq)
		}

		// If the producer ID fn or txn req errored, we fail everything.
		// The error is unrecoverable except in some specific instances.
		// We do not need to clear the addedToTxn flag for any recBuf
		// it was set on, since producer id recovery resets the flag.
		if err != nil {
			s.cl.cfg.logger.Log(LogLevelInfo, "InitProducerID or AddPartitionsToTxn error, failing producer id",
				"err", err,
			)
			s.cl.failProducerID(req.producerID, req.producerEpoch, err)
			for _, partitions := range req.batches {
				for _, batch := range partitions {
					batch.owner.failAllRecords(err)
				}
			}
			again = s.drainState.maybeFinish(again)
			<-sem
			continue
		}

		// Again we check if there are any batches to send: our txn req
		// could have had some non-fatal partition errors that removed
		// partitions from our req, such as unknown topic.
		if len(req.batches) == 0 {
			again = s.drainState.maybeFinish(again)
			<-sem
			continue
		}

		// Finally, set our final fields in the req struct.
		req.producerID = id
		req.producerEpoch = epoch
		req.backoffSeq = s.backoffSeq // safe to read outside mu since we are in drain loop

		s.doSequenced(req, func(resp kmsg.Response, err error) {
			s.handleReqResp(req, resp, err)
			<-sem
		})

		again = s.drainState.maybeFinish(again)
	}
}

// With handleseqReqResps below, this function ensures that all request responses
// are handled in order. We use this guarantee while in handleReqResp below.
//
// Note that some request may finish while a later concurrently issued one
// is successful; this is fine.
func (s *sink) doSequenced(
	req kmsg.Request,
	promise func(kmsg.Response, error),
) {
	s.seqReqRespsMu.Lock()
	defer s.seqReqRespsMu.Unlock()

	s.seqReqResps = append(s.seqReqResps, seqSinkReqResp{req, promise})
	if len(s.seqReqResps) == 1 {
		go s.handleSeqReqResps(s.seqReqResps[0])
	}
}

// Ensures that all request responses are processed in order.
func (s *sink) handleSeqReqResps(reqResp seqSinkReqResp) {
more:
	br, err := s.cl.brokerOrErr(s.cl.ctx, s.nodeID, ErrUnknownBroker)
	if err != nil {
		reqResp.promise(nil, err)
	}
	resp, err := br.waitResp(s.cl.ctx, reqResp.req)
	reqResp.promise(resp, err)

	s.seqReqRespsMu.Lock()
	s.seqReqResps = s.seqReqResps[1:]
	if len(s.seqReqResps) > 0 {
		reqResp = s.seqReqResps[0]
		s.seqReqRespsMu.Unlock()
		goto more
	}
	s.seqReqRespsMu.Unlock()
}

// doTxnReq issues an AddPartitionsToTxnRequest for a produce request for all
// partitions that need to be added to a transaction.
func (s *sink) doTxnReq(
	req *produceRequest,
	txnReq *kmsg.AddPartitionsToTxnRequest,
) error {
	start := time.Now()
	tries := 0
start:
	resp, err := txnReq.RequestWith(s.cl.ctx, s.cl)
	if err != nil { // if we could not even complete the request, this is fatal.
		return err
	}

	for _, topic := range resp.Topics {
		topicBatches := req.batches[topic.Topic]
		for _, partition := range topic.Partitions {
			if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
				if err == kerr.ConcurrentTransactions && time.Since(start) < 10*time.Second {
					tries++
					s.cl.cfg.logger.Log(LogLevelInfo, "AddPartitionsToTxn failed with CONCURRENT_TRANSACTIONS, which may be because we ended a txn and began producing in a new txn too quickly; backing off and retrying",
						"backoff", 100*time.Millisecond,
						"since_request_tries_start", time.Since(start),
						"tries", tries,
					)
					select {
					case <-time.After(100 * time.Millisecond):
					case <-s.cl.ctx.Done():
						s.cl.cfg.logger.Log(LogLevelError, "abandoning AddPartitionsToTxn retry due to client ctx quitting")
						return err
					}
					goto start
				}
				if !kerr.IsRetriable(err) {
					return err
				}
				batch := topicBatches[partition.Partition]

				// Lock before resetting the drain index, as
				// buffering records reads it. We lock around
				// addedToTxn as well just because.
				batch.owner.mu.Lock()
				batch.owner.addedToTxn = false
				batch.owner.resetBatchDrainIdx()
				batch.owner.mu.Unlock()

				delete(topicBatches, partition.Partition)
			}
			if len(topicBatches) == 0 {
				delete(req.batches, topic.Topic)
			}
		}
	}
	return nil
}

// requeueUnattemptedReq resets all drain indices to zero on all buffers
// where the batch is the first in the buffer.
func (s *sink) requeueUnattemptedReq(req *produceRequest, err error) {
	var maybeDrain bool
	req.batches.onEachFirstBatchWhileBatchOwnerLocked(func(batch seqRecBatch) {
		// If we fail all records here, we likely will have out of
		// order seq nums; hopefully the client user does not stop
		// on data loss, since this is not truly data loss.
		if batch.isTimedOut(s.cl.cfg.recordTimeout) {
			batch.owner.lockedFailAllRecords(ErrRecordTimeout)
		} else if batch.tries == s.cl.cfg.retries {
			batch.owner.lockedFailAllRecords(err)
		}
		batch.owner.resetBatchDrainIdx()
		maybeDrain = true
	})
	if maybeDrain {
		s.maybeTriggerBackoff(req.backoffSeq)
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
		s.handleRetryBatches(req.batches)

	case isRetriableBrokerErr(err):
		s.requeueUnattemptedReq(req, err)

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

	// If we have no acks, we will have no response. The following block is
	// basically an extremely condensed version of everything that follows.
	// We *do* retry on error even with no acks, because an error would
	// mean the write itself failed.
	if req.acks == 0 {
		for _, partitions := range req.batches {
			for partition, batch := range partitions {
				if !batch.isFirstBatchInRecordBuf() {
					continue
				}
				s.cl.finishBatch(batch.recBatch, req.producerID, req.producerEpoch, partition, 0, nil)
			}
		}
		return
	}

	var reqRetry seqRecBatches // handled at the end

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
				batch.tries < s.cl.cfg.retries:
				reqRetry.addSeqBatch(topic, partition, batch)

			case err == kerr.OutOfOrderSequenceNumber,
				err == kerr.UnknownProducerID,
				err == kerr.InvalidProducerIDMapping,
				err == kerr.InvalidProducerEpoch:

				// OOOSN always means data loss 1.0.0+ and is ambiguous prior.
				// We assume the worst and only continue if requested.
				//
				// UnknownProducerID was introduced to allow some form of safe
				// handling, but KIP-360 demonstrated that resetting sequence
				// numbers is fundamentally unsafe, so we treat it like OOOSN.
				//
				// InvalidMapping is similar to UnknownProducerID, but occurs
				// when the txnal coordinator timed out our transaction.
				//
				// 2.5.0
				// =====
				// 2.5.0 introduced some behavior to potentially safely reset
				// the sequence numbers by bumping an epoch (see KIP-360).
				//
				// For the idempotent producer, the solution is to fail all
				// buffered records and then let the client user reset things
				// with the understanding that they cannot guard against
				// potential dups / reordering at that point. Realistically,
				// that's no better than a config knob that allows the user
				// to continue (our stopOnDataLoss flag), so for the idempotent
				// producer, if stopOnDataLoss is false, we just continue.
				//
				// For the transactional producer, we always fail the producerID.
				// EndTransaction will trigger recovery if possible.
				//
				// 2.7.0
				// =====
				// InvalidProducerEpoch became retriable in 2.7.0. Prior, it
				// was ambiguous (timeout? fenced?). In 2.7.0, InvalidProducerEpoch
				// is only returned on produce, and then we can recover on other
				// txn coordinator requests, which have PRODUCER_FENCED vs
				// TRANSACTION_TIMED_OUT. Supposedly the InvalidProducerEpoch is
				// now retriable, but it is actually removed entirely from the
				// Kafka source.

				if s.cl.cfg.txnID != nil || s.cl.cfg.stopOnDataLoss {
					s.cl.cfg.logger.Log(LogLevelInfo, "batch errored, failing the producer ID",
						"topic", topic,
						"partition", partition,
						"producer_id", req.producerID,
						"producer_epoch", req.producerEpoch,
						"err", err,
					)
					s.cl.failProducerID(req.producerID, req.producerEpoch, err)
					s.cl.finishBatch(batch.recBatch, req.producerID, req.producerEpoch, partition, rPartition.BaseOffset, err)
					continue
				}
				if s.cl.cfg.onDataLoss != nil {
					s.cl.cfg.onDataLoss(topic, partition)
				}

				// For OOOSN,
				//
				// We could be here because we do not have unlimited
				// retries and previously failed a retriable error.
				// The broker could technically have been fine, but we
				// locally failed a batch causing our sequence number to
				// bump (this is why we should have unlimited retries, but
				// sometimes that is not a perfect option).
				//
				// When we reset sequence numbers here, we need to also
				// fail the producer ID to ensure we do not send to a
				// broker that thinks we are still at a high seq when
				// we are sending 0. If we did not fail, then we would
				// loop with an OOOSN error.
				//
				// For UnknownProducerID,
				//
				// We could be here because we have an idempotent producer.
				// If we were transactional, we would have failed above.
				// We could just reset sequence numbers, but we may as well
				// also fail the producer ID which triggers epoch bumping
				// and simplifies logic for the OOSN thing described above.
				//
				// For InvalidProducerIDMapping && InvalidProducerEpoch,
				//
				// We should not be here, since this error occurs in the
				// context of transactions, which would be caught above.
				s.cl.cfg.logger.Log(LogLevelInfo, "batch errored with OutOfOrderSequenceNumber or UnknownProducerID, failing the producer ID and resetting the partition sequence number",
					"topic", topic,
					"partition", partition,
					"producer_id", req.producerID,
					"producer_epoch", req.producerEpoch,
					"err", err,
				)
				s.cl.failProducerID(req.producerID, req.producerEpoch, errReloadProducerID)
				batch.owner.resetSeq()
				reqRetry.addSeqBatch(topic, partition, batch)

			case err == kerr.DuplicateSequenceNumber: // ignorable, but we should not get
				s.cl.cfg.logger.Log(LogLevelInfo, "received unexpected duplicate sequence number, ignoring and treating batch as successful",
					"topic", topic,
					"partition", partition,
				)
				err = nil
				fallthrough
			default:
				if err != nil {
					s.cl.cfg.logger.Log(LogLevelInfo, "batch in a produce request failed",
						"topic", topic,
						"partition", partition,
						"err", err,
						"err_is_retriable", kerr.IsRetriable(err),
						"max_retries_reached", batch.tries == s.cl.cfg.retries,
					)
				}
				s.cl.finishBatch(batch.recBatch, req.producerID, req.producerEpoch, partition, rPartition.BaseOffset, err)
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
		s.handleRetryBatches(reqRetry)
	}
}

// finishBatch removes a batch from its owning record buffer and finishes all
// records in the batch.
//
// This is safe even if the owning recBuf migrated sinks, since we are
// finishing based off the status of an inflight req from the original sink.
func (cl *Client) finishBatch(batch *recBatch, producerID int64, producerEpoch int16, partition int32, baseOffset int64, err error) {
	recBuf := batch.owner
	recBuf.mu.Lock()
	defer recBuf.mu.Unlock()

	// This could not be the first batch if the user aborted everything
	// concurrently.
	if !batch.lockedIsFirstBatch() {
		return
	}

	// An error at this point is a non-retriable error. To guard sequence
	// number ordering, we have to fail every buffered record for this
	// partition.
	if err != nil {
		recBuf.lockedFailAllRecords(err)
		return
	}

	// Here, we know the batch made it to Kafka successfully without error.
	// We remove this batch and finish all records appropriately.
	recBuf.batch0Seq += int32(len(recBuf.batches[0].records))
	recBuf.batches[0] = nil
	recBuf.batches = recBuf.batches[1:]
	recBuf.batchDrainIdx--

	for i, pnr := range batch.records {
		pnr.Offset = baseOffset + int64(i)
		pnr.Partition = partition
		pnr.ProducerID = producerID
		pnr.ProducerEpoch = producerEpoch

		// A recBuf.attrs is updated when appending to be written.  For
		// v0 && v1 produce requests, we set bit 8 in the attrs
		// corresponding to our own RecordAttr's bit 8 being no
		// timestamp type. Thus, we can directly convert the batch
		// attrs to our own RecordAttrs.
		pnr.Attrs = RecordAttrs{uint8(batch.attrs)}

		cl.finishRecordPromise(pnr.promisedRec, err)
		batch.records[i] = noPNR
	}
	emptyRecordsPool.Put(&batch.records)
}

// handleRetryBatches sets any first-buf-batch to failing and triggers a
// metadata that will eventually clear the failing state.
func (s *sink) handleRetryBatches(retry seqRecBatches) {
	var needsMetaUpdate bool
	retry.onEachFirstBatchWhileBatchOwnerLocked(func(batch seqRecBatch) {
		// If we fail all records here, we likely will have out of
		// order seq nums; hopefully the client user does not stop
		// on data loss, since this is not truly data loss.
		if batch.isTimedOut(s.cl.cfg.recordTimeout) {
			batch.owner.lockedFailAllRecords(ErrRecordTimeout)
		} else if batch.tries == s.cl.cfg.retries {
			err := errors.New("record failed after being retried too many times")
			batch.owner.lockedFailAllRecords(err)
		}
		batch.owner.resetBatchDrainIdx()
		batch.owner.failing = true
		needsMetaUpdate = true
	})
	if needsMetaUpdate {
		s.cl.triggerUpdateMetadata()
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

// Returns the base produce length for non-flexible versions of a produce
// request, including the topic array length. See the large comment in
// maxRecordBatchBytesForTopic for why we always use non-flexible lengths (in
// short: it's strictly larger than flexible).
func (cl *Client) baseProduceRequestLength() int32 {
	const messageRequestOverhead int32 = 4 + // full length
		2 + // key
		2 + // version
		4 + // correlation ID
		2 // client ID len (always non flexible)
		// empty tag section skipped due to below description

	const produceRequestBaseOverhead int32 = 2 + // transactional ID len (flexible or not, since we cap at 16382)
		2 + // acks
		4 + // timeout
		4 // topics array length
		// empty tag section skipped due to below description

	baseLength := messageRequestOverhead + produceRequestBaseOverhead
	if cl.cfg.id != nil {
		baseLength += int32(len(*cl.cfg.id))
	}
	if cl.cfg.txnID != nil {
		baseLength += int32(len(*cl.cfg.txnID))
	}
	return baseLength
}

// Returns the maximum size a record batch can be for this given topic.
func (cl *Client) maxRecordBatchBytesForTopic(topic string) int32 {
	// At a minimum, we will have a produce request containing this one
	// topic with one partition and its record batch.
	//
	// The maximum topic length is 249, which has a 2 byte prefix for
	// flexible or non-flexible.
	//
	// Non-flexible versions will have a 4 byte length topic array prefix
	// and a 4 byte length partition array prefix.
	//
	// Flexible versions would have a 1 byte length topic array prefix and
	// a 1 byte length partition array prefix, and would also have 3 empty
	// tag sections resulting in 3 extra bytes.
	//
	// Non-flexible versions would have a 4 byte length record bytes
	// prefix. Flexible versions could have up to 5.
	//
	// For the message header itself, with flexible versions, we have one
	// extra byte for an empty tag section.
	//
	// Thus in the worst case, the flexible encoding would still be two
	// bytes short of the non-flexible encoding. We will use the
	// non-flexible encoding for our max size calculations.
	minOnePartitionBatchLength := cl.baseProduceRequestLength() +
		2 + // topic string length prefix length
		int32(len(topic)) +
		4 + // partitions array length
		4 + // partition int32 encoding length
		4 // record bytes array length

	wireLengthLimit := cl.cfg.maxBrokerWriteBytes

	recordBatchLimit := wireLengthLimit - minOnePartitionBatchLength
	if cfgLimit := cl.cfg.maxRecordBatchBytes; cfgLimit < recordBatchLimit {
		recordBatchLimit = cfgLimit
	}
	return recordBatchLimit
}

// recBuf is a buffer of records being produced to a partition and (usually)
// being drained by a sink. This is only not drained if the partition has a
// load error and thus does not a have a sink to be drained into.
//
// NOTE if we add leaderEpoch / leader to this eventually (per one of the
// KIPs), we will need to update it when migrating a recBuf on metadata update.
// Unlike a cursor, we update record buffers live (no session stopping), thus
// we will likely need an atomic. Once this happens, like in a cursor, we may
// as well just add a topicPartition pointer field, and when this happens, we
// can remove the topic and partition fields themselves back into
// topicPartition (and do the same in the cursor).
type recBuf struct {
	cl *Client // for cfg, record finishing

	topic     string
	partition int32

	// The number of bytes we can buffer in a batch for this particular
	// topic/partition. This may be less than the configured
	// maxRecordBatchBytes because of produce request overhead.
	maxRecordBatchBytes int32

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

	// seq is used for the seq in each record batch. It is incremented when
	// produce requests are made and can be reset on errors to batch0Seq.
	//
	// If idempotency is disabled, we just use "0" for the first sequence
	// when encoding our payload (see noIdempotency).
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
// This returns whether the promised record was processed or not (buffered or
// immediately errored).
func (recBuf *recBuf) bufferRecord(pr promisedRec, abortOnNewBatch bool) bool {
	recBuf.mu.Lock()
	defer recBuf.mu.Unlock()

	// Timestamp after locking to ensure sequential, and truncate to
	// milliseconds to avoid some accumulated rounding error problems
	// (see Shopify/sarama#1455)
	pr.Timestamp = time.Now().Truncate(time.Millisecond)

	newBatch := true
	drainBatch := recBuf.batchDrainIdx == len(recBuf.batches)

	produceVersionKnown := atomic.LoadUint32(&recBuf.sink.produceVersionKnown) == 1
	if !drainBatch {
		batch := recBuf.batches[len(recBuf.batches)-1]
		recordNumbers := batch.calculateRecordNumbers(pr.Record)

		newBatchLength := batch.wireLength + recordNumbers.wireLength

		// If we do not know the broker version, we may be talking
		// to <0.11.0 and be using message sets. Until we know the
		// broker version, we pessimisitically cut our batch off using
		// the largest record length numbers.
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
				newBatchLength = batch.v0wireLength() + messageSet0Length(pr.Record)
			case 2:
				newBatchLength = batch.v1wireLength + messageSet1Length(pr.Record)
			}
		}

		if batch.tries == 0 && newBatchLength <= recBuf.maxRecordBatchBytes {
			newBatch = false
			batch.appendRecord(pr, recordNumbers)
		}
	}

	if newBatch {
		newBatch := recBuf.newRecordBatch(pr)

		// Before we decide to keep this new batch, if this single record is too
		// large for a batch, then we immediately fail it.
		newBatchLength := newBatch.wireLength
		if !produceVersionKnown {
			if newBatch.v1wireLength > newBatchLength {
				newBatchLength = newBatch.v1wireLength
			}
		} else {
			switch recBuf.sink.produceVersion {
			case 0, 1:
				newBatchLength = newBatch.v0wireLength()
			case 2:
				newBatchLength = newBatch.v1wireLength
			}
		}
		if newBatchLength > recBuf.maxRecordBatchBytes {
			recBuf.cl.finishRecordPromise(pr, kerr.MessageTooLarge)
			return true
		}

		if abortOnNewBatch {
			return false
		}
		recBuf.batches = append(recBuf.batches, newBatch)
	}

	if recBuf.cl.cfg.linger == 0 {
		if drainBatch {
			recBuf.sink.maybeDrain()
		}
	} else {
		// With linger, if this is a new batch but not the first, we
		// stop lingering and begin draining. The drain loop will
		// restart our linger once this buffer has one batch left.
		if newBatch && !drainBatch ||
			// If this is the first batch, try lingering; if
			// we cannot, we are being flushed and must drain.
			drainBatch && !recBuf.lockedMaybeStartLinger() {
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
	recBuf.lingering = time.AfterFunc(recBuf.cl.cfg.linger, recBuf.sink.maybeDrain)
	return true
}

// lockedStopLinger stops a linger if there is one.
func (recBuf *recBuf) lockedStopLinger() {
	if recBuf.lingering != nil {
		recBuf.lingering.Stop()
		recBuf.lingering = nil
	}
}

// stops lingering if it is started and begins draining.
func (recBuf *recBuf) unlingerAndManuallyDrain() {
	recBuf.mu.Lock()
	defer recBuf.mu.Unlock()
	recBuf.lockedStopLinger()
	recBuf.sink.maybeDrain()
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
// This is used anywhere where we have to fail and remove an entire batch,
// if we just removed the one batch, the seq num chain would be broken.
func (recBuf *recBuf) lockedFailAllRecords(err error) {
	recBuf.lockedStopLinger()
	for _, batch := range recBuf.batches {
		// We need to guard our clearing of records against a
		// concurrent write. This is the only spot that the records
		// slice can be modified outside of the main recBuf
		// batch-buffering logic. Since we lock the recBuf, we do not
		// need to worry about record buffering. However, we need to
		// worry about a buffered write that will eventually read this
		// batch. So, we lock while we clear.
		batch.mu.Lock()
		for i, pnr := range batch.records {
			recBuf.cl.finishRecordPromise(pnr.promisedRec, err)
			batch.records[i] = noPNR
		}
		batch.records = nil
		batch.mu.Unlock()
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
func (recBuf *recBuf) clearFailing() {
	recBuf.mu.Lock()
	defer recBuf.mu.Unlock()

	recBuf.failing = false
	if len(recBuf.batches) != recBuf.batchDrainIdx {
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
// 2.5.0+, it is safe to call this if the producer ID can be reset (KIP-360),
// which is a pretty edgy edge condition. Otherwise, we still reset if
// configured to do so on data loss.
func (recBuf *recBuf) resetSeq() {
	recBuf.mu.Lock()
	defer recBuf.mu.Unlock()
	recBuf.seq = 0
	recBuf.batch0Seq = 0

	// Since we are resetting the sequence numbers, we want to **be sure**
	// that the next batch chosen will be the first. Otherwise, we will use
	// seq 0 for a later batch.
	recBuf.resetBatchDrainIdx()
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

	v1wireLength int32 // same as wireLength, but for message set v1
	wireLength   int32 // tracks total size this batch would currently encode as

	attrs          int16 // updated during apending; read and converted to RecordAttrs on success
	firstTimestamp int64 // since unix epoch, in millis

	// mu guards against records being concurrently modified in
	// lockedFailAllRecords just as we are writing the request.
	// See comment in that function and in appendTo.
	mu sync.Mutex

	records []promisedNumberedRecord
}

func (b *recBatch) v0wireLength() int32 { return b.v1wireLength - 8 }

// seqRecBatch is a recBatch with a sequence number ready to be written
// into a request.
type seqRecBatch struct {
	seq int32 // 0 in addBatch if idempotency is disabled
	*recBatch
}

// appendRecord saves a new record to a batch.
//
// This is called under the owning recBuf's mu, meaning records cannot be
// concurrently modified by failing.
func (b *recBatch) appendRecord(pr promisedRec, nums recordNumbers) {
	b.wireLength += nums.wireLength
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
		kbin.VarintLen(tsDelta) +
		kbin.VarintLen(offsetDelta) +
		kbin.VarintLen(int32(len(r.Key))) +
		len(r.Key) +
		kbin.VarintLen(int32(len(r.Value))) +
		len(r.Value) +
		kbin.VarintLen(int32(len(r.Headers))) // varint array len headers

	for _, h := range r.Headers {
		l += kbin.VarintLen(int32(len(h.Key))) +
			len(h.Key) +
			kbin.VarintLen(int32(len(h.Value))) +
			len(h.Value)
	}

	return recordNumbers{
		wireLength:     int32(kbin.VarintLen(int32(l)) + l),
		lengthField:    int32(l),
		timestampDelta: tsDelta,
	}
}

// lockedIsFirstBatch returns if the batch in a recBatch is the first batch in
// a records. We only ever want to update batch / buffer logic if the batch is
// the first in the buffer.
func (b *recBatch) lockedIsFirstBatch() bool {
	return len(b.owner.batches) > 0 && b.owner.batches[0] == b
}

// The above, but inside the owning recBuf mutex.
func (b *recBatch) isFirstBatchInRecordBuf() bool {
	b.owner.mu.Lock()
	defer b.owner.mu.Unlock()

	return b.lockedIsFirstBatch()
}

// isTimedOut, called only on frozen batches, returns whether the first record
// in a batch is past the limit.
func (b *recBatch) isTimedOut(limit time.Duration) bool {
	if limit == 0 {
		return false
	}
	return time.Since(b.records[0].Timestamp) > limit
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

// NOTE: if bumping max version, check for new fields in producer.init.
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
			// Concurrently, a lockedFailAllRecords could have
			// failed this batch WHILE IT WAS BUFFERED before it
			// was sent. We need to guard against our records being
			// cleared.
			batch.mu.Lock()
			if batch.records == nil {
				batch.mu.Unlock()
				continue
			}
			dst = kbin.AppendInt32(dst, partition)
			if p.version < 3 {
				dst = batch.appendToAsMessageSet(dst, uint8(p.version), p.compressor)
			} else {
				dst = batch.appendTo(
					dst,
					p.version,
					p.producerID,
					p.producerEpoch,
					p.txnID != nil,
					p.compressor,
				)
			}
			batch.mu.Unlock()
		}
	}
	return dst
}
func (*produceRequest) ReadFrom([]byte) error {
	panic("unreachable -- the client never uses ReadFrom on its internal produceRequest")
}

func (p *produceRequest) ResponseKind() kmsg.Response {
	return &kmsg.ProduceResponse{Version: p.version}
}

func (r seqRecBatch) appendTo(
	dst []byte,
	version int16,
	producerID int64,
	producerEpoch int16,
	transactional bool,
	compressor *compressor,
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
	r.attrs = 0
	if transactional {
		r.attrs |= 0x0010 // bit 5 is the "is transactional" bit
	}
	dst = kbin.AppendInt16(dst, r.attrs)
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
			r.attrs |= int16(codec)
			kbin.AppendInt32(dst[:nullableBytesLenAt], nullableBytesLen)
			kbin.AppendInt32(dst[:batchLenAt], batchLen)
			kbin.AppendInt16(dst[:attrsAt], r.attrs)
		}
	}

	kbin.AppendInt32(dst[:crcStart], int32(crc32.Checksum(dst[crcStart+4:], crc32c)))

	return dst
}

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

	r.attrs = 0

	// Produce request v0 and v1 uses message set v0, which does not have
	// timestamps. We set bit 8 in our attrs which corresponds with our own
	// kgo.RecordAttrs's bit. The attrs field is unused in a sink / recBuf
	// outside of the appending functions or finishing records; if we use
	// more bits in our internal RecordAttrs, the below will need to
	// change.
	if version == 0 || version == 1 {
		r.attrs |= 0b1000_0000
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

			r.attrs |= int16(codec)

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
