package sink

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/kafka-go/pkg/kbin"
	"github.com/twmb/kafka-go/pkg/kerr"
	"github.com/twmb/kafka-go/pkg/kgo/internal/broker"
	"github.com/twmb/kafka-go/pkg/kgo/internal/cfg"
	"github.com/twmb/kafka-go/pkg/kgo/internal/compress"
	"github.com/twmb/kafka-go/pkg/kgo/internal/work"
	"github.com/twmb/kafka-go/pkg/kmsg"
	"github.com/twmb/kafka-go/pkg/krec"
)

var (
	// ErrRecordTimeout is returned when records are unable to be produced
	// and they hit the configured record timeout limit.
	ErrRecordTimeout = errors.New("records have timed out before they were able to be produced")

	// ErrNoResp is the error used if Kafka does not reply to a topic or
	// partition in a produce request. This error should never be seen.
	ErrNoResp = errors.New("message was not replied to in a response")

	// ErrAborting is returned for all buffered records while
	// AbortBufferedRecords is being called.
	ErrAborting = errors.New("client is aborting buffered records")
)

// Broker issues requests, calling promises sequentially.
type Broker interface {
	SeqReq(context.Context, kmsg.Request, func(kmsg.Response, error))
}

type Client interface {
	// Request issues a request and waits for the response.
	Request(context.Context, kmsg.Request) (kmsg.Response, error)
	// TriggerUpdateMetadata triggers a metadata update. This is called
	// when a sink or a partition is backing off.
	TriggerUpdateMetadata()
	// QuitContext returns a context that is only done when the client
	// is stopped; this allows a sink in backoff to return early.
	QuitContext() context.Context
	// Compressor returns the client's configured compressor.
	Compressor() *compress.Compressor

	// ProducerID returns the client's producer ID and epoch, or an error
	// if not possible.
	//
	// This can be called concurrently (multiple sinks).
	ProducerID() (int64, int16, error)
	// FailProducerID invalidates a client's producer ID. This is a fatal
	// error; all calls to ProducerID after FailProducerID should return
	// the fail error. However, Kafka 2.5.0 provides a way to potentially
	// reset the producer ID on UknownProducerID.
	//
	// This can be called concurrently (multiple sink, same sink), or
	// multiple times for the same id, epoch, and error.
	FailProducerID(int64, int16, error)

	PromiseFinisher
}

// PromiseFinisher finishes records.
type PromiseFinisher interface {
	FinishRecordPromise(func(*krec.Rec, error), *krec.Rec, error)
}

// Sink contains record buffers whose buffered records are continually flushed
// to a broker.
type Sink struct {
	cfg *cfg.Cfg
	b   Broker
	cl  Client

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

	drainState work.Loop

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

	recBufs      []*RecBuf // contains all partition records for batch building
	recBufsStart int       // incremented every req to avoid large batch starvation

	aborting bool // set to true if aborting in EndTransaction
}

// NewSink returns a new sink.
func NewSink(
	cfg *cfg.Cfg,
	cl Client,
	b Broker,
) *Sink {
	const messageRequestOverhead int32 = 4 + // full length
		2 + // key
		2 + // version
		4 + // correlation ID
		2 // client ID len
	const produceRequestOverhead int32 = 2 + // transactional ID len
		2 + // acks
		4 + // timeout
		4 // topics array length

	s := &Sink{
		cfg: cfg,
		b:   b,
		cl:  cl,

		baseWireLength: messageRequestOverhead + produceRequestOverhead,
	}

	s.inflightSem.Store(make(chan struct{}, 1))

	if cfg.TxnID != nil {
		s.baseWireLength += int32(len(*cfg.TxnID))
	}
	if cfg.ClientID != nil {
		s.baseWireLength += int32(len(*cfg.ClientID))
	}

	return s
}

// createReq returns a produceRequest from currently buffered records and
// whether there are more records to create more requests immediately.
func (s *Sink) createReq() (*produceRequest, *kmsg.AddPartitionsToTxnRequest, bool) {
	req := &produceRequest{
		txnID:   s.cfg.TxnID,
		acks:    s.cfg.Acks,
		timeout: int32(s.cfg.ProduceTimeout.Milliseconds()),
		batches: make(reqBatches, 5),

		compressor: s.cl.Compressor(),
	}

	var (
		wireLength      = s.baseWireLength
		wireLengthLimit = s.cfg.BrokerMaxWriteBytes

		moreToDrain bool

		transactional  = req.txnID != nil
		txnReq         *kmsg.AddPartitionsToTxnRequest
		txnAddedTopics map[string]int // topic => index in txnReq
	)

	s.mu.Lock() // prevent concurrent modification to recBufs
	defer s.mu.Unlock()

	if s.aborting {
		for _, recBuf := range s.recBufs {
			recBuf.FailAllRecords(ErrAborting)
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
		if recBuf.cfg.Linger > 0 {
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

// SetAborting sets the sink to abort all records until ClearAborting is called.
func (s *Sink) SetAborting() {
	s.mu.Lock()
	s.aborting = true
	s.mu.Unlock()
	s.MaybeDrain()
}

// ClearAborting clears the sink of an aborting state.
func (s *Sink) ClearAborting() {
	s.mu.Lock()
	s.aborting = false
	s.mu.Unlock()
}

// MaybeDrain begins a drain loop on the sink if the sink is not yet draining.
func (s *Sink) MaybeDrain() {
	if s.drainState.MaybeBegin() {
		go s.drain()
	}
}

func (s *Sink) backoff() {
	tries := int(atomic.AddUint32(&s.consecutiveFailures, 1))
	after := time.NewTimer(s.cfg.RetryBackoff(tries))
	defer after.Stop()
	select {
	case <-after.C:
	case <-s.cl.QuitContext().Done():
	}
}

// drain drains buffered records and issues produce requests.
//
// This function is harmless if there are no records that need draining.
// We rely on that to not worry about accidental triggers of this function.
func (s *Sink) drain() {
	// Before we begin draining, sleep a tiny bit. This helps when a high
	// volume new sink began draining with no linger; rather than
	// immediately eating just one record, we allow it to buffer a bit
	// before we loop draining.
	time.Sleep(time.Millisecond)

	again := true
	for again {
		if atomic.SwapUint32(&s.doBackoff, 0) == 1 {
			s.cl.TriggerUpdateMetadata()
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
			again = s.drainState.MaybeFinish(again)
			<-sem
			continue
		}

		// At this point, we need our producer ID.
		id, epoch, err := s.cl.ProducerID()
		if err == nil && txnReq != nil {
			err = s.doTxnReq(req, txnReq, id, epoch)
		}

		// If the producer ID errored, or the txn req had an
		// unrecoverable error, then we fail all remaining batches,
		// release our sem, and continue.
		//
		// The ProducerID func above should forever after return
		// the fatal error (unless it can reset, KIP-360).
		if err != nil {
			for _, partitions := range req.batches {
				for partition, batch := range partitions {
					finishBatch(s.cl, batch, partition, 0, err)
				}
			}
			again = s.drainState.MaybeFinish(again)
			<-sem
			continue
		}

		// Again we check if there are any batches to send: our txn req
		// could have non-fatal errored, but removed some batches that
		// could not be added to the txn.
		if len(req.batches) == 0 {
			again = s.drainState.MaybeFinish(again)
			<-sem
			continue
		}

		// Finally, just before we issue our request, we set the final
		// fields of the struct.
		req.producerID = id
		req.producerEpoch = epoch
		req.backoffSeq = s.backoffSeq

		s.b.SeqReq(
			s.cl.QuitContext(),
			req,
			func(resp kmsg.Response, err error) {
				s.handleReqResp(req, resp, err)
				<-sem
			},
		)

		again = s.drainState.MaybeFinish(again)
	}
}

// doTxnReq issues an AddPartitionsToTxnRequest for a produce request for all
// partitions that need to be added to a transaction.
//
// If the entire request fails, all batches of the produce request are finished
// with an the error. Otherwise, all partitions that have errors are removed.
// If any partition has a fatal transactional error, this fails the client's
// producer id and returns the error.
func (s *Sink) doTxnReq(
	req *produceRequest,
	txnReq *kmsg.AddPartitionsToTxnRequest,
	producerID int64,
	producerEpoch int16,
) error {
	txnReq.ProducerID = producerID
	txnReq.ProducerEpoch = producerEpoch
	kresp, err := s.cl.Request(s.cl.QuitContext(), txnReq)
	if err != nil {
		for _, topic := range txnReq.Topics {
			topicBatches := req.batches[topic.Topic]
			for _, partition := range topic.Partitions {
				batch := topicBatches[partition]
				batch.owner.addedToTxn = false
				finishBatch(s.cl, batch, partition, 0, err)
				delete(topicBatches, partition)
			}
			if len(topicBatches) == 0 {
				delete(req.batches, topic.Topic)
			}
		}
		// We return nil here because the client CAN actually continue.
		// However, users likely want to abort the batch on error, and
		// they can do so with their promise.
		return nil
	}

	resp := kresp.(*kmsg.AddPartitionsToTxnResponse)
	var retErr error
	for _, topic := range resp.Topics {
		topicBatches := req.batches[topic.Topic]
		for _, partition := range topic.Partitions {
			if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
				switch err {
				case kerr.InvalidProducerIDMapping,
					kerr.InvalidProducerEpoch:
					retErr = err // all partitions should have the same error

					s.cl.FailProducerID(producerID, producerEpoch, err) // this is unrecoverable (unless KIP-360)
				}

				batch := topicBatches[partition.Partition]
				batch.owner.addedToTxn = false
				finishBatch(s.cl, batch, partition.Partition, 0, err)
				delete(topicBatches, partition.Partition)
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
func (s *Sink) requeueUnattemptedReq(req *produceRequest) {
	var maybeDrain bool
	req.batches.onEachFirstBatchWhileBatchOwnerLocked(func(batch *recBatch) {
		if batch.isTimedOut(s.cfg.RecordTimeout) {
			batch.owner.lockedFailBatch0(ErrRecordTimeout)
		}
		maybeDrain = true
		batch.owner.batchDrainIdx = 0
	})
	if maybeDrain {
		// If the sink has not backed off since issuing this request,
		// we ensure here that it will backoff before the next request.
		if req.backoffSeq == atomic.LoadUint32(&s.backoffSeq) {
			atomic.StoreUint32(&s.doBackoff, 1)
		}
		s.MaybeDrain()
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
func (s *Sink) errorAllRecordsInAllRecordBuffersInRequest(
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
func (s *Sink) errorAllRecordsInAllRecordBuffersInPartitions(
	partitions map[int32]*recBatch,
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
func (s *Sink) firstRespCheck(version int16) {
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
func (s *Sink) handleReqClientErr(req *produceRequest, err error) {
	switch {
	case err == broker.ErrBrokerDead:
		// A dead broker means the broker may have migrated, so we
		// retry to force a metadata reload.
		s.handleRetryBatches(req.batches, true)

	case broker.IsRetriableErr(err):
		s.requeueUnattemptedReq(req)

	default:
		s.errorAllRecordsInAllRecordBuffersInRequest(req, err)
	}
}

func (s *Sink) handleReqResp(req *produceRequest, resp kmsg.Response, err error) {
	// If we had an err, it is from the client itself. This is either a
	// retriable conn failure or a total loss (e.g. auth failure).
	if err != nil {
		s.handleReqClientErr(req, err)
		return
	}
	s.firstRespCheck(req.version)
	atomic.StoreUint32(&s.consecutiveFailures, 0)

	var reqRetry reqBatches // handled at the end
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
				batch.tries < s.cfg.ProduceRetries:
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
				if s.cfg.StopOnDataLoss {
					s.cl.FailProducerID(req.producerID, req.producerEpoch, err)
					finishBatch(s.cl, batch, partition, rPartition.BaseOffset, err)
					continue
				}
				if s.cfg.OnDataLoss != nil {
					s.cfg.OnDataLoss(topic, partition)
				}
				batch.owner.ResetSequenceNums()
				reqRetry.addBatch(topic, partition, batch)

			case err == kerr.DuplicateSequenceNumber: // ignorable, but we should not get
				err = nil
				fallthrough
			default:
				finishBatch(s.cl, batch, partition, rPartition.BaseOffset, err)
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
// This is safe even if the owning RecBuf migrated sinks, since we are
// finishing based off the status of an inflight req from the original sink.
func finishBatch(cl Client, batch *recBatch, partition int32, baseOffset int64, err error) {
	batch.removeFromRecordBuf()
	for i, pnr := range batch.records {
		pnr.Offset = baseOffset + int64(i)
		pnr.Partition = partition
		cl.FinishRecordPromise(pnr.Promise, pnr.Rec, err)
		batch.records[i] = noPNR
	}
	emptyRecordsPool.Put(&batch.records)
}

// handleRetryBatches sets any first-buf-batch to failing and triggers a
// metadata update that will eventually clear the failing state.
//
// If the retry is due to detecting data loss (and only that), then we
// do not need to refresh metadata.
func (s *Sink) handleRetryBatches(retry reqBatches, withBackoff bool) {
	var needsMetaUpdate bool
	retry.onEachFirstBatchWhileBatchOwnerLocked(func(batch *recBatch) {
		batch.owner.batchDrainIdx = 0
		if batch.isTimedOut(s.cfg.RecordTimeout) {
			batch.owner.lockedFailBatch0(ErrRecordTimeout)
		}
		if withBackoff {
			batch.owner.failing = true
			needsMetaUpdate = true
		}
	})

	if needsMetaUpdate {
		s.cl.TriggerUpdateMetadata()
	} else if !withBackoff {
		s.MaybeDrain()
	}
}

// AddRecBuf adds a new record buffer to be drained to a sink and clears the
// buffer's failing state.
func (s *Sink) AddRecBuf(add *RecBuf) {
	s.mu.Lock()
	add.recBufsIdx = len(s.recBufs)
	s.recBufs = append(s.recBufs, add)
	s.mu.Unlock()

	add.ClearFailing()
}

// RemoveRecBuf removes a record buffer from a sink.
func (s *Sink) RemoveRecBuf(rm *RecBuf) {
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

// RecBuf is a buffer of records being produced to a partition and (usually)
// being drained by a sink. This is only not drained if the partition has
// a load error and thus does not a have a sink to be drained into.
type RecBuf struct {
	cfg *cfg.Cfg

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
	// Since only metadata updates can change the sink, the Sink() method
	// on this type reads this without a mutex (the assumption being that
	// only the metadata updating code needs to read this).
	sink *Sink
	// recBufsIdx is our index into our current sink's recBufs field.
	// This exists to aid in removing the buffer from the sink.
	recBufsIdx int

	// sequenceNum is used for the baseSequence in each record batch. This
	// is incremented in bufferRecord and can be reset when processing a
	// response.
	sequenceNum int32

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

	// lingering is a timer that avoids starting MaybeDrain until
	// expired, allowing for more records to be buffered in a single batch.
	//
	// Note that if something else starts a drain, if the first batch of
	// this buffer fits into the request, it will be used.
	//
	// This is on RecBuf rather than Sink to avoid some complicated
	// interactions of triggering the sink to loop or not. Ideally, with
	// the sticky partition hashers, we will only have a few partitions
	// lingering and that this is on a RecBuf should not matter.
	lingering *time.Timer
	noLinger  bool // disables lingering (for flushing)

	// failing is set when we encounter a temporary partition error during
	// producing, such as UnknownTopicOrPartition (signifying the partition
	// moved to a different broker).
	//
	// It is always cleared on metadata update.
	failing bool

	// finisher is only used if a record buffer reaches enough errors to
	// fail all buffered records.
	finisher PromiseFinisher
}

// NewRecBuf returns a record buffer for the given topic and partition.
//
// If linger is non-zero, the buffer lingers before beginning a flush loop,
// and the flush loop does not flush any final buffered batch that is not yet
// full (instead restarting a linger).
//
// All finished records use finisher.
func NewRecBuf(
	cfg *cfg.Cfg,
	topic string,
	partition int32,
	finisher PromiseFinisher,
) *RecBuf {
	return &RecBuf{
		cfg:       cfg,
		topic:     topic,
		partition: partition,
		finisher:  finisher,
	}
}

func messageSet0Length(r *krec.Rec) int32 {
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

func messageSet1Length(r *krec.Rec) int32 {
	return messageSet0Length(r) + 8 // timestamp
}

// PromisedRec ties a record with the callback that will be called once a batch
// is finally written and receives a response.
type PromisedRec struct {
	Promise func(*krec.Rec, error)
	*krec.Rec
}

// BufferRecord usually buffers a record, but does not if abortOnNewBatch is
// true and if this function would create a new batch.
//
// This function is careful not to touch the record sink if the sink is nil,
// which it could be on metadata load err. Note that if the sink is ever not
// nil, then the sink will forever not be nil.
func (recBuf *RecBuf) BufferRecord(pr PromisedRec, abortOnNewBatch bool) bool {
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
		recordNumbers := batch.calculateRecordNumbers(pr.Rec)

		newBatchLength := batch.wireLength + recordNumbers.wireLength

		// If we do not know the broker version, we may be talking
		// to <0.11.0 and be using message sets. Until we know the
		// broker version, we pessimisitically cut our batch off using
		// the largest record length numbers.
		produceVersionKnown := recBuf.sink != nil && atomic.LoadUint32(&recBuf.sink.produceVersionKnown) == 1
		if !produceVersionKnown {
			v1newBatchLength := batch.v1wireLength + messageSet1Length(pr.Rec)
			if v1newBatchLength > newBatchLength { // we only check v1 since it is larger than v0
				newBatchLength = v1newBatchLength
			}
		} else {
			// If we do know our broker version and it is indeed
			// an old one, we use the appropriate length.
			switch recBuf.sink.produceVersion {
			case 0, 1:
				newBatchLength = batch.v0wireLength + messageSet0Length(pr.Rec)
			case 2:
				newBatchLength = batch.v1wireLength + messageSet1Length(pr.Rec)
			}
		}

		if batch.tries == 0 && newBatchLength <= recBuf.cfg.MaxBatchBytes {
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
	recBuf.sequenceNum++

	// Our sink could be nil if our metadata loaded a partition that is
	// erroring.
	if recBuf.sink == nil {
		return true
	}

	if recBuf.cfg.Linger == 0 {
		if firstBatch {
			recBuf.sink.MaybeDrain()
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
			recBuf.sink.MaybeDrain()
		}
	}
	return true
}

func (recBuf *RecBuf) lockedMaybeStartLinger() bool {
	if recBuf.noLinger {
		return false
	}
	recBuf.lingering = time.AfterFunc(recBuf.cfg.Linger, recBuf.sink.MaybeDrain)
	return true
}

// lockedStopLinger stops a linger if there is one.
func (recBuf *RecBuf) lockedStopLinger() {
	if recBuf.lingering != nil {
		recBuf.lingering.Stop()
		recBuf.lingering = nil
	}
}

// SetNoLinger disables lingering on the RecBuf, ensuring that records will be
// continuously flushed (if it is on a sink).
//
// This function is only necessary if a client has lingering enabled.
func (recBuf *RecBuf) SetNoLinger() {
	recBuf.mu.Lock()
	defer recBuf.mu.Unlock()
	recBuf.noLinger = true
	recBuf.lockedStopLinger()
	if recBuf.sink != nil {
		recBuf.sink.MaybeDrain()
	}
}

// ClearNoLinger clears any no linger status, allowing the RecBuf to linger if
// configured to.
func (recBuf *RecBuf) ClearNoLinger() {
	recBuf.mu.Lock()
	defer recBuf.mu.Unlock()
	recBuf.noLinger = false
}

func (recBuf *RecBuf) lockedRemoveBatch0() {
	recBuf.batches[0] = nil
	recBuf.batches = recBuf.batches[1:]
	recBuf.batchDrainIdx--
}

func (recBuf *RecBuf) lockedFailBatch0(err error) {
	batch0 := recBuf.batches[0]
	recBuf.lockedRemoveBatch0()
	for i, pnr := range batch0.records {
		recBuf.finisher.FinishRecordPromise(pnr.Promise, pnr.Rec, err)
		batch0.records[i] = noPNR
	}
	emptyRecordsPool.Put(&batch0.records)
}

// BumpRepeatedLoadError is provided to bump a buffer's number of consecutive
// load errors during metadata updates.
//
// If the metadata loads an error for the topicPartition that this recordBuffer
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
func (recBuf *RecBuf) BumpRepeatedLoadError(err error) {
	recBuf.mu.Lock()
	defer recBuf.mu.Unlock()
	if len(recBuf.batches) == 0 {
		return
	}
	batch0 := recBuf.batches[0]
	batch0.tries++
	if batch0.tries > recBuf.cfg.ProduceRetries {
		recBuf.lockedFailAllRecords(err)
	}
}

// FailAllRecords fails all buffered records with err.
func (recBuf *RecBuf) FailAllRecords(err error) {
	recBuf.mu.Lock()
	defer recBuf.mu.Unlock()
	recBuf.lockedFailAllRecords(err)
}

// lockedFailAllRecords is the same as above, but already in a lock.
func (recBuf *RecBuf) lockedFailAllRecords(err error) {
	recBuf.lockedStopLinger()
	for _, batch := range recBuf.batches {
		for i, pnr := range batch.records {
			recBuf.finisher.FinishRecordPromise(pnr.Promise, pnr.Rec, err)
			batch.records[i] = noPNR
		}
		emptyRecordsPool.Put(&batch.records)
	}
	recBuf.batches = nil
	recBuf.batchDrainIdx = 0
}

// ClearFailing clears a buffer's failing state if it is failing.
//
// This is called when a buffer is added to a sink (to clear a failing state
// from migrating buffers between sinks) or when a metadata update sees the
// sink is still on the same source.
//
// Note the sink cannot be nil here, since nil sinks correspond to load errors,
// and partitions with load errors do not call ClearFailing.
func (recBuf *RecBuf) ClearFailing() {
	recBuf.mu.Lock()
	defer recBuf.mu.Unlock()

	wasFailing := recBuf.failing
	recBuf.failing = false

	if wasFailing && len(recBuf.batches) != recBuf.batchDrainIdx {
		recBuf.sink.MaybeDrain()
	}
}

// ResetSequenceNums resets a buffer's sequence numbers.
//
// Pre 2.5.0, this function should only be called if it is *acceptable* to
// continue on data loss. The client does this automatically given proper
// configuration.
//
// 2.5.0+, it is safe to call this if the producer ID can be reset (KIP-360).
func (recBuf *RecBuf) ResetSequenceNums() {
	recBuf.mu.Lock() // for sequenceNum and batches access
	defer recBuf.mu.Unlock()

	recBuf.sequenceNum = 0
	for _, batch := range recBuf.batches {
		// We store the new sequence atomically because there may be
		// more requests being built and sent concurrently. It is fine
		// that they get the new sequence num, they will still fail
		// with OOOSN, but the error will be dropped since they are not
		// the first batch.
		atomic.StoreInt32(&batch.baseSequence, recBuf.sequenceNum)
		recBuf.sequenceNum += int32(len(batch.records))
	}
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
	PromisedRec
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
	owner *RecBuf // who owns us

	tries int // if this was sent before and is thus now immutable

	v0wireLength int32 // same as wireLength, but for message set v0
	v1wireLength int32 // same as wireLength, but for message set v1
	wireLength   int32 // tracks total size this batch would currently encode as

	attrs          int16
	firstTimestamp int64 // since unix epoch, in millis

	baseSequence int32

	records []promisedNumberedRecord
}

// appendRecord saves a new record to a batch.
func (b *recBatch) appendRecord(pr PromisedRec, nums recordNumbers) {
	b.wireLength += nums.wireLength
	b.v0wireLength += messageSet0Length(pr.Rec)
	b.v1wireLength += messageSet1Length(pr.Rec)
	b.records = append(b.records, promisedNumberedRecord{
		nums,
		pr,
	})
}

// newRecordBatch returns a new record batch for a topic and partition
// containing the given record.
func (recBuf *RecBuf) newRecordBatch(pr PromisedRec) *recBatch {
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
		4 + // baseSequence
		4 // record array length
	b := &recBatch{
		owner:          recBuf,
		firstTimestamp: pr.Timestamp.UnixNano() / 1e6,
		records:        (*(emptyRecordsPool.Get().(*[]promisedNumberedRecord)))[:0],
		baseSequence:   recBuf.sequenceNum,
	}
	pnr := promisedNumberedRecord{
		b.calculateRecordNumbers(pr.Rec),
		pr,
	}
	b.records = append(b.records, pnr)
	b.wireLength = recordBatchOverhead + pnr.wireLength
	b.v0wireLength = messageSet0Length(pr.Rec)
	b.v1wireLength = messageSet1Length(pr.Rec)
	return b
}

// calculateRecordNumbers returns the numbers for a record if it were added to
// the record batch. Nothing accounts for overflows; that should be done prior.
func (b *recBatch) calculateRecordNumbers(r *krec.Rec) recordNumbers {
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

// The above, but inside the owning recordBuffer mutex.
func (batch *recBatch) isFirstBatchInRecordBuf() bool {
	batch.owner.mu.Lock()
	r := batch.lockedIsFirstBatch()
	batch.owner.mu.Unlock()
	return r
}

// removeFromRecordBuf is called in a successful produce response, incrementing
// past the record buffer's now-known-to-be-in-Kafka-batch.
func (batch *recBatch) removeFromRecordBuf() {
	recBuf := batch.owner
	recBuf.mu.Lock()
	recBuf.lockedRemoveBatch0()
	recBuf.mu.Unlock()
}

// isTimedOut, called only on frozen batches, returns whether the first record
// in a batch is past the limit.
func (batch *recBatch) isTimedOut(limit time.Duration) bool {
	if limit == 0 {
		return false
	}
	return time.Since(batch.records[0].Timestamp) > limit
}
