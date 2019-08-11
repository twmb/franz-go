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

type recordSink struct {
	broker *broker // the broker this sink belongs to

	// inflightSem controls the number of concurrent produce requests.  We
	// start with a limit of 1, which covers Kafka v0.11.0.0. On the first
	// response, we check what version was set in the request. If it is at
	// least 4, which 1.0.0 introduced, we upgrade the sem size.
	//
	// Note that both v4 and v5 were introduced with 1.0.0.
	inflightSem      atomic.Value
	handledFirstResp bool

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
	maybeBeginDraining := false
	eachSentBatchLocked(req.batches, func(batch *recordBatch) {
		if !batch.isFirstBatchInRecordBuf() {
			return
		}
		batch.owner.batchDrainIdx = 0
		batch.owner.backoffDeadline = backoffDeadline
		maybeBeginDraining = true
	})

	// TODO here, all sinks
	if maybeBeginDraining {
		sink.maybeBeginDraining()
	}
}

// Called on unrecoverable error (auth failure, lack of response).
func errorAllRecordsInAllRecordBuffersInRequest(
	client *Client,
	req *produceRequest,
	err error,
) {
	for _, partitions := range req.batches {
		errorAllRecordsInAllRecordBuffersInPartitions(
			client,
			partitions,
			err,
		)
	}
}

// Called on unrecoverable error (auth failure, lack of response).
func errorAllRecordsInAllRecordBuffersInPartitions(
	client *Client,
	partitions map[int32]*recordBatch,
	err error,
) {
	for _, batch := range partitions {
		recordBuffer := batch.owner
		recordBuffer.mu.Lock()
		if batch.isFirstBatchInRecordBuf() {
			for _, batch := range recordBuffer.batches {
				for i, record := range batch.records {
					client.promise(record.pr, err)
					batch.records[i] = noPNR
				}
				emptyRecordsPool.Put(&batch.records)
			}
			recordBuffer.batches = nil
		}
		recordBuffer.mu.Unlock()
	}
}

func (sink *recordSink) firstRespCheck(version int16) {
	if !sink.handledFirstResp {
		sink.handledFirstResp = true
		if version >= 4 {
			// NOTE we CANNOT store inflight >= 5. Kafka only
			// supports up to 5 concurrent in flight requests per
			// topic/partition. The first store here races with our
			// original 1 buffer, allowing one more than we store.
			sink.inflightSem.Store(make(chan struct{}, 4))
		}
	}
}

func (sink *recordSink) handleReqClientErr(req *produceRequest, err error) {
	switch {
	case err == ErrBrokerDead:
		// A dead broker means the broker may have migrated, so we
		// retry to force a metadata reload.
		sink.handleRetryBatches(req.batches)

	case isRetriableBrokerErr(err):
		sink.requeueUnattemptedReq(req)

	default:
		errorAllRecordsInAllRecordBuffersInRequest(
			sink.broker.client,
			req,
			err,
		)
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
			if !batch.isFirstBatchInRecordBufLocked() {
				continue
			}

			err := kerr.ErrorForCode(responsePartition.ErrorCode)
			finishBatch := func() { sink.finishBatch(batch, partition, responsePartition.BaseOffset, err) }
			switch {
			case kerr.IsRetriable(err) && err != kerr.CorruptMessage &&
				batch.tries < sink.broker.client.cfg.client.retries:
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
			errorAllRecordsInAllRecordBuffersInPartitions(
				sink.broker.client,
				partitions,
				errNoResp,
			)
		}
	}
	if len(req.batches) > 0 {
		errorAllRecordsInAllRecordBuffersInRequest(
			sink.broker.client,
			req,
			errNoResp,
		)
	}

	if len(reqRetry) > 0 {
		sink.handleRetryBatches(reqRetry)
	}
}

func (sink *recordSink) finishBatch(batch *recordBatch, partition int32, baseOffset int64, err error) {
	batch.removeFromRecordBuf()
	if err == nil {
		batch.owner.lastAckedOffset = baseOffset + int64(len(batch.records))
	}
	for i, record := range batch.records {
		record.pr.r.Offset = baseOffset + int64(i)
		record.pr.r.Partition = partition
		sink.broker.client.promise(record.pr, err)
		batch.records[i] = noPNR
	}
	emptyRecordsPool.Put(&batch.records)
}

var forever = time.Now().Add(100 * 365 * 24 * time.Hour)

func (sink *recordSink) handleRetryBatches(retry reqBatches) {
	// First, get rid of everything that is not the first batch in its
	// records chain, or where the chain is already backing off.
	//
	// This *could* not be the first batch because this handle is called
	// from handleReqClientErr.
	//
	// All batches that remain need to have metadata fetched and be
	// potentially migrated.
	eachSentBatchLocked(retry, func(batch *recordBatch) {
		if !batch.isFirstBatchInRecordBuf() ||
			batch.owner.backoffDeadline == forever ||
			batch.owner.sink != sink {

			// TODO verify correctness
			if batch.isFirstBatchInRecordBuf() &&
				batch.owner.sink != sink {
				batch.owner.sink.maybeBeginDraining()
			}

			skipTopicRetry := retry[batch.owner.topicPartition.topic]
			delete(skipTopicRetry, batch.owner.topicPartition.partition)
			if len(skipTopicRetry) == 0 {
				delete(retry, batch.owner.topicPartition.topic)
			}

			return
		}

		batch.owner.backoffDeadline = forever // tombstone
		batch.owner.batchDrainIdx = 0
	})

	sink.broker.client.triggerUpdateMetadata()
}

func (sink *recordSink) addSource(add *recordBuffer) {
	sink.mu.Lock()
	add.recordBuffersIdx = len(sink.recordBuffers)
	sink.recordBuffers = append(sink.recordBuffers, add)
	sink.mu.Unlock()

	add.maybeBeginDraining()
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

	mu sync.Mutex

	sink             *recordSink // who is draining us
	recordBuffersIdx int         // index into sink's recordBuffers; aids in removal from drain

	sequenceNum     int32 // used for baseSequence in a record batch
	lastAckedOffset int64 // Kafka 1.0.0+ (v5+), used for data loss detection
	batches         []*recordBatch
	batchDrainIdx   int // where the next produce request will drain from

	backoffDeadline time.Time // for retries
}

func (recordBuffer *recordBuffer) bufferRecord(pr promisedRecord) {
	recordBuffer.mu.Lock()

	pr.r.Timestamp = time.Now() // timestamp after locking to ensure sequential

	sink := recordBuffer.sink
	client := sink.broker.client

	newBatch := true
	firstBatch := recordBuffer.batchDrainIdx == len(recordBuffer.batches)

	if !firstBatch {
		batch := recordBuffer.batches[len(recordBuffer.batches)-1]
		rNums := batch.calculateRecordNumbers(pr.r)
		newBatchLength := batch.wireLength + rNums.wireLength
		if batch.tries == 0 &&
			newBatchLength <= client.cfg.producer.maxRecordBatchBytes {
			newBatch = false
			batch.appendRecord(pr, rNums)
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

func (recordBuffer *recordBuffer) removeBatch0Locked() {
	recordBuffer.batches[0] = nil
	recordBuffer.batches = recordBuffer.batches[1:]
	recordBuffer.batchDrainIdx--
}

// TODO if not retriable, should we fail all?
func (recordBuffer *recordBuffer) maybeBumpTriesAndFailBatch0(err error) {
	recordBuffer.mu.Lock()
	defer recordBuffer.mu.Unlock()
	if len(recordBuffer.batches) == 0 {
		return
	}
	batch0 := recordBuffer.batches[0]
	batch0.tries++
	client := recordBuffer.sink.broker.client
	if batch0.tries > client.cfg.client.retries {
		recordBuffer.removeBatch0Locked()
		for i, record := range batch0.records {
			client.promise(record.pr, err)
			batch0.records[i] = noPNR
		}
		emptyRecordsPool.Put(&batch0.records)
	}
}

func (recordBuffer *recordBuffer) maybeBeginDraining() {
	recordBuffer.mu.Lock()
	recordBuffer.backoffDeadline = time.Time{}
	drain := len(recordBuffer.batches) > 0
	recordBuffer.mu.Unlock()

	if drain {
		recordBuffer.sink.maybeBeginDraining()
	}
}

func (recordBuffer *recordBuffer) resetSequenceNums() {
	recordBuffer.mu.Lock()
	defer recordBuffer.mu.Unlock()

	recordBuffer.sequenceNum = 0
	recordBuffer.lastAckedOffset = -1
	for _, batch := range recordBuffer.batches {
		batch.baseSequence = recordBuffer.sequenceNum
		recordBuffer.sequenceNum += int32(len(batch.records))
	}
}

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
}

// promisedNumberedRecord ties a promisedRecord to its calculated numbers.
type promisedNumberedRecord struct {
	n  recordNumbers
	pr promisedRecord
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
		owner:          records,
		firstTimestamp: pr.r.Timestamp.UnixNano() / 1e6,
		records:        (*(emptyRecordsPool.Get().(*[]promisedNumberedRecord)))[:0],
		producerID:     producerID,
		producerEpoch:  producerEpoch,
		baseSequence:   records.sequenceNum,
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
	owner *recordBuffer // who owns us

	tries int // if this was sent before and is thus now immutable

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
	}
}

func eachSentBatchLocked(sent reqBatches, fn func(*recordBatch)) {
	for _, partitions := range sent {
		for _, batch := range partitions {
			batch.owner.mu.Lock()
			fn(batch)
			batch.owner.mu.Unlock()
		}
	}
}

// isFirstBatchInRecordBuf returns if the batch in an recordBatch is the first batch
// in a records.
//
// This function is necessary because we only want to remove leading batches.
// One batch could be received successfully but the response dies in
// disconnect, then a second batch in flight batch could cause a reconnect and
// send successfully. We do not want to operate on that one since we still
// think the first failed.
func (batch *recordBatch) isFirstBatchInRecordBuf() bool {
	return len(batch.owner.batches) > 0 && batch.owner.batches[0] == batch
}

// isFirstBatchInRecordBufLocked is used when not already inside the batch's
// records mu.
func (batch *recordBatch) isFirstBatchInRecordBufLocked() bool {
	batch.owner.mu.Lock()
	r := batch.isFirstBatchInRecordBuf()
	batch.owner.mu.Unlock()
	return r
}

// removeFromRecordBuf is called in a successful produce response, incrementing
// past the record buffer's now-known-to-be-in-Kafka-batch.
func (batch *recordBatch) removeFromRecordBuf() {
	recordBuffer := batch.owner
	recordBuffer.mu.Lock()
	if !batch.isFirstBatchInRecordBuf() {
		panic("removeFromRecordBuf called on non-first batch")
	}
	recordBuffer.removeBatch0Locked()
	recordBuffer.mu.Unlock()
}

var crc32c = crc32.MakeTable(crc32.Castagnoli) // record crc's use Castagnoli table

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

func (*produceRequest) Key() int16           { return 0 }
func (*produceRequest) MaxVersion() int16    { return 7 }
func (*produceRequest) MinVersion() int16    { return 2 }
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

func (pnr promisedNumberedRecord) appendTo(dst []byte, offsetDelta int32) []byte {
	dst = kbin.AppendVarint(dst, pnr.n.lengthField)
	dst = kbin.AppendInt8(dst, 0) // attributes, currently unused
	dst = kbin.AppendVarint(dst, pnr.n.timestampDelta)
	dst = kbin.AppendVarint(dst, offsetDelta)
	dst = kbin.AppendVarintBytes(dst, pnr.pr.r.Key)
	dst = kbin.AppendVarintBytes(dst, pnr.pr.r.Value)
	dst = kbin.AppendVarint(dst, int32(len(pnr.pr.r.Headers)))
	for _, h := range pnr.pr.r.Headers {
		dst = kbin.AppendVarintString(dst, h.Key)
		dst = kbin.AppendVarintBytes(dst, h.Value)
	}
	return dst
}
