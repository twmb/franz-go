package kgo

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/kgo/kerr"
	"github.com/twmb/kgo/kmsg"
)

// TODO KIP-359

// TODO
// init producer ID only on send
// change toppars "loaded" to a state (uninit, ok, loading)

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

	mu sync.Mutex // guards all fields below

	draining     bool
	backoffTimer *time.Timer // runs if all partition records are in a backoff state
	backoffSeq   uint64      // guards against timers starting drains they should not

	allPartRecs []*records // contains all partition's records; used for batch building
	// allPartRecsStart is where we will begin in allPartRecs for building a
	// batch. This increments by one every produce request, avoiding
	// starvation for large record batches that cannot fit into the request
	// that is currently being built.
	allPartRecsStart int
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
		baseWireLength: messageRequestOverhead + produceRequestOverhead, // TODO + txn id len
	}
	sink.inflightSem.Store(make(chan struct{}, 1))
	if broker.client.cfg.client.id != nil {
		sink.baseWireLength += int32(len(*broker.client.cfg.client.id))
	}

	return sink
}

// createRequest returns a produceRequest from currently buffered records
// and whether there are more records to create more requests immediately.
func (sink *recordSink) createRequest() (*produceRequest, bool) {
	request := &produceRequest{
		// TODO transactional ID
		acks:    sink.broker.client.cfg.producer.acks.val,
		timeout: sink.broker.client.cfg.client.requestTimeout,
		batches: make(reqBatches, 5),

		compression: sink.broker.client.cfg.producer.compression,
	}

	wireLength := sink.baseWireLength
	wireLengthLimit := sink.broker.client.cfg.client.maxBrokerWriteBytes

	var (
		soonestDeadline time.Time
		numDeadlined    int
		moreToDrain     bool
		now             = time.Now()
	)

	// Over every record buffer, check to see if the first batch is not
	// backing off and that it can can fit in our request.
	allPartRecsIdx := sink.allPartRecsStart
	for i := 0; i < len(sink.allPartRecs); i++ {
		recs := sink.allPartRecs[allPartRecsIdx]
		allPartRecsIdx = (allPartRecsIdx + 1) % len(sink.allPartRecs)

		recs.mu.Lock()
		if dl := recs.backoffDeadline; now.Before(dl) {
			recs.mu.Unlock()
			if soonestDeadline.IsZero() || dl.Before(soonestDeadline) {
				soonestDeadline = dl
			}
			numDeadlined++
			continue
		}

		if len(recs.batches) == recs.batchDrainIdx {
			recs.mu.Unlock()
			continue
		}

		batch := recs.batches[recs.batchDrainIdx]
		batchWireLength := 4 + batch.wireLength // partition, batch len
		topicBatches := request.batches[recs.topicPartition.topic]
		if topicBatches == nil {
			batchWireLength += 2 + int32(len(recs.topicPartition.topic)) + 4 // string len, topic, array len
		}
		if wireLength+batchWireLength > wireLengthLimit {
			recs.mu.Unlock()
			moreToDrain = true
			continue
		}

		batch.tries++
		recs.batchDrainIdx++

		if !moreToDrain {
			moreToDrain = len(recs.batches) > recs.batchDrainIdx
		}
		recs.mu.Unlock()

		wireLength += batchWireLength
		request.batches.addToTopicBatches(
			recs.topicPartition.topic,
			recs.topicPartition.partition,
			topicBatches,
			sentBatch{
				owner:       recs,
				recordBatch: batch,
			},
		)
	}

	// If we have no more to drain, yet some bufs are backing off, we
	// begin the drain backoff for when the soonest is ready.
	if !moreToDrain && numDeadlined != 0 && soonestDeadline != forever {
		sink.beginDrainBackoff(soonestDeadline.Sub(now))
	}

	sink.allPartRecsStart = (sink.allPartRecsStart + 1) % len(sink.allPartRecs)
	sink.draining = moreToDrain
	return request, moreToDrain
}

func (sink *recordSink) maybeBeginDraining() {
	sink.mu.Lock()

	// TODO draining can probably be a combo of an atomic with multiple
	// states and a mu. This would unblock maybeBegin.
	if sink.draining {
		sink.mu.Unlock()
		return
	}
	sink.draining = true

	if sink.backoffTimer != nil {
		sink.backoffTimer.Stop()
		sink.backoffTimer = nil
		sink.backoffSeq++
	}
	sink.mu.Unlock()

	go sink.drain()
}

// beginDrainBackoff is called if all part recs are detected to be in a backoff
// state and no produce request can be built.
func (sink *recordSink) beginDrainBackoff(after time.Duration) {
	seq := sink.backoffSeq

	sink.backoffTimer = time.AfterFunc(after, func() {
		sink.mu.Lock()
		defer sink.mu.Unlock()

		if seq != sink.backoffSeq {
			return
		}

		sink.backoffTimer = nil
		sink.draining = true

		go sink.drain()
	})
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
		sem := sink.inflightSem.Load().(chan struct{})
		sem <- struct{}{}

		// We must hold the mu while creating the request all the way
		// thru issuing it. If we release before issuing, the create
		// could set sink.draining to false, a new record could start a
		// new drain, and a new request could sneak in and be issued
		// before this first one was.
		sink.mu.Lock()
		var req *produceRequest
		req, again = sink.createRequest()

		if len(req.batches) == 0 { // everything entered backoff
			sink.mu.Unlock()
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
		sink.mu.Unlock()
	}
}

// requeueUnattemptedReq requeues all batches in req to the recordSink.
// This is done if a retriable network error occured.
func (sink *recordSink) requeueUnattemptedReq(req *produceRequest) {
	// We use a first-level backoff since these network errors are
	// presumed to be exceedingly temporary.
	backoffDeadline := time.Now().Add(sink.broker.client.cfg.client.retryBackoff(1))
	maybeBeginDraining := false
	eachSentBatchLocked(req.batches, func(batch sentBatch) {
		if !batch.isFirstBatchInRecordBuf() {
			return
		}
		batch.owner.batchDrainIdx = 0
		batch.owner.backoffDeadline = backoffDeadline
		maybeBeginDraining = true
	})

	if maybeBeginDraining {
		sink.maybeBeginDraining()
	}
}

func (sink *recordSink) errorAllReqBuffers(req *produceRequest, err error) {
	for _, partitions := range req.batches {
		sink.errorAllReqPartitionBuffers(partitions, err)
	}
}

// errorAllReqPartitionBuffers errors all records in all partitions in a topic.
// This is called for unrecoverable errors, such as auth failures.
func (sink *recordSink) errorAllReqPartitionBuffers(partitions map[int32]sentBatch, err error) {
	for _, batch := range partitions {
		recs := batch.owner
		recs.mu.Lock()
		if batch.isFirstBatchInRecordBuf() {
			for _, batch := range recs.batches {
				for i, record := range batch.records {
					sink.broker.client.promise(record.pr, err)
					batch.records[i] = noPNR
				}
				emptyRecordsPool.Put(&batch.records)
			}
			recs.batches = nil
		}
		recs.mu.Unlock()
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
		sink.errorAllReqBuffers(req, err)
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
			sink.errorAllReqPartitionBuffers(partitions, errNoResp)
		}
	}
	if len(req.batches) > 0 {
		sink.errorAllReqBuffers(req, errNoResp)
	}

	if len(reqRetry) > 0 {
		sink.handleRetryBatches(reqRetry)
	}
}

func (sink *recordSink) finishBatch(batch sentBatch, partition int32, baseOffset int64, err error) {
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
	eachSentBatchLocked(retry, func(batch sentBatch) {
		if !batch.isFirstBatchInRecordBuf() ||
			batch.owner.backoffDeadline == forever {

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

	sink.broker.client.addMetadataWaiters(retry)

	// TODO: we can switch this to one metadata fetch for all retry topics
	for topic, migrateParts := range retry {
		go sink.migrateTopic(topic, migrateParts)
	}
}

func (sink *recordSink) migrateTopic(topic string, migrateParts map[int32]sentBatch) {
	client := sink.broker.client

	tries := 1
start:
	loadParts := newTopicParts()
	client.fetchTopicMetadataIntoParts(map[string]*topicPartitions{
		topic: loadParts,
	}, false)
	if loadParts.loadErr != nil {
		time.Sleep(client.cfg.client.retryBackoff(tries)) // TODO max retries
		tries++
		goto start
	}

	var deletedParts []*topicPartition
	defer func() {
		for _, tp := range deletedParts {
			tp.records.sink.removeToppar(&tp.records)

			tp.records.mu.Lock()
			defer tp.records.mu.Unlock()

			for _, batch := range tp.records.batches {
				for i, record := range batch.records {
					sink.broker.client.promise(record.pr, ErrPartitionDeleted)
					batch.records[i] = noPNR
				}
				emptyRecordsPool.Put(&batch.records)
			}
		}
	}()

	// If any part we want to migrate no longer exists, the partition
	// has been deleted.
	for migratePart, migrateToppar := range migrateParts {
		if _, exists := loadParts.all[migratePart]; !exists {
			deletedParts = append(deletedParts, migrateToppar.owner.topicPartition)
		}
	}

	existingParts, err := client.partitionsForTopicProduce(topic)
	if err != nil {
		panic("migrating existing topic, existing parts have err " + err.Error())
	}

	// We block all records from being added while we migrate partitions.
	existingParts.mu.Lock()
	defer existingParts.mu.Unlock()

	existingParts.partitions = loadParts.partitions

	// For all existing parts, if they no longer exist, we will call all
	// buffered records with a partition deleted error.
	//
	// If the toppar does exist, but the drain is different (leader
	// broker changed), we remove the toppar from the old drain and add
	// it to the new.
	for id, tp := range existingParts.all {
		if newTP, exists := loadParts.all[id]; !exists {
			deletedParts = append(deletedParts, tp)
		} else if newTP.records.sink != tp.records.sink {
			tp.records.sink.removeToppar(&tp.records)
			tp.records.sink = newTP.records.sink
			tp.records.sink.addToppar(&tp.records)
		} else {
			tp.records.clearBackoff()
			tp.records.sink.maybeBeginDraining()
		}
		delete(loadParts.all, id)
	}

	// For any new parts that we did not know about prior, we add them.
	for id, tp := range loadParts.all {
		existingParts.all[id] = tp
		tp.records.sink.addToppar(&tp.records)
	}

	// We store the new writable parts into the existing parts.
	// Over all of them, we set the new toppar to the old (but updated).
	existingParts.writable = loadParts.writable
	for id := range existingParts.writable {
		existingParts.writable[id] = existingParts.all[id]
		// TODO err if went from writable to non-writable and not
		// requires hash consistency.
	}
}

func (sink *recordSink) addToppar(add *records) {
	sink.mu.Lock()
	add.allPartRecsIdx = len(sink.allPartRecs)
	sink.allPartRecs = append(sink.allPartRecs, add)
	sink.mu.Unlock()

	add.clearBackoff()

	sink.maybeBeginDraining()
}

// removeToppar removes the tracking of a toppar from the recordSink.
func (sink *recordSink) removeToppar(rm *records) {
	sink.mu.Lock()
	defer sink.mu.Unlock()

	if rm.allPartRecsIdx != len(sink.allPartRecs)-1 {
		sink.allPartRecs[rm.allPartRecsIdx], sink.allPartRecs[len(sink.allPartRecs)-1] =
			sink.allPartRecs[len(sink.allPartRecs)-1], nil

		sink.allPartRecs[rm.allPartRecsIdx].allPartRecsIdx = rm.allPartRecsIdx
	}

	sink.allPartRecs = sink.allPartRecs[:len(sink.allPartRecs)-1]
	if sink.allPartRecsStart == len(sink.allPartRecs) {
		sink.allPartRecsStart = 0
	}
}

// records buffers records to be sent to a topic/partition.
type records struct {
	topicPartition *topicPartition

	mu sync.Mutex

	sink           *recordSink // who is draining us
	allPartRecsIdx int         // index into sink's allPartRecs; aids in removal from drain

	sequenceNum     int32 // used for baseSequence in a record batch
	lastAckedOffset int64 // Kafka 1.0.0+ (v5+), used for data loss detection
	batches         []*recordBatch
	batchDrainIdx   int // where the next produce request will drain from

	backoffDeadline time.Time // for retries
}

func (recs *records) bufferRecord(pr promisedRecord) {
	recs.mu.Lock()

	pr.r.Timestamp = time.Now() // timestamp after locking to ensure sequential

	sink := recs.sink

	newBatch := true
	firstBatch := recs.batchDrainIdx == len(recs.batches)

	if !firstBatch {
		batch := recs.batches[len(recs.batches)-1]
		rNums := batch.calculateRecordNumbers(pr.r)
		newBatchLength := batch.wireLength + rNums.wireLength
		if batch.tries == 0 &&
			newBatchLength <= sink.broker.client.cfg.producer.maxRecordBatchBytes {
			newBatch = false
			batch.appendRecord(pr, rNums)
		}
	}

	if newBatch {
		recs.batches = append(recs.batches, sink.newRecordBatch(recs.sequenceNum, pr))
	}
	recs.sequenceNum++

	recs.mu.Unlock()

	if firstBatch {
		sink.maybeBeginDraining()
	}
}

func (recs *records) clearBackoff() {
	recs.mu.Lock()
	recs.backoffDeadline = time.Time{}
	recs.mu.Unlock()
}

func (recs *records) resetSequenceNums() {
	recs.mu.Lock()
	defer recs.mu.Unlock()

	recs.sequenceNum = 0
	recs.lastAckedOffset = -1
	for _, batch := range recs.batches {
		batch.baseSequence = recs.sequenceNum
		recs.sequenceNum += int32(len(batch.records))
	}
}

// sentBatch wraps a recordBatch for a produce request with the record buffer
// the batch came from.
type sentBatch struct {
	owner *records
	*recordBatch
}

// isFirstBatchInRecordBuf returns if the batch in an sentBatch is the first batch
// in a records.
//
// This function is necessary because we only want to remove leading batches.
// One batch could be received successfully but the response dies in
// disconnect, then a second batch in flight batch could cause a reconnect and
// send successfully. We do not want to operate on that one since we still
// think the first failed.
func (batch sentBatch) isFirstBatchInRecordBuf() bool {
	return len(batch.owner.batches) > 0 && batch.owner.batches[0] == batch.recordBatch
}

// isFirstBatchInRecordBufLocked is used when not already inside the batch's
// records mu.
func (batch sentBatch) isFirstBatchInRecordBufLocked() bool {
	batch.owner.mu.Lock()
	r := batch.isFirstBatchInRecordBuf()
	batch.owner.mu.Unlock()
	return r
}

// removeFromRecordBuf is called in a successful produce response, incrementing
// past the record buffer's now-known-to-be-in-Kafka-batch.
func (batch sentBatch) removeFromRecordBuf() {
	recs := batch.owner
	recs.mu.Lock()
	if !batch.isFirstBatchInRecordBuf() {
		panic("removeFromRecordBuf called on non-first batch")
	}
	recs.batches[0] = nil
	recs.batches = recs.batches[1:]
	recs.batchDrainIdx--
	recs.mu.Unlock()
}

func eachSentBatchLocked(sent reqBatches, fn func(sentBatch)) {
	for _, partitions := range sent {
		for _, batch := range partitions {
			batch.owner.mu.Lock()
			fn(batch)
			batch.owner.mu.Unlock()
		}
	}
}
