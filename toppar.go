package kgo

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/kgo/kerr"
	"github.com/twmb/kgo/kmsg"
)

type brokerToppars struct {
	// br is the broker this brokerToppars belongs to.
	br *broker

	// inflightSem controls the number of concurrent produce requests.  We
	// start with a limit of 1, which covers Kafka v0.11.0.0. On the first
	// response, we check what version was set in the request. If it is at
	// least 4, which 1.0.0 introduced, we upgrade the sem size.
	inflightSem    atomic.Value
	inflightV1Plus bool

	// baseWireLength is the minimum wire length of a produce request
	// for a client.
	baseWireLength int32

	// nrecs is an atomic covering the total number of records buffered
	// in a brokerToppar.
	nrecs int64

	// The following backoff fields are used to ensure that the drain loop
	// begins appropriately after noticing all batches are in a backoff
	// state. The usage is complicated but documented.
	backoffRecsTimer *time.Timer
	backoffRecsMu    sync.Mutex
	backoffRecsSeq   uint64 // incremented under backoffRecsMu
	backoffRecs      int64  // zeroed under backoffRecsMu

	// mu guards concurrent concurrent access to toppars and allToppars.
	mu sync.Mutex
	// toppars contains all toppars for a broker, grouped by
	// topic and partition.
	toppars map[string]map[int32]*toppar // topic => partition => toppar
	// allToppars, used for building batches, contains all toppars.
	allToppars []*toppar

	// allTopparsStart is where we will begin in allToppars for building a
	// batch. This increments by one every produce request, avoiding
	// starvation for large record batches that cannot fit into
	// currently-built requests.
	allTopparsStart int
}

// toppar captures buffered records for a TOPic and PARtition.
type toppar struct {
	// topic is the topic this toppar belongs to.
	topic string
	// part is the partition this toppar belongs to.
	part int32

	// allTopparsIdx signifies the index into brokerToppars' allToppars
	// field that this toppar is.
	//
	// This field is updated whenever a toppar moves around in allToppars.
	allTopparsIdx int

	// idWireLength covers the topic string and the part array length.
	// It does not cover part numbers nor record batches.
	idWireLength int32

	// mu guards all fields below
	mu sync.Mutex

	// batches contains batches being built for produce requests.
	// This could be empty if a request just drained every record
	// buffered for the toppar.
	batches []*recordBatch

	// backoffDeadline is used for retries: if a toppar's records fail and
	// are requeued to the front of batches, the batch will not be retried
	// until after the deadline.
	backoffDeadline time.Time
}

func newBrokerToppars(br *broker) *brokerToppars {
	const messageRequestOverhead int32 = 4 + // full length
		2 + // key
		2 + // version
		4 + // correlation ID
		2 // client ID len
	const produceRequestOverhead int32 = 2 + // transactional ID len
		2 + // acks
		4 + // timeout
		4 // topics array length

	bt := &brokerToppars{
		br:             br,
		baseWireLength: messageRequestOverhead + produceRequestOverhead,
		toppars:        make(map[string]map[int32]*toppar, 1),
	}
	bt.inflightSem.Store(make(chan struct{}, 1))
	if br.cl.cfg.client.id != nil {
		bt.baseWireLength += int32(len(*br.cl.cfg.client.id))
	}

	return bt
}

// createRequest returns a produceRequest from currently buffered records
// and whether there are more records to create requests from.
func (bt *brokerToppars) createRequest() (*produceRequest, bool) {
	request := &produceRequest{
		// TODO transactional ID
		acks:             bt.br.cl.cfg.producer.acks.val,
		timeout:          int32(time.Second / 1e6), // TODO
		topicsPartitions: make(map[string]map[int32]*recordBatch, 1),

		compression: bt.br.cl.cfg.producer.compression,
	}

	wireLength := bt.baseWireLength
	wireLengthLimit := bt.br.cl.cfg.producer.maxBrokerWriteBytes

	// To minimize mutex usage, we count records used and track
	// toppars used in two local variables.
	var reqRecs int
	var reqTPs []*toppar

	// We load the current allToppars here. If any new appends happen, they
	// will not invalidate allToppars since the order has not changed.
	// We will just miss those new toppars in this request.
	bt.mu.Lock()
	allToppars := bt.allToppars
	bt.mu.Unlock()

	idx := bt.allTopparsStart
	now := time.Now()
	for i := 0; i < len(allToppars); i++ {
		tp := allToppars[idx]
		if idx = idx + 1; idx == len(allToppars) {
			idx = 0
		}

		tp.mu.Lock()
		if now.Before(tp.backoffDeadline) {
			tp.mu.Unlock()
			continue
		}

		// This toppar could have zero batches if a prior produce
		// request drained the records.
		if len(tp.batches) == 0 {
			tp.mu.Unlock()
			reqTPs = append(reqTPs, tp)
			continue
		}

		batch := tp.batches[0]
		batchWireLength := 4 + batch.wireLength // part ID + batch

		// If this topic does not exist yet in the request,
		// we need to add the topic's overhead.
		topicPartitions, topicPartitionExists := request.topicsPartitions[tp.topic]
		if !topicPartitionExists {
			batchWireLength += tp.idWireLength
		}

		// If this new topic/part/batch would exceed the max wire
		// length, we skip it. It will fit on a future built request.
		if wireLength+batchWireLength > wireLengthLimit {
			tp.mu.Unlock()
			continue
		}

		// Before unlocking the toppar, track that we are trying
		// the batch to avoid new records being added to it.
		batch.tried = true
		tp.mu.Unlock()

		// Now that we are for sure using the batch, create the
		// topic and partition in the request for it if necessary.
		if !topicPartitionExists {
			topicPartitions = make(map[int32]*recordBatch, 1)
			request.topicsPartitions[tp.topic] = topicPartitions
		}

		wireLength += batchWireLength
		topicPartitions[tp.part] = batch
		reqRecs += len(batch.records)
		reqTPs = append(reqTPs, tp)
	}

	// Over all toppars we used, increment past the toppar's first batch.
	//
	// If the toppar had no batches, we remove it. We only remove toppars
	// from being tracked after we have seen, in a new produce request,
	// that it still has no batches.
	//
	// This allows us to avoid repeatedly deleting toppars that are then
	// recreated immediately.
	bt.mu.Lock()
	for _, tp := range reqTPs {
		tp.mu.Lock()
		if len(tp.batches) == 0 {
			// If we are removing the toppar that we began on,
			// decrement allTopparsStart so we do not skip past
			// whatever we swap into its place.
			if tp.allTopparsIdx == bt.allTopparsStart {
				bt.allTopparsStart--
			}
			bt.removeToppar(tp)
		} else {
			tp.batches = tp.batches[1:]
		}
		tp.mu.Unlock()
	}
	lenToppars := len(bt.toppars)
	bt.mu.Unlock()

	// Finally, for the next request, start on the next toppar.
	bt.allTopparsStart++
	if bt.allTopparsStart == lenToppars {
		bt.allTopparsStart = 0
	}

	return request, atomic.AddInt64(&bt.nrecs, int64(-reqRecs)) > 0
}

// removeToppar removes the tracking of a toppar from the brokerToppars.
func (bt *brokerToppars) removeToppar(rm *toppar) {
	topicParts := bt.toppars[rm.topic]
	delete(topicParts, rm.part)
	if len(topicParts) == 0 {
		delete(bt.toppars, rm.topic)
	}

	if rm.allTopparsIdx != len(bt.allToppars)-1 {
		bt.allToppars[rm.allTopparsIdx], bt.allToppars[len(bt.allToppars)-1] =
			bt.allToppars[len(bt.allToppars)-1], nil

		bt.allToppars[rm.allTopparsIdx].allTopparsIdx = rm.allTopparsIdx
	}

	bt.allToppars = bt.allToppars[:len(bt.allToppars)-1]
}

// newToppar creates and returns a new toppar under the given topic and part.
// bt must be locked.
// The new toppar is added to all toppars, but is not added to the toppars
// map (the calling loaded the map to know the toppar does not exist, so
// it can add the new toppar easily).
func (bt *brokerToppars) newToppar(topic string, part int32) *toppar {
	tp := &toppar{
		topic:         topic,
		part:          part,
		allTopparsIdx: len(bt.allToppars),
		idWireLength:  2 + int32(len(topic)) + 4, // topic len, topic, parts array len
	}
	bt.allToppars = append(bt.allToppars, tp)
	return tp
}

// loadAndLockToppar returns the toppar under topic, part, creating it as
// necessary, and returning it locked.
func (bt *brokerToppars) loadAndLockToppar(topic string, part int32) *toppar {
	bt.mu.Lock()

	topicParts, partsExist := bt.toppars[topic]
	if !partsExist {
		topicParts = make(map[int32]*toppar, 1)
		bt.toppars[topic] = topicParts
	}

	tp, topparExists := topicParts[part]
	if !topparExists {
		tp = bt.newToppar(topic, part)
		topicParts[part] = tp
	}

	tp.mu.Lock()
	bt.mu.Unlock()

	return tp
}

// bufferRecord buffers a promised record under the given topic / partition.
//
// By the time we are adding a record, we know it has a valid length, for
// we have already looked up the part from Kafka with the topic name.
func (bt *brokerToppars) bufferRecord(topic string, part int32, pr promisedRecord) {
	tp := bt.loadAndLockToppar(topic, part)

	newBatch := true
	hasBatches := len(tp.batches) > 0
	if hasBatches {
		batch := tp.batches[len(tp.batches)-1]
		prNums := batch.calculateRecordNumbers(pr.r)
		newBatchLength := batch.wireLength + prNums.wireLength
		if !batch.tried &&
			newBatchLength <= bt.br.cl.cfg.producer.maxRecordBatchBytes {
			newBatch = false
			batch.appendRecord(pr, prNums)
		}
	}

	if newBatch {
		tp.batches = append(tp.batches, newRecordBatch(pr))
	}

	// APPEND:
	// first batch, first append
	//   lock, check if draining, no:
	//     drain
	//
	// BACKOFF:
	// not draining:
	//

	// Why nrecs is important:
	// - If we keep a two tier locking system, then a new toppar could
	//   be created as we drain batches. If this is the case...
	//   ... ok we can continue draining if len(toppars different)

	// So in drain,
	// capture toppars
	// if everything in our own set is backoff,
	// and at the end, if no new toppars exist
	// backoff
	// inside mutex to ensure a concurrent append does not create new
	// toppar that we would miss
	//

	// NO: if we enqueue to a backoff toppar we will STILL NEVER
	// RESTART DRAINING

	// We only begin draining if this is truly the first record.
	// We could have batches in the backoff state where their record count
	// is tracked in backoffRecs.
	if atomic.AddInt64(&bt.nrecs, 1) == 1 && !hasBatches {
		bt.beginDraining()
	}

	// hasBatches ensures that we do not start draining batches if the
	// toppar we added the record to is in a backoff state.
	//
	// If we did not hold the mu...
	//
	// A drain could be running, consume a newly added record, and then
	// return setting the nrecs to zero. If this mu were not held, nrecs
	// would then increase again, but hasBatches would be true so the drain
	// loop would not start. Then the loop would never start because nrecs
	// would never increment to 1 again.
	//
	// This lock ensures that we do not both have the record added to a
	// concurrently built batch while still thinking this record is bufd.
	tp.mu.Unlock()
}

// beginDrainBackoff is called if all toppars are detected to be in a backoff
// state and no produce request can be built.
//
// This stops the world to ensure that everything is still in a backoff state,
// and if so, begins a timer that will eventually restart the drain loop
// after the soonest backoff deadline.
func (bt *brokerToppars) beginDrainBackoff() {
	bt.mu.Lock() // block any new record appends
	defer bt.mu.Unlock()

	now := time.Now()

	var soonestDeadline time.Time
	for _, tp := range bt.allToppars {
		tp.mu.Lock()
		if now.Before(tp.backoffDeadline) && len(tp.batches) > 0 {
			tp.mu.Unlock()
			go bt.drain() // something recovered, begin draining again
			return
		}

		if soonestDeadline.IsZero() || tp.backoffDeadline.Before(soonestDeadline) {
			soonestDeadline = tp.backoffDeadline
		}
		tp.mu.Unlock()
	}

	// If we make it here, we have blocked any new toppar from being
	// created and all existing toppars are in a backoff state.
	// No drain is running.
	// We have non-zero nrecs.
	// Save those nrecs into backoff recs.
	bt.backoffRecs = atomic.SwapInt64(&bt.nrecs, 0)
	seq := bt.backoffRecsSeq

	// After this timer starts, new records could come pouring in and may
	// race restarting the drain.
	//
	// If the timer fires right as beginDrain is called and the timer's
	// goro is blocked, beginDrain will incremnt the backoff seq to ensure
	// this blocked goro will do nothing.
	//
	// If this timer fires before a record is added, this will see the
	// backoff seq is the same, add the backoff recs, and begin draining.
	bt.backoffRecsTimer = time.AfterFunc(soonestDeadline.Sub(now), func() {
		bt.backoffRecsMu.Lock()
		defer bt.backoffRecsMu.Unlock()

		if seq != bt.backoffRecsSeq {
			return
		}

		// If we are this far, we will be the one to start draining.
		bt.backoffRecsTimer = nil
		atomic.AddInt64(&bt.nrecs, bt.backoffRecs)
		bt.backoffRecs = 0

		go bt.drain()
	})
}

func (bt *brokerToppars) beginDraining() {
	bt.backoffRecsMu.Lock()
	defer bt.backoffRecsMu.Unlock()

	if bt.backoffRecsTimer != nil { // some backoff is afoot
		bt.backoffRecsTimer.Stop()
		bt.backoffRecsTimer = nil
		bt.backoffRecsSeq++
		atomic.AddInt64(&bt.nrecs, bt.backoffRecs)
		bt.backoffRecs = 0
	}

	go bt.drain()
}

func (bt *brokerToppars) drain() {
	again := true
	for again {
		var req *produceRequest
		sem := bt.inflightSem.Load().(chan struct{})
		sem <- struct{}{}

		req, again = bt.createRequest()

		// The only way this request has no topics is if all topics
		// were in a backoff state.
		if len(req.topicsPartitions) == 0 {
			<-sem // wont be using that
			bt.beginDrainBackoff()
			return
		}

		bt.br.doSequencedAsyncPromise(
			req,
			func(resp kmsg.Response, err error) {
				bt.handleReqResp(req, resp, err)
				<-sem
			},
		)
	}
}

func (bt *brokerToppars) topparForEachBatch(recordBatches map[string]map[int32]*recordBatch, fn func(*toppar, *recordBatch)) {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	for topic, partitions := range recordBatches {
		topicParts, partsExists := bt.toppars[topic]
		if !partsExists {
			topicParts = make(map[int32]*toppar, len(partitions))
			bt.toppars[topic] = topicParts
		}
		for partition, batch := range partitions {
			tp, topparExists := topicParts[partition]
			if !topparExists {
				tp = bt.newToppar(topic, partition)
				topicParts[partition] = tp
			}
			fn(tp, batch)
		}
	}
}

func (tp *toppar) requeueBatchInOrder(b *batch) {
	next := make([]*recordBatch, 0, len(tp.batches)+1)
	for i, on := range tp.batches {
		if on.created.After(b.created) {
			next = append(next, b)
			next = append(next, tp.batches[i+1:]...)
			break
		}
		next = append(next, on)
	}
	tp.batches = on
}

// requeueEntireReq requeues all batches in req to the brokerToppars.
// This is done if a retriable network error occured.
func (bt *brokerToppars) requeueEntireReq(req *produceRequest) {
	bt.topparForEachBatch(req.topicPartitions, func(tp *toppar, batch *recordBatch) {
		tp.backoffDeadline = backoffDeadline
		tp.requeueBatchInOrder(batch)
	})
}

// errorReq calls all promises in the request with err.
func errorReq(req *produceRequest, err error) {
	for topic, partitions := range req.topicsPartitions {
		errorTopicPartitions(topic, partitions, err)
	}
}

// errorTopicPartitions calls all promises in a topic's partitions' with err.
func errorTopicPartitions(topic string, partitions map[int32]*recordBatch, err error) {
	for _, batch := range partitions {
		for _, record := range batch.records {
			record.pr.promise(topic, record.pr.r, err)
		}
	}
}

func (bt *brokerToppars) handleReqResp(req *produceRequest, resp kmsg.Response, err error) {
	if !bt.inflightV1Plus && req.version >= 4 {
		bt.inflightV1Plus = true
		// NOTE we CANNOT store inflight >= 5. Kafka only supports up
		// to 5 concurrent in flight requests per topic/partition. The
		// first store here races with our original 1 buffer, allowing
		// one more than we store.
		bt.inflightSem.Store(make(chan struct{}, 2))
	}

	// If we had an err, it is from the client itself. This is either a
	// retriable conn failure or a total loss (e.g. auth failure).
	if err != nil {
		if connErr, ok := err.(*connErr); ok && connErr.retriable {
			bt.requeueEntireReq(req)
			return
		}
		errorReq(req, err)
		return
	}

	var retry map[string]map[int32]*recordBatch
	defer bt.handleRetryBatches(retry)
	pr := resp.(*kmsg.ProduceResponse)

	// This block calls the promise for all records in the produce request.
	//
	// If a topic/partition has a retriable error, we retry rather than
	// calling the promise.
	for _, responseTopic := range pr.Responses {
		topic := responseTopic.Topic
		partitions, ok := req.topicsPartitions[topic]
		if !ok {
			continue
		}
		delete(req.topicsPartitions, topic)

		for _, responsePartition := range responseTopic.PartitionResponses {
			partition := responsePartition.Partition
			batch, ok := partitions[partition]
			if !ok {
				continue
			}
			delete(partitions, partition)

			err := kerr.ErrorForCode(responsePartition.ErrorCode)
			switch err {
			case kerr.UnknownTopicOrPartition, // we know more than the broker
				kerr.NotLeaderForPartition, // stale metadata
				kerr.NotEnoughReplicas,
				kerr.RequestTimedOut:

				if retry == nil {
					retry = make(map[string]map[int32]*recordBatch, 5)
				}
				retryParts, exists := retry[topic]
				if !exists {
					retryParts = make(map[int32]*recordBatch, 1)
					retry[topic] = retryParts
				}
				retryParts[partition] = batch

			case kerr.InvalidProducerEpoch,
				kerr.UnknownProducerId,
				kerr.OutOfOrderSequenceNumber:
				panic("fuxoid")

			default:
				retErr := err
				if err == kerr.DuplicateSequenceNumber {
					retErr = nil
				}
				for i, record := range batch.records {
					if err == nil {
						record.pr.r.Offset = responsePartition.BaseOffset + int64(i)
						record.pr.r.Partition = partition
						if record.pr.r.TimestampType.IsLogAppendTime() {
							record.pr.r.Timestamp = time.Unix(0, responsePartition.LogAppendTime*1e4)
						}
					}
					record.pr.promise(topic, record.pr.r, retErr)
				}
			}
		}

		errorTopicPartitions(topic, partitions, errNoResp) // any remaining partitions were not replied to
	}
	errorReq(req, errNoResp) // any remaining topics were not replied to
}

type retryBatch struct {
	topic string
	part  int32
	batch *recordBatch
}

// For all errors:
// If the error is retriable,
// always requeue to self.
// Once done, begin a goroutine that refreshes all metadata for the topics
//   that were requeued.
// Once that returns, move toppars from this broker as appropriate.

var forever = time.Now().Add(100 * 365 * 24 * time.Hour)

// handleRetryBatches requeues all batches that failed in a produce request
// back to the brokerToppars. The toppar is then considered "dead"
// relevant topic/partitions,
func (bt *brokerToppars) handleRetryBatches(retry map[string]map[int32]*recordBatch) {
	if retry == nil {
		return
	}

	migrate := make(map[string][]int32, len(retry))

	bt.topparForEachBatch(retry, func(tp *toppar, batch *recordBatch) {
		// If the first batch has the "forever" backoff, then
		// this topic is already being refreshed. This batch will
		// auto migrate whenever that refresh finishes.
		if len(tp.batches) == 0 ||
			!tp.batches.backoffDeadline.Equal(forever) {

			migrate[tp.topic] = append(migrate[tp.topic], tp.part)
		}
		tp.backoffDeadline = forever // tombstone
		tp.requeueBatchInOrder(batch)
	})

	for topic, migrateParts := range migrate {
		go bt.migrateTopic(topic, migrateParts)
	}
}

func (bt *brokerToppars) migrateTopic(topic string, migrateParts []int32) {
	cl := bt.br.cl

start:
	loadParts := newTopicParts()
	cl.fetchTopicMetadata(loadParts, topic)
	if loadParts.loadErr != nil {
		time.Sleep(time.Second) // TODO, also max retries
		goto start
	}

	cl.brokersMu.Lock()
	var retryMigrate []byte
	for _, migratePart := range migrateParts {
		broker, exists := cl.brokers[loadParts.all[migratePart].leader]
		if !exists {
			retryMigrate = append(retryMigrate, migratePart)
			continue
		}

		bt.migrateTopicPartTo(topic, migratePart, broker)
	}
	cl.brokersMu.Unlock()

	if len(retryMigrate) != 0 {
		time.Sleep(time.Second) // TODO
		goto start
	}

	// FINALLY, let new writes go to the new broker.
	cl.topicPartsMu.Lock()
	cl.topicParts[topic] = parts
	cl.topicPartsMu.Unlock()
}

func (bt *brokerToppars) migrateTopicPartTo(topic string, partition int32, br *broker) {
	bt.mu.Lock()
	tp := bt.toppars[topic][partition]
	bt.mu.Unlock()

	if br == br.bt {
		// TODO EAAASY
	}

	br.bt.Lock()
	destParts, exists := br.bt[topic]
	if !exists {
		destParts = make(map[int32]*toppar, 1)
		br.bt.topic[topic] = destParts
	}
	if _, exists = destParts[partition]; exists {
		panic("other broker should not have parts yet for migrating partition")
	}
	destParts[partition] = tp
	br.bt.Unlock()

	tp.mu.Lock()
	tp.backoffDeadline = time.Time{}
	for _, batch := range tp.batches {
		nreqs += len(batch.records)
	}
	tp.mu.Unlock()

	bt.removeToppar(tp)
}
