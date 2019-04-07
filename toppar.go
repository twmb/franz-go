package kgo

import (
	"fmt"
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
	inflightSem      atomic.Value
	handledFirstResp bool

	// baseWireLength is the minimum wire length of a produce request
	// for a client.
	baseWireLength int32

	// The following backoff fields are used to ensure that the drain loop
	// begins appropriately after noticing all batches are in a backoff
	// state. The usage is complicated but documented.
	backoffTimer *time.Timer
	backoffSeq   uint64 // incremented under mu
	draining     bool

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

	sequenceNum int32

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
	batches       []*recordBatch
	batchDrainIdx int

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

	var (
		soonestDeadline time.Time
		deadlineTPs     int
		moreToDrain     bool
		now             = time.Now()
	)

	idx := bt.allTopparsStart
	for i := 0; i < len(bt.allToppars); i++ {
		tp := bt.allToppars[idx]
		if idx = idx + 1; idx == len(bt.allToppars) {
			idx = 0
		}

		tp.mu.Lock()
		if dl := tp.backoffDeadline; now.Before(dl) {
			tp.mu.Unlock()

			if dl.Before(soonestDeadline) {
				soonestDeadline = dl
			}
			deadlineTPs++
			continue
		}

		if len(tp.batches) == tp.batchDrainIdx {
			tp.mu.Unlock()
			continue
		}
		if tp.batchDrainIdx < 0 || tp.batchDrainIdx > len(tp.batches) {
			panic(fmt.Sprintf("INVALID LENGTHS %d %d", tp.batchDrainIdx, len(tp.batches)))
		}

		batch := tp.batches[tp.batchDrainIdx]
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

			moreToDrain = true
			continue
		}

		// Set that we are trying this batch before unlocking the
		// toppar so no new records will not be added.
		batch.tried = true
		batch.tp = tp
		tp.batchDrainIdx++
		if !moreToDrain {
			moreToDrain = len(tp.batches) > tp.batchDrainIdx
		}
		tp.mu.Unlock()

		// Now that we are for sure using the batch, create the
		// topic and partition in the request for it if necessary.
		if !topicPartitionExists {
			topicPartitions = make(map[int32]*recordBatch, 1)
			request.topicsPartitions[tp.topic] = topicPartitions
		}

		wireLength += batchWireLength
		topicPartitions[tp.part] = batch
	}

	// If we have no more to drain, if toppars are deadlined, begin the
	// drain backoff.
	if !moreToDrain && deadlineTPs != 0 {
		bt.beginDrainBackoff(soonestDeadline.Sub(now))
	}

	// For the next produce request, start on the next toppar.
	bt.allTopparsStart++
	if bt.allTopparsStart == len(bt.allToppars) {
		bt.allTopparsStart = 0
	}

	bt.draining = moreToDrain
	return request, moreToDrain
}

// incPastTopparBatchSuccess is called in a successful produce response,
// incrementing past the toppar's now-known-to-be-in-Kafka batch.
func (bt *brokerToppars) incPastTopparBatchSuccess(tp *toppar) {
	tp.mu.Lock()
	tp.batches = tp.batches[1:]
	tp.batchDrainIdx--
	//more := len(tp.batches) > 0
	tp.mu.Unlock()

	// If the toppar has no more data, remove it from the broker toppars.
	//if !more {
	//	bt.mu.Lock()
	//	tp.mu.Lock()
	//	if len(tp.batches) == 0 {
	//		bt.removeToppar(tp)
	//	}
	//	tp.mu.Unlock()
	//	bt.mu.Unlock()
	//}
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
	if bt.allTopparsStart == len(bt.allToppars) {
		bt.allTopparsStart = 0
	}
}

// newToppar creates and returns a new toppar under the given topic and part.
//
// The new toppar is added to all toppars, but is not added to the toppars map.
// The caller must have loaded the map to know the toppar does not exist, so
// the caller can add the new toppar cheaply.
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
func (bt *brokerToppars) bufferRecord(topic string, part int32, pr promisedRecord) {
	tp := bt.loadAndLockToppar(topic, part)

	newBatch := true
	firstBatch := tp.batchDrainIdx == len(tp.batches)

	if !firstBatch {
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
		tp.batches = append(tp.batches, bt.newRecordBatch(tp.sequenceNum, pr))
	}
	tp.sequenceNum++

	tp.mu.Unlock()

	if firstBatch {
		bt.maybeBeginDraining()
	}
}

func (bt *brokerToppars) maybeBeginDraining() {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	if bt.draining {
		return
	}
	bt.draining = true

	if bt.backoffTimer != nil {
		bt.backoffTimer.Stop()
		bt.backoffTimer = nil
		bt.backoffSeq++
	}

	go bt.drain()
}

// beginDrainBackoff is called if all toppars are detected to be in a backoff
// state and no produce request can be built.
func (bt *brokerToppars) beginDrainBackoff(after time.Duration) {
	seq := bt.backoffSeq

	bt.backoffTimer = time.AfterFunc(after, func() {
		bt.mu.Lock()
		defer bt.mu.Unlock()

		if seq != bt.backoffSeq {
			return
		}

		bt.backoffTimer = nil
		bt.draining = true
		go bt.drain()
	})
}

func (bt *brokerToppars) drain() {
	again := true
	for again {
		var req *produceRequest
		sem := bt.inflightSem.Load().(chan struct{})
		sem <- struct{}{}

		bt.mu.Lock()
		req, again = bt.createRequest()

		// We must hold the full lock while building this request.

		// We may have nothing to send if everything entered backoff.
		if len(req.topicsPartitions) == 0 {
			bt.mu.Unlock()
			<-sem // wont be using that
			continue
		}

		bt.br.doSequencedAsyncPromise(
			req,
			func(resp kmsg.Response, err error) {
				bt.handleReqResp(req, resp, err)
				<-sem
			},
		)
		bt.mu.Unlock()
	}
}

func (bt *brokerToppars) eachExistingTopparForBatch(recordBatches map[string]map[int32]*recordBatch, fn func(*toppar)) {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	for topic, partitions := range recordBatches {
		topicParts, topicPartsExist := bt.toppars[topic]
		if !topicPartsExist {
			// If the topic does not exists, this topic was
			// migrated off of this batch.
			continue
		}
		for partition, _ := range partitions {
			tp, partitionExists := topicParts[partition]
			if !partitionExists {
				continue // same
			}
			tp.mu.Lock()
			fn(tp)
			tp.mu.Unlock()
		}
	}
}

// requeueEntireReq requeues all batches in req to the brokerToppars.
// This is done if a retriable network error occured.
func (bt *brokerToppars) requeueEntireReq(req *produceRequest) {
	backoffDeadline := time.Now().Add(time.Second) // TODO
	bt.eachExistingTopparForBatch(req.topicsPartitions, func(tp *toppar) {
		// Only if the batch drain index is not zero do we reset it
		// and begin a backoff. If the drain index is already zero,
		// this req must have been a second inflight req and we do
		// not want to further backoff unnecessarily.
		if tp.batchDrainIdx != 0 {
			tp.batchDrainIdx = 0
			tp.backoffDeadline = backoffDeadline
		}
	})
}

func (bt *brokerToppars) errorReqAndRemoveToppars(req *produceRequest, err error) {
	for topic, partitions := range req.topicsPartitions {
		bt.errorTopicPartsAndRemoveToppars(topic, partitions, err)
	}
}

func (bt *brokerToppars) errorTopicPartsAndRemoveToppars(topic string, partitions map[int32]*recordBatch, err error) {
	for _, batch := range partitions {
		tp := batch.tp
		bt.mu.Lock()
		tp.mu.Lock()
		bt.removeToppar(tp)
		tp.mu.Unlock()
		bt.mu.Unlock()
		for _, batch := range tp.batches {
			for _, record := range batch.records {
				record.pr.promise(topic, record.pr.r, err)
			}
		}
		tp.batches = nil
	}
}

func (bt *brokerToppars) handleReqResp(req *produceRequest, resp kmsg.Response, err error) {
	if !bt.handledFirstResp {
		bt.handledFirstResp = true
		if req.version >= 4 {
			// NOTE we CANNOT store inflight >= 5. Kafka only
			// supports up to 5 concurrent in flight requests per
			// topic/partition. The first store here races with our
			// original 1 buffer, allowing one more than we store.
			bt.inflightSem.Store(make(chan struct{}, 2))
		}
	}

	// If we had an err, it is from the client itself. This is either a
	// retriable conn failure or a total loss (e.g. auth failure).
	if err != nil {
		if isRetriableBrokerErr(err) {
			bt.requeueEntireReq(req)
		} else {
			bt.errorReqAndRemoveToppars(req, err)
		}
		return
	}

	// retry tracks topics and partitions that we will retry.
	// It is initialized on the first failed topic/partition.
	var retry map[string]map[int32]*recordBatch
	defer bt.handleRetryBatches(retry)

	pr := resp.(*kmsg.ProduceResponse)
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

			case kerr.OutOfOrderSequenceNumber:
				// 1.0.0+: data loss
				// before: either data loss, or our write was so infrequent that
				// the old data rotated out and kafka no longer knows of our ID.
				// In that case, re-get produce ID and retry request.

			case kerr.UnknownProducerID:
				// 1.0.0+: if LogStartOffset is not our last acked + 1, then data loss
				// Otherwise, same as the outoforder thing.

			case kerr.InvalidProducerEpoch:
				fmt.Println("RESP", batch.producerID, batch.producerEpoch, batch.baseSequence)
				panic("fuxoid")

			default:
				for i, record := range batch.records {
					record.pr.r.Offset = responsePartition.BaseOffset + int64(i)
					record.pr.r.Partition = partition
					record.pr.promise(topic, record.pr.r, err)
				}

				bt.incPastTopparBatchSuccess(batch.tp)
			}
		}

		bt.errorTopicPartsAndRemoveToppars(topic, partitions, errNoResp)
	}
	bt.errorReqAndRemoveToppars(req, errNoResp)
}

var forever = time.Now().Add(100 * 365 * 24 * time.Hour)

// handleRetryBatches requeues all batches that failed in a produce request
// back to the brokerToppars.
func (bt *brokerToppars) handleRetryBatches(retry map[string]map[int32]*recordBatch) {
	if retry == nil {
		return
	}

	migrate := make(map[string][]int32, len(retry))

	bt.eachExistingTopparForBatch(retry, func(tp *toppar) {
		if tp.backoffDeadline != forever { // if we are not already migrating
			migrate[tp.topic] = append(migrate[tp.topic], tp.part)
		}
		tp.backoffDeadline = forever // tombstone
		tp.batchDrainIdx = 0
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

	type partDest struct {
		part int32
		dest *broker
	}
	var partDests []partDest

	cl.brokersMu.Lock()
	var retryMigrate []int32
	for _, migratePart := range migrateParts {
		broker, exists := cl.brokers[loadParts.all[migratePart].leader]
		if !exists {
			retryMigrate = append(retryMigrate, migratePart)
			continue
		}
		partDests = append(partDests, partDest{migratePart, broker})
	}
	cl.brokersMu.Unlock()

	if len(retryMigrate) != 0 {
		time.Sleep(time.Second) // TODO
		goto start
	}

	defer func() {
		bt.maybeBeginDraining()
		for _, partDest := range partDests {
			partDest.dest.bt.maybeBeginDraining()
		}
	}()

	// until everything is migrated.
	cl.topicPartsMu.Lock()
	oldParts := cl.topicParts[topic]
	cl.topicPartsMu.Unlock()

	// blocks any writes to toppar, after the unlock, everything
	// is waiting. concurrent writes unordered, who cares.
	// TODO cleanup doc
	oldParts.mu.Lock()
	oldParts.migrated = true
	oldParts.migrateCh = make(chan struct{})
	oldParts.mu.Unlock()

	defer close(oldParts.migrateCh)

	defer func() {
		cl.topicPartsMu.Lock()
		cl.topicParts[topic] = loadParts
		cl.topicPartsMu.Unlock()
	}()

	bt.mu.Lock()
	defer bt.mu.Unlock()

	for _, partDest := range partDests {
		bt.migrateTopicPartTo(topic, partDest.part, partDest.dest)
	}
}

func (bt *brokerToppars) migrateTopicPartTo(topic string, partition int32, br *broker) {
	tp := bt.toppars[topic][partition]
	if bt == br.bt {
		bt.clearTopparBackoff(tp)
		return
	}

	bt.removeToppar(tp)

	br.bt.mu.Lock()
	destParts, exists := br.bt.toppars[topic]
	if !exists {
		destParts = make(map[int32]*toppar, 1)
		br.bt.toppars[topic] = destParts
	}
	destTP, exists := destParts[partition]
	if !exists {
		destParts[partition] = tp
		tp.allTopparsIdx = len(br.bt.allToppars)
		br.bt.allToppars = append(br.bt.allToppars, tp)
	}
	br.bt.mu.Unlock()

	tp.mu.Lock()
	if exists {
		// If the destination topic and partition already exists,
		// whoever is using this client is producing to the same
		// topic across multiple goroutines. One of those was
		// sitting in Produce for an inordinate amount of time
		// and had a stale loaded partitions.
		//
		// They clearly do not care about strict ordering, so
		// we will append all of the logs in this re-migrated
		// partiion to the existing toppar.
		for _, batch := range tp.batches {
			batch.tp = destTP
			destTP.batches = append(destTP.batches, batch)
		}
	}
	tp.backoffDeadline = time.Time{}
	tp.mu.Unlock()
}

func (bt *brokerToppars) clearTopparBackoff(tp *toppar) {
	tp.mu.Lock()
	tp.backoffDeadline = time.Time{}
	tp.mu.Unlock()
}
