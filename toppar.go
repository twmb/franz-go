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
	inflightSem      atomic.Value
	handledFirstResp bool

	// baseWireLength is the minimum wire length of a produce request
	// for a client.
	baseWireLength int32

	mu sync.Mutex // guards all fields below

	draining     bool                         // are we actively draining toppars into batches?
	backoffTimer *time.Timer                  // runs if all toppars are in a backoff state
	backoffSeq   uint64                       // guards against timers starting drains they should not
	toppars      map[string]map[int32]*toppar // topic => partition => toppar
	allToppars   []*toppar                    // contains all toppers; used for batch building

	// allTopparsStart is where we will begin in allToppars for building a
	// batch. This increments by one every produce request, avoiding
	// starvation for large record batches that cannot fit into the request
	// that is currently being built.
	allTopparsStart int
}

// toppar captures buffered records for a TOPic and PARtition.
type toppar struct {
	topic string
	part  int32

	// allTopparsIdx is this toppar's index into the owning brokerToppar's
	// allToppars. This is used to aid in removing the toppar.
	allTopparsIdx int

	// idWireLength covers the topic string and the part array length.
	// It does not cover part numbers nor record batches.
	idWireLength int32

	mu sync.Mutex // guards all fields below

	// sequenceNum is what we use for the baseSequence in a record batch.
	// This is incremented by one for every record appended to the toppar.
	sequenceNum int32

	// batches contains all un-acknowledged or actively being built
	// record batches. Once a batch is acknowledged successfully,
	// it is removed.
	batches []*recordBatch
	// batchDrainIdx is where the next brokerToppar's drain will start
	// in batches. If any batch fails, this is reset to 0 to automatically
	// "requeue" everything.
	batchDrainIdx int

	// requeueSeq is incremented whenever a record batch is "requeued".
	//
	// This field is only needed because we allow multiple in flight
	// requests. One request could be accepted by Kafka and then the
	// connection could die. The second in flight batch that we cannot
	// cancel would trigger a new connection, can be sent successfully and
	// then receive a successful response.
	//
	// We do not want to track the first successful response because we
	// still think we failed on the first batch and will end up re-sending
	// this second batch. When we re-send the first batch, Kafka will reply
	// as if it is the first time it is seeing it. We do not want to call
	// the second batch's promises before the first.
	requeueSeq uint64

	// backoffDeadline is used for retries: if a toppar's records fail and
	// are requeued to the front of batches, the batch will not be retried
	// until after the deadline.
	backoffDeadline time.Time
}

// topparBatch wraps a recordBatch for a produce request with the toppar the
// recordBatch came from and the requeueSeq at the time it was used.
type topparBatch struct {
	toppar     *toppar
	requeueSeq uint64
	*recordBatch
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
// and whether there are more records to create more requests immediately.
func (bt *brokerToppars) createRequest() (*produceRequest, bool) {
	request := &produceRequest{
		// TODO transactional ID
		acks:             bt.br.cl.cfg.producer.acks.val,
		timeout:          bt.br.cl.cfg.client.requestTimeout,
		topicsPartitions: make(map[string]map[int32]topparBatch, 5),

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

		// While checking toppar fields, we have to hold its lock.
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

		batch := tp.batches[tp.batchDrainIdx]
		batchWireLength := 4 + batch.wireLength // part ID + batch

		topicPartitions, topicPartitionExists := request.topicsPartitions[tp.topic]
		if !topicPartitionExists {
			batchWireLength += tp.idWireLength // new topic in the request: add topic overhead
		}

		if wireLength+batchWireLength > wireLengthLimit {
			tp.mu.Unlock()
			// The new topic/part/batch would exceed the max wire
			// length so we skip and use it in a future request.
			moreToDrain = true
			continue
		}

		batch.tried = true // we are using the batch, so it must now be immutable
		tp.batchDrainIdx++

		requeueSeq := tp.requeueSeq // save this for below before unlocking tp

		if !moreToDrain {
			moreToDrain = len(tp.batches) > tp.batchDrainIdx
		}
		tp.mu.Unlock()

		// Now that we are for sure using the batch, create the
		// topic and partition in the request for it if necessary.
		if !topicPartitionExists {
			topicPartitions = make(map[int32]topparBatch, 1)
			request.topicsPartitions[tp.topic] = topicPartitions
		}

		wireLength += batchWireLength
		topicPartitions[tp.part] = topparBatch{
			toppar:      tp,
			requeueSeq:  requeueSeq,
			recordBatch: batch,
		}
	}

	if !moreToDrain && deadlineTPs != 0 {
		bt.beginDrainBackoff(soonestDeadline.Sub(now))
	}

	bt.allTopparsStart++
	if bt.allTopparsStart == len(bt.allToppars) {
		bt.allTopparsStart = 0
	}

	bt.draining = moreToDrain
	return request, moreToDrain
}

// maybeIncPastTopparBatchSuccess is called in a successful produce response,
// incrementing past the toppar's now-known-to-be-in-Kafka ONLY IF the batch
// has the same requeue seq as the toppar.
//
// See docs on the toppar requeueSeq field.
func (bt *brokerToppars) maybeIncPastTopparBatchSuccess(batch topparBatch) bool {
	tp := batch.toppar
	incd := false
	tp.mu.Lock()
	if batch.requeueSeq == tp.requeueSeq {
		tp.batches = tp.batches[1:]
		tp.batchDrainIdx--
		incd = true
	}
	tp.mu.Unlock()
	return incd
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

// drain drains buffered records and issues produce requests.
func (bt *brokerToppars) drain() {
	again := true
	for again {
		var req *produceRequest
		sem := bt.inflightSem.Load().(chan struct{})
		sem <- struct{}{}

		// We must hold the toppars mu while creating the request all
		// the way thru issuing it. If we release before issuing, the
		// create could set that drains are stopping, a new record
		// could start a new drain, and a new request could sneak in
		// and be issued before ours was.
		bt.mu.Lock()
		req, again = bt.createRequest()

		if len(req.topicsPartitions) == 0 { // everything entered backoff
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

func (bt *brokerToppars) eachExistingTopparForBatch(topparBatches map[string]map[int32]topparBatch, fn func(*toppar, topparBatch)) {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	for topic, partitions := range topparBatches {
		topicParts, topicPartsExist := bt.toppars[topic]
		if !topicPartsExist {
			// If the topic does not exists, this topic was
			// migrated off of this batch.
			continue
		}
		for partition, batch := range partitions {
			tp, partitionExists := topicParts[partition]
			if !partitionExists {
				continue // same
			}
			tp.mu.Lock()
			fn(tp, batch)
			tp.mu.Unlock()
		}
	}
}

// requeueEntireReq requeues all batches in req to the brokerToppars.
// This is done if a retriable network error occured.
func (bt *brokerToppars) requeueEntireReq(req *produceRequest) {
	backoffDeadline := time.Now().Add(bt.br.cl.cfg.client.retryBackoff)
	maybeBeginDraining := false
	// The only reason a toppar would not exist here is if were migrated
	// off of the broker to a different toppar, in which case the drain
	// index has already been reset.
	bt.eachExistingTopparForBatch(req.topicsPartitions, func(tp *toppar, batch topparBatch) {
		if batch.requeueSeq == tp.requeueSeq {
			tp.batchDrainIdx = 0
			tp.backoffDeadline = backoffDeadline
			tp.requeueSeq++
			maybeBeginDraining = true
		}
	})

	if maybeBeginDraining {
		bt.maybeBeginDraining()
	}
}

func (bt *brokerToppars) errorReqAndRemoveToppars(req *produceRequest, err error) {
	for topic, partitions := range req.topicsPartitions {
		bt.errorTopicPartsAndRemoveToppars(topic, partitions, err)
	}
}

func (bt *brokerToppars) errorTopicPartsAndRemoveToppars(topic string, partitions map[int32]topparBatch, err error) {
	for _, batch := range partitions {
		tp := batch.toppar
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
	var retry map[string]map[int32]topparBatch
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
			case kerr.UnknownTopicOrPartition,
				kerr.NotLeaderForPartition, // stale metadata
				kerr.NotEnoughReplicas,
				kerr.RequestTimedOut:

				if retry == nil {
					retry = make(map[string]map[int32]topparBatch, 5)
				}
				retryParts, exists := retry[topic]
				if !exists {
					retryParts = make(map[int32]topparBatch, 1)
					retry[topic] = retryParts
				}
				retryParts[partition] = batch

			case kerr.OutOfOrderSequenceNumber:
				// 1.0.0+: data loss
				// before: either data loss, or our write was so infrequent that
				// the old data rotated out and kafka no longer knows of our ID.
				// In that case, re-get produce ID and retry request.
				// If the sequence numbers do not align, then we had two
				// requests in flight: the first issued on a cxn that died,
				// the second issued on a new connection causing this error.
				if batch.requeueSeq != batch.toppar.requeueSeq {
					continue
				}
				panic("OOO SEQUENCE NUM - TODO") // TODO

			case kerr.UnknownProducerID:
				// 1.0.0+: if LogStartOffset is not our last acked + 1, then data loss
				// Otherwise, same log rotation as in OOO sequence number.
				panic("UNKNOWN PRODUCER ID - TODO") // TODO

			case kerr.InvalidProducerEpoch:
				panic("INVALID EPOCH - TODO") // TODO

			default:
				incd := bt.maybeIncPastTopparBatchSuccess(batch)
				if !incd {
					continue
				}
				for i, record := range batch.records {
					record.pr.r.Offset = responsePartition.BaseOffset + int64(i)
					record.pr.r.Partition = partition
					record.pr.promise(topic, record.pr.r, err)
				}
			}
		}

		bt.errorTopicPartsAndRemoveToppars(topic, partitions, errNoResp)
	}
	bt.errorReqAndRemoveToppars(req, errNoResp)
}

var forever = time.Now().Add(100 * 365 * 24 * time.Hour)

// handleRetryBatches requeues all batches that failed in a produce request
// back to the brokerToppars.
func (bt *brokerToppars) handleRetryBatches(retry map[string]map[int32]topparBatch) {
	if retry == nil {
		return
	}

	migrate := make(map[string][]int32, len(retry))

	// The only way the toppar does not exist is if it is already
	// migrated. This is called inside handling a produce resp, so
	// a prior resp may have failed and the retry here would be
	// from the second in flight. This is further reinforced
	// with the requeueSeq check.
	bt.eachExistingTopparForBatch(retry, func(tp *toppar, batch topparBatch) {
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
			//batch.toppar = destTP
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
