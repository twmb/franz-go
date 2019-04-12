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

	draining     bool            // are we actively draining toppars into batches?
	backoffTimer *time.Timer     // runs if all toppars are in a backoff state
	backoffSeq   uint64          // guards against timers starting drains they should not
	allToppars   []*topparBuffer // contains all toppers; used for batch building

	// allTopparsStart is where we will begin in allToppars for building a
	// batch. This increments by one every produce request, avoiding
	// starvation for large record batches that cannot fit into the request
	// that is currently being built.
	allTopparsStart int
}

type topparBuffer struct {
	owner *topicPartition // we reach back for our topic, partition

	// idWireLength covers the topic string and the part array length.
	// It does not cover part numbers nor record batches.
	idWireLength int32

	mu sync.Mutex // gaurds all fields below

	// drainer is who is currently draining this buffer. This can change
	// if a topic / partition is remapped to a different broker.
	drainer *brokerToppars

	// allTopparsIdx is our index into the drainer's allToppars. This exists
	// to aid removing this buffer from the drainer when necessary.
	allTopparsIdx int

	// sequenceNum is what we use for the baseSequence in a record batch.
	// This is incremented by one for every record appended to the toppar.
	sequenceNum int32

	// batches contains all batches that have not had promises called.
	batches []*recordBatch
	// batchDrainIdx is where the next produce request will drain from.
	batchDrainIdx int

	// failSeq is incremented whenever a record batch is fails somehow.
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
	failSeq uint64

	backoffDeadline time.Time // used for retries
}

// topparBatch wraps a recordBatch for a produce request with the toppar the
// recordBatch came from and the failSeq at the time it was used.
type topparBatch struct {
	toppar  *topparBuffer
	failSeq uint64
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

		visited int
	)

	idx := bt.allTopparsStart
	for ; visited < len(bt.allToppars); visited++ {
		tp := bt.allToppars[idx]
		if idx = idx + 1; idx == len(bt.allToppars) {
			idx = 0
		}

		// While checking toppar fields, we have to hold its lock.
		tp.mu.Lock()
		if dl := tp.backoffDeadline; now.Before(dl) {
			tp.mu.Unlock()

			if soonestDeadline.IsZero() || dl.Before(soonestDeadline) {
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

		topicPartitions, topicPartitionsExists := request.topicsPartitions[tp.owner.topic]
		if !topicPartitionsExists {
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

		failSeq := tp.failSeq // save this for below before unlocking tp

		if !moreToDrain {
			moreToDrain = len(tp.batches) > tp.batchDrainIdx
		}
		tp.mu.Unlock()

		// Now that we are for sure using the batch, create the
		// topic and partition in the request for it if necessary.
		if !topicPartitionsExists {
			topicPartitions = make(map[int32]topparBatch, 1)
			request.topicsPartitions[tp.owner.topic] = topicPartitions
		}

		wireLength += batchWireLength
		topicPartitions[tp.owner.partition] = topparBatch{
			toppar:      tp,
			failSeq:     failSeq,
			recordBatch: batch,
		}
	}

	if visited != len(bt.allToppars) {
		moreToDrain = true
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
// See docs on the toppar failSeq field.
func maybeIncPastTopparBatchSuccess(batch topparBatch) bool {
	tp := batch.toppar
	incd := false
	tp.mu.Lock()
	if batch.failSeq == tp.failSeq {
		tp.batches[0] = nil
		tp.batches = tp.batches[1:]
		tp.batchDrainIdx--
		incd = true
	}
	tp.mu.Unlock()
	return incd
}

func (tp *topparBuffer) bufferRecord(pr promisedRecord) {
	tp.mu.Lock()

	bt := tp.drainer

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

	if bt.draining {
		bt.mu.Unlock()
		return
	}
	bt.draining = true

	if bt.backoffTimer != nil {
		bt.backoffTimer.Stop()
		bt.backoffTimer = nil
		bt.backoffSeq++
	}
	bt.mu.Unlock()

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
	// Before we begin draining, sleep a tiny bit. This helps when a
	// high volume new toppar began draining; rather than immediately
	// eating just one record, we allow it to buffer a bit before we
	// loop draining.
	time.Sleep(time.Millisecond)

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

func (bt *brokerToppars) eachTopparBatch(topparBatches map[string]map[int32]topparBatch, fn func(topparBatch)) {
	for _, partitions := range topparBatches {
		for _, batch := range partitions {
			batch.toppar.mu.Lock()
			fn(batch)
			batch.toppar.mu.Unlock()
		}
	}
}

// requeueEntireReq requeues all batches in req to the brokerToppars.
// This is done if a retriable network error occured.
func (bt *brokerToppars) requeueEntireReq(req *produceRequest) {
	backoffDeadline := time.Now().Add(bt.br.cl.cfg.client.retryBackoff)
	maybeBeginDraining := false
	bt.eachTopparBatch(req.topicsPartitions, func(batch topparBatch) {
		if batch.failSeq != batch.toppar.failSeq {
			return
		}
		batch.toppar.batchDrainIdx = 0
		batch.toppar.backoffDeadline = backoffDeadline
		batch.toppar.failSeq++
		maybeBeginDraining = true
	})

	if maybeBeginDraining {
		bt.maybeBeginDraining()
	}
}

// errorAllReqToppars errors every record buffered in all toppars in a request.
// This is called for unrecoverable errors, such as auth failures.
func (bt *brokerToppars) errorAllReqToppars(req *produceRequest, err error) {
	for topic, partitions := range req.topicsPartitions {
		bt.errorAllPartitionToppars(topic, partitions, err)
	}
}

// errorAllPartitionToppars errors every record buffered in all toppars in
// a request's topic partition.
func (bt *brokerToppars) errorAllPartitionToppars(topic string, partitions map[int32]topparBatch, err error) {
	for _, batch := range partitions {
		tp := batch.toppar
		tp.mu.Lock()
		if batch.failSeq == tp.failSeq {
			tp.failSeq++ // make sure a second in-flight does not do this again
			for _, batch := range tp.batches {
				for i, record := range batch.records {
					bt.br.cl.promise(topic, record.pr, err)
					batch.records[i] = noPNR
				}
				emptyRecordsPool.Put(batch.records[:0])
			}
			tp.batches = nil
		}
		tp.mu.Unlock()
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
			bt.inflightSem.Store(make(chan struct{}, 4))
		}
	}

	// retry tracks topics and partitions that we will retry.
	// It is initialized on the first failed topic/partition.
	var retry map[string]map[int32]topparBatch
	defer func() {
		if len(retry) > 0 {
			bt.handleRetryBatches(retry)
		}
	}()

	// If we had an err, it is from the client itself. This is either a
	// retriable conn failure or a total loss (e.g. auth failure).
	if err != nil {
		if isRetriableBrokerErr(err) {
			if err == ErrBrokerDead {
				// The broker was closed. This could be from
				// broker migration, so we retry.
				retry = req.topicsPartitions
			} else {
				bt.requeueEntireReq(req)
			}
		} else {
			bt.errorAllReqToppars(req, err)
		}
		return
	}

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
				if batch.failSeq != batch.toppar.failSeq {
					continue
				}
				panic("OOO SEQUENCE NUM - TODO") // TODO

			case kerr.UnknownProducerID:
				// 1.0.0+: if LogStartOffset is not our last acked + 1, then data loss
				// Otherwise, same log rotation as in OOO sequence number.
				if batch.failSeq != batch.toppar.failSeq {
					continue
				}
				panic("UNKNOWN PRODUCER ID - TODO") // TODO

			case kerr.InvalidProducerEpoch:
				panic("INVALID EPOCH - TODO") // TODO

			default:
				incd := maybeIncPastTopparBatchSuccess(batch)
				if !incd {
					continue
				}
				for i, record := range batch.records {
					record.pr.r.Offset = responsePartition.BaseOffset + int64(i)
					record.pr.r.Partition = partition
					bt.br.cl.promise(topic, record.pr, err)
					batch.records[i] = noPNR
				}
				emptyRecordsPool.Put(batch.records[:0])
			}
		}

		if len(partitions) > 0 {
			bt.errorAllPartitionToppars(topic, partitions, errNoResp)
		}
	}
	if len(req.topicsPartitions) > 0 {
		bt.errorAllReqToppars(req, errNoResp)
	}
}

var forever = time.Now().Add(100 * 365 * 24 * time.Hour)

func (bt *brokerToppars) handleRetryBatches(retry map[string]map[int32]topparBatch) {
	bt.eachTopparBatch(retry, func(batch topparBatch) {
		if batch.failSeq != batch.toppar.failSeq ||
			batch.toppar.backoffDeadline == forever {

			skipTopicRetry := retry[batch.toppar.owner.topic]
			delete(skipTopicRetry, batch.toppar.owner.partition)
			if len(skipTopicRetry) == 0 {
				delete(retry, batch.toppar.owner.topic)
			}

			return
		}

		batch.toppar.backoffDeadline = forever // tombstone
		batch.toppar.batchDrainIdx = 0
		batch.toppar.failSeq++
	})

	for topic, migrateParts := range retry {
		go bt.migrateTopic(topic, migrateParts)
	}
}

func (bt *brokerToppars) migrateTopic(topic string, migrateParts map[int32]topparBatch) {
	cl := bt.br.cl

start:
	loadParts := newTopicParts()
	cl.fetchTopicMetadataIntoParts(loadParts, topic, false)
	if loadParts.loadErr != nil {
		time.Sleep(cl.cfg.client.retryBackoff) // TODO max retries
		goto start
	}

	var deletedParts []*topicPartition
	defer func() {
		for _, tp := range deletedParts {
			tp.toppar.drainer.removeToppar(&tp.toppar)

			tp.toppar.mu.Lock()
			defer tp.toppar.mu.Unlock()

			tp.toppar.failSeq++ // just in case
			for _, batch := range tp.toppar.batches {
				for i, record := range batch.records {
					bt.br.cl.promise(topic, record.pr, ErrPartitionDeleted)
					batch.records[i] = noPNR
				}
				emptyRecordsPool.Put(batch.records[:0])
			}
		}
	}()

	// If any part we want to migrate no longer exists, the partition
	// has been deleted.
	for migratePart, migrateToppar := range migrateParts {
		if _, exists := loadParts.all[migratePart]; !exists {
			deletedParts = append(deletedParts, migrateToppar.toppar.owner)
		}
	}

	existingParts, err := cl.partitionsForTopic(topic)
	if err != nil {
		panic("migrating existing topic, existing parts have err " + err.Error())
	}

	// We block all records from being added while we migrate partitions.
	existingParts.mu.Lock()
	defer existingParts.mu.Unlock()

	existingParts.allIDs = loadParts.allIDs
	existingParts.writableIDs = loadParts.writableIDs

	// For all existing parts, if they no longer exist, we will call all
	// buffered records with a partition deleted error.
	//
	// If the toppar does exist, but the drainer is different (leader
	// broker changed), we remove the toppar from the old drainer and add
	// it to the new.
	for id, tp := range existingParts.all {
		if newTP, exists := loadParts.all[id]; !exists {
			deletedParts = append(deletedParts, tp)
		} else if newTP.toppar.drainer != tp.toppar.drainer {
			tp.toppar.drainer.removeToppar(&tp.toppar)
			tp.toppar.drainer = newTP.toppar.drainer
			tp.toppar.drainer.addToppar(&tp.toppar)
		} else {
			tp.toppar.clearBackoff()
			tp.toppar.drainer.maybeBeginDraining()
		}
		delete(loadParts.all, id)
	}

	// For any new parts that we did not know about prior, we add them.
	for id, tp := range loadParts.all {
		existingParts.all[id] = tp
		tp.toppar.drainer.addToppar(&tp.toppar)
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

func (tp *topparBuffer) clearBackoff() {
	tp.mu.Lock()
	tp.backoffDeadline = time.Time{}
	tp.mu.Unlock()
}

func (bt *brokerToppars) addToppar(add *topparBuffer) {
	bt.mu.Lock()
	add.allTopparsIdx = len(bt.allToppars)
	bt.allToppars = append(bt.allToppars, add)
	bt.mu.Unlock()

	add.clearBackoff()

	bt.maybeBeginDraining()
}

// removeToppar removes the tracking of a toppar from the brokerToppars.
func (bt *brokerToppars) removeToppar(rm *topparBuffer) {
	bt.mu.Lock()
	defer bt.mu.Unlock()

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
