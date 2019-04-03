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
	inflightSem    atomic.Value
	inflightV1Plus bool

	// baseWireLength is the minimum wire length of a produce request
	// for a client.
	baseWireLength int32

	// nrecs is an atomic covering the total number of records buffered
	// in a brokerToppar.
	nrecs int64

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
		if !tp.backoffDeadline.IsZero() && now.Before(tp.backoffDeadline) {
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
			bt.removeTopparID(tp)
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
func (bt *brokerToppars) removeTopparID(rm *toppar) {
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
		tp = &toppar{
			topic:         topic,
			part:          part,
			allTopparsIdx: len(bt.allToppars),
			idWireLength:  2 + int32(len(topic)) + 4, // topic len, topic, parts array len
		}
		topicParts[part] = tp
		bt.allToppars = append(bt.allToppars, tp)
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
	if len(tp.batches) > 0 {
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
	tp.mu.Unlock()

	if atomic.AddInt64(&bt.nrecs, 1) == 1 {
		go bt.drain()
	}
}

func (bt *brokerToppars) drain() {
	again := true
	for again {
		var req *produceRequest
		sem := bt.inflightSem.Load().(chan struct{})
		sem <- struct{}{}
		req, again = bt.createRequest()

		bt.br.doSequencedAsyncPromise(
			req,
			func(resp kmsg.Response, err error) {
				bt.handleReqResp(req, resp, err)
				<-sem
			},
		)
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
	if err != nil {
		for topic, partitions := range req.topicsPartitions {
			for _, batch := range partitions {
				for _, record := range batch.records {
					record.pr.promise(topic, record.pr.r, err)
				}
			}
		}
		return
	}

	pr := resp.(*kmsg.ProduceResponse)
	for _, responseTopic := range pr.Responses {
		topic := responseTopic.Topic
		partitions, ok := req.topicsPartitions[topic]
		if !ok {
			continue // ???
		}
		delete(req.topicsPartitions, topic)

		for _, responsePartition := range responseTopic.PartitionResponses {
			partition := responsePartition.Partition
			batch, ok := partitions[partition]
			if !ok {
				continue // ???
			}
			delete(partitions, partition)

			err := kerr.ErrorForCode(responsePartition.ErrorCode)
			// TODO:
			// retriable errors
			// duplicate sequence num
			for i, record := range batch.records {
				if err == nil {
					record.pr.r.Offset = int64(i)
					record.pr.r.Partition = partition
					if record.pr.r.TimestampType.IsLogAppendTime() {
						record.pr.r.Timestamp = time.Unix(0, responsePartition.LogAppendTime*1e4)
					}
				}
				record.pr.promise(topic, record.pr.r, err)
			}
		}

		for _, batch := range partitions {
			for _, record := range batch.records {
				record.pr.promise(topic, record.pr.r, errNoResp)
			}
		}
	}
	for topic, partitions := range req.topicsPartitions {
		for _, batch := range partitions {
			for _, record := range batch.records {
				fmt.Println("YO")
				record.pr.promise(topic, record.pr.r, errNoResp)
			}
		}
	}
}
