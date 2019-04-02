package kgo

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/kgo/kerr"
	"github.com/twmb/kgo/kmsg"
)

type brokerToppars struct {
	br *broker

	baseWireLength int32

	mu         sync.Mutex
	nrecs      int64
	toppars    map[topparID]*toppar
	allToppars []*toppar
}

func newBrokerToppars(br *broker) *brokerToppars {
	const messageRequestOverhead int32 = 2 + // key
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
		toppars:        make(map[topparID]*toppar, 1),
	}
	if br.cl.cfg.client.id != nil {
		bt.baseWireLength += int32(len(*br.cl.cfg.client.id))
	}

	return bt
}

func (bt *brokerToppars) createRequest() (*messageBufferedProduceRequest, bool) {
	request := &messageBufferedProduceRequest{
		// TODO transactional ID
		acks:    bt.br.cl.cfg.producer.acks.val,
		timeout: int32(time.Second / 1e6), // TODO
		data:    make(map[string]map[int32]*recordBatch, 1),

		compression: bt.br.cl.cfg.producer.compression,
	}

	// TODO vary starting index. We may skip a large batch.
	// We can use starting index and always increment it by
	// one. This will get a single large batch at some point,
	// problem is then we should keep toppars in order.
	// Remove currently loses ordering.

	now := time.Now()
	wireLength := bt.baseWireLength
	wireLengthLimit := bt.br.cl.cfg.producer.maxBrokerWriteBytes

	var reqRecs int
	var reqTPs []*toppar

	bt.mu.Lock()
	allToppars := bt.allToppars
	bt.mu.Unlock()

	for _, tp := range allToppars {
		tp.mu.Lock()
		if !tp.backoffDeadline.IsZero() &&
			now.Before(tp.backoffDeadline) {
			tp.mu.Unlock()
			continue
		}

		if len(tp.batches) == 0 {
			tp.mu.Unlock()
			reqTPs = append(reqTPs, tp)
			continue
		}

		batch := tp.batches[0]
		tp.mu.Unlock()

		batch.mu.Lock()
		batchWireLength := 4 + batch.wireLength // part ID + batch

		// If this topic does not exist yet in the request,
		// we need to add the topic's overhead.
		reqTopicPart, reqTopicPartExists := request.data[tp.id.topic]
		if !reqTopicPartExists {
			batchWireLength += tp.idWireLength
		}

		// If this new topic/part/batch would exceed
		// the max wire length, we skip it.
		// It will fit on a future built request.
		if wireLength+batchWireLength > wireLengthLimit {
			batch.mu.Unlock()
			continue
		}
		batch.tried = true
		batch.mu.Unlock()

		if !reqTopicPartExists {
			reqTopicPart = make(map[int32]*recordBatch, 1)
			request.data[tp.id.topic] = reqTopicPart
		}

		reqTopicPart[tp.id.part] = batch
		reqRecs += len(batch.records)
		reqTPs = append(reqTPs, tp)
	}

	bt.mu.Lock()
	for _, tp := range reqTPs {
		tp.mu.Lock()
		if len(tp.batches) > 0 {
			tp.batches = tp.batches[1:]
		}
		if len(tp.batches) == 0 {
			bt.removeTopparID(tp.id)
		}
		tp.mu.Unlock()
	}
	bt.mu.Unlock()

	return request, atomic.AddInt64(&bt.nrecs, int64(-reqRecs)) > 0
}

func (bt *brokerToppars) removeTopparID(id topparID) {
	rm := bt.toppars[id]
	delete(bt.toppars, id)

	if rm.allTopparsIdx != len(bt.allToppars)-1 {
		bt.allToppars[rm.allTopparsIdx], bt.allToppars[len(bt.allToppars)-1] =
			bt.allToppars[len(bt.allToppars)-1], nil

		bt.allToppars[rm.allTopparsIdx].allTopparsIdx = rm.allTopparsIdx
	}

	bt.allToppars = bt.allToppars[:len(bt.allToppars)-1]
}

func (bt *brokerToppars) addNewToppar(tp *toppar) {
	bt.toppars[tp.id] = tp
	bt.allToppars = append(bt.allToppars, tp)
	tp.allTopparsIdx = len(bt.allToppars) - 1
}

// By the time we are adding a record, we know it has a valid length, for
// we have already looked up the part from Kafka with the topic name.
func (bt *brokerToppars) addRecord(topic string, part int32, pr promisedRecord) error {
	// TODO check record length

	id := topparID{topic, part}

	// Lock the broker toppars to load or create this toppar.
	// Lock the toppar while at it.
	bt.mu.Lock()
	tp, topparExists := bt.toppars[id]
	if !topparExists {
		tp = &toppar{
			id:           topparID{topic, part},
			idWireLength: 2 + int32(len(topic)) + 4,
		}
		bt.addNewToppar(tp)
		tp.mu.Lock()
	} else {
		tp.mu.Lock()
	}
	bt.mu.Unlock()
	defer tp.mu.Unlock()

	// fits returns if an extra amount of data could fit with the
	// base amount of data in a produce request write.
	fits := func(extra int32) bool {
		return bt.baseWireLength+extra <= bt.br.cl.cfg.producer.maxBrokerWriteBytes
	}

	newBatch := true
	if len(tp.batches) > 0 {
		batch := tp.batches[len(tp.batches)-1]
		prNums := batch.calculateRecordNumbers(pr.r)
		batch.mu.Lock()
		if !batch.tried && fits(batch.wireLength+prNums.wireLength) {
			newBatch = false
			batch.appendRecord(pr, prNums)
		}
		batch.mu.Unlock()
	}

	if newBatch {
		batch := newRecordBatch(pr, bt.baseWireLength+tp.idWireLength)
		if !fits(tp.idWireLength + batch.wireLength) {
			return errRecordTooLarge // this batch alone is too large
		}
		tp.batches = append(tp.batches, batch)
	}

	if atomic.AddInt64(&bt.nrecs, 1) == 1 {
		go bt.drain()
	}

	return nil
}

type topparID struct {
	topic string
	part  int32
}

type toppar struct {
	id            topparID
	allTopparsIdx int

	// idWireLength covers the topic string and the part array length.
	// It does not cover part numbers nor record batches.
	idWireLength int32

	tried bool // for retries TODO

	mu sync.Mutex

	batches         []*recordBatch
	backoffDeadline time.Time
}

func (bt *brokerToppars) drain() {
	again := true
	for again {
		var req *messageBufferedProduceRequest
		req, again = bt.createRequest()

		bt.br.doSequencedAsyncPromise(
			req,
			func(resp kmsg.Response, err error) {
				if err != nil {
					for topic, partitions := range req.data {
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
					partitions, ok := req.data[topic]
					if !ok {
						continue // ???
					}
					delete(req.data, topic)

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
				for topic, partitions := range req.data {
					for _, batch := range partitions {
						for _, record := range batch.records {
							record.pr.promise(topic, record.pr.r, errNoResp)
						}
					}
				}
			},
		)

	}
}
