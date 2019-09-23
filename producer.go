package kgo

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/kgo/kerr"
	"github.com/twmb/kgo/kmsg"
)

// TODO ctx for start before partitions loaded

// func (p *Producer) BeginTransaction() *ProducerTransaction
// func (p *ProducerTransaction) Produce(r *Record)

type producer struct {
	id       int64
	epoch    int16
	idLoaded int32
	idMu     sync.Mutex

	bufferedRecords int64
	waitBuffer      chan struct{}
}

func noPromise(*Record, error) {}

// Produce sends a Kafka record to the topic in the record's Topic field,
// calling promise with the record or an error when Kafka replies.
//
// If the record cannot be written, due to it being too large or the client
// being unable to find a partition, this will return an error.
//
// For simplicity, this function considers messages too large if they are
// within 512 bytes of the record batch byte limit.
//
// The promise is optional, but not using it means you will not know if Kafka
// recorded a record properly.
func (c *Client) Produce(
	r *Record,
	promise func(*Record, error),
) error {
	if len(r.Key)+len(r.Value) > int(c.cfg.producer.maxRecordBatchBytes)-512 {
		return kerr.MessageTooLarge
	}

	if atomic.AddInt64(&c.producer.bufferedRecords, 1) > c.cfg.producer.maxBufferedRecords {
		<-c.producer.waitBuffer
	}

	if err := c.ensureProducerIDInit(); err != nil {
		return err
	}

	if promise == nil {
		promise = noPromise
	}

	parts, partsData := c.partitionsForTopicProduce(r.Topic)
	if partsData.loadErr != nil && !kerr.IsRetriable(partsData.loadErr) {
		return partsData.loadErr
	}
	if len(partsData.all) == 0 {
		return ErrNoPartitionsAvailable
	}

	parts.partsMu.Lock()
	defer parts.partsMu.Unlock()
	if parts.partitioner == nil {
		parts.partitioner = c.cfg.producer.partitioner.forTopic(r.Topic)
	}

	mapping := partsData.writable
	possibilities := partsData.writablePartitions
	if parts.partitioner.requiresConsistency(r) {
		mapping = partsData.all
		possibilities = partsData.partitions
	}
	if len(possibilities) == 0 {
		return ErrNoPartitionsAvailable
	}

	idIdx := parts.partitioner.partition(r, len(possibilities))
	id := possibilities[idIdx]
	partition := mapping[id]

	appended := partition.records.bufferRecord(
		promisedRecord{
			promise,
			r,
		},
		true, // KIP-480
	)
	if !appended {
		parts.partitioner.onNewBatch()
		idIdx = parts.partitioner.partition(r, len(possibilities))
		id = possibilities[idIdx]
		partition = mapping[id]
		partition.records.bufferRecord(
			promisedRecord{
				promise,
				r,
			},
			false, // KIP-480
		)
	}
	return nil
}

func (c *Client) ensureProducerIDInit() error {
	if atomic.LoadInt32(&c.producer.idLoaded) == 1 {
		return nil
	}
	c.producer.idMu.Lock()
	defer c.producer.idMu.Unlock()
	if c.producer.idLoaded == 1 {
		return nil
	}
	return c.initProducerID()
}

func (c *Client) initProducerID() error {
	resp, err := c.Request(c.ctx, &kmsg.InitProducerIDRequest{
		// TODO txn id
	})
	if err != nil {
		// If our broker is too old, then well...
		//
		// Note this is dependent on the first broker we hit;
		// there are other areas in this client where we assume
		// what we hit first is the default.
		if err == ErrUnknownRequestKey {
			atomic.StoreInt32(&c.producer.idLoaded, 1)
			return nil
		}
		return err
	}
	initResp := resp.(*kmsg.InitProducerIDResponse)

	c.producer.id = initResp.ProducerID
	c.producer.epoch = initResp.ProducerEpoch

	atomic.StoreInt32(&c.producer.idLoaded, 1)
	return nil
}

func (c *Client) finishRecordPromise(pnr promisedNumberedRecord, err error) {
	if atomic.AddInt64(&c.producer.bufferedRecords, -1) >= c.cfg.producer.maxBufferedRecords {
		go func() { c.producer.waitBuffer <- struct{}{} }()
	}
	pnr.promise(pnr.Record, err)
}

func (c *Client) partitionsForTopicProduce(topic string) (*topicPartitions, *topicPartitionsData) {
	topics := c.loadTopics()
	parts, exists := topics[topic]

	if !exists {
		c.topicsMu.Lock()
		topics = c.loadTopics()
		parts, exists = topics[topic]
		if !exists {
			parts = newTopicPartitions()
			newTopics := c.cloneTopics()
			newTopics[topic] = parts
			c.topics.Store(newTopics)
		}
		c.topicsMu.Unlock()
	}

	v := parts.load()
	if len(v.partitions) > 0 {
		return parts, v // fast, normal path
	}

	start := time.Now()
	parts.mu.RLock()
	defer parts.mu.RUnlock()

	tries := 0
	for tries < c.cfg.client.retries && len(v.partitions) == 0 &&
		(c.cfg.producer.recordTimeout == 0 || time.Since(start) < c.cfg.producer.recordTimeout) {
		tries++
		c.triggerUpdateMetadata()
		parts.c.Wait()
		v = parts.load()
	}
	return parts, v
}
