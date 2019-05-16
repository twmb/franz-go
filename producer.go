package kgo

import (
	"sync"
	"sync/atomic"

	"github.com/twmb/kgo/kerr"
	"github.com/twmb/kgo/kmsg"
)

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
// The promise is optional, but not using it means you will not know if Kafka
// recorded a record properly.
func (c *Client) Produce(
	r *Record,
	promise func(*Record, error),
) error {
	// TODO validate lengths here

	if atomic.AddInt64(&c.producer.bufferedRecords, 1) > c.cfg.producer.maxBufferedRecords {
		<-c.producer.waitBuffer
	}

	if err := c.ensureProducerIDInit(); err != nil {
		return err
	}

	if promise == nil {
		promise = noPromise
	}

	partitions, err := c.partitionsForTopicProduce(r.Topic)
	if err != nil {
		return err
	}

	partitions.mu.RLock()

	mapping := partitions.writable
	if c.cfg.producer.partitioner.RequiresConsistency(r) {
		mapping = partitions.all
	}
	if len(mapping) == 0 {
		partitions.mu.RUnlock()
		return ErrNoPartitionsAvailable
	}

	idIdx := c.cfg.producer.partitioner.Partition(r, len(partitions.partitions))
	id := partitions.partitions[idIdx]
	partition := mapping[id]

	partition.records.bufferRecord(
		promisedRecord{
			promise: promise,
			r:       r,
		},
	)
	partitions.mu.RUnlock()
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
	resp, err := c.Request(&kmsg.InitProducerIDRequest{
		// TODO txn id
	})
	if err != nil {
		return err
	}
	initResp := resp.(*kmsg.InitProducerIDResponse)

	c.producer.id = initResp.ProducerID
	c.producer.epoch = initResp.ProducerEpoch

	atomic.StoreInt32(&c.producer.idLoaded, 1)
	return nil
}

func (c *Client) promise(pr promisedRecord, err error) {
	if atomic.AddInt64(&c.producer.bufferedRecords, -1) >= c.cfg.producer.maxBufferedRecords {
		go func() { c.producer.waitBuffer <- struct{}{} }()
	}
	pr.promise(pr.r, err)
}

func (c *Client) partitionsForTopicProduce(topic string) (*topicPartitions, error) {
	c.topicsMu.RLock()
	parts, exists := c.topics[topic]
	c.topicsMu.RUnlock()

	if !exists {
		c.topicsMu.Lock()
		parts, exists = c.topics[topic]
		if !exists {
			parts = newTopicParts()
			c.topics[topic] = parts
		}
		c.topicsMu.Unlock()
	}

	tries := 0
	parts.mu.RLock()
	for tries < c.cfg.client.retries &&
		(parts.seq == 0 || kerr.IsRetriable(parts.loadErr)) {
		tries++
		c.triggerUpdateMetadata()
		parts.c.Wait()
	}
	loadErr := parts.loadErr
	parts.mu.RUnlock()

	return parts, loadErr
}
