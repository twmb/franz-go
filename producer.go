package kgo

import (
	"sync"
	"sync/atomic"

	"github.com/twmb/kgo/kerr"
	"github.com/twmb/kgo/kmsg"
)

// TODO KIP-480:
// - change bufferRecord API to bufferRecord((pr promisedRecord, notIfNewBatch bool) (appended bool)
// - call with (pr, true)
// - if not appended, repartition, then call again on new partition with (pr, false)

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

	partitions := c.partitionsForTopicProduce(r.Topic)
	if partitions.loadErr != nil && !kerr.IsRetriable(partitions.loadErr) {
		return partitions.loadErr
	}

	mapping := partitions.writable
	if c.cfg.producer.partitioner.RequiresConsistency(r) {
		mapping = partitions.all
	}
	if len(mapping) == 0 {
		return ErrNoPartitionsAvailable
	}

	idIdx := c.cfg.producer.partitioner.Partition(r, len(partitions.partitions))
	id := partitions.partitions[idIdx]
	partition := mapping[id]

	partition.records.bufferRecord(
		promisedRecord{
			promise,
			r,
		},
	)
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

func (c *Client) partitionsForTopicProduce(topic string) *topicPartitionsData {
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
		return v // fast, normal path
	}

	parts.mu.RLock()
	defer parts.mu.RUnlock()

	tries := 0
	for tries < c.cfg.client.retries && len(v.partitions) == 0 {
		tries++
		c.triggerUpdateMetadata()
		parts.c.Wait()
		v = parts.load()
	}
	return v
}
