package kgo

import (
	"sync/atomic"
	"time"

	"github.com/twmb/kgo/kerr"
	"github.com/twmb/kgo/kmsg"
)

// func (p *Producer) BeginTransaction() *ProducerTransaction
// func (p *ProducerTransaction) Produce(r *Record)

func noPromise(string, *Record, error) {}

// Produce sends a record to Kafka under the given topic, calling promise with
// the topic/record/error when Kafka replies.
//
// If the record cannot be written, due to it being too large or the client
// being unable to find a partition, this will return an error.
//
// The promise is optional, but not using it means you will not know if Kafka
// recorded a record properly.
func (c *Client) Produce(
	topic string,
	r *Record,
	promise func(string, *Record, error),
) error {
	if atomic.AddInt64(&c.bufferedRecords, 1) > c.cfg.producer.maxBufferedRecords {
		<-c.waitBuffer
	}

	if err := c.ensureProducerIDInit(); err != nil {
		return err
	}

	partitions, err := c.partitionsForTopic(topic)
	if err != nil {
		return err
	}

	partitions.mu.RLock()
	// TODO switch to defer? large loss.

	ids := partitions.writableIDs
	mapping := partitions.writable
	if c.cfg.producer.partitioner.RequiresConsistency(r) {
		ids = partitions.allIDs
		mapping = partitions.all
	}
	if len(ids) == 0 {
		partitions.mu.RUnlock()
		return errNoPartitionIDs
	}

	idIdx := c.cfg.producer.partitioner.Partition(r, len(ids))
	if idIdx > len(ids) {
		idIdx = len(ids) - 1
	}

	id := ids[idIdx]
	partition, exists := mapping[id]
	if !exists {
		partitions.mu.RUnlock()
		return errUnknownPartition // should never happen
	}

	if r.Timestamp.IsZero() {
		r.Timestamp = time.Now()
	}

	if promise == nil {
		promise = noPromise
	}

	// TODO validate lengths here

	// TODO KIP-359 will eventually introduce leader epoch to
	// differentiation between UNKNOWN_LEADER_EPOCH (we have have more up
	// to date info) and FENCED_LEADER_EPOCH.

	partition.toppar.bufferRecord(
		promisedRecord{
			promise: promise,
			r:       r,
		},
	)
	partitions.mu.RUnlock()
	return nil
}

func (c *Client) ensureProducerIDInit() error {
	if atomic.LoadInt64(&c.producerIDLoaded) == 1 {
		return nil
	}

	c.producerIDMu.Lock()
	defer c.producerIDMu.Unlock()

	if atomic.LoadInt64(&c.producerIDLoaded) == 1 {
		return nil
	}

	return c.initProducerID()
}

func (c *Client) initProducerID() error {
	broker := c.broker()

	var initResp *kmsg.InitProducerIDResponse
	var err error
start:
	broker.wait(
		&kmsg.InitProducerIDRequest{ /*TODO transactional ID */ },
		func(resp kmsg.Response, respErr error) {
			if err = respErr; err != nil {
				return
			}
			initResp = resp.(*kmsg.InitProducerIDResponse)
		},
	)

	if err != nil {
		if isRetriableBrokerErr(err) {
			time.Sleep(c.cfg.client.retryBackoff)
			goto start // TODO limit amount
		} else {
			return err
		}
	}

	// TODO handle initResp err
	if err = kerr.ErrorForCode(initResp.ErrorCode); err != nil {
		return err
	}

	c.producerID = initResp.ProducerID
	c.producerEpoch = initResp.ProducerEpoch

	atomic.StoreInt64(&c.producerIDLoaded, 1)

	return nil
}

func (c *Client) promise(topic string, pr promisedRecord, err error) {
	if atomic.AddInt64(&c.bufferedRecords, -1) >= c.cfg.producer.maxBufferedRecords {
		go func() { c.waitBuffer <- struct{}{} }()
	}
	pr.promise(topic, pr.r, err)
}
