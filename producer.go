package kgo

import (
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
	/*
		produceID := c.produceID.Load().(*string)
		if produceID == nil {
			c.produceIDMu.Lock()
			produceID = c.produceID.Load().(*string)
			if produceID == nil {
				produceID = c.fetchProduceID()
				if produceID != nil {
					return errUnableToFetchProduceID
				}
			}
			c.produceIDMu.Unlock()
		}
	*/

start:
	partitions, err := c.partitionsForTopic(topic)
	if err != nil {
		return err
	}

	ids := partitions.writableIDs
	mapping := partitions.writable
	if c.cfg.producer.partitioner.RequiresConsistency(r) {
		ids = partitions.allIDs
		mapping = partitions.all
	}
	if len(ids) == 0 {
		return errNoPartitionIDs
	}

	idIdx := c.cfg.producer.partitioner.Partition(r, len(ids))
	if idIdx > len(ids) {
		idIdx = len(ids) - 1
	}

	id := ids[idIdx]
	partition, exists := mapping[id]
	if !exists {
		return errUnknownPartition // should never happen
	}

	partitions.mu.RLock()
	if partitions.migrated {
		partitions.mu.RUnlock()
		<-partitions.migrateCh
		goto start
	}

	c.brokersMu.RLock()
	broker, exists := c.brokers[partition.leader]
	c.brokersMu.RUnlock()

	if !exists {
		partitions.mu.RUnlock()
		return errUnknownBrokerForLeader
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

	broker.bt.bufferRecord(
		topic,
		partition.id,
		promisedRecord{
			promise: promise,
			r:       r,
		},
	)
	partitions.mu.RUnlock()
	return nil
}

func (c *Client) initProducerID() {
	broker := c.broker()

	var initResp *kmsg.InitProducerIDResponse
	var retry bool
start:
	broker.wait(
		&kmsg.InitProducerIDRequest{ /*TODO transactional ID */ },
		func(resp kmsg.Response, respErr error) {
			if respErr != nil {
				retry = isRetriableBrokerErr(respErr)
				return
			}
			initResp = resp.(*kmsg.InitProducerIDResponse)
		},
	)

	if retry {
		goto start // TODO limit amount
	}

	// TODO handle initResp err
	if kerr.ErrorForCode(initResp.ErrorCode) != nil {
		panic("TODO")
	}

	c.producerID = initResp.ProducerID
	c.producerEpoch = initResp.ProducerEpoch
}
