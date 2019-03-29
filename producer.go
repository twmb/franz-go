package kgo

import "time"

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

	c.brokersMu.RLock()
	broker, exists := c.brokers[partition.leader]
	c.brokersMu.RUnlock()

	if !exists {
		return errUnknownBrokerForLeader
	}

	if r.Timestamp.IsZero() {
		r.Timestamp = time.Now()
	}

	if promise == nil {
		promise = noPromise
	}

	return broker.bufferedReq.buffer(
		topic,
		partition.id,
		promisedRecord{
			promise: promise,
			r:       r,
		},
	)
}
