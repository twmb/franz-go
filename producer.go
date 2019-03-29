package kgo

import "time"

// func (p *Producer) BeginTransaction() *ProducerTransaction
// func (p *ProducerTransaction) Produce(r *Record)

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

	return broker.bufferedReq.buffer(
		topic,
		partition.id,
		promisedRecord{
			promise: promise,
			r:       r,
		},
	)
}
