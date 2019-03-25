package kgo

// func (p *Producer) BeginTransaction() *ProducerTransaction
// func (p *ProducerTransaction) Produce(r *Record)

func (c *Client) Produce(
	topic string,
	r *Record,
	callback func(string, *Record),
) error {
	var partitions []int32
	var err error
	if c.cfg.producer.partitioner.RequiresConsistency(r) {
		partitions, err = c.allPartitionsFor(topic)
	} else {
		partitions, err = c.writablePartitionsFor(topic)
	}
	if err != nil {
		return err
	}
	partition := c.cfg.producer.partitioner.Partition(r, len(partitions))
	_ = partition
	// if partition not in partitions
	//broker := p.cl.findBrokerLeader(partition)
	//_ = broker
	// TODO KIP-359: if broker LeaderEpoch known, set it in produce request
	// and handle response errors
	return nil
}

// Server: all requests are responded to in order; we can rely on that.
