package kgo

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/kafka-go/pkg/kerr"
	"github.com/twmb/kafka-go/pkg/kmsg"
)

// TODO KIP-359 leader epoch in produce request when it is released

// func (p *Producer) BeginTransaction() *ProducerTransaction
// func (p *ProducerTransaction) Produce(r *Record)

type producer struct {
	bufferedRecords int64

	id         int64
	epoch      int16
	idLoaded   int32
	idMu       sync.Mutex
	idLoading  bool
	waitBuffer chan struct{}

	flushing     int32
	flushingMu   sync.Mutex
	flushingCond *sync.Cond
}

func noPromise(*Record, error) {}

// Produce sends a Kafka record to the topic in the record's Topic field,
// calling promise with the record or an error when Kafka replies.
//
// The promise is optional, but not using it means you will not know if Kafka
// recorded a record properly.
//
// If the record is too large, this will return an error.  For simplicity, this
// function considers messages too large if they are within 512 bytes of the
// record batch byte limit. This may be made more precise in the future if
// necessary.
//
// The context is used if the client currently currently has the max amount of
// buffered records. If so, the client waits for some records to complete or
// for the context or client to quit.
//
// The first buffered record for an unknown topic begins a timeout for the
// configured record timeout limit; all records buffered within the wait will
// expire with the same timeout if the topic does not load in time. For
// simplicity, any time spent waiting for the topic to load is not persisted
// through once the topic loads, meaning the record may further wait once
// buffered. This may be changed in the future if necessary, however, the only
// reason for a topic to not load promptly is if it does not exist.
func (c *Client) Produce(
	ctx context.Context,
	r *Record,
	promise func(*Record, error),
) error {
	if len(r.Key)+len(r.Value) > int(c.cfg.producer.maxRecordBatchBytes)-512 {
		return kerr.MessageTooLarge
	}

	if atomic.AddInt64(&c.producer.bufferedRecords, 1) > c.cfg.producer.maxBufferedRecords {
		select {
		case <-c.producer.waitBuffer:
		case <-c.ctx.Done():
			return c.ctx.Err()
		case <-ctx.Done():
			return ctx.Err()
		}
	}

	if promise == nil {
		promise = noPromise
	}
	pr := promisedRecord{promise, r}

	if atomic.LoadInt32(&c.producer.idLoaded) == 0 {
		var buffered bool
		c.producer.idMu.Lock()
		if atomic.LoadInt32(&c.producer.idLoaded) == 0 {
			// unknownTopics is guarded under either the
			// unknownTopicsMu or producer idMu. Since
			// we must have an ID before we move to the
			// loading topics stage, we will always have
			// producer idMu and then unknownTopicsMu
			// non overlapping.
			c.addUnknownTopicRecord(pr)
			buffered = true

			if !c.producer.idLoading {
				c.producer.idLoading = true
				go c.initIdempotentID()
			}
		}
		c.producer.idMu.Unlock()
		if buffered {
			return nil
		}
	}
	c.partitionRecord(pr)
	return nil
}

func (c *Client) finishRecordPromise(pr promisedRecord, err error) {
	buffered := atomic.AddInt64(&c.producer.bufferedRecords, -1)
	if buffered >= c.cfg.producer.maxBufferedRecords {
		go func() { c.producer.waitBuffer <- struct{}{} }()
	} else if buffered == 0 && atomic.LoadInt32(&c.producer.flushing) > 0 {
		c.producer.flushingMu.Lock()
		c.producer.flushingMu.Unlock()
		c.producer.flushingCond.Broadcast()
	}
	pr.promise(pr.Record, err)
}

// partitionRecord loads the partitions for a topic and produce to them. If
// the topic does not currently exist, the record is buffered in unknownTopics
// for a metadata update to deal with.
func (c *Client) partitionRecord(pr promisedRecord) {
	parts, partsData := c.partitionsForTopicProduce(pr)
	if parts == nil {
		return
	}
	c.doPartitionRecord(parts, partsData, pr)
}

// doPartitionRecord is the logic behind record partitioning and producing if
// the client knows of the topic's partitions.
func (c *Client) doPartitionRecord(parts *topicPartitions, partsData *topicPartitionsData, pr promisedRecord) {
	if partsData.loadErr != nil && !kerr.IsRetriable(partsData.loadErr) {
		c.finishRecordPromise(pr, partsData.loadErr)
		return
	}

	parts.partsMu.Lock()
	defer parts.partsMu.Unlock()
	if parts.partitioner == nil {
		parts.partitioner = c.cfg.producer.partitioner.forTopic(pr.Topic)
	}

	mapping := partsData.writable
	possibilities := partsData.writablePartitions
	if parts.partitioner.requiresConsistency(pr.Record) {
		mapping = partsData.all
		possibilities = partsData.partitions
	}
	if len(possibilities) == 0 {
		c.finishRecordPromise(pr, ErrNoPartitionsAvailable)
		return
	}

	idIdx := parts.partitioner.partition(pr.Record, len(possibilities))
	id := possibilities[idIdx]
	partition := mapping[id]

	appended := partition.records.bufferRecord(pr, true) // KIP-480
	if !appended {
		parts.partitioner.onNewBatch()
		idIdx = parts.partitioner.partition(pr.Record, len(possibilities))
		id = possibilities[idIdx]
		partition = mapping[id]
		partition.records.bufferRecord(pr, false) // KIP-480
	}
}

// initIdempotentID initalizes the client's producer ID for idempotent
// producing only (no transactions, which are more special). After the first
// load, this clears all buffered unknown topics.
func (c *Client) initIdempotentID() {
	err := c.doInitIdempotentID()

	// If we were successful, to ensure ordering with concurrent produces,
	// we must store idLoaded as the very last thing.
	if err == nil {
		defer atomic.StoreInt32(&c.producer.idLoaded, 1)
	}

	// Grab our lock. We need to block producing until this function
	// returns to ensure order.
	c.producer.idMu.Lock()
	defer c.producer.idMu.Unlock()

	c.producer.idLoading = false

	unknown := c.unknownTopics
	unknownWait := c.unknownTopicsWait
	c.unknownTopics = make(map[string][]promisedRecord)
	c.unknownTopicsWait = make(map[string]chan struct{})

	if err != nil {
		for i, prs := range unknown {
			close(unknownWait[i])
			for _, pr := range prs {
				c.finishRecordPromise(pr, err)
			}
		}
		return
	}
	for i, prs := range unknown {
		close(unknownWait[i])
		for _, pr := range prs {
			c.partitionRecord(pr)
		}
	}
}

// doInitIdempotentID is used to initialize the idempotent producer ID only. If
// we are using transactions, more logic happens close to producing.
func (c *Client) doInitIdempotentID() error {
	kresp, err := c.Request(c.ctx, new(kmsg.InitProducerIDRequest))
	if err != nil {
		// If our broker is too old, then well...
		//
		// Note this is dependent on the first broker we hit;
		// there are other areas in this client where we assume
		// what we hit first is the default.
		if err == ErrUnknownRequestKey {
			return nil
		}
		return err
	}
	resp := kresp.(*kmsg.InitProducerIDResponse)
	c.producer.id = resp.ProducerID
	c.producer.epoch = resp.ProducerEpoch // should always be 0
	return nil
}

// partitionsForTopicProduce returns the topic partitions for a record.
// If the topic is not loaded yet, this buffers the record.
func (c *Client) partitionsForTopicProduce(pr promisedRecord) (*topicPartitions, *topicPartitionsData) {
	topic := pr.Topic

	// 1) if the topic exists and there are partitions, then we can simply
	// return the parts.
	topics := c.loadTopics()
	parts, exists := topics[topic]
	if exists {
		v := parts.load()
		if len(v.partitions) > 0 {
			return parts, v
		}
	}

	if !exists {
		// 2) if the topic does not exist, we check again under the
		// topics mu.
		c.topicsMu.Lock()
		topics = c.loadTopics()
		if _, exists = topics[topic]; !exists {
			// 2a) the topic definitely does not exist; we create it.
			//
			// Before we release the topic mu, we lock unknownTopics
			// and add our record to ensure ordering.
			parts = newTopicPartitions(topic)
			newTopics := c.cloneTopics()
			newTopics[topic] = parts
			c.topics.Store(newTopics)

			c.unknownTopicsMu.Lock()
			c.topicsMu.Unlock()
			c.addUnknownTopicRecord(pr)
			c.unknownTopicsMu.Unlock()

		} else {
			// 2b) the topic exists now; exists is true and we fall
			// into the logic below.
			c.topicsMu.Unlock()
		}
	}

	if exists {
		// 3) if the topic does exist, either partitions were loaded,
		// meaning we can just return the load since we are guaranteed
		// (single goroutine) sequential now, or they were not loaded
		// and we must add our record in order under unknownTopicsMu.
		c.unknownTopicsMu.Lock()
		topics = c.loadTopics()
		parts = topics[topic]
		v := parts.load()
		if len(v.partitions) > 0 {
			c.unknownTopicsMu.Unlock()
			return parts, v
		}
		c.addUnknownTopicRecord(pr)
		c.unknownTopicsMu.Unlock()
	}

	c.triggerUpdateMetadataNow()

	// Our record is buffered waiting for a metadata update to discover
	// the topic. We return nil here.
	return nil, nil
}

func (c *Client) addUnknownTopicRecord(pr promisedRecord) {
	existing := c.unknownTopics[pr.Topic]
	existing = append(existing, pr)
	c.unknownTopics[pr.Topic] = existing
	if len(existing) != 1 {
		return
	}

	wait := make(chan struct{}, 1)
	c.unknownTopicsWait[pr.Topic] = wait
	go c.waitUnknownTopic(pr.Topic, wait)
}

func (c *Client) waitUnknownTopic(topic string, wait chan struct{}) {
	var after <-chan time.Time
	if timeout := c.cfg.producer.recordTimeout; timeout > 0 {
		timer := time.NewTimer(c.cfg.producer.recordTimeout)
		defer timer.Stop()
		after = timer.C
	}
	var tries int
	var err error
	for err == nil {
		select {
		case <-c.ctx.Done():
			err = ErrBrokerDead
		case <-after:
			err = ErrRecordTimeout
		case _, ok := <-wait:
			if !ok {
				return // metadata was successful!
			}
			tries++
			if tries >= c.cfg.client.retries {
				err = ErrNoPartitionsAvailable
			}
		}
	}

	// We only get down here if we errored above. Clear everything waiting
	// and call the promises with our error.
	c.unknownTopicsMu.Lock()
	prs, ok := c.unknownTopics[topic]
	delete(c.unknownTopics, topic)
	delete(c.unknownTopicsWait, topic)
	c.unknownTopicsMu.Unlock()

	// We could have raced with a metadata update successfully clearing the
	// partitions.
	if ok {
		for _, pr := range prs {
			c.finishRecordPromise(pr, err)
		}
	}
}

// Flush hangs waiting for all buffered records to be flushed, stopping all
// lingers if necessary.
//
// If the context finishes (Done), this returns the context's error.
func (cl *Client) Flush(ctx context.Context) error {
	// Signal to finishRecord that we want to be notified once thins hit 0.
	atomic.AddInt32(&cl.producer.flushing, 1)
	defer atomic.AddInt32(&cl.producer.flushing, -1)

	// At this point, if lingering,
	// nothing new will fall into a timer waiting,
	// so we can just wake everything up through the lock,
	// and be sure that all sinks will loop draining.
	if cl.cfg.producer.linger > 0 {
		for _, parts := range cl.loadTopics() {
			for _, part := range parts.load().all {
				part.records.mu.Lock()
				part.records.lockedStopLinger()
				part.records.sink.maybeBeginDraining()
				part.records.mu.Unlock()
			}
		}
	}

	quit := false
	done := make(chan struct{})
	go func() {
		cl.producer.flushingMu.Lock()
		defer cl.producer.flushingMu.Unlock()
		defer close(done)

		for !quit && atomic.LoadInt64(&cl.producer.bufferedRecords) > 0 {
			cl.producer.flushingCond.Wait()
		}
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		quit = true
		cl.producer.flushingCond.Broadcast()
		return ctx.Err()
	}
}
