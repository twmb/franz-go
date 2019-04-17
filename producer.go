package kgo

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/kgo/kerr"
	"github.com/twmb/kgo/kmsg"
)

// func (p *Producer) BeginTransaction() *ProducerTransaction
// func (p *ProducerTransaction) Produce(r *Record)

type producer struct {
	id       int64
	epoch    int16
	idLoaded int64
	idMu     sync.Mutex

	tpsMu sync.RWMutex
	tps   map[string]*topicPartitions

	bufferedRecords int64
	waitBuffer      chan struct{}
}

type topicPartition struct {
	topic     string // our topic
	partition int32  // our partition number

	leader      int32 // our current broker leader
	leaderEpoch int32 // our current broker leader epoch

	toppar topparBuffer

	writable bool

	replicas []int32
	isr      []int32
	offline  []int32
}

type topicPartitions struct {
	loaded  int64
	loadErr error

	mu sync.RWMutex

	// allIDs and writableIDs correspond to the partition IDs in the
	// two slices below. We save the IDs here as well since that is
	// the common want.
	allIDs      []int32
	writableIDs []int32

	all      map[int32]*topicPartition // partition num => partition
	writable map[int32]*topicPartition // partition num => partition, eliding partitions with no leader

	loading chan struct{}
}

func (p *topicPartitions) loadComplete() {
	atomic.StoreInt64(&p.loaded, 1)
	close(p.loading)
}

// newTopicParts creates and returns new topicPartitions
func newTopicParts() *topicPartitions {
	parts := &topicPartitions{
		loading:  make(chan struct{}),
		all:      make(map[int32]*topicPartition),
		writable: make(map[int32]*topicPartition),
	}
	return parts
}

func (c *Client) partitionsForTopicProduce(topic string) (*topicPartitions, error) {
	var retries int
start:
	c.producer.tpsMu.RLock()
	parts, exists := c.producer.tps[topic]
	c.producer.tpsMu.RUnlock()

	if !exists {
		c.producer.tpsMu.Lock()
		parts, exists = c.producer.tps[topic]
		if !exists {
			parts = newTopicParts()
			c.producer.tps[topic] = parts
			go c.fetchTopicMetadataIntoParts(map[string]*topicPartitions{
				topic: parts,
			}, true)
		}
		c.producer.tpsMu.Unlock()
	}

	if atomic.LoadInt64(&parts.loaded) == 0 {
		<-parts.loading
	}

	if parts.loadErr != nil && isRetriableErr(parts.loadErr) {
		c.producer.tpsMu.Lock()
		partsNow := c.producer.tps[topic]
		if partsNow == parts {
			delete(c.producer.tps, topic)
		}
		c.producer.tpsMu.Unlock()

		if retries < 3 { // TODO config opt
			fmt.Println("sleeping 1s before topic partition retry")
			time.Sleep(time.Second)
			goto start
		}
	}

	return parts, parts.loadErr
}

func (c *Client) fetchTopicMetadataIntoParts(fetches map[string]*topicPartitions, forFirstProduce bool) {
	topics := make([]string, 0, len(fetches))
	for topic := range fetches {
		topics = append(topics, topic)
	}

	meta, err := c.fetchMetadata(false, topics...)
	if err != nil {
		for _, part := range fetches {
			part.loadErr = err
			part.loadComplete()
		}
		return
	}

	c.brokersMu.RLock()
	defer c.brokersMu.RUnlock()

	for i := range meta.TopicMetadata {
		t := &meta.TopicMetadata[i]
		parts, exists := fetches[t.Topic]
		if !exists {
			continue // odd...
		}
		delete(fetches, t.Topic)

		parts.loadErr = kerr.ErrorForCode(t.ErrorCode)
		if parts.loadErr != nil {
			parts.loadComplete()
			continue
		}

		for i := range t.PartitionMetadata {
			partMeta := &t.PartitionMetadata[i]

			broker, exists := c.brokers[partMeta.Leader]
			if !exists {
				parts.loadErr = errUnknownBrokerForLeader
				parts.loadComplete()
				continue
			}

			p := &topicPartition{
				topic:     t.Topic,
				partition: partMeta.Partition,

				leader:      partMeta.Leader,
				leaderEpoch: partMeta.LeaderEpoch,

				toppar: topparBuffer{
					idWireLength: 2 + int32(len(t.Topic)) + 4,
					drainer:      broker.bt,
				},

				replicas: partMeta.Replicas,
				isr:      partMeta.ISR,
				offline:  partMeta.OfflineReplicas,
			}
			p.toppar.owner = p

			parts.allIDs = append(parts.allIDs, p.partition)
			parts.all[p.partition] = p

			switch kerr.ErrorForCode(partMeta.ErrorCode) {
			case kerr.LeaderNotAvailable,
				kerr.ListenerNotFound:
				continue
			}

			p.writable = true
			parts.writableIDs = append(parts.writableIDs, p.partition)
			parts.writable[p.partition] = p
		}

		if forFirstProduce && parts.loadErr == nil {
			for _, p := range parts.all {
				p.toppar.drainer.addToppar(&p.toppar)
			}
			parts.loadComplete()
		}
	}

	for _, parts := range fetches {
		parts.loadErr = errNoResp
		parts.loadComplete()
	}
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
	if atomic.AddInt64(&c.producer.bufferedRecords, 1) > c.cfg.producer.maxBufferedRecords {
		<-c.producer.waitBuffer
	}

	if err := c.ensureProducerIDInit(); err != nil {
		return err
	}

	partitions, err := c.partitionsForTopicProduce(r.Topic)
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
	if atomic.LoadInt64(&c.producer.idLoaded) == 1 {
		return nil
	}

	c.producer.idMu.Lock()
	defer c.producer.idMu.Unlock()

	if atomic.LoadInt64(&c.producer.idLoaded) == 1 {
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

	c.producer.id = initResp.ProducerID
	c.producer.epoch = initResp.ProducerEpoch

	atomic.StoreInt64(&c.producer.idLoaded, 1)

	return nil
}

func (c *Client) promise(pr promisedRecord, err error) {
	if atomic.AddInt64(&c.producer.bufferedRecords, -1) >= c.cfg.producer.maxBufferedRecords {
		go func() { c.producer.waitBuffer <- struct{}{} }()
	}
	pr.promise(pr.r, err)
}
