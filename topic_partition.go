package kgo

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/kgo/kerr"
)

type topicPartition struct {
	topic     string // our topic
	partition int32  // our partition number

	leader      int32 // our broker leader
	leaderEpoch int32 // our broker leader epoch

	replicas []int32
	isr      []int32
	offline  []int32

	toppar recordBuffer
}

type topicPartitions struct {
	loaded  int64
	loadErr error

	mu sync.RWMutex

	partitions []int32

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

				toppar: recordBuffer{
					idWireLength: 2 + int32(len(t.Topic)) + 4,
					drainer:      broker.bt,
				},

				replicas: partMeta.Replicas,
				isr:      partMeta.ISR,
				offline:  partMeta.OfflineReplicas,
			}
			p.toppar.owner = p

			parts.partitions = append(parts.partitions, p.partition)
			parts.all[p.partition] = p

			switch kerr.ErrorForCode(partMeta.ErrorCode) {
			case kerr.LeaderNotAvailable,
				kerr.ListenerNotFound:
				continue
			}

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
