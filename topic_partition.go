package kgo

import (
	"sync"

	"github.com/twmb/kgo/kerr"
)

type topicPartition struct {
	topic     string // our topic
	partition int32  // our partition number
	loadErr   error  // leader/listener/replica not avail

	leader      int32 // our broker leader
	leaderEpoch int32 // our broker leader epoch

	replicas []int32
	isr      []int32
	offline  []int32

	records *records
}

// TODO convert topicPartitions loadErr/partitions/all/writable to atomic

type topicPartitions struct {
	mu      sync.RWMutex
	loadErr error // auth, unknown, leader not avail, or creation err

	partitions []int32

	all      map[int32]*topicPartition // partition num => partition
	writable map[int32]*topicPartition // partition num => partition, eliding partitions with no leader / listener

	seq uint64
	c   *sync.Cond
}

// newTopicParts creates and returns new topicPartitions
func newTopicParts() *topicPartitions {
	parts := &topicPartitions{
		all:      make(map[int32]*topicPartition),
		writable: make(map[int32]*topicPartition),
	}
	parts.c = sync.NewCond(parts.mu.RLocker())
	return parts
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
