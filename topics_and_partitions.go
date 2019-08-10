package kgo

import (
	"sync"
	"sync/atomic"
)

func (c *Client) loadTopics() map[string]*topicPartitions {
	return c.topics.Load().(map[string]*topicPartitions)
}

func (c *Client) cloneTopics() map[string]*topicPartitions {
	old := c.loadTopics()
	new := make(map[string]*topicPartitions, len(old)+5)
	for topic, topicPartition := range old {
		new[topic] = topicPartition
	}
	return new
}

// topicPartition contains all information from Kafka for a topic's partition,
// as well as all records waiting to be sent to it.
type topicPartition struct {
	topic     string
	partition int32
	loadErr   error // could be leader/listener/replica not avail

	leader      int32 // our broker leader
	leaderEpoch int32 // our broker leader epoch

	replicas []int32
	isr      []int32
	offline  []int32

	records     *records
	consumption *consumption
}

func (old *topicPartition) migrateProductionTo(new *topicPartition) {
	// If the new sink is different, we have to migrate any
	// buffered records safely and we must do it such that new
	// records being produced during this migration will still be
	// sent after buffered records, not before.

	// First, we remove the records from the old source.
	old.records.sink.removeSource(old.records)

	// Before this next lock, record producing will buffer to the
	// in-migration-progress records and may trigger draining to
	// the old sink. That is fine, the old sink no longer consumes
	// from these records. We just have wasted drain triggers.
	old.records.mu.Lock() // guard setting sink
	old.records.sink = new.records.sink
	old.records.mu.Unlock()

	// After the unlock above, record buffering can trigger drains
	// on the new sink, which is not yet consuming from these
	// records. Again, just more wasted drain triggers.

	// Once we add this source to the new sink (through the old
	// topic partitions pointers), the new sink will begin
	// draining our records.
	old.records.sink.addSource(old.records)

	// Finally, copy the records pointer to our new topic
	// partitions, which will be atomically stored on defer.
	new.records = old.records
}

func (old *topicPartition) migrateConsumptionTo(new *topicPartition) {
	// This follows much the same pattern as before, but we have fewer
	// subtle issues around needing to be careful here.
	old.consumption.source.removeConsumption(old.consumption)
	old.consumption.mu.Lock()
	old.consumption.source = new.consumption.source
	old.consumption.mu.Unlock()
	old.consumption.source.addConsumption(old.consumption)
}

// topicPartitionsData is the data behind a topicPartitions' v.
//
// We keep this in an atomic because it is expected to be extremely read heavy,
// and if it were behind a lock, the lock would need holding for a good chunk
// of code.
type topicPartitionsData struct {
	loadErr    error // could be auth, unknown, leader not avail, or creation err
	partitions []int32
	all        map[int32]*topicPartition // partition num => partition
	writable   map[int32]*topicPartition // partition num => partition, eliding partitions with no leader / listener
}

// topicPartitions contains all information about a topic's partitions.
type topicPartitions struct {
	v atomic.Value // *topicPartitionsData

	mu sync.RWMutex
	c  *sync.Cond
}

func (t *topicPartitions) load() *topicPartitionsData {
	return t.v.Load().(*topicPartitionsData)
}

// newTopicParts creates and returns new topicPartitions
func newTopicParts() *topicPartitions {
	parts := new(topicPartitions)
	parts.v.Store(&topicPartitionsData{
		all:      make(map[int32]*topicPartition),
		writable: make(map[int32]*topicPartition),
	})
	parts.c = sync.NewCond(parts.mu.RLocker())
	return parts
}
