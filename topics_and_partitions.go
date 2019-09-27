package kgo

import (
	"sync"
	"sync/atomic"
)

// loadTopics returns the client's current topics and their partitions.
func (c *Client) loadTopics() map[string]*topicPartitions {
	return c.topics.Load().(map[string]*topicPartitions)
}

// cloneTopics returns a copy of the client's current topics and partitions.
// This is a shallow copy; only the map is copied.
func (c *Client) cloneTopics() map[string]*topicPartitions {
	old := c.loadTopics()
	new := make(map[string]*topicPartitions, len(old)+5)
	for topic, partitions := range old {
		new[topic] = partitions
	}
	return new
}

// loadShortTopics returns topic names and a copy of their partition numbers.
func (c *Client) loadShortTopics() map[string][]int32 {
	topics := c.loadTopics()
	short := make(map[string][]int32, len(topics))
	for topic, partitions := range topics {
		short[topic] = append([]int32(nil), partitions.load().partitions...)
	}
	return short
}

func newTopicPartitions() *topicPartitions {
	parts := new(topicPartitions)
	parts.v.Store(&topicPartitionsData{
		all:      make(map[int32]*topicPartition),
		writable: make(map[int32]*topicPartition),
	})
	parts.c = sync.NewCond(parts.mu.RLocker())
	return parts
}

// topicPartitions contains all information about a topic's partitions.
type topicPartitions struct {
	v atomic.Value // *topicPartitionsData

	partsMu     sync.Mutex
	partitioner topicPartitioner

	mu sync.RWMutex
	c  *sync.Cond
}

func (t *topicPartitions) load() *topicPartitionsData {
	return t.v.Load().(*topicPartitionsData)
}

// topicPartitionsData is the data behind a topicPartitions' v.
//
// We keep this in an atomic because it is expected to be extremely read heavy,
// and if it were behind a lock, the lock would need to be held for a while.
type topicPartitionsData struct {
	// NOTE if adding anything to this struct, be sure to fix meta merge.
	loadErr            error // could be auth, unknown, leader not avail, or creation err
	isInternal         bool
	partitions         []int32
	writablePartitions []int32
	all                map[int32]*topicPartition // partition num => partition
	writable           map[int32]*topicPartition // partition num => partition, eliding partitions with no leader / listener
}

// topicPartition contains all information from Kafka for a topic's partition,
// as well as what a client is producing to it or info about consuming from it.
type topicPartition struct {
	topic     string
	partition int32
	loadErr   error // could be leader/listener/replica not avail

	leader      int32 // our broker leader
	leaderEpoch int32 // our broker leader epoch

	replicas []int32
	isr      []int32
	offline  []int32

	records     *recordBuffer
	consumption *consumption
}

// migrateProductionTo is called on metadata update if a topic partition's sink
// has changed. This moves record production from one sink to the other; this
// must be done such that records produced during migration follow those
// already buffered.
func (old *topicPartition) migrateProductionTo(new *topicPartition) {
	// We could be migrating _from_ a nil sink if the original sink had a
	// load error. The new sink will not be nil, since we do not migrate
	// to failing sinks.
	if old.records.sink != nil {
		old.records.sink.removeSource(old.records) // first, remove our record source from the old sink
	}

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

	old.records.sink.addSource(old.records) // add our record source to the new sink

	// At this point, the new sink will be draining our records. We lastly
	// need to copy the records pointer to our new topicPartition and clear
	// its failing state.
	new.records = old.records
}

// migrateConsumptionTo is called on metadata update if a topic partition's
// source has changed. This happens whenever the sink changes, since a source
// is just the counterpart to a sink for the same topic partition.
//
// The pattern is the same as above, albeit no concurrent-produce issues to
// worry about.
func (old *topicPartition) migrateConsumptionTo(new *topicPartition) {
	if old.consumption.source != nil {
		old.consumption.source.removeConsumption(old.consumption)
	}
	old.consumption.mu.Lock()
	old.consumption.source = new.consumption.source
	old.consumption.mu.Unlock()
	old.consumption.source.addConsumption(old.consumption)
	new.consumption = old.consumption
}
