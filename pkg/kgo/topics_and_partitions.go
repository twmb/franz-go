package kgo

import (
	"sync"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kerr"
)

// loadTopics returns the client's current topics and their partitions.
func (cl *Client) loadTopics() map[string]*topicPartitions {
	return cl.topics.Load().(map[string]*topicPartitions)
}

// cloneTopics returns a copy of the client's current topics and partitions.
// This is a shallow copy; only the map is copied.
func (cl *Client) cloneTopics() map[string]*topicPartitions {
	old := cl.loadTopics()
	new := make(map[string]*topicPartitions, len(old)+5)
	for topic, partitions := range old {
		new[topic] = partitions
	}
	return new
}

// loadShortTopics returns topic names and a copy of their partition numbers.
func (cl *Client) loadShortTopics() map[string][]int32 {
	topics := cl.loadTopics()
	short := make(map[string][]int32, len(topics))
	for topic, partitions := range topics {
		short[topic] = append([]int32(nil), partitions.load().partitions...)
	}
	return short
}

// updates the stored list of topics if any in the list is not yet stored in
// the client. The input is allowed to have duplicates.
func (cl *Client) storeTopics(topics []string) {
	cl.topicsMu.Lock()
	defer cl.topicsMu.Unlock()

	var cloned bool
	loaded := cl.loadTopics()
	for _, topic := range topics {
		if _, exists := loaded[topic]; !exists {
			if !cloned {
				loaded = cl.cloneTopics()
				cloned = true
			}
			loaded[topic] = newTopicPartitions()
		}
	}
	if cloned {
		cl.topics.Store(loaded)
	}
}

func newTopicPartitions() *topicPartitions {
	parts := new(topicPartitions)
	parts.v.Store(&topicPartitionsData{
		all:      make(map[int32]*topicPartition),
		writable: make(map[int32]*topicPartition),
	})
	return parts
}

// topicPartitions contains all information about a topic's partitions.
type topicPartitions struct {
	v atomic.Value // *topicPartitionsData

	partsMu     sync.Mutex
	partitioner TopicPartitioner
}

func (t *topicPartitions) load() *topicPartitionsData {
	return t.v.Load().(*topicPartitionsData)
}

// Updates the topic partitions data atomic value.
//
// If this is the first time seeing partitions, we do processing of unknown
// partitions that may be buffered for producing.
func (cl *Client) storePartitionsUpdate(topic string, l *topicPartitions, lv *topicPartitionsData, hadPartitions bool) {
	// If the topic already had partitions, then there would be no
	// unknown topic waiting and we do not need to notify anything.
	if hadPartitions {
		l.v.Store(lv)
		return
	}

	cl.unknownTopicsMu.Lock()
	defer cl.unknownTopicsMu.Unlock()

	// If the topic did not have partitions, then we need to store the
	// partition update BEFORE unlocking the mutex.
	//
	// Otherwise, this function would do all the "unwait the waiters"
	// logic, then a produce could see "oh, no partitions, I will wait",
	// then this would store the update and never notify that new waiter.
	//
	// By storing before releasing the lock, we ensure that later partition
	// loads for this topic under the unknownTopicsMu will see our update.
	defer l.v.Store(lv)

	// If there are no unknown topics or this topic is not unknown, then we
	// have nothing to do.
	if len(cl.unknownTopics) == 0 {
		return
	}
	unknown, exists := cl.unknownTopics[topic]
	if !exists {
		return
	}

	// If we loaded no partitions because of a retriable error, we signal
	// the waiting goroutine that a try happened. It is possible the
	// goroutine is quitting and will not be draining unknownWait, so we do
	// not require the send.
	if len(lv.all) == 0 && kerr.IsRetriable(lv.loadErr) {
		select {
		case unknown.wait <- lv.loadErr:
		default:
		}
		return
	}

	// We loaded partitions and there are waiting topics. We delete the
	// topic from the unknown maps, close the unknown wait to kill the
	// waiting goroutine, and partition all records.
	//
	// Note that we have to partition records _or_ finish with error now
	// while under the unknown topics mu to ensure ordering.
	delete(cl.unknownTopics, topic)
	close(unknown.wait)

	if lv.loadErr != nil {
		for _, pr := range unknown.buffered {
			cl.finishRecordPromise(pr, lv.loadErr)
		}
		return
	}
	for _, pr := range unknown.buffered {
		cl.doPartitionRecord(l, lv, pr)
	}
}

// topicPartitionsData is the data behind a topicPartitions' v.
//
// We keep this in an atomic because it is expected to be extremely read heavy,
// and if it were behind a lock, the lock would need to be held for a while.
type topicPartitionsData struct {
	// NOTE if adding anything to this struct, be sure to fix meta merge.
	loadErr            error // could be auth, unknown, leader not avail, or creation err
	isInternal         bool
	partitions         []int32 // TODO does not need to be a slice
	writablePartitions []int32
	all                map[int32]*topicPartition // partition num => partition TODO does not need to be a map, can be slice
	writable           map[int32]*topicPartition // partition num => partition, eliding partitions with no leader / listener
}

// topicPartition contains all information from Kafka for a topic's partition,
// as well as what a client is producing to it or info about consuming from it.
type topicPartition struct {
	// NOTE all of these fields are copied when updating metadata;
	// we copy all fields and keep the new topicPartition pointer.
	leader      int32 // our broker leader
	leaderEpoch int32 // the broker leader's epoch

	loadErr error // could be leader/listener/replica not avail

	records *recBuf
	cursor  *cursor
}

// migrateProductionTo is called on metadata update if a topic partition's sink
// has changed. This moves record production from one sink to the other; this
// must be done such that records produced during migration follow those
// already buffered.
func (old *topicPartition) migrateProductionTo(new *topicPartition) {
	// First, remove our record buffer from the old sink.
	old.records.sink.removeRecBuf(old.records)

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

	old.records.sink.addRecBuf(old.records) // add our record source to the new sink

	// At this point, the new sink will be draining our records. We lastly
	// need to copy the records pointer to our new topicPartition.
	new.records = old.records
}

// migrateCursorTo is called on metadata update if a topic partition's leader
// or leader epoch has changed.
//
// This is a little bit different from above, in that we do this logic only
// after stopping a consumer session. With the consumer session stopped, we
// have fewer concurrency issues to worry about.
func (old *topicPartition) migrateCursorTo(
	new *topicPartition,
	consumer *consumer,
	consumerSessionStopped *bool,
	reloadOffsets *listOrEpochLoads,
) {
	// Migrating a cursor requires stopping any consumer session. If we
	// stop a session, we need to eventually re-start any offset listing or
	// epoch loading that was stopped. Thus, we simply merge what we
	// stopped into what we will reload.
	if !*consumerSessionStopped {
		stoppedListOrEpochLoads := consumer.stopSession()
		reloadOffsets.mergeFrom(stoppedListOrEpochLoads)
		*consumerSessionStopped = true
	}

	old.cursor.source.removeCursor(old.cursor)

	// With the session stopped, we can update fields on the old cursor
	// with no concurrency issue.
	old.cursor.source = new.cursor.source

	// KIP-320: if we had consumed some messages, we need to validate the
	// leader epoch on the new broker to see if we experienced data loss
	// before we can use this cursor.
	if new.leaderEpoch != -1 && old.cursor.lastConsumedEpoch >= 0 {
		// Since the cursor consumed messages, it is definitely usable.
		// We use it so that the epoch load can finish using it
		// properly.
		old.cursor.use()
		reloadOffsets.addLoad(old.cursor.topic, old.cursor.partition, loadTypeEpoch, offsetLoad{
			replica: -1,
			Offset: Offset{
				at:    old.cursor.offset,
				epoch: old.cursor.lastConsumedEpoch,
			},
		})
	}

	old.cursor.leader = new.cursor.leader
	old.cursor.leaderEpoch = new.cursor.leaderEpoch

	old.cursor.source.addCursor(old.cursor)
	new.cursor = old.cursor
}
