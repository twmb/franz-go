package kgo

import (
	"sync"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kerr"
)

func newTopicPartitions() *topicPartitions {
	parts := new(topicPartitions)
	parts.v.Store(new(topicPartitionsData))
	return parts
}

// Contains all information about a topic's partitions.
type topicPartitions struct {
	v atomic.Value // *topicPartitionsData

	partsMu     sync.Mutex
	partitioner TopicPartitioner
}

func (t *topicPartitions) load() *topicPartitionsData { return t.v.Load().(*topicPartitionsData) }

var noTopicsPartitions = newTopicsPartitions()

func newTopicsPartitions() *topicsPartitions {
	var t topicsPartitions
	t.v.Store(make(topicsPartitionsData))
	return &t
}

// A helper type mapping topics to their partitions;
// this is the inner value of topicPartitions.v.
type topicsPartitionsData map[string]*topicPartitions

func (d topicsPartitionsData) hasTopic(t string) bool { _, exists := d[t]; return exists }
func (d topicsPartitionsData) loadTopic(t string) *topicPartitionsData {
	tp, exists := d[t]
	if !exists {
		return nil
	}
	return tp.load()
}

// A helper type mapping topics to their partitions that can be updated
// atomically.
type topicsPartitions struct {
	v atomic.Value // topicsPartitionsData (map[string]*topicPartitions)
}

func (t *topicsPartitions) load() topicsPartitionsData {
	if t == nil {
		return nil
	}
	return t.v.Load().(topicsPartitionsData)
}
func (t *topicsPartitions) storeData(d topicsPartitionsData) { t.v.Store(d) }
func (t *topicsPartitions) storeTopics(topics []string)      { t.v.Store(t.ensureTopics(topics)) }
func (t *topicsPartitions) clone() topicsPartitionsData {
	current := t.load()
	clone := make(map[string]*topicPartitions, len(current))
	for k, v := range current {
		clone[k] = v
	}
	return clone
}

// Ensures that the topics exist in the returned map, but does not store the
// update. This can be used to update the data and store later, rather than
// storing immediately.
func (t *topicsPartitions) ensureTopics(topics []string) topicsPartitionsData {
	var cloned bool
	current := t.load()
	for _, topic := range topics {
		if _, exists := current[topic]; !exists {
			if !cloned {
				current = t.clone()
				cloned = true
			}
			current[topic] = newTopicPartitions()
		}
	}
	return current
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

	p := &cl.producer

	p.unknownTopicsMu.Lock()
	defer p.unknownTopicsMu.Unlock()

	// If the topic did not have partitions, then we need to store the
	// partition update BEFORE unlocking the mutex to guard against this
	// sequence of events:
	//
	//   - unlock waiters
	//   - delete waiter
	//   - new produce recreates waiter
	//   - we store update
	//   - we never notify the recreated waiter
	//
	// By storing before releasing the locks, we ensure that later
	// partition loads for this topic under the mu will see our update.
	defer l.v.Store(lv)

	// If there are no unknown topics or this topic is not unknown, then we
	// have nothing to do.
	if len(p.unknownTopics) == 0 {
		return
	}
	unknown, exists := p.unknownTopics[topic]
	if !exists {
		return
	}

	// If we loaded no partitions because of a retriable error, we signal
	// the waiting goroutine that a try happened. It is possible the
	// goroutine is quitting and will not be draining unknownWait, so we do
	// not require the send.
	if len(lv.partitions) == 0 && kerr.IsRetriable(lv.loadErr) {
		select {
		case unknown.wait <- lv.loadErr:
		default:
		}
		return
	}

	// Either we have a fatal error or we can successfully partition.
	//
	// Even with a fatal error, if we loaded any partitions, we partition.
	// If we only had a fatal error, we can finish promises in a goroutine.
	// If we are partitioning, we have to do it under the unknownMu to
	// ensure prior buffered records are produced in order before we
	// release the mu.
	delete(p.unknownTopics, topic)
	close(unknown.wait) // allow waiting goroutine to quit

	if len(lv.partitions) == 0 {
		cl.failUnknownTopicRecords(topic, unknown, lv.loadErr)
	} else {
		for _, pr := range unknown.buffered {
			cl.doPartitionRecord(l, lv, pr)
		}
	}

}

// If a metadata request fails after retrying (internally retrying, so only a
// few times), or the metadata request does not return topics that we requested
// (which may also happen additionally consuming via regex), then we need to
// bump errors for topics that were previously loaded, and bump errors for
// topics awaiting load.
//
// This has two modes of operation:
//
//   1) if no topics were missing, then the metadata request failed outright,
//      and we need to bump errors on all stored topics and unknown topics.
//
//   2) if topics were missing, then the metadata request was successful but
//      had missing data, and we need to bump errors on only what was mising.
//
func (cl *Client) bumpMetadataFailForTopics(requested map[string]*topicPartitions, err error, missingTopics ...string) {
	p := &cl.producer

	// mode 1
	if len(missingTopics) == 0 {
		for _, topic := range requested {
			for _, topicPartition := range topic.load().partitions {
				topicPartition.records.bumpRepeatedLoadErr(err)
			}
		}
	}

	// mode 2
	var missing map[string]bool
	for _, failTopic := range missingTopics {
		if missing == nil {
			missing = make(map[string]bool, len(missingTopics))
		}
		missing[failTopic] = true

		if topic, exists := requested[failTopic]; exists {
			for _, topicPartition := range topic.load().partitions {
				topicPartition.records.bumpRepeatedLoadErr(err)
			}
		}
	}

	p.unknownTopicsMu.Lock()
	defer p.unknownTopicsMu.Unlock()

	for topic, unknown := range p.unknownTopics {
		// if nil, mode 1, else mode 2
		if missing != nil && !missing[topic] {
			continue
		}

		select {
		case unknown.wait <- err:
		default:
		}
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
	partitions         []*topicPartition // partition num => partition
	writablePartitions []*topicPartition // subset of above
}

// topicPartition contains all information from Kafka for a topic's partition,
// as well as what a client is producing to it or info about consuming from it.
type topicPartition struct {
	// If we have a load error (leader/listener/replica not available), we
	// keep the old topicPartition data and the new error.
	loadErr error

	// If we do not have a load error, we determine if the new
	// topicPartition is the same or different from the old based on
	// whether the data changed (leader or leader epoch, etc.).
	topicPartitionData

	// If we do not have a load error, we copy the records and cursor
	// pointers from the old after updating any necessary fields in them
	// (see migrate functions below).
	//
	// Only one of records or cursor is non-nil.
	records *recBuf
	cursor  *cursor
}

// Contains stuff that changes on metadata update that we copy into a cursor or
// recBuf.
type topicPartitionData struct {
	// Our leader; if metadata sees this change, the metadata update
	// migrates the cursor to a different source with the session stopped,
	// and the recBuf to a different sink under a tight mutex.
	leader int32

	// What we believe to be the epoch of the leader for this partition.
	//
	// For cursors, for KIP-320, if a broker receives a fetch request where
	// the current leader epoch does not match the brokers, either the
	// broker is behind and returns UnknownLeaderEpoch, or we are behind
	// and the broker returns FencedLeaderEpoch. For the former, we back
	// off and retry. For the latter, we update our metadata.
	leaderEpoch int32
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

	old.records.mu.Lock() // guard setting sink and topic partition data
	old.records.sink = new.records.sink
	old.records.topicPartitionData = new.topicPartitionData
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
	tpsPrior **topicsPartitions,
) {
	// Migrating a cursor requires stopping any consumer session. If we
	// stop a session, we need to eventually re-start any offset listing or
	// epoch loading that was stopped. Thus, we simply merge what we
	// stopped into what we will reload.
	if !*consumerSessionStopped {
		loads, tps := consumer.stopSession()
		reloadOffsets.mergeFrom(loads)
		*tpsPrior = tps
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

	old.cursor.topicPartitionData = new.topicPartitionData

	old.cursor.source.addCursor(old.cursor)
	new.cursor = old.cursor
}
