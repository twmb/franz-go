package kgo

import (
	"context"
	"sync"
	"time"

	"github.com/twmb/kafka-go/pkg/kerr"
)

type metawait struct {
	mu         sync.Mutex
	c          *sync.Cond
	lastUpdate time.Time
}

func (m *metawait) init() { m.c = sync.NewCond(&m.mu) }
func (m *metawait) signal() {
	m.mu.Lock()
	m.lastUpdate = time.Now()
	m.mu.Unlock()
	m.c.Broadcast()
}

// waitmeta returns immediately if metadata was updated within the last second,
// otherwise this waits for up to wait for a metadata update to complete.
func (cl *Client) waitmeta(ctx context.Context, wait time.Duration) {
	now := time.Now()

	cl.metawait.mu.Lock()
	if now.Sub(cl.metawait.lastUpdate) < time.Second {
		cl.metawait.mu.Unlock()
		return
	}
	cl.metawait.mu.Unlock()

	cl.triggerUpdateMetadataNow()

	quit := false
	done := make(chan struct{})
	timeout := time.NewTimer(wait)
	defer timeout.Stop()

	go func() {
		defer close(done)
		cl.metawait.mu.Lock()
		defer cl.metawait.mu.Unlock()

		for !quit {
			if now.Sub(cl.metawait.lastUpdate) < time.Second {
				return
			}
			cl.metawait.c.Wait()
		}
	}()

	select {
	case <-done:
		return
	case <-timeout.C:
	case <-ctx.Done():
	case <-cl.ctx.Done():
	}

	cl.metawait.mu.Lock()
	quit = true
	cl.metawait.mu.Unlock()
	cl.metawait.c.Broadcast()
}

func (cl *Client) triggerUpdateMetadata() {
	select {
	case cl.updateMetadataCh <- struct{}{}:
	default:
	}
}

func (cl *Client) triggerUpdateMetadataNow() {
	select {
	case cl.updateMetadataNowCh <- struct{}{}:
	default:
	}
}

// updateMetadataLoop updates metadata whenever the update ticker ticks,
// or whenever deliberately triggered.
func (cl *Client) updateMetadataLoop() {
	defer close(cl.metadone)
	var consecutiveErrors int
	var lastAt time.Time

	ticker := time.NewTicker(cl.cfg.metadataMaxAge)
	defer ticker.Stop()
	for {
		var now bool
		select {
		case <-cl.ctx.Done():
			return
		case <-ticker.C:
		case <-cl.updateMetadataCh:
		case <-cl.updateMetadataNowCh:
			now = true
		}

		var nowTries int
	start:
		nowTries++
		if !now {
			if wait := cl.cfg.metadataMinAge - time.Since(lastAt); wait > 0 {
				timer := time.NewTimer(wait)
				select {
				case <-cl.ctx.Done():
					timer.Stop()
					return
				case <-cl.updateMetadataNowCh:
					timer.Stop()
				case <-timer.C:
				}
			}
		} else {
			// Even with an "update now", we sleep just a bit to allow some
			// potential pile on now triggers.
			time.Sleep(50 * time.Millisecond)
		}

		// Drain any refires that occured during our waiting.
		select {
		case <-cl.updateMetadataCh:
		default:
		}
		select {
		case <-cl.updateMetadataNowCh:
		default:
		}

		again, err := cl.updateMetadata()
		if again || err != nil {
			if now && nowTries < 10 {
				goto start
			}
			cl.triggerUpdateMetadata()
		}
		if err == nil {
			lastAt = time.Now()
			consecutiveErrors = 0
			continue
		}

		consecutiveErrors++
		after := time.NewTimer(cl.cfg.retryBackoff(consecutiveErrors))
		select {
		case <-cl.ctx.Done():
			after.Stop()
			return
		case <-after.C:
		}

	}
}

// updateMetadata updates all of a client's topic's metadata, returning whether
// a new update needs scheduling or if an error occured.
//
// If any topics or partitions have an error, all record buffers in the topic,
// or the record buffer for each erroring partition, has the first batch's
// try count bumped by one.
func (cl *Client) updateMetadata() (needsRetry bool, err error) {
	defer cl.metawait.signal()

	topics := cl.loadTopics()
	toUpdate := make([]string, 0, len(topics))
	for topic := range topics {
		toUpdate = append(toUpdate, topic)
	}

	meta, all, err := cl.fetchTopicMetadata(toUpdate)
	if err != nil {
		return true, err
	}

	// If we are consuming with regex and thus fetched all topics, the
	// metadata may have returned topics we are not yet tracking.
	// We have to add those topics to our topics map so that we can
	// save their information in the merge just below.
	if all {
		var hasNew bool
		cl.topicsMu.Lock()
		topics = cl.loadTopics()
		for topic := range meta {
			if _, exists := topics[topic]; !exists {
				hasNew = true
				break
			}
		}
		if hasNew {
			topics = cl.cloneTopics()
			for topic := range meta {
				if _, exists := topics[topic]; !exists {
					topics[topic] = newTopicPartitions(topic)
				}
			}
			cl.topics.Store(topics)
		}
		cl.topicsMu.Unlock()
	}

	// Merge the producer side of the update.
	for topic, oldParts := range topics {
		newParts, exists := meta[topic]
		if !exists {
			continue
		}
		needsRetry = cl.mergeTopicPartitions(oldParts, newParts) || needsRetry
	}

	// Trigger any consumer updates.
	cl.consumer.doOnMetadataUpdate()

	return needsRetry, nil
}

// fetchTopicMetadata fetches metadata for all reqTopics and returns new
// topicPartitionsData for each topic.
func (cl *Client) fetchTopicMetadata(reqTopics []string) (map[string]*topicPartitionsData, bool, error) {
	cl.consumer.mu.Lock()
	all := cl.consumer.typ == consumerTypeDirect && cl.consumer.direct.regexTopics ||
		cl.consumer.typ == consumerTypeGroup && cl.consumer.group.regexTopics
	cl.consumer.mu.Unlock()
	meta, err := cl.fetchMetadata(cl.ctx, all, reqTopics)
	if err != nil {
		return nil, all, err
	}

	topics := make(map[string]*topicPartitionsData, len(reqTopics))

	cl.brokersMu.RLock()
	defer cl.brokersMu.RUnlock()

	for i := range meta.Topics {
		topicMeta := &meta.Topics[i]

		parts := &topicPartitionsData{
			loadErr:    kerr.ErrorForCode(topicMeta.ErrorCode),
			isInternal: topicMeta.IsInternal,
			all:        make(map[int32]*topicPartition, len(topicMeta.Partitions)),
			writable:   make(map[int32]*topicPartition, len(topicMeta.Partitions)),
		}
		topics[topicMeta.Topic] = parts

		if parts.loadErr != nil {
			continue
		}

		for i := range topicMeta.Partitions {
			partMeta := &topicMeta.Partitions[i]
			leaderEpoch := partMeta.LeaderEpoch
			if meta.Version < 7 {
				leaderEpoch = -1
			}

			p := &topicPartition{
				loadErr: kerr.ErrorForCode(partMeta.ErrorCode),

				leader:      partMeta.Leader,
				leaderEpoch: leaderEpoch,

				records: &recBuf{
					cfg: &cl.cfg,
					cl:  cl,

					topic:     topicMeta.Topic,
					partition: partMeta.Partition,

					recBufsIdx: -1, // required, see below
				},

				cursor: &cursor{
					topic:     topicMeta.Topic,
					partition: partMeta.Partition,

					keepControl: cl.cfg.keepControl,

					allCursorsIdx: -1, // same, see below
					seqOffset: seqOffset{
						offset:             -1, // required to not consume until needed
						currentLeaderEpoch: leaderEpoch,
						lastConsumedEpoch:  -1, // required sentinel
					},
				},
			}

			broker, exists := cl.brokers[p.leader]
			if !exists {
				if p.loadErr == nil {
					p.loadErr = &errUnknownBrokerForPartition{topicMeta.Topic, partMeta.Partition, p.leader}
				}
			} else {
				p.records.sink = broker.sink
				p.cursor.source = broker.source
			}

			parts.partitions = append(parts.partitions, partMeta.Partition)
			parts.all[partMeta.Partition] = p
			if p.loadErr == nil {
				parts.writablePartitions = append(parts.writablePartitions, partMeta.Partition)
				parts.writable[partMeta.Partition] = p
			}
		}
	}

	return topics, all, nil
}

// mergeTopicPartitions merges a new topicPartition into an old and returns
// whether the metadata update that caused this merge needs to be retried.
//
// Retries are necessary if the topic or any partition has a retriable error.
func (cl *Client) mergeTopicPartitions(l *topicPartitions, r *topicPartitionsData) (needsRetry bool) {
	lv := *l.load() // copy so our field writes do not collide with reads
	hadPartitions := len(lv.all) != 0
	defer func() { cl.storePartitionsUpdate(l, &lv, hadPartitions) }()

	lv.loadErr = r.loadErr
	lv.isInternal = r.isInternal
	if r.loadErr != nil {
		retriable := kerr.IsRetriable(r.loadErr)
		if retriable {
			for _, topicPartition := range lv.all {
				topicPartition.records.bumpRepeatedLoadErr(lv.loadErr)
			}
		} else {
			for _, topicPartition := range lv.all {
				topicPartition.records.failAllRecords(lv.loadErr)
			}
		}
		return retriable
	}

	lv.partitions = r.partitions
	lv.writablePartitions = r.writablePartitions

	// We should have no deleted partitions, but there are two cases where
	// we could.
	//
	// 1) an admin added partitions, we saw, then we re-fetched metadata
	//    from an out of date broker that did not have the new partitions
	// 2) a topic was deleted and recreated with fewer partitions
	//
	// Both of these scenarios should be rare to non-existent. If we see a
	// delete partition, we remove it from sinks / sources and error all
	// buffered records for it. This isn't the best behavior in the first
	// scenario, but it isn't showstopping. The new broker will eventually
	// see the new partitions and we will eventually pick them up. In the
	// latter, we avoid trying to forever produce to a partition that truly
	// does no longer exist.
	var deleted []*topicPartition

	// Migrating topicPartitions is a little tricky because we have to
	// worry about map contents.
	//
	// We update everything appropriately in the new r.all, and after
	// this loop we copy the updated map to lv.all (which is stored
	// atomically after the defer above).
	for part, oldTP := range lv.all {
		newTP, exists := r.all[part]
		if !exists {
			// Individual partitions cannot be deleted, so if this
			// partition does not exist anymore, either the topic
			// was deleted and recreated, which we do not handle
			// yet (and cannot on most Kafka's), or the broker we
			// fetched metadata from is out of date.
			deleted = append(deleted, oldTP)
			continue
		}

		if newTP.loadErr != nil { // partition errors should generally be temporary
			err := newTP.loadErr
			*newTP = *oldTP
			newTP.loadErr = err
			newTP.records.bumpRepeatedLoadErr(newTP.loadErr)
			needsRetry = true
			continue
		}

		// Update the old's leader epoch before we do any pointer
		// copying. Our epoch should not go backwards, but just in
		// case, we can guard against it.
		if newTP.leaderEpoch < oldTP.leaderEpoch {
			continue
		}

		// If the new sink is the same as the old, we simply copy over
		// the records pointer and maybe begin draining again.
		// Same logic for the cursor.
		if newTP.records.sink == oldTP.records.sink {
			newTP.records = oldTP.records
			newTP.cursor = oldTP.cursor
		} else {
			oldTP.migrateProductionTo(newTP)
			oldTP.migrateCursorTo(newTP)
		}
		newTP.records.clearFailing()
		newTP.cursor.clearFailing()
	}

	// Anything left with a negative allPartsRecsIdx is a new topic
	// partition. We use this to add the new tp's records to its sink.
	// Same reasoning applies to the cursor offset.
	for _, newTP := range r.all {
		// If the partition has a load error, even if it is new, we
		// can't do anything with it now. Its record sink and source
		// cursor will be nil.
		if newTP.loadErr != nil {
			continue
		}
		if newTP.records.recBufsIdx == -1 {
			newTP.records.sink.addRecBuf(newTP.records)
		}
		if newTP.cursor.allCursorsIdx == -1 { // should be true if recBufsIdx == -1
			newTP.cursor.source.addCursor(newTP.cursor)
		}
	}

	lv.all = r.all
	lv.writable = r.writable

	// Handle anything deleted. We do this serially so that something can't
	// re-trigger a metadata update and have some logic collide with our
	// deletion cleanup.
	if len(deleted) > 0 {
		handleDeletedPartitions(deleted)
	}

	// The left writable map needs no further updates: all changes above
	// happened to r.all, of which r.writable contains a subset of.
	// Modifications to r.all are seen in r.writable.
	return needsRetry
}

// handleDeletedPartitions calls all promises in all records in all partitions
// in deleted with ErrPartitionDeleted, as well as removes topic partition
// cursors from their sources.
//
// We can encounter a deleted partition if a topic is deleted and recreated
// with fewer partitions. We have to clear the cursors so that if more
// partitions are reencountered in the future, they will be used.
func handleDeletedPartitions(deleted []*topicPartition) {
	for _, d := range deleted {
		sink := d.records.sink
		sink.removeRecBuf(d.records)
		for _, batch := range d.records.batches {
			for i, pnr := range batch.records {
				sink.cl.finishRecordPromise(pnr.promisedRec, ErrPartitionDeleted)
				batch.records[i] = noPNR
			}
			emptyRecordsPool.Put(&batch.records)
		}

		source := d.cursor.source
		source.removeCursor(d.cursor)
		source.cl.consumer.deletePartition(d)
	}
}
