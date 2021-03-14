package kgo

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
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
		allTopics := make([]string, 0, len(meta))
		for topic := range meta {
			allTopics = append(allTopics, topic)
		}
		cl.storeTopics(allTopics)
		topics = cl.loadTopics()
	}

	var consumerSessionStopped bool
	var reloadOffsets listOrEpochLoads
	for topic, oldParts := range topics {
		newParts, exists := meta[topic]
		if !exists {
			continue
		}
		needsRetry = cl.mergeTopicPartitions(topic, oldParts, newParts, &consumerSessionStopped, &reloadOffsets) || needsRetry
	}

	if consumerSessionStopped {
		reloadOffsets.loadWithSession(cl.consumer.startNewSession())
	}

	// Finally, trigger the consumer to process any updated metadata, which
	// can look for new partitions to consume or something or signal a
	// waiting list or epoch load to continue.
	cl.consumer.doOnMetadataUpdate()

	return needsRetry, nil
}

// fetchTopicMetadata fetches metadata for all reqTopics and returns new
// topicPartitionsData for each topic.
func (cl *Client) fetchTopicMetadata(reqTopics []string) (map[string]*topicPartitionsData, bool, error) {
	var all bool
	switch v := cl.consumer.loadKind().(type) {
	case *groupConsumer:
		all = v.regexTopics
	case *directConsumer:
		all = v.regexTopics
	}
	_, meta, err := cl.fetchMetadataForTopics(cl.ctx, all, reqTopics)
	if err != nil {
		return nil, all, err
	}

	topics := make(map[string]*topicPartitionsData, len(reqTopics))

	for i := range meta.Topics {
		topicMeta := &meta.Topics[i]

		parts := &topicPartitionsData{
			loadErr:            kerr.ErrorForCode(topicMeta.ErrorCode),
			isInternal:         topicMeta.IsInternal,
			partitions:         make([]*topicPartition, 0, len(topicMeta.Partitions)),
			writablePartitions: make([]*topicPartition, 0, len(topicMeta.Partitions)),
		}
		topics[topicMeta.Topic] = parts

		if parts.loadErr != nil {
			continue
		}

		// Kafka partitions are strictly increasing from 0. We enforce
		// that here; if any partition is missing, we consider this
		// topic a load failure.
		sort.Slice(topicMeta.Partitions, func(i, j int) bool {
			return topicMeta.Partitions[i].Partition < topicMeta.Partitions[j].Partition
		})
		for i := range topicMeta.Partitions {
			if got := topicMeta.Partitions[i].Partition; got != int32(i) {
				parts.loadErr = fmt.Errorf("kafka did not reply with a comprensive set of partitions for a topic; we expected partition %d but saw %d", i, got)
				break
			}
		}

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
				loadErr:     kerr.ErrorForCode(partMeta.ErrorCode),
				leader:      partMeta.Leader,
				leaderEpoch: leaderEpoch,

				records: &recBuf{
					cl: cl,

					topic:     topicMeta.Topic,
					partition: partMeta.Partition,

					maxRecordBatchBytes: cl.maxRecordBatchBytesForTopic(topicMeta.Topic),

					recBufsIdx: -1,
				},

				cursor: &cursor{
					topic:       topicMeta.Topic,
					partition:   partMeta.Partition,
					keepControl: cl.cfg.keepControl,
					cursorsIdx:  -1,

					leader:      partMeta.Leader,
					leaderEpoch: leaderEpoch,

					cursorOffset: cursorOffset{
						offset:            -1, // required to not consume until needed
						lastConsumedEpoch: -1, // required sentinel
					},
				},
			}
			// Any partition that has a load error uses the first
			// seed broker as a leader. This ensures that every
			// record buffer and every cursor can use a sink or
			// source.
			if p.loadErr != nil {
				p.leader = unknownSeedID(0)
			}

			cl.sinksAndSourcesMu.Lock()
			sns, exists := cl.sinksAndSources[p.leader]
			if !exists {
				sns = sinkAndSource{
					sink:   cl.newSink(p.leader),
					source: cl.newSource(p.leader),
				}
				cl.sinksAndSources[p.leader] = sns
			}
			cl.sinksAndSourcesMu.Unlock()
			p.records.sink = sns.sink
			p.cursor.source = sns.source

			parts.partitions = append(parts.partitions, p)
			if p.loadErr == nil {
				parts.writablePartitions = append(parts.writablePartitions, p)
			}
		}
	}

	return topics, all, nil
}

// mergeTopicPartitions merges a new topicPartition into an old and returns
// whether the metadata update that caused this merge needs to be retried.
//
// Retries are necessary if the topic or any partition has a retriable error.
func (cl *Client) mergeTopicPartitions(
	topic string,
	l *topicPartitions,
	r *topicPartitionsData,
	consumerSessionStopped *bool,
	reloadOffsets *listOrEpochLoads,
) (needsRetry bool) {
	lv := *l.load() // copy so our field writes do not collide with reads
	hadPartitions := len(lv.partitions) != 0
	defer func() { cl.storePartitionsUpdate(topic, l, &lv, hadPartitions) }()

	lv.loadErr = r.loadErr
	lv.isInternal = r.isInternal

	// If the load had an error for the entire topic, we set the load error
	// but keep our stale partition information. For anything being
	// produced, we bump the respective error or fail everything. There is
	// nothing to be done in a consumer.
	if r.loadErr != nil {
		retriable := kerr.IsRetriable(r.loadErr)
		if retriable {
			for _, topicPartition := range lv.partitions {
				topicPartition.records.bumpRepeatedLoadErr(lv.loadErr)
			}
		} else {
			for _, topicPartition := range lv.partitions {
				topicPartition.records.failAllRecords(lv.loadErr)
			}
		}
		return retriable
	}

	// Before the atomic update, we keep the latest partitions / writable
	// partitions. All updates happen in r's slices, and we keep the
	// results and store them in lv.
	defer func() {
		lv.partitions = r.partitions
		lv.writablePartitions = r.writablePartitions
	}()

	// We should have no deleted partitions, but there are two cases where
	// we could.
	//
	// 1) an admin added partitions, we saw, then we re-fetched metadata
	// from an out of date broker that did not have the new partitions
	//
	// 2) a topic was deleted and recreated with fewer partitions
	//
	// Both of these scenarios should be rare to non-existent, and we do
	// nothing if we encounter them.
	//
	// NOTE: we previously removed deleted partitions, but this was
	// removed. Deleting partitions really should not happen, and not
	// doing so simplifies the code.
	//
	// See commit 385cecb928e9ec3d9610c7beb223fcd1ed303fd0 for the last
	// commit that contained deleting partitions.

	// Migrating topicPartitions is a little tricky because we have to
	// worry about underlying pointers that may currently be loaded.
	for part, oldTP := range lv.partitions {
		exists := part < len(r.partitions)
		if !exists {
			// This is the "deleted" case; see the comment above.
			// We will just keep our old information.
			r.partitions = append(r.partitions, oldTP)
			if oldTP.loadErr == nil {
				r.writablePartitions = append(r.writablePartitions, oldTP)
			}
			continue
		}
		newTP := r.partitions[part]

		// Like above for the entire topic, an individual partittion
		// can have a load error. Unlike for the topic, individual
		// partition errors are always retriable.
		//
		// If the load errored, we keep all old information minus the
		// load error itself (the new load will have no information).
		if newTP.loadErr != nil {
			err := newTP.loadErr
			*newTP = *oldTP
			newTP.loadErr = err
			newTP.records.bumpRepeatedLoadErr(newTP.loadErr)
			needsRetry = true
			continue
		}

		// If the new partition has an older leader epoch, then we
		// fetched from an out of date broker. We just keep the old
		// information.
		if newTP.leaderEpoch < oldTP.leaderEpoch {
			*newTP = *oldTP
			continue
		}

		// If the new sink is the same as the old, we simply copy over
		// the records pointer and maybe begin producing again.
		//
		// We always clear the failing state; migration does this itself.
		if newTP.records.sink == oldTP.records.sink {
			newTP.records = oldTP.records
			newTP.records.clearFailing()
		} else {
			oldTP.migrateProductionTo(newTP)
		}

		// The cursor source could be different because we could be
		// fetching from a preferred replica.
		if newTP.cursor.leader == oldTP.cursor.leader &&
			newTP.cursor.leaderEpoch == oldTP.cursor.leaderEpoch {

			newTP.cursor = oldTP.cursor

			// Unlike above, there is no failing state for a
			// cursor. If a cursor has a fetch error, we buffer
			// that information for a poll, and then we continue to
			// re-fetch that error.

		} else {
			oldTP.migrateCursorTo(
				newTP,
				&cl.consumer,
				consumerSessionStopped,
				reloadOffsets,
			)
		}
	}

	// Anything left with a negative recBufsIdx / cursorsIdx is a new topic
	// partition. We use this to add the new tp's records to its sink.
	// Same reasoning applies to the cursor offset.
	for _, newTP := range r.partitions {
		if newTP.records.recBufsIdx == -1 {
			newTP.records.sink.addRecBuf(newTP.records)
			newTP.cursor.source.addCursor(newTP.cursor)
		}
	}

	return needsRetry
}
