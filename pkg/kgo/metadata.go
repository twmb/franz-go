package kgo

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
)

// This corresponds to the amount of time we cache stale metadata before
// forcing a refresh when calling `waitmeta` or `triggerUpdateMetadata`. If
// either function is called within the refresh window, then we just return
// immediately / avoid triggering a refresh.
//
// This is similar to the client configurable metadata min age, the difference
// being is that that limit kicks in when a trigger actually does happen and
// causes a sleep before proceeding into a metadata request.
const minRefreshTrigger = 5 * time.Second / 2

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
	if now.Sub(cl.metawait.lastUpdate) < minRefreshTrigger {
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
			if now.Sub(cl.metawait.lastUpdate) < minRefreshTrigger {
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

func (cl *Client) triggerUpdateMetadata(must bool) bool {
	if !must {
		cl.metawait.mu.Lock()
		defer cl.metawait.mu.Unlock()
		if time.Since(cl.metawait.lastUpdate) < minRefreshTrigger {
			return false
		}
	}

	select {
	case cl.updateMetadataCh <- struct{}{}:
	default:
	}
	return true
}

func (cl *Client) triggerUpdateMetadataNow() {
	select {
	case cl.updateMetadataNowCh <- struct{}{}:
	default:
	}
}

func (cl *Client) blockingMetadataFn(fn func()) {
	select {
	case cl.blockingMetadataFnCh <- fn:
	case <-cl.ctx.Done():
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
loop:
	for {
		var now bool
		select {
		case <-cl.ctx.Done():
			return
		case <-ticker.C:
		case <-cl.updateMetadataCh:
		case <-cl.updateMetadataNowCh:
			now = true
		case fn := <-cl.blockingMetadataFnCh:
			fn()
			continue loop
		}

		var nowTries int
	start:
		nowTries++
		if !now {
			if wait := cl.cfg.metadataMinAge - time.Since(lastAt); wait > 0 {
				timer := time.NewTimer(wait)
			prewait:
				select {
				case <-cl.ctx.Done():
					timer.Stop()
					return
				case <-cl.updateMetadataNowCh:
					timer.Stop()
				case <-timer.C:
				case fn := <-cl.blockingMetadataFnCh:
					fn()
					goto prewait
				}
			}
		} else {
			// Even with an "update now", we sleep just a bit to allow some
			// potential pile on now triggers.
			time.Sleep(10 * time.Millisecond)
		}

		// Drain any refires that occured during our waiting.
	out:
		for {
			select {
			case <-cl.updateMetadataCh:
			case <-cl.updateMetadataNowCh:
			case fn := <-cl.blockingMetadataFnCh:
				fn()
			default:
				break out
			}
		}

		again, err := cl.updateMetadata()
		if again || err != nil {
			if now && nowTries < 3 {
				goto start
			}
			cl.triggerUpdateMetadata(true)
		}
		if err == nil {
			lastAt = time.Now()
			consecutiveErrors = 0
			continue
		}

		consecutiveErrors++
		after := time.NewTimer(cl.cfg.retryBackoff(consecutiveErrors))
	backoff:
		select {
		case <-cl.ctx.Done():
			after.Stop()
			return
		case <-after.C:
		case fn := <-cl.blockingMetadataFnCh:
			fn()
			goto backoff
		}

	}
}

// Updates all producer and consumer partition data, returning whether a new
// update needs scheduling or if an error occured.
//
// The producer and consumer use different topic maps and underlying
// topicPartitionsData pointers, but we update those underlying pointers
// equally.
func (cl *Client) updateMetadata() (needsRetry bool, err error) {
	defer cl.metawait.signal()
	defer cl.consumer.doOnMetadataUpdate()

	var (
		tpsProducerLoad = cl.producer.topics.load()
		tpsConsumer     *topicsPartitions
		all             bool
		reqTopics       []string
	)
	switch v := cl.consumer.loadKind().(type) {
	case *groupConsumer:
		tpsConsumer, all = v.tps, v.regexTopics
	case *directConsumer:
		tpsConsumer, all = v.tps, v.regexTopics
	}

	if !all {
		reqTopicsSet := make(map[string]struct{})
		for _, m := range []map[string]*topicPartitions{
			tpsProducerLoad,
			tpsConsumer.load(),
		} {
			for topic := range m {
				reqTopicsSet[topic] = struct{}{}
			}
		}
		reqTopics = make([]string, 0, len(reqTopicsSet))
		for topic := range reqTopicsSet {
			reqTopics = append(reqTopics, topic)
		}
	}

	latest, err := cl.fetchTopicMetadata(all, reqTopics)
	if err != nil {
		cl.bumpMetadataFailForTopics( // bump load failures for all topics
			tpsProducerLoad,
			err,
		)
		return true, err
	}

	// If we are consuming with regex and fetched all topics, the metadata
	// may have returned topics the consumer is not yet tracking. We ensure
	// that we will store the topics at the end of our metadata update.
	tpsConsumerLoad := tpsConsumer.load()
	if all && len(latest) > 0 {
		allTopics := make([]string, 0, len(latest))
		for topic := range latest {
			allTopics = append(allTopics, topic)
		}
		tpsConsumerLoad = tpsConsumer.ensureTopics(allTopics)
		defer tpsConsumer.storeData(tpsConsumerLoad)
	}

	var (
		consumerSessionStopped bool
		reloadOffsets          listOrEpochLoads
		tpsPrior               *topicsPartitions
	)

	// Before we return, if we stopped the session, we need to restart it
	// with the topic partitions we were consuming.  Lastly, we need to
	// trigger the consumer metadata update to allow consumers waiting to
	// continue.
	defer func() {
		if consumerSessionStopped {
			reloadOffsets.loadWithSession(cl.consumer.startNewSession(tpsPrior))
		}
	}()

	var missingProduceTopics []string
	for _, m := range []struct {
		priors    map[string]*topicPartitions
		isProduce bool
	}{
		{tpsProducerLoad, true},
		{tpsConsumerLoad, false},
	} {
		for topic, priorParts := range m.priors {
			newParts, exists := latest[topic]
			if !exists {
				if m.isProduce {
					missingProduceTopics = append(missingProduceTopics, topic)
				}
				continue
			}
			needsRetry = needsRetry || cl.mergeTopicPartitions(
				topic,
				priorParts,
				newParts,
				m.isProduce,
				&consumerSessionStopped,
				&reloadOffsets,
				&tpsPrior,
			)
		}
	}
	if len(missingProduceTopics) > 0 {
		cl.bumpMetadataFailForTopics(
			tpsProducerLoad,
			errors.New("metadata request did not return this topic"),
			missingProduceTopics...,
		)
	}

	return needsRetry, nil
}

// fetchTopicMetadata fetches metadata for all reqTopics and returns new
// topicPartitionsData for each topic.
func (cl *Client) fetchTopicMetadata(all bool, reqTopics []string) (map[string]*topicPartitionsData, error) {
	_, meta, err := cl.fetchMetadataForTopics(cl.ctx, all, reqTopics)
	if err != nil {
		return nil, err
	}

	topics := make(map[string]*topicPartitionsData, len(meta.Topics))

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

		// This 249 limit is in Kafka itself, we copy it here to rely on it while producing.
		if len(topicMeta.Topic) > 249 {
			parts.loadErr = fmt.Errorf("invalid long topic name of (len %d) greater than max allowed 249", len(topicMeta.Topic))
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
				loadErr: kerr.ErrorForCode(partMeta.ErrorCode),
				topicPartitionData: topicPartitionData{
					leader:      partMeta.Leader,
					leaderEpoch: leaderEpoch,
				},

				records: &recBuf{
					cl: cl,

					topic:     topicMeta.Topic,
					partition: partMeta.Partition,

					maxRecordBatchBytes: cl.maxRecordBatchBytesForTopic(topicMeta.Topic),

					recBufsIdx: -1,
					failing:    partMeta.ErrorCode != 0,
				},

				cursor: &cursor{
					topic:       topicMeta.Topic,
					partition:   partMeta.Partition,
					keepControl: cl.cfg.keepControl,
					cursorsIdx:  -1,

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

			p.cursor.topicPartitionData = p.topicPartitionData
			p.records.topicPartitionData = p.topicPartitionData

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

	return topics, nil
}

// mergeTopicPartitions merges a new topicPartition into an old and returns
// whether the metadata update that caused this merge needs to be retried.
//
// Retries are necessary if the topic or any partition has a retriable error.
func (cl *Client) mergeTopicPartitions(
	topic string,
	l *topicPartitions,
	r *topicPartitionsData,
	isProduce bool,
	consumerSessionStopped *bool,
	reloadOffsets *listOrEpochLoads,
	tpsPrior **topicsPartitions,
) (needsRetry bool) {
	lv := *l.load() // copy so our field writes do not collide with reads

	// Producers must store the update through a special function that
	// manages unknown topic waiting, whereas consumers can just simply
	// store the update.
	if isProduce {
		hadPartitions := len(lv.partitions) != 0
		defer func() { cl.storePartitionsUpdate(topic, l, &lv, hadPartitions) }()
	} else {
		defer l.v.Store(&lv)
	}

	lv.loadErr = r.loadErr
	lv.isInternal = r.isInternal

	// If the load had an error for the entire topic, we set the load error
	// but keep our stale partition information. For anything being
	// produced, we bump the respective error or fail everything. There is
	// nothing to be done in a consumer.
	if r.loadErr != nil {
		if isProduce {
			for _, topicPartition := range lv.partitions {
				topicPartition.records.bumpRepeatedLoadErr(lv.loadErr)
			}
		}
		return true
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
			if isProduce {
				newTP.records.bumpRepeatedLoadErr(newTP.loadErr)
			}
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

		// If the tp data is the same, we simply copy over the records
		// and cursor pointers.
		//
		// If the tp data equals the old, then the sink / source is the
		// same, because the sink/source is from the tp leader.
		if newTP.topicPartitionData == oldTP.topicPartitionData {
			if isProduce {
				newTP.records = oldTP.records
				newTP.records.clearFailing() // always clear failing state for producing after meta update
			} else {
				newTP.cursor = oldTP.cursor // unlike records, there is no failing state for a cursor
			}

		} else {
			if isProduce {
				oldTP.migrateProductionTo(newTP) // migration clears failing state
			} else {
				oldTP.migrateCursorTo(
					newTP,
					&cl.consumer,
					consumerSessionStopped,
					reloadOffsets,
					tpsPrior,
				)
			}
		}
	}

	// Anything left with a negative recBufsIdx / cursorsIdx is a new topic
	// partition and must be added to the sink / source.
	for _, newTP := range r.partitions {
		if isProduce && newTP.records.recBufsIdx == -1 {
			newTP.records.sink.addRecBuf(newTP.records)
		} else if !isProduce && newTP.cursor.cursorsIdx == -1 {
			newTP.cursor.source.addCursor(newTP.cursor)
		}
	}
	return needsRetry
}
