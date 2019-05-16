package kgo

import (
	"time"

	"github.com/twmb/kgo/kerr"
)

func (c *Client) triggerUpdateMetadata() {
	select {
	case c.updateMetadataCh <- struct{}{}:
	default:
	}
}

// updateMetadataLoop updates metadata whenever the update ticker ticks,
// or whenever deliberately triggered.
func (c *Client) updateMetadataLoop() {
	defer c.metadataTicker.Stop()
	for {
		select {
		case <-c.closedCh:
			return
		case <-c.metadataTicker.C:
			c.triggerUpdateMetadata()
		case <-c.updateMetadataCh:
			time.Sleep(time.Millisecond)
			tries := 0
		start:
			tries++
			again, err := c.updateMetadata()
			if err != nil && tries < c.cfg.client.retries {
				select {
				case <-c.closedCh:
					return
				case <-time.After(c.cfg.client.retryBackoff(tries)):
					goto start
				}
			}

			if again {
				c.triggerUpdateMetadata()
			}

			// To avoid unnecessary repeated updates, we sleep 1s
			// between updates.
			select {
			case <-c.closedCh:
				return
			case <-time.After(time.Second):
			}
		}
	}
}

// updateMetadata updates all of a client's topic's metadata, returning whether
// a new update needs scheduling or if an error occured.
//
// If any topics or partitions have an error, all record buffers in the topic,
// or the record buffer for each erroring partition, has the first batch's
// try count bumped by one.
func (c *Client) updateMetadata() (needsRetry bool, err error) {
	// Quickly fetch all topics we have so we can update them.
	c.topicsMu.Lock()
	toUpdate := make([]string, 0, len(c.topics))
	for topic := range c.topics {
		toUpdate = append(toUpdate, topic)
	}
	c.topicsMu.Unlock()

	meta, err := c.fetchTopicMetadata(toUpdate)
	if err != nil {
		return true, err
	}

	// Again over our topics, see what needs merging from the update.
	c.topicsMu.Lock()
	type oldNew struct {
		l, r *topicPartitions
	}
	var toMerge []oldNew
	for topic, oldParts := range c.topics {
		newParts, exists := meta[topic]
		if !exists {
			continue
		}
		toMerge = append(toMerge, oldNew{
			l: oldParts,
			r: newParts,
		})
	}
	c.topicsMu.Unlock()

	// Finally, merge all topic partitions.
	for _, m := range toMerge {
		needsRetry = m.l.merge(m.r) || needsRetry
	}
	return needsRetry, nil
}

// fetchTopicMetadata fetches metadata for all reqTopics and returns new
// topicPartitions for each topic.
func (c *Client) fetchTopicMetadata(reqTopics []string) (map[string]*topicPartitions, error) {
	meta, err := c.fetchMetadata(false, reqTopics...)
	if err != nil {
		return nil, err
	}

	topics := make(map[string]*topicPartitions, len(reqTopics))

	c.brokersMu.RLock()
	defer c.brokersMu.RUnlock()

	for i := range meta.TopicMetadata {
		topicMeta := &meta.TopicMetadata[i]

		parts := &topicPartitions{
			loadErr:  kerr.ErrorForCode(topicMeta.ErrorCode),
			all:      make(map[int32]*topicPartition),
			writable: make(map[int32]*topicPartition),
		}
		topics[topicMeta.Topic] = parts

		if parts.loadErr != nil {
			continue
		}

		for i := range topicMeta.PartitionMetadata {
			partMeta := &topicMeta.PartitionMetadata[i]

			p := &topicPartition{
				topic:     topicMeta.Topic,
				partition: partMeta.Partition,
				loadErr:   kerr.ErrorForCode(partMeta.ErrorCode),

				leader:      partMeta.Leader,
				leaderEpoch: partMeta.LeaderEpoch,

				records: &records{
					lastAckedOffset: -1,
				},

				replicas: partMeta.Replicas,
				isr:      partMeta.ISR,
				offline:  partMeta.OfflineReplicas,
			}

			broker, exists := c.brokers[partMeta.Leader]
			if !exists {
				if p.loadErr == nil {
					p.loadErr = errUnknownBrokerForLeader // should not...
				}
			} else {
				p.records.sink = broker.recordSink
				p.records.topicPartition = p
			}

			parts.partitions = append(parts.partitions, p.partition)
			parts.all[p.partition] = p
			if p.loadErr == nil {
				parts.writable[p.partition] = p
			}
		}
	}

	return topics, nil
}

// merge merges a new topicPartition into an old and returns whether the
// metadata update that caused this merge needs to be retried.
//
// Retries are necessary if the topic or any partition has a retriable error.
func (l *topicPartitions) merge(r *topicPartitions) (needsRetry bool) {
	l.mu.Lock()
	defer func() {
		l.seq++
		l.mu.Unlock()
		l.c.Broadcast()
	}()

	l.loadErr = r.loadErr
	if r.loadErr != nil {
		for _, topicPartition := range l.all {
			topicPartition.records.maybeBumpTriesAndFailBatch0(l.loadErr)
		}
		return kerr.IsRetriable(l.loadErr)
	}

	l.partitions = r.partitions

	var deleted []*topicPartition // should be empty

	for part, oldTP := range l.all {
		newTP, exists := r.all[part]
		if !exists {
			deleted = append(deleted, oldTP)
			continue
		}
		needsRetry = oldTP.merge(newTP) || needsRetry
		delete(r.all, part)
	}

	for part, newTP := range r.all { // anything left in r.all is new
		l.all[part] = newTP
		newTP.records.sink.addSource(newTP.records)
	}

	l.writable = r.writable
	for part := range l.writable {
		l.writable[part] = l.all[part]
	}

	if len(deleted) > 0 {
		go handleDeletedPartitions(deleted)
	}

	return needsRetry
}

// merge merges a new topicPartition into an old and returns whether the
// metadata update that caused this merge needs to be retried. This is only
// done under the owning topicPartition's mutex.
//
// Retries are necessary if the partition has a load error; this error is one
// of leader not available, listener not found, or replica not available. All
// of these are retriable and hopefully temporary.
func (l *topicPartition) merge(r *topicPartition) (needsRetry bool) {
	// We do a piecewise copy because we cannot overwrite the records
	// field when merging. r is always from a metadata update and is new,
	// whereas l is our existing topic and may have buffered records.
	l.loadErr = r.loadErr
	l.leader = r.leader
	l.leaderEpoch = r.leaderEpoch
	l.replicas = r.replicas
	l.isr = r.isr
	l.offline = r.offline
	if l.loadErr != nil {
		l.records.maybeBumpTriesAndFailBatch0(l.loadErr)
		return true
	}
	if l.records.sink == r.records.sink {
		l.records.maybeBeginDraining()
	} else {
		l.records.sink.removeSource(l.records)
		l.records.sink = r.records.sink
		l.records.sink.addSource(l.records)
	}
	return false
}

// handleDeletedPartitions calls all promises in all records in all partitions
// in deleted with ErrPartitionDeleted.
//
// I do not think Kafka can actually delete a partition, but, just in case.
func handleDeletedPartitions(deleted []*topicPartition) {
	for _, d := range deleted {
		d.records.mu.Lock()
		sink := d.records.sink
		sink.removeSource(d.records)
		for _, batch := range d.records.batches {
			for i, record := range batch.records {
				sink.broker.client.promise(record.pr, ErrPartitionDeleted)
				batch.records[i] = noPNR
			}
			emptyRecordsPool.Put(&batch.records)
		}
		d.records.mu.Unlock()
	}
}
