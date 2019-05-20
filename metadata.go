package kgo

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/kgo/kerr"
)

func (c *Client) loadTopics() map[string]*topicPartitions {
	return c.topics.Load().(map[string]*topicPartitions)
}

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

// topicPartitionsData is the data behind a topicPartition's v.
//
// We keep this in an atomic because it is expected to be extremely read heavy,
// and if it were behind a lock, the lock would need holding for a good chunk
// of code.
type topicPartitionsData struct {
	loadErr    error // auth, unknown, leader not avail, or creation err
	partitions []int32
	all        map[int32]*topicPartition // partition num => partition
	writable   map[int32]*topicPartition // partition num => partition, eliding partitions with no leader / listener
}

type topicPartitions struct {
	v atomic.Value // *topicPartitionsData

	// We actually do not guard any "new" condition with this condition,
	// unlike normal conditions. The main key to producing is to have no
	// load error. If we produce to an out of date topicPartitionData, that
	// is fine.
	c *sync.Cond
}

func (t *topicPartitions) load() *topicPartitionsData {
	return t.v.Load().(*topicPartitionsData)
}

// newTopicParts creates and returns new topicPartitions
func newTopicParts() *topicPartitions {
	parts := &topicPartitions{}
	parts.v.Store(&topicPartitionsData{
		all:      make(map[int32]*topicPartition),
		writable: make(map[int32]*topicPartition),
	})
	var mu sync.RWMutex
	parts.c = sync.NewCond(mu.RLocker())
	return parts
}

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
	topics := c.topics.Load().(map[string]*topicPartitions)
	toUpdate := make([]string, 0, len(topics))
	for topic := range topics {
		toUpdate = append(toUpdate, topic)
	}

	meta, err := c.fetchTopicMetadata(toUpdate)
	if err != nil {
		return true, err
	}

	for topic, oldParts := range topics {
		newParts, exists := meta[topic]
		if !exists {
			continue
		}
		needsRetry = oldParts.merge(newParts) || needsRetry
	}

	return needsRetry, nil
}

// fetchTopicMetadata fetches metadata for all reqTopics and returns new
// topicPartitionsData for each topic.
func (c *Client) fetchTopicMetadata(reqTopics []string) (map[string]*topicPartitionsData, error) {
	meta, err := c.fetchMetadata(false, reqTopics...)
	if err != nil {
		return nil, err
	}

	topics := make(map[string]*topicPartitionsData, len(reqTopics))

	c.brokersMu.RLock()
	defer c.brokersMu.RUnlock()

	for i := range meta.TopicMetadata {
		topicMeta := &meta.TopicMetadata[i]

		parts := &topicPartitionsData{
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
					allPartRecsIdx:  -1, // required, see below
					lastAckedOffset: -1,
				},

				replicas: partMeta.Replicas,
				isr:      partMeta.ISR,
				offline:  partMeta.OfflineReplicas,
			}

			broker, exists := c.brokers[p.leader]
			if !exists {
				if p.loadErr == nil {
					p.loadErr = &errUnknownBrokerForPartition{p.topic, p.partition, p.leader}
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
func (l *topicPartitions) merge(r *topicPartitionsData) (needsRetry bool) {
	// We do not gate our broadcast with a mutex.
	defer l.c.Broadcast()

	lv := *l.load() // copy so our field writes do not collide with reads
	defer func() { l.v.Store(&lv) }()

	lv.loadErr = r.loadErr
	if r.loadErr != nil {
		for _, topicPartition := range lv.all {
			topicPartition.records.maybeBumpTriesAndFailBatch0(lv.loadErr)
		}
		return kerr.IsRetriable(lv.loadErr)
	}

	lv.partitions = r.partitions

	var deleted []*topicPartition // should end up empty

	// Migrating topicPartitions is a little tricky because we have to
	// worry about map contents. Basically, though, we always have to keep
	// the old "records", but we want the new of everything else.
	for part, oldTP := range lv.all {
		newTP, exists := r.all[part]
		if !exists {
			deleted = append(deleted, oldTP)
			continue
		}
		if newTP.loadErr != nil { // partition errors should generally be temporary
			err := newTP.loadErr
			*newTP = *oldTP
			newTP.loadErr = err
			newTP.records.maybeBumpTriesAndFailBatch0(newTP.loadErr)
			needsRetry = true
			continue
		}
		// With the same sink, we have to copy the records pointer and
		// maybe begin draining again.
		if newTP.records.sink == oldTP.records.sink {
			newTP.records = oldTP.records
			newTP.records.maybeBeginDraining()
			continue
		}
		// With a different sink, we have to remove the records from
		// the old sink, update the record's sink, add the records to
		// the new sink, and finally copy the pointer to the new tp.
		//
		// Between the removal and the setting of the new sink,
		// produced records could trigger drains for the old sink.
		// This is ok because it will not drain from the records we
		// just removed.
		oldTP.records.sink.removeSource(oldTP.records)
		oldTP.records.mu.Lock() // guard setting sink
		oldTP.records.sink = newTP.records.sink
		oldTP.records.mu.Unlock()
		oldTP.records.sink.addSource(oldTP.records)
		newTP.records = oldTP.records
	}

	for _, newTP := range r.all { // anything left in r.all is new
		if newTP.records.allPartRecsIdx == -1 {
			newTP.records.sink.addSource(newTP.records)
		}
	}

	lv.all = r.all
	lv.writable = r.writable
	for part := range lv.writable {
		lv.writable[part] = lv.all[part]
	}

	if len(deleted) > 0 {
		go handleDeletedPartitions(deleted)
	}

	return needsRetry
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
