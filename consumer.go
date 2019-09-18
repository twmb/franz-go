package kgo

import (
	"context"
	"sync"

	"github.com/twmb/kgo/kerr"
	"github.com/twmb/kgo/kmsg"
)

type consumerType int8

const (
	consumerTypeUnset = iota
	consumerTypeDirect
	consumerTypeGroup
)

// NOTE
// For epoch
// On metadata update
// just update the consumption epoch!!
// Same can be done in producer side when kafka gets that.

// Offset is a message offset into a partition.
type Offset struct {
	request  int64
	relative int64
}

// ConsumeStartOffset begins consuming at the earliest timestamp in a partition.
func ConsumeStartOffset() Offset {
	return Offset{request: -2}
}

// ConsumeEndOffset begins consuming at the latest timestamp in a partition.
func ConsumeEndOffset() Offset {
	return Offset{request: -1}
}

// ConsumeStartRelativeOffset begins consume n after the earliest offset.
func ConsumeStartRelativeOffset(n int) Offset {
	return Offset{request: -2, relative: int64(n)}
}

// ConsumeEndRelativeOffset begins consuming n before the latest offset.
func ConsumeEndRelativeOffset(n int) Offset {
	return Offset{request: -1, relative: int64(-n)}
}

// ConsumeExactOffset begins consuming at the given offset.
func ConsumeExactOffset(o int64) Offset {
	if o < 0 {
		o = 0
	}
	return Offset{request: o}
}

type consumer struct {
	cl *Client

	mu     sync.Mutex
	group  *groupConsumer
	direct *directConsumer
	typ    consumerType

	usingPartitions []*topicPartition

	offsetsWaitingLoad *offsetsWaitingLoad

	sourcesReadyMu          sync.Mutex
	sourcesReadyCond        *sync.Cond
	sourcesReadyForDraining []*recordSource

	// seq corresponds to the number of assigned groups or partitions.
	//
	// It is updated under both the sources ready mu and potentially
	// also the consumer mu itself.
	//
	// Incrementing it invalidates prior assignments and fetches.
	seq uint64
}

// unassignPrior invalidates old assignments, ensures that nothing is assigned,
// and leaves any group.
func (c *consumer) unassignPrior() {
	c.assignPartitions(nil, true) // invalidate old assignments
	if c.typ == consumerTypeGroup {
		c.typ = consumerTypeUnset
		c.group.leave()
	}
}

// addSourceReadyForDraining tracks that a source needs its buffered fetch
// consumed. If the seq this source is from is out of date, the source is
// immediately drained.
func (c *consumer) addSourceReadyForDraining(seq uint64, source *recordSource) {
	var broadcast bool
	c.sourcesReadyMu.Lock()
	if seq < c.seq {
		source.takeBuffered()
	} else {
		c.sourcesReadyForDraining = append(c.sourcesReadyForDraining, source)
		broadcast = true
	}
	c.sourcesReadyMu.Unlock()
	if broadcast {
		c.sourcesReadyCond.Broadcast()
	}
}

func (cl *Client) PollFetches(ctx context.Context) Fetches {
	c := &cl.consumer

	var fetches Fetches

	fill := func() {
		c.sourcesReadyMu.Lock()
		for _, ready := range c.sourcesReadyForDraining {
			// If PollFetches is running concurrent with an
			// assignment, the assignment may have invalidated
			// some buffered fetches.
			fetch, seq := ready.takeBuffered()
			if seq < c.seq {
				continue
			}
			fetches = append(fetches, fetch)
		}
		c.sourcesReadyForDraining = nil
		c.sourcesReadyMu.Unlock()
	}

	fill()
	if len(fetches) > 0 {
		return fetches
	}

	done := make(chan struct{})
	quit := false
	go func() {
		c.sourcesReadyMu.Lock()
		defer c.sourcesReadyMu.Unlock()
		defer close(done)

		for !quit {
			if len(c.sourcesReadyForDraining) > 0 {
				return
			}
			c.sourcesReadyCond.Wait()
		}
	}()

	select {
	case <-ctx.Done():
	case <-done:
	}

	fill()
	return fetches
}

// assignPartitions, called under the consumer's mu, is used to set new
// consumptions or add to the existing consumptions. If invalidateOld is true,
// this invalidates old assignments / active fetches / buffered fetches.
func (c *consumer) assignPartitions(assignments map[string]map[int32]Offset, invalidateOld bool) {
	seq := c.seq

	if invalidateOld {
		c.sourcesReadyMu.Lock()
		c.seq++
		seq = c.seq

		// First, stop all fetches for prior assignments. After our consumer
		// lock is released, fetches will return nothing historic.
		for _, usedPartition := range c.usingPartitions {
			usedPartition.consumption.setOffset(-1, seq)
		}

		// Also drain any buffered, now stale, fetches.
		for _, ready := range c.sourcesReadyForDraining {
			ready.takeBuffered()
		}
		c.sourcesReadyForDraining = nil
		c.sourcesReadyMu.Unlock()

		c.usingPartitions = c.usingPartitions[:0]
	}

	// This assignment could contain nothing (for the purposes of
	// invalidating active fetches), so we only do this if needed.
	if len(assignments) == 0 {
		return
	}

	// Ensure all topics exist so that we will fetch their metadata.
	c.cl.topicsMu.Lock()
	clientTopics := c.cl.cloneTopics()
	for topic := range assignments {
		if _, exists := clientTopics[topic]; !exists {
			clientTopics[topic] = newTopicPartitions()
		}
	}
	c.cl.topics.Store(clientTopics)
	c.cl.topicsMu.Unlock()

	// If we have a topic and partition loaded and the assignments use
	// exact offsets, we can avoid looking up offsets.
	for topic, partitions := range assignments {
		topicParts := clientTopics[topic].load() // must be loadable; ensured above
		if topicParts == nil {
			continue
		}

		for partition, offset := range partitions {
			part := topicParts.all[partition]
			if part == nil {
				continue
			}

			if offset.request >= 0 {
				part.consumption.setOffset(offset.request, seq)
				c.usingPartitions = append(c.usingPartitions, part)
				delete(partitions, partition)
			}
		}
		if len(partitions) == 0 {
			delete(assignments, topic)
		}
	}

	// For all remaining offsets, await load.
	if len(assignments) > 0 {
		(&offsetsWaitingLoad{
			fromSeq: seq,
			waiting: assignments,
		}).mergeIntoLocked(c)
	}
}

// mergeInto is used to merge waiting offsets into a consumer.
//
// When we load partition offsets, we send many requests to all brokers
// responsible for topic partitions. All failing loads get merged back into the
// consumer for a future load retry.
func (o *offsetsWaitingLoad) mergeInto(c *consumer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	o.mergeIntoLocked(c)
}

// mergeIntoLocked is called directly from assignOffsets, which already
// has the consumer locked.
func (o *offsetsWaitingLoad) mergeIntoLocked(c *consumer) {
	if len(o.waiting) == 0 {
		return
	}

	if o.fromSeq < c.seq {
		return
	}

	// If this is the first reload, we trigger a metadata update.
	// If this is non-nil, then a metadata update has not returned
	// yet and we merge into the exisiting wait and avoid updating.
	existing := c.offsetsWaitingLoad
	if existing == nil {
		c.offsetsWaitingLoad = o
		c.cl.triggerUpdateMetadata()
		return
	}

	for topic, partitions := range o.waiting {
		curTopic, exists := existing.waiting[topic]
		if !exists {
			existing.setTopicParts(topic, partitions)
			continue
		}
		for partition, offset := range partitions {
			curTopic[partition] = offset
		}
	}
}

func (c *consumer) doOnMetadataUpdate() {
	c.mu.Lock()
	defer c.mu.Unlock()

	// First, call our direct or group on updates; these may set more
	// partitions to load.
	switch c.typ {
	case consumerTypeUnset:
		return
	case consumerTypeDirect:
		c.assignPartitions(c.direct.findNewAssignments(c.cl.loadTopics()), false)
	case consumerTypeGroup:
		// TODO if leader, reprocess partitions to see if new assignments
	}

	// Finally, process any updates.
	c.resetAndLoadOffsets()
}

// resetAndLoadOffsets empties offsetsWaitingLoad and tries loading them.
func (c *consumer) resetAndLoadOffsets() {
	toLoad := c.offsetsWaitingLoad
	c.offsetsWaitingLoad = nil
	if toLoad == nil || toLoad.waiting == nil {
		return
	}
	go c.tryOffsetLoad(toLoad)
}

func (c *consumer) tryOffsetLoad(toLoad *offsetsWaitingLoad) {
	// If any partitions do not exist in the metadata, or we cannot find
	// the broker leader for a partition, we reload the metadata.
	toReload := &offsetsWaitingLoad{fromSeq: toLoad.fromSeq}
	brokersToLoadFrom := make(map[*broker]*offsetsWaitingLoad)

	// For most of this function, we hold the broker mu so that we can
	// check if topic partition leaders exist.
	c.cl.brokersMu.RLock()
	brokers := c.cl.brokers

	// Map all waiting partition loads to the brokers that can load the
	// offsets for those partitions.
	topics := c.cl.loadTopics()
	for topic, partitions := range toLoad.waiting {
		// The topicPartitions must exist, since assignPartitions
		// creates the topic if the topic is new.
		topicPartitions := topics[topic].load()

		for partition, offset := range partitions {
			topicPartition, exists := topicPartitions.all[partition]
			if !exists {
				toReload.setTopicPart(topic, partition, offset)
				continue
			}

			broker := brokers[topicPartition.leader]
			if broker == nil { // should not happen
				toReload.setTopicPart(topic, partition, offset)
				continue
			}

			addLoad := brokersToLoadFrom[broker]
			if addLoad == nil {
				addLoad = &offsetsWaitingLoad{fromSeq: toLoad.fromSeq}
				brokersToLoadFrom[broker] = addLoad
			}
			addLoad.setTopicPart(topic, partition, offset)
		}
	}

	c.cl.brokersMu.RUnlock()

	for broker, brokerLoad := range brokersToLoadFrom {
		go c.tryBrokerOffsetLoad(broker, brokerLoad)
	}

	toReload.mergeInto(c)
}

func (c *consumer) tryBrokerOffsetLoad(broker *broker, load *offsetsWaitingLoad) {
	kresp, err := broker.waitResp(c.cl.ctx, load.buildReq())
	if err != nil {
		load.mergeInto(c)
		return
	}
	resp := kresp.(*kmsg.ListOffsetsResponse)

	type toSet struct {
		topicPartition *topicPartition
		offset         int64
	}
	var toSets []toSet

	for _, rTopic := range resp.Topics {
		topic := rTopic.Topic
		waitingParts, ok := load.waiting[topic]
		if !ok {
			continue
		}

		for _, rPartition := range rTopic.Partitions {
			partition := rPartition.Partition
			waitingPart, ok := waitingParts[partition]
			if !ok {
				continue
			}

			err := kerr.ErrorForCode(rPartition.ErrorCode)
			if err != nil {
				if !kerr.IsRetriable(err) {
					// TODO notify client users somehow
					// Maybe a single fake Fetch in the
					// first Poll.
					delete(waitingParts, partition)
				}
				continue
			}

			topicPartitions := c.cl.loadTopics()[topic].load()
			topicPartition, ok := topicPartitions.all[partition]
			if !ok {
				continue // very weird
			}

			// We have a response for what we wanted to load:
			// delete our want from the map to avoid reload.
			delete(waitingParts, partition)
			if len(waitingParts) == 0 {
				delete(load.waiting, topic)
			}

			offset := rPartition.Offset
			if waitingPart.request >= 0 {
				offset = waitingPart.request + waitingPart.relative
			}
			toSets = append(toSets, toSet{
				topicPartition,
				offset,
			})
		}
	}

	// If for some reason Kafka did not reply to some of our list request,
	// we re-request.
	if len(load.waiting) > 0 {
		load.mergeInto(c)
	}
	if len(toSets) == 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if load.fromSeq < c.seq {
		return
	}
	for _, toSet := range toSets {
		toSet.topicPartition.consumption.setOffset(toSet.offset, c.seq)
		c.usingPartitions = append(c.usingPartitions, toSet.topicPartition)
	}
}

type offsetsWaitingLoad struct {
	fromSeq uint64
	waiting map[string]map[int32]Offset
}

func (o *offsetsWaitingLoad) maybeInit() {
	if o.waiting == nil {
		o.waiting = make(map[string]map[int32]Offset)
	}
}

func (o *offsetsWaitingLoad) setTopicParts(topic string, partitions map[int32]Offset) {
	o.maybeInit()
	o.waiting[topic] = partitions
}

func (o *offsetsWaitingLoad) setTopicPart(topic string, partition int32, offset Offset) {
	o.maybeInit()
	parts := o.waiting[topic]
	if parts == nil {
		parts = make(map[int32]Offset)
		o.waiting[topic] = parts
	}
	parts[partition] = offset
}

func (o *offsetsWaitingLoad) buildReq() *kmsg.ListOffsetsRequest {
	req := &kmsg.ListOffsetsRequest{
		ReplicaID: -1,
		Topics:    make([]kmsg.ListOffsetsRequestTopic, 0, len(o.waiting)),
	}
	for topic, partitions := range o.waiting {
		parts := make([]kmsg.ListOffsetsRequestTopicPartition, 0, len(partitions))
		for partition, offset := range partitions {
			// If this partition is using an exact offset request,
			// then Assign was called with the partition not
			// existing. We just use -1 to ensure the partition
			// is loaded.
			timestamp := offset.request
			if timestamp >= 0 {
				timestamp = -1
			}
			parts = append(parts, kmsg.ListOffsetsRequestTopicPartition{
				Partition:          partition,
				CurrentLeaderEpoch: -1, // TODO KIP-320
				Timestamp:          offset.request,
			})
		}
		req.Topics = append(req.Topics, kmsg.ListOffsetsRequestTopic{
			Topic:      topic,
			Partitions: parts,
		})
	}
	return req
}
