package kgo

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/twmb/kgo/kerr"
	"github.com/twmb/kgo/kmsg"
)

type consumerType uint32

const (
	consumerTypeUnset consumerType = iota
	consumerTypeAssigned
	consumerTypeGroup
)

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

type TopicPartitions struct {
	Topic      string
	Partitions []int32
}

// TopicPartitions requests and returns partitions for the requested topics or
// an error if the request failed.
//
// If no topics are requested, this returns all topics and their partitions.
func (c *Client) TopicPartitions(topics ...string) ([]TopicPartitions, error) {
	resp, err := c.fetchMetadata(len(topics) == 0, topics)
	if err != nil {
		return nil, err
	}

	tps := make([]TopicPartitions, 0, len(resp.TopicMetadata))
	for _, topicMeta := range resp.TopicMetadata {
		if topicMeta.IsInternal {
			continue
		}
		tp := TopicPartitions{
			Topic:      topicMeta.Topic,
			Partitions: make([]int32, 0, len(topicMeta.PartitionMetadata)),
		}
		for _, partMeta := range topicMeta.PartitionMetadata {
			tp.Partitions = append(tp.Partitions, partMeta.Partition)
		}
		tps = append(tps, tp)
	}
	return tps, nil
}

type consumer struct {
	client *Client

	mu  sync.Mutex
	typ consumerType

	group consumerGroup

	// seq is a sequence number used reloading assignments from a new
	// asignment.
	seq uint64

	offsetsWaitingLoad *offsetsWaitingLoad

	sourcesReadyMu          sync.Mutex
	sourcesReadyCond        *sync.Cond
	sourcesReadyForDraining []*recordSource
}

func (c *consumer) addSourceReadyForDraining(source *recordSource) {
	c.sourcesReadyMu.Lock()
	c.sourcesReadyForDraining = append(c.sourcesReadyForDraining, source)
	c.sourcesReadyMu.Unlock()
	c.sourcesReadyCond.Broadcast()
}

func (c *Client) PollConsumer(ctx context.Context) Fetches {
	consumer := &c.consumer

	if consumerType(atomic.LoadUint32((*uint32)(&consumer.typ))) == consumerTypeUnset {
		return nil
	}

	var fetches Fetches

	fill := func() {
		consumer.sourcesReadyMu.Lock()
		for _, ready := range consumer.sourcesReadyForDraining {
			fetches = append(fetches, ready.takeBuffered())
		}
		consumer.sourcesReadyForDraining = nil
		consumer.sourcesReadyMu.Unlock()
	}

	fill()
	if len(fetches) > 0 {
		return fetches
	}

	done := make(chan struct{})
	quit := false
	go func() {
		consumer.sourcesReadyMu.Lock()
		defer consumer.sourcesReadyMu.Unlock()
		defer close(done)

		for !quit {
			if len(consumer.sourcesReadyForDraining) > 0 {
				return
			}
			consumer.sourcesReadyCond.Wait()
		}
	}()

	select {
	case <-ctx.Done():
	case <-done:
	}

	fill()
	return fetches
}

func (c *consumer) maybeInit(client *Client, typ consumerType) error {
	if c.typ == consumerTypeUnset {
		c.client = client
		atomic.StoreUint32((*uint32)(&c.typ), uint32(typ))
		c.typ = typ
		c.sourcesReadyCond = sync.NewCond(&c.sourcesReadyMu)
		return nil
	}
	if c.typ == typ {
		return nil
	}
	switch c.typ {
	case consumerTypeAssigned:
		return fmt.Errorf("cannot assign partitions to a client that is being used as a group consumer")
	case consumerTypeGroup:
		return fmt.Errorf("cannot assign partitions to a client that is being used as a direct partition consumer")
	}
	panic("unreachable")
}

// TODO arg: op (set, add, remove)
// If added, we need to keep original assignments in case of the mergeInto below
// This takes ownership of the assignments.
func (c *Client) AssignPartitions(assignments map[string]map[int32]Offset) error {
	consumer := &c.consumer
	consumer.mu.Lock()
	defer consumer.mu.Unlock()

	if err := consumer.maybeInit(c, consumerTypeAssigned); err != nil {
		return err
	}

	// Ensure all topics exist so that we will fetch their metadata.
	c.topicsMu.Lock()
	clientTopics := c.cloneTopics()
	for topic := range assignments {
		if _, exists := clientTopics[topic]; !exists {
			clientTopics[topic] = newTopicPartitions()
		}
	}
	c.topics.Store(clientTopics)
	c.topicsMu.Unlock()

	// If by chance we have a topic and partition loaded and the
	// assignments use exact offsets, we can avoid looking up offsets.
	for topic, partitions := range assignments {
		topicParts := clientTopics[topic].load() // must exist; ensured above
		if topicParts == nil {
			continue
		}

		for partition, offset := range partitions {
			part := topicParts.all[partition]
			if part == nil {
				continue
			}

			if offset.request >= 0 {
				part.consumption.setOffset(offset.request)
				delete(partitions, partition)
			}
		}
		if len(partitions) == 0 {
			delete(assignments, topic)
		}
	}

	// For all remaining offsets, await load.
	if len(assignments) > 0 {
		consumer.seq++
		consumer.offsetsWaitingLoad = &offsetsWaitingLoad{
			fromSeq: consumer.seq,
			waiting: assignments,
		}
		c.triggerUpdateMetadata()
	}
	return nil
}

// mergeInto is used to merge waiting offsets into a consumer.
//
// When we load partition offsets, we send many requests to all brokers
// responsible for topic partitions. All failing loads get merged back into the
// consumer for a future load retry.
func (o *offsetsWaitingLoad) mergeInto(c *consumer) {
	if len(o.waiting) == 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	if c.offsetsWaitingLoad != nil &&
		o.fromSeq < c.offsetsWaitingLoad.fromSeq {
		return
	}

	existing := c.offsetsWaitingLoad
	if existing == nil {
		c.offsetsWaitingLoad = o
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

	if len(existing.waiting) > 0 {
		c.client.triggerUpdateMetadata()
	}
}

func (c *consumer) doOnMetadataUpdate() {
	c.mu.Lock()
	toLoad := c.offsetsWaitingLoad
	c.offsetsWaitingLoad = nil
	// TODO inc tries here
	c.mu.Unlock()

	if toLoad == nil || toLoad.waiting == nil {
		return
	}

	c.tryOffsetLoad(toLoad)
}

func (c *consumer) tryOffsetLoad(toLoad *offsetsWaitingLoad) {
	// If any partitions do not exist in the metadata, or we cannot find
	// the broker leader for a partition, we reload the metadata.
	toReload := &offsetsWaitingLoad{fromSeq: toLoad.fromSeq}
	brokersToLoadFrom := make(map[*broker]*offsetsWaitingLoad)

	// For most of this function, we hold the broker mu so that we can
	// check if topic partition leaders exist.
	c.client.brokersMu.RLock()
	brokers := c.client.brokers

	// Map all waiting partition loads to the brokers that can load the
	// offsets for those partitions.
	topics := c.client.loadTopics()
	for topic, partitions := range toLoad.waiting {
		// The topicPartitions must exist, since AssignPartitions
		// creates the topic (empty) if the topic is new.
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

	c.client.brokersMu.RUnlock()

	for broker, brokerLoad := range brokersToLoadFrom {
		go c.tryBrokerOffsetLoad(broker, brokerLoad)
	}

	toReload.mergeInto(c)
}

func (c *consumer) tryBrokerOffsetLoad(broker *broker, load *offsetsWaitingLoad) {
	var resp *kmsg.ListOffsetsResponse
	var err error
	broker.wait(
		load.buildReq(),
		func(kresp kmsg.Response, respErr error) {
			if err = respErr; err != nil {
				return
			}
			resp = kresp.(*kmsg.ListOffsetsResponse)
		},
	)

	if err != nil {
		load.mergeInto(c)
		return
	}

	for _, responseTopic := range resp.Responses {
		topic := responseTopic.Topic
		waitingParts, ok := load.waiting[topic]
		if !ok {
			continue
		}

		for _, responsePartition := range responseTopic.PartitionResponses {
			partition := responsePartition.Partition
			waitingPart, ok := waitingParts[partition]
			if !ok {
				continue
			}

			err := kerr.ErrorForCode(responsePartition.ErrorCode)
			if err != nil {
				if !kerr.IsRetriable(err) {
					// TODO notify client users somehow
					// Maybe a single fake Fetch in the
					// first Poll.
					delete(waitingParts, partition)
				}
				continue
			}

			topicPartitions := c.client.loadTopics()[topic].load()
			topicPartition, ok := topicPartitions.all[partition]
			if !ok {
				continue // very weird
			}

			offset := responsePartition.Offset
			if waitingPart.request >= 0 {
				offset = waitingPart.request + waitingPart.relative
			}
			topicPartition.consumption.setOffset(offset)
		}
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
