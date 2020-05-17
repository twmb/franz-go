package kgo

import (
	"context"
	"sync"

	"github.com/twmb/kafka-go/pkg/kerr"
	"github.com/twmb/kafka-go/pkg/kmsg"
)

// Offset is a message offset in a partition.
type Offset struct {
	request      int64
	relative     int64
	epoch        int32
	currentEpoch int32 // set by us
}

// NewOffsetcreates and returns an offset to use in AssignPartitions.
//
// The default offset begins at the end.
func NewOffset() Offset {
	return Offset{
		request: -1,
		epoch:   -1,
	}
}

// AtStart returns a copy of the calling offset, changing the returned offset
// to begin at the beginning of a partition.
func (o Offset) AtStart() Offset {
	o.request = -2
	return o
}

// AtEnd returns a copy of the calling offset, changing the returned offset to
// begin at the end of a partition.
func (o Offset) AtEnd() Offset {
	o.request = -1
	return o
}

// Relative returns a copy of the calling offset, changing the returned offset
// to be n relative to what it currently is. If the offset is beginning at the
// end, Relative(-100) will begin 100 before the end.
func (o Offset) Relative(n int64) Offset {
	o.relative = n
	return o
}

// WithEpoch returns a copy of the calling offset, changing the returned offset
// to use the given epoch. This epoch is used for truncation detection; the
// default of -1 implies no truncation detection.
func (o Offset) WithEpoch(e int32) Offset {
	if e < 0 {
		e = -1
	}
	o.epoch = e
	return o
}

// At returns a copy of the calling offset, changing the returned offset
// to begin at exactly the requested offset.
func (o Offset) At(at int64) Offset {
	if at < 0 {
		at = -2
	}
	o.request = at
	return o
}

type consumerType int8

const (
	consumerTypeUnset = iota
	consumerTypeDirect
	consumerTypeGroup
)

type consumer struct {
	cl *Client

	mu     sync.Mutex
	group  *groupConsumer
	direct *directConsumer
	typ    consumerType

	// fetchMu gaurds concurrent PollFetches. While polling should happen
	// serially, we must ensure it, especially to ensure we track updating
	// offsets properly.
	fetchMu sync.Mutex

	usingPartitions []*topicPartition

	offsetsWaitingLoad *offsetsWaitingLoad

	sourcesReadyMu          sync.Mutex
	sourcesReadyCond        *sync.Cond
	sourcesReadyForDraining []*source
	fakeReadyForDraining    []fetchSeq

	// seq corresponds to the number of assigned groups or partitions.
	//
	// It is updated under both the sources ready mu and potentially
	// also the consumer mu itself.
	//
	// Incrementing it invalidates prior assignments and fetches.
	seq uint64

	// dead is set when the client closes; this being true means that any
	// Assign does nothing (aside from unassigning everything prior).
	dead bool
}

// fetchSeq is used for fake fetches that have no corresponding cursor.
type fetchSeq struct {
	Fetch
	seq uint64
}

// unassignPrior invalidates old assignments, ensures that nothing is assigned,
// and leaves any group.
func (c *consumer) unassignPrior() {
	c.assignPartitions(nil, assignInvalidateAll) // invalidate old assignments
	if c.typ == consumerTypeGroup {
		c.typ = consumerTypeUnset
		c.group.leave()
	}
}

// addSourceReadyForDraining tracks that a source needs its buffered fetch
// consumed. If the seq this source is from is out of date, the source is
// immediately drained.
func (c *consumer) addSourceReadyForDraining(seq uint64, source *source) {
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

// addFakeReadyForDraining saves a fake fetch that has important partition
// errors--data loss or auth failures.
func (c *consumer) addFakeReadyForDraining(topic string, partition int32, err error, seq uint64) {
	c.sourcesReadyMu.Lock()
	defer c.sourcesReadyMu.Unlock()
	if seq < c.seq {
		return
	}

	c.fakeReadyForDraining = append(c.fakeReadyForDraining, fetchSeq{
		Fetch{
			Topics: []FetchTopic{{
				Topic: topic,
				Partitions: []FetchPartition{{
					Partition: partition,
					Err:       err,
				}},
			}},
		},
		seq,
	})

	c.sourcesReadyCond.Broadcast()
}

// PollFetches waits for fetches to be available, returning as soon as any
// broker returns a fetch. If the ctx quits, this function quits.
//
// It is important to check all partition errors in the returned fetches. If
// any partition has a fatal error and actually had no records, fake fetch will
// be injected with the error.
func (cl *Client) PollFetches(ctx context.Context) Fetches {
	c := &cl.consumer
	c.fetchMu.Lock()
	defer c.fetchMu.Unlock()

	var fetches Fetches

	if cl.cfg.logger.Level() >= LogLevelDebug {
		defer func() {
			if len(fetches) > 0 {
				toppars := make(map[int32][2]int64)
				for _, fetch := range fetches {
					for _, topic := range fetch.Topics {
						for _, partition := range topic.Partitions {
							toppars[partition.Partition] = [2]int64{
								partition.Records[0].Offset,
								partition.Records[len(partition.Records)-1].Offset,
							}
						}
					}
				}
				cl.cfg.logger.Log(LogLevelDebug, "returning fetch", "part_stop_start", toppars)
			}
		}()
	}

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
		for _, ready := range c.fakeReadyForDraining {
			fetch, seq := ready.Fetch, ready.seq
			if seq < c.seq {
				continue
			}
			fetches = append(fetches, fetch)
		}
		c.sourcesReadyForDraining = nil

		// Before releasing the sourcesReadyMu, we want to update our
		// uncommitted. If we updated after, then we could end up with
		// weird interactions with group invalidations where we return
		// a stale fetch after committing in onRevoke.
		//
		// A blocking onRevoke commit, on finish, allows a new group
		// session to start. If we returned stale fetches that did not
		// have their uncommitted offset tracked, then we would allow
		// duplicates.
		if c.typ == consumerTypeGroup && len(fetches) > 0 {
			c.group.updateUncommitted(fetches)
		}

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

		for !quit && len(c.sourcesReadyForDraining) == 0 {
			c.sourcesReadyCond.Wait()
		}
	}()

	select {
	case <-ctx.Done():
		c.sourcesReadyMu.Lock()
		quit = true
		c.sourcesReadyMu.Unlock()
		c.sourcesReadyCond.Broadcast()
	case <-done:
	}

	fill()
	return fetches
}

// maybeAssignPartitions assigns partitions if seq is equal to the consumer
// seq, returning true if assignment occured. If true, this also updates seq to
// the new seq.
func (c *consumer) maybeAssignPartitions(seq *uint64, assignments map[string]map[int32]Offset, how assignHow) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.seq != *seq {
		return false
	}
	c.assignPartitions(assignments, how)
	*seq = c.seq
	return true
}

// assignHow controls how assignPartitions operates.
type assignHow int8

const (
	// This option simply assigns new offsets, doing nothing with existing
	// offsets / active fetches / buffered fetches.
	assignWithoutInvalidating assignHow = iota

	// This option invalidates active fetches so they will not buffer and
	// drops all buffered fetches, and then continues to assign the new
	// assignments.
	assignInvalidateAll

	// This option does not assign, but instead invalidates any active
	// fetches for "assigned" (actually lost) partitions. This additionally
	// drops all buffered fetches, because they could contain partitions we
	// lost. Thus, with this option, the actual offset in the map is
	// meaningless / a dummy offset.
	assignInvalidateMatching

	// The counterpart to assignInvalidateMatching, assignSetMatching
	// resets all matching partitions to the specified offset / epoch.
	assignSetMatching
)

func (h assignHow) String() string {
	switch h {
	case assignWithoutInvalidating:
		return "assign without invalidating"
	case assignInvalidateAll:
		return "assign invalidate all"
	case assignInvalidateMatching:
		return "assign invalidate matching"
	case assignSetMatching:
		return "assign set matching"
	}
	return ""
}

// assignPartitions, called under the consumer's mu, is used to set new
// cursors or add to the existing cursors.
func (c *consumer) assignPartitions(assignments map[string]map[int32]Offset, how assignHow) {
	seq := c.seq

	c.cl.cfg.logger.Log(LogLevelInfo, "assigning partitions", "how", how.String())

	if how != assignWithoutInvalidating {
		// In this block, we immediately want to ensure that nothing
		// currently buffered will be returned and that no active
		// fetches will keep their results.
		//
		// This lock ensures that nothing new will be buffered,
		// and below bump the seq num on all cursors to ensure
		// that
		// 1) now unused cursors will not continue to loop
		// 2) still used cursors will continue to loop at the
		//    appropriate offset.
		c.sourcesReadyMu.Lock()
		c.seq++
		seq = c.seq

		keep := c.usingPartitions[:0]
		for _, usedPartition := range c.usingPartitions {
			needsReset := true
			if how == assignInvalidateAll {
				usedPartition.cursor.setOffset(usedPartition.leaderEpoch, true, -1, -1, seq) // case 1
				needsReset = false
			} else {
				if matchTopic, ok := assignments[usedPartition.cursor.topic]; ok {
					if matchPartition, ok := matchTopic[usedPartition.cursor.partition]; ok {
						needsReset = false
						if how == assignInvalidateMatching {
							usedPartition.cursor.setOffset(usedPartition.leaderEpoch, true, -1, -1, seq) // case 1
						} else { // how == assignSetMatching
							usedPartition.cursor.setOffset(usedPartition.leaderEpoch, true, matchPartition.request, matchPartition.epoch, seq) // case 2
							keep = append(keep, usedPartition)
						}
					}
				}
			}
			if needsReset {
				usedPartition.cursor.resetOffset(seq) // case 2
				keep = append(keep, usedPartition)
			}
		}

		// Before releasing the lock, we drain any buffered (now stale)
		// fetches that were waiting to be polled.
		for _, ready := range c.sourcesReadyForDraining {
			ready.takeBuffered()
		}
		c.fakeReadyForDraining = nil
		c.sourcesReadyForDraining = nil
		c.sourcesReadyMu.Unlock()

		c.usingPartitions = keep
	}

	// This assignment could contain nothing (for the purposes of
	// invalidating active fetches), so we only do this if needed.
	if len(assignments) == 0 || (how == assignInvalidateMatching || how == assignSetMatching) {
		return
	}

	c.cl.cfg.logger.Log(LogLevelInfo, "assign requires loading offsets")

	// If we have a topic and partition loaded and the assignments use
	// exact offsets, we can avoid looking up offsets.
	waiting := offsetsWaitingLoad{
		fromSeq: seq,
	}
	clientTopics := c.cl.loadTopics()
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

			// First, if the request is exact, get rid of the relative
			// portion.
			if offset.request >= 0 {
				offset.request = offset.request + offset.relative
				if offset.request < 0 {
					offset.request = 0
				}
				offset.relative = 0
			}

			// If we are requesting an exact offset and have an
			// epoch, we do truncation detection and then use the
			// offset.
			//
			// Otherwise, an epoch is specified without an exact
			// request which is useless for us, or a request is
			// specified without a known epoch.
			//
			// If an exact offset is specified, we use it. Without
			// an epoch, if it is out of bounds, we just reset
			// appropriately. If an offset is unspecified, we list
			// offsets to find out what to use.
			if offset.request >= 0 && offset.epoch >= 0 {
				waiting.setTopicPartForEpoch(topic, partition, offset)
				delete(partitions, partition)
			} else if offset.request >= 0 {
				part.cursor.setOffset(part.leaderEpoch, true, offset.request, offset.epoch, seq)
				c.usingPartitions = append(c.usingPartitions, part)
				delete(partitions, partition)
			}
		}
		if len(partitions) == 0 {
			delete(assignments, topic)
		}
	}

	c.cl.cfg.logger.Log(LogLevelInfo, "assign setting offsets to load", "to_load", assignments)

	waiting.waitingList = assignments
	if !waiting.isEmpty() {
		waiting.mergeIntoLocked(c)
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

// mergeIntoLocked is called directly from assignPartitions, which already
// has the consumer locked.
func (o *offsetsWaitingLoad) mergeIntoLocked(c *consumer) {
	if o.isEmpty() {
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

	for topic, partitions := range o.waitingList {
		curTopic, exists := existing.waitingList[topic]
		if !exists {
			existing.setTopicPartsForList(topic, partitions)
			continue
		}
		for partition, offset := range partitions {
			curTopic[partition] = offset
		}
	}

	for topic, partitions := range o.waitingEpoch {
		curTopic, exists := existing.waitingEpoch[topic]
		if !exists {
			existing.setTopicPartsForEpoch(topic, partitions)
			continue
		}
		for partition, ask := range partitions {
			curTopic[partition] = ask
		}
	}
}

func (c *consumer) deletePartition(p *topicPartition) {
	c.mu.Lock()
	defer c.mu.Unlock()

	for i, using := range c.usingPartitions {
		if using == p {
			// No calling setOffset here to invalidate the cursor;
			// partition deletion does not cause a seq bump. But,
			// the cursor has been removed from its source, meaning
			// it will not be consumed anymore.
			c.usingPartitions[i] = c.usingPartitions[len(c.usingPartitions)-1]
			c.usingPartitions = c.usingPartitions[:len(c.usingPartitions)-1]
			break
		}
	}

	switch c.typ {
	case consumerTypeUnset:
		return
	case consumerTypeDirect:
		c.direct.deleteUsing(p.cursor.topic, p.cursor.partition)
	case consumerTypeGroup:
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
		c.assignPartitions(c.direct.findNewAssignments(c.cl.loadTopics()), assignWithoutInvalidating)
	case consumerTypeGroup:
		c.group.findNewAssignments(c.cl.loadTopics())
	}

	// Finally, process any updates.
	c.resetAndLoadOffsets()
}

// resetAndLoadOffsets empties offsetsWaitingLoad and tries loading them.
func (c *consumer) resetAndLoadOffsets() {
	toLoad := c.offsetsWaitingLoad
	c.offsetsWaitingLoad = nil
	if toLoad.isEmpty() {
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
	for topic, partitions := range toLoad.waitingList {
		// The topicPartitions must exist, since assignPartitions
		// creates the topic if the topic is new.
		topicPartitions := topics[topic].load()

		for partition, offset := range partitions {
			topicPartition, exists := topicPartitions.all[partition]
			if !exists {
				toReload.setTopicPartForList(topic, partition, offset)
				continue
			}

			broker := brokers[topicPartition.leader]
			if broker == nil { // should not happen
				toReload.setTopicPartForList(topic, partition, offset)
				continue
			}

			addLoad := brokersToLoadFrom[broker]
			if addLoad == nil {
				addLoad = &offsetsWaitingLoad{fromSeq: toLoad.fromSeq}
				brokersToLoadFrom[broker] = addLoad
			}
			// Before we set this offset to load from the broker,
			// we must set what we understand to be the current
			// epoch.
			offset.currentEpoch = topicPartition.leaderEpoch
			addLoad.setTopicPartForList(topic, partition, offset)
		}
	}

	// Now we do that exact same logic for the waiting epoch stuff.
	for topic, partitions := range toLoad.waitingEpoch {
		topicPartitions := topics[topic].load()
		for partition, offset := range partitions {
			topicPartition, exists := topicPartitions.all[partition]
			if !exists {
				toReload.setTopicPartForEpoch(topic, partition, offset)
				continue
			}
			broker := brokers[topicPartition.leader]
			if broker == nil {
				toReload.setTopicPartForEpoch(topic, partition, offset)
				continue
			}
			addLoad := brokersToLoadFrom[broker]
			if addLoad == nil {
				addLoad = &offsetsWaitingLoad{fromSeq: toLoad.fromSeq}
				brokersToLoadFrom[broker] = addLoad
			}
			offset.currentEpoch = topicPartition.leaderEpoch
			addLoad.setTopicPartForEpoch(topic, partition, offset)
		}
	}

	c.cl.brokersMu.RUnlock()

	for broker, brokerLoad := range brokersToLoadFrom {
		if len(brokerLoad.waitingList) > 0 {
			go c.tryBrokerOffsetLoadList(broker, brokerLoad)
		}
		if len(brokerLoad.waitingEpoch) > 0 {
			go c.tryBrokerOffsetLoadEpoch(broker, brokerLoad)
		}
	}

	toReload.mergeInto(c)
}

type offsetsWaitingLoad struct {
	fromSeq      uint64
	waitingList  map[string]map[int32]Offset
	waitingEpoch map[string]map[int32]Offset
}

func (o *offsetsWaitingLoad) isEmpty() bool {
	return o == nil || len(o.waitingList) == 0 && len(o.waitingEpoch) == 0
}

func (o *offsetsWaitingLoad) maybeInitList() {
	if o.waitingList == nil {
		o.waitingList = make(map[string]map[int32]Offset)
	}
}
func (o *offsetsWaitingLoad) setTopicPartsForList(topic string, partitions map[int32]Offset) {
	o.maybeInitList()
	o.waitingList[topic] = partitions
}
func (o *offsetsWaitingLoad) setTopicPartForList(topic string, partition int32, offset Offset) {
	o.maybeInitList()
	parts := o.waitingList[topic]
	if parts == nil {
		parts = make(map[int32]Offset)
		o.waitingList[topic] = parts
	}
	parts[partition] = offset
}

func (c *consumer) tryBrokerOffsetLoadList(broker *broker, load *offsetsWaitingLoad) {
	c.cl.cfg.logger.Log(LogLevelInfo, "issuing list offsets request", "broker", broker.id, "addr", broker.addr)
	kresp, err := broker.waitResp(c.cl.ctx,
		load.buildListReq(c.cl.cfg.isolationLevel))
	if err != nil {
		load.mergeInto(c)
		return
	}
	resp := kresp.(*kmsg.ListOffsetsResponse)

	type toSet struct {
		topicPartition *topicPartition
		offset         int64
		leaderEpoch    int32
		currentEpoch   int32
	}
	var toSets []toSet

	for _, rTopic := range resp.Topics {
		topic := rTopic.Topic
		waitingParts, ok := load.waitingList[topic]
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
					c.addFakeReadyForDraining(topic, partition, err, load.fromSeq)
					delete(waitingParts, partition)
				}
				continue
			}

			topicPartitions := c.cl.loadTopics()[topic].load()
			topicPartition, ok := topicPartitions.all[partition]
			if !ok {
				continue // very weird
			}

			delete(waitingParts, partition)
			if len(waitingParts) == 0 { // avoid reload
				delete(load.waitingList, topic)
			}

			offset := rPartition.Offset + waitingPart.relative
			if waitingPart.request >= 0 {
				offset = waitingPart.request + waitingPart.relative
			}
			if offset < 0 {
				offset = 0
			}
			leaderEpoch := rPartition.LeaderEpoch
			if resp.Version < 4 {
				leaderEpoch = -1
			}

			toSets = append(toSets, toSet{
				topicPartition,
				offset,
				leaderEpoch,
				waitingPart.currentEpoch,
			})
		}
	}

	if len(load.waitingList) > 0 { // Kafka did not reply to everything (odd)
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

	c.cl.cfg.logger.Log(LogLevelInfo, "listed offsets, setting offsets")
	for _, toSet := range toSets {
		toSet.topicPartition.cursor.setOffset(toSet.currentEpoch, true, toSet.offset, toSet.leaderEpoch, c.seq)
		c.usingPartitions = append(c.usingPartitions, toSet.topicPartition)
	}
}

func (o *offsetsWaitingLoad) buildListReq(isolationLevel int8) *kmsg.ListOffsetsRequest {
	req := &kmsg.ListOffsetsRequest{
		ReplicaID:      -1,
		IsolationLevel: isolationLevel,
		Topics:         make([]kmsg.ListOffsetsRequestTopic, 0, len(o.waitingList)),
	}
	for topic, partitions := range o.waitingList {
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
				CurrentLeaderEpoch: offset.currentEpoch, // KIP-320
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

// the following functions are exactly the same, but on the epoch map
func (o *offsetsWaitingLoad) maybeInitEpoch() {
	if o.waitingEpoch == nil {
		o.waitingEpoch = make(map[string]map[int32]Offset)
	}
}
func (o *offsetsWaitingLoad) setTopicPartsForEpoch(topic string, partitions map[int32]Offset) {
	o.maybeInitEpoch()
	o.waitingEpoch[topic] = partitions
}
func (o *offsetsWaitingLoad) setTopicPartForEpoch(topic string, partition int32, offset Offset) {
	o.maybeInitEpoch()
	parts := o.waitingEpoch[topic]
	if parts == nil {
		parts = make(map[int32]Offset)
		o.waitingEpoch[topic] = parts
	}
	parts[partition] = offset
}

func (c *consumer) tryBrokerOffsetLoadEpoch(broker *broker, load *offsetsWaitingLoad) {
	c.cl.cfg.logger.Log(LogLevelInfo, "issuing offset for leader epoch request", "broker", broker.id, "addr", broker.addr)
	kresp, err := broker.waitResp(c.cl.ctx, load.buildEpochReq())
	if err != nil {
		load.mergeInto(c)
		return
	}
	resp := kresp.(*kmsg.OffsetForLeaderEpochResponse)
	// If the response version is < 2, then we cannot do truncation
	// detection. We fallback to just listing offsets and hoping for
	// the best. Of course, we should not be in this function if we
	// never had a current leader to begin with, but it is possible
	// we talked to one new broker and now are talking to a different
	// older one in the same cluster.
	if resp.Version < 2 {
		(&offsetsWaitingLoad{
			fromSeq:     load.fromSeq,
			waitingList: load.waitingEpoch,
		}).mergeInto(c)
		return
	}

	type toSet struct {
		topicPartition *topicPartition
		offset         int64
		leaderEpoch    int32
		currentEpoch   int32
	}
	var toSets []toSet

	for _, rTopic := range resp.Topics {
		topic := rTopic.Topic
		waitingParts, ok := load.waitingEpoch[topic]
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
					c.addFakeReadyForDraining(topic, partition, err, load.fromSeq)
					delete(waitingParts, partition)
				}
				continue
			}

			topicPartitions := c.cl.loadTopics()[topic].load()
			topicPartition, ok := topicPartitions.all[partition]
			if !ok {
				continue // very weird
			}

			delete(waitingParts, partition)
			if len(waitingParts) == 0 { // avoid reload
				delete(load.waitingEpoch, topic)
			}

			if waitingPart.request < 0 {
				panic("we should not be here with unknown offsets")
			}
			offset := waitingPart.request
			if rPartition.EndOffset < offset {
				offset = rPartition.EndOffset
				err = &ErrDataLoss{topic, partition, offset, rPartition.EndOffset}
				c.addFakeReadyForDraining(topic, partition, err, load.fromSeq)
			}

			toSets = append(toSets, toSet{
				topicPartition,
				offset,
				rPartition.LeaderEpoch,
				waitingPart.currentEpoch,
			})
		}
	}

	if len(load.waitingEpoch) > 0 { // Kafka did not reply to everything (odd)
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
	c.cl.cfg.logger.Log(LogLevelInfo, "found offset leader epoch, setting offsets")
	for _, toSet := range toSets {
		toSet.topicPartition.cursor.setOffset(toSet.currentEpoch, true, toSet.offset, toSet.leaderEpoch, c.seq)
		c.usingPartitions = append(c.usingPartitions, toSet.topicPartition)
	}
}

func (o *offsetsWaitingLoad) buildEpochReq() *kmsg.OffsetForLeaderEpochRequest {
	req := &kmsg.OffsetForLeaderEpochRequest{
		ReplicaID: -1,
		Topics:    make([]kmsg.OffsetForLeaderEpochRequestTopic, 0, len(o.waitingEpoch)),
	}
	for topic, partitions := range o.waitingEpoch {
		parts := make([]kmsg.OffsetForLeaderEpochRequestTopicPartition, 0, len(partitions))
		for partition, offset := range partitions {
			if offset.epoch < 0 {
				panic("we should not be here with negative epochs")
			}
			parts = append(parts, kmsg.OffsetForLeaderEpochRequestTopicPartition{
				Partition:          partition,
				CurrentLeaderEpoch: offset.currentEpoch,
				LeaderEpoch:        offset.epoch,
			})
		}
		req.Topics = append(req.Topics, kmsg.OffsetForLeaderEpochRequestTopic{
			Topic:      topic,
			Partitions: parts,
		})
	}
	return req
}
