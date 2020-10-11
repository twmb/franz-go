package kgo

import (
	"context"
	"sync"

	"github.com/twmb/frang/pkg/kerr"
	"github.com/twmb/frang/pkg/kmsg"
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

	offsetsWaitingLoad offsetsLoad
	offsetsLoading     offsetsLoad

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
	//
	// This does not change the seq. All other options below bump the seq.
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

	isLoading := !c.offsetsWaitingLoad.isEmpty() // stash if we are loading before we bump, which moves loading to load

	// If we are invalidating or setting matching offsets, then we need to
	// bump the seq's for loads or waiting loads that we are keeping.
	//
	// Otherwise, they will eventually load but not set offsets because
	// their seq's would be out of date.
	if how == assignInvalidateMatching || how == assignSetMatching {
		c.bumpLoadingFetches()
	}

	// Bumping loading could have moved stuff back to load, so if we
	// were not loading and load is non-empty, we need to refresh
	// metadata to trigger the eventual offset load.
	if !isLoading && !c.offsetsWaitingLoad.isEmpty() {
		c.cl.cfg.logger.Log(LogLevelInfo, "assign invalidated offsets that were loading and bumped some back to waiting load; triggering meta update")
		c.cl.triggerUpdateMetadata()
	}

	// This assignment could contain nothing (for the purposes of
	// invalidating active fetches), so we only do this if needed.
	if len(assignments) == 0 || how == assignInvalidateMatching || how == assignSetMatching {
		return
	}

	c.cl.cfg.logger.Log(LogLevelInfo, "assign requires loading offsets")

	clientTopics := c.cl.loadTopics()

	for topic, partitions := range assignments {
		topicParts := clientTopics[topic].load()
		// clientTopics should always have all topics we are interested
		// in. This is ensured in AssignPartitions, or in AssignGroup,
		// or in metadata updating if consuming regex topics.
		if topicParts == nil {
			continue
		}

		for partition, offset := range partitions {
			// First, if the request is exact, get rid of the relative
			// portion. We are modifying a copy of the offset, i.e. we
			// are appropriately not modfying 'assignments' itself.
			if offset.request >= 0 {
				offset.request = offset.request + offset.relative
				if offset.request < 0 {
					offset.request = 0
				}
				offset.relative = 0
			}

			// If we are requesting an exact offset with an epoch,
			// we do truncation detection and then use the offset.
			//
			// Otherwise, an epoch is specified without an exact
			// request which is useless for us, or a request is
			// specified without a known epoch.
			if offset.request >= 0 && offset.epoch >= 0 {
				c.offsetsWaitingLoad.epoch.setLoadOffset(topic, partition, offset, -1, seq)
				continue
			}

			// If an exact offset is specified and we have loaded
			// the partition, we use it. Without an epoch, if it is
			// out of bounds, we just reset appropriately.
			//
			// If an offset is unspecified or we have not loaded
			// the partition, we list offsets to find out what to
			// use.
			part := topicParts.all[partition]
			if offset.request >= 0 && part != nil {
				part.cursor.setOffset(part.leaderEpoch, true, offset.request, -1, seq)
				c.usingPartitions = append(c.usingPartitions, part)
				continue
			}

			c.offsetsWaitingLoad.list.setLoadOffset(topic, partition, offset, -1, seq)
		}
	}

	if !c.offsetsWaitingLoad.isEmpty() {
		if !isLoading {
			c.cl.cfg.logger.Log(LogLevelInfo, "assign offsets waiting load nonempty; triggering meta update",
				"need_list", c.offsetsWaitingLoad.list, "need_epoch", c.offsetsWaitingLoad.epoch)
		} else {
			c.cl.cfg.logger.Log(LogLevelInfo, "assign offsets waiting load nonempty; load in progress, not triggering meta update",
				"need_list", c.offsetsWaitingLoad.list, "need_epoch", c.offsetsWaitingLoad.epoch)
		}
		c.cl.triggerUpdateMetadata()
	}

}

// bumpLoadingFetches, called from assignOffsets, bumps the seq's for any load
// or loading requests. We do this so that once the request eventually is
// issued and returns, we do not ignore the return due to out of date seq's.
//
// For simplicity, this moves everything loading back to load; their loading
// fetches are per-broker and we don't bump those loading seq's.
func (c *consumer) bumpLoadingFetches() {
	newSeq := c.seq
	oldSeq := newSeq - 1

	var matching map[string][]int32
	switch c.typ {
	case consumerTypeGroup:
		matching = c.group.nowAssigned
	case consumerTypeDirect:
		matching = make(map[string][]int32)
		for topic, partitions := range c.direct.using {
			for partition := range partitions {
				matching[topic] = append(matching[topic], partition)
			}
		}
	}

	for topic, partitions := range matching {
		for _, partition := range partitions {
			for _, loads := range []struct {
				src offsetLoadMap
				dst *offsetLoadMap
			}{
				{c.offsetsWaitingLoad.list, &c.offsetsWaitingLoad.list},
				{c.offsetsWaitingLoad.epoch, &c.offsetsWaitingLoad.epoch},
				{c.offsetsLoading.list, &c.offsetsWaitingLoad.list},
				{c.offsetsLoading.epoch, &c.offsetsWaitingLoad.epoch},
			} {
				if existing, exists := loads.src.removeLoad(topic, partition, oldSeq); exists {
					loads.dst.setLoadOffset(topic, partition, existing.Offset, existing.replica, newSeq)
				}
			}
		}
	}
}

// mergeInto is used by a source itself if it detects it needs to reload some
// offsets that it was working with.
func (o *offsetsLoad) mergeInto(c *consumer) {
	c.mu.Lock()
	defer c.mu.Unlock()

	isLoading := !c.offsetsWaitingLoad.isEmpty()
	var needLoad bool

	for _, loads := range []struct {
		src offsetLoadMap
		dst *offsetLoadMap
	}{
		{o.list, &c.offsetsWaitingLoad.list},
		{o.epoch, &c.offsetsWaitingLoad.epoch},
	} {
		for topic, partitions := range loads.src {
			for partition, offset := range partitions {
				if offset.seq == c.seq {
					loads.dst.setLoad(topic, partition, offset)
					needLoad = true
				}
			}
		}
	}

	if needLoad && !isLoading {
		c.cl.triggerUpdateMetadata()
	}
}

// loadingToWaitingLocked moves everything that is loading back to waiting
// load. This is used for any failed load.
func (o *offsetsLoad) loadingToWaiting(c *consumer) {
	c.mu.Lock()
	defer c.mu.Unlock()
	o.loadingToWaitingLocked(c)
}

func (o *offsetsLoad) loadingToWaitingLocked(c *consumer) {
	if o.isEmpty() {
		return
	}

	isLoading := !c.offsetsWaitingLoad.isEmpty()
	var needLoad bool

	// For all offsets, if the offset is the same offset that is in the
	// load map (same seq), we remove it from the load map. Then, if the
	// offset seq is the same as the consumer seq, we move it to the
	// loading map.
	for _, loads := range []struct {
		src offsetLoadMap
		chk offsetLoadMap
		dst *offsetLoadMap
	}{
		{o.list, c.offsetsLoading.list, &c.offsetsWaitingLoad.list},
		{o.epoch, c.offsetsLoading.epoch, &c.offsetsWaitingLoad.epoch},
	} {
		for topic, partitions := range loads.src {
			for partition, offset := range partitions {
				if _, exists := loads.chk.removeLoad(topic, partition, offset.seq); exists && offset.seq == c.seq {
					loads.dst.setLoad(topic, partition, offset)
					needLoad = true
				}
			}
		}
	}

	if needLoad && !isLoading {
		c.cl.triggerUpdateMetadata()
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
	// First, clear all stale (old seq) offsets.
	for _, loads := range []offsetLoadMap{
		c.offsetsWaitingLoad.list,
		c.offsetsWaitingLoad.epoch,
		c.offsetsLoading.list,
		c.offsetsLoading.epoch,
	} {
		for topic, partitions := range loads {
			for partition, offset := range partitions {
				if offset.seq < c.seq {
					delete(partitions, partition)
				}
			}
			if len(partitions) == 0 {
				delete(loads, topic)
			}
		}
	}

	c.cl.cfg.logger.Log(LogLevelDebug, "moving waiting load to loading", "waiting", c.offsetsWaitingLoad, "load", c.offsetsLoading)

	// Now, move everything from the waiting load map to the loading map,
	// clear the loading map, and load the load map.
	for _, loads := range []struct {
		src offsetLoadMap
		dst *offsetLoadMap
	}{
		{c.offsetsWaitingLoad.list, &c.offsetsLoading.list},
		{c.offsetsWaitingLoad.epoch, &c.offsetsLoading.epoch},
	} {
		for topic, partitions := range loads.src {
			for partition, offset := range partitions {
				// we do not remove because we clear below
				loads.dst.setLoad(topic, partition, offset)
			}
		}
	}

	toLoad := c.offsetsWaitingLoad.clear()
	if toLoad.isEmpty() {
		return
	}
	c.tryOffsetLoad(toLoad)
}

func (c *consumer) tryOffsetLoad(toLoad offsetsLoad) {
	// If any partitions do not exist in the metadata, or we cannot find
	// the broker leader for a partition, we reload the metadata.
	var toReload offsetsLoad
	brokersToLoadFrom := make(map[*broker]*offsetsLoad)

	// For most of this function, we hold the broker mu so that we can
	// check if topic partition leaders exist.
	c.cl.brokersMu.RLock()
	brokers := c.cl.brokers

	// Map all waiting partition loads to the brokers that can load the
	// offsets for those partitions.
	topics := c.cl.loadTopics()

	for _, loads := range []struct {
		src    offsetLoadMap
		dst    *offsetLoadMap
		isList bool
	}{
		{toLoad.list, &toReload.list, true},
		{toLoad.epoch, &toReload.epoch, false},
	} {
		for topic, partitions := range loads.src {
			topicPartitions := topics[topic].load()

			for partition, offset := range partitions {
				dst := loads.dst

				if topicPartition, exists := topicPartitions.all[partition]; exists {
					brokerID := topicPartition.leader
					if offset.replica != -1 {
						// Fetching from followers can issue list offsets
						// against the follower itself, not the leader.
						brokerID = offset.replica
					}
					if broker := brokers[brokerID]; broker != nil {
						brokerLoad := brokersToLoadFrom[broker]
						if brokerLoad == nil {
							brokerLoad = new(offsetsLoad)
							brokersToLoadFrom[broker] = brokerLoad
						}
						// We have to set the offset's currentEpoch
						// for our load requests.
						offset.currentEpoch = topicPartition.leaderEpoch
						if loads.isList {
							dst = &brokerLoad.list
						} else {
							dst = &brokerLoad.epoch
						}
					}
				}

				dst.setLoad(topic, partition, offset)
			}
		}
	}

	c.cl.brokersMu.RUnlock()

	for broker, brokerLoad := range brokersToLoadFrom {
		c.cl.cfg.logger.Log(LogLevelDebug, "offsets to load broker", "broker", broker.id, "load", brokerLoad)
		if len(brokerLoad.list) > 0 {
			go c.tryBrokerOffsetLoadList(broker, brokerLoad.list)
		}
		if len(brokerLoad.epoch) > 0 {
			go c.tryBrokerOffsetLoadEpoch(broker, brokerLoad.epoch)
		}
	}

	if !toReload.isEmpty() {
		c.cl.cfg.logger.Log(LogLevelDebug, "offsets to reload", "reload", toReload)
		// We area already under consumer mu from doOnMetadataUpdate.
		toReload.loadingToWaitingLocked(c)
	}
}

type offsetLoad struct {
	seq     uint64
	replica int32 // -1 means leader
	Offset
}

type offsetLoadMap map[string]map[int32]offsetLoad

func (o *offsetLoadMap) setLoadOffset(t string, p int32, offset Offset, replica int32, seq uint64) {
	o.setLoad(t, p, offsetLoad{seq, replica, offset})
}
func (o *offsetLoadMap) setLoad(t string, p int32, load offsetLoad) {
	if *o == nil {
		*o = make(offsetLoadMap)
	}
	if (*o)[t] == nil {
		(*o)[t] = make(map[int32]offsetLoad)
	}
	(*o)[t][p] = load
}
func (o offsetLoadMap) removeLoad(t string, p int32, seq uint64) (offsetLoad, bool) {
	if o == nil {
		return offsetLoad{}, false
	}
	ps := o[t]
	if ps == nil {
		return offsetLoad{}, false
	}
	existing, exists := ps[p]
	if !exists {
		return offsetLoad{}, false
	}
	if seq < existing.seq {
		return offsetLoad{}, false
	}
	delete(ps, p)
	return existing, true
}

type offsetsLoad struct {
	list  offsetLoadMap
	epoch offsetLoadMap
}

func (o *offsetsLoad) isEmpty() bool {
	return o == nil || len(o.list) == 0 && len(o.epoch) == 0
}
func (o *offsetsLoad) clear() offsetsLoad { r := *o; *o = offsetsLoad{}; return r }

func (c *consumer) tryBrokerOffsetLoadList(broker *broker, load offsetLoadMap) {
	kresp, err := broker.waitResp(c.cl.ctx,
		load.buildListReq(c.cl.cfg.isolationLevel))
	if err != nil {
		(&offsetsLoad{list: load}).loadingToWaiting(c)
		return
	}
	resp := kresp.(*kmsg.ListOffsetsResponse)

	type toSet struct {
		topic          string
		partition      int32
		topicPartition *topicPartition
		offset         int64
		leaderEpoch    int32
		load           offsetLoad
	}
	var toSets []toSet

	for _, rTopic := range resp.Topics {
		topic := rTopic.Topic
		waitingParts, ok := load[topic]
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
					c.addFakeReadyForDraining(topic, partition, err, waitingPart.seq)
					delete(waitingParts, partition)
				}
				continue
			}

			topicPartitions := c.cl.loadTopics()[topic].load()
			topicPartition, ok := topicPartitions.all[partition]
			if !ok {
				continue // we have not yet loaded the partition
			}

			delete(waitingParts, partition)
			if len(waitingParts) == 0 {
				delete(load, topic)
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
				topic,
				partition,
				topicPartition,
				offset,
				leaderEpoch,
				waitingPart,
			})
		}
	}

	if len(load) > 0 {
		(&offsetsLoad{list: load}).loadingToWaiting(c)
	}
	if len(toSets) == 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, toSet := range toSets {
		// Always try removing the load; only set the cursor to use the
		// load if the load's seq is not stale.
		if _, exists := c.offsetsLoading.list.removeLoad(toSet.topic, toSet.partition, toSet.load.seq); exists && toSet.load.seq == c.seq {
			toSet.topicPartition.cursor.setOffset(toSet.load.currentEpoch, true, toSet.offset, toSet.leaderEpoch, c.seq)
			c.usingPartitions = append(c.usingPartitions, toSet.topicPartition)
		}
	}
}

func (o offsetLoadMap) buildListReq(isolationLevel int8) *kmsg.ListOffsetsRequest {
	req := &kmsg.ListOffsetsRequest{
		ReplicaID:      -1,
		IsolationLevel: isolationLevel,
		Topics:         make([]kmsg.ListOffsetsRequestTopic, 0, len(o)),
	}
	for topic, partitions := range o {
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

func (c *consumer) tryBrokerOffsetLoadEpoch(broker *broker, load offsetLoadMap) {
	kresp, err := broker.waitResp(c.cl.ctx, load.buildEpochReq())
	if err != nil {
		(&offsetsLoad{epoch: load}).loadingToWaiting(c)
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
		c.mu.Lock()
		loading := offsetsLoad{list: c.offsetsLoading.epoch}
		c.offsetsLoading.epoch = nil
		c.mu.Unlock()
		loading.loadingToWaiting(c)
		return
	}

	type toSet struct {
		topic          string
		partition      int32
		topicPartition *topicPartition
		offset         int64
		leaderEpoch    int32
		load           offsetLoad
	}
	var toSets []toSet

	for _, rTopic := range resp.Topics {
		topic := rTopic.Topic
		waitingParts, ok := load[topic]
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
					c.addFakeReadyForDraining(topic, partition, err, waitingPart.seq)
					delete(waitingParts, partition)
				}
				continue
			}

			topicPartitions := c.cl.loadTopics()[topic].load()
			topicPartition, ok := topicPartitions.all[partition]
			if !ok {
				continue // we have not yet loaded the partition
			}

			delete(waitingParts, partition)
			if len(waitingParts) == 0 {
				delete(load, topic)
			}

			if waitingPart.request < 0 {
				panic("we should not be here with unknown offsets")
			}
			offset := waitingPart.request
			if rPartition.EndOffset < offset {
				offset = rPartition.EndOffset
				err = &ErrDataLoss{topic, partition, offset, rPartition.EndOffset}
				c.addFakeReadyForDraining(topic, partition, err, waitingPart.seq)
			}

			toSets = append(toSets, toSet{
				topic,
				partition,
				topicPartition,
				offset,
				rPartition.LeaderEpoch,
				waitingPart,
			})
		}
	}

	if len(load) > 0 {
		(&offsetsLoad{epoch: load}).loadingToWaiting(c)
	}
	if len(toSets) == 0 {
		return
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	for _, toSet := range toSets {
		// Always try removing the load; only set the cursor to use the
		// load if the load's seq is not stale.
		if _, exists := c.offsetsLoading.epoch.removeLoad(toSet.topic, toSet.partition, toSet.load.seq); exists && toSet.load.seq == c.seq {
			toSet.topicPartition.cursor.setOffset(toSet.load.currentEpoch, true, toSet.offset, toSet.leaderEpoch, c.seq)
			c.usingPartitions = append(c.usingPartitions, toSet.topicPartition)
		}
	}
}

func (o offsetLoadMap) buildEpochReq() *kmsg.OffsetForLeaderEpochRequest {
	req := &kmsg.OffsetForLeaderEpochRequest{
		ReplicaID: -1,
		Topics:    make([]kmsg.OffsetForLeaderEpochRequestTopic, 0, len(o)),
	}
	for topic, partitions := range o {
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
