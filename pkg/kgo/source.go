package kgo

import (
	"fmt"
	"sync"
	"time"

	"github.com/twmb/kafka-go/pkg/kerr"
	"github.com/twmb/kafka-go/pkg/kmsg"
)

type recordSource struct {
	broker *broker

	inflightSem chan struct{} // capacity of 1
	fillState   uint32

	consecutiveFailures int

	// guards all below
	mu sync.Mutex

	// consuming tracks topics, partitions, and offsets/epochs that this
	// source owns.
	allConsumptions      []*consumption
	allConsumptionsStart int

	buffered bufferedFetch
}

func newRecordSource(broker *broker) *recordSource {
	source := &recordSource{
		broker:      broker,
		inflightSem: make(chan struct{}, 1),
	}
	return source
}

func (source *recordSource) addConsumption(add *consumption) {
	source.mu.Lock()
	add.allConsumptionsIdx = len(source.allConsumptions)
	source.allConsumptions = append(source.allConsumptions, add)
	source.mu.Unlock()

	// We always clear the failing, since this could have been from moving
	// a failing partition from one source to another (clearing the fail).
	add.clearFailing()
}

func (source *recordSource) removeConsumption(rm *consumption) {
	source.mu.Lock()
	defer source.mu.Unlock()

	if rm.allConsumptionsIdx != len(source.allConsumptions)-1 {
		source.allConsumptions[rm.allConsumptionsIdx], source.allConsumptions[len(source.allConsumptions)-1] =
			source.allConsumptions[len(source.allConsumptions)-1], nil

		source.allConsumptions[rm.allConsumptionsIdx].allConsumptionsIdx = rm.allConsumptionsIdx
	} else {
		source.allConsumptions[rm.allConsumptionsIdx] = nil // do not let this source hang around
	}

	source.allConsumptions = source.allConsumptions[:len(source.allConsumptions)-1]
	if source.allConsumptionsStart == len(source.allConsumptions) {
		source.allConsumptionsStart = 0
	}
}

type consumption struct {
	topic     string
	partition int32

	keepControl bool

	mu sync.Mutex

	source             *recordSource
	allConsumptionsIdx int

	seqOffset

	// inUse is set whenever the consumption is chosen for an in flight
	// fetch request and reset to false if the fetch response has no usable
	// records or when the buffered usable records are taken.
	inUse bool

	// failing is set when we encounter a partition error.
	// It is always cleared on metadata update.
	failing bool

	// loadingOffsets is true when we are resetting offsets with
	// ListOffsets or with OffsetsForLeaderEpoch.
	//
	// This is unconditionally reset whenever assigning partitions
	// or when the requests mentioned in the prior sentence finish.
	loadingOffsets bool
}

// seqOffset is an offset we are consuming and the corresponding assign seq
// this offset is originally from. The seq is key to ensuring we do not
// return old fetches if a client's Assign* is called again.
type seqOffset struct {
	offset             int64
	lastConsumedEpoch  int32
	currentLeaderEpoch int32
	seq                uint64
}

// seqOffsetFrom is updated while processing a fetch response. One the response
// is taken, we only update the consumption's offset _if_ the seq is the same
// (by going through setOffset).
type seqOffsetFrom struct {
	seqOffset
	from *consumption
}

// use is called when adding an offset to a fetch request; from
// hereafter until the buffered response is taken, we update the offset
// in the frozen seqOffsetFrom view.
func (c *consumption) use() *seqOffsetFrom {
	c.inUse = true
	return &seqOffsetFrom{
		seqOffset: c.seqOffset,
		from:      c,
	}
}

// resetOffset is like setOffset, but strictly for bumping the consumption's
// sequence number so that consumption resets from its current offset/epoch.
func (c *consumption) resetOffset(fromSeq uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// fromSeq could be less than seq if this setOffset is from a
	// takeBuffered after assignment invalidation.
	if fromSeq < c.seq {
		return
	}
	c.seq = fromSeq
	c.failing = false
	c.inUse = false
	c.loadingOffsets = false
	if c.offset != -1 && c.source != nil {
		c.source.maybeBeginConsuming()
	}
}

// setOffset sets the consumptions offset and seq, doing nothing if the seq is
// out of date. The seq will be out of date if an Assign is called and then a
// buffered fetch is drained (invalidated).
//
// Otherwise, normally, buffered fetches call setOffset to update the
// consuption's offset and to allow the source to continue draining.
//
// If a buffered fetch had an error, this does not clear the error state. We
// leave that for metadata updating.
func (c *consumption) setOffset(
	currentEpoch int32,
	clearLoading bool,
	offset int64,
	epoch int32,
	fromSeq uint64,
) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// fromSeq could be less than seq if this setOffset is from a
	// takeBuffered after assignment invalidation.
	if fromSeq < c.seq {
		return
	}
	if fromSeq > c.seq {
		c.failing = false
		c.seq = fromSeq
	} else if currentEpoch < c.currentLeaderEpoch {
		// If the seq is the same but the current epoch is stale, this set
		// is from something out of date and a prior set bumped the epoch.
		return
	}
	c.inUse = false
	if clearLoading {
		c.loadingOffsets = false
	}

	c.offset = offset
	c.lastConsumedEpoch = epoch
	c.currentLeaderEpoch = currentEpoch
	// The source could theoretically be nil here if we loaded a failing
	// partition.
	if offset != -1 && c.source != nil {
		c.source.maybeBeginConsuming()
	}
}

// restartOffset resets a consumption to usable and triggers the source to
// begin consuming again. This is called if a consumption was used in a fetch
// that ultimately returned no new data.
func (c *consumption) setUnused(fromSeq uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// fromSeq could be less than seq if this setOffset is from a
	// takeBuffered after assignment invalidation.
	if fromSeq < c.seq {
		return
	}

	c.inUse = false
	// No need to maybeBeginConsuming since this is called from unuseAll
	// which allows the source to continue when it finishes.
}

// setFailing is called once a partition has an error response. The consumption
// is not used until a metadata update clears the failing state.
//
// In sinks, we only update recordBuffer state if the buffer is on the same
// sink when it is time to update. That is, we only update state if the buffer
// did not move due to a concurrent metadata update during a produce request.
//
// We do not need to worry about that type of movement for a c. At
// worst, an in flight fetch will see NotLeaderForPartition, setting the
// consumption to failing and triggering a metadata update. The metadata update
// will quickly clear the failing state and the consumption will resume as
// normal on the new source.
func (c *consumption) setFailing(fromSeq uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if fromSeq < c.seq {
		return
	}

	c.failing = true
}

// clearFailing is called to clear any failing state and compare
// the current leader epoch to our last leader epoch.
//
// This is called when a consumption is added to a source (to clear a failing
// state from migrating consumptions between sources) or when a metadata update
// sees the consumption is still on the same source.
//
// Note the source cannot be nil here, since nil sources correspond to load
// errors, and partitions with load errors do not call this.
func (c *consumption) clearFailing() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.failing = false

	// If we are in use, there is nothing to be gained by checking the
	// epoch now. Additionally, if we are fenced, then no use starting.
	if c.inUse || c.loadingOffsets {
		return
	}

	// We always restart draining if we can and if the partition is not in
	// use; it is possible that we were not failing in the erroring sense,
	// but that the broker the consumption was on disappeared and the
	// consumption migrated.
	if c.offset != -1 && !c.inUse {
		c.source.maybeBeginConsuming()
	}
}

func (c *consumption) setLoadingOffsets(fromSeq uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if fromSeq < c.seq {
		return
	}

	c.loadingOffsets = true
}

// bufferedFetch is a fetch response waiting to be consumed by the client, as
// well as offsests to update consumptions to once the fetch is taken.
type bufferedFetch struct {
	fetch      Fetch
	seq        uint64
	reqOffsets map[string]map[int32]*seqOffsetFrom
}

type fetchSeq struct {
	Fetch
	seq uint64
}

// takeBuffered drains a buffered fetch and updates offsets.
func (source *recordSource) takeBuffered() (Fetch, uint64) {
	r := source.buffered
	source.buffered = bufferedFetch{}
	go source.updateOffsets(r.reqOffsets)
	return r.fetch, r.seq
}

// updateOffsets is called when a buffered fetch is taken; we update all
// consumption offsets and set them usable for new fetches.
func (source *recordSource) updateOffsets(reqOffsets map[string]map[int32]*seqOffsetFrom) {
	for _, partitions := range reqOffsets {
		for _, o := range partitions {
			o.from.setOffset(o.currentLeaderEpoch, false, o.offset, o.lastConsumedEpoch, o.seq)
		}
	}
	<-source.inflightSem
}

// unuseAll is called when a fetch returns no data; we set all the
// consumptions to usable again so we can reissue a new fetch.
func (source *recordSource) unuseAll(reqOffsets map[string]map[int32]*seqOffsetFrom) {
	for _, partitions := range reqOffsets {
		for _, o := range partitions {
			o.from.setUnused(o.seq)
		}
	}
	<-source.inflightSem
}

func (source *recordSource) createRequest() (req *fetchRequest, again bool) {
	req = &fetchRequest{
		maxWait:        source.broker.client.cfg.maxWait,
		maxBytes:       source.broker.client.cfg.maxBytes,
		maxPartBytes:   source.broker.client.cfg.maxPartBytes,
		isolationLevel: source.broker.client.cfg.isolationLevel,
	}

	source.mu.Lock()
	defer source.mu.Unlock()

	consumptionIdx := source.allConsumptionsStart
	for i := 0; i < len(source.allConsumptions); i++ {
		c := source.allConsumptions[consumptionIdx]
		consumptionIdx = (consumptionIdx + 1) % len(source.allConsumptions)

		// Ensure this consumption cannot be moved across topicPartitions
		// while we using its fields.
		c.mu.Lock()

		// If the offset is -1, a metadata update added a consumption to
		// this source, but it is not yet in use.
		if c.offset == -1 || c.inUse || c.failing || c.loadingOffsets {
			c.mu.Unlock()
			continue
		}

		again = true
		req.fetchConsumptionLocked(c)
		c.mu.Unlock()
	}

	// We could have lost our only record buffer just before we grabbed the
	// lock above.
	if len(source.allConsumptions) > 0 {
		source.allConsumptionsStart = (source.allConsumptionsStart + 1) % len(source.allConsumptions)
	}

	return req, again
}

func (source *recordSource) maybeBeginConsuming() {
	if maybeBeginWork(&source.fillState) {
		go source.fill()
	}
}

func (source *recordSource) fill() {
	time.Sleep(time.Millisecond)

	again := true
	for again {
		source.inflightSem <- struct{}{}

		var req *fetchRequest
		req, again = source.createRequest()

		if req.numOffsets == 0 { // must be at least one if a consumption was usable
			again = maybeTryFinishWork(&source.fillState, again)
			<-source.inflightSem
			continue
		}

		source.broker.do(
			source.broker.client.ctx,
			req,
			func(resp kmsg.Response, err error) {
				source.handleReqResp(req, resp, err)
			},
		)
		again = maybeTryFinishWork(&source.fillState, again)
	}
}

func (source *recordSource) backoff() {
	source.consecutiveFailures++
	after := time.NewTimer(source.broker.client.cfg.retryBackoff(source.consecutiveFailures))
	defer after.Stop()
	select {
	case <-after.C:
	case <-source.broker.client.ctx.Done():
	}
}

func (source *recordSource) handleReqResp(req *fetchRequest, kresp kmsg.Response, err error) {
	var needsMetaUpdate bool

	if err != nil {
		source.backoff() // backoff before unuseAll to avoid inflight race
		source.unuseAll(req.offsets)
		source.broker.client.triggerUpdateMetadata()
		return
	}
	source.consecutiveFailures = 0

	resp := kresp.(*kmsg.FetchResponse)
	newFetch := Fetch{
		Topics: make([]FetchTopic, 0, len(resp.Topics)),
	}

	// We do not look at the overall ErrorCode; this should only be set if
	// using sessions, which we are not.
	//
	// If any partition errors with OffsetOutOfRange, we reload the offset
	// for that partition according to the client's configured offset
	// policy. Everything to reload gets stuffed into reloadOffsets, which
	// is merged into the consumer before a metadata update is triggered.
	reloadOffsets := offsetsWaitingLoad{
		fromSeq: req.maxSeq,
	}
	for _, rTopic := range resp.Topics {
		topic := rTopic.Topic
		topicOffsets, ok := req.offsets[topic]
		if !ok {
			continue
		}

		fetchTopic := FetchTopic{
			Topic:      topic,
			Partitions: make([]FetchPartition, 0, len(rTopic.Partitions)),
		}

		for i := range rTopic.Partitions {
			rPartition := &rTopic.Partitions[i]
			partition := rPartition.Partition
			partOffset, ok := topicOffsets[partition]
			if !ok {
				continue
			}

			fetchPart, partNeedsMetaUpdate := partOffset.processRespPartition(topic, resp.Version, rPartition)
			if len(fetchPart.Records) > 0 || fetchPart.Err != nil {
				fetchTopic.Partitions = append(fetchTopic.Partitions, fetchPart)
			}
			needsMetaUpdate = needsMetaUpdate || partNeedsMetaUpdate

			// If we are out of range, we reset to what we can.
			// With Kafka >= 2.1.0, we should only get offset out
			// of range if we fetch before the start, but a user
			// user could start past the end and want to reset to
			// the end. We respect that.
			if fetchPart.Err == kerr.OffsetOutOfRange {
				partOffset.from.setLoadingOffsets(partOffset.seq)
				reloadOffsets.setTopicPartForList(topic, partition, source.broker.client.cfg.resetOffset)

			} else if fetchPart.Err == kerr.FencedLeaderEpoch {
				// With fenced leader epoch, we notify an error only if
				// necessary after we find out if loss occurred.
				fetchPart.Err = nil
				// If we have consumed nothing, then we got unlucky
				// by being fenced right after we grabbed metadata.
				// We just refresh metadata and try again.
				if partOffset.lastConsumedEpoch >= 0 {
					partOffset.from.setLoadingOffsets(partOffset.seq)
					reloadOffsets.setTopicPartForEpoch(topic, partition, Offset{
						request:      partOffset.offset,
						epoch:        partOffset.lastConsumedEpoch,
						currentEpoch: partOffset.currentLeaderEpoch,
					})
				}
			}
		}

		if len(fetchTopic.Partitions) > 0 {
			newFetch.Topics = append(newFetch.Topics, fetchTopic)
		}
	}

	if !reloadOffsets.isEmpty() {
		consumer := &source.broker.client.consumer
		consumer.mu.Lock()
		reloadOffsets.mergeIntoLocked(consumer)
		consumer.mu.Unlock()
	}

	if needsMetaUpdate {
		source.broker.client.triggerUpdateMetadata()
	}

	if len(newFetch.Topics) > 0 {
		source.buffered = bufferedFetch{
			fetch:      newFetch,
			seq:        req.maxSeq,
			reqOffsets: req.offsets,
		}
		source.broker.client.consumer.addSourceReadyForDraining(req.maxSeq, source)
	} else {
		source.updateOffsets(req.offsets)
	}
}

// processRespPartition processes all records in all potentially compressed
// batches (or message sets) and returns a fetch partition containing those
// records.
//
// This returns that a metadata update is needed if any part has a recoverable
// error.
//
// Recoverable errors are stripped; if a partition has a recoverable error
// immediately with no records, it should be discarded.
func (o *seqOffsetFrom) processRespPartition(
	topic string,
	version int16,
	rPartition *kmsg.FetchResponseTopicPartition,
) (
	fetchPart FetchPartition,
	needsMetaUpdate bool,
) {
	fetchPart = FetchPartition{
		Partition:        rPartition.Partition,
		Err:              kerr.ErrorForCode(rPartition.ErrorCode),
		HighWatermark:    rPartition.HighWatermark,
		LastStableOffset: rPartition.LastStableOffset,
	}

	keepControl := o.from.keepControl

	switch version {
	case 0, 1:
		o.processV0Messages(topic, &fetchPart, kmsg.ReadV0Messages(rPartition.RecordBatches))
	case 2, 3:
		o.processV1Messages(topic, &fetchPart, kmsg.ReadV1Messages(rPartition.RecordBatches))
	default:
		batches := kmsg.ReadRecordBatches(rPartition.RecordBatches)
		var numPartitionRecords int
		for i := range batches {
			numPartitionRecords += int(batches[i].NumRecords)
		}
		fetchPart.Records = make([]*Record, 0, numPartitionRecords)
		aborter := buildAborter(rPartition)
		for i := range batches {
			o.processRecordBatch(topic, &fetchPart, &batches[i], keepControl, aborter)
			if fetchPart.Err != nil {
				break
			}
		}
	}

	if fetchPart.Err != nil {
		needsMetaUpdate = true
	}

	switch fetchPart.Err {
	case nil:
		// do nothing

	case kerr.UnknownTopicOrPartition,
		kerr.NotLeaderForPartition,
		kerr.ReplicaNotAvailable,
		kerr.KafkaStorageError,
		kerr.UnknownLeaderEpoch:

		fetchPart.Err = nil // recoverable with client backoff; hide the error
		fallthrough

	default:
		// - bad auth
		// - unsupported compression
		// - unsupported message version
		// - unknown error
		o.from.setFailing(o.seq)
	}

	return fetchPart, needsMetaUpdate
}

type aborter map[int64][]int64

func buildAborter(rPartition *kmsg.FetchResponseTopicPartition) aborter {
	if len(rPartition.AbortedTransactions) == 0 {
		return nil
	}
	a := make(aborter)
	for _, abort := range rPartition.AbortedTransactions {
		a[abort.ProducerID] = append(a[abort.ProducerID], abort.FirstOffset)
	}
	return a
}

func (a aborter) shouldAbortBatch(b *kmsg.RecordBatch) bool {
	if len(a) == 0 || b.Attributes&0b0001_0000 == 0 {
		return false
	}
	pidAborts := a[b.ProducerID]
	if len(pidAborts) == 0 {
		return false
	}
	// If the first offset in this batch is less than the first offset
	// aborted, then this batch is not aborted.
	if b.FirstOffset < pidAborts[0] {
		return false
	}
	return true
}

//////////////////////////////////////
// processing records to fetch part //
//////////////////////////////////////

func (o *seqOffset) processRecordBatch(
	topic string,
	fetchPart *FetchPartition,
	batch *kmsg.RecordBatch,
	keepControl bool,
	aborter aborter,
) {
	if batch.Magic != 2 {
		fetchPart.Err = fmt.Errorf("unknown batch magic %d", batch.Magic)
		return
	}
	rawRecords := batch.Records
	if compression := byte(batch.Attributes & 0x0007); compression != 0 {
		var err error
		if rawRecords, err = decompress(rawRecords, compression); err != nil {
			fetchPart.Err = fmt.Errorf("unable to decompress batch: %v", err)
			return
		}
	}
	krecords, err := kmsg.ReadRecords(int(batch.NumRecords), rawRecords)
	if err != nil {
		fetchPart.Err = fmt.Errorf("invalid record batch: %v", err)
		return
	}

	abortBatch := aborter.shouldAbortBatch(batch)
	var lastRecord *Record
	for i := range krecords {
		record := recordToRecord(
			topic,
			fetchPart.Partition,
			batch,
			&krecords[i],
		)
		lastRecord = record
		o.maybeAddRecord(fetchPart, record, keepControl, abortBatch)
	}

	if abortBatch && lastRecord != nil && lastRecord.Attrs.IsControl() {
		remaining := aborter[batch.ProducerID][1:]
		if len(remaining) == 0 {
			delete(aborter, batch.ProducerID)
		} else {
			aborter[batch.ProducerID] = remaining
		}
	}
}

func (o *seqOffset) processV1Messages(
	topic string,
	fetchPart *FetchPartition,
	messages []kmsg.MessageV1,
) {
	for i := range messages {
		message := &messages[i]
		compression := byte(message.Attributes & 0x0003)
		if compression == 0 {
			o.processV1Message(topic, fetchPart, message)
			continue
		}

		rawMessages, err := decompress(message.Value, compression)
		if err != nil {
			fetchPart.Err = fmt.Errorf("unable to decompress messages: %v", err)
			return
		}
		innerMessages := kmsg.ReadV1Messages(rawMessages)
		if len(innerMessages) == 0 {
			return
		}
		firstOffset := message.Offset - int64(len(innerMessages)) + 1
		for i := range innerMessages {
			innerMessage := &innerMessages[i]
			innerMessage.Offset = firstOffset + int64(i)
			o.processV1Message(topic, fetchPart, innerMessage)
		}
	}
}

func (o *seqOffset) processV1Message(
	topic string,
	fetchPart *FetchPartition,
	message *kmsg.MessageV1,
) {
	if message.Magic != 1 {
		fetchPart.Err = fmt.Errorf("unknown message magic %d", message.Magic)
		return
	}
	if message.Attributes != 0 {
		fetchPart.Err = fmt.Errorf("unknown attributes on uncompressed message %d", message.Attributes)
		return
	}
	record := v1MessageToRecord(topic, fetchPart.Partition, message)
	o.maybeAddRecord(fetchPart, record, false, false)
}

func (o *seqOffset) processV0Messages(
	topic string,
	fetchPart *FetchPartition,
	messages []kmsg.MessageV0,
) {
	for i := range messages {
		message := &messages[i]
		compression := byte(message.Attributes & 0x0003)
		if compression == 0 {
			o.processV0Message(topic, fetchPart, message)
			continue
		}

		rawMessages, err := decompress(message.Value, compression)
		if err != nil {
			fetchPart.Err = fmt.Errorf("unable to decompress messages: %v", err)
			return
		}
		innerMessages := kmsg.ReadV0Messages(rawMessages)
		if len(innerMessages) == 0 {
			return
		}
		firstOffset := message.Offset - int64(len(innerMessages)) + 1
		for i := range innerMessages {
			innerMessage := &innerMessages[i]
			innerMessage.Offset = firstOffset + int64(i)
			o.processV0Message(topic, fetchPart, innerMessage)
		}
	}
}

func (o *seqOffset) processV0Message(
	topic string,
	fetchPart *FetchPartition,
	message *kmsg.MessageV0,
) {
	if message.Magic != 0 {
		fetchPart.Err = fmt.Errorf("unknown message magic %d", message.Magic)
		return
	}
	if message.Attributes != 0 {
		fetchPart.Err = fmt.Errorf("unknown attributes on uncompressed message %d", message.Attributes)
		return
	}
	record := v0MessageToRecord(topic, fetchPart.Partition, message)
	o.maybeAddRecord(fetchPart, record, false, false)
}

// maybeAddRecord keeps a record if it is within our range of offsets to keep.
// However, if the record is being aborted, or the record is a control record
// and the client does not want to keep control records, this does not keep
// the record and instead only updates the seqOffset metadata.
func (o *seqOffset) maybeAddRecord(fetchPart *FetchPartition, record *Record, keepControl bool, abort bool) {
	if record.Offset < o.offset {
		// We asked for offset 5, but that was in the middle of a
		// batch; we got offsets 0 thru 4 that we need to skip.
		return
	}

	// We only keep control records if specifically requested.
	if record.Attrs.IsControl() && !keepControl {
		abort = true
	}
	if !abort {
		fetchPart.Records = append(fetchPart.Records, record)
	}

	// The record offset may be much larger than our expected offset if the
	// topic is compacted. That is fine; we ensure increasing offsets and
	// only keep the resulting offset if the seq is the same.
	o.offset = record.Offset + 1
	o.lastConsumedEpoch = record.LeaderEpoch
}

///////////////////////////////
// kmsg.Record to kgo.Record //
///////////////////////////////

func timeFromMillis(millis int64) time.Time {
	return time.Unix(0, millis*1e6)
}

// recordToRecord converts a kmsg.RecordBatch's Record to a kgo Record.
func recordToRecord(
	topic string,
	partition int32,
	batch *kmsg.RecordBatch,
	record *kmsg.Record,
) *Record {
	h := make([]RecordHeader, 0, len(record.Headers))
	for _, kv := range record.Headers {
		h = append(h, RecordHeader{
			Key:   kv.Key,
			Value: kv.Value,
		})
	}

	return &Record{
		Key:         record.Key,
		Value:       record.Value,
		Headers:     h,
		Timestamp:   timeFromMillis(batch.FirstTimestamp + int64(record.TimestampDelta)),
		Attrs:       RecordAttrs{uint8(batch.Attributes)},
		Topic:       topic,
		Partition:   partition,
		LeaderEpoch: batch.PartitionLeaderEpoch,
		Offset:      batch.FirstOffset + int64(record.OffsetDelta),
	}
}

func messageAttrsToRecordAttrs(attrs int8, v0 bool) RecordAttrs {
	uattrs := uint8(attrs)
	timestampType := uattrs & 0b0000_0100
	uattrs = uattrs&0b0000_0011 | timestampType<<1
	if v0 {
		uattrs = uattrs | 0b1000_0000
	}
	return RecordAttrs{uattrs}
}

func v0MessageToRecord(
	topic string,
	partition int32,
	message *kmsg.MessageV0,
) *Record {
	return &Record{
		Key:         message.Key,
		Value:       message.Value,
		Attrs:       messageAttrsToRecordAttrs(message.Attributes, true),
		Topic:       topic,
		Partition:   partition,
		LeaderEpoch: -1,
		Offset:      message.Offset,
	}
}

func v1MessageToRecord(
	topic string,
	partition int32,
	message *kmsg.MessageV1,
) *Record {
	return &Record{
		Key:         message.Key,
		Value:       message.Value,
		Timestamp:   timeFromMillis(message.Timestamp),
		Attrs:       messageAttrsToRecordAttrs(message.Attributes, false),
		Topic:       topic,
		Partition:   partition,
		LeaderEpoch: -1,
		Offset:      message.Offset,
	}
}

//////////////////
// fetchRequest //
//////////////////

type fetchRequest struct {
	version      int16
	maxWait      int32
	maxBytes     int32
	maxPartBytes int32

	isolationLevel int8

	maxSeq     uint64
	numOffsets int
	offsets    map[string]map[int32]*seqOffsetFrom
}

func (f *fetchRequest) fetchConsumptionLocked(c *consumption) {
	if f.offsets == nil {
		f.offsets = make(map[string]map[int32]*seqOffsetFrom)
	}
	partitions := f.offsets[c.topic]
	if partitions == nil {
		partitions = make(map[int32]*seqOffsetFrom)
		f.offsets[c.topic] = partitions
	}
	o := c.use()

	// AssignPartitions or AssignGroup could have been called in the middle
	// of our req being built, invalidating part of the req. We invalidate
	// by only tracking the latest seq.
	if o.seq > f.maxSeq {
		f.maxSeq = o.seq
		f.numOffsets = 0
	}
	f.numOffsets++
	partitions[c.partition] = o
}

func (*fetchRequest) Key() int16           { return 1 }
func (*fetchRequest) MaxVersion() int16    { return 11 }
func (f *fetchRequest) SetVersion(v int16) { f.version = v }
func (f *fetchRequest) GetVersion() int16  { return f.version }
func (f *fetchRequest) IsFlexible() bool   { return false } // version 11 is not flexible
func (f *fetchRequest) AppendTo(dst []byte) []byte {
	req := kmsg.FetchRequest{
		Version:        f.version,
		ReplicaID:      -1,
		MaxWaitMillis:  f.maxWait,
		MinBytes:       1,
		MaxBytes:       f.maxBytes,
		IsolationLevel: f.isolationLevel,
		SessionID:      -1,
		SessionEpoch:   -1, // KIP-227, we do not want to support
		Topics:         make([]kmsg.FetchRequestTopic, 0, len(f.offsets)),
	}
	for topic, partitions := range f.offsets {
		req.Topics = append(req.Topics, kmsg.FetchRequestTopic{
			Topic:      topic,
			Partitions: make([]kmsg.FetchRequestTopicPartition, 0, len(partitions)),
		})
		reqTopic := &req.Topics[len(req.Topics)-1]
		for partition, seqOffset := range partitions {
			if seqOffset.seq < f.maxSeq {
				continue // all offsets in the fetch must come from the same seq
			}
			reqTopic.Partitions = append(reqTopic.Partitions, kmsg.FetchRequestTopicPartition{
				Partition:          partition,
				CurrentLeaderEpoch: seqOffset.currentLeaderEpoch,
				FetchOffset:        seqOffset.offset,
				LogStartOffset:     -1,
				PartitionMaxBytes:  f.maxPartBytes,
			})
		}
	}
	return req.AppendTo(dst)
}
func (f *fetchRequest) ResponseKind() kmsg.Response {
	return &kmsg.FetchResponse{Version: f.version}
}
