package kgo

import (
	"fmt"
	"sync"
	"time"

	"github.com/twmb/kgo/kerr"
	"github.com/twmb/kgo/kmsg"
)

// TODO doc that if fetch partition errors midway thru, Fetch will have valid
// records to consume
// TODO backoff consumptions on partition errors

type recordSource struct {
	broker *broker

	inflightSem chan struct{} // capacity of 1
	fillState   uint32

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
	topicPartition *topicPartition

	mu sync.Mutex

	source             *recordSource
	allConsumptionsIdx int

	seqOffset
}

// seqOffset is an offset we are consuming and the corresponding assign seq
// this offset is originally from. The seq is key to ensuring we do not
// return old fetches if a client's Assign* is called again.
type seqOffset struct {
	offset int64
	seq    uint64
}

// seqOffsetFrom is updated while processing a fetch response. One the response
// is taken, we only update the consumption's offset _if_ the seq is the same
// (by going through setOffset).
type seqOffsetFrom struct {
	seqOffset
	from *consumption
}

// freezeFrom is called when adding an offset to a fetch request; from
// hereafter until the buffered response is taken, we update the offset
// in the frozen seqOffsetFrom view.
func (c *consumption) freezeFrom() *seqOffsetFrom {
	return &seqOffsetFrom{
		seqOffset: c.seqOffset,
		from:      c,
	}
}

// setOffset sets the consumptions offset and seq, doing nothing if the seq is
// out of date. The seq will be out of date if an Assign is called and then a
// buffered fetch is drained (invalidated).
//
// Otherwise, normally, buffered fetches call setOffset to update the
// consuption's offset and to allow the source to continue draining.
//
// Note that, since this is always the entry to update offsets, we do not need
// to do much complicated "is this still the source" management if the
// consumption moves across sources due to metadata updates. Buffered fetches
// will update the consumption and then simply start the new source.
func (consumption *consumption) setOffset(offset int64, fromSeq uint64) {
	consumption.mu.Lock()
	defer consumption.mu.Unlock()

	// fromSeq could be less than seq if this setOffset is from a
	// takeBuffered after assignment invalidation.
	if fromSeq < consumption.seq {
		return
	}

	consumption.offset = offset
	consumption.seq = fromSeq
	source := consumption.source

	if offset != -1 {
		source.maybeBeginConsuming()
	}
}

// bufferedFetch is a fetch response waiting to be consumed by the client, as
// well as offsests to update consumptions to once the fetch is taken.
type bufferedFetch struct {
	fetch      Fetch
	seq        uint64
	reqOffsets map[string]map[int32]*seqOffsetFrom
}

// takeBuffered drains a buffered fetch and updates offsets.
func (source *recordSource) takeBuffered() (Fetch, uint64) {
	r := source.buffered
	source.buffered = bufferedFetch{}
	go source.updateOffsets(r.reqOffsets)
	return r.fetch, r.seq
}

func (source *recordSource) updateOffsets(reqOffsets map[string]map[int32]*seqOffsetFrom) {
	for _, partitions := range reqOffsets {
		for _, o := range partitions {
			o.from.setOffset(o.offset, o.seq)
		}
	}
	<-source.inflightSem
}

func (source *recordSource) createRequest() (req *fetchRequest, again bool) {
	req = &fetchRequest{
		maxWait:      source.broker.client.cfg.consumer.maxWait,
		maxBytes:     source.broker.client.cfg.consumer.maxBytes,
		maxPartBytes: source.broker.client.cfg.consumer.maxPartBytes,
	}

	source.mu.Lock()
	defer source.mu.Unlock()

	consumptionIdx := source.allConsumptionsStart
	for i := 0; i < len(source.allConsumptions); i++ {
		consumption := source.allConsumptions[consumptionIdx]
		consumptionIdx = (consumptionIdx + 1) % len(source.allConsumptions)

		// Ensure this consumption cannot be moved across topicPartitions
		// while we using its fields.
		consumption.mu.Lock()

		// If the offset is -1, a metadata update added a consumption to
		// this source, but it is not yet in use.
		if consumption.offset == -1 {
			consumption.mu.Unlock()
			continue
		}

		again = true
		req.addConsumptionLocked(consumption)
		consumption.mu.Unlock()
	}

	source.allConsumptionsStart = (source.allConsumptionsStart + 1) % len(source.allConsumptions)

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

		if req.numOffsets == 0 {
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

func (source *recordSource) handleReqResp(req *fetchRequest, resp kmsg.Response, err error) {
	var needMetaUpdate bool

	if err != nil {
		// TODO
		// ErrBrokerDead: ok
		return
	}

	r := resp.(*kmsg.FetchResponse)
	newFetch := Fetch{
		Topics: make([]FetchTopic, 0, len(r.Topics)),
	}

	if err = kerr.ErrorForCode(r.ErrorCode); err != nil {
		// TODO
		// ErrBrokerDead: ok
		return
	}

	for _, rTopic := range r.Topics {
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

			fetchPart, partNeedsMetaUpdate := partOffset.processRespPartition(topic, r.Version, rPartition)
			if len(fetchPart.Records) > 0 || fetchPart.Err != nil {
				fetchTopic.Partitions = append(fetchTopic.Partitions, fetchPart)
			}
			needMetaUpdate = needMetaUpdate || partNeedsMetaUpdate
		}

		if len(fetchTopic.Partitions) > 0 {
			newFetch.Topics = append(newFetch.Topics, fetchTopic)
		}
	}

	if needMetaUpdate {
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
		<-source.inflightSem
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
func (o *seqOffset) processRespPartition(
	topic string,
	version int16,
	rPartition *kmsg.FetchResponseTopicPartition,
) (
	fetchPart FetchPartition,
	needMetaUpdate bool,
) {
	fetchPart = FetchPartition{
		Partition:        rPartition.Partition,
		Err:              kerr.ErrorForCode(rPartition.ErrorCode),
		HighWatermark:    rPartition.HighWatermark,
		LastStableOffset: rPartition.LastStableOffset,
	}

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
		for i := range batches {
			o.processRecordBatch(topic, &fetchPart, &batches[i])
			if fetchPart.Err != nil {
				break
			}
		}
	}

	switch fetchPart.Err {
	case kerr.UnknownTopicOrPartition,
		kerr.NotLeaderForPartition,
		kerr.ReplicaNotAvailable,
		kerr.KafkaStorageError,
		kerr.UnknownLeaderEpoch,
		kerr.FencedLeaderEpoch:

		needMetaUpdate = true
		fetchPart.Err = nil
		// TODO backoff

	default:
		// Fatal:
		// - bad auth
		// - unsupported compression
		// - unsupported message version
		// - out of range offset
		// - unknown error
		// TODO backoff permanently?
	}

	return fetchPart, needMetaUpdate
}

//////////////////////////////////////
// processing records to fetch part //
//////////////////////////////////////

func (o *seqOffset) processRecordBatch(
	topic string,
	fetchPart *FetchPartition,
	batch *kmsg.RecordBatch,
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
	for i := range krecords {
		record := recordToRecord(topic, fetchPart.Partition, batch, &krecords[i])
		o.maybeAddRecord(fetchPart, record)
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
	o.maybeAddRecord(fetchPart, record)
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
	o.maybeAddRecord(fetchPart, record)
}

func (o *seqOffset) maybeAddRecord(fetchPart *FetchPartition, record *Record) {
	if record.Offset < o.offset {
		// We asked for offset 5, but that was in the middle of a
		// batch; we got offsets 0 thru 4 that we need to skip.
		return
	}
	if record.Offset != o.offset {
		// We asked for offset 5, then the client user reset the
		// offset to something else while this was inflight.
		// This response out of date.
		return
	}
	fetchPart.Records = append(fetchPart.Records, record)
	o.offset++
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
		Key:           record.Key,
		Value:         record.Value,
		Headers:       h,
		Timestamp:     timeFromMillis(batch.FirstTimestamp + int64(record.TimestampDelta)),
		TimestampType: int8((batch.Attributes & 0x0008) >> 3),
		Topic:         topic,
		Partition:     partition,
		Offset:        batch.FirstOffset + int64(record.OffsetDelta),
	}
}

func v0MessageToRecord(
	topic string,
	partition int32,
	message *kmsg.MessageV0,
) *Record {
	return &Record{
		Key:           message.Key,
		Value:         message.Value,
		TimestampType: -1,
		Topic:         topic,
		Partition:     partition,
		Offset:        message.Offset,
	}
}

func v1MessageToRecord(
	topic string,
	partition int32,
	message *kmsg.MessageV1,
) *Record {
	return &Record{
		Key:           message.Key,
		Value:         message.Value,
		Timestamp:     timeFromMillis(message.Timestamp),
		TimestampType: (message.Attributes & 0x0004) >> 2,
		Topic:         topic,
		Partition:     partition,
		Offset:        message.Offset,
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

	maxSeq     uint64
	numOffsets int
	offsets    map[string]map[int32]*seqOffsetFrom
}

func (f *fetchRequest) addConsumptionLocked(c *consumption) {
	if f.offsets == nil {
		f.offsets = make(map[string]map[int32]*seqOffsetFrom)
	}
	topic := c.topicPartition.topic
	partitions := f.offsets[topic]
	if partitions == nil {
		partitions = make(map[int32]*seqOffsetFrom)
		f.offsets[topic] = partitions
	}
	partition := c.topicPartition.partition
	o := c.freezeFrom()

	// AssignPartitions or AssignGroup could have been called in the middle
	// of our req being built, invalidating part of the req. We invalidate
	// by only tracking the latest seq.
	if o.seq > f.maxSeq {
		f.maxSeq = o.seq
		f.numOffsets = 0
	}
	f.numOffsets++
	partitions[partition] = o
}

func (*fetchRequest) Key() int16           { return 1 }
func (*fetchRequest) MaxVersion() int16    { return 11 }
func (f *fetchRequest) SetVersion(v int16) { f.version = v }
func (f *fetchRequest) GetVersion() int16  { return f.version }
func (f *fetchRequest) AppendTo(dst []byte) []byte {
	req := kmsg.FetchRequest{
		Version:      f.version,
		ReplicaID:    -1,
		MaxWaitTime:  f.maxWait,
		MinBytes:     1,
		MaxBytes:     f.maxBytes,
		SessionID:    -1,
		SessionEpoch: -1, // KIP-227, we do not want to support
		Topics:       make([]kmsg.FetchRequestTopic, 0, len(f.offsets)),
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
				CurrentLeaderEpoch: -1, // KIP-320
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
