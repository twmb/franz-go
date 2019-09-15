package kgo

import (
	"fmt"
	"sync"
	"time"

	"github.com/twmb/kgo/kerr"
	"github.com/twmb/kgo/kmsg"
)

// TODO introduce backoff below
// TODO cleanup code at bottom
// TODO fixup offset if message set / record batch decoding fails.
//      If any batch fails, we set error, but we may have bumped offset.

type recordSource struct {
	broker *broker

	inflightSem chan struct{} // capacity of 1

	fillState uint32

	mu sync.Mutex

	// consuming tracks topics, partitions, and offsets/epochs that this
	// source owns.
	allConsumptions []*consumption

	allConsumptionsStart int

	buffered Fetch
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

	source.maybeBeginConsuming()
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

	offset int64
}

func (consumption *consumption) setOffset(offset int64) {
	consumption.mu.Lock()
	consumption.offset = offset
	source := consumption.source
	consumption.mu.Unlock()

	source.maybeBeginConsuming()
}

func (source *recordSource) createRequest() (req *fetchRequest, again bool) {
	req = new(fetchRequest)

	source.mu.Lock()
	defer source.mu.Unlock()

	consumptionIdx := source.allConsumptionsStart
	for i := 0; i < len(source.allConsumptions); i++ {
		consumption := source.allConsumptions[consumptionIdx]
		consumptionIdx = (consumptionIdx + 1) % len(source.allConsumptions)

		// Ensure this consumption cannot be moved across topicPartitions
		// while we using its fields.
		consumption.mu.Lock()

		// If the offset is -1, a metadata update added a consuption to
		// this source, but it is not yet in use.
		if consumption.offset == -1 {
			consumption.mu.Unlock()
			continue
		}

		again = true
		req.addTopicPartitionConsumption(
			consumption.topicPartition.topic,
			consumption.topicPartition.partition,
			consumption,
		)

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

		if len(req.consumptions) == 0 {
			again = maybeTryFinishWork(&source.fillState, again)
			<-source.inflightSem
			continue
		}

		source.broker.doSequencedAsyncPromise(
			req,
			func(resp kmsg.Response, err error) {
				source.handleReqResp(req, resp, err)
			},
		)
		again = maybeTryFinishWork(&source.fillState, again)
	}
}

func (source *recordSource) handleReqResp(req *fetchRequest, resp kmsg.Response, err error) {
	var needMetadataUpdate bool

	source.mu.Lock()
	defer source.mu.Unlock()

	if err != nil {
		// TODO
		// ErrBrokerDead: ok
		return
	}

	r := resp.(*kmsg.FetchResponse)
	newFetch := Fetch{
		Topics: make([]FetchTopic, 0, len(r.Responses)),
	}

	if err = kerr.ErrorForCode(r.ErrorCode); err != nil {
		// TODO
		// ErrBrokerDead: ok
		return
	}

	for _, responseTopic := range r.Responses {
		topic := responseTopic.Topic
		consumedPartions, ok := req.consumptions[topic]
		if !ok {
			continue
		}

		newFetchTopic := FetchTopic{
			Topic:      topic,
			Partitions: make([]FetchPartition, 0, len(responseTopic.PartitionResponses)),
		}

		for i := range responseTopic.PartitionResponses {
			responsePartition := &responseTopic.PartitionResponses[i]
			partition := responsePartition.Partition
			consumption, ok := consumedPartions[partition]
			if !ok {
				continue
			}

			newFetchPartition, keep, partitionNeedsMetadataUpdate :=
				consumption.processResponsePartition(
					source,
					topic,
					r.Version,
					responsePartition,
				)

			if keep {
				newFetchTopic.Partitions = append(newFetchTopic.Partitions, newFetchPartition)
			}
			needMetadataUpdate = needMetadataUpdate || partitionNeedsMetadataUpdate
		}

		if len(newFetchTopic.Partitions) > 0 {
			newFetch.Topics = append(newFetch.Topics, newFetchTopic)
		}
	}

	if needMetadataUpdate {
		source.broker.client.triggerUpdateMetadata()
	}

	if len(newFetch.Topics) > 0 {
		source.buffered = newFetch
		source.broker.client.consumer.addSourceReadyForDraining(source)
	} else {
		<-source.inflightSem
	}
}

func (c *consumption) processResponsePartition(
	source *recordSource,
	topic string,
	version int16,
	responsePartition *kmsg.FetchResponseResponsePartitionResponse,
) (
	newFetchPartition FetchPartition,
	keep bool,
	requiresMetadataUpdate bool,
) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.source != source {
		return FetchPartition{}, false, false
	}

	newFetchPartition = FetchPartition{
		Partition:        responsePartition.Partition,
		Err:              kerr.ErrorForCode(responsePartition.ErrorCode),
		HighWatermark:    responsePartition.HighWatermark,
		LastStableOffset: responsePartition.LastStableOffset,
	}

	switch version {
	case 0, 1:
		messages := kmsg.ReadFetchResponseV0Messages(responsePartition.RecordBatches)
		newFetchPartition.Records = make([]*Record, 0, len(messages))
		c.processV0Messages(topic, &newFetchPartition, messages)
	case 2, 3:
		messages := kmsg.ReadFetchResponseV1Messages(responsePartition.RecordBatches)
		newFetchPartition.Records = make([]*Record, 0, len(messages))
		c.processV1Messages(topic, &newFetchPartition, messages)
	default:
		batches := kmsg.ReadFetchResponseBatches(responsePartition.RecordBatches)
		var numPartitionRecords int
		for i := range batches {
			numPartitionRecords += int(batches[i].NumRecords)
		}
		newFetchPartition.Records = make([]*Record, 0, numPartitionRecords)
		for i := range batches {
			if newFetchPartition.Err != nil {
				break
			}
			c.processResponsePartitionBatch(
				topic,
				&newFetchPartition,
				&batches[i],
			)
		}
	}

	switch newFetchPartition.Err {
	case kerr.UnknownTopicOrPartition,
		kerr.NotLeaderForPartition,
		kerr.ReplicaNotAvailable,
		kerr.KafkaStorageError,
		kerr.UnknownLeaderEpoch,
		kerr.FencedLeaderEpoch:

		requiresMetadataUpdate = true
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

	return newFetchPartition,
		len(newFetchPartition.Records) > 0,
		requiresMetadataUpdate
}

func (c *consumption) processResponsePartitionBatch(
	topic string,
	newFetchPartition *FetchPartition,
	batch *kmsg.RecordBatch,
) {
	if batch.Magic != 2 {
		newFetchPartition.Err = fmt.Errorf("unknown batch magic %d", batch.Magic)
		return
	}

	rawRecords := batch.Records
	if compression := byte(batch.Attributes & 0x0007); compression != 0 {
		var err error
		rawRecords, err = decompress(rawRecords, compression)
		if err != nil {
			newFetchPartition.Err = fmt.Errorf("unable to decompress batch: %v", err)
			return
		}
	}

	krecords, err := kmsg.ReadRecords(int(batch.NumRecords), rawRecords)
	if err != nil {
		newFetchPartition.Err = fmt.Errorf("invalid record batch: %v", err)
		return
	}

	for i := range krecords {
		record := recordToRecord(topic, newFetchPartition.Partition, batch, &krecords[i])
		c.maybeAddRecord(newFetchPartition, record)
	}
}

func (c *consumption) processV1Messages(
	topic string,
	newFetchPartition *FetchPartition,
	messages []kmsg.MessageV1,
) {
	for i := range messages {
		message := &messages[i]
		compression := byte(message.Attributes & 0x0003)
		if compression == 0 {
			c.processV1Message(topic, newFetchPartition, message)
			continue
		}

		rawMessages, err := decompress(message.Value, compression)
		if err != nil {
			newFetchPartition.Err = fmt.Errorf("unable to decompress messages: %v", err)
			return
		}
		innerMessages := kmsg.ReadFetchResponseV1Messages(rawMessages)
		if len(innerMessages) == 0 {
			return
		}
		firstOffset := message.Offset - int64(len(innerMessages)) + 1
		for i := range innerMessages {
			innerMessage := &innerMessages[i]
			innerMessage.Offset = firstOffset + int64(i)
			c.processV1Message(topic, newFetchPartition, innerMessage)
		}
	}
}

func (c *consumption) processV1Message(
	topic string,
	newFetchPartition *FetchPartition,
	message *kmsg.MessageV1,
) {
	if message.Magic != 1 {
		newFetchPartition.Err = fmt.Errorf("unknown message magic %d", message.Magic)
		return
	}
	if message.Attributes != 0 {
		newFetchPartition.Err = fmt.Errorf("unknown attributes on uncompressed message %d", message.Attributes)
		return
	}
	record := v1MessageToRecord(topic, newFetchPartition.Partition, message)
	c.maybeAddRecord(newFetchPartition, record)
}

func (c *consumption) processV0Messages(
	topic string,
	newFetchPartition *FetchPartition,
	messages []kmsg.MessageV0,
) {
	for i := range messages {
		message := &messages[i]
		compression := byte(message.Attributes & 0x0003)
		if compression == 0 {
			c.processV0Message(topic, newFetchPartition, message)
			continue
		}

		rawMessages, err := decompress(message.Value, compression)
		if err != nil {
			newFetchPartition.Err = fmt.Errorf("unable to decompress messages: %v", err)
			return
		}
		innerMessages := kmsg.ReadFetchResponseV0Messages(rawMessages)
		if len(innerMessages) == 0 {
			return
		}
		firstOffset := message.Offset - int64(len(innerMessages)) + 1
		for i := range innerMessages {
			innerMessage := &innerMessages[i]
			innerMessage.Offset = firstOffset + int64(i)
			c.processV0Message(topic, newFetchPartition, innerMessage)
		}
	}
}

func (c *consumption) processV0Message(
	topic string,
	newFetchPartition *FetchPartition,
	message *kmsg.MessageV0,
) {
	if message.Magic != 0 {
		newFetchPartition.Err = fmt.Errorf("unknown message magic %d", message.Magic)
		return
	}
	if message.Attributes != 0 {
		newFetchPartition.Err = fmt.Errorf("unknown attributes on uncompressed message %d", message.Attributes)
		return
	}
	record := v0MessageToRecord(topic, newFetchPartition.Partition, message)
	c.maybeAddRecord(newFetchPartition, record)
}

func (c *consumption) maybeAddRecord(newFetchPartition *FetchPartition, record *Record) {
	if record.Offset < c.offset {
		// We asked for offset 5, but that was in the middle of a
		// batch; we got offsets 0 thru 4 that we need to skip.
		return
	}
	if record.Offset != c.offset {
		// We asked for offset 5, then the client user reset the
		// offset to something else while this was inflight.
		// This response out of date.
		return
	}
	newFetchPartition.Records = append(newFetchPartition.Records, record)
	c.offset++
}

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

func (source *recordSource) takeBuffered() Fetch {
	source.mu.Lock()
	r := source.buffered
	source.buffered = Fetch{}
	source.mu.Unlock()

	<-source.inflightSem
	return r
}

type fetchRequest struct {
	version int16

	consumptions map[string]map[int32]*consumption
}

func (req *fetchRequest) addTopicPartitionConsumption(
	topic string,
	partition int32,
	c *consumption,
) {
	if req.consumptions == nil {
		req.consumptions = make(map[string]map[int32]*consumption)
	}
	partitions := req.consumptions[topic]
	if partitions == nil {
		partitions = make(map[int32]*consumption)
		req.consumptions[topic] = partitions
	}
	partitions[partition] = c
}

func (*fetchRequest) Key() int16           { return 1 }
func (*fetchRequest) MaxVersion() int16    { return 11 }
func (f *fetchRequest) SetVersion(v int16) { f.version = v }
func (f *fetchRequest) GetVersion() int16  { return f.version }
func (f *fetchRequest) AppendTo(dst []byte) []byte {
	req := kmsg.FetchRequest{
		Version:      f.version,
		ReplicaID:    -1,
		MaxWaitTime:  200, // TODO
		MinBytes:     1,
		MaxBytes:     5 << 20, // TODO
		SessionID:    -1,
		SessionEpoch: -1, // KIP-227, we do not want to support
		Topics:       make([]kmsg.FetchRequestTopic, 0, len(f.consumptions)),
	}
	for topic, partitions := range f.consumptions {
		req.Topics = append(req.Topics, kmsg.FetchRequestTopic{
			Topic:      topic,
			Partitions: make([]kmsg.FetchRequestTopicPartition, 0, len(partitions)),
		})
		reqTopic := &req.Topics[len(req.Topics)-1]
		for partition, consumption := range partitions {
			reqTopic.Partitions = append(reqTopic.Partitions, kmsg.FetchRequestTopicPartition{
				Partition:          partition,
				CurrentLeaderEpoch: -1, // KIP-320
				FetchOffset:        consumption.offset,
				LogStartOffset:     -1,
				PartitionMaxBytes:  500 << 20, // TODO
			})
		}
	}
	return req.AppendTo(dst)
}
func (f *fetchRequest) ResponseKind() kmsg.Response {
	return &kmsg.FetchResponse{Version: f.version}
}
