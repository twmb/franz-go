// +build none

package kgo

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/twmb/kgo/kbin"
	"github.com/twmb/kgo/kerr"
	"github.com/twmb/kgo/kmsg"
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
	return Offset{request: o}
}

func (cl *Client) ConsumePartitions(assignments map[string]map[int32]Offset) {
	c := &cl.consumer
	c.mu.Lock()
	defer c.mu.Unlock()

	c.cl = cl
	c.topicsConsumers = make(map[string]map[int32]*consumedPartition, 1)
	c.brokerConsumers = make(map[int32]*brokerConsumer, 1)

	for topic, partitions := range assignments {
		topicConsumer, exists := c.topicsConsumers[topic]
		if !exists {
			topicConsumer = make(map[int32]*consumedPartition, 1)
			c.topicsConsumers[topic] = topicConsumer
		}

		if _, exists := topicConsumer[partition]; exists {
			continue // exists twice in assignment
		}

		topicConsumer[partition] = &consumedPartition{
			topic:     topic,
			partition: partition,

			c: c,

			closeDone: make(chan struct{}),
		}
	}

	go c.load()
}

// consumer maps topic partitions to brokers and fetches records for those
// topic partitions from those brokers.
type consumer struct {
	cl *Client

	mu sync.Mutex // guards below

	topicsConsumers map[string]map[int32]*consumedPartition
	brokerConsumers map[int32]*brokerConsumer
}

// brokerConsumer pairs a broker with the topics and partitions on that broker
// that are being consumed.
type brokerConsumer struct {
	c               *consumer
	broker          *broker
	mu              sync.Mutex // guards below
	topicsConsumers map[string]map[int32]*consumedPartition
}

// consumedPartition is a partition that has been requested to be consumed.
// This is mapped to a single broker once; if the partition ever moves
// between brokers, the errors channel will return the relevant error
// and the consumer will need closing.
type consumedPartition struct {
	c *consumer // our owner

	topic     string
	partition int32
	leader    int32 // our broker leader ID
	offset    int64
	epoch     int32

	closedMu  sync.Mutex
	closed    int64
	closeDone chan struct{}
}

// fetch metadata for all parts

// load fetches metadata for a partition consumer to discover the
// partition's leader.
//
// Once the leader is found, this triggers offset lookups.
func (p *consumedPartition) load(offset Offset) {
	retries := 0
retry:
	resp, err := p.c.cl.fetchMetadata(false, p.topic)
	if err != nil {
		if retries < 3 { // TODO retries
			retries++
			goto retry
		}
		p.errorClose(err)
		return
	}

	if len(resp.TopicMetadata) != 1 || resp.TopicMetadata[0].Topic != p.topic {
		p.errorClose(errNoResp)
		return
	}

	topicMeta := resp.TopicMetadata[0]
	for _, partMeta := range topicMeta.PartitionMetadata {
		if partMeta.Partition == p.partition {
			leader := partMeta.Leader

			p.c.cl.brokersMu.RLock()
			broker, exists := p.c.cl.brokers[leader]
			p.c.cl.brokersMu.RUnlock()

			if !exists {
				p.errorClose(errUnknownBrokerForLeader)
				return
			}

			p.leader = leader
			p.epoch = partMeta.LeaderEpoch

			p.loadOffsets(broker, offset)
			return
		}
	}

	p.errorClose(errNoResp)
}

// loadOffsets loads the offset and leader epoch for the consumer using the
// given broker.
//
// If the offset is a specific offset, we load the latest and earliest to
// ensure that the requested offset is in range.
//
// If this is successful, we finally save the consumer to the broker and
// begin consuming.
func (p *consumedPartition) loadOffsets(broker *broker, offset Offset) {
	const earliest int64 = -2
	const latest int64 = -1

	ts := earliest
	if offset.request == latest && offset.relative == 0 {
		ts = latest
	}
	req := &kmsg.ListOffsetsRequest{
		ReplicaID: -1,
		Topics: []kmsg.ListOffsetsRequestTopic{{
			Topic: p.topic,
			Partitions: []kmsg.ListOffsetsRequestTopicPartition{{
				Partition:          p.partition,
				CurrentLeaderEpoch: p.epoch,
				Timestamp:          ts,
			}},
		}},
	}

	var earliestResp *kmsg.ListOffsetsResponse

	retries := 0
start:
	var resp *kmsg.ListOffsetsResponse
	var err error
	broker.wait(
		req,
		func(kresp kmsg.Response, respErr error) {
			if err = respErr; err != nil {
				return
			}
			resp = kresp.(*kmsg.ListOffsetsResponse)
		},
	)
	if err != nil {
		if retries < 3 {
			goto start
		}
		p.errorsCh <- err
		return
	}

	// If the client user request was not for earliest or latest, we issued
	// for the earliest and will now will issue for the latest to check
	// if the client offset request was in bounds.
	//
	// Same for if the request was relative.
	if earliestResp == nil &&
		(offset.relative != 0 ||
			offset.request != earliest && offset.request != latest) {
		earliestResp = resp
		req.Topics[0].Partitions[0].Timestamp = latest
		retries = 0
		goto start
	}

	getOffsetAndEpoch := func(resp *kmsg.ListOffsetsResponse) (int64, int32, error) {
		if len(resp.Responses) != 1 || resp.Responses[0].Topic != p.topic {
			return 0, 0, errNoResp
		}
		partResps := resp.Responses[0].PartitionResponses
		if len(partResps) != 1 || partResps[0].Partition != p.partition {
			return 0, 0, errNoResp
		}
		partResp := partResps[0]
		if err := kerr.ErrorForCode(partResp.ErrorCode); err != nil {
			return 0, 0, err
		}
		return partResp.Offset, partResp.LeaderEpoch, nil
	}

	gotOffset, epoch, err := getOffsetAndEpoch(resp)
	p.epoch = epoch

	if err != nil {
		p.errorClose(err)
		return
	}

	if earliestResp != nil {
		earliestOffset, _, err := getOffsetAndEpoch(earliestResp)
		if err != nil {
			p.errorClose(err)
			return
		}
		latestOffset := gotOffset // second request was for latest

		var reqOffset int64
		if offset.request == earliest {
			reqOffset = earliestOffset
		} else if offset.request == latest {
			reqOffset = latestOffset
		}

		reqOffset += offset.relative

		if reqOffset < earliestOffset || reqOffset > latestOffset {
			p.errorClose(kerr.OffsetOutOfRange)
			return
		}
		p.offset = reqOffset
	} else {
		p.offset = gotOffset
	}

	p.saveAndConsume(broker)
}

// saveAndConsume finally saves the partition consumer into the broker that
// will be used for fetching.
func (p *consumedPartition) saveAndConsume(broker *broker) {
	p.closedMu.Lock()
	defer p.closedMu.Unlock()

	if atomic.LoadInt64(&p.closed) == 1 { // closed while we were loading!
		return
	}

	p.c.mu.Lock()
	defer p.c.mu.Unlock()

	bc, exists := p.c.brokerConsumers[p.leader]
	if !exists {
		bc = &brokerConsumer{
			c:               p.c,
			broker:          broker,
			topicsConsumers: make(map[string]map[int32]*consumedPartition),
		}
		p.c.brokerConsumers[p.leader] = bc
		bc.mu.Lock()

		go bc.fetchLoop()
	} else {
		bc.mu.Lock()
	}
	defer bc.mu.Unlock()

	brokerTopicConsumer, exists := bc.topicsConsumers[p.topic]
	if !exists {
		brokerTopicConsumer = make(map[int32]*consumedPartition, 1)
		bc.topicsConsumers[p.topic] = brokerTopicConsumer
	}
	if _, exists := brokerTopicConsumer[p.partition]; exists {
		panic("broker consumer should not have partition being consumed twice")
	}
	brokerTopicConsumer[p.partition] = p
}

func (p *consumedPartition) errorClose(err error) {
	p.closedMu.Lock()
	defer p.closedMu.Unlock()

	if atomic.LoadInt64(&p.closed) == 1 {
		return
	}

	p.errorsCh <- err
	go p.Close()
}

// Close removes this partition consumer from the broker that is loop
// fetching for it, and removes the consumer from being tracked at all.
//
// This must be called once a partition consumer is done being used.
// It is safe to call multiple times.
func (p *consumedPartition) Close() {
	if atomic.SwapInt64(&p.closed, 1) == 1 {
		<-p.closeDone
		return
	}

	p.closedMu.Lock()
	p.closedMu.Unlock()

	defer func() {
		close(p.errorsCh)
		close(p.recordsCh)
	}()

	defer close(p.closeDone)

	p.c.mu.Lock()
	defer p.c.mu.Unlock()

	// Remove self from simple consumer so that this topic/partition
	// can be re-consumed if desired.
	topicsConsumers := p.c.topicsConsumers[p.topic]
	delete(topicsConsumers, p.partition)
	if len(topicsConsumers) == 0 {
		delete(p.c.topicsConsumers, p.topic)
	}

	bc := p.c.brokerConsumers[p.leader]
	if bc == nil {
		return // died before saving ourselves to the broker
	}

	bc.mu.Lock()
	defer bc.mu.Unlock()

	// Remove self from the broker that was fetching for us.
	// This could have already been deleted if the broker noticed
	// and error and removed it from itself first.
	// We do not delete replacement partition consumers because
	// those are still removed from the simple consumer map
	// separately.
	brokerTopicConsumers, exists := bc.topicsConsumers[p.topic]
	if exists {
		delete(brokerTopicConsumers, p.partition)
		if len(brokerTopicConsumers) == 0 {
			delete(bc.topicsConsumers, p.topic)

			// If this broker is completely empty, remove it from the
			// simple consumer. The broker's fetchLoop will stop once it
			// sees it is empty.
			if len(bc.topicsConsumers) == 0 {
				delete(p.c.brokerConsumers, p.leader)
			}
		}
	}
}

// Errors returns the error channel that consume errors are sent along.
// This must be drained completely.
func (p *consumedPartition) Errors() <-chan error {
	return p.errorsCh
}

// Records returns the records channel that consumed records are sent along.
// This must be drained completely.
func (p *consumedPartition) Records() <-chan []*Record {
	return p.recordsCh
}

func (bc *brokerConsumer) fetchLoop() {
	for {
		bc.mu.Lock()
		req := bc.buildRequest()
		bc.mu.Unlock()

		// We have no topics if all partitions have been closed, and
		// thus we need to return.
		if len(req.Topics) == 0 {
			return
		}

		// We need to wait for the response to be consumed
		// because the response will update our offset.
		var resp *kmsg.FetchResponse
		var err error
		bc.broker.wait(req,
			func(kresp kmsg.Response, respErr error) {
				if err = respErr; err != nil {
					return
				}
				resp = kresp.(*kmsg.FetchResponse)
			},
		)
		bc.mu.Lock()
		bc.handleResp(req, resp, err)
		bc.mu.Unlock()
	}
}

func (bc *brokerConsumer) buildRequest() *kmsg.FetchRequest {
	req := &kmsg.FetchRequest{
		ReplicaID: -1,

		MaxWaitTime: 500, // TODO
		MinBytes:    1,
		MaxBytes:    500 << 20,

		SessionEpoch: -1, // KIP-227, do not create a session

		Topics: make([]kmsg.FetchRequestTopic, 0, len(bc.topicsConsumers)),
	}

	for topic, partitionsConsumers := range bc.topicsConsumers {
		partReqs := make([]kmsg.FetchRequestTopicPartition, 0, len(partitionsConsumers))
		for partition, partitionConsumer := range partitionsConsumers {
			partReqs = append(partReqs, kmsg.FetchRequestTopicPartition{
				Partition:          partition,
				CurrentLeaderEpoch: partitionConsumer.epoch,
				FetchOffset:        partitionConsumer.offset,
				LogStartOffset:     -1,
				PartitionMaxBytes:  500 << 20,
			})
		}
		req.Topics = append(req.Topics, kmsg.FetchRequestTopic{
			Topic:      topic,
			Partitions: partReqs,
		})
	}

	return req
}

func (bc *brokerConsumer) handleResp(req *kmsg.FetchRequest, resp *kmsg.FetchResponse, err error) {
	if err == nil {
		// The top level error code should be 0, but just in case.
		err = kerr.ErrorForCode(resp.ErrorCode)
	}

	if err != nil {
		if isRetriableBrokerErr(err) {
			// If our "broker" died, it was either permanently
			// closed or the host migrated. We close our
			// brokerConsumer and trigger a relead of the
			// partitionConsumers that were on it.
			if err == ErrBrokerDead {
				bc.reloadAllAndRemoveSelf()
			}
			return
		}

		// Non retriable errors need to have the consumers' closed.
		for i := range req.Topics {
			topicReq := &req.Topics[i]
			topicConsumers, exists := bc.topicsConsumers[topicReq.Topic]
			if !exists {
				continue // consume closed while request was issuing
			}
			for i := range topicReq.Partitions {
				partReq := &topicReq.Partitions[i]
				partitionConsumer, exists := topicConsumers[partReq.Partition]
				if !exists {
					continue
				}
				partitionConsumer.errorClose(err)

				delete(topicConsumers, partReq.Partition)
				if len(topicConsumers) == 0 {
					delete(bc.topicsConsumers, topicReq.Topic)
				}
			}
		}
		return
	}

	for i := range resp.Responses {
		topicResp := &resp.Responses[i]
		topicConsumers, exists := bc.topicsConsumers[topicResp.Topic]
		if !exists {
			continue // part consumer was closed by client user
		}

		for i := range topicResp.PartitionResponses {
			partResp := &topicResp.PartitionResponses[i]
			partitionConsumer, exists := topicConsumers[partResp.Partition]
			if !exists {
				continue // part consumer was closed by client user
			}
			partitionConsumer.processResponse(partResp)
		}
	}
}

func (p *consumedPartition) processResponse(resp *kmsg.FetchResponseResponsePartitionResponse) {
	p.closedMu.Lock()
	defer p.closedMu.Unlock()

	if atomic.LoadInt64(&p.closed) == 1 {
		p.closedMu.Unlock()
		return
	}

	// TODO
	switch err := kerr.ErrorForCode(resp.ErrorCode); err {
	case nil:

	case kerr.UnknownTopicOrPartition,
		kerr.NotLeaderForPartition,
		kerr.ReplicaNotAvailable,
		kerr.KafkaStorageError,
		kerr.UnknownLeaderEpoch,
		kerr.FencedLeaderEpoch:
		// backoff

	default:
		// Fatal:
		// - bad auth
		// - unsupported compression
		// - unsupported message version
		// - out of range offset
		// - unknown error
	}

	if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
		p.errorsCh <- err
		return
	}

	for i := range resp.RecordBatches {
		batch := &resp.RecordBatches[i]
		p.processBatch(batch)
	}
}

func (p *consumedPartition) processBatch(batch *kmsg.RecordBatch) {
	if batch.Length == 0 { // record batch had size of zero; there was no batch
		return
	}
	if batch.Magic != 2 {
		p.errorsCh <- fmt.Errorf("unknown message batch magic %d", batch.Magic)
		return
	}

	rawRecords := batch.Records
	if compression := byte(batch.Attributes & 0x0007); compression != 0 {
		var err error
		rawRecords, err = decompress(rawRecords, compression)
		if err != nil {
			p.errorsCh <- fmt.Errorf("unable to decompress batch: %v", err)
			return
		}
	}

	kgoRecords := make([]*Record, 0, batch.NumRecords)
	var r kmsg.Record
	var err error
	for i := batch.NumRecords; i > 0; i-- {
		rawRecords, err = r.ReadFrom(rawRecords)
		if err != nil {
			p.errorsCh <- fmt.Errorf("invalid record batch: %v", err)
			return
		}
		kgoR := recordToRecord(p.topic, p.partition, batch, &r)
		// Offset could be less than ours if we asked for an offset in
		// the middle of a batch.
		if kgoR.Offset < p.offset {
			continue
		}
		kgoRecords = append(kgoRecords, kgoR)
	}

	if len(rawRecords) != 0 {
		p.errorsCh <- kbin.ErrTooMuchData
		return
	}
	if len(kgoRecords) == 0 {
		return
	}

	p.offset += int64(len(kgoRecords))
	p.recordsCh <- kgoRecords
}

func (bc *brokerConsumer) reloadAllAndRemoveSelf() {
	reload := bc.topicsConsumers
	bc.topicsConsumers = nil

	go func() {
		bc.c.mu.Lock()
		delete(bc.c.brokerConsumers, bc.broker.id)
		bc.c.mu.Unlock()
		for _, partitionConsumers := range reload {
			for _, partitionConsumer := range partitionConsumers {
				go partitionConsumer.load(Offset{request: partitionConsumer.offset})
			}
		}
	}()
}
