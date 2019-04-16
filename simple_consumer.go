package kgo

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/twmb/kgo/kbin"
	"github.com/twmb/kgo/kerr"
	"github.com/twmb/kgo/kmsg"
)

const (
	// ConsumeEarliestOffset begins consuming at the earliest timestamp in
	// a partition.
	ConsumeEarliestOffset int64 = -2
	// ConsumeEarliestOffset begins consuming at the latest timestamp in a
	// partition.
	ConsumeLatestOffset int64 = -1
)

type TopicPartitions struct {
	Topic      string
	Partitions []int32
}

// TopicPartitions requests and returns partitions for the requested topics or
// an error if the request failed.
//
// If no topics are requested, this returns all topics and their partitions.
func (c *Client) TopicPartitions(topics ...string) ([]TopicPartitions, error) {
	all := false
	if len(topics) == 0 {
		all = true
	}
	resp, err := c.fetchMetadata(all, topics...)
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

// ConsumePartition returns a partition consumer for a topic and partition at
// the given offset.
//
// This returns an error if the topic and partition is already being consumed.
func (c *Client) ConsumePartition(topic string, partition int32, offset int64) (*PartitionConsumer, error) {
	sc := &c.simpleConsumer
	sc.mu.Lock()
	defer sc.mu.Unlock()

	// If we have never consumed, init our simple consumer.
	if sc.topicsConsumers == nil {
		sc.topicsConsumers = make(map[string]map[int32]*PartitionConsumer, 1)
		sc.brokerConsumers = make(map[int32]*brokerConsumer, 1)
		sc.cl = c
	}

	// If we have never consumed this topic, init it.
	topicsConsumers, exists := sc.topicsConsumers[topic]
	if !exists {
		topicsConsumers = make(map[int32]*PartitionConsumer, 1)
		sc.topicsConsumers[topic] = topicsConsumers
	}

	// We can only have one consumer of a partition.
	partitionConsumer, exists := topicsConsumers[partition]
	if exists {
		return nil, errors.New("partition is already being consumed")
	}

	partitionConsumer = &PartitionConsumer{
		topic:     topic,
		partition: partition,
		offset:    offset,

		sc: sc,

		recordsCh: make(chan []*Record, 100),
		errorsCh:  make(chan error, 5),

		closeDone: make(chan struct{}),
	}

	// Return the partition consumer, which will load itself appropriately.
	go partitionConsumer.load()

	return partitionConsumer, nil
}

// simpleConsumer is the "simple" consumer that does not use consumer groups.
// It is dumb at handling errors; responsibility for error handling is punted
// to the user of the client.
type simpleConsumer struct {
	cl *Client

	// mu guards the two fields below.
	mu sync.Mutex
	// topipcsConsumers maps topics that are being consumed to the
	// partitions in those topics that are being consumed.
	topicsConsumers map[string]map[int32]*PartitionConsumer
	// brokerConsumers contain brokers that have topics/partitions being
	// consumed.
	brokerConsumers map[int32]*brokerConsumer
}

// brokerConsumer pairs a broker with the topics and partitions on that broker
// that are being consumed.
type brokerConsumer struct {
	sc              *simpleConsumer
	broker          *broker
	mu              sync.Mutex // guards below
	topicsConsumers map[string]map[int32]*PartitionConsumer
}

// PartitionConsumer is a partition that has been requested to be consumed.
// This is mapped to a single broker once; if the partition ever moves
// between brokers, the errors channel will return the relevant error
// and the consumer will need closing.
type PartitionConsumer struct {
	sc *simpleConsumer // our owner

	topic     string
	partition int32
	leader    int32 // our broker leader ID
	offset    int64
	epoch     int32

	recordsCh chan []*Record
	errorsCh  chan error

	closedMu  sync.Mutex
	closed    int64
	closeDone chan struct{}
}

// load fetches metadata for a partition consumer to discover the
// partition's leader.
//
// Once the leader is found, this triggers offset lookups.
func (p *PartitionConsumer) load() {
	retries := 0
retry:
	resp, err := p.sc.cl.fetchMetadata(false, p.topic)
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

			p.sc.cl.brokersMu.RLock()
			broker, exists := p.sc.cl.brokers[leader]
			p.sc.cl.brokersMu.RUnlock()

			if !exists {
				p.errorClose(errUnknownBrokerForLeader)
				return
			}

			p.leader = leader

			p.loadOffsets(broker)
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
func (p *PartitionConsumer) loadOffsets(broker *broker) {
	ts := ConsumeEarliestOffset
	if p.offset == ConsumeLatestOffset {
		ts = ConsumeLatestOffset
	}

	req := &kmsg.ListOffsetsRequest{
		ReplicaID: -1,
		Topics: []kmsg.ListOffsetsRequestTopic{{
			Topic: p.topic,
			Partitions: []kmsg.ListOffsetsRequestTopicPartition{{
				Partition:          p.partition,
				CurrentLeaderEpoch: -1,
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

	// If the request was not for earliest or latest, redo the request
	// with the other bound.
	if p.offset != ConsumeEarliestOffset && p.offset != ConsumeLatestOffset {
		earliestResp = resp
		req.Topics[0].Partitions[0].Timestamp = ConsumeLatestOffset
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

	offset, epoch, err := getOffsetAndEpoch(resp)
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
		if p.offset < earliestOffset || p.offset > offset {
			p.errorClose(kerr.OffsetOutOfRange)
			return
		}
	} else {
		p.offset = offset
	}

	p.saveAndConsume(broker)
}

// saveAndConsume finally saves the partition consumer into the broker that
// will be used for fetching.
func (p *PartitionConsumer) saveAndConsume(broker *broker) {
	p.closedMu.Lock()
	defer p.closedMu.Unlock()

	if atomic.LoadInt64(&p.closed) == 1 { // closed while we were loading!
		return
	}

	p.sc.mu.Lock()
	defer p.sc.mu.Unlock()

	bc, exists := p.sc.brokerConsumers[p.leader]
	if !exists {
		bc = &brokerConsumer{
			sc:              p.sc,
			broker:          broker,
			topicsConsumers: make(map[string]map[int32]*PartitionConsumer),
		}
		p.sc.brokerConsumers[p.leader] = bc
		bc.mu.Lock()

		go bc.fetchLoop()
	} else {
		bc.mu.Lock()
	}
	defer bc.mu.Unlock()

	brokerTopicConsumer, exists := bc.topicsConsumers[p.topic]
	if !exists {
		brokerTopicConsumer = make(map[int32]*PartitionConsumer, 1)
		bc.topicsConsumers[p.topic] = brokerTopicConsumer
	}
	if _, exists := brokerTopicConsumer[p.partition]; exists {
		panic("broker consumer should not have partition being consumed twice")
	}
	brokerTopicConsumer[p.partition] = p
}

func (p *PartitionConsumer) errorClose(err error) {
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
func (p *PartitionConsumer) Close() {
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

	p.sc.mu.Lock()
	defer p.sc.mu.Unlock()

	// Remove self from simple consumer so that this topic/partition
	// can be re-consumed if desired.
	topicsConsumers := p.sc.topicsConsumers[p.topic]
	delete(topicsConsumers, p.partition)
	if len(topicsConsumers) == 0 {
		delete(p.sc.topicsConsumers, p.topic)
	}

	bc := p.sc.brokerConsumers[p.leader]
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
				delete(p.sc.brokerConsumers, p.leader)
			}
		}
	}
}

// Errors returns the error channel that consume errors are sent along.
// This must be drained completely.
func (p *PartitionConsumer) Errors() <-chan error {
	return p.errorsCh
}

// Records returns the records channel that consumed records are sent along.
// This must be drained completely.
func (p *PartitionConsumer) Records() <-chan []*Record {
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

		MaxWaitTime: 1000, // TODO

		SessionID:    -1,
		SessionEpoch: -1,

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
	if err != nil {
		if isRetriableBrokerErr(err) {
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
			continue
		}

	partitionResponses:
		for i := range topicResp.PartitionResponses {
			partResp := &topicResp.PartitionResponses[i]
			partHdr := &partResp.PartitionHeader
			partitionConsumer, exists := topicConsumers[partHdr.Partition]
			if !exists {
				continue
			}

			partitionConsumer.closedMu.Lock()

			if atomic.LoadInt64(&partitionConsumer.closed) == 1 {
				partitionConsumer.closedMu.Unlock()
				continue
			}

			if err := kerr.ErrorForCode(partHdr.ErrorCode); err != nil {
				partitionConsumer.errorsCh <- err
				partitionConsumer.closedMu.Unlock()
				continue
			}

			batch := &partResp.RecordSet
			if batch.Magic == 0 {
				// record batch had size of zero and thus there
				// was no batch; continue.
				partitionConsumer.closedMu.Unlock()
				continue
			}
			if batch.Magic != 2 {
				partitionConsumer.errorsCh <- fmt.Errorf("unknown message batch magic %d", batch.Magic)
				partitionConsumer.closedMu.Unlock()
				continue
			}

			rawRecords := batch.Records
			if compression := byte(batch.Attributes & 0x0007); compression != 0 {
				var err error
				rawRecords, err = decompress(rawRecords, compression)
				if err != nil {
					partitionConsumer.errorsCh <- fmt.Errorf("unable to decompress batch: %v", err)
					partitionConsumer.closedMu.Unlock()
					continue
				}
			}

			kgoRecords := make([]*Record, 0, batch.NumRecords)
			var r kmsg.Record
			for i := batch.NumRecords; i > 0; i-- {
				rawRecords, err = r.ReadFrom(rawRecords)
				if err != nil {
					partitionConsumer.errorsCh <- fmt.Errorf("invalid record batch: %v", err)
					partitionConsumer.closedMu.Unlock()
					continue partitionResponses
				}
				kgoR := recordToRecord(partHdr.Partition, batch, &r)
				kgoRecords = append(kgoRecords, kgoR)
			}

			if len(rawRecords) != 0 {
				partitionConsumer.errorsCh <- kbin.ErrTooMuchData
				partitionConsumer.closedMu.Unlock()
				continue
			}
			if len(kgoRecords) == 0 {
				partitionConsumer.closedMu.Unlock()
				continue
			}

			partitionConsumer.offset += int64(len(kgoRecords))
			partitionConsumer.recordsCh <- kgoRecords
			partitionConsumer.closedMu.Unlock()
		}
	}
}

func (bc *brokerConsumer) reloadAllAndRemoveSelf() {
	reload := bc.topicsConsumers
	bc.topicsConsumers = nil

	go func() {
		bc.sc.mu.Lock()
		delete(bc.sc.brokerConsumers, bc.broker.id)
		bc.sc.mu.Unlock()
		for _, partitionConsumers := range reload {
			for _, partitionConsumer := range partitionConsumers {
				go partitionConsumer.load()
			}
		}
	}()
}
