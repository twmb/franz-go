package kgo

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/kafka-go/pkg/kerr"
	"github.com/twmb/kafka-go/pkg/kmsg"
)

// Client issues requests and handles responses to a Kafka cluster.
type Client struct {
	cfg cfg

	ctx       context.Context
	ctxCancel func()

	rng *rand.Rand

	brokersMu    sync.RWMutex
	brokers      map[int32]*broker // broker id => broker
	anyBroker    []*broker
	anyBrokerIdx int
	stopBrokers  bool // set to true on close to stop updateBrokers

	controllerID int32 // atomic

	producer producer
	consumer consumer

	compressor   *compressor
	decompressor *decompressor

	coordinatorsMu sync.Mutex
	coordinators   map[coordinatorKey]int32

	topicsMu sync.Mutex
	topics   atomic.Value // map[string]*topicPartitions

	// unknownTopics buffers all records for topics that are not loaded
	unknownTopicsMu   sync.Mutex
	unknownTopics     map[string][]promisedRecord
	unknownTopicsWait map[string]chan struct{}

	updateMetadataCh    chan struct{}
	updateMetadataNowCh chan struct{} // like above, but with high priority
	metawait            metawait
	metadone            chan struct{}
}

// stddialer is the default dialer for dialing connections.
var stddialer = net.Dialer{Timeout: 10 * time.Second}

func stddial(addr string) (net.Conn, error) { return stddialer.Dial("tcp", addr) }

func NewClient(opts ...Opt) (*Client, error) {
	cfg := defaultCfg()
	for _, opt := range opts {
		opt.apply(&cfg)
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	seedAddrs := make([]string, 0, len(cfg.seedBrokers))
	for _, seedBroker := range cfg.seedBrokers {
		addr := seedBroker
		port := 9092 // default kafka port
		var err error
		if colon := strings.IndexByte(addr, ':'); colon > 0 {
			port, err = strconv.Atoi(addr[colon+1:])
			if err != nil {
				return nil, fmt.Errorf("unable to parse addr:port in %q", seedBroker)
			}
			addr = addr[:colon]
		}

		if addr == "localhost" {
			addr = "127.0.0.1"
		}

		seedAddrs = append(seedAddrs, net.JoinHostPort(addr, strconv.Itoa(port)))
	}

	ctx, cancel := context.WithCancel(context.Background())

	c := &Client{
		cfg:       cfg,
		ctx:       ctx,
		ctxCancel: cancel,
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),

		controllerID: unknownControllerID,
		brokers:      make(map[int32]*broker),

		producer: producer{
			waitBuffer: make(chan struct{}, 100),
		},

		decompressor: newDecompressor(),

		coordinators:      make(map[coordinatorKey]int32),
		unknownTopics:     make(map[string][]promisedRecord),
		unknownTopicsWait: make(map[string]chan struct{}),

		updateMetadataCh:    make(chan struct{}, 1),
		updateMetadataNowCh: make(chan struct{}, 1),
		metadone:            make(chan struct{}),
	}
	c.producer.flushingCond = sync.NewCond(&c.producer.flushingMu)
	c.consumer.cl = c
	c.consumer.sourcesReadyCond = sync.NewCond(&c.consumer.sourcesReadyMu)
	c.topics.Store(make(map[string]*topicPartitions))
	c.metawait.init()

	compressor, err := newCompressor(c.cfg.compression...)
	if err != nil {
		return nil, err
	}
	c.compressor = compressor

	for i, seedAddr := range seedAddrs {
		b := c.newBroker(seedAddr, unknownSeedID(i))
		c.brokers[b.id] = b
		c.anyBroker = append(c.anyBroker, b)
	}
	go c.updateMetadataLoop()

	return c, nil
}

// broker returns a random broker from all brokers ever known.
func (c *Client) broker() *broker {
	c.brokersMu.Lock()
	defer c.brokersMu.Unlock()

	if c.anyBrokerIdx >= len(c.anyBroker) {
		c.anyBrokerIdx = 0
	}

	b := c.anyBroker[c.anyBrokerIdx]
	c.anyBrokerIdx++
	if c.anyBrokerIdx == len(c.anyBroker) {
		c.anyBrokerIdx = 0
		c.rng.Shuffle(len(c.anyBroker), func(i, j int) { c.anyBroker[i], c.anyBroker[j] = c.anyBroker[j], c.anyBroker[i] })
	}
	return b
}

func (c *Client) waitTries(ctx context.Context, tries int) bool {
	after := time.NewTimer(c.cfg.retryBackoff(tries))
	defer after.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-c.ctx.Done():
		return false
	case <-after.C:
		return true
	}
}

// fetchBrokerMetadata issues a metadata request solely for broker information.
func (c *Client) fetchBrokerMetadata(ctx context.Context) error {
	_, err := c.fetchMetadata(ctx, false, nil)
	return err
}

func (c *Client) fetchMetadata(ctx context.Context, all bool, topics []string) (*kmsg.MetadataResponse, error) {
	if all {
		topics = nil
	} else if len(topics) == 0 {
		topics = []string{}
	}
	tries := 0
start:
	tries++
	broker := c.broker()
	req := &kmsg.MetadataRequest{
		AllowAutoTopicCreation: c.cfg.allowAutoTopicCreation,
	}
	for _, topic := range topics {
		req.Topics = append(req.Topics, kmsg.MetadataRequestTopic{topic})
	}
	kresp, err := broker.waitResp(ctx, req)
	if err != nil {
		if isRetriableBrokerErr(err) && tries < c.cfg.retries {
			if ok := c.waitTries(ctx, tries); ok {
				goto start
			}
			return nil, err
		}
		return nil, err
	}
	meta := kresp.(*kmsg.MetadataResponse)
	if meta.ControllerID >= 0 {
		atomic.StoreInt32(&c.controllerID, meta.ControllerID)
	}
	c.updateBrokers(meta.Brokers)
	return meta, err
}

// updateBrokers is called with the broker portion of every metadata response.
// All metadata responses contain all known live brokers, so we can always
// use the response.
func (c *Client) updateBrokers(brokers []kmsg.MetadataResponseBroker) {
	newBrokers := make(map[int32]*broker, len(brokers))
	newAnyBroker := make([]*broker, 0, len(brokers))

	c.brokersMu.Lock()
	defer c.brokersMu.Unlock()

	if c.stopBrokers {
		return
	}

	for _, broker := range brokers {
		addr := net.JoinHostPort(broker.Host, strconv.Itoa(int(broker.Port)))

		b, exists := c.brokers[broker.NodeID]
		if exists {
			delete(c.brokers, b.id)
			if b.addr != addr {
				b.stopForever()
				b = c.newBroker(addr, b.id)
			}
		} else {
			b = c.newBroker(addr, broker.NodeID)
		}

		newBrokers[b.id] = b
		newAnyBroker = append(newAnyBroker, b)
	}

	for goneID, goneBroker := range c.brokers {
		if goneID < -1 { // seed broker, unknown ID, always keep
			newBrokers[goneID] = goneBroker
			newAnyBroker = append(newAnyBroker, goneBroker)
		} else {
			goneBroker.stopForever()
		}
	}

	c.brokers = newBrokers
	c.anyBroker = newAnyBroker
}

// Close leaves any group and closes all connections and goroutines.
func (cl *Client) Close() {
	// First, kill the consumer. Setting dead to true and then assigning
	// nothing will
	// 1) invalidate active fetches
	// 2) ensure consumptions are unassigned, stopping all source filling
	// 3) ensures no more assigns can happen
	cl.consumer.mu.Lock()
	if cl.consumer.dead { // client already closed
		cl.consumer.mu.Unlock()
		return
	}
	cl.consumer.dead = true
	cl.consumer.mu.Unlock()
	cl.AssignPartitions()

	// Now we kill the client context and all brokers, ensuring all
	// requests fail. This will finish all producer callbacks and
	// stop the metadata loop.
	cl.ctxCancel()
	cl.brokersMu.Lock()
	cl.stopBrokers = true
	for _, broker := range cl.brokers {
		broker.stopForever()
		broker.recordSink.maybeBeginDraining()    // awaken anything in backoff
		broker.recordSource.maybeBeginConsuming() // same
	}
	cl.brokersMu.Unlock()

	// Wait for metadata to quit so we know no more erroring topic
	// partitions will be created.
	<-cl.metadone

	// We must manually fail all partitions that never had a sink.
	for _, partitions := range cl.loadTopics() {
		for _, partition := range partitions.load().all {
			if partition.records.sink == nil {
				partition.records.failAllRecords(ErrBrokerDead)
			}
		}
	}

	cl.compressor.close()
	cl.decompressor.close()
}

// Request issues a request to Kafka, waiting for and returning the response.
// If a retriable network error occurs, or if a retriable group / transaction
// coordinator error occurs, the request is retried. All other errors are
// returned.
//
// If the request is an admin request, this will issue it to the Kafka
// controller. If the controller ID is unknown, this will attempt to fetch it.
// If the fetch errors, this will return an unknown controller error.
//
// If the request is a group or transaction coordinator request, this will
// issue the request to the appropriate group or transaction coordinator.
//
// For group coordinator requests, if the request contains multiple groups
// (delete groups, describe groups), the request will be split into one request
// for each group (since they could have different coordinators), all requests
// will be issued, and then all responses are merged. Only if all requests
// error is an error returned.
//
// In short, this tries to do the correct thing depending on what type of
// request is being issued.
//
// The passed context can be used to cancel a request and return early.
// Note that if the request is not canceled before it is written to Kafka,
// you may just end up canceling and not receiving the response to what Kafka
// inevitably does.
func (cl *Client) Request(ctx context.Context, req kmsg.Request) (kmsg.Response, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var resp kmsg.Response
	var err error
	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, err = cl.request(ctx, req)
	}()
	select {
	case <-done:
		return resp, err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-cl.ctx.Done():
		return nil, cl.ctx.Err()
	}
}

// request is the logic for Request.
func (c *Client) request(ctx context.Context, req kmsg.Request) (kmsg.Response, error) {
	var resp kmsg.Response
	var err error
	tries := 0
start:
	tries++
	if metaReq, isMetaReq := req.(*kmsg.MetadataRequest); isMetaReq {
		// We hijack any metadata request so as to populate our
		// own brokers and controller ID.
		topics := make([]string, 0, len(metaReq.Topics))
		for _, topic := range metaReq.Topics {
			topics = append(topics, topic.Topic)
		}
		resp, err = c.fetchMetadata(ctx, metaReq.Topics == nil, topics)
	} else if _, admin := req.(kmsg.AdminRequest); admin {
		var controller *broker
		if controller, err = c.controller(ctx); err == nil {
			resp, err = controller.waitResp(ctx, req)
		}
	} else if groupReq, isGroupReq := req.(kmsg.GroupCoordinatorRequest); isGroupReq {
		resp, err = c.handleGroupReq(ctx, groupReq)
	} else if txnReq, isTxnReq := req.(kmsg.TxnCoordinatorRequest); isTxnReq {
		resp, err = c.handleTxnReq(ctx, txnReq)
	} else {
		resp, err = c.broker().waitResp(ctx, req)
	}

	if (kerr.IsRetriable(err) || isRetriableBrokerErr(err)) && tries < c.cfg.retries {
		if ok := c.waitTries(ctx, tries); ok {
			goto start
		}
		return nil, err
	}
	return resp, err
}

// brokerOrErr returns the broker for ID or the error if the broker does not
// exist.
func (c *Client) brokerOrErr(id int32, err error) (*broker, error) {
	c.brokersMu.RLock()
	broker := c.brokers[id]
	c.brokersMu.RUnlock()
	if broker == nil {
		return nil, err
	}
	return broker, nil
}

// controller returns the controller broker, forcing a broker load if
// necessary.
func (c *Client) controller(ctx context.Context) (*broker, error) {
	tries := 0
start:
	var id int32
	if id = atomic.LoadInt32(&c.controllerID); id < 0 {
		tries++
		if err := c.fetchBrokerMetadata(ctx); err != nil {
			if isRetriableBrokerErr(err) && tries < c.cfg.retries {
				if ok := c.waitTries(ctx, tries); ok {
					goto start
				}
				return nil, err
			}
			return nil, err
		}
		if id = atomic.LoadInt32(&c.controllerID); id < 0 {
			return nil, &errUnknownController{id}
		}
	}

	return c.brokerOrErr(id, &errUnknownController{id})
}

const (
	coordinatorTypeGroup int8 = 0
	coordinatorTypeTxn   int8 = 1
)

type coordinatorKey struct {
	name string
	typ  int8
}

// loadController returns the group/txn coordinator for the given key, retrying
// as necessary.
func (c *Client) loadCoordinator(ctx context.Context, key coordinatorKey) (*broker, error) {
	// If there is no controller, we have never loaded brokers. We will
	// need the brokers after we know which one owns this key, so force
	// a load of the brokers now.
	if atomic.LoadInt32(&c.controllerID) < 0 {
		if _, err := c.controller(ctx); err != nil {
			return nil, err
		}
	}

	tries := 0
start:
	// This lock blocks other group lookups, but in general there should
	// only be one group and one transaction ID per client.
	c.coordinatorsMu.Lock()

	coordinator, ok := c.coordinators[key]
	if ok {
		c.coordinatorsMu.Unlock()
		return c.brokerOrErr(coordinator, &errUnknownCoordinator{coordinator, key})
	}

	tries++
	kresp, err := c.broker().waitResp(ctx, &kmsg.FindCoordinatorRequest{
		CoordinatorKey:  key.name,
		CoordinatorType: key.typ,
	})

	var resp *kmsg.FindCoordinatorResponse
	if err == nil {
		resp = kresp.(*kmsg.FindCoordinatorResponse)
		err = kerr.ErrorForCode(resp.ErrorCode)
	}

	if err != nil {
		c.coordinatorsMu.Unlock()
		if isRetriableErr(err) && tries < c.cfg.retries {
			if ok := c.waitTries(ctx, tries); ok {
				goto start
			}
			return nil, err
		}
		return nil, err
	}

	coordinator = resp.NodeID
	c.coordinators[key] = coordinator
	c.coordinatorsMu.Unlock()

	return c.brokerOrErr(coordinator, &errUnknownCoordinator{coordinator, key})

}

// handleGroupReq issues a group request.
//
// The logic for group requests is mildly convoluted; a single request can
// contain multiple groups which could go to multiple brokers due to the group
// coordinators being different.
//
// Most requests go to one coordinator; those are simple and we issue those
// simply.
//
// Those that go to multiple have the groups split into individual requests
// containing a single group. All requests are issued serially and then the
// responses are merged. We only return err if all requests error.
func (c *Client) handleGroupReq(ctx context.Context, req kmsg.GroupCoordinatorRequest) (kmsg.Response, error) {
	var group2req map[string]kmsg.Request
	var kresp kmsg.Response
	var merge func(kmsg.Response)

	switch t := req.(type) {
	default:
		// All group requests should be listed below, so if it isn't,
		// then we do not know what this request is.
		return nil, ErrClientTooOld

	case *kmsg.OffsetCommitRequest:
		return c.handleGroupReqSimple(ctx, t.Group, req)
	case *kmsg.TxnOffsetCommitRequest:
		return c.handleGroupReqSimple(ctx, t.Group, req)
	case *kmsg.OffsetFetchRequest:
		return c.handleGroupReqSimple(ctx, t.Group, req)
	case *kmsg.JoinGroupRequest:
		return c.handleGroupReqSimple(ctx, t.Group, req)
	case *kmsg.HeartbeatRequest:
		return c.handleGroupReqSimple(ctx, t.Group, req)
	case *kmsg.LeaveGroupRequest:
		return c.handleGroupReqSimple(ctx, t.Group, req)
	case *kmsg.SyncGroupRequest:
		return c.handleGroupReqSimple(ctx, t.Group, req)

	case *kmsg.DescribeGroupsRequest:
		group2req = make(map[string]kmsg.Request)
		for _, id := range t.Groups {
			group2req[id] = &kmsg.DescribeGroupsRequest{
				Groups:                      []string{id},
				IncludeAuthorizedOperations: t.IncludeAuthorizedOperations,
			}
		}
		resp := new(kmsg.DescribeGroupsResponse)
		kresp = resp
		merge = func(newKResp kmsg.Response) {
			newResp := newKResp.(*kmsg.DescribeGroupsResponse)
			resp.Version = newResp.Version
			resp.ThrottleMillis = newResp.ThrottleMillis
			resp.Groups = append(resp.Groups, newResp.Groups...)
		}

	case *kmsg.DeleteGroupsRequest:
		group2req = make(map[string]kmsg.Request)
		for _, id := range t.Groups {
			group2req[id] = &kmsg.DeleteGroupsRequest{
				Groups: []string{id},
			}
		}
		resp := new(kmsg.DeleteGroupsResponse)
		kresp = resp
		merge = func(newKResp kmsg.Response) {
			newResp := newKResp.(*kmsg.DeleteGroupsResponse)
			resp.Version = newResp.Version
			resp.ThrottleMillis = newResp.ThrottleMillis
			resp.Groups = append(resp.Groups, newResp.Groups...)
		}
	}

	var firstErr error
	var errs int
	for id, req := range group2req {
		resp, err := c.handleGroupReqSimple(ctx, id, req)
		if err != nil {
			errs++
			if firstErr == nil {
				firstErr = err
			}
		} else {
			merge(resp)
		}
	}

	if errs == len(group2req) {
		return nil, firstErr
	}
	return kresp, nil
}

// handleReqGroupSimple issues a request that contains a single group ID to
// the coordinator for the given group ID.
//
// Response errors are inspected to see if they are retriable group errors;
// that is, th group is loading or not available, not individual partition
// errors. If so, the coordinator is deleted. Thus, if a response contains
// many errors (one for each partition, say), then only one partition needs
// to be investigated.
func (c *Client) handleGroupReqSimple(ctx context.Context, group string, req kmsg.Request) (kmsg.Response, error) {
	coordinator, err := c.loadCoordinator(ctx, coordinatorKey{
		name: group,
		typ:  coordinatorTypeGroup,
	})
	if err != nil {
		return nil, err
	}
	kresp, err := coordinator.waitResp(ctx, req)
	if err != nil {
		return kresp, err
	}

	var errCode int16
	switch t := kresp.(type) {
	case *kmsg.OffsetCommitResponse:
		if len(t.Topics) > 0 && len(t.Topics[0].Partitions) > 0 {
			errCode = t.Topics[0].Partitions[0].ErrorCode
		}
	case *kmsg.TxnOffsetCommitResponse:
		if len(t.Topics) > 0 {
			if len(t.Topics[0].Partitions) > 0 {
				errCode = t.Topics[0].Partitions[0].ErrorCode
			}
		}
	case *kmsg.OffsetFetchResponse:
		if t.Version >= 2 {
			errCode = t.ErrorCode
		} else if len(t.Topics) > 0 && len(t.Topics[0].Partitions) > 0 {
			errCode = t.Topics[0].Partitions[0].ErrorCode
		}
	case *kmsg.JoinGroupResponse:
		errCode = t.ErrorCode
	case *kmsg.HeartbeatResponse:
		errCode = t.ErrorCode
	case *kmsg.LeaveGroupResponse:
		errCode = t.ErrorCode
	case *kmsg.SyncGroupResponse:
		errCode = t.ErrorCode
	case *kmsg.DescribeGroupsResponse:
		if len(t.Groups) > 0 {
			errCode = t.Groups[0].ErrorCode
		}
	case *kmsg.DeleteGroupsResponse:
		if len(t.Groups) > 0 {
			errCode = t.Groups[0].ErrorCode
		}
	}

	switch groupErr := kerr.ErrorForCode(errCode); groupErr {
	case kerr.CoordinatorNotAvailable,
		kerr.CoordinatorLoadInProgress,
		kerr.NotCoordinator:
		err = groupErr

		c.coordinatorsMu.Lock()
		delete(c.coordinators, coordinatorKey{
			name: group,
			typ:  coordinatorTypeGroup,
		})
		c.coordinatorsMu.Unlock()
	}

	return kresp, err
}

// handleTxnReq issues a transaction request.
//
// Transaction requests are not as convoluted as group requests, but we do
// still have to route the request to the proper coordinator. Doing so requires
// looking into the actual request type and pulling out the txn id.
func (c *Client) handleTxnReq(ctx context.Context, req kmsg.TxnCoordinatorRequest) (kmsg.Response, error) {
	switch t := req.(type) {
	default:
		// All txn requests should be listed below, so if it isn't,
		// then we do not know what this request is.
		return nil, ErrClientTooOld

	case *kmsg.InitProducerIDRequest:
		if t.TransactionalID != nil {
			return c.handleTxnRequest(ctx, *t.TransactionalID, req)
		}
		// InitProducerID can go to any broker if the transactional ID
		// is nil.
		return c.broker().waitResp(ctx, req)
	case *kmsg.AddPartitionsToTxnRequest:
		return c.handleTxnRequest(ctx, t.TransactionalID, req)
	case *kmsg.AddOffsetsToTxnRequest:
		return c.handleTxnRequest(ctx, t.TransactionalID, req)
	case *kmsg.EndTxnRequest:
		return c.handleTxnRequest(ctx, t.TransactionalID, req)
	}
}

// handleTxnRequest issues a request for a transaction to the coordinator for
// that transaction.
//
// The error is inspected to see if it is a retriable group error and, if so,
// the coordinator is deleted. That is, we only retry on coordinator errors,
// which would be common on all partitions. Thus, if the response contains many
// errors due to many partitions, only the first partition needs to be
// investigated.
func (c *Client) handleTxnRequest(ctx context.Context, txnID string, req kmsg.Request) (kmsg.Response, error) {
	coordinator, err := c.loadCoordinator(ctx, coordinatorKey{
		name: txnID,
		typ:  coordinatorTypeTxn,
	})
	if err != nil {
		return nil, err
	}
	kresp, err := coordinator.waitResp(ctx, req)
	if err != nil {
		return kresp, err
	}

	var errCode int16
	switch t := kresp.(type) {
	case *kmsg.InitProducerIDResponse:
		errCode = t.ErrorCode
	case *kmsg.AddPartitionsToTxnResponse:
		if len(t.Topics) > 0 {
			if len(t.Topics[0].Partitions) > 0 {
				errCode = t.Topics[0].Partitions[0].ErrorCode
			}
		}
	case *kmsg.AddOffsetsToTxnResponse:
		errCode = t.ErrorCode
	case *kmsg.EndTxnResponse:
		errCode = t.ErrorCode
	}

	switch txnErr := kerr.ErrorForCode(errCode); txnErr {
	case kerr.CoordinatorNotAvailable,
		kerr.CoordinatorLoadInProgress,
		kerr.NotCoordinator:
		err = txnErr

		c.coordinatorsMu.Lock()
		delete(c.coordinators, coordinatorKey{
			name: txnID,
			typ:  coordinatorTypeTxn,
		})
		c.coordinatorsMu.Unlock()
	}

	return kresp, err
}

// Broker returns a handle to a specific broker to directly issue requests to.
// Note that there is no guarantee that this broker exists; if it does not,
// requests will fail with ErrUnknownBroker.
func (cl *Client) Broker(id int) *Broker {
	return &Broker{
		id: int32(id),
		cl: cl,
	}
}

// Broker pairs a broker ID with a client to directly issue requests to a
// specific broker.
type Broker struct {
	id int32
	cl *Client
}

// Request issues a request to a broker. If the broker does not exist in the
// client, this returns ErrUnknownBroker. Requests are not retried.
//
// The passed context can be used to cancel a request and return early.
// Note that if the request is not canceled before it is written to Kafka,
// you may just end up canceling and not receiving the response to what Kafka
// inevitably does.
func (b *Broker) Request(ctx context.Context, req kmsg.Request) (kmsg.Response, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var resp kmsg.Response
	var err error
	done := make(chan struct{})
	go func() {
		defer close(done)
		resp, err = b.request(ctx, req)
	}()
	select {
	case <-done:
		return resp, err
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-b.cl.ctx.Done():
		return nil, b.cl.ctx.Err()
	}
}

// request is the logic for Request.
func (b *Broker) request(ctx context.Context, req kmsg.Request) (kmsg.Response, error) {
	b.cl.brokersMu.RLock()
	br, exists := b.cl.brokers[b.id]
	b.cl.brokersMu.RUnlock()

	if !exists {
		// If the broker does not exist, we try once to update brokers.
		if err := b.cl.fetchBrokerMetadata(ctx); err == nil {
			b.cl.brokersMu.RLock()
			br, exists = b.cl.brokers[b.id]
			b.cl.brokersMu.RUnlock()
			if !exists {
				return nil, ErrUnknownBroker
			}
		} else {
			return nil, ErrUnknownBroker
		}
	}

	return br.waitResp(ctx, req)
}
