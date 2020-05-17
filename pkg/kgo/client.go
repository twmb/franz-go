// Package kgo provides a pure Go efficient Kafka client for Kafka 0.8.0+ with
// support for transactions, regex topic consuming, the latest partition
// strategies, and more. This client supports all client KIPs.
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

	bufPool bufPool // for to brokers to share underlying reusable request buffers

	controllerID int32 // atomic

	producer producer
	consumer consumer

	compressor   *compressor
	decompressor *decompressor

	coordinatorsMu sync.Mutex
	coordinators   map[coordinatorKey]int32

	topicsMu sync.Mutex   // locked to prevent concurrent updates; reads are always atomic
	topics   atomic.Value // map[string]*topicPartitions

	// unknownTopics buffers all records for topics that are not loaded.
	// The map is to a pointer to a slice for reasons documented in
	// waitUnknownTopic.
	unknownTopicsMu sync.Mutex
	unknownTopics   map[string]*unknownTopicProduces

	updateMetadataCh    chan struct{}
	updateMetadataNowCh chan struct{} // like above, but with high priority
	metawait            metawait
	metadone            chan struct{}
}

// stddialer is the default dialer for dialing connections.
var stddialer = net.Dialer{Timeout: 10 * time.Second}

func stddial(addr string) (net.Conn, error) { return stddialer.Dial("tcp", addr) }

// NewClient returns a new Kafka client with the given options or an error if
// the options are invalid.
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

	cl := &Client{
		cfg:       cfg,
		ctx:       ctx,
		ctxCancel: cancel,
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),

		controllerID: unknownControllerID,
		brokers:      make(map[int32]*broker),

		bufPool: newBufPool(),

		decompressor: newDecompressor(),

		coordinators:  make(map[coordinatorKey]int32),
		unknownTopics: make(map[string]*unknownTopicProduces),

		updateMetadataCh:    make(chan struct{}, 1),
		updateMetadataNowCh: make(chan struct{}, 1),
		metadone:            make(chan struct{}),
	}
	cl.producer.init()
	cl.consumer.cl = cl
	cl.consumer.sourcesReadyCond = sync.NewCond(&cl.consumer.sourcesReadyMu)
	cl.topics.Store(make(map[string]*topicPartitions))
	cl.metawait.init()

	compressor, err := newCompressor(cl.cfg.compression...)
	if err != nil {
		return nil, err
	}
	cl.compressor = compressor

	for i, seedAddr := range seedAddrs {
		b := cl.newBroker(seedAddr, unknownSeedID(i))
		cl.brokers[b.id] = b
		cl.anyBroker = append(cl.anyBroker, b)
	}
	go cl.updateMetadataLoop()

	return cl, nil
}

// broker returns a random broker from all brokers ever known.
func (cl *Client) broker() *broker {
	cl.brokersMu.Lock()
	defer cl.brokersMu.Unlock()

	if cl.anyBrokerIdx >= len(cl.anyBroker) { // metadata update lost us brokers
		cl.anyBrokerIdx = 0
	}

	b := cl.anyBroker[cl.anyBrokerIdx]
	cl.anyBrokerIdx++
	if cl.anyBrokerIdx == len(cl.anyBroker) {
		cl.anyBrokerIdx = 0
		cl.rng.Shuffle(len(cl.anyBroker), func(i, j int) { cl.anyBroker[i], cl.anyBroker[j] = cl.anyBroker[j], cl.anyBroker[i] })
	}
	return b
}

func (cl *Client) waitTries(ctx context.Context, tries int) bool {
	after := time.NewTimer(cl.cfg.retryBackoff(tries))
	defer after.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-cl.ctx.Done():
		return false
	case <-after.C:
		return true
	}
}

// fetchBrokerMetadata issues a metadata request solely for broker information.
func (cl *Client) fetchBrokerMetadata(ctx context.Context) error {
	_, err := cl.fetchMetadata(ctx, false, nil)
	return err
}

func (cl *Client) fetchMetadata(ctx context.Context, all bool, topics []string) (*kmsg.MetadataResponse, error) {
	if all {
		topics = nil
	} else if len(topics) == 0 {
		topics = []string{}
	}
	tries := 0
	tryStart := time.Now()
start:
	tries++
	broker := cl.broker()
	req := &kmsg.MetadataRequest{
		AllowAutoTopicCreation: cl.cfg.allowAutoTopicCreation,
		// DO NOT preallocate topics, since nil is significant
	}
	for _, topic := range topics {
		req.Topics = append(req.Topics, kmsg.MetadataRequestTopic{Topic: topic})
	}
	kresp, err := broker.waitResp(ctx, req)
	if err != nil {
		if cl.cfg.retryTimeout > 0 && time.Since(tryStart) > cl.cfg.retryTimeout {
			return nil, err
		}
		if kerr.IsRetriable(err) && tries < cl.cfg.retries || isRetriableBrokerErr(err) && tries < cl.cfg.brokerErrRetries {
			if ok := cl.waitTries(ctx, tries); ok {
				goto start
			}
			return nil, err
		}
		return nil, err
	}
	meta := kresp.(*kmsg.MetadataResponse)
	if meta.ControllerID >= 0 {
		atomic.StoreInt32(&cl.controllerID, meta.ControllerID)
	}
	cl.updateBrokers(meta.Brokers)
	return meta, err
}

// updateBrokers is called with the broker portion of every metadata response.
// All metadata responses contain all known live brokers, so we can always
// use the response.
func (cl *Client) updateBrokers(brokers []kmsg.MetadataResponseBroker) {
	newBrokers := make(map[int32]*broker, len(brokers))
	newAnyBroker := make([]*broker, 0, len(brokers))

	cl.brokersMu.Lock()
	defer cl.brokersMu.Unlock()

	if cl.stopBrokers {
		return
	}

	for _, broker := range brokers {
		addr := net.JoinHostPort(broker.Host, strconv.Itoa(int(broker.Port)))

		b, exists := cl.brokers[broker.NodeID]
		if exists {
			delete(cl.brokers, b.id)
			if b.addr != addr {
				b.stopForever()
				b = cl.newBroker(addr, b.id)
			}
		} else {
			b = cl.newBroker(addr, broker.NodeID)
		}

		newBrokers[b.id] = b
		newAnyBroker = append(newAnyBroker, b)
	}

	for goneID, goneBroker := range cl.brokers {
		if goneID < -1 { // seed broker, unknown ID, always keep
			newBrokers[goneID] = goneBroker
			newAnyBroker = append(newAnyBroker, goneBroker)
		} else {
			goneBroker.stopForever()
		}
	}

	cl.brokers = newBrokers
	cl.anyBroker = newAnyBroker
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
		broker.sink.maybeDrain()     // awaken anything in backoff
		broker.source.maybeConsume() // same
	}
	cl.brokersMu.Unlock()

	// Wait for metadata to quit so we know no more erroring topic
	// partitions will be created.
	<-cl.metadone

	// We must manually fail all partitions that never had a sink.
	for _, partitions := range cl.loadTopics() {
		for _, partition := range partitions.load().all {
			partition.records.failAllRecords(ErrBrokerDead)
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
// (delete groups, describe groups), the request is split into one request per
// broker containing the groups that broker can respond to. Thus, you do not
// have to worry about maxing groups that different brokers are coordinators
// for. All responses are merged. Only if all requests error is an error
// returned.
//
// For transaction requests, the request is issued to the transaction
// coordinator. However, if the request is an init producer ID request and the
// request has no transactional ID, the request goes to any broker.
//
// In short, this method tries to do the correct thing depending on what type
// of request is being issued.
//
// The passed context can be used to cancel a request and return early. Note
// that if the request is not canceled before it is written to Kafka, you may
// just end up canceling and not receiving the response to what Kafka
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
func (cl *Client) request(ctx context.Context, req kmsg.Request) (kmsg.Response, error) {
	var resp kmsg.Response
	var err error
	tries := 0
	tryStart := time.Now()
start:
	tries++
	if metaReq, isMetaReq := req.(*kmsg.MetadataRequest); isMetaReq {
		// We hijack any metadata request so as to populate our
		// own brokers and controller ID.
		topics := make([]string, 0, len(metaReq.Topics))
		for _, topic := range metaReq.Topics {
			topics = append(topics, topic.Topic)
		}
		// fetchMetadata does its own retrying, so we do not go
		// into the retrying logic below.
		return cl.fetchMetadata(ctx, metaReq.Topics == nil, topics)
	} else if _, admin := req.(kmsg.AdminRequest); admin {
		var controller *broker
		if controller, err = cl.controller(ctx); err == nil {
			resp, err = controller.waitResp(ctx, req)
		}
	} else if groupReq, isGroupReq := req.(kmsg.GroupCoordinatorRequest); isGroupReq {
		resp, err = cl.handleCoordinatorReq(ctx, groupReq, coordinatorTypeGroup)
	} else if txnReq, isTxnReq := req.(kmsg.TxnCoordinatorRequest); isTxnReq {
		resp, err = cl.handleCoordinatorReq(ctx, txnReq, coordinatorTypeTxn)
	} else {
		resp, err = cl.broker().waitResp(ctx, req)
	}

	if err != nil {
		if cl.cfg.retryTimeout > 0 && time.Since(tryStart) > cl.cfg.retryTimeout {
			return nil, err
		}
		if kerr.IsRetriable(err) && tries < cl.cfg.retries || isRetriableBrokerErr(err) && tries < cl.cfg.brokerErrRetries {
			if ok := cl.waitTries(ctx, tries); ok {
				goto start
			}
			return nil, err
		}
	}
	return resp, err
}

// brokerOrErr returns the broker for ID or the error if the broker does not
// exist.
func (cl *Client) brokerOrErr(id int32, err error) (*broker, error) {
	cl.brokersMu.RLock()
	broker := cl.brokers[id]
	cl.brokersMu.RUnlock()
	if broker == nil {
		return nil, err
	}
	return broker, nil
}

// controller returns the controller broker, forcing a broker load if
// necessary.
func (cl *Client) controller(ctx context.Context) (*broker, error) {
	tries := 0
	tryStart := time.Now()
start:
	var id int32
	if id = atomic.LoadInt32(&cl.controllerID); id < 0 {
		tries++
		if err := cl.fetchBrokerMetadata(ctx); err != nil {
			if cl.cfg.retryTimeout > 0 && time.Since(tryStart) > cl.cfg.retryTimeout {
				return nil, err
			}
			if kerr.IsRetriable(err) && tries < cl.cfg.retries || isRetriableBrokerErr(err) && tries < cl.cfg.brokerErrRetries {
				if ok := cl.waitTries(ctx, tries); ok {
					goto start
				}
				return nil, err
			}
			return nil, err
		}
		if id = atomic.LoadInt32(&cl.controllerID); id < 0 {
			return nil, &errUnknownController{id}
		}
	}

	return cl.brokerOrErr(id, &errUnknownController{id})
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
func (cl *Client) loadCoordinator(ctx context.Context, key coordinatorKey) (*broker, error) {
	// If there is no controller, we have never loaded brokers. We will
	// need the brokers after we know which one owns this key, so force
	// a load of the brokers now.
	if atomic.LoadInt32(&cl.controllerID) < 0 {
		if _, err := cl.controller(ctx); err != nil {
			return nil, err
		}
	}

	tries := 0
	tryStart := time.Now()
start:
	cl.coordinatorsMu.Lock()
	coordinator, ok := cl.coordinators[key]
	cl.coordinatorsMu.Unlock()

	if ok {
		return cl.brokerOrErr(coordinator, &errUnknownCoordinator{coordinator, key})
	}

	tries++
	kresp, err := cl.broker().waitResp(ctx, &kmsg.FindCoordinatorRequest{
		CoordinatorKey:  key.name,
		CoordinatorType: key.typ,
	})

	var resp *kmsg.FindCoordinatorResponse
	if err == nil {
		resp = kresp.(*kmsg.FindCoordinatorResponse)
		err = kerr.ErrorForCode(resp.ErrorCode)
	}

	if err != nil {
		if cl.cfg.retryTimeout > 0 && time.Since(tryStart) > cl.cfg.retryTimeout {
			return nil, err
		}
		if kerr.IsRetriable(err) && tries < cl.cfg.retries || isRetriableBrokerErr(err) && tries < cl.cfg.brokerErrRetries {
			if ok := cl.waitTries(ctx, tries); ok {
				goto start
			}
			return nil, err
		}
		return nil, err
	}

	coordinator = resp.NodeID
	cl.coordinatorsMu.Lock()
	cl.coordinators[key] = coordinator
	cl.coordinatorsMu.Unlock()

	return cl.brokerOrErr(coordinator, &errUnknownCoordinator{coordinator, key})
}

// loadCoordinators does a concurrent load of many coordinators.
func (cl *Client) loadCoordinators(typ int8, names ...string) (map[string]*broker, error) {
	ctx, cancel := context.WithCancel(cl.ctx)
	defer cancel()

	var mu sync.Mutex
	m := make(map[string]*broker)
	var errQuit error

	var wg sync.WaitGroup
	for _, name := range names {
		myName := name
		wg.Add(1)
		go func() {
			defer wg.Done()
			coordinator, err := cl.loadCoordinator(ctx, coordinatorKey{
				name: myName,
				typ:  typ,
			})

			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				if errQuit != nil {
					errQuit = err
					cancel()
				}
				return
			}
			m[myName] = coordinator
		}()
	}
	wg.Wait()

	return m, errQuit
}

// handleCoordinatorEq issues group or txn requests.
//
// The logic for group requests is mildly convoluted; a single request can
// contain multiple groups which could go to multiple brokers due to the group
// coordinators being different.
//
// All transaction requests are simple.
//
// Most requests go to one coordinator; those are simple and we issue those
// simply.
//
// Requests that go to multiple have the groups split into individual requests
// containing a single group. We only return err if all requests error.
func (cl *Client) handleCoordinatorReq(ctx context.Context, req kmsg.Request, typ int8) (kmsg.Response, error) {
	// If we have to split requests, the following four variables are
	// used for splitting and then merging responses.
	var (
		broker2req map[*broker]kmsg.Request
		names      []string
		kresp      kmsg.Response
		merge      func(kmsg.Response)
	)

	switch t := req.(type) {
	default:
		// All group requests should be listed below, so if it isn't,
		// then we do not know what this request is.
		return nil, ErrClientTooOld

	/////////
	// TXN // -- all txn reqs are simple
	/////////

	case *kmsg.InitProducerIDRequest:
		if t.TransactionalID != nil {
			return cl.handleCoordinatorReqSimple(ctx, coordinatorTypeTxn, *t.TransactionalID, req)
		}
		// InitProducerID can go to any broker if the transactional ID
		// is nil. By using handleReqWithCoordinator, we get the
		// retriable-error parsing, even though we are not actually
		// using a defined txn coordinator. This is fine; by passing no
		// names, we delete no coordinator.
		return cl.handleReqWithCoordinator(ctx, cl.broker(), coordinatorTypeTxn, nil, req)
	case *kmsg.AddPartitionsToTxnRequest:
		return cl.handleCoordinatorReqSimple(ctx, coordinatorTypeTxn, t.TransactionalID, req)
	case *kmsg.AddOffsetsToTxnRequest:
		return cl.handleCoordinatorReqSimple(ctx, coordinatorTypeTxn, t.TransactionalID, req)
	case *kmsg.EndTxnRequest:
		return cl.handleCoordinatorReqSimple(ctx, coordinatorTypeTxn, t.TransactionalID, req)

	///////////
	// GROUP // -- most group reqs are simple
	///////////

	case *kmsg.OffsetCommitRequest:
		return cl.handleCoordinatorReqSimple(ctx, coordinatorTypeGroup, t.Group, req)
	case *kmsg.TxnOffsetCommitRequest:
		return cl.handleCoordinatorReqSimple(ctx, coordinatorTypeGroup, t.Group, req)
	case *kmsg.OffsetFetchRequest:
		return cl.handleCoordinatorReqSimple(ctx, coordinatorTypeGroup, t.Group, req)
	case *kmsg.JoinGroupRequest:
		return cl.handleCoordinatorReqSimple(ctx, coordinatorTypeGroup, t.Group, req)
	case *kmsg.HeartbeatRequest:
		return cl.handleCoordinatorReqSimple(ctx, coordinatorTypeGroup, t.Group, req)
	case *kmsg.LeaveGroupRequest:
		return cl.handleCoordinatorReqSimple(ctx, coordinatorTypeGroup, t.Group, req)
	case *kmsg.SyncGroupRequest:
		return cl.handleCoordinatorReqSimple(ctx, coordinatorTypeGroup, t.Group, req)

	case *kmsg.DescribeGroupsRequest:
		for _, group := range t.Groups {
			names = append(names, group)
		}
		coordinators, err := cl.loadCoordinators(coordinatorTypeGroup, names...)
		if err != nil {
			return nil, err
		}
		broker2req = make(map[*broker]kmsg.Request)

		for _, group := range t.Groups {
			broker := coordinators[group]
			if broker2req[broker] == nil {
				broker2req[broker] = &kmsg.DescribeGroupsRequest{
					IncludeAuthorizedOperations: t.IncludeAuthorizedOperations,
				}
			}
			req := broker2req[broker].(*kmsg.DescribeGroupsRequest)
			req.Groups = append(req.Groups, group)
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
		for _, group := range t.Groups {
			names = append(names, group)
		}
		coordinators, err := cl.loadCoordinators(coordinatorTypeGroup, names...)
		if err != nil {
			return nil, err
		}
		broker2req = make(map[*broker]kmsg.Request)

		for _, group := range t.Groups {
			broker := coordinators[group]
			if broker2req[broker] == nil {
				broker2req[broker] = new(kmsg.DeleteGroupsRequest)
			}
			req := broker2req[broker].(*kmsg.DeleteGroupsRequest)
			req.Groups = append(req.Groups, group)
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

	var (
		mergeMu  sync.Mutex
		wg       sync.WaitGroup
		firstErr error
		errs     int
	)
	for broker, req := range broker2req {
		wg.Add(1)
		myBroker, myReq := broker, req
		go func() {
			defer wg.Done()
			resp, err := cl.handleReqWithCoordinator(ctx, myBroker, typ, names, myReq)

			mergeMu.Lock()
			defer mergeMu.Unlock()

			if err != nil {
				errs++
				if firstErr == nil {
					firstErr = err
				}
				return
			}
			merge(resp)
		}()
	}
	wg.Wait()

	if errs == len(broker2req) {
		return nil, firstErr
	}
	return kresp, nil
}

// handleCoordinatorReqSimple issues a request that contains a single group or
// txn to its coordinator.
//
// The error is inspected to see if it is a retriable error and, if so, the
// coordinator is deleted. That is, we only retry on coordinator errors, which
// would be common on all partitions. Thus, if the response contains many
// errors due to many partitions, only the first partition needs to be
// investigated.
func (cl *Client) handleCoordinatorReqSimple(ctx context.Context, typ int8, name string, req kmsg.Request) (kmsg.Response, error) {
	coordinator, err := cl.loadCoordinator(ctx, coordinatorKey{
		name: name,
		typ:  typ,
	})
	if err != nil {
		return nil, err
	}
	return cl.handleReqWithCoordinator(ctx, coordinator, typ, []string{name}, req)
}

// handleReqWithCoordinator actually issues a request to a coordinator and
// does retry error parsing.
func (cl *Client) handleReqWithCoordinator(
	ctx context.Context,
	coordinator *broker,
	typ int8,
	names []string, // group IDs or the transactional id
	req kmsg.Request,
) (kmsg.Response, error) {
	kresp, err := coordinator.waitResp(ctx, req)
	if err != nil {
		return kresp, err
	}

	var errCode int16
	switch t := kresp.(type) {

	/////////
	// TXN //
	/////////

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

	///////////
	// GROUP //
	///////////

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

	switch retriableErr := kerr.ErrorForCode(errCode); retriableErr {
	case kerr.CoordinatorNotAvailable,
		kerr.CoordinatorLoadInProgress,
		kerr.NotCoordinator:
		err = retriableErr

		cl.coordinatorsMu.Lock()
		for _, name := range names {
			delete(cl.coordinators, coordinatorKey{
				name: name,
				typ:  typ,
			})
		}
		cl.coordinatorsMu.Unlock()
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
