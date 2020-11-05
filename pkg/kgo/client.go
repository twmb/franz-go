// Package kgo provides a pure Go efficient Kafka client for Kafka 0.8.0+ with
// support for transactions, regex topic consuming, the latest partition
// strategies, and more. This client aims to support all KIPs.
//
// This client aims to be simple to use while still interacting with Kafka in a
// near ideal way. If any of this client is confusing, please raise GitHub
// issues so we can make this clearer.
//
// For more overview of the entire client itself, please see the package
// source's README.
//
// Note that the default group consumer balancing strategy is
// "cooperative-sticky", which is incompatible with the historical (pre 2.4.0)
// balancers. If you are planning to work with an older Kafka or in an existing
// consumer group that uses eager balancers, be sure to use the Balancers
// option when assigning a group. See the documentation on balancers for more
// information.
package kgo

import (
	"context"
	"fmt"
	"math/rand"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
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

	reqFormatter  *kmsg.RequestFormatter
	connTimeoutFn func(kmsg.Request) (time.Duration, time.Duration)

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

// NewClient returns a new Kafka client with the given options or an error if
// the options are invalid. Connections to brokers are lazily created only when
// requests are written to them.
//
// By default, the client uses the latest stable request versions when talking
// to Kafka. If you use a broker older than 0.10.0, then you need to manually
// set a MaxVersions option. Otherwise, there is usually no harm in defaulting
// to the latest API versions, although occasionally Kafka introduces new
// required parameters that do not have zero value defaults.
//
// NewClient also launches a goroutine which periodically updates the cached
// topic metadata.
func NewClient(opts ...Opt) (*Client, error) {
	cfg := defaultCfg()
	for _, opt := range opts {
		opt.apply(&cfg)
	}

	if err := cfg.validate(); err != nil {
		return nil, err
	}

	type hostport struct {
		host string
		port int32
	}
	seeds := make([]hostport, 0, len(cfg.seedBrokers))
	for _, seedBroker := range cfg.seedBrokers {
		addr := seedBroker
		port := int32(9092) // default kafka port
		if colon := strings.IndexByte(addr, ':'); colon > 0 {
			port64, err := strconv.ParseInt(addr[colon+1:], 10, 64)
			if err != nil {
				return nil, fmt.Errorf("unable to parse addr:port in %q", seedBroker)
			}
			addr = addr[:colon]
			port = int32(port64)
		}

		if addr == "localhost" {
			addr = "127.0.0.1"
		}

		seeds = append(seeds, hostport{addr, port})
	}

	ctx, cancel := context.WithCancel(context.Background())

	cl := &Client{
		cfg:       cfg,
		ctx:       ctx,
		ctxCancel: cancel,
		rng:       rand.New(rand.NewSource(time.Now().UnixNano())),

		controllerID: unknownControllerID,
		brokers:      make(map[int32]*broker),

		reqFormatter:  new(kmsg.RequestFormatter),
		connTimeoutFn: connTimeoutBuilder(cfg.connTimeoutOverhead),

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

	if cfg.id != nil {
		cl.reqFormatter = kmsg.NewRequestFormatter(kmsg.FormatterClientID(*cfg.id))
	}

	compressor, err := newCompressor(cl.cfg.compression...)
	if err != nil {
		return nil, err
	}
	cl.compressor = compressor

	for i, seed := range seeds {
		b := cl.newBroker(unknownSeedID(i), seed.host, seed.port, nil)
		cl.brokers[b.meta.NodeID] = b
		cl.anyBroker = append(cl.anyBroker, b)
	}
	go cl.updateMetadataLoop()

	return cl, nil
}

func connTimeoutBuilder(defaultTimeout time.Duration) func(kmsg.Request) (time.Duration, time.Duration) {
	var joinMu sync.Mutex
	var lastRebalanceTimeout time.Duration

	return func(req kmsg.Request) (read, write time.Duration) {
		// We use a default of 15s for all write timeouts. Since we
		// build requests in memory and flush in one go, we expect
		// the process of writing to the connection to be quick.
		// 15s is mighty generous.
		const def = 15 * time.Second
		millis := func(m int32) time.Duration { return time.Duration(m) * time.Millisecond }
		switch t := req.(type) {
		default:
			// Many fields in the definitions have a common field
			// "TimeoutMillis". If that exists and is an int32,
			// we use it, otherwise we fallback to our default
			// for both read and write.
			v := reflect.Indirect(reflect.ValueOf(req))
			if v.Kind() == reflect.Struct { // should be but just in case
				v = v.FieldByName("TimeoutMillis")
				var zero reflect.Value
				if v != zero {
					v := v.Interface()
					if timeoutMillis, ok := v.(int32); ok {
						return def + millis(timeoutMillis), def
					}
				}
			}
			return def, def

		case *produceRequest:
			return def + millis(t.timeout), def
		case *fetchRequest:
			return def + millis(t.maxWait), def
		case *kmsg.FetchRequest:
			return def + millis(t.MaxWaitMillis), def

		// SASL may interact with an external system; we give each step
		// of the read process 30s by default.

		case *kmsg.SASLHandshakeRequest,
			*kmsg.SASLAuthenticateRequest:
			return 30 * time.Second, def

		// Join and sync can take a long time. Sync has no notion of
		// timeouts, but since the flow of requests should be first
		// join, then sync, we can stash the timeout from the join.

		case *kmsg.JoinGroupRequest:
			joinMu.Lock()
			lastRebalanceTimeout = millis(t.RebalanceTimeoutMillis)
			joinMu.Unlock()

			return def + millis(t.RebalanceTimeoutMillis), def
		case *kmsg.SyncGroupRequest:
			read := def
			joinMu.Lock()
			if lastRebalanceTimeout != 0 {
				read = lastRebalanceTimeout
			}
			joinMu.Unlock()

			return read, def

		}
	}
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
	retryTimeout := cl.cfg.retryTimeout(3) // 3 is metadata request key
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
		if retryTimeout > 0 && time.Since(tryStart) > retryTimeout {
			return nil, err
		}
		if err == ErrConnDead && tries < cl.cfg.brokerConnDeadRetries || (kerr.IsRetriable(err) || isRetriableBrokerErr(err)) && tries < cl.cfg.retries {
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
		b, exists := cl.brokers[broker.NodeID]
		if exists {
			// delete the broker to avoid stopping it below in goneBrokers
			delete(cl.brokers, broker.NodeID)
			if !b.meta.equals(broker) {
				b.stopForever()
				b = cl.newBroker(broker.NodeID, broker.Host, broker.Port, broker.Rack)
			}
		} else {
			b = cl.newBroker(broker.NodeID, broker.Host, broker.Port, broker.Rack)
		}

		newBrokers[broker.NodeID] = b
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
// If the request is an of a ListOffsets, DeleteRecords, OffsetForLeaderEpoch,
// AlterReplicaLogDirs, or DescribeLogDirs request, this will properly split
// the request to send partitions to the appropriate broker. If you want to
// ensure the request is not split and instead sent directly to a single
// broker, use the Broker function.
//
// If the request is a ListGroups request, this will send ListGroups to every
// known broker after a broker metadata lookup. The first error code of any
// response is kept, and all responded groups are merged.
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
	retryTimeout := cl.cfg.retryTimeout(req.Key())
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
		switch t := req.(type) {
		case *kmsg.ListOffsetsRequest,
			*kmsg.DeleteRecordsRequest,
			*kmsg.OffsetForLeaderEpochRequest,
			*kmsg.AlterReplicaLogDirsRequest,
			*kmsg.DescribeLogDirsRequest:
			resp, err = cl.handleShardedReq(ctx, req)
		case *kmsg.ListGroupsRequest:
			resp, err = cl.handleListGroupsReq(ctx, t)
		case *kmsg.ApiVersionsRequest:
			// As of v3, software name and version are required.
			// If they are missing, we use the config options.
			if t.ClientSoftwareName == "" && t.ClientSoftwareVersion == "" {
				dup := *t
				dup.ClientSoftwareName = cl.cfg.softwareName
				dup.ClientSoftwareVersion = cl.cfg.softwareVersion
				req = &dup
			}
			resp, err = cl.broker().waitResp(ctx, req)
		default:
			resp, err = cl.broker().waitResp(ctx, req)
		}
	}

	if err != nil {
		if retryTimeout > 0 && time.Since(tryStart) > retryTimeout {
			return nil, err
		}
		if err == ErrConnDead && tries < cl.cfg.brokerConnDeadRetries || (kerr.IsRetriable(err) || isRetriableBrokerErr(err)) && tries < cl.cfg.retries {
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
	var id int32
	if id = atomic.LoadInt32(&cl.controllerID); id < 0 {
		if err := cl.fetchBrokerMetadata(ctx); err != nil {
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
	retryTimeout := cl.cfg.retryTimeout(10) // 10 is find coordinator key
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
		if retryTimeout > 0 && time.Since(tryStart) > retryTimeout {
			return nil, err
		}
		if err == ErrConnDead && tries < cl.cfg.brokerConnDeadRetries || (kerr.IsRetriable(err) || isRetriableBrokerErr(err)) && tries < cl.cfg.retries {
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
		names = append(names, t.Groups...)
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
		names = append(names, t.Groups...)
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
		return kresp, firstErr
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

// DiscoveredBrokers returns all brokers that were discovered from prior
// metadata responses. This does not actually issue a metadata request to load
// brokers; if you wish to ensure this returns all brokers, be sure to manually
// issue a metadata request before this. This also does not include seed
// brokers, which are internally saved under special internal broker IDs (but,
// it does include those brokers under their normal IDs as returned from a
// metadata response).
func (cl *Client) DiscoveredBrokers() []*Broker {
	cl.brokersMu.Lock()
	defer cl.brokersMu.Unlock()

	var bs []*Broker
	for _, broker := range cl.brokers {
		if broker.meta.NodeID >= 0 {
			bs = append(bs, &Broker{id: broker.meta.NodeID, cl: cl})
		}
	}
	return bs
}

// SeedBrokers returns the all seed brokers.
func (cl *Client) SeedBrokers() []*Broker {
	cl.brokersMu.Lock()
	defer cl.brokersMu.Unlock()

	var bs []*Broker
	for i := 0; ; i++ {
		id := unknownSeedID(i)
		if _, exists := cl.brokers[id]; !exists {
			return bs
		}
		bs = append(bs, &Broker{id: id, cl: cl})
	}
}

// handleListGroupsReq issues a list group request to every broker following a
// metadata update. We do no retries unless everything fails, at which point
// the calling function will retry.
func (cl *Client) handleListGroupsReq(ctx context.Context, req *kmsg.ListGroupsRequest) (kmsg.Response, error) {
	if err := cl.fetchBrokerMetadata(ctx); err != nil {
		return nil, err
	}

	var wg sync.WaitGroup
	type respErr struct {
		resp kmsg.Response
		err  error
	}
	cl.brokersMu.RLock()
	respErrs := make(chan respErr, len(cl.brokers))
	var numReqs int
	for _, br := range cl.brokers {
		if br.meta.NodeID < 0 {
			continue // we skip seed brokers
		}
		wg.Add(1)
		numReqs++
		myReq := *req
		go func(br *broker) {
			defer wg.Done()
			resp, err := br.waitResp(ctx, &myReq)
			respErrs <- respErr{resp, err}
		}(br)
	}
	cl.brokersMu.RUnlock()
	wg.Wait()
	close(respErrs)

	var mergeResp kmsg.ListGroupsResponse
	var firstErr error
	var errs int
	for re := range respErrs {
		if re.err != nil {
			if firstErr == nil {
				firstErr = re.err
				errs++
			}
			continue
		}
		resp := re.resp.(*kmsg.ListGroupsResponse)
		if mergeResp.ErrorCode == 0 {
			mergeResp.ErrorCode = resp.ErrorCode
		}
		mergeResp.Version = resp.Version
		mergeResp.ThrottleMillis = resp.ThrottleMillis
		mergeResp.Groups = append(mergeResp.Groups, resp.Groups...)
	}

	if errs == numReqs {
		return nil, firstErr
	}
	return &mergeResp, nil
}

// handleShardedReq is simple-in-theory function that is long due to types.
// This simply sends all partitions of a broker-sharded request to the
// appropriate brokers and then merges the response.
//
// Handled:
//   - list offsets (key 2)
//   - delete records request (key 21)
//   - offset for leader epoch (key 23)
//   - alter replica log dirs (key 34)
//   - describe log dirs (key 35)
func (cl *Client) handleShardedReq(ctx context.Context, req kmsg.Request) (kmsg.Response, error) {
	// First, pull out the topics from either request and set them as
	// topics we need to load metadata for.
	var needTopics []string
	switch t := req.(type) {
	case *kmsg.ListOffsetsRequest:
		for _, topic := range t.Topics {
			needTopics = append(needTopics, topic.Topic)
		}
	case *kmsg.DeleteRecordsRequest:
		for _, topic := range t.Topics {
			needTopics = append(needTopics, topic.Topic)
		}
	case *kmsg.OffsetForLeaderEpochRequest:
		for _, topic := range t.Topics {
			needTopics = append(needTopics, topic.Topic)
		}
	case *kmsg.AlterReplicaLogDirsRequest:
		for _, dir := range t.Dirs {
			for _, topic := range dir.Topics {
				needTopics = append(needTopics, topic.Topic)
			}
		}
	case *kmsg.DescribeLogDirsRequest:
		for _, topic := range t.Topics {
			needTopics = append(needTopics, topic.Topic)
		}
	}
	cl.topicsMu.Lock()
	topics := cl.cloneTopics()
	for _, topic := range needTopics {
		if _, exists := topics[topic]; !exists {
			topics[topic] = newTopicPartitions(topic)
		}
	}
	cl.topics.Store(topics)
	cl.topicsMu.Unlock()

	// While we have not loaded metadata for *all* of the topics, force
	// load metadata. Ideally, this will only wait for one metadata.
	needLoad := true
	for needLoad && ctx.Err() == nil {
		cl.waitmeta(ctx, 5*time.Second)
		needLoad = false
		topics = cl.loadTopics()
		for _, topic := range needTopics {
			topicPartitions := topics[topic].load()
			if len(topicPartitions.all) == 0 && topicPartitions.loadErr == nil {
				needLoad = true
			}
		}
	}

	// Now, we split the incoming request by broker that handles the
	// request's partitions.
	broker2req := make(map[*broker]kmsg.Request)
	var kresp kmsg.Response
	var merge func(kmsg.Response) // serially called
	var finalize func()

	// We hold the brokers mu while determining what to split by
	// so that we can look up leader partitions.
	cl.brokersMu.RLock()
	brokers := cl.brokers

	switch t := req.(type) {
	case *kmsg.ListOffsetsRequest:
		resp := new(kmsg.ListOffsetsResponse)
		kresp = resp

		reqParts := make(map[*broker]map[string][]kmsg.ListOffsetsRequestTopicPartition)
		respParts := make(map[string][]kmsg.ListOffsetsResponseTopicPartition)

		// Over all the req topics,
		for _, topic := range t.Topics {
			topicPartitions := topics[topic.Topic].load()
			// Over each topic's partitions,
			for _, partition := range topic.Partitions {
				// if we could not load the metadata for this partition, we save
				// in the resp UnknownTopicOrPartition,
				topicPartition, exists := topicPartitions.all[partition.Partition]
				if !exists {
					respParts[topic.Topic] = append(respParts[topic.Topic], kmsg.ListOffsetsResponseTopicPartition{
						Partition: partition.Partition,
						ErrorCode: kerr.UnknownTopicOrPartition.Code,
					})
					continue
				}

				// or, if we could load the metadata, but the load err is non-nil
				// or the broker is nil, we save an error,
				broker := brokers[topicPartition.leader]
				if topicPartition.loadErr != nil || broker == nil {
					errCode := kerr.UnknownServerError.Code
					if topicPartition.loadErr != nil {
						if ke, ok := topicPartition.loadErr.(*kerr.Error); ok {
							errCode = ke.Code
						}
					}
					respParts[topic.Topic] = append(respParts[topic.Topic], kmsg.ListOffsetsResponseTopicPartition{
						Partition: partition.Partition,
						ErrorCode: errCode,
					})
					continue
				}

				// otherwise, for this broker, we ask for this partition.
				brokerReqParts := reqParts[broker]
				if brokerReqParts == nil {
					brokerReqParts = make(map[string][]kmsg.ListOffsetsRequestTopicPartition)
					reqParts[broker] = brokerReqParts
				}
				brokerReqParts[topic.Topic] = append(brokerReqParts[topic.Topic], partition)
			}
		}

		// Now, over each req part (per broker request), we initialize our
		// request for the broker.
		for broker, brokerReqParts := range reqParts {
			req := &kmsg.ListOffsetsRequest{
				ReplicaID:      t.ReplicaID,
				IsolationLevel: t.IsolationLevel,
			}
			for topic, parts := range brokerReqParts {
				req.Topics = append(req.Topics, kmsg.ListOffsetsRequestTopic{
					Topic:      topic,
					Partitions: parts,
				})
			}
			broker2req[broker] = req
		}

		// Merging the responses is pretty simple. We always keep the final version / throttle,
		// but we expect both to be identical across all brokers.
		merge = func(newKResp kmsg.Response) {
			newResp := newKResp.(*kmsg.ListOffsetsResponse)
			resp.Version = newResp.Version
			resp.ThrottleMillis = newResp.ThrottleMillis

			for _, topic := range newResp.Topics {
				respParts[topic.Topic] = append(respParts[topic.Topic], topic.Partitions...)
			}
		}

		// To finalize the response, we take our per-topic respParts and merge
		// them into our final response.
		finalize = func() {
			for topic, parts := range respParts {
				resp.Topics = append(resp.Topics, kmsg.ListOffsetsResponseTopic{
					Topic:      topic,
					Partitions: parts,
				})
			}
		}

	case *kmsg.DeleteRecordsRequest: // similar to above, except types
		resp := new(kmsg.DeleteRecordsResponse)
		kresp = resp

		reqParts := make(map[*broker]map[string][]kmsg.DeleteRecordsRequestTopicPartition)
		respParts := make(map[string][]kmsg.DeleteRecordsResponseTopicPartition)

		for _, topic := range t.Topics {
			topicPartitions := topics[topic.Topic].load()
			for _, partition := range topic.Partitions {
				topicPartition, exists := topicPartitions.all[partition.Partition]
				if !exists {
					respParts[topic.Topic] = append(respParts[topic.Topic], kmsg.DeleteRecordsResponseTopicPartition{
						Partition: partition.Partition,
						ErrorCode: kerr.UnknownTopicOrPartition.Code,
					})
					continue
				}

				broker := brokers[topicPartition.leader]
				if topicPartition.loadErr != nil || broker == nil {
					errCode := kerr.UnknownServerError.Code
					if topicPartition.loadErr != nil {
						if ke, ok := topicPartition.loadErr.(*kerr.Error); ok {
							errCode = ke.Code
						}
					}
					respParts[topic.Topic] = append(respParts[topic.Topic], kmsg.DeleteRecordsResponseTopicPartition{
						Partition: partition.Partition,
						ErrorCode: errCode,
					})
					continue
				}

				brokerReqParts := reqParts[broker]
				if brokerReqParts == nil {
					brokerReqParts = make(map[string][]kmsg.DeleteRecordsRequestTopicPartition)
					reqParts[broker] = brokerReqParts
				}
				brokerReqParts[topic.Topic] = append(brokerReqParts[topic.Topic], partition)
			}
		}

		for broker, brokerReqParts := range reqParts {
			req := &kmsg.DeleteRecordsRequest{
				TimeoutMillis: t.TimeoutMillis,
			}
			for topic, parts := range brokerReqParts {
				req.Topics = append(req.Topics, kmsg.DeleteRecordsRequestTopic{
					Topic:      topic,
					Partitions: parts,
				})
			}
			broker2req[broker] = req
		}
		merge = func(newKResp kmsg.Response) {
			newResp := newKResp.(*kmsg.DeleteRecordsResponse)
			resp.Version = newResp.Version
			resp.ThrottleMillis = newResp.ThrottleMillis

			for _, topic := range newResp.Topics {
				respParts[topic.Topic] = append(respParts[topic.Topic], topic.Partitions...)
			}
		}

		finalize = func() {
			for topic, parts := range respParts {
				resp.Topics = append(resp.Topics, kmsg.DeleteRecordsResponseTopic{
					Topic:      topic,
					Partitions: parts,
				})
			}
		}

	case *kmsg.OffsetForLeaderEpochRequest: // similar to above, except types
		resp := new(kmsg.OffsetForLeaderEpochResponse)
		kresp = resp

		reqParts := make(map[*broker]map[string][]kmsg.OffsetForLeaderEpochRequestTopicPartition)
		respParts := make(map[string][]kmsg.OffsetForLeaderEpochResponseTopicPartition)

		for _, topic := range t.Topics {
			topicPartitions := topics[topic.Topic].load()
			for _, partition := range topic.Partitions {
				topicPartition, exists := topicPartitions.all[partition.Partition]
				if !exists {
					respParts[topic.Topic] = append(respParts[topic.Topic], kmsg.OffsetForLeaderEpochResponseTopicPartition{
						Partition: partition.Partition,
						ErrorCode: kerr.UnknownTopicOrPartition.Code,
					})
					continue
				}

				broker := brokers[topicPartition.leader]
				if topicPartition.loadErr != nil || broker == nil {
					errCode := kerr.UnknownServerError.Code
					if topicPartition.loadErr != nil {
						if ke, ok := topicPartition.loadErr.(*kerr.Error); ok {
							errCode = ke.Code
						}
					}
					respParts[topic.Topic] = append(respParts[topic.Topic], kmsg.OffsetForLeaderEpochResponseTopicPartition{
						Partition: partition.Partition,
						ErrorCode: errCode,
					})
					continue
				}

				brokerReqParts := reqParts[broker]
				if brokerReqParts == nil {
					brokerReqParts = make(map[string][]kmsg.OffsetForLeaderEpochRequestTopicPartition)
					reqParts[broker] = brokerReqParts
				}
				brokerReqParts[topic.Topic] = append(brokerReqParts[topic.Topic], partition)
			}
		}

		for broker, brokerReqParts := range reqParts {
			req := &kmsg.OffsetForLeaderEpochRequest{
				ReplicaID: t.ReplicaID,
			}
			for topic, parts := range brokerReqParts {
				req.Topics = append(req.Topics, kmsg.OffsetForLeaderEpochRequestTopic{
					Topic:      topic,
					Partitions: parts,
				})
			}
			broker2req[broker] = req
		}
		merge = func(newKResp kmsg.Response) {
			newResp := newKResp.(*kmsg.OffsetForLeaderEpochResponse)
			resp.Version = newResp.Version
			resp.ThrottleMillis = newResp.ThrottleMillis

			for _, topic := range newResp.Topics {
				respParts[topic.Topic] = append(respParts[topic.Topic], topic.Partitions...)
			}
		}

		finalize = func() {
			for topic, parts := range respParts {
				resp.Topics = append(resp.Topics, kmsg.OffsetForLeaderEpochResponseTopic{
					Topic:      topic,
					Partitions: parts,
				})
			}
		}

	case *kmsg.AlterReplicaLogDirsRequest: // similar to above, except types
		resp := new(kmsg.AlterReplicaLogDirsResponse)
		kresp = resp

		reqParts := make(map[*broker]map[string]map[string][]int32) // broker => dir => topic => partitions
		respParts := make(map[string][]kmsg.AlterReplicaLogDirsResponseTopicPartition)

		for _, dir := range t.Dirs {
			for _, topic := range dir.Topics {
				topicPartitions := topics[topic.Topic].load()
				for _, partition := range topic.Partitions {
					topicPartition, exists := topicPartitions.all[partition]
					if !exists {
						respParts[topic.Topic] = append(respParts[topic.Topic], kmsg.AlterReplicaLogDirsResponseTopicPartition{
							Partition: partition,
							ErrorCode: kerr.UnknownTopicOrPartition.Code,
						})
						continue
					}

					broker := brokers[topicPartition.leader]
					if topicPartition.loadErr != nil || broker == nil {
						errCode := kerr.UnknownServerError.Code
						if topicPartition.loadErr != nil {
							if ke, ok := topicPartition.loadErr.(*kerr.Error); ok {
								errCode = ke.Code
							}
						}
						respParts[topic.Topic] = append(respParts[topic.Topic], kmsg.AlterReplicaLogDirsResponseTopicPartition{
							Partition: partition,
							ErrorCode: errCode,
						})
						continue
					}

					brokerReqParts := reqParts[broker]
					if brokerReqParts == nil {
						brokerReqParts = make(map[string]map[string][]int32)
						reqParts[broker] = brokerReqParts
					}
					brokerDirReqParts := brokerReqParts[dir.Dir]
					if brokerDirReqParts == nil {
						brokerDirReqParts = make(map[string][]int32)
						brokerReqParts[dir.Dir] = brokerDirReqParts
					}
					brokerDirReqParts[topic.Topic] = append(brokerDirReqParts[topic.Topic], partition)
				}
			}
		}

		for broker, brokerReqParts := range reqParts {
			req := new(kmsg.AlterReplicaLogDirsRequest)
			for dir, topics := range brokerReqParts {
				dirReq := kmsg.AlterReplicaLogDirsRequestDir{
					Dir: dir,
				}
				for topic, parts := range topics {
					dirReq.Topics = append(dirReq.Topics, kmsg.AlterReplicaLogDirsRequestDirTopic{
						Topic:      topic,
						Partitions: parts,
					})
				}
				req.Dirs = append(req.Dirs, dirReq)
			}
			broker2req[broker] = req
		}
		merge = func(newKResp kmsg.Response) {
			newResp := newKResp.(*kmsg.AlterReplicaLogDirsResponse)
			resp.Version = newResp.Version
			resp.ThrottleMillis = newResp.ThrottleMillis

			for _, topic := range newResp.Topics {
				respParts[topic.Topic] = append(respParts[topic.Topic], topic.Partitions...)
			}
		}

		finalize = func() {
			for topic, parts := range respParts {
				resp.Topics = append(resp.Topics, kmsg.AlterReplicaLogDirsResponseTopic{
					Topic:      topic,
					Partitions: parts,
				})
			}
		}

	// This case is similar to above, but because the resp is per _dir_, we
	// do not make up a fake directory when we cannot load the topic.
	// Types are a bit weirder here too.
	case *kmsg.DescribeLogDirsRequest:
		resp := new(kmsg.DescribeLogDirsResponse)
		kresp = resp

		reqParts := make(map[*broker]map[string][]int32) // broker => topic => partitions

		type respDir struct {
			errCode int16
			topics  map[string][]kmsg.DescribeLogDirsResponseDirTopicPartition // topic => partitions
		}
		respParts := make(map[string]respDir)

		for _, topic := range t.Topics {
			topicPartitions := topics[topic.Topic].load()
			for _, partition := range topic.Partitions {
				topicPartition, exists := topicPartitions.all[partition]
				if !exists {
					continue
				}

				broker := brokers[topicPartition.leader]
				if topicPartition.loadErr != nil || broker == nil {
					continue
				}

				brokerReqParts := reqParts[broker]
				if brokerReqParts == nil {
					brokerReqParts = make(map[string][]int32)
					reqParts[broker] = brokerReqParts
				}
				brokerReqParts[topic.Topic] = append(brokerReqParts[topic.Topic], partition)
			}
		}

		for broker, brokerReqParts := range reqParts {
			req := new(kmsg.DescribeLogDirsRequest)
			for topic, parts := range brokerReqParts {
				req.Topics = append(req.Topics, kmsg.DescribeLogDirsRequestTopic{
					Topic:      topic,
					Partitions: parts,
				})
			}
			broker2req[broker] = req
		}

		// If the request has nil topics, that means describe all.
		// We need to fan that out.
		if t.Topics == nil {
			for _, broker := range brokers {
				if broker.meta.NodeID < 0 { // do not use seed brokers
					continue
				}
				broker2req[broker] = new(kmsg.DescribeLogDirsRequest)
			}
		}

		merge = func(newKResp kmsg.Response) {
			newResp := newKResp.(*kmsg.DescribeLogDirsResponse)
			resp.Version = newResp.Version
			resp.ThrottleMillis = newResp.ThrottleMillis

			for _, dir := range newResp.Dirs {
				existing := respParts[dir.Dir]
				if existing.topics == nil {
					existing.topics = make(map[string][]kmsg.DescribeLogDirsResponseDirTopicPartition)
				}
				if existing.errCode == 0 {
					existing.errCode = dir.ErrorCode
				}
				respParts[dir.Dir] = existing
				for _, topic := range dir.Topics {
					existing.topics[topic.Topic] = append(existing.topics[topic.Topic], topic.Partitions...)
				}
			}
		}

		finalize = func() {
			for dir, inner := range respParts {
				dirResp := kmsg.DescribeLogDirsResponseDir{
					ErrorCode: inner.errCode,
					Dir:       dir,
				}
				for topic, parts := range inner.topics {
					dirResp.Topics = append(dirResp.Topics, kmsg.DescribeLogDirsResponseDirTopic{
						Topic:      topic,
						Partitions: parts,
					})
				}
				resp.Dirs = append(resp.Dirs, dirResp)
			}
		}
	}

	cl.brokersMu.RUnlock()

	// Now with everything setup, we concurrently request all brokers,
	// merge the responses, and finalize!
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

			resp, err := myBroker.waitResp(ctx, myReq)

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
		return kresp, firstErr
	}

	finalize()

	return kresp, nil
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
