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
	"errors"
	"fmt"
	"hash/crc32"
	"math/rand"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

var crc32c = crc32.MakeTable(crc32.Castagnoli) // record crc's use Castagnoli table; for consuming/producing

// Client issues requests and handles responses to a Kafka cluster.
type Client struct {
	cfg cfg

	ctx       context.Context
	ctxCancel func()

	rng *rand.Rand

	brokersMu    sync.RWMutex
	brokers      map[int32]*broker // broker id => broker
	anyBrokerIdx int32
	anySeedIdx   int32
	stopBrokers  bool // set to true on close to stop updateBrokers

	// A sink and a source is created once per node ID and persists
	// forever. We expect the list to be small.
	//
	// The mutex only exists to allow consumer session stopping to read
	// sources to notify when starting a session; all writes happen in the
	// metadata loop.
	sinksAndSourcesMu sync.Mutex
	sinksAndSources   map[int32]sinkAndSource

	reqFormatter  *kmsg.RequestFormatter
	connTimeoutFn func(kmsg.Request) (time.Duration, time.Duration)

	bufPool bufPool // for to brokers to share underlying reusable request buffers
	pnrPool pnrPool // for sinks to reuse []promisedNumberedRecord

	controllerIDMu sync.Mutex
	controllerID   int32

	// The following two ensure that we only have one fetchBrokerMetadata
	// at once. This avoids unnecessary broker metadata requests and
	// metadata trampling.
	fetchingBrokersMu sync.Mutex
	fetchingBrokers   *struct {
		done chan struct{}
		err  error
	}

	producer producer
	consumer consumer

	compressor   *compressor
	decompressor *decompressor

	coordinatorsMu sync.Mutex
	coordinators   map[coordinatorKey]*coordinatorLoad

	updateMetadataCh     chan struct{}
	updateMetadataNowCh  chan struct{} // like above, but with high priority
	blockingMetadataFnCh chan func()
	metawait             metawait
	metadone             chan struct{}
}

func (cl *Client) idempotent() bool { return !cl.cfg.disableIdempotency }

type sinkAndSource struct {
	sink   *sink
	source *source
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

		sinksAndSources: make(map[int32]sinkAndSource),

		reqFormatter:  new(kmsg.RequestFormatter),
		connTimeoutFn: connTimeoutBuilder(cfg.connTimeoutOverhead),

		bufPool: newBufPool(),
		pnrPool: newPnrPool(),

		decompressor: newDecompressor(),

		coordinators: make(map[coordinatorKey]*coordinatorLoad),

		updateMetadataCh:     make(chan struct{}, 1),
		updateMetadataNowCh:  make(chan struct{}, 1),
		blockingMetadataFnCh: make(chan func()),
		metadone:             make(chan struct{}),
	}
	cl.producer.init()
	cl.consumer.init(cl)
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
	}
	go cl.updateMetadataLoop()
	go cl.reapConnectionsLoop()

	return cl, nil
}

func connTimeoutBuilder(def time.Duration) func(kmsg.Request) (time.Duration, time.Duration) {
	var joinMu sync.Mutex
	var lastRebalanceTimeout time.Duration

	return func(req kmsg.Request) (read, write time.Duration) {
		millis := func(m int32) time.Duration { return time.Duration(m) * time.Millisecond }
		switch t := req.(type) {
		default:
			if timeoutRequest, ok := req.(kmsg.TimeoutRequest); ok {
				timeoutMillis := timeoutRequest.Timeout()
				return def + millis(timeoutMillis), def
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
	cl.brokersMu.Lock() // full lock needed for anyBrokerIdx below
	defer cl.brokersMu.Unlock()

	b, exists := cl.brokers[cl.anyBrokerIdx]
	if !exists && cl.anyBrokerIdx != 0 {
		cl.anyBrokerIdx = 0
		b, exists = cl.brokers[cl.anyBrokerIdx]
	}
	cl.anyBrokerIdx++

	// Maybe we have not loaded brokers yet--fallback to seeds.
	if !exists {
		b, exists = cl.brokers[unknownSeedID(int(cl.anySeedIdx))]
		if !exists {
			cl.anySeedIdx = 0 // seed 0 **must** exists.
			b = cl.brokers[unknownSeedID(int(cl.anySeedIdx))]
		}
		cl.anySeedIdx++
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
	cl.fetchingBrokersMu.Lock()
	wait := cl.fetchingBrokers
	if wait != nil {
		cl.fetchingBrokersMu.Unlock()
		<-wait.done
		return wait.err
	}
	wait = &struct {
		done chan struct{}
		err  error
	}{done: make(chan struct{})}
	cl.fetchingBrokers = wait
	cl.fetchingBrokersMu.Unlock()

	defer func() {
		cl.fetchingBrokersMu.Lock()
		defer cl.fetchingBrokersMu.Unlock()
		cl.fetchingBrokers = nil
		close(wait.done)
	}()

	_, _, wait.err = cl.fetchMetadata(ctx, kmsg.NewPtrMetadataRequest(), true)
	return wait.err
}

func (cl *Client) fetchMetadataForTopics(ctx context.Context, all bool, topics []string) (*broker, *kmsg.MetadataResponse, error) {
	req := &kmsg.MetadataRequest{
		AllowAutoTopicCreation: cl.cfg.allowAutoTopicCreation,
	}
	if all {
		req.Topics = nil
	} else if len(topics) == 0 {
		req.Topics = []kmsg.MetadataRequestTopic{}
	} else {
		for _, topic := range topics {
			t := topic
			req.Topics = append(req.Topics, kmsg.MetadataRequestTopic{Topic: &t})
		}
	}
	return cl.fetchMetadata(ctx, req, true)
}

func (cl *Client) fetchMetadata(ctx context.Context, req *kmsg.MetadataRequest, limitRetries bool) (*broker, *kmsg.MetadataResponse, error) {
	r := cl.retriable()

	// We limit retries for internal metadata refreshes, because these do
	// not need to retry forever and are usually blocking *other* requests.
	// e.g., producing bumps load errors when metadata returns, so 3
	// failures here will correspond to 1 bumped error count. To make the
	// number more accurate, we should *never* retry here, but this is
	// pretty intolerant of immediately-temporary network issues. Rather,
	// we use a small count of 3 retries, which with the default backoff,
	// will be <500ms of retrying. This is still intolerant of temporary
	// failures, but it does allow recovery from a dns issue / bad path.
	if limitRetries {
		r.limitRetries = 3
	}

	meta, err := req.RequestWith(ctx, r)
	if err == nil {
		if meta.ControllerID >= 0 {
			cl.controllerIDMu.Lock()
			cl.controllerID = meta.ControllerID
			cl.controllerIDMu.Unlock()
		}
		cl.updateBrokers(meta.Brokers)
	}
	return r.last, meta, err
}

// updateBrokers is called with the broker portion of every metadata response.
// All metadata responses contain all known live brokers, so we can always
// use the response.
func (cl *Client) updateBrokers(brokers []kmsg.MetadataResponseBroker) {
	newBrokers := make(map[int32]*broker, len(brokers))

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
	}

	for goneID, goneBroker := range cl.brokers {
		if goneID < -1 { // seed broker, unknown ID, always keep
			newBrokers[goneID] = goneBroker
		} else {
			goneBroker.stopForever()
		}
	}

	cl.brokers = newBrokers
}

// Close leaves any group and closes all connections and goroutines.
//
// If you are group consuming and have overridden the default OnRevoked, you
// must manually commit offsets before closing the client.
func (cl *Client) Close() {
	// First, kill the consumer. This waits for the consumer to unset
	// gracefully, ensuring we leave groups properly, and then stores the
	// dead consumer, meaning no more assigns can happen.
	if wasDead := cl.consumer.kill(); wasDead {
		return // client was already closed
	}

	// Now we kill the client context and all brokers, ensuring all
	// requests fail. This will finish all producer callbacks and
	// stop the metadata loop.
	cl.ctxCancel()
	cl.brokersMu.Lock()
	cl.stopBrokers = true
	for _, broker := range cl.brokers {
		broker.stopForever()
	}
	cl.brokersMu.Unlock()

	// Wait for metadata to quit so we know no more erroring topic
	// partitions will be created. After metadata has quit, we can
	// safely stop sinks and sources, as no more will be made.
	<-cl.metadone

	for _, sns := range cl.sinksAndSources {
		sns.sink.maybeDrain()     // awaken anything in backoff
		sns.source.maybeConsume() // same
	}

	cl.failBufferedRecords(errClientClosing)
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
// For transaction requests, the request is issued to the transaction
// coordinator. However, if the request is an init producer ID request and the
// request has no transactional ID, the request goes to any broker.
//
// Some requests need to be split and sent to many brokers. For these requests,
// it is *highly* recommended to use RequestSharded. Not all responses from
// many brokers can be cleanly merged. However, for the requests that are
// split, this does attempt to merge them in a sane way.
//
// The following requests are split:
//
//     ListOffsets
//     DescribeGroups
//     ListGroups
//     DeleteRecords
//     OffsetForLeaderEpoch
//     DescribeConfigs
//     AlterConfigs
//     AlterReplicaLogDirs
//     DescribeLogDirs
//     DeleteGroups
//     IncrementalAlterConfigs
//     DescribeProducers
//     DescribeTransactions
//     ListTransactions
//
// In short, this method tries to do the correct thing depending on what type
// of request is being issued.
//
// The passed context can be used to cancel a request and return early. Note
// that if the request was written to Kafka but the context canceled before a
// response is received, Kafka may still operate on the received request.
//
// If using this function to issue kmsg.ProduceRequest's, you must configure
// the client with the same RequiredAcks option that you use in the request.
// If you are issuing produce requests with 0 acks, you must configure the
// client with the same timeout you use in the request. The client will
// internally rewrite the incoming request's acks to match the client's
// configuration, and it will rewrite the timeout millis if the acks is 0. It
// is strongly recommended to not issue raw kmsg.ProduceRequest's.
func (cl *Client) Request(ctx context.Context, req kmsg.Request) (kmsg.Response, error) {
	resps, merge := cl.shardedRequest(ctx, req)
	// If there is no merge function, only one request was issued directly
	// to a broker. Return the resp and err directly.
	if merge == nil {
		return resps[0].Resp, resps[0].Err
	}
	return merge(resps)
}

func (cl *Client) retriable() *retriable {
	return cl.retriableBrokerFn(func() (*broker, error) { return cl.broker(), nil })
}

func (cl *Client) retriableBrokerFn(fn func() (*broker, error)) *retriable {
	return &retriable{cl: cl, br: fn}
}

func (cl *Client) shouldRetry(tries int, err error) bool {
	switch err.(type) {
	case *errDeadConn:
		return tries < cl.cfg.brokerConnDeadRetries
	default:
		return (kerr.IsRetriable(err) || isRetriableBrokerErr(err)) && int64(tries) < cl.cfg.retries
	}
}

type retriable struct {
	cl   *Client
	br   func() (*broker, error)
	last *broker

	// If non-zero, limitRetries may specify a smaller # of retries than
	// the client RequestRetries number. This is used for internal requests
	// that can fail / do not need to retry forever.
	limitRetries int

	// parseRetryErr, if non-nil, can parse a retriable error out of the
	// response and return it. This error is *not* returned from the
	// request if the req cannot be retried due to timeout or retry limits,
	// but it *can* allow a retry if neither limit is hit yet.
	parseRetryErr func(kmsg.Response) error
}

func (r *retriable) Request(ctx context.Context, req kmsg.Request) (kmsg.Response, error) {
	tries := 0
	tryStart := time.Now()
	retryTimeout := r.cl.cfg.retryTimeout(req.Key())
start:
	tries++
	br, err := r.br()
	r.last = br
	var resp kmsg.Response
	var retryErr error
	if err == nil {
		resp, err = r.last.waitResp(ctx, req)
		if err == nil && r.parseRetryErr != nil {
			retryErr = r.parseRetryErr(resp)
		}
	}
	if err != nil || retryErr != nil {
		if retryTimeout == 0 || time.Since(tryStart) <= retryTimeout {
			if (r.cl.shouldRetry(tries, err) || r.cl.shouldRetry(tries, retryErr)) &&
				(r.limitRetries == 0 || tries < r.limitRetries) &&
				r.cl.waitTries(ctx, tries) {

				goto start
			}
		}
	}
	return resp, err
}

// ResponseShard ties together a request with either the response it received
// or an error that prevented a response from being received.
type ResponseShard struct {
	// Meta contains the broker that this request was issued to, or an
	// unknown (node ID -1) metadata if the request could not be issued.
	//
	// Requests can fail to even be issued if an appropriate broker cannot
	// be loaded of if the client cannot understand the request.
	Meta BrokerMetadata

	// Req is the request that was issued to this broker.
	Req kmsg.Request

	// Resp is the response received from the broker, if any.
	Resp kmsg.Response

	// Err, if non-nil, is the error that prevented a response from being
	// received or the request from being issued.
	Err error
}

// RequestSharded performs the same logic as Request, but returns all responses
// from any broker that the request was split to. This always returns at least
// one shard. If the request does not need to be issued (describing no groups),
// this issues the request to a random broker just to ensure that one shard
// exists.
//
// There are only a few requests that are strongly recommended to explicitly
// use RequestSharded; the rest can by default use Request. These few requests
// are mentioned in the documentation for Request.
//
// If, in the process of splitting a request, some topics or partitions are
// found to not exist, or Kafka replies that a request should go to a broker
// that does not exist, all those non-existent pieces are grouped into one
// request to the first seed broker. This will show up as a seed broker node ID
// (min int32) and the response will likely contain purely errors.
//
// The response shards are ordered by broker metadata.
func (cl *Client) RequestSharded(ctx context.Context, req kmsg.Request) []ResponseShard {
	resps, _ := cl.shardedRequest(ctx, req)
	sort.Slice(resps, func(i, j int) bool {
		l := &resps[i].Meta
		r := &resps[j].Meta

		if l.NodeID < r.NodeID {
			return true
		}
		if r.NodeID < l.NodeID {
			return false
		}
		if l.Host < r.Host {
			return true
		}
		if r.Host < l.Host {
			return false
		}
		if l.Port < r.Port {
			return true
		}
		if r.Port < l.Port {
			return false
		}
		if l.Rack == nil {
			return true
		}
		if r.Rack == nil {
			return false
		}
		return *l.Rack < *r.Rack
	})
	return resps
}

type shardMerge func([]ResponseShard) (kmsg.Response, error)

func (cl *Client) shardedRequest(ctx context.Context, req kmsg.Request) ([]ResponseShard, shardMerge) {
	ctx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	defer close(done)
	go func() {
		defer cancel()
		select {
		case <-done:
		case <-ctx.Done():
		case <-cl.ctx.Done():
		}
	}()

	// First, handle any sharded request. This comes before the conditional
	// below because this handles two group requests, which we do not want
	// to fall into the handleCoordinatorReq logic.
	switch req.(type) {
	case *kmsg.ListOffsetsRequest, // key 2
		*kmsg.DescribeGroupsRequest,          // key 15
		*kmsg.ListGroupsRequest,              // key 16
		*kmsg.DeleteRecordsRequest,           // key 21
		*kmsg.OffsetForLeaderEpochRequest,    // key 23
		*kmsg.DescribeConfigsRequest,         // key 32
		*kmsg.AlterConfigsRequest,            // key 33
		*kmsg.AlterReplicaLogDirsRequest,     // key 34
		*kmsg.DescribeLogDirsRequest,         // key 35
		*kmsg.DeleteGroupsRequest,            // key 42
		*kmsg.IncrementalAlterConfigsRequest, // key 44
		*kmsg.DescribeProducersRequest,       // key 61
		*kmsg.DescribeTransactionsRequest,    // key 65
		*kmsg.ListTransactionsRequest:        // key 66
		return cl.handleShardedReq(ctx, req)
	}

	if metaReq, isMetaReq := req.(*kmsg.MetadataRequest); isMetaReq {
		// We hijack any metadata request so as to populate our
		// own brokers and controller ID.
		br, resp, err := cl.fetchMetadata(ctx, metaReq, false)
		return shards(shard(br, req, resp, err)), nil

	} else if adminReq, admin := req.(kmsg.AdminRequest); admin {
		return shards(cl.handleAdminReq(ctx, adminReq)), nil

	} else if groupReq, isGroupReq := req.(kmsg.GroupCoordinatorRequest); isGroupReq {
		return shards(cl.handleCoordinatorReq(ctx, groupReq, coordinatorTypeGroup)), nil

	} else if txnReq, isTxnReq := req.(kmsg.TxnCoordinatorRequest); isTxnReq {
		return shards(cl.handleCoordinatorReq(ctx, txnReq, coordinatorTypeTxn)), nil

	} else if apiVersReq, isApiVersReq := req.(*kmsg.ApiVersionsRequest); isApiVersReq {
		// As of v3, software name and version are required.
		// If they are missing, we use the config options.
		if apiVersReq.ClientSoftwareName == "" && apiVersReq.ClientSoftwareVersion == "" {
			dup := *apiVersReq
			dup.ClientSoftwareName = cl.cfg.softwareName
			dup.ClientSoftwareVersion = cl.cfg.softwareVersion
			req = &dup
		}
	}

	// All other requests not handled above can be issued to any broker
	// with the default retriable logic.
	r := cl.retriable()
	resp, err := r.Request(ctx, req)
	return shards(shard(r.last, req, resp, err)), nil
}

func shard(br *broker, req kmsg.Request, resp kmsg.Response, err error) ResponseShard {
	if br == nil { // the broker could be nil if loading the broker failed.
		return ResponseShard{unknownMetadata, req, resp, err}
	}
	return ResponseShard{br.meta, req, resp, err}
}

func shards(shard ...ResponseShard) []ResponseShard {
	return shard
}

// brokerOrErr returns the broker for ID or the error if the broker does not
// exist.
//
// If tryLoad is true and the broker does not exist, this attempts a broker
// metadata load once before failing. If the metadata load fails, this returns
// that error.
func (cl *Client) brokerOrErr(ctx context.Context, id int32, err error) (*broker, error) {
	tryLoad := ctx != nil
	tries := 0
start:
	cl.brokersMu.RLock()
	broker := cl.brokers[id]
	cl.brokersMu.RUnlock()

	if broker == nil {
		if tryLoad {
			if loadErr := cl.fetchBrokerMetadata(ctx); loadErr != nil {
				return nil, loadErr
			}
			// We will retry loading up to two times, if we load broker
			// metadata twice successfully but neither load has the broker
			// we are looking for, then we say our broker does not exist.
			tries++
			if tries < 2 {
				goto start
			}
		}
		return nil, err
	}
	return broker, nil
}

// controller returns the controller broker, forcing a broker load if
// necessary.
func (cl *Client) controller(ctx context.Context) (*broker, error) {
	get := func() int32 {
		cl.controllerIDMu.Lock()
		defer cl.controllerIDMu.Unlock()
		return cl.controllerID
	}

	var id int32
	if id = get(); id < 0 {
		if err := cl.fetchBrokerMetadata(ctx); err != nil {
			return nil, err
		}
		if id = get(); id < 0 {
			return nil, &errUnknownController{id}
		}
	}

	return cl.brokerOrErr(nil, id, &errUnknownController{id})
}

// forgetControllerID is called once an admin requests sees NOT_CONTROLLER.
func (cl *Client) forgetControllerID(id int32) {
	cl.controllerIDMu.Lock()
	defer cl.controllerIDMu.Unlock()

	if cl.controllerID == id {
		cl.controllerID = unknownControllerID
	}
}

const (
	coordinatorTypeGroup int8 = 0
	coordinatorTypeTxn   int8 = 1
)

type coordinatorKey struct {
	name string
	typ  int8
}

type coordinatorLoad struct {
	done chan struct{}
	node int32
	err  error
}

// loadController returns the group/txn coordinator for the given key, retrying
// as necessary. If reload is true, this does not used a cache coordinator.
func (cl *Client) loadCoordinator(reload bool, ctx context.Context, key coordinatorKey) (*broker, error) {
	cl.coordinatorsMu.Lock()
	c, ok := cl.coordinators[key]
	if reload || !ok {
		c = &coordinatorLoad{
			done: make(chan struct{}), // all requests for the same coordinator get collapsed into one
		}
		defer func() {
			// If our load fails, we avoid caching the coordinator,
			// but only if something else has not already replaced
			// our pointer. We could be overwritten by a function
			// setting reload to true.
			if c.err != nil {
				cl.coordinatorsMu.Lock()
				if existing, ok := cl.coordinators[key]; ok && c == existing {
					delete(cl.coordinators, key)
				}
				cl.coordinatorsMu.Unlock()
			}
			close(c.done)
		}()
		cl.coordinators[key] = c
	}
	cl.coordinatorsMu.Unlock()

	if !reload && ok {
		<-c.done
		if c.err != nil {
			return nil, c.err
		}
		return cl.brokerOrErr(nil, c.node, &errUnknownCoordinator{c.node, key})
	}

	var resp *kmsg.FindCoordinatorResponse
	resp, c.err = (&kmsg.FindCoordinatorRequest{
		CoordinatorKey:  key.name,
		CoordinatorType: key.typ,
	}).RequestWith(ctx, cl.retriable())

	if c.err != nil {
		return nil, c.err
	}

	if c.err = kerr.ErrorForCode(resp.ErrorCode); c.err != nil {
		return nil, c.err
	}

	c.node = resp.NodeID
	var b *broker
	b, c.err = cl.brokerOrErr(ctx, c.node, &errUnknownCoordinator{c.node, key})
	return b, c.err
}

func (cl *Client) maybeDeleteStaleCoordinator(name string, typ int8, err error) bool {
	switch err {
	case kerr.CoordinatorNotAvailable,
		kerr.CoordinatorLoadInProgress,
		kerr.NotCoordinator:

		cl.coordinatorsMu.Lock()
		delete(cl.coordinators, coordinatorKey{
			name: name,
			typ:  typ,
		})
		cl.coordinatorsMu.Unlock()
		return true
	}
	return false
}

// loadCoordinators does a concurrent load of many coordinators.
func (cl *Client) loadCoordinators(reload bool, typ int8, names ...string) (map[string]*broker, error) {
	ctx, cancel := context.WithCancel(cl.ctx)
	defer cancel()

	var mu sync.Mutex
	m := make(map[string]*broker)
	var firstErr error

	var wg sync.WaitGroup
	for _, name := range names {
		myName := name
		wg.Add(1)
		go func() {
			defer wg.Done()
			coordinator, err := cl.loadCoordinator(reload, ctx, coordinatorKey{
				name: myName,
				typ:  typ,
			})

			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				if firstErr == nil {
					firstErr = err
					cancel()
				}
				return
			}
			m[myName] = coordinator
		}()
	}
	wg.Wait()

	return m, firstErr
}

func (cl *Client) handleAdminReq(ctx context.Context, req kmsg.Request) ResponseShard {
	// Loading a controller can perform some wait; we accept that and do
	// not account for the retries or the time to load the controller as
	// part of the retries / time to issue the req.
	r := cl.retriableBrokerFn(func() (*broker, error) {
		return cl.controller(ctx)
	})

	r.parseRetryErr = func(resp kmsg.Response) error {
		var code int16
		switch t := resp.(type) {
		case *kmsg.CreateTopicsResponse:
			if len(t.Topics) > 0 {
				code = t.Topics[0].ErrorCode
			}
		case *kmsg.DeleteTopicsResponse:
			if len(t.Topics) > 0 {
				code = t.Topics[0].ErrorCode
			}
		case *kmsg.CreatePartitionsResponse:
			if len(t.Topics) > 0 {
				code = t.Topics[0].ErrorCode
			}
		case *kmsg.ElectLeadersResponse:
			if len(t.Topics) > 0 && len(t.Topics[0].Partitions) > 0 {
				code = t.Topics[0].Partitions[0].ErrorCode
			}
		case *kmsg.AlterPartitionAssignmentsResponse:
			code = t.ErrorCode
		case *kmsg.ListPartitionReassignmentsResponse:
			code = t.ErrorCode
		case *kmsg.AlterUserSCRAMCredentialsResponse:
			if len(t.Results) > 0 {
				code = t.Results[0].ErrorCode
			}
		case *kmsg.VoteResponse:
			code = t.ErrorCode
		case *kmsg.BeginQuorumEpochResponse:
			code = t.ErrorCode
		case *kmsg.EndQuorumEpochResponse:
			code = t.ErrorCode
		case *kmsg.DescribeQuorumResponse:
			code = t.ErrorCode
		case *kmsg.AlterISRResponse:
			code = t.ErrorCode
		case *kmsg.UpdateFeaturesResponse:
			code = t.ErrorCode
		case *kmsg.EnvelopeResponse:
			code = t.ErrorCode
		}
		if err := kerr.ErrorForCode(code); err == kerr.NotController {
			// There must be a last broker if we were able to issue
			// the request and get a response.
			cl.forgetControllerID(r.last.meta.NodeID)
			return err
		}
		return nil
	}

	resp, err := r.Request(ctx, req)
	return shard(r.last, req, resp, err)
}

// handleCoordinatorReq issues simple (non-shardable) group or txn requests.
func (cl *Client) handleCoordinatorReq(ctx context.Context, req kmsg.Request, typ int8) ResponseShard {
	switch t := req.(type) {
	default:
		// All group requests should be listed below, so if it isn't,
		// then we do not know what this request is.
		return shard(nil, req, nil, errors.New("client is too old; this client does not know what to do with this request"))

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
		coordinator, resp, err := cl.handleReqWithCoordinator(ctx, func() (*broker, error) { return cl.broker(), nil }, coordinatorTypeTxn, "", req)
		return shard(coordinator, req, resp, err)
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
	case *kmsg.OffsetDeleteRequest:
		return cl.handleCoordinatorReqSimple(ctx, coordinatorTypeGroup, t.Group, req)
	}
}

// handleCoordinatorReqSimple issues a request that contains a single group or
// txn to its coordinator.
//
// The error is inspected to see if it is a retriable error and, if so, the
// coordinator is deleted.
func (cl *Client) handleCoordinatorReqSimple(ctx context.Context, typ int8, name string, req kmsg.Request) ResponseShard {
	coordinator, resp, err := cl.handleReqWithCoordinator(ctx, func() (*broker, error) {
		return cl.loadCoordinator(false, ctx, coordinatorKey{
			name: name,
			typ:  typ,
		})
	}, typ, name, req)
	return shard(coordinator, req, resp, err)
}

// handleReqWithCoordinator actually issues a request to a coordinator and
// does retry handling.
//
// This avoids retries on the two group requests that need to be sharded.
func (cl *Client) handleReqWithCoordinator(
	ctx context.Context,
	coordinator func() (*broker, error),
	typ int8,
	name string, // group ID or the transactional id
	req kmsg.Request,
) (*broker, kmsg.Response, error) {

	r := cl.retriableBrokerFn(coordinator)
	r.parseRetryErr = func(resp kmsg.Response) error {
		var code int16
		switch t := resp.(type) {

		// TXN
		case *kmsg.InitProducerIDResponse:
			code = t.ErrorCode
		case *kmsg.AddPartitionsToTxnResponse:
			if len(t.Topics) > 0 && len(t.Topics[0].Partitions) > 0 {
				code = t.Topics[0].Partitions[0].ErrorCode
			}
		case *kmsg.AddOffsetsToTxnResponse:
			code = t.ErrorCode
		case *kmsg.EndTxnResponse:
			code = t.ErrorCode

		// GROUP
		case *kmsg.OffsetCommitResponse:
			if len(t.Topics) > 0 && len(t.Topics[0].Partitions) > 0 {
				code = t.Topics[0].Partitions[0].ErrorCode
			}
		case *kmsg.TxnOffsetCommitResponse:
			if len(t.Topics) > 0 && len(t.Topics[0].Partitions) > 0 {
				code = t.Topics[0].Partitions[0].ErrorCode
			}
		case *kmsg.OffsetFetchResponse:
			if t.Version >= 2 {
				code = t.ErrorCode
			} else if len(t.Topics) > 0 && len(t.Topics[0].Partitions) > 0 {
				code = t.Topics[0].Partitions[0].ErrorCode
			}
		case *kmsg.JoinGroupResponse:
			code = t.ErrorCode
		case *kmsg.HeartbeatResponse:
			code = t.ErrorCode
		case *kmsg.LeaveGroupResponse:
			code = t.ErrorCode
		case *kmsg.SyncGroupResponse:
			code = t.ErrorCode

		}
		// Describe and Delete handled in sharding.

		if err := kerr.ErrorForCode(code); cl.maybeDeleteStaleCoordinator(name, typ, err) {
			return err
		}
		return nil
	}

	resp, err := r.Request(ctx, req)
	return r.last, resp, err
}

// Broker returns a handle to a specific broker to directly issue requests to.
// Note that there is no guarantee that this broker exists; if it does not,
// requests will fail with with an unknown broker error.
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
	cl.brokersMu.RLock()
	defer cl.brokersMu.RUnlock()

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
	cl.brokersMu.RLock()
	defer cl.brokersMu.RUnlock()

	var bs []*Broker
	for i := 0; ; i++ {
		id := unknownSeedID(i)
		if _, exists := cl.brokers[id]; !exists {
			return bs
		}
		bs = append(bs, &Broker{id: id, cl: cl})
	}
}

// Broker pairs a broker ID with a client to directly issue requests to a
// specific broker.
type Broker struct {
	id int32
	cl *Client
}

// Request issues a request to a broker. If the broker does not exist in the
// client, this returns an unknown broker error. Requests are not retried.
//
// The passed context can be used to cancel a request and return early.
// Note that if the request is not canceled before it is written to Kafka,
// you may just end up canceling and not receiving the response to what Kafka
// inevitably does.
//
// It is more beneficial to always use RetriableRequest.
func (b *Broker) Request(ctx context.Context, req kmsg.Request) (kmsg.Response, error) {
	return b.request(false, ctx, req)
}

// RetriableRequest issues a request to a broker the same as Broker, but
// retries in the face of retriable broker connection errors. This does not
// retry on response internal errors.
func (b *Broker) RetriableRequest(ctx context.Context, req kmsg.Request) (kmsg.Response, error) {
	return b.request(true, ctx, req)
}

func (b *Broker) request(retry bool, ctx context.Context, req kmsg.Request) (kmsg.Response, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	var resp kmsg.Response
	var err error
	done := make(chan struct{})

	go func() {
		defer close(done)

		if !retry {
			var br *broker
			br, err = b.cl.brokerOrErr(ctx, b.id, errUnknownBroker)
			if err == nil {
				resp, err = br.waitResp(ctx, req)
			}
		} else {
			resp, err = b.cl.retriableBrokerFn(func() (*broker, error) {
				return b.cl.brokerOrErr(ctx, b.id, errUnknownBroker)
			}).Request(ctx, req)
		}
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

//////////////////////
// REQUEST SHARDING //
//////////////////////

// Below here lies all logic to handle requests that need to be split and sent
// to many brokers. A lot of the logic for each sharding function is very
// similar, but each sharding function uses slightly different types.

// issueShard is a request that has been split and is ready to be sent to the
// given broker ID.
type issueShard struct {
	req    kmsg.Request
	broker int32
	any    bool
}

// sharder splits a request.
type sharder interface {
	// shard splits a request and returns the requests to issue tied to the
	// brokers to issue the requests to. This can return an error if there
	// is some pre-loading that needs to happen. If an error is returned,
	// the request that was intended for splitting is failed wholesale.
	//
	// Due to sharded requests not being retriable if a response is
	// received, to avoid stale coordinator errors, this function should
	// not use any previously cached metadata.
	shard(context.Context, kmsg.Request) ([]issueShard, bool, error)

	// onResp is called on a successful response to investigate the
	// response and potentially perform cleanup.
	//
	// We cannot retry responses that have retriable errors inside of them;
	// doing so would require a very manual and tedious process:
	//   - pair all request partitions to the response partition (maybe the
	//     response is missing some pieces because of a buggy kafka)
	//   - split non-retriable pieces of the request & response:
	//     - any missing response pieces have a request piece that is not
	//       retriable
	//     - any matching piece can be retriable if the response piece err
	//       is retriable
	//   - return the non-retriable request & response piece, and the retriable
	//     request piece and err.
	//
	// Because the pairing is manual and tedious, and because the shard
	// function above loads fresh metadata, we expect to not fall into
	// stale metadata / coordinators before we issue the sharded requests.
	//
	// As well, for group describing or deleting, we force a load of the
	// coordinators on every shard request. Thus, we do not expect the
	// coordinators to be stale.
	onResp(kmsg.Response)

	// merge is a function that can be used to merge sharded responses into
	// one response. This is used by the client.Request method.
	merge([]ResponseShard) (kmsg.Response, error)
}

// handleShardedReq splits and issues requests to brokers, recursively
// splitting as necessary if requests fail and need remapping.
func (cl *Client) handleShardedReq(ctx context.Context, req kmsg.Request) ([]ResponseShard, shardMerge) {

	// First, determine our sharder.
	var sharder sharder
	switch req.(type) {
	case *kmsg.ListOffsetsRequest:
		sharder = &listOffsetsSharder{cl}
	case *kmsg.DescribeGroupsRequest:
		sharder = &describeGroupsSharder{cl}
	case *kmsg.ListGroupsRequest:
		sharder = &listGroupsSharder{cl}
	case *kmsg.DeleteRecordsRequest:
		sharder = &deleteRecordsSharder{cl}
	case *kmsg.OffsetForLeaderEpochRequest:
		sharder = &offsetForLeaderEpochSharder{cl}
	case *kmsg.DescribeConfigsRequest:
		sharder = &describeConfigsSharder{cl}
	case *kmsg.AlterConfigsRequest:
		sharder = &alterConfigsSharder{cl}
	case *kmsg.AlterReplicaLogDirsRequest:
		sharder = &alterReplicaLogDirsSharder{cl}
	case *kmsg.DescribeLogDirsRequest:
		sharder = &describeLogDirsSharder{cl}
	case *kmsg.DeleteGroupsRequest:
		sharder = &deleteGroupsSharder{cl}
	case *kmsg.IncrementalAlterConfigsRequest:
		sharder = &incrementalAlterConfigsSharder{cl}
	case *kmsg.DescribeProducersRequest:
		sharder = &describeProducersSharder{cl}
	case *kmsg.DescribeTransactionsRequest:
		sharder = &describeTransactionsSharder{cl}
	case *kmsg.ListTransactionsRequest:
		sharder = &listTransactionsSharder{cl}
	}

	// If a request fails, we re-shard it (in case it needs to be split
	// again). reqTry tracks how many total tries a request piece has had;
	// we quit at either the max configured tries or max configured time.
	type reqTry struct {
		tries int
		req   kmsg.Request
	}

	var (
		shardsMu sync.Mutex
		shards   []ResponseShard

		addShard = func(shard ResponseShard) {
			shardsMu.Lock()
			defer shardsMu.Unlock()
			shards = append(shards, shard)
		}

		start        = time.Now()
		retryTimeout = cl.cfg.retryTimeout(req.Key())

		wg    sync.WaitGroup
		issue func(reqTry)
	)

	l := cl.cfg.logger
	debug := l.Level() >= LogLevelDebug

	// issue is called to progressively split and issue requests.
	//
	// This recursively calls itself if a request fails and can be retried.
	issue = func(try reqTry) {
		issues, reshardable, err := sharder.shard(ctx, try.req)
		if err != nil {
			l.Log(LogLevelDebug, "unable to shard request", "previous_tries", try.tries, "err", err)
			addShard(shard(nil, try.req, nil, err)) // failure to shard means data loading failed; this request is failed
			return
		}

		// If the request actually does not need to be issued, we issue
		// it to a random broker. There is no benefit to this, but at
		// least we will return one shard.
		if len(issues) == 0 {
			issues = []issueShard{{
				req: try.req,
				any: true,
			}}
			reshardable = true
		}

		if debug {
			var brokerAnys []string
			for _, issue := range issues {
				if issue.any {
					brokerAnys = append(brokerAnys, "any")
				} else {
					brokerAnys = append(brokerAnys, fmt.Sprintf("%d", issue.broker))
				}
			}
			l.Log(LogLevelDebug, "sharded request", "destinations", brokerAnys)
		}

		for i := range issues {
			tries := try.tries
			myIssue := issues[i]
			wg.Add(1)
			go func() {
				defer wg.Done()
			start:
				tries++

				broker := cl.broker()
				var err error
				if !myIssue.any {
					broker, err = cl.brokerOrErr(ctx, myIssue.broker, errUnknownBroker)
				}
				if err != nil {
					addShard(shard(nil, myIssue.req, nil, err)) // failure to load a broker is a failure to issue a request
					return
				}

				resp, err := broker.waitResp(ctx, myIssue.req)
				if err == nil {
					// Successful responses may need to perform some
					// response internal error checking cleanup.
					// So, we call onResp, then keep the response.
					sharder.onResp(resp)
					addShard(shard(broker, myIssue.req, resp, nil))
					return
				}

				// If we failed to issue the request, we *maybe* will retry.
				// We could have failed to even issue the request or receive
				// a response, which is retriable.
				if err != nil && (retryTimeout == 0 || time.Since(start) < retryTimeout) && cl.shouldRetry(tries, err) && cl.waitTries(ctx, tries) {
					// Non-reshardable re-requests just jump back to the
					// top where the broker is loaded. This is the case on
					// requests where the original request is split to
					// dedicated brokers; we do not want to re-shard that.
					if !reshardable {
						l.Log(LogLevelDebug, "sharded request failed, reissuing without resharding", "time_since_start", time.Since(start), "tries", try.tries, "err", err)
						goto start
					}
					l.Log(LogLevelDebug, "sharded request failed, resharding and reissuing", "time_since_start", time.Since(start), "tries", try.tries, "err", err)
					issue(reqTry{tries, myIssue.req})
					return
				}

				addShard(shard(broker, myIssue.req, nil, err)) // the error was not retriable
			}()
		}
	}

	issue(reqTry{0, req})
	wg.Wait()

	return shards, sharder.merge
}

// a convenience function for when a request needs to be issued identically to
// all brokers.
func (cl *Client) allBrokersShardedReq(ctx context.Context, fn func() kmsg.Request) ([]issueShard, bool, error) {
	if err := cl.fetchBrokerMetadata(ctx); err != nil {
		return nil, false, err
	}

	var issues []issueShard
	cl.brokersMu.RLock()
	for _, broker := range cl.brokers {
		if broker.meta.NodeID < 0 {
			continue // we skip seed brokers
		}
		issues = append(issues, issueShard{
			req:    fn(),
			broker: broker.meta.NodeID,
		})
	}
	cl.brokersMu.RUnlock()

	return issues, false, nil // we do NOT re-shard these requests request
}

// a convenience function for saving the first ResponseShard error.
func firstErrMerger(sresps []ResponseShard, merge func(kresp kmsg.Response)) error {
	var firstErr error
	for _, sresp := range sresps {
		if sresp.Err != nil {
			if firstErr == nil {
				firstErr = sresp.Err
			}
			continue
		}
		merge(sresp.Resp)
	}
	return firstErr
}

type mappedMetadataTopic struct {
	topic   kmsg.MetadataResponseTopic
	mapping map[int32]kmsg.MetadataResponseTopicPartition
}

// fetchMappedMetadata provides a convenience type of working with metadata;
// this is garbage heavy, so it is only used in one off requests in this
// package.
func (cl *Client) fetchMappedMetadata(ctx context.Context, topics []string) (map[string]mappedMetadataTopic, error) {
	_, meta, err := cl.fetchMetadataForTopics(ctx, false, topics)
	if err != nil {
		return nil, err
	}
	mapping := make(map[string]mappedMetadataTopic)
	for _, topic := range meta.Topics {
		t := mappedMetadataTopic{
			topic:   topic,
			mapping: make(map[int32]kmsg.MetadataResponseTopicPartition),
		}
		mapping[topic.Topic] = t
		for _, partition := range topic.Partitions {
			t.mapping[partition.Partition] = partition
		}
	}
	return mapping, nil
}

// handles sharding ListOffsetsRequest
type listOffsetsSharder struct{ *Client }

func (cl *listOffsetsSharder) shard(ctx context.Context, kreq kmsg.Request) ([]issueShard, bool, error) {
	req := kreq.(*kmsg.ListOffsetsRequest)

	// For listing offsets, we need the broker leader for each partition we
	// are listing. Thus, we first load metadata for the topics.
	//
	// Metadata loading performs retries; if we fail here, the we do not
	// issue sharded requests.
	var need []string
	for _, topic := range req.Topics {
		need = append(need, topic.Topic)
	}
	mapping, err := cl.fetchMappedMetadata(ctx, need)
	if err != nil {
		return nil, false, err
	}

	brokerReqs := make(map[int32]map[string][]kmsg.ListOffsetsRequestTopicPartition)
	unknowns := make(map[string][]kmsg.ListOffsetsRequestTopicPartition)

	// For any topic or partition that had an error load, we blindly issue
	// a load to the first seed broker. We expect the list to fail, but it
	// is the best we could do.
	for _, topic := range req.Topics {
		tmapping, exists := mapping[topic.Topic]
		if err := kerr.ErrorForCode(tmapping.topic.ErrorCode); err != nil || !exists {
			unknowns[topic.Topic] = append(unknowns[topic.Topic], topic.Partitions...)
			continue
		}
		for _, partition := range topic.Partitions {
			p, exists := tmapping.mapping[partition.Partition]
			if !exists || kerr.ErrorForCode(p.ErrorCode) != nil {
				unknowns[topic.Topic] = append(unknowns[topic.Topic], partition)
				continue
			}

			brokerReq := brokerReqs[p.Leader]
			if brokerReq == nil {
				brokerReq = make(map[string][]kmsg.ListOffsetsRequestTopicPartition)
				brokerReqs[p.Leader] = brokerReq
			}
			brokerReq[topic.Topic] = append(brokerReq[topic.Topic], partition)
		}
	}

	if len(unknowns) > 0 {
		brokerReqs[unknownSeedID(0)] = unknowns
	}

	var issues []issueShard
	for brokerID, brokerReq := range brokerReqs {
		req := &kmsg.ListOffsetsRequest{
			ReplicaID:      req.ReplicaID,
			IsolationLevel: req.IsolationLevel,
		}
		for topic, parts := range brokerReq {
			req.Topics = append(req.Topics, kmsg.ListOffsetsRequestTopic{
				Topic:      topic,
				Partitions: parts,
			})
		}

		issues = append(issues, issueShard{
			req:    req,
			broker: brokerID,
		})
	}

	return issues, true, nil // this is reshardable
}

func (cl *listOffsetsSharder) onResp(kreq kmsg.Response) {} // metadata could be stale, but no cleanup we can do

func (cl *listOffsetsSharder) merge(sresps []ResponseShard) (kmsg.Response, error) {
	merged := new(kmsg.ListOffsetsResponse)
	topics := make(map[string][]kmsg.ListOffsetsResponseTopicPartition)

	firstErr := firstErrMerger(sresps, func(kresp kmsg.Response) {
		resp := kresp.(*kmsg.ListOffsetsResponse)
		merged.Version = resp.Version
		merged.ThrottleMillis = resp.ThrottleMillis

		for _, topic := range resp.Topics {
			topics[topic.Topic] = append(topics[topic.Topic], topic.Partitions...)
		}
	})
	for topic, partitions := range topics {
		merged.Topics = append(merged.Topics, kmsg.ListOffsetsResponseTopic{
			Topic:      topic,
			Partitions: partitions,
		})
	}
	return merged, firstErr
}

// handles sharding DescribeGroupsRequest
type describeGroupsSharder struct{ *Client }

func (cl *describeGroupsSharder) shard(ctx context.Context, kreq kmsg.Request) ([]issueShard, bool, error) {
	req := kreq.(*kmsg.DescribeGroupsRequest)

	coordinators, err := cl.loadCoordinators(true, coordinatorTypeGroup, req.Groups...)
	if err != nil {
		return nil, false, err
	}

	brokerReqs := make(map[int32]*kmsg.DescribeGroupsRequest)

	for _, group := range req.Groups {
		broker := coordinators[group]
		brokerReq := brokerReqs[broker.meta.NodeID]
		if brokerReq == nil {
			brokerReq = &kmsg.DescribeGroupsRequest{
				IncludeAuthorizedOperations: req.IncludeAuthorizedOperations,
			}
			brokerReqs[broker.meta.NodeID] = brokerReq
		}
		brokerReq.Groups = append(brokerReq.Groups, group)
	}

	var issues []issueShard
	for id, req := range brokerReqs {
		issues = append(issues, issueShard{
			req:    req,
			broker: id,
		})
	}
	return issues, true, nil // this is reshardable
}

func (cl *describeGroupsSharder) onResp(kresp kmsg.Response) { // cleanup any stale groups
	resp := kresp.(*kmsg.DescribeGroupsResponse)
	for i := range resp.Groups {
		group := &resp.Groups[i]
		err := kerr.ErrorForCode(group.ErrorCode)
		cl.maybeDeleteStaleCoordinator(group.Group, coordinatorTypeGroup, err)
	}
}

func (cl *describeGroupsSharder) merge(sresps []ResponseShard) (kmsg.Response, error) {
	merged := new(kmsg.DescribeGroupsResponse)

	return merged, firstErrMerger(sresps, func(kresp kmsg.Response) {
		resp := kresp.(*kmsg.DescribeGroupsResponse)
		merged.Version = resp.Version
		merged.ThrottleMillis = resp.ThrottleMillis
		merged.Groups = append(merged.Groups, resp.Groups...)
	})
}

// handles sharding ListGroupsRequest
type listGroupsSharder struct{ *Client }

func (cl *listGroupsSharder) shard(ctx context.Context, kreq kmsg.Request) ([]issueShard, bool, error) {
	req := kreq.(*kmsg.ListGroupsRequest)
	return cl.allBrokersShardedReq(ctx, func() kmsg.Request {
		dup := *req
		return &dup
	})
}

func (cl *listGroupsSharder) onResp(kresp kmsg.Response) {} // nothing to be done here

func (cl *listGroupsSharder) merge(sresps []ResponseShard) (kmsg.Response, error) {
	merged := new(kmsg.ListGroupsResponse)

	return merged, firstErrMerger(sresps, func(kresp kmsg.Response) {
		resp := kresp.(*kmsg.ListGroupsResponse)
		merged.Version = resp.Version
		merged.ThrottleMillis = resp.ThrottleMillis
		if merged.ErrorCode == 0 {
			merged.ErrorCode = resp.ErrorCode
		}
		merged.Groups = append(merged.Groups, resp.Groups...)
	})
}

// handle sharding DeleteRecordsRequest
type deleteRecordsSharder struct{ *Client }

func (cl *deleteRecordsSharder) shard(ctx context.Context, kreq kmsg.Request) ([]issueShard, bool, error) {
	req := kreq.(*kmsg.DeleteRecordsRequest)

	var need []string
	for _, topic := range req.Topics {
		need = append(need, topic.Topic)
	}
	mapping, err := cl.fetchMappedMetadata(ctx, need)
	if err != nil {
		return nil, false, err
	}

	brokerReqs := make(map[int32]map[string][]kmsg.DeleteRecordsRequestTopicPartition)
	unknowns := make(map[string][]kmsg.DeleteRecordsRequestTopicPartition)

	for _, topic := range req.Topics {
		tmapping, exists := mapping[topic.Topic]
		if err := kerr.ErrorForCode(tmapping.topic.ErrorCode); err != nil || !exists {
			unknowns[topic.Topic] = append(unknowns[topic.Topic], topic.Partitions...)
			continue
		}
		for _, partition := range topic.Partitions {
			p, exists := tmapping.mapping[partition.Partition]
			if !exists || kerr.ErrorForCode(p.ErrorCode) != nil {
				unknowns[topic.Topic] = append(unknowns[topic.Topic], partition)
				continue
			}

			brokerReq := brokerReqs[p.Leader]
			if brokerReq == nil {
				brokerReq = make(map[string][]kmsg.DeleteRecordsRequestTopicPartition)
				brokerReqs[p.Leader] = brokerReq
			}
			brokerReq[topic.Topic] = append(brokerReq[topic.Topic], partition)
		}
	}

	if len(unknowns) > 0 {
		brokerReqs[unknownSeedID(0)] = unknowns
	}

	var issues []issueShard
	for brokerID, brokerReq := range brokerReqs {
		req := &kmsg.DeleteRecordsRequest{
			TimeoutMillis: req.TimeoutMillis,
		}
		for topic, parts := range brokerReq {
			req.Topics = append(req.Topics, kmsg.DeleteRecordsRequestTopic{
				Topic:      topic,
				Partitions: parts,
			})
		}

		issues = append(issues, issueShard{
			req:    req,
			broker: brokerID,
		})
	}

	return issues, true, nil // this is reshardable
}

func (cl *deleteRecordsSharder) onResp(kmsg.Response) {} // nothing to be done here

func (cl *deleteRecordsSharder) merge(sresps []ResponseShard) (kmsg.Response, error) {
	merged := new(kmsg.DeleteRecordsResponse)
	topics := make(map[string][]kmsg.DeleteRecordsResponseTopicPartition)

	firstErr := firstErrMerger(sresps, func(kresp kmsg.Response) {
		resp := kresp.(*kmsg.DeleteRecordsResponse)
		merged.Version = resp.Version
		merged.ThrottleMillis = resp.ThrottleMillis

		for _, topic := range resp.Topics {
			topics[topic.Topic] = append(topics[topic.Topic], topic.Partitions...)
		}
	})
	for topic, partitions := range topics {
		merged.Topics = append(merged.Topics, kmsg.DeleteRecordsResponseTopic{
			Topic:      topic,
			Partitions: partitions,
		})
	}
	return merged, firstErr
}

// handle sharding OffsetForLeaderEpochRequest
type offsetForLeaderEpochSharder struct{ *Client }

func (cl *offsetForLeaderEpochSharder) shard(ctx context.Context, kreq kmsg.Request) ([]issueShard, bool, error) {
	req := kreq.(*kmsg.OffsetForLeaderEpochRequest)

	var need []string
	for _, topic := range req.Topics {
		need = append(need, topic.Topic)
	}
	mapping, err := cl.fetchMappedMetadata(ctx, need)
	if err != nil {
		return nil, false, err
	}

	brokerReqs := make(map[int32]map[string][]kmsg.OffsetForLeaderEpochRequestTopicPartition)
	unknowns := make(map[string][]kmsg.OffsetForLeaderEpochRequestTopicPartition)

	for _, topic := range req.Topics {
		tmapping, exists := mapping[topic.Topic]
		if err := kerr.ErrorForCode(tmapping.topic.ErrorCode); err != nil || !exists {
			unknowns[topic.Topic] = append(unknowns[topic.Topic], topic.Partitions...)
			continue
		}
		for _, partition := range topic.Partitions {
			p, exists := tmapping.mapping[partition.Partition]
			if !exists || kerr.ErrorForCode(p.ErrorCode) != nil {
				unknowns[topic.Topic] = append(unknowns[topic.Topic], partition)
				continue
			}

			brokerReq := brokerReqs[p.Leader]
			if brokerReq == nil {
				brokerReq = make(map[string][]kmsg.OffsetForLeaderEpochRequestTopicPartition)
				brokerReqs[p.Leader] = brokerReq
			}
			brokerReq[topic.Topic] = append(brokerReq[topic.Topic], partition)
		}
	}

	if len(unknowns) > 0 {
		brokerReqs[unknownSeedID(0)] = unknowns
	}

	var issues []issueShard
	for brokerID, brokerReq := range brokerReqs {
		req := &kmsg.OffsetForLeaderEpochRequest{
			ReplicaID: req.ReplicaID,
		}
		for topic, parts := range brokerReq {
			req.Topics = append(req.Topics, kmsg.OffsetForLeaderEpochRequestTopic{
				Topic:      topic,
				Partitions: parts,
			})
		}

		issues = append(issues, issueShard{
			req:    req,
			broker: brokerID,
		})
	}

	return issues, true, nil // this is reshardable
}

func (cl *offsetForLeaderEpochSharder) onResp(kmsg.Response) {}

func (cl *offsetForLeaderEpochSharder) merge(sresps []ResponseShard) (kmsg.Response, error) {
	merged := new(kmsg.OffsetForLeaderEpochResponse)
	topics := make(map[string][]kmsg.OffsetForLeaderEpochResponseTopicPartition)

	firstErr := firstErrMerger(sresps, func(kresp kmsg.Response) {
		resp := kresp.(*kmsg.OffsetForLeaderEpochResponse)
		merged.Version = resp.Version
		merged.ThrottleMillis = resp.ThrottleMillis

		for _, topic := range resp.Topics {
			topics[topic.Topic] = append(topics[topic.Topic], topic.Partitions...)
		}
	})
	for topic, partitions := range topics {
		merged.Topics = append(merged.Topics, kmsg.OffsetForLeaderEpochResponseTopic{
			Topic:      topic,
			Partitions: partitions,
		})
	}
	return merged, firstErr
}

// handle sharding DescribeConfigsRequest
type describeConfigsSharder struct{ *Client }

func (cl *describeConfigsSharder) shard(ctx context.Context, kreq kmsg.Request) ([]issueShard, bool, error) {
	req := kreq.(*kmsg.DescribeConfigsRequest)

	brokerReqs := make(map[int32][]kmsg.DescribeConfigsRequestResource)
	var any []kmsg.DescribeConfigsRequestResource

	for i := range req.Resources {
		resource := req.Resources[i]
		switch resource.ResourceType {
		case kmsg.ConfigResourceTypeBroker:
		case kmsg.ConfigResourceTypeBrokerLogger:
		default:
			any = append(any, resource)
			continue
		}
		id, err := strconv.ParseInt(resource.ResourceName, 10, 32)
		if err != nil || id < 0 {
			any = append(any, resource)
			continue
		}
		brokerReqs[int32(id)] = append(brokerReqs[int32(id)], resource)
	}

	var issues []issueShard
	for brokerID, brokerReq := range brokerReqs {
		req := &kmsg.DescribeConfigsRequest{
			Resources:            brokerReq,
			IncludeSynonyms:      req.IncludeSynonyms,
			IncludeDocumentation: req.IncludeDocumentation,
		}

		issues = append(issues, issueShard{
			req:    req,
			broker: brokerID,
		})
	}

	if len(any) > 0 {
		issues = append(issues, issueShard{
			req: &kmsg.DescribeConfigsRequest{
				Resources:            any,
				IncludeSynonyms:      req.IncludeSynonyms,
				IncludeDocumentation: req.IncludeDocumentation,
			},
			any: true,
		})
	}

	return issues, false, nil // this is not reshardable, but the any block can go anywhere
}

func (cl *describeConfigsSharder) onResp(kmsg.Response) {}

func (cl *describeConfigsSharder) merge(sresps []ResponseShard) (kmsg.Response, error) {
	merged := new(kmsg.DescribeConfigsResponse)

	return merged, firstErrMerger(sresps, func(kresp kmsg.Response) {
		resp := kresp.(*kmsg.DescribeConfigsResponse)
		merged.Version = resp.Version
		merged.ThrottleMillis = resp.ThrottleMillis
		merged.Resources = append(merged.Resources, resp.Resources...)
	})
}

// handle sharding AlterConfigsRequest
type alterConfigsSharder struct{ *Client }

func (cl *alterConfigsSharder) shard(ctx context.Context, kreq kmsg.Request) ([]issueShard, bool, error) {
	req := kreq.(*kmsg.AlterConfigsRequest)

	brokerReqs := make(map[int32][]kmsg.AlterConfigsRequestResource)
	var any []kmsg.AlterConfigsRequestResource

	for i := range req.Resources {
		resource := req.Resources[i]
		switch resource.ResourceType {
		case kmsg.ConfigResourceTypeBroker:
		case kmsg.ConfigResourceTypeBrokerLogger:
		default:
			any = append(any, resource)
			continue
		}
		id, err := strconv.ParseInt(resource.ResourceName, 10, 32)
		if err != nil || id < 0 {
			any = append(any, resource)
			continue
		}
		brokerReqs[int32(id)] = append(brokerReqs[int32(id)], resource)
	}

	var issues []issueShard
	for brokerID, brokerReq := range brokerReqs {
		req := &kmsg.AlterConfigsRequest{
			Resources:    brokerReq,
			ValidateOnly: req.ValidateOnly,
		}

		issues = append(issues, issueShard{
			req:    req,
			broker: brokerID,
		})
	}

	if len(any) > 0 {
		issues = append(issues, issueShard{
			req: &kmsg.AlterConfigsRequest{
				Resources:    any,
				ValidateOnly: req.ValidateOnly,
			},
			any: true,
		})
	}

	return issues, false, nil // this is not reshardable, but the any block can go anywhere
}

func (cl *alterConfigsSharder) onResp(kmsg.Response) {}

func (cl *alterConfigsSharder) merge(sresps []ResponseShard) (kmsg.Response, error) {
	merged := new(kmsg.AlterConfigsResponse)

	return merged, firstErrMerger(sresps, func(kresp kmsg.Response) {
		resp := kresp.(*kmsg.AlterConfigsResponse)
		merged.Version = resp.Version
		merged.ThrottleMillis = resp.ThrottleMillis
		merged.Resources = append(merged.Resources, resp.Resources...)
	})
}

// handles sharding AlterReplicaLogDirsRequest
type alterReplicaLogDirsSharder struct{ *Client }

func (cl *alterReplicaLogDirsSharder) shard(ctx context.Context, kreq kmsg.Request) ([]issueShard, bool, error) {
	req := kreq.(*kmsg.AlterReplicaLogDirsRequest)

	needMap := make(map[string]struct{})
	for _, dir := range req.Dirs {
		for _, topic := range dir.Topics {
			needMap[topic.Topic] = struct{}{}
		}
	}
	var need []string
	for topic := range needMap {
		need = append(need, topic)
	}
	mapping, err := cl.fetchMappedMetadata(ctx, need)
	if err != nil {
		return nil, false, err
	}

	brokerReqs := make(map[int32]map[string]map[string][]int32) // broker => dir => topic => partitions
	unknowns := make(map[string]map[string][]int32)             // dir => topic => partitions

	addBroker := func(broker int32, dir, topic string, partition int32) {
		brokerDirs := brokerReqs[broker]
		if brokerDirs == nil {
			brokerDirs = make(map[string]map[string][]int32)
			brokerReqs[broker] = brokerDirs
		}
		dirTopics := brokerDirs[dir]
		if dirTopics == nil {
			dirTopics = make(map[string][]int32)
			brokerDirs[dir] = dirTopics
		}
		dirTopics[topic] = append(dirTopics[topic], partition)
	}

	addUnknown := func(dir, topic string, partition int32) {
		dirTopics := unknowns[dir]
		if dirTopics == nil {
			dirTopics = make(map[string][]int32)
			unknowns[dir] = dirTopics
		}
		dirTopics[topic] = append(dirTopics[topic], partition)
	}

	for _, dir := range req.Dirs {
		for _, topic := range dir.Topics {
			tmapping, exists := mapping[topic.Topic]
			if err := kerr.ErrorForCode(tmapping.topic.ErrorCode); err != nil || !exists {
				for _, partition := range topic.Partitions {
					addUnknown(dir.Dir, topic.Topic, partition)
				}
				continue
			}
			for _, partition := range topic.Partitions {
				p, exists := tmapping.mapping[partition]
				if !exists || kerr.ErrorForCode(p.ErrorCode) != nil {
					addUnknown(dir.Dir, topic.Topic, partition)
					continue
				}

				for _, replica := range p.Replicas {
					addBroker(replica, dir.Dir, topic.Topic, partition)
				}
			}
		}
	}

	if len(unknowns) > 0 {
		brokerReqs[unknownSeedID(0)] = unknowns
	}

	var issues []issueShard
	for brokerID, brokerReq := range brokerReqs {
		req := new(kmsg.AlterReplicaLogDirsRequest)
		for dir, topics := range brokerReq {
			rd := kmsg.AlterReplicaLogDirsRequestDir{
				Dir: dir,
			}
			for topic, partitions := range topics {
				rd.Topics = append(rd.Topics, kmsg.AlterReplicaLogDirsRequestDirTopic{
					Topic:      topic,
					Partitions: partitions,
				})
			}
			req.Dirs = append(req.Dirs, rd)
		}

		issues = append(issues, issueShard{
			req:    req,
			broker: brokerID,
		})
	}

	return issues, true, nil // this is reshardable
}

func (cl *alterReplicaLogDirsSharder) onResp(kmsg.Response) {}

// merge does not make sense for this function, but we provide a one anyway.
func (cl *alterReplicaLogDirsSharder) merge(sresps []ResponseShard) (kmsg.Response, error) {
	merged := new(kmsg.AlterReplicaLogDirsResponse)
	topics := make(map[string][]kmsg.AlterReplicaLogDirsResponseTopicPartition)

	firstErr := firstErrMerger(sresps, func(kresp kmsg.Response) {
		resp := kresp.(*kmsg.AlterReplicaLogDirsResponse)
		merged.Version = resp.Version
		merged.ThrottleMillis = resp.ThrottleMillis

		for _, topic := range resp.Topics {
			topics[topic.Topic] = append(topics[topic.Topic], topic.Partitions...)
		}
	})
	for topic, partitions := range topics {
		merged.Topics = append(merged.Topics, kmsg.AlterReplicaLogDirsResponseTopic{
			Topic:      topic,
			Partitions: partitions,
		})
	}
	return merged, firstErr
}

// handles sharding DescribeLogDirsRequest
type describeLogDirsSharder struct{ *Client }

func (cl *describeLogDirsSharder) shard(ctx context.Context, kreq kmsg.Request) ([]issueShard, bool, error) {
	req := kreq.(*kmsg.DescribeLogDirsRequest)

	// If req.Topics is nil, the request is to describe all logdirs. Thus,
	// we will issue the request to all brokers (similar to ListGroups).
	if req.Topics == nil {
		return cl.allBrokersShardedReq(ctx, func() kmsg.Request {
			dup := *req
			return &dup
		})
	}

	var need []string
	for _, topic := range req.Topics {
		need = append(need, topic.Topic)
	}
	mapping, err := cl.fetchMappedMetadata(ctx, need)
	if err != nil {
		return nil, false, err
	}

	brokerReqs := make(map[int32]map[string][]int32)
	unknowns := make(map[string][]int32)

	for _, topic := range req.Topics {
		tmapping, exists := mapping[topic.Topic]
		if !exists || kerr.ErrorForCode(tmapping.topic.ErrorCode) != nil {
			unknowns[topic.Topic] = append(unknowns[topic.Topic], topic.Partitions...)
			continue
		}
		for _, partition := range topic.Partitions {
			p, exists := tmapping.mapping[partition]
			if !exists || kerr.ErrorForCode(p.ErrorCode) != nil {
				unknowns[topic.Topic] = append(unknowns[topic.Topic], partition)
				continue
			}

			for _, replica := range p.Replicas {
				brokerReq := brokerReqs[replica]
				if brokerReq == nil {
					brokerReq = make(map[string][]int32)
					brokerReqs[replica] = brokerReq
				}
				brokerReq[topic.Topic] = append(brokerReq[topic.Topic], partition)
			}
		}
	}

	if len(unknowns) > 0 {
		brokerReqs[unknownSeedID(0)] = unknowns
	}

	var issues []issueShard
	for brokerID, brokerReq := range brokerReqs {
		req := new(kmsg.DescribeLogDirsRequest)
		for topic, parts := range brokerReq {
			req.Topics = append(req.Topics, kmsg.DescribeLogDirsRequestTopic{
				Topic:      topic,
				Partitions: parts,
			})
		}

		issues = append(issues, issueShard{
			req:    req,
			broker: brokerID,
		})
	}

	return issues, true, nil // this is reshardable
}

func (cl *describeLogDirsSharder) onResp(kmsg.Response) {}

// merge does not make sense for this function, but we provide one anyway.
// We lose the error code for directories.
func (cl *describeLogDirsSharder) merge(sresps []ResponseShard) (kmsg.Response, error) {
	merged := new(kmsg.DescribeLogDirsResponse)
	dirs := make(map[string]map[string][]kmsg.DescribeLogDirsResponseDirTopicPartition)

	firstErr := firstErrMerger(sresps, func(kresp kmsg.Response) {
		resp := kresp.(*kmsg.DescribeLogDirsResponse)
		merged.Version = resp.Version
		merged.ThrottleMillis = resp.ThrottleMillis

		for _, dir := range resp.Dirs {
			mergeDir := dirs[dir.Dir]
			if mergeDir == nil {
				mergeDir = make(map[string][]kmsg.DescribeLogDirsResponseDirTopicPartition)
				dirs[dir.Dir] = mergeDir
			}
			for _, topic := range dir.Topics {
				mergeDir[topic.Topic] = append(mergeDir[topic.Topic], topic.Partitions...)
			}
		}
	})
	for dir, topics := range dirs {
		md := kmsg.DescribeLogDirsResponseDir{
			Dir: dir,
		}
		for topic, partitions := range topics {
			md.Topics = append(md.Topics, kmsg.DescribeLogDirsResponseDirTopic{
				Topic:      topic,
				Partitions: partitions,
			})
		}
		merged.Dirs = append(merged.Dirs, md)
	}
	return merged, firstErr
}

// handles sharding DeleteGroupsRequest
type deleteGroupsSharder struct{ *Client }

func (cl *deleteGroupsSharder) shard(ctx context.Context, kreq kmsg.Request) ([]issueShard, bool, error) {
	req := kreq.(*kmsg.DeleteGroupsRequest)

	coordinators, err := cl.loadCoordinators(true, coordinatorTypeGroup, req.Groups...)
	if err != nil {
		return nil, false, err
	}

	brokerReqs := make(map[int32]*kmsg.DeleteGroupsRequest)

	for _, group := range req.Groups {
		broker := coordinators[group]
		brokerReq := brokerReqs[broker.meta.NodeID]
		if brokerReq == nil {
			brokerReq = new(kmsg.DeleteGroupsRequest)
			brokerReqs[broker.meta.NodeID] = brokerReq
		}
		brokerReq.Groups = append(brokerReq.Groups, group)
	}

	var issues []issueShard
	for id, req := range brokerReqs {
		issues = append(issues, issueShard{
			req:    req,
			broker: id,
		})
	}
	return issues, true, nil // this is reshardable
}

func (cl *deleteGroupsSharder) onResp(kresp kmsg.Response) {
	resp := kresp.(*kmsg.DeleteGroupsResponse)
	for i := range resp.Groups {
		group := &resp.Groups[i]
		err := kerr.ErrorForCode(group.ErrorCode)
		cl.maybeDeleteStaleCoordinator(group.Group, coordinatorTypeGroup, err)
	}
}

func (cl *deleteGroupsSharder) merge(sresps []ResponseShard) (kmsg.Response, error) {
	merged := new(kmsg.DeleteGroupsResponse)

	return merged, firstErrMerger(sresps, func(kresp kmsg.Response) {
		resp := kresp.(*kmsg.DeleteGroupsResponse)
		merged.Version = resp.Version
		merged.ThrottleMillis = resp.ThrottleMillis
		merged.Groups = append(merged.Groups, resp.Groups...)
	})
}

// handle sharding IncrementalAlterConfigsRequest
type incrementalAlterConfigsSharder struct{ *Client }

func (cl *incrementalAlterConfigsSharder) shard(ctx context.Context, kreq kmsg.Request) ([]issueShard, bool, error) {
	req := kreq.(*kmsg.IncrementalAlterConfigsRequest)

	brokerReqs := make(map[int32][]kmsg.IncrementalAlterConfigsRequestResource)
	var any []kmsg.IncrementalAlterConfigsRequestResource

	for i := range req.Resources {
		resource := req.Resources[i]
		switch resource.ResourceType {
		case kmsg.ConfigResourceTypeBroker:
		case kmsg.ConfigResourceTypeBrokerLogger:
		default:
			any = append(any, resource)
			continue
		}
		id, err := strconv.ParseInt(resource.ResourceName, 10, 32)
		if err != nil || id < 0 {
			any = append(any, resource)
			continue
		}
		brokerReqs[int32(id)] = append(brokerReqs[int32(id)], resource)
	}

	var issues []issueShard
	for brokerID, brokerReq := range brokerReqs {
		req := &kmsg.IncrementalAlterConfigsRequest{
			Resources:    brokerReq,
			ValidateOnly: req.ValidateOnly,
		}

		issues = append(issues, issueShard{
			req:    req,
			broker: brokerID,
		})
	}

	if len(any) > 0 {
		issues = append(issues, issueShard{
			req: &kmsg.IncrementalAlterConfigsRequest{
				Resources:    any,
				ValidateOnly: req.ValidateOnly,
			},
			any: true,
		})
	}

	return issues, false, nil // this is not reshardable, but the any block can go anywhere
}

func (cl *incrementalAlterConfigsSharder) onResp(kmsg.Response) {}

func (cl *incrementalAlterConfigsSharder) merge(sresps []ResponseShard) (kmsg.Response, error) {
	merged := new(kmsg.IncrementalAlterConfigsResponse)

	return merged, firstErrMerger(sresps, func(kresp kmsg.Response) {
		resp := kresp.(*kmsg.IncrementalAlterConfigsResponse)
		merged.Version = resp.Version
		merged.ThrottleMillis = resp.ThrottleMillis
		merged.Resources = append(merged.Resources, resp.Resources...)
	})
}

// handle sharding DescribeProducersRequest
type describeProducersSharder struct{ *Client }

func (cl *describeProducersSharder) shard(ctx context.Context, kreq kmsg.Request) ([]issueShard, bool, error) {
	req := kreq.(*kmsg.DescribeProducersRequest)

	var need []string
	for _, topic := range req.Topics {
		need = append(need, topic.Topic)
	}
	mapping, err := cl.fetchMappedMetadata(ctx, need)
	if err != nil {
		return nil, false, err
	}

	brokerReqs := make(map[int32]map[string][]int32) // broker => topic => partitions
	unknowns := make(map[string][]int32)             // topic => partitions

	for _, topic := range req.Topics {
		tmapping, exists := mapping[topic.Topic]
		if err := kerr.ErrorForCode(tmapping.topic.ErrorCode); err != nil || !exists {
			unknowns[topic.Topic] = append(unknowns[topic.Topic], topic.Partitions...)
			continue
		}
		for _, partition := range topic.Partitions {
			p, exists := tmapping.mapping[partition]
			if !exists || kerr.ErrorForCode(p.ErrorCode) != nil {
				unknowns[topic.Topic] = append(unknowns[topic.Topic], partition)
				continue
			}

			brokerReq := brokerReqs[p.Leader]
			if brokerReq == nil {
				brokerReq = make(map[string][]int32)
				brokerReqs[p.Leader] = brokerReq
			}
			brokerReq[topic.Topic] = append(brokerReq[topic.Topic], partition)
		}
	}

	if len(unknowns) > 0 {
		brokerReqs[unknownSeedID(0)] = unknowns
	}

	var issues []issueShard
	for brokerID, brokerReq := range brokerReqs {
		req := &kmsg.DescribeProducersRequest{}
		for topic, parts := range brokerReq {
			req.Topics = append(req.Topics, kmsg.DescribeProducersRequestTopic{
				Topic:      topic,
				Partitions: parts,
			})
		}

		issues = append(issues, issueShard{
			req:    req,
			broker: brokerID,
		})
	}

	return issues, true, nil // this is reshardable
}

func (cl *describeProducersSharder) onResp(kmsg.Response) {}

func (cl *describeProducersSharder) merge(sresps []ResponseShard) (kmsg.Response, error) {
	merged := new(kmsg.DescribeProducersResponse)
	topics := make(map[string][]kmsg.DescribeProducersResponseTopicPartition)

	firstErr := firstErrMerger(sresps, func(kresp kmsg.Response) {
		resp := kresp.(*kmsg.DescribeProducersResponse)
		merged.Version = resp.Version
		merged.ThrottleMillis = resp.ThrottleMillis

		for _, topic := range resp.Topics {
			topics[topic.Topic] = append(topics[topic.Topic], topic.Partitions...)
		}
	})
	for topic, partitions := range topics {
		merged.Topics = append(merged.Topics, kmsg.DescribeProducersResponseTopic{
			Topic:      topic,
			Partitions: partitions,
		})
	}
	return merged, firstErr
}

// handles sharding DescribeTransactionsRequest
type describeTransactionsSharder struct{ *Client }

func (cl *describeTransactionsSharder) shard(ctx context.Context, kreq kmsg.Request) ([]issueShard, bool, error) {
	req := kreq.(*kmsg.DescribeTransactionsRequest)

	coordinators, err := cl.loadCoordinators(true, coordinatorTypeTxn, req.TransactionalIDs...)
	if err != nil {
		return nil, false, err
	}

	brokerReqs := make(map[int32]*kmsg.DescribeTransactionsRequest)

	for _, txnID := range req.TransactionalIDs {
		broker := coordinators[txnID]
		brokerReq := brokerReqs[broker.meta.NodeID]
		if brokerReq == nil {
			brokerReq = &kmsg.DescribeTransactionsRequest{}
			brokerReqs[broker.meta.NodeID] = brokerReq
		}
		brokerReq.TransactionalIDs = append(brokerReq.TransactionalIDs, txnID)
	}

	var issues []issueShard
	for id, req := range brokerReqs {
		issues = append(issues, issueShard{
			req:    req,
			broker: id,
		})
	}
	return issues, true, nil // this is reshardable
}

func (cl *describeTransactionsSharder) onResp(kresp kmsg.Response) { // cleanup any stale coordinators
	resp := kresp.(*kmsg.DescribeTransactionsResponse)
	for i := range resp.TransactionStates {
		txnState := &resp.TransactionStates[i]
		err := kerr.ErrorForCode(txnState.ErrorCode)
		cl.maybeDeleteStaleCoordinator(txnState.TransactionalID, coordinatorTypeTxn, err)
	}
}

func (cl *describeTransactionsSharder) merge(sresps []ResponseShard) (kmsg.Response, error) {
	merged := new(kmsg.DescribeTransactionsResponse)

	return merged, firstErrMerger(sresps, func(kresp kmsg.Response) {
		resp := kresp.(*kmsg.DescribeTransactionsResponse)
		merged.Version = resp.Version
		merged.ThrottleMillis = resp.ThrottleMillis
		merged.TransactionStates = append(merged.TransactionStates, resp.TransactionStates...)
	})
}

// handles sharding ListTransactionsRequest
type listTransactionsSharder struct{ *Client }

func (cl *listTransactionsSharder) shard(ctx context.Context, kreq kmsg.Request) ([]issueShard, bool, error) {
	req := kreq.(*kmsg.ListTransactionsRequest)
	return cl.allBrokersShardedReq(ctx, func() kmsg.Request {
		dup := *req
		return &dup
	})
}

func (cl *listTransactionsSharder) onResp(kresp kmsg.Response) {} // nothing to do

func (cl *listTransactionsSharder) merge(sresps []ResponseShard) (kmsg.Response, error) {
	merged := new(kmsg.ListTransactionsResponse)

	unknownStates := make(map[string]struct{})

	firstErr := firstErrMerger(sresps, func(kresp kmsg.Response) {
		resp := kresp.(*kmsg.ListTransactionsResponse)
		merged.Version = resp.Version
		merged.ThrottleMillis = resp.ThrottleMillis
		if merged.ErrorCode == 0 {
			merged.ErrorCode = resp.ErrorCode
		}
		for _, state := range resp.UnknownStateFilters {
			unknownStates[state] = struct{}{}
		}
		merged.TransactionStates = append(merged.TransactionStates, resp.TransactionStates...)
	})
	for unknownState := range unknownStates {
		merged.UnknownStateFilters = append(merged.UnknownStateFilters, unknownState)
	}

	return merged, firstErr

}
