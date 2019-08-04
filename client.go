package kgo

import (
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/exp/rand"

	"github.com/twmb/kgo/kerr"
	"github.com/twmb/kgo/kmsg"
)

// TODO: producing should use its own broker connections, and consuming
// should use its own broker connections. This way, metadata/offset
// commits are not blocked.
//
// This can be done on the broker struct!
// - have three cxn pointers, three loops (spun up as necessary)
// - depending on what type of request used,
//   used appropriate loo
// - request type can have field
// - or can call broker.waitConsumer, waitProducer, wait

// Client issues requests and handles responses to a Kafka cluster.
type Client struct {
	cfg cfg

	rng *rand.Rand

	brokersMu    sync.RWMutex
	brokers      map[int32]*broker // broker id => broker
	anyBroker    []*broker
	anyBrokerIdx int

	controllerID int32 // atomic

	producer producer
	consumer consumer

	coordinatorsMu sync.Mutex
	coordinators   map[coordinatorKey]int32

	topicsMu sync.Mutex
	topics   atomic.Value // map[string]*topicPartitions

	metadataTicker   *time.Ticker
	updateMetadataCh chan struct{}

	closedCh chan struct{}
}

// broker returns a random broker from all brokers ever known.
func (c *Client) broker() *broker {
	c.brokersMu.Lock()
	defer c.brokersMu.Unlock()

	b := c.anyBroker[c.anyBrokerIdx]
	c.anyBrokerIdx++
	if c.anyBrokerIdx == len(c.anyBroker) {
		c.anyBrokerIdx = 0
		c.rng.Shuffle(len(c.anyBroker), func(i, j int) { c.anyBroker[i], c.anyBroker[j] = c.anyBroker[j], c.anyBroker[i] })
	}
	return b
}

// fetchBrokerMetadata issues a metadata request solely for broker information.
func (c *Client) fetchBrokerMetadata() error {
	_, err := c.fetchMetadata(false, nil)
	return err
}

func (c *Client) fetchMetadata(all bool, topics []string) (*kmsg.MetadataResponse, error) {
	if all {
		topics = nil
	} else {
		topics = []string{}
	}
	broker := c.broker()
	var meta *kmsg.MetadataResponse
	var err error
	broker.wait(
		&kmsg.MetadataRequest{
			Topics:                 topics,
			AllowAutoTopicCreation: c.cfg.producer.allowAutoTopicCreation,
		},
		func(resp kmsg.Response, respErr error) {
			if err = respErr; err != nil {
				return
			}
			meta = resp.(*kmsg.MetadataResponse)
		},
	)
	if err == nil {
		if meta.ControllerID > 0 {
			atomic.StoreInt32(&c.controllerID, meta.ControllerID)
		}
		c.updateBrokers(meta.Brokers)
	}
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

// TODO Shutdown the client.
//
// For producing: we can just rely on the passed callbacks to ensure we do not
// interrupt message sends. Users should just not close until the callbacks
// return if they care about that.
//
// For consuming: we can interrupt everything. Is it worth it to do async
// close? Should not matter, since it is fundamentally racy (stop consuming
// and checkpoint later vs. interrupt, reconnect, consume where left off).
//
// Returns a function that waits until all connections have died.
func (c *Client) Close() func() {
	return nil
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
func (c *Client) Request(req kmsg.Request) (kmsg.Response, error) {
	tries := 0
	var resp kmsg.Response
	var err error
start:
	tries++
	if metaReq, isMetaReq := req.(*kmsg.MetadataRequest); isMetaReq {
		// We hijack any metadata request so as to populate our
		// own brokers and controller ID.
		resp, err = c.fetchMetadata(metaReq.Topics == nil, metaReq.Topics)
	} else if _, admin := req.(kmsg.AdminRequest); admin {
		var controller *broker
		if controller, err = c.controller(); err == nil {
			resp, err = controller.waitResp(req)
		}
	} else if groupReq, isGroupReq := req.(kmsg.GroupCoordinatorRequest); isGroupReq {
		resp, err = c.handleGroupReq(groupReq)
	} else if txnReq, isTxnReq := req.(kmsg.TxnCoordinatorRequest); isTxnReq {
		resp, err = c.handleTxnReq(txnReq)
	} else {
		resp, err = c.broker().waitResp(req)
	}

	if isRetriableBrokerErr(err) && tries < c.cfg.client.retries {
		select {
		case <-c.closedCh:
			return nil, err
		case <-time.After(c.cfg.client.retryBackoff(tries)):
			goto start
		}
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
func (c *Client) controller() (*broker, error) {
	tries := 0
start:
	var id int32
	if id = atomic.LoadInt32(&c.controllerID); id < 0 {
		tries++
		if err := c.fetchBrokerMetadata(); err != nil {
			if isRetriableBrokerErr(err) && tries < c.cfg.client.retries {
				select {
				case <-c.closedCh:
					return nil, err
				case <-time.After(c.cfg.client.retryBackoff(tries)):
					goto start
				}
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
func (c *Client) loadCoordinator(key coordinatorKey) (*broker, error) {
	// If there is no controller, we have never loaded brokers. We will
	// need the brokers after we know which one owns this key, so force
	// a load of the brokers now.
	if atomic.LoadInt32(&c.controllerID) < 0 {
		if _, err := c.controller(); err != nil {
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
	kresp, err := c.broker().waitResp(&kmsg.FindCoordinatorRequest{
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
		if isRetriableErr(err) && tries < c.cfg.client.retries {
			select {
			case <-c.closedCh:
				return nil, err
			case <-time.After(c.cfg.client.retryBackoff(tries)):
				goto start
			}
		}
		return nil, err
	}

	coordinator = resp.Coordinator.NodeID
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
func (c *Client) handleGroupReq(req kmsg.GroupCoordinatorRequest) (kmsg.Response, error) {
	var group2req map[string]kmsg.Request
	var kresp kmsg.Response
	var merge func(kmsg.Response)

	switch t := req.(type) {
	default:
		// All group requests should be listed below, so if it isn't,
		// then we do not know what this request is.
		return nil, ErrClientTooOld

	case *kmsg.OffsetCommitRequest:
		return c.handleGroupReqSimple(t.GroupID, req)
	case *kmsg.OffsetFetchRequest:
		return c.handleGroupReqSimple(t.GroupID, req)
	case *kmsg.JoinGroupRequest:
		return c.handleGroupReqSimple(t.GroupID, req)
	case *kmsg.HeartbeatRequest:
		return c.handleGroupReqSimple(t.GroupID, req)
	case *kmsg.LeaveGroupRequest:
		return c.handleGroupReqSimple(t.GroupID, req)
	case *kmsg.SyncGroupRequest:
		return c.handleGroupReqSimple(t.GroupID, req)

	case *kmsg.DescribeGroupsRequest:
		group2req = make(map[string]kmsg.Request)
		for _, id := range t.GroupIDs {
			group2req[id] = &kmsg.DescribeGroupsRequest{
				GroupIDs:                    []string{id},
				IncludeAuthorizedOperations: t.IncludeAuthorizedOperations,
			}
		}
		resp := new(kmsg.DescribeGroupsResponse)
		kresp = resp
		merge = func(newKResp kmsg.Response) {
			newResp := newKResp.(*kmsg.DescribeGroupsResponse)
			resp.Version = newResp.Version
			resp.ThrottleTimeMs = newResp.ThrottleTimeMs
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
			resp.ThrottleTimeMs = newResp.ThrottleTimeMs
			resp.GroupErrorCodes = append(resp.GroupErrorCodes, newResp.GroupErrorCodes...)
		}
	}

	var firstErr error
	var errs int
	for id, req := range group2req {
		resp, err := c.handleGroupReqSimple(id, req)
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
// if so, the coordinator is deleted.
func (c *Client) handleGroupReqSimple(groupID string, req kmsg.Request) (kmsg.Response, error) {
	coordinator, err := c.loadCoordinator(coordinatorKey{
		name: groupID,
		typ:  coordinatorTypeGroup,
	})
	if err != nil {
		return nil, err
	}
	kresp, err := coordinator.waitResp(req)
	if err != nil {
		return kresp, err
	}

	var errCode int16
	switch t := kresp.(type) {
	case *kmsg.OffsetCommitResponse:
		if len(t.Responses) > 0 && len(t.Responses[0].PartitionResponses) > 0 {
			errCode = t.Responses[0].PartitionResponses[0].ErrorCode
		}
	case *kmsg.OffsetFetchResponse:
		if t.Version >= 2 {
			errCode = t.ErrorCode
		} else if len(t.Responses) > 0 && len(t.Responses[0].PartitionResponses) > 0 {
			errCode = t.Responses[0].PartitionResponses[0].ErrorCode
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
		if len(t.GroupErrorCodes) > 0 {
			errCode = t.GroupErrorCodes[0].ErrorCode
		}
	}

	switch groupErr := kerr.ErrorForCode(errCode); groupErr {
	case kerr.CoordinatorNotAvailable,
		kerr.CoordinatorLoadInProgress,
		kerr.NotCoordinator:
		err = groupErr

		c.coordinatorsMu.Lock()
		delete(c.coordinators, coordinatorKey{
			name: groupID,
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
func (c *Client) handleTxnReq(req kmsg.TxnCoordinatorRequest) (kmsg.Response, error) {
	switch t := req.(type) {
	default:
		// All txn requests should be listed below, so if it isn't,
		// then we do not know what this request is.
		return nil, ErrClientTooOld

	case *kmsg.InitProducerIDRequest:
		if t.TransactionalID != nil {
			return c.handleTxnRequest(*t.TransactionalID, req)
		}
		// InitProducerID can go to any broker if the transactional ID
		// is nil.
		return c.broker().waitResp(req)
	case *kmsg.AddPartitionsToTxnRequest:
		return c.handleTxnRequest(t.TransactionalID, req)
	case *kmsg.AddOffsetsToTxnRequest:
		return c.handleTxnRequest(t.TransactionalID, req)
	case *kmsg.EndTxnRequest:
		return c.handleTxnRequest(t.TransactionalID, req)
	case *kmsg.TxnOffsetCommitRequest:
		return c.handleTxnRequest(t.TransactionalID, req)
	}
}

// handleTxnRequest issues a request for a transaction to the coordinator for
// that transaction.
//
// The error is inspected to see if it is a retriable group error and, if so,
// the coordinator is deleted.
func (c *Client) handleTxnRequest(txnID string, req kmsg.Request) (kmsg.Response, error) {
	coordinator, err := c.loadCoordinator(coordinatorKey{
		name: txnID,
		typ:  coordinatorTypeTxn,
	})
	if err != nil {
		return nil, err
	}
	kresp, err := coordinator.waitResp(req)
	if err != nil {
		return kresp, err
	}

	var errCode int16
	switch t := kresp.(type) {
	case *kmsg.InitProducerIDResponse:
		errCode = t.ErrorCode
	case *kmsg.AddPartitionsToTxnResponse:
		if len(t.Errors) > 0 {
			if len(t.Errors[0].PartitionErrors) > 0 {
				errCode = t.Errors[0].PartitionErrors[0].ErrorCode
			}
		}
	case *kmsg.AddOffsetsToTxnResponse:
		errCode = t.ErrorCode
	case *kmsg.EndTxnResponse:
		errCode = t.ErrorCode
	case *kmsg.TxnOffsetCommitResponse:
		if len(t.Topics) > 0 {
			if len(t.Topics[0].Partitions) > 0 {
				errCode = t.Topics[0].Partitions[0].ErrorCode
			}
		}
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
func (c *Client) Broker(id int) *Broker {
	return &Broker{
		id: int32(id),
		cl: c,
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
func (b *Broker) Request(req kmsg.Request) (kmsg.Response, error) {
	b.cl.brokersMu.RLock()
	br, exists := b.cl.brokers[b.id]
	b.cl.brokersMu.RUnlock()

	if !exists {
		// If the broker does not exist, we try once to update brokers.
		if err := b.cl.fetchBrokerMetadata(); err == nil {
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

	return br.waitResp(req)
}
