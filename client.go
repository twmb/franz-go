package kgo

import (
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/exp/rand"

	"github.com/twmb/kgo/kmsg"
)

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

	// simple consumer
	simpleConsumer simpleConsumer
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
	_, err := c.fetchMetadata(false)
	return err
}

// TODO this should favor using a seedBroker so as to not use anything blocked
// in consuming.
func (c *Client) fetchMetadata(all bool, topics ...string) (*kmsg.MetadataResponse, error) {
	if all {
		topics = nil
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

// Request issues a request to Kafka, waiting for and returning the response or
// an error.
//
// If the request is an admin request, this will issue it to the Kafka
// controller. If the controller ID is unknown, this will attempt to fetch it.
// If the fetch errors, this will return an unknown controller error.
func (c *Client) Request(req kmsg.Request) (kmsg.Response, error) {
	var broker *broker

	if _, admin := req.(kmsg.AdminRequest); admin {
		retries := 0
	start:
		if c.controllerID < 0 {
			if err := c.fetchBrokerMetadata(); err != nil {
				if isRetriableErr(err) && retries < 3 {
					retries++
					time.Sleep(c.cfg.client.retryBackoff)
					goto start
				}
				return nil, err
			}
			if c.controllerID < 0 {
				return nil, errUnknownController
			}
		}

		c.brokersMu.RLock()
		controller, exists := c.brokers[c.controllerID]
		c.brokersMu.RUnlock()

		if !exists {
			return nil, errUnknownController
		}
		broker = controller

	} else {
		broker = c.broker()
	}

	var resp kmsg.Response
	var err error
	broker.wait(req, func(kresp kmsg.Response, kerr error) {
		resp, err = kresp, kerr
	})
	return resp, err
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
// client, this returns ErrUnknownBroker.
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

	var resp kmsg.Response
	var err error
	br.wait(req, func(kresp kmsg.Response, kerr error) {
		resp, err = kresp, kerr
	})
	return resp, err
}
