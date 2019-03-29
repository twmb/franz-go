package kgo

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"

	"golang.org/x/exp/rand"

	"github.com/twmb/kgo/kerr"
	"github.com/twmb/kgo/kmsg"
)

type partition struct {
	id          int32
	leader      int32
	leaderEpoch int32
	replicas    []int32
	isr         []int32
	offline     []int32
}

type partitions struct {
	loaded  int64
	loadErr error

	// allIDs and writableIDs correspond to the partition IDs in the
	// two slices below. We save the IDs here as well since that is
	// the common want.
	allIDs      []int32
	writableIDs []int32

	all      map[int32]*partition // id => partition
	writable map[int32]*partition // id => partition, eliding partitions with no leader

	loading chan struct{}
}

// Client issues requests and handles responses to a Kafka cluster.
type Client struct {
	cfg cfg

	rng *rand.Rand

	brokersMu    sync.RWMutex
	brokers      map[int32]*broker // broker id => broker
	anyBroker    []*broker         // TODO multiArmBandit anyBroker
	anyBrokerIdx int

	// TODO can add lastReq to broker and a daily ticker to clean up
	// gone brokers

	controllerID int32 // atomic

	topicPartsMu sync.RWMutex
	topicParts   map[string]*partitions // topic => partitions, from metadata resp
}

func (c *Client) partitionsForTopic(topic string) (*partitions, error) {
	c.topicPartsMu.RLock()
	parts, exists := c.topicParts[topic]
	c.topicPartsMu.RUnlock()

	if !exists {
		c.topicPartsMu.Lock()
		parts, exists = c.topicParts[topic]
		if !exists {
			parts = &partitions{
				loading:  make(chan struct{}),
				all:      make(map[int32]*partition),
				writable: make(map[int32]*partition),
			}
			c.topicParts[topic] = parts
			go c.fetchTopicMetadata(parts, topic)
		}
		c.topicPartsMu.Unlock()
	}

	if atomic.LoadInt64(&parts.loaded) == 0 {
		<-parts.loading
	}

	// TODO retriable
	// -- add kerr.Retriable(error)
	// -- no to auth fail, yes to all else

	return parts, parts.loadErr
}

// broker returns a random broker from all brokers ever known.
func (c *Client) broker() *broker {
	c.brokersMu.RLock()
	defer c.brokersMu.RUnlock()

	b := c.anyBroker[c.anyBrokerIdx]
	c.anyBrokerIdx++
	if c.anyBrokerIdx == len(c.anyBroker) {
		c.anyBrokerIdx = 0
		c.rng.Shuffle(len(c.anyBroker), func(i, j int) { c.anyBroker[i], c.anyBroker[j] = c.anyBroker[j], c.anyBroker[i] })
	}
	return b
}

// fetchBrokerMetadata issues a metadata request solely for broker information.
// TODO: retriable
func (c *Client) fetchBrokerMetadata() error {
	broker := c.broker()
	var meta *kmsg.MetadataResponse
	var err error
	broker.wait(
		new(kmsg.MetadataRequest),
		func(resp kmsg.Response, respErr error) {
			if err = respErr; err != nil {
				return
			}
			meta = resp.(*kmsg.MetadataResponse)
		},
	)
	if err != nil {
		return err
	}
	if meta.ControllerID > 0 {
		atomic.StoreInt32(&c.controllerID, meta.ControllerID)
	}
	c.updateBrokers(meta.Brokers)
	return nil
}

// fetchTopicMetadata fetches metadata for a topic, storing results into parts.
//
// Since metadata requests always return all live brokers and the controller
// ID, this additionally updates the client's known brokers and controller ID.
func (c *Client) fetchTopicMetadata(parts *partitions, topic string) {
	defer atomic.StoreInt64(&parts.loaded, 1)
	defer close(parts.loading)

	broker := c.broker()

	var meta *kmsg.MetadataResponse
	broker.wait(
		&kmsg.MetadataRequest{
			Topics:                 []string{topic},
			AllowAutoTopicCreation: c.cfg.producer.allowAutoTopicCreation,
		},
		func(resp kmsg.Response, respErr error) {
			if parts.loadErr = respErr; parts.loadErr != nil {
				return
			}
			meta = resp.(*kmsg.MetadataResponse)
		},
	)

	if parts.loadErr != nil {
		return // TODO error in read/write, retry
	}

	// Update the controller ID and brokers now since they are always
	// included in metadata responses.
	if meta.ControllerID > 0 {
		atomic.StoreInt32(&c.controllerID, meta.ControllerID)
	}
	c.updateBrokers(meta.Brokers)

	// Since we requested one topic, we expect one topic metadata
	// and the topic should match.
	if len(meta.TopicMetadata) != 1 || meta.TopicMetadata[0].Topic != topic {
		parts.loadErr = fmt.Errorf("kafka did not reply to topic %s in metadata request", topic)
		return
	}
	t := meta.TopicMetadata[0]
	parts.loadErr = kerr.ErrorForCode(t.ErrorCode)
	if parts.loadErr != nil {
		return
	}

	// Finally, update the topic's partition metadata.
	for i := range t.PartitionMetadata {
		partMeta := &t.PartitionMetadata[i]

		p := &partition{
			id:          partMeta.Partition,
			leader:      partMeta.Leader,
			leaderEpoch: partMeta.LeaderEpoch,
			replicas:    partMeta.Replicas,
			isr:         partMeta.ISR,
			offline:     partMeta.OfflineReplicas,
		}

		parts.allIDs = append(parts.allIDs, p.id)
		parts.all[p.id] = p

		switch kerr.ErrorForCode(partMeta.ErrorCode) {
		case kerr.LeaderNotAvailable,
			kerr.ListenerNotFound:
			continue
		}

		parts.writableIDs = append(parts.writableIDs, p.id)
		parts.writable[p.id] = p
	}
}

// updateBrokers is called with the broker portion of every metadata response.
// All metadata responses contain all known live brokers, so we can always
// use the response.
func (c *Client) updateBrokers(brokers []kmsg.MetadataResponseBrokers) {
	c.brokersMu.Lock()
	defer c.brokersMu.Unlock()

	addrChanged := false
	for _, broker := range brokers {
		addr := net.JoinHostPort(broker.Host, strconv.Itoa(int(broker.Port)))

		b, exists := c.brokers[broker.NodeID]
		if exists { // exists, but addr changed: migrate
			if b.addr != addr {
				b.stopForever()
				c.brokers[broker.NodeID] = c.newBroker(addr, broker.NodeID)
				addrChanged = true
			}
		} else { // does not exist: make new
			b = c.newBroker(addr, broker.NodeID)
			c.brokers[broker.NodeID] = b
			c.anyBroker = append(c.anyBroker, b)
		}
	}

	if addrChanged { // if any addr changed, we need to update the pointers in anyBrokers
		c.anyBroker = make([]*broker, 0, len(c.brokers))
		for _, broker := range c.brokers {
			c.anyBroker = append(c.anyBroker, broker)
		}
		c.anyBrokerIdx = 0
	}
}

// Admin issues an admin request to the controller broker, waiting for and
// returning the Kafka response or an error.
//
// If the controller ID is unknown, this will attempt to fetch it. If the
// fetch errors, this will return an unknown controller error.
func (c *Client) Admin(req kmsg.AdminRequest) (kmsg.Response, error) {
	if c.controllerID < 0 {
		if err := c.fetchBrokerMetadata(); err != nil {
			return nil, err
		}
		if c.controllerID < 0 {
			return nil, errUnknownController
		}
	}

	c.brokersMu.Lock()
	controller, exists := c.brokers[c.controllerID]
	c.brokersMu.Unlock()

	if !exists {
		return nil, errUnknownController
	}

	var resp kmsg.Response
	var err error
	controller.wait(req, func(kresp kmsg.Response, kerr error) {
		resp, err = kresp, kerr
	})
	return resp, err
}
