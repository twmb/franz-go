package kgo

import (
	"fmt"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/exp/rand"

	"github.com/twmb/kgo/kerr"
	"github.com/twmb/kgo/kmsg"
)

type topicPartition struct {
	topic     string // our topic
	partition int32  // our partition number

	leader      int32 // our current broker leader
	leaderEpoch int32 // our current broker leader epoch

	toppar topparBuffer

	writable bool

	replicas []int32
	isr      []int32
	offline  []int32
}

type topicPartitions struct {
	loaded  int64
	loadErr error

	mu sync.RWMutex

	// allIDs and writableIDs correspond to the partition IDs in the
	// two slices below. We save the IDs here as well since that is
	// the common want.
	allIDs      []int32
	writableIDs []int32

	all      map[int32]*topicPartition // partition num => partition
	writable map[int32]*topicPartition // partition num => partition, eliding partitions with no leader

	loading chan struct{}
}

func (p *topicPartitions) loadComplete() {
	atomic.StoreInt64(&p.loaded, 1)
	close(p.loading)
}

// Client issues requests and handles responses to a Kafka cluster.
type Client struct {
	cfg cfg

	rng *rand.Rand

	brokersMu    sync.RWMutex
	brokers      map[int32]*broker // broker id => broker
	anyBroker    []*broker
	anyBrokerIdx int

	controllerID int32 // atomic

	producerID       int64
	producerEpoch    int16
	producerIDLoaded int64
	producerIDMu     sync.Mutex

	produceMu sync.RWMutex

	topicPartsMu sync.RWMutex
	topicParts   map[string]*topicPartitions
}

// newTopicParts creates and returns new topicPartitions
func newTopicParts() *topicPartitions {
	parts := &topicPartitions{
		loading:  make(chan struct{}),
		all:      make(map[int32]*topicPartition),
		writable: make(map[int32]*topicPartition),
	}
	return parts
}

func (c *Client) partitionsForTopic(topic string) (*topicPartitions, error) {
	var retries int
start:
	c.topicPartsMu.RLock()
	parts, exists := c.topicParts[topic]
	c.topicPartsMu.RUnlock()

	if !exists {
		c.topicPartsMu.Lock()
		parts, exists = c.topicParts[topic]
		if !exists {
			parts = newTopicParts()
			c.topicParts[topic] = parts
			go c.fetchTopicMetadataIntoParts(parts, topic, true)
		}
		c.topicPartsMu.Unlock()
	}

	if atomic.LoadInt64(&parts.loaded) == 0 {
		<-parts.loading
	}

	if parts.loadErr != nil && isRetriableErr(parts.loadErr) {
		c.topicPartsMu.Lock()
		partsNow := c.topicParts[topic]
		if partsNow == parts {
			delete(c.topicParts, topic)
		}
		c.topicPartsMu.Unlock()

		if retries < 3 { // TODO config opt
			fmt.Println("sleeping 1s before topic partition retry")
			time.Sleep(time.Second)
			goto start
		}
	}

	return parts, parts.loadErr
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
	meta, err := c.fetchMetadata()
	if err != nil {
		return err
	}
	if meta.ControllerID > 0 {
		atomic.StoreInt32(&c.controllerID, meta.ControllerID)
	}
	c.updateBrokers(meta.Brokers)
	return nil
}

func (c *Client) fetchTopicMetadataIntoParts(parts *topicPartitions, topic string, addToBroker bool) {
	defer parts.loadComplete()

	var meta *kmsg.MetadataResponse
	meta, parts.loadErr = c.fetchMetadata(topic)
	if parts.loadErr != nil {
		return
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
		parts.loadErr = ErrInvalidResp
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

		c.brokersMu.RLock()
		broker, exists := c.brokers[partMeta.Leader]
		c.brokersMu.RUnlock()

		if !exists {
			parts.loadErr = errUnknownBrokerForLeader
			return
		}

		p := &topicPartition{
			topic:     topic,
			partition: partMeta.Partition,

			leader:      partMeta.Leader,
			leaderEpoch: partMeta.LeaderEpoch,

			toppar: topparBuffer{
				idWireLength: 2 + int32(len(topic)) + 4,
				drainer:      broker.bt,
			},

			replicas: partMeta.Replicas,
			isr:      partMeta.ISR,
			offline:  partMeta.OfflineReplicas,
		}
		p.toppar.owner = p

		if addToBroker {
			broker.bt.addToppar(&p.toppar)
		}

		parts.allIDs = append(parts.allIDs, p.partition)
		parts.all[p.partition] = p

		switch kerr.ErrorForCode(partMeta.ErrorCode) {
		case kerr.LeaderNotAvailable,
			kerr.ListenerNotFound:
			continue
		}

		p.writable = true
		parts.writableIDs = append(parts.writableIDs, p.partition)
		parts.writable[p.partition] = p
	}
}

func (c *Client) fetchMetadata(topics ...string) (*kmsg.MetadataResponse, error) {
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
	return meta, err
}

// updateBrokers is called with the broker portion of every metadata response.
// All metadata responses contain all known live brokers, so we can always
// use the response.
func (c *Client) updateBrokers(brokers []kmsg.MetadataResponseBrokers) {
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

func (c *Client) Request(req kmsg.Request) (kmsg.Response, error) {
	broker := c.broker()

	var resp kmsg.Response
	var err error
	broker.wait(req, func(kresp kmsg.Response, kerr error) {
		resp, err = kresp, kerr
	})
	return resp, err
}

// Admin issues an admin request to the controller broker, waiting for and
// returning the Kafka response or an error.
//
// If the controller ID is unknown, this will attempt to fetch it. If the
// fetch errors, this will return an unknown controller error.
func (c *Client) Admin(req kmsg.AdminRequest) (kmsg.Response, error) {
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

	var resp kmsg.Response
	var err error
	controller.wait(req, func(kresp kmsg.Response, kerr error) {
		resp, err = kresp, kerr
	})
	return resp, err
}
