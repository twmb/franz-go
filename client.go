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

func (p *partitions) loadComplete() {
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

	topicPartsMu sync.RWMutex
	topicParts   map[string]*partitions // topic => partitions, from metadata resp
}

func (c *Client) partitionsForTopic(topic string) (*partitions, error) {
	var retries int
start:
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

	if parts.loadErr != nil && errIsRetriable(parts.loadErr) {
		c.topicPartsMu.Lock()
		partsNow := c.topicParts[topic]
		if partsNow == parts {
			delete(c.topicParts, topic)
		}
		c.topicPartsMu.Unlock()

		if retries < 3 { // TODO config opt
			fmt.Println("sleeping before topic partition retry")
			time.Sleep(time.Second)
			goto start
		}
	}

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
	defer parts.loadComplete()

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

// Admin issues an admin request to the controller broker, waiting for and
// returning the Kafka response or an error.
//
// If the controller ID is unknown, this will attempt to fetch it. If the
// fetch errors, this will return an unknown controller error.
func (c *Client) Admin(req kmsg.AdminRequest) (kmsg.Response, error) {
start:
	if c.controllerID < 0 {
		if err := c.fetchBrokerMetadata(); err != nil {
			if isRetriable(err) && retries < 3 {
				retries++
				time.Sleep(time.Second) // TODO make better
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
