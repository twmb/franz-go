package kgo

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/kgo/kmsg"
	"golang.org/x/exp/rand"
)

type partitions struct {
	when     time.Time
	all      []int32
	writable []int32
}

type Client struct {
	cfg cfg

	rngMu sync.Mutex
	rng   *rand.Rand

	seedBrokers []string

	brokersMu      sync.Mutex
	untriedBrokers map[int32]*broker // unopened brokers
	brokers        map[int32]*broker // opened brokers

	// seedBroker is the broker we use until a metadata request is issued.
	// Once we have broker IDs, we close the unknown-id seed broker.
	seedBroker *broker

	topicPartsMu sync.Mutex   // guards writes
	topicParts   atomic.Value // map[string]partitions
}

// Brokers: always tick at fastest frequency of all topics sending to that broker
// Brokers: each topic requires its own ProduceRequest

func (c *Client) allPartitionsForTopic(topic string) ([]int32, error) {
	parts, err := c.partitionsForTopic(topic)
	if err != nil {
		return nil, err
	}
	return parts.all, nil
}

func (c *Client) writablePartitionsForTopic(topic string) ([]int32, error) {
	parts, err := c.partitionsForTopic(topic)
	if err != nil {
		return nil, err
	}
	return parts.writable, nil
}

func (c *Client) partitionsForTopic(topic string) (partitions, error) {
	topicParts := c.topicParts.Load().(map[string]partitions)
	parts, exists := topicParts[topic]

	if !exists {
		if err := c.fetchMetadataForTopic(topic); err != nil {
			return partitions{}, err
		}
		topicParts = c.topicParts.Load().(map[string]partitions)
		if parts, exists = topicParts[topic]; !exists {
			return partitions{}, errors.New("TODO") // TODO
		}
	}

	return parts, nil
}

func (c *Client) closeBroker(b *broker) {
	b.brokersMu.Lock()
	defer b.brokersMu.Unlock()

	b.dieOnce()
	if b == c.seedBroker {
		c.seedBroker = nil
		return
	}
	delete(b.brokers, id)
}

func (c *Client) tryUntriedSeedBroker(addr string) error {
	broker := &broker{cl: c, addr: addr}
	if err := broker.connect(); err != nil {
		return err
	}
	c.seedBroker = c
	return nil
}

func (c *Client) tryUntriedBroker(id int32, broker *broker) error {
	delete(c.untriedBrokers, id)
	if err := broker.connect(); err != nil {
		return err
	}
	c.brokers[id] = broker
	return nil
}

// broker returns a random broker from the set of known live brokers,
// attempting to create a connection to a broker if none are live.
func (c *Client) broker() (*broker, error) {
	c.brokersMu.Lock()
	defer c.brokersMu.Unlock()

	var err error
	if len(c.brokers) == 0 && c.seedBroker != nil {
		// Try all of our ID-known untried brokers.
		for id, broker := range c.untriedBrokers {
			if err = c.tryUntriedBroker(id, broker); err != nil {
				continue
			}
			break
		}

		// Otherwise, try a permutation of the seeds until one opens.
		if len(c.brokers) == 0 {
			seeds := append([]string(nil), c.seedBrokers...)
			c.rngMu.Lock()
			c.rng.Shuffle(len(seeds), func(i, j int) { seeds[i], seeds[j] = seeds[j], seeds[i] })
			c.rngMu.Unlock()

			for len(seeds) > 0 {
				if err = c.tryUntriedSeedBroker(); err == nil {
					break
				}
			}
		}
	}

	if err != nil {
		return nil, err
	}

	// With no err, either an untried broker or a seed broker opened.
	var b *broker
	for _, broker := range c.brokers {
		b = broker
		break
	}
	if b == nil {
		b = c.seedBroker
	}
	return b, nil
}

func (c *Client) fetchMetadataForTopic(topic string) error {
	broker, err := c.broker()
	if err != nil {
		return err
	}
	broker.wait(
		new(kmsg.MetadataRequest),
		func(resp kmsg.Response, respErr error) {
			if err = respErr; err != nil {
				c.closeBroker(broker)
				// error in write / read: TODO dereg broker
				return
			}
			meta := resp.(*kmsg.MetadataResponse)
			c.parseMetadata(meta)

		},
	)
	return err
}

func (c *Client) parseMetadata(meta *kmsg.MetadataResponse) {
	c.brokersMu.Lock()
	defer c.brokersMu.Unlock()

	for _, broker := range c.Brokers {
		// id, host, port
	}

	// register all brokers
	// TODO save controller ID (broker #) for admin client
	// over all topics,
	// check topic err...
	// save partition, unless leader not available
	_ = meta
}
