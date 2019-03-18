package kgo

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/exp/rand"
)

type version int8

const (
	v0_0_11_0 version = iota
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
	untriedSeeds   []string
	untriedBrokers map[int32]*broker  // unopened brokers
	triedBrokers   map[int32]struct{} // dead broker IDs
	brokers        []*broker          // opened brokers

	topicPartsMu sync.Mutex   // guards writes
	topicParts   atomic.Value // map[string]partitions
}

// Brokers: always tick at fastest frequency of all topics sending to that broker
// Brokers: each topic requires its own ProduceRequest

func (c *Client) allPartitionsFor(topic string) ([]int32, error) {
	parts, err := c.partitionsFor(topic)
	if err != nil {
		return nil, err
	}
	return parts.all, nil
}

func (c *Client) writablePartitionsFor(topic string) ([]int32, error) {
	parts, err := c.partitionsFor(topic)
	if err != nil {
		return nil, err
	}
	return parts.writable, nil
}

func (c *Client) partitionsFor(topic string) (partitions, error) {
	topicParts := c.topicParts.Load().(map[string]partitions)
	parts, exists := topicParts[topic]

	if !exists {
		if err := c.fetchMetadataFor(topic); err != nil {
			return partitions{}, err
		}
		topicParts = c.topicParts.Load().(map[string]partitions)
		if parts, exists = topicParts[topic]; !exists {
			return partitions{}, errors.New("TODO") // TODO
		}
	}

	return parts, nil
}

func (c *Client) resetBrokers() {
	c.untriedSeeds = append(c.untriedSeeds[:0], c.seedBrokers...)
	for id := range c.untriedBrokers { // should be no untried brokers
		delete(c.untriedBrokers, id)
	}
	for id := range c.triedBrokers {
		delete(c.triedBrokers, id)
	}
}

// tryBroker tries to open a broker, moving the addr from the untried list
// to the tried list.
func (c *Client) tryUntriedSeed() error {
	addr := c.untriedSeeds[0]
	c.untriedSeeds = c.untriedSeeds[1:]
	broker := &broker{cl: c, addr: addr}
	if err := broker.connect(); err != nil {
		return err
	}
	c.brokers = append(c.brokers, broker)
	return nil
}

func (c *Client) tryUntriedBroker(id int32, broker *broker) error {
	delete(c.untriedBrokers, id)
	c.triedBrokers[id] = struct{}{}
	if err := broker.connect(); err != nil {
		return err
	}
	c.brokers = append(c.brokers, broker)
	return nil
}

// broker returns a random broker from the set of known live brokers,
// attempting to create a connection to a broker if none are live.
func (c *Client) broker() (*broker, error) {
	c.brokersMu.Lock()
	defer c.brokersMu.Unlock()

	var err error
	// If we have no live brokers, attempt to open a new one from
	// first our untried known brokers, and then our untried seeds.
	if len(c.brokers) == 0 {
		// If we tried everything and everything died,
		// reset the seeds and begin retrying.
		if len(c.untriedSeeds) == 0 && len(c.untriedBrokers) == 0 {
			c.resetBrokers()
		}

		for id, broker := range c.untriedBrokers {
			if err = c.tryUntriedBroker(id, broker); err != nil {
				continue
			}
			break
		}

		// If we still have no brokers, try while we have untried seeds.
		if len(c.brokers) == 0 {
			for len(c.untriedSeeds) > 0 {
				if err = c.tryUntriedSeed(); err != nil {
					continue
				}
				break
			}
		}
	}

	if err != nil {
		return nil, err
	}

	if len(c.brokers) == 0 {
		// TODO: we could have, over time, connected to everything and
		// had only one untried broker. If we had a single untried
		// broker, then we will only try that one rather than resetting
		// brokers. We should have a better reset policy.
		return nil, errNoBrokers
	}

	n := 0
	if l := len(c.brokers); l > 0 {
		c.rngMu.Lock()
		n = c.rng.Intn(l)
		c.rngMu.Unlock()
	}

	return c.brokers[n], nil
}

func (c *Client) fetchMetadataFor(topic string) error {
	broker, err := c.broker()
	if err != nil {
		return err
	}
	_ = broker
	return nil
}
