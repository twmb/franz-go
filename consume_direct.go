package kgo

import (
	"regexp"
)

// ConsumeOpt is an option to configure direct topic / partition consuming.
type ConsumeOpt interface {
	apply(*consumerDirect)
}

type directConsumeOpt struct {
	fn func(cfg *consumerDirect)
}

func (opt directConsumeOpt) apply(cfg *consumerDirect) { opt.fn(cfg) }

// ConsumeTopics sets topics to consume directly and the offsets to start
// consuming partitions from in those topics.
//
// If a metadata update sees partitions added to a topic, the client will
// automatically begin consuming from those new partitions.
func ConsumeTopics(offset Offset, topics ...string) ConsumeOpt {
	return directConsumeOpt{func(cfg *consumerDirect) {
		cfg.topics = make(map[string]Offset, len(topics))
		for _, topic := range topics {
			cfg.topics[topic] = offset
		}
	}}
}

// ConsumePartitions sets partitions to consume from directly and the offsets
// to start consuming those partitions from.
//
// Offsets from option have higher precedence than ConsumeTopics. If a topic's
// partition is set in this option and that topic is also set in ConsumeTopics,
// offsets on partitions in this option are used in favor of the more general
// topic offset from ConsumeTopics.
func ConsumePartitions(partitions map[string]map[int32]Offset) ConsumeOpt {
	return directConsumeOpt{func(cfg *consumerDirect) { cfg.partitions = partitions }}
}

// ConsumeTopicsRegex sets all topics in ConsumeTopics to be parsed as regular
// expressions.
func ConsumeTopicsRegex() ConsumeOpt {
	return directConsumeOpt{func(cfg *consumerDirect) { cfg.regexTopics = true }}
}

type consumerDirect struct {
	topics     map[string]Offset
	partitions map[string]map[int32]Offset

	regexTopics bool
	reTopics    map[string]Offset
	reIgnore    map[string]struct{}

	using map[string]map[int32]struct{}
}

// This takes ownership of the assignments.
func (cl *Client) AssignPartitions(opts ...ConsumeOpt) {
	c := &cl.consumer
	c.mu.Lock()
	defer c.mu.Unlock()

	c.assignPartitions(nil, true) // invalidate old assignments
	c.direct = consumerDirect{
		topics:     make(map[string]Offset),
		partitions: make(map[string]map[int32]Offset),
		reTopics:   make(map[string]Offset),
		reIgnore:   make(map[string]struct{}),
		using:      make(map[string]map[int32]struct{}),
	}

	for _, opt := range opts {
		opt.apply(&c.direct)
	}

	if c.typ == consumerTypeGroup {
		// TODO leave group
	}
	c.typ = consumerTypeDirect
	if len(c.direct.topics) == 0 && len(c.direct.partitions) == 0 {
		c.typ = consumerTypeUnset
		return
	}

	if !c.direct.regexTopics {
		ensureTopics := make(map[string]struct{})
		for topic := range c.direct.topics {
			ensureTopics[topic] = struct{}{}
		}
		for topic := range c.direct.partitions {
			ensureTopics[topic] = struct{}{}
		}
		cl.topicsMu.Lock()
		clientTopics := cl.cloneTopics()
		for topic := range ensureTopics {
			if _, exists := clientTopics[topic]; !exists {
				clientTopics[topic] = newTopicPartitions()
			}
		}
		cl.topics.Store(clientTopics)
		cl.topicsMu.Unlock()
	}

	cl.triggerUpdateMetadata()
}

// requires consumer lock
// and then client topics lock in assign
func (c *consumer) onMetadataUpdateDirect() {
	d := &c.direct

	// First, we build everything we could theoretically want to consume.
	toUse := make(map[string]map[int32]Offset, 10)
	for topic, topicPartitions := range c.client.loadTopics() {
		var useTopic bool
		var useOffset Offset

		// If we are using regex topics, we have to check all
		// topic regexes to see if any match on this topic.
		if d.regexTopics {
			// If we have already matched this topic prior,
			// we do not need to check all regexes.
			if offset, exists := d.reTopics[topic]; exists {
				useTopic = true
				useOffset = offset
			} else if _, exists := d.reIgnore[topic]; exists {
				// skip
			} else {
				for reTopic, offset := range d.topics {
					if match, _ := regexp.MatchString(reTopic, topic); match {
						useOffset = offset
						useTopic = true
						d.reTopics[topic] = offset
						break
					}
				}
				if !useTopic {
					d.reIgnore[topic] = struct{}{}
				}
			}

		} else {
			// If we are not using regex, we can just lookup.
			useOffset, useTopic = d.topics[topic]
		}

		// If the above detected that we want to keep this topic, we
		// set all partitions as usable.
		if useTopic {
			partitions := topicPartitions.load()
			toUseTopic := make(map[int32]Offset, len(partitions.partitions))
			for _, partition := range partitions.partitions {
				toUseTopic[partition] = useOffset
			}
			toUse[topic] = toUseTopic
		}

		// Lastly, if this topic has some specific partitions pinned,
		// we set those.
		for partition, offset := range d.partitions[topic] {
			toUseTopic, exists := toUse[topic]
			if !exists {
				toUseTopic = make(map[int32]Offset, 10)
				toUse[topic] = toUseTopic
			}
			toUseTopic[partition] = offset
		}
	}

	// With everything we want to consume, remove what we are already.
	for topic, partitions := range d.using {
		toUseTopic, exists := toUse[topic]
		if !exists {
			continue // weird; TODO forgotten topic
		}
		if len(partitions) == len(toUseTopic) {
			delete(toUse, topic)
			continue
		}
		for partition := range partitions {
			delete(toUseTopic, partition)
		}
	}

	if len(toUse) == 0 {
		return
	}

	// Finally, toUse contains new partitions that we must consume.
	// Add them to our using map and assign them.
	for topic, partitions := range toUse {
		topicUsing, exists := d.using[topic]
		if !exists {
			topicUsing = make(map[int32]struct{})
			d.using[topic] = topicUsing
		}
		for partition := range partitions {
			topicUsing[partition] = struct{}{}
		}
	}
	c.assignPartitions(toUse, false)
}
