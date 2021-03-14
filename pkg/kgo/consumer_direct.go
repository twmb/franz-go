package kgo

import "regexp"

// DirectConsumeOpt is an option to configure direct topic / partition consuming.
type DirectConsumeOpt interface {
	apply(*directConsumer)
}

type directConsumeOpt struct {
	fn func(cfg *directConsumer)
}

func (opt directConsumeOpt) apply(cfg *directConsumer) { opt.fn(cfg) }

// ConsumeTopics sets topics to consume directly and the offsets to start
// consuming partitions from in those topics.
//
// If a metadata update sees partitions added to a topic, the client will
// automatically begin consuming from those new partitions.
func ConsumeTopics(offset Offset, topics ...string) DirectConsumeOpt {
	return directConsumeOpt{func(cfg *directConsumer) {
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
func ConsumePartitions(partitions map[string]map[int32]Offset) DirectConsumeOpt {
	return directConsumeOpt{func(cfg *directConsumer) { cfg.partitions = partitions }}
}

// ConsumeTopicsRegex sets all topics in ConsumeTopics to be parsed as regular
// expressions.
func ConsumeTopicsRegex() DirectConsumeOpt {
	return directConsumeOpt{func(cfg *directConsumer) { cfg.regexTopics = true }}
}

type directConsumer struct {
	topics     map[string]Offset
	partitions map[string]map[int32]Offset

	regexTopics bool
	reTopics    map[string]Offset
	reIgnore    map[string]struct{}

	using map[string]map[int32]struct{}
}

// AssignPartitions assigns an exact set of partitions for the client to
// consume from. Any prior direct assignment or group assignment is
// invalidated.
//
// This takes ownership of any assignments.
func (cl *Client) AssignPartitions(opts ...DirectConsumeOpt) {
	c := &cl.consumer

	c.assignMu.Lock()
	defer c.assignMu.Unlock()

	if wasDead := c.unsetAndWait(); wasDead {
		return
	}

	d := &directConsumer{
		topics:     make(map[string]Offset),
		partitions: make(map[string]map[int32]Offset),
		reTopics:   make(map[string]Offset),
		reIgnore:   make(map[string]struct{}),
		using:      make(map[string]map[int32]struct{}),
	}
	for _, opt := range opts {
		opt.apply(d)
	}
	if len(d.topics) == 0 && len(d.partitions) == 0 || c.dead {
		return
	}

	c.storeDirect(d)

	defer cl.triggerUpdateMetadata()

	if d.regexTopics {
		return
	}

	var topics []string
	for topic := range d.topics {
		topics = append(topics, topic)
	}
	for topic := range d.partitions {
		topics = append(topics, topic)
	}
	cl.storeTopics(topics)
}

// findNewAssignments returns new partitions to consume at given offsets
// based off the current topics.
func (d *directConsumer) findNewAssignments(
	topics map[string]*topicPartitions,
) map[string]map[int32]Offset {
	// First, we build everything we could theoretically want to consume.
	toUse := make(map[string]map[int32]Offset, 10)
	for topic, topicPartitions := range topics {
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
						useTopic = true
						useOffset = offset
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
			if d.regexTopics && partitions.isInternal {
				continue
			}
			toUseTopic := make(map[int32]Offset, len(partitions.partitions))
			for partition := range partitions.partitions {
				toUseTopic[int32(partition)] = useOffset
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
			continue // forgotten topic
		}
		for partition := range partitions {
			delete(toUseTopic, partition)
		}
		if len(toUseTopic) == 0 {
			delete(toUse, topic)
		}
	}

	if len(toUse) == 0 {
		return nil
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

	return toUse
}
