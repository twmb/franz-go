package kgo

import (
	"errors"
	"time"

	"github.com/twmb/kgo/kerr"
	"github.com/twmb/kgo/kmsg"
)

// TODO strengthen errors

// GroupOpt is an option to configure group consuming.
type GroupOpt interface {
	apply(*consumerGroup)
}

// groupOpt implements GroupOpt.
type groupOpt struct {
	fn func(cfg *consumerGroup)
}

func (opt groupOpt) apply(cfg *consumerGroup) { opt.fn(cfg) }

// GroupTopics adds topics to use for group consuming.
func GroupTopics(topics ...string) GroupOpt {
	return groupOpt{func(cfg *consumerGroup) { cfg.topics = append(cfg.topics, topics...) }}
}

// GroupBalancers sets the balancer to use for dividing topic partitions
// among group members, overriding the defaults.
//
// The current default is [sticky, roundrobin, range].
//
// For balancing, Kafka chooses the first protocol that all group members agree
// to support.
func GroupBalancers(balancers ...GroupBalancer) GroupOpt {
	return groupOpt{func(cfg *consumerGroup) { cfg.balancers = balancers }}
}

// GroupSessionTimeout sets how long a member the group can go between
// heartbeats, overriding the default 10,000ms. If a member does not heartbeat
// in this timeout, the broker will remove the member from the group and
// initiate a rebalance.
//
// This corresponds to Kafka's session.timeout.ms setting and must be within
// the broker's group.min.session.timeout.ms and group.max.session.timeout.ms.
func GroupSessionTimeout(timeout time.Duration) GroupOpt {
	return groupOpt{func(cfg *consumerGroup) { cfg.sessionTimeoutMS = int32(timeout.Milliseconds()) }}
}

// GroupRebalanceTimeout sets how long group members are allowed to take
// when a JoinGroup is initiated (i.e., a rebalance has begun), overriding the
// default 60,000ms. This is essentially how long all members are allowed to
// complete work and commit offsets.
//
// Kafka uses the largest rebalance timeout of all members in the group. If a
// member does not rejoin within this timeout, Kafka will kick that member from
// the group.
//
// This corresponds to Kafka's rebalance.timeout.ms.
func GroupRebalanceTimeout(timeout time.Duration) GroupOpt {
	return groupOpt{func(cfg *consumerGroup) { cfg.rebalanceTimeoutMS = int32(timeout.Milliseconds()) }}
}

// GroupHeartbeatInterval sets how long a group member goes between
// heartbeats to Kafka, overriding the default 3,000ms.
//
// Kafka uses heartbeats to ensure that a group member's session stays active.
// This value can be any value lower than the session timeout, but should be no
// higher than 1/3rd the session timeout.
//
// This corresponds to Kafka's heartbeat.interval.ms.
func GroupHeartbeatInterval(interval time.Duration) GroupOpt {
	return groupOpt{func(cfg *consumerGroup) { cfg.heartbeatIntervalMS = int32(interval.Milliseconds()) }}
}

// AssignGroup assigns a group to consume from, overriding any prior
// assignment. To leave a group, you can AssignGroup with an empty group.
func (c *Client) AssignGroup(group string, opts ...GroupOpt) {
	consumer := &c.consumer
	consumer.mu.Lock()
	defer consumer.mu.Unlock()

	// TODO:
	// empty: leave
	// non-empty, same: do nothing
	// non-empty, different: leave, switch group
	// 	if group == "" {
	//		return errors.New("invalid empty group name")
	//	}
	//	if consumer.group.id != "" {
	//		return errors.New("client already has a group")
	//	}
	consumer.group = consumerGroup{
		id: group,
		// topics from opts
		balancers: []GroupBalancer{
			StickyBalancer(),
			RoundRobinBalancer(),
			RangeBalancer(),
		},

		sessionTimeoutMS:    10000,
		rebalanceTimeoutMS:  60000,
		heartbeatIntervalMS: 3000,
	}
	for _, opt := range opts {
		opt.apply(&consumer.group)
	}

	// Ensure all topics exist so that we will fetch their metadata.
	c.topicsMu.Lock()
	clientTopics := c.cloneTopics()
	for _, topic := range c.consumer.group.topics {
		if _, exists := clientTopics[topic]; !exists {
			clientTopics[topic] = newTopicPartitions()
		}
	}
	c.topics.Store(clientTopics)
	c.topicsMu.Unlock()

	go c.consumeGroup()
}

type (
	consumerGroup struct {
		id        string
		topics    []string
		balancers []GroupBalancer

		memberID   string
		generation int32
		assigned   map[string][]int32

		sessionTimeoutMS    int32
		rebalanceTimeoutMS  int32
		heartbeatIntervalMS int32

		// TODO autocommit
		// OnAssign
		// OnRevoke
		// OnLost (incremental)
	}
)

func (c *Client) consumeGroup() {
	var consecutiveErrors int
loop:
	for {
		err := c.consumer.joinAndSync()
		if err == nil {
			err = c.consumer.fetchOffsets()
			if err == nil {
				err = c.heartbeat()
				if err == kerr.RebalanceInProgress {
					continue
				}
			}
		}
		if err != nil {
			consecutiveErrors++
			select {
			case <-c.closedCh:
				return
			case <-time.After(c.cfg.client.retryBackoff(consecutiveErrors)):
				continue loop
			}
		}
		consecutiveErrors = 0
	}
}

func (c *Client) heartbeat() error {
	interval := time.Millisecond * time.Duration(c.consumer.group.heartbeatIntervalMS)
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
		case <-c.closedCh:
			return errors.New("client closed")
		}

		req := &kmsg.HeartbeatRequest{
			GroupID:      c.consumer.group.id,
			GenerationID: c.consumer.group.generation,
			MemberID:     c.consumer.group.memberID,
		}
		kresp, err := c.Request(req)
		if err != nil {
			return err
		}
		resp := kresp.(*kmsg.HeartbeatResponse)
		if err = kerr.ErrorForCode(resp.ErrorCode); err != nil {
			return err
		}
	}
}

func (c *consumer) joinAndSync() error {
	c.client.waitmeta()

start:
	var memberID string
	req := kmsg.JoinGroupRequest{
		GroupID:          c.group.id,
		SessionTimeout:   c.group.sessionTimeoutMS,
		RebalanceTimeout: c.group.rebalanceTimeoutMS,
		ProtocolType:     "consumer",
		MemberID:         memberID,
		GroupProtocols:   c.joinGroupProtocols(),
	}
	kresp, err := c.client.Request(&req)
	if err != nil {
		return err
	}
	resp := kresp.(*kmsg.JoinGroupResponse)

	if err = kerr.ErrorForCode(resp.ErrorCode); err != nil {
		if err == kerr.MemberIDRequired {
			memberID = resp.MemberID // KIP-394
			goto start
		}
		return err // Request retries as necesary, so this must be a failure
	}

	c.group.memberID = resp.MemberID
	c.group.generation = resp.GenerationID

	var plan balancePlan
	if resp.LeaderID == resp.MemberID {
		plan, err = c.balanceGroup(resp.GroupProtocol, resp.Members)
		if err != nil {
			return err
		}
	}

	if err = c.syncGroup(plan, resp.GenerationID); err != nil {
		if err == kerr.RebalanceInProgress {
			goto start
		}
		return err
	}

	return nil
}

func (c *consumer) syncGroup(plan balancePlan, generation int32) error {
	req := kmsg.SyncGroupRequest{
		GroupID:         c.group.id,
		GenerationID:    generation,
		MemberID:        c.group.memberID,
		GroupAssignment: plan.intoAssignment(),
	}
	kresp, err := c.client.Request(&req)
	if err != nil {
		return err // Request retries as necesary, so this must be a failure
	}
	resp := kresp.(*kmsg.SyncGroupResponse)

	kassignment := new(kmsg.GroupMemberAssignment)
	if err = kassignment.ReadFrom(resp.MemberAssignment); err != nil {
		return err
	}

	c.group.assigned = make(map[string][]int32)
	for _, topic := range kassignment.Topics {
		c.group.assigned[topic.Topic] = topic.Partitions
	}

	return nil
}

func (c *consumer) joinGroupProtocols() []kmsg.JoinGroupRequestGroupProtocol {
	var protos []kmsg.JoinGroupRequestGroupProtocol
	for _, balancer := range c.group.balancers {
		protos = append(protos, kmsg.JoinGroupRequestGroupProtocol{
			ProtocolName: balancer.protocolName(),
			ProtocolMetadata: balancer.metaFor(
				c.group.topics,
				c.group.assigned,
				c.group.generation,
			),
		})
	}
	return protos
}

func (c *consumer) fetchOffsets() error {
	req := kmsg.OffsetFetchRequest{
		GroupID: c.group.id,
	}
	for topic, partitions := range c.group.assigned {
		req.Topics = append(req.Topics, kmsg.OffsetFetchRequestTopic{
			Topic:      topic,
			Partitions: partitions,
		})
	}
	kresp, err := c.client.Request(&req)
	if err != nil {
		return err
	}
	resp := kresp.(*kmsg.OffsetFetchResponse)
	errCode := resp.ErrorCode
	if resp.Version < 2 && len(resp.Responses) > 0 && len(resp.Responses[0].PartitionResponses) > 0 {
		errCode = resp.Responses[0].PartitionResponses[0].ErrorCode
	}
	if err = kerr.ErrorForCode(errCode); err != nil && !kerr.IsRetriable(err) {
		return err
	}

	// TODO KIP-320
	offsets := make(map[string]map[int32]Offset)
	for _, response := range resp.Responses {
		topicOffsets := make(map[int32]Offset)
		offsets[response.Topic] = topicOffsets
		for _, partitionResponse := range response.PartitionResponses {
			if partitionResponse.ErrorCode != 0 {
				return kerr.ErrorForCode(partitionResponse.ErrorCode)
			}
			topicOffsets[partitionResponse.Partition] = ConsumeExactOffset(partitionResponse.Offset)
		}
	}

	c.mu.Lock()
	c.assignPartitions(offsets, true)
	c.resetAndLoadOffsets()
	c.mu.Unlock()
	return nil
}
