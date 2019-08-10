package kgo

import (
	"errors"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/twmb/kgo/kerr"
	"github.com/twmb/kgo/kmsg"
)

// TODO strengthen errors
// TODO remove error from AssignGroup / AssignPartitions

// GroupOpt is an option to configure group consuming.
type GroupOpt interface {
	apply(*consumerGroup)
}

// groupOpt implements GroupOpt.
type groupOpt struct {
	fn func(cfg *consumerGroup)
}

func (opt groupOpt) apply(cfg *consumerGroup) { opt.fn(cfg) }

// WithGroupTopics adds topics to use for group consuming.
func WithGroupTopics(topics ...string) GroupOpt {
	return groupOpt{func(cfg *consumerGroup) { cfg.topics = append(cfg.topics, topics...) }}
}

// WithGroupBalancers sets the balancer to use for dividing topic partitions
// among group members, overriding the default two [roundrobin, range].
func WithGroupBalancers(balancers ...GroupBalancer) GroupOpt {
	return groupOpt{func(cfg *consumerGroup) { cfg.balancer = balancer }}
}

// AssignGroup assigns a group to consume from, overriding any prior
// assignment. To leave a group, you can AssignGroup with an empty group.
func (c *Client) AssignGroup(group string, opts ...GroupOpt) error {
	consumer := &c.consumer
	consumer.mu.Lock()
	defer consumer.mu.Unlock()

	if err := consumer.maybeInit(c, consumerTypeGroup); err != nil {
		return err
	}
	if group == "" {
		return errors.New("invalid empty group name")
	}
	if consumer.group.id != "" {
		return errors.New("client already has a group")
	}
	consumer.group.balancer = RangeBalancer()
	consumer.group.id = group
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

	return nil
}

type (
	consumerGroup struct {
		id       string
		topics   []string
		balancer GroupBalancer

		memberID string

		generation int32

		// TODO autocommit
		// OnAssign
		// OnRevoke
		// SessionTimeout
		// RebalanceTimeout
	}
)

func (c *Client) consumeGroup() {
	// TODO await first metadata update
	c.triggerUpdateMetadata()
	time.Sleep(time.Second)
	c.consumer.joinGroup()
}

func balancerMetadata(topics []string, userdata []byte) []byte {
	return (&kmsg.GroupMemberMetadata{
		Version:  0,
		Topics:   topics,
		UserData: userdata,
	}).AppendTo(nil)
}

func (c *consumer) joinGroup() error {
start:
	var memberID string
	req := kmsg.JoinGroupRequest{
		GroupID:          c.group.id,
		SessionTimeout:   10000, // TODO also rename to MS?
		RebalanceTimeout: 1000,
		ProtocolType:     "consumer",
		MemberID:         memberID,
		GroupProtocols: []kmsg.JoinGroupRequestGroupProtocol{
			{
				ProtocolName:     c.group.balancer.protocolName(),
				ProtocolMetadata: balancerMetadata(c.group.topics, nil),
			},
		},
	}
	kresp, err := c.client.Request(&req)
	if err != nil {
		return err
	}
	resp := kresp.(*kmsg.JoinGroupResponse)

	err = kerr.ErrorForCode(resp.ErrorCode)
	if err == kerr.MemberIDRequired {
		memberID = resp.MemberID // KIP-394
		goto start
	} else if err != nil {
		return err // TODO differentiate retriable?
	}

	c.group.memberID = resp.MemberID
	c.group.generation = resp.GenerationID

	var plan balancePlan
	if resp.LeaderID == resp.MemberID {
		// TODO take into account the protocol here, combined w/ multiple protos above
		plan, err = c.balanceGroup(resp.Members)
		if err != nil {
			return err
		}
	}

	if err = c.syncGroup(plan, resp.GenerationID); err != nil {
		return err
	}

	return nil
}

// TODO commit, leave group, member id

func (c *consumer) syncGroup(plan balancePlan, generation int32) error {
	req := kmsg.SyncGroupRequest{
		GroupID:         c.group.id,
		GenerationID:    generation,
		MemberID:        c.group.memberID,
		GroupAssignment: plan.intoAssignment(),
	}
	kresp, err := c.client.Request(&req)
	if err != nil {
		return err
	}
	resp := kresp.(*kmsg.SyncGroupResponse)
	if err != nil {
		return err // TODO differentiate retriable?
	}

	kassignment := new(kmsg.GroupMemberAssignment)
	err = kassignment.ReadFrom(resp.MemberAssignment)
	if err != nil {
		return err
	}
	spew.Dump(kassignment)
	return nil
}
