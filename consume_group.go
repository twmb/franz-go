package kgo

import (
	"context"
	"errors"
	"time"

	"github.com/twmb/kgo/kerr"
	"github.com/twmb/kgo/kmsg"
)

// GroupOpt is an option to configure group consuming.
type GroupOpt interface {
	apply(*groupConsumer)
}

// groupOpt implements GroupOpt.
type groupOpt struct {
	fn func(cfg *groupConsumer)
}

func (opt groupOpt) apply(cfg *groupConsumer) { opt.fn(cfg) }

// GroupTopics adds topics to use for group consuming.
func GroupTopics(topics ...string) GroupOpt {
	return groupOpt{func(cfg *groupConsumer) {
		cfg.topics = make(map[string]struct{}, len(topics))
		for _, topic := range topics {
			cfg.topics[topic] = struct{}{}
		}
	}}
}

// GroupTopicsRegex sets all topics in GroupTopics to be parsed as regular
// expressions.
func GroupTopicsRegex() GroupOpt {
	return groupOpt{func(cfg *groupConsumer) { cfg.regexTopics = true }}
}

// GroupBalancers sets the balancer to use for dividing topic partitions
// among group members, overriding the defaults.
//
// The current default is [sticky, roundrobin, range].
//
// For balancing, Kafka chooses the first protocol that all group members agree
// to support.
func GroupBalancers(balancers ...GroupBalancer) GroupOpt {
	return groupOpt{func(cfg *groupConsumer) { cfg.balancers = balancers }}
}

// GroupSessionTimeout sets how long a member the group can go between
// heartbeats, overriding the default 10,000ms. If a member does not heartbeat
// in this timeout, the broker will remove the member from the group and
// initiate a rebalance.
//
// This corresponds to Kafka's session.timeout.ms setting and must be within
// the broker's group.min.session.timeout.ms and group.max.session.timeout.ms.
func GroupSessionTimeout(timeout time.Duration) GroupOpt {
	return groupOpt{func(cfg *groupConsumer) { cfg.sessionTimeout = timeout }}
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
	return groupOpt{func(cfg *groupConsumer) { cfg.rebalanceTimeout = timeout }}
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
	return groupOpt{func(cfg *groupConsumer) { cfg.heartbeatInterval = interval }}
}

// GroupResetOffset sets the offset to reset to when consuming a partition
// that has no commits, overriding the default start offset.
func GroupResetOffset(offset Offset) GroupOpt {
	return groupOpt{func(cfg *groupConsumer) { cfg.resetOffset = offset }}
}

type groupConsumer struct {
	c   *consumer // used to change consumer state; generally c.mu is grabbed on access
	cl  *Client   // used for running requests / adding to topics map
	seq uint64    // consumer's seq at time of Assign and after every fetch offsets

	ctx    context.Context
	cancel func()

	id        string
	topics    map[string]struct{}
	balancers []GroupBalancer
	leader    bool

	regexTopics bool
	reTopics    map[string]struct{}
	reIgnore    map[string]struct{}

	memberID   string
	generation int32
	assigned   map[string][]int32

	sessionTimeout    time.Duration
	rebalanceTimeout  time.Duration
	heartbeatInterval time.Duration

	resetOffset Offset

	// TODO autocommit
	// OnAssign
	// OnRevoke
	// OnLost (incremental)
}

// AssignGroup assigns a group to consume from, overriding any prior
// assignment. To leave a group, you can AssignGroup with an empty group.
func (cl *Client) AssignGroup(group string, opts ...GroupOpt) {
	c := &cl.consumer
	c.mu.Lock()
	defer c.mu.Unlock()

	c.unassignPrior()
	if len(group) == 0 {
		return
	}

	ctx, cancel := context.WithCancel(cl.ctx)
	g := &groupConsumer{
		c:   c,
		cl:  cl,
		seq: c.seq,

		ctx:    ctx,
		cancel: cancel,

		id: group,

		balancers: []GroupBalancer{
			StickyBalancer(),
			RoundRobinBalancer(),
			RangeBalancer(),
		},

		reTopics: make(map[string]struct{}),
		reIgnore: make(map[string]struct{}),

		sessionTimeout:    10000 * time.Millisecond,
		rebalanceTimeout:  60000 * time.Millisecond,
		heartbeatInterval: 3000 * time.Millisecond,

		resetOffset: ConsumeStartOffset(),
	}
	for _, opt := range opts {
		opt.apply(g)
	}
	if len(g.topics) == 0 {
		c.typ = consumerTypeUnset
		return
	}
	c.typ = consumerTypeGroup
	c.group = g

	// Ensure all topics exist so that we will fetch their metadata.
	if !g.regexTopics {
		cl.topicsMu.Lock()
		clientTopics := cl.cloneTopics()
		for topic := range g.topics {
			if _, exists := clientTopics[topic]; !exists {
				clientTopics[topic] = newTopicPartitions()
			}
		}
		cl.topics.Store(clientTopics)
		cl.topicsMu.Unlock()
	}

	go g.manage()
}

func (g *groupConsumer) manage() {
	var consecutiveErrors int
loop:
	for {
		err := g.joinAndSync()
		if err == nil {
			err = g.fetchOffsets()
			if err == nil {
				err = g.heartbeat()
				if err == kerr.RebalanceInProgress {
					err = nil
				}
			}
		}
		if err != nil {
			consecutiveErrors++
			select {
			case <-g.ctx.Done():
				return
			case <-time.After(g.cl.cfg.client.retryBackoff(consecutiveErrors)):
				continue loop
			}
		}
		consecutiveErrors = 0
	}
}

func (g *groupConsumer) leave() {
	g.cancel()
	g.cl.Request(g.cl.ctx, &kmsg.LeaveGroupRequest{
		GroupID:  g.id,
		MemberID: g.memberID,
		Members: []kmsg.LeaveGroupRequestMember{{
			MemberID:        g.memberID,
			GroupInstanceID: nil, // TODO KIP-345
		}},
	})
}

func (g *groupConsumer) heartbeat() error {
	ticker := time.NewTicker(g.heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
		case <-g.ctx.Done():
			return errors.New("left group or client closed")
		}

		req := &kmsg.HeartbeatRequest{
			GroupID:      g.id,
			GenerationID: g.generation,
			MemberID:     g.memberID,
		}
		kresp, err := g.cl.Request(g.ctx, req)
		if err != nil {
			return err
		}
		resp := kresp.(*kmsg.HeartbeatResponse)
		if err = kerr.ErrorForCode(resp.ErrorCode); err != nil {
			return err
		}
	}
}

func (g *groupConsumer) joinAndSync() error {
	g.cl.waitmeta()

	g.leader = false
start:
	var memberID string
	req := kmsg.JoinGroupRequest{
		GroupID:          g.id,
		SessionTimeout:   int32(g.sessionTimeout.Milliseconds()),
		RebalanceTimeout: int32(g.rebalanceTimeout.Milliseconds()),
		ProtocolType:     "consumer",
		MemberID:         memberID,
		GroupProtocols:   g.joinGroupProtocols(),
	}
	kresp, err := g.cl.Request(g.ctx, &req)
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

	g.memberID = resp.MemberID
	g.generation = resp.GenerationID

	var plan balancePlan
	if resp.LeaderID == resp.MemberID {
		plan, err = g.balanceGroup(resp.GroupProtocol, resp.Members)
		if err != nil {
			return err
		}
		g.leader = true
	}

	if err = g.syncGroup(plan, resp.GenerationID); err != nil {
		if err == kerr.RebalanceInProgress {
			goto start
		}
		return err
	}

	return nil
}

func (g *groupConsumer) syncGroup(plan balancePlan, generation int32) error {
	req := kmsg.SyncGroupRequest{
		GroupID:         g.id,
		GenerationID:    generation,
		MemberID:        g.memberID,
		GroupAssignment: plan.intoAssignment(),
	}
	kresp, err := g.cl.Request(g.ctx, &req)
	if err != nil {
		return err // Request retries as necesary, so this must be a failure
	}
	resp := kresp.(*kmsg.SyncGroupResponse)

	kassignment := new(kmsg.GroupMemberAssignment)
	if err = kassignment.ReadFrom(resp.MemberAssignment); err != nil {
		return err
	}

	g.assigned = make(map[string][]int32)
	for _, topic := range kassignment.Topics {
		g.assigned[topic.Topic] = topic.Partitions
	}

	return nil
}

func (g *groupConsumer) joinGroupProtocols() []kmsg.JoinGroupRequestGroupProtocol {
	topics := make([]string, 0, len(g.topics))
	for topic := range g.topics {
		topics = append(topics, topic)
	}
	var protos []kmsg.JoinGroupRequestGroupProtocol
	for _, balancer := range g.balancers {
		protos = append(protos, kmsg.JoinGroupRequestGroupProtocol{
			ProtocolName: balancer.protocolName(),
			ProtocolMetadata: balancer.metaFor(
				topics,
				g.assigned,
				g.generation,
			),
		})
	}
	return protos
}

func (g *groupConsumer) fetchOffsets() error {
	req := kmsg.OffsetFetchRequest{
		GroupID: g.id,
	}
	for topic, partitions := range g.assigned {
		req.Topics = append(req.Topics, kmsg.OffsetFetchRequestTopic{
			Topic:      topic,
			Partitions: partitions,
		})
	}
	kresp, err := g.cl.Request(g.ctx, &req)
	if err != nil {
		return err
	}
	resp := kresp.(*kmsg.OffsetFetchResponse)
	errCode := resp.ErrorCode
	if resp.Version < 2 && len(resp.Topics) > 0 && len(resp.Topics[0].Partitions) > 0 {
		errCode = resp.Topics[0].Partitions[0].ErrorCode
	}
	if err = kerr.ErrorForCode(errCode); err != nil && !kerr.IsRetriable(err) {
		return err
	}

	// TODO KIP-320
	offsets := make(map[string]map[int32]Offset)
	for _, rTopic := range resp.Topics {
		topicOffsets := make(map[int32]Offset)
		offsets[rTopic.Topic] = topicOffsets
		for _, rPartition := range rTopic.Partitions {
			if rPartition.ErrorCode != 0 {
				return kerr.ErrorForCode(rPartition.ErrorCode)
			}
			offset := ConsumeExactOffset(rPartition.Offset)
			if rPartition.Offset == -1 {
				offset = g.resetOffset
			}
			topicOffsets[rPartition.Partition] = offset
		}
	}

	g.c.mu.Lock()
	defer g.c.mu.Unlock()

	if g.seq < g.c.seq {
		return errors.New("stale group")
	}
	g.c.assignPartitions(offsets, true)
	g.seq = g.c.seq // track bumped

	g.c.resetAndLoadOffsets()
	return nil
}
