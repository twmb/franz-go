package kgo

import (
	"context"
	"errors"
	"regexp"
	"sync"
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

// GroupOnAssign sets the function to be called when a group is joined after
// partitions are assigned before fetches begin.
//
// Note that this function combined with onRevoke should combined not exceed
// the rebalance interval. It is possible for the group, immediately after
// finishing a balance, to re-enter a new balancing session.
//
// The onAssign function is passed the group's context, which is only canceled
// if the group is left or the client is closed.
func GroupOnAssign(onAssign func(context.Context, map[string][]int32)) GroupOpt {
	return groupOpt{func(cfg *groupConsumer) { cfg.onAssign = onAssign }}
}

// GroupOnRevoke sets the function to be called once a group transitions from
// stable to rebalancing.
//
// Note that this function combined with onAssign should combined not exceed
// the rebalance interval. It is possible for the group, immediately after
// finishing a balance, to re-enter a new balancing session.
//
// The onRevoke function is passed the group's context, which is only canceled
// if the group is left or the client is closed.
func GroupOnRevoke(onRevoke func(context.Context, map[string][]int32)) GroupOpt {
	return groupOpt{func(cfg *groupConsumer) { cfg.onRevoke = onRevoke }}
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

	mu     sync.Mutex          // guards the two below
	leader bool                // whether we are leader right now
	using  map[string]struct{} // topics we are currently using

	leaderRejoin chan struct{} // cap 1; potentially sent to when leader

	regexTopics bool
	reSeen      map[string]struct{}

	memberID   string
	generation int32
	assigned   map[string][]int32

	sessionTimeout    time.Duration
	rebalanceTimeout  time.Duration
	heartbeatInterval time.Duration

	onAssign func(context.Context, map[string][]int32)
	onRevoke func(context.Context, map[string][]int32)

	// TODO autocommit
	// OnLost (incremental)
}

// AssignGroup assigns a group to consume from, overriding any prior
// assignment. To leave a group, you can AssignGroup with an empty group.
func (cl *Client) AssignGroup(group string, opts ...GroupOpt) {
	c := &cl.consumer
	c.mu.Lock()
	defer c.mu.Unlock()

	c.unassignPrior()

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

		using:        make(map[string]struct{}),
		leaderRejoin: make(chan struct{}, 1),
		reSeen:       make(map[string]struct{}),

		sessionTimeout:    10000 * time.Millisecond,
		rebalanceTimeout:  60000 * time.Millisecond,
		heartbeatInterval: 3000 * time.Millisecond,
	}
	for _, opt := range opts {
		opt.apply(g)
	}
	if len(group) == 0 || len(g.topics) == 0 || c.dead {
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

	cl.triggerUpdateMetadata()
}

func (g *groupConsumer) manage() {
	var consecutiveErrors int
loop:
	for {
		err := g.joinAndSync()
		if err == nil {
			if err = g.setupAssigned(); err != nil {
				if err == kerr.RebalanceInProgress {
					err = nil
				}
			}
		}

		if err != nil {
			consecutiveErrors++
			// Waiting for the backoff is a good time to update our
			// metadata; maybe the error is from stale metadata.
			backoff := g.cl.cfg.client.retryBackoff(consecutiveErrors)
			deadline := time.Now().Add(backoff)
			g.cl.waitmeta(g.ctx, backoff)
			after := time.NewTimer(time.Until(deadline))
			select {
			case <-g.ctx.Done():
				after.Stop()
				return
			case <-after.C:
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

type assignRevokeSession struct {
	mu         sync.Mutex
	assigned   bool
	assignDone chan struct{}
	revoked    bool
	revokeDone chan struct{}
}

func (s *assignRevokeSession) revoke(g *groupConsumer) <-chan struct{} {
	s.mu.Lock()
	revoked := s.revoked
	assigned := s.assigned

	s.revoked = true
	if s.revokeDone == nil {
		s.revokeDone = make(chan struct{})
	}
	s.mu.Unlock()

	if !revoked {
		go func() {
			defer close(s.revokeDone)
			if assigned {
				<-s.assignDone
				if g.onRevoke != nil {
					g.onRevoke(g.ctx, g.assigned)
				}
			}
		}()
	}
	return s.revokeDone
}

func (s *assignRevokeSession) assign(g *groupConsumer) {
	s.mu.Lock()
	if s.revoked {
		s.mu.Unlock()
		return
	}
	s.assigned = true
	s.assignDone = make(chan struct{})
	s.mu.Unlock()
	defer close(s.assignDone)

	if g.onAssign != nil {
		g.onAssign(g.ctx, g.assigned)
	}
}

func (g *groupConsumer) setupAssigned() error {
	hbErrCh := make(chan error, 1)
	fetchErrCh := make(chan error, 1)
	ctx, cancel := context.WithCancel(g.ctx)

	s := new(assignRevokeSession)

	go func() {
		hbErrCh <- g.heartbeat(fetchErrCh, s)
		cancel() // potentially kill fetching
	}()

	doneOnAssign := make(chan struct{})
	go func() {
		s.assign(g)
		close(doneOnAssign)
	}()

	select {
	case err := <-hbErrCh:
		// heartbeat calls onRevoke if necessary, so we do not here.
		return err
	case <-doneOnAssign:
	}

	go func() {
		fetchErrCh <- g.fetchOffsets(ctx)
	}()

	return <-hbErrCh
}

// heartbeat issues heartbeat requests to Kafka for the duration of a group
// session.
//
// This function is began before fetching offsets to allow the consumer's
// onAssign to be called before fetching. If the eventual offset fetch errors,
// we continue heartbeating until onRevoke finishes and our metadata is
// updated.
//
// If the offset fetch is successful, then we basically sit in this function
// until a heartbeat errors or us, being the leader, decides to re-join.
func (g *groupConsumer) heartbeat(fetchErrCh <-chan error, s *assignRevokeSession) error {
	ticker := time.NewTicker(g.heartbeatInterval)
	defer ticker.Stop()

	var metadone, revoked <-chan struct{}
	var didMetadone, didRevoke bool
	var lastErr error

	for {
		var err error
		select {
		case <-ticker.C:
			req := &kmsg.HeartbeatRequest{
				GroupID:      g.id,
				GenerationID: g.generation,
				MemberID:     g.memberID,
			}
			var kresp kmsg.Response
			kresp, err = g.cl.Request(g.ctx, req)
			if err == nil {
				resp := kresp.(*kmsg.HeartbeatResponse)
				err = kerr.ErrorForCode(resp.ErrorCode)
			}
		case <-g.leaderRejoin:
			// If we are leader and a metadata update
			// triggers us to rejoin, we just pretend
			// we are rebalancing.
			//
			// No need to maintain leader at this point
			// since we are going to rejoin.
			err = kerr.RebalanceInProgress
			g.clearLeader()
		case err = <-fetchErrCh:
			fetchErrCh = nil
		case <-metadone:
			metadone = nil
			didMetadone = true
		case <-revoked:
			revoked = nil
			didRevoke = true
		case <-g.ctx.Done():
			s.revoke(g)
			return errors.New("left group or client closed")
		}

		if didMetadone && didRevoke {
			return lastErr
		}

		if err == nil {
			continue
		}

		// Since we errored, we must revoke.
		if !didRevoke && revoked == nil {
			revoked = s.revoke(g)
		}
		// Since we errored, while waiting for the revoke to finish, we
		// update our metadata. A leader may have re-joined with new
		// metadata, and we want the update.
		if !didMetadone && metadone == nil {
			waited := make(chan struct{})
			metadone = waited
			go func() {
				g.cl.waitmeta(g.ctx, g.sessionTimeout)
				close(waited)
			}()
		}

		// We always save the latest error; generally this should be
		// REBALANCE_IN_PROGRESS, but if the revoke takes too long,
		// Kafka may boot us and we will get a different error.
		lastErr = err
	}
}

func (g *groupConsumer) setLeader() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.leader = true
}

func (g *groupConsumer) clearLeader() {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.leader = false

	// After we set leader false, we always drain the rejoin channel to
	// ensure we will not be doubly rejoined when we should not be.
	select {
	case <-g.leaderRejoin:
	default:
	}
}

// rejoin is called if we are leader: this ensures the heartbeat loop will
// see we need to rejoin.
func (g *groupConsumer) rejoin() {
	select {
	case g.leaderRejoin <- struct{}{}:
	default:
	}
}

func (g *groupConsumer) joinAndSync() error {
	g.clearLeader()
start:
	req := kmsg.JoinGroupRequest{
		GroupID:          g.id,
		SessionTimeout:   int32(g.sessionTimeout.Milliseconds()),
		RebalanceTimeout: int32(g.rebalanceTimeout.Milliseconds()),
		ProtocolType:     "consumer",
		MemberID:         g.memberID,
		GroupProtocols:   g.joinGroupProtocols(),
	}
	kresp, err := g.cl.Request(g.ctx, &req)
	if err != nil {
		return err
	}
	resp := kresp.(*kmsg.JoinGroupResponse)

	if err = kerr.ErrorForCode(resp.ErrorCode); err != nil {
		switch err {
		case kerr.MemberIDRequired:
			g.memberID = resp.MemberID // KIP-394
			goto start
		case kerr.UnknownMemberID:
			g.memberID = ""
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
		g.setLeader()
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
	g.mu.Lock()
	topics := make([]string, 0, len(g.using))
	for topic := range g.using {
		topics = append(topics, topic)
	}
	g.mu.Unlock()
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

func (g *groupConsumer) fetchOffsets(ctx context.Context) error {
	req := kmsg.OffsetFetchRequest{
		GroupID: g.id,
	}
	for topic, partitions := range g.assigned {
		req.Topics = append(req.Topics, kmsg.OffsetFetchRequestTopic{
			Topic:      topic,
			Partitions: partitions,
		})
	}
	kresp, err := g.cl.Request(ctx, &req)
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
				offset = g.cl.cfg.consumer.resetOffset
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

func (g *groupConsumer) findNewAssignments(topics map[string]*topicPartitions) {
	g.mu.Lock()
	defer g.mu.Unlock()

	toUse := make(map[string]struct{}, len(topics))
	for topic, topicPartitions := range topics {
		// If we are already using this topic, no need to check it.
		if _, exists := g.using[topic]; exists {
			continue
		}

		var useTopic bool
		if g.regexTopics {
			if _, exists := g.reSeen[topic]; !exists {
				g.reSeen[topic] = struct{}{} // set we have seen so we do not reevaluate next time
				for reTopic := range g.topics {
					if match, _ := regexp.MatchString(reTopic, topic); match {
						useTopic = true
						break
					}
				}
			}
		} else {
			_, useTopic = g.topics[topic]
		}

		if useTopic {
			if g.regexTopics && topicPartitions.load().isInternal {
				continue
			}
			toUse[topic] = struct{}{}
		}

	}

	// Nothing new to add.
	if len(toUse) == 0 {
		return
	}

	wasManaging := len(g.using) != 0
	for topic := range toUse {
		g.using[topic] = struct{}{}
	}

	if !wasManaging {
		go g.manage()
	}

	if g.leader {
		g.rejoin()
	}
}
