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
// If autocommit is enabled, the default onRevoke is to commit all offsets.
//
// The onRevoke function is passed the group's context, which is only canceled
// if the group is left or the client is closed.
func GroupOnRevoke(onRevoke func(context.Context, map[string][]int32)) GroupOpt {
	return groupOpt{func(cfg *groupConsumer) { cfg.onRevoke = onRevoke }}
}

// GroupDisableAutoCommit disable auto committing.
func GroupDisableAutoCommit() GroupOpt {
	return groupOpt{func(cfg *groupConsumer) { cfg.autocommitDisable = true }}
}

// GroupAutoCommitInterval sets how long to go between autocommits, overriding the
// default 5s.
func GroupAutoCommitInterval(interval time.Duration) GroupOpt {
	return groupOpt{func(cfg *groupConsumer) { cfg.autocommitInterval = interval }}
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

	mu           sync.Mutex          // guards this block
	leader       bool                // whether we are leader right now
	using        map[string]struct{} // topics we are currently using
	uncommitted  uncommitted
	commitCancel func()
	commitDone   chan struct{}

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

	blockAuto          bool
	autocommitDisable  bool
	autocommitInterval time.Duration

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

		autocommitInterval: 5 * time.Second,
	}
	g.onRevoke = g.defaultRevoke
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

	if !g.autocommitDisable && g.autocommitInterval > 0 {
		go g.loopCommit()
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

// assignRevokeSession ensures that we call onRevoke if onAssign is called
// once we join a group, and that onAssign is NOT called if the group is
// already dead (and, in that case, that revoke is not called either).
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
			// First, immediately stop fetching what we were and
			// ensure we will fetch no more.
			g.c.mu.Lock()
			if g.seq == g.c.seq {
				g.c.assignPartitions(nil, true)
				g.seq = g.c.seq // track bump
			}
			g.c.mu.Unlock()

			// Now we call the user provided revoke callback.
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
	g.uncommitted = nil

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

// fetchOffsets is issued once we join a group to see what the prior commits
// were for the partitions we were assigned.
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
	g.seq = g.c.seq // track bump

	g.c.resetAndLoadOffsets()
	return nil
}

// findNewAssignments is called under the consumer lock at the end of a
// metadata update. This updates which topics the group wants to use and (1)
// joins the group if not yet joined and (2) rejoins the group if leader and
// there are new topics to use.
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

// uncommit tracks the latest offset polled (+1) and the latest commit.
// The reason head is just past the latest offset is beceause we want
// to commit TO an offset, not BEFORE an offset.
type uncommit struct {
	head      int64
	committed int64
}

type uncommitted map[string]map[int32]uncommit

// updateUncommitted sets the latest uncommitted offset. This is called under
// the consumer lock, but grabs the group lock to ensure no collision with
// commit.
func (g *groupConsumer) updateUncommitted(fetches Fetches) {
	g.mu.Lock()
	defer g.mu.Unlock()

	for _, fetch := range fetches {
		var topicOffsets map[int32]uncommit
		for _, topic := range fetch.Topics {
			for _, partition := range topic.Partitions {
				if len(partition.Records) == 0 {
					continue
				}
				offset := partition.Records[len(partition.Records)-1].Offset

				if topicOffsets == nil {
					if g.uncommitted == nil {
						g.uncommitted = make(uncommitted, 10)
					}
					topicOffsets = g.uncommitted[topic.Topic]
					if topicOffsets == nil {
						topicOffsets = make(map[int32]uncommit, 20)
						g.uncommitted[topic.Topic] = topicOffsets
					}
				}
				uncommit, exists := topicOffsets[partition.Partition]
				// Our new head points just past the final consumed offset,
				// that is, if we rejoin, this is the offset to begin at.
				newhead := offset + 1
				if exists && uncommit.head > newhead {
					continue // odd
				}
				uncommit.head = newhead
				topicOffsets[partition.Partition] = uncommit
			}
		}
	}
}

// updateCommitted updates the group's uncommitted map. This function triply
// verifies that the resp matches the req as it should and that the req does
// not somehow contain more than what is in our uncommitted map.
func (g *groupConsumer) updateCommitted(
	req *kmsg.OffsetCommitRequest,
	resp *kmsg.OffsetCommitResponse,
) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if req.GenerationID != g.generation {
		return
	}
	if g.uncommitted == nil || // just in case
		len(req.Topics) != len(resp.Topics) { // bad kafka
		return
	}

	for i := range resp.Topics {
		reqTopic := &req.Topics[i]
		respTopic := &resp.Topics[i]
		topic := g.uncommitted[respTopic.Topic]
		if topic == nil || // just in case
			reqTopic.Topic != respTopic.Topic || // bad kafka
			len(reqTopic.Partitions) != len(respTopic.Partitions) { // same
			continue
		}

		for i := range respTopic.Partitions {
			reqPart := &reqTopic.Partitions[i]
			respPart := &respTopic.Partitions[i]
			uncommit, exists := topic[respPart.Partition]
			if !exists || // just in case
				respPart.ErrorCode != 0 || // bad commit
				reqPart.Partition != respPart.Partition { // bad kafka
				continue
			}

			uncommit.committed = reqPart.Offset
			topic[respPart.Partition] = uncommit
		}
	}
}

func (g *groupConsumer) loopCommit() {
	ticker := time.NewTicker(g.autocommitInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
		case <-g.ctx.Done():
			return
		}

		g.mu.Lock()
		if !g.blockAuto {
			g.commit(context.Background(), g.getUncommittedLocked(), nil)
		}
		g.mu.Unlock()
	}
}

// Uncommitted returns the latest uncommitted offsets. Uncommitted offsets are
// always updated on calls to PollFetches.
//
// If there are no uncommitted offsets, this returns nil.
func (cl *Client) Uncommitted() map[string]map[int32]int64 {
	cl.consumer.mu.Lock()
	defer cl.consumer.mu.Unlock()
	if cl.consumer.typ != consumerTypeGroup {
		return nil
	}
	return cl.consumer.group.getUncommitted()
}

func (g *groupConsumer) getUncommitted() map[string]map[int32]int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.getUncommittedLocked()
}

func (g *groupConsumer) getUncommittedLocked() map[string]map[int32]int64 {
	if g.uncommitted == nil {
		return nil
	}

	var uncommitted map[string]map[int32]int64
	for topic, partitions := range g.uncommitted {
		var topicUncommitted map[int32]int64
		for partition, uncommit := range partitions {
			if uncommit.head == uncommit.committed {
				continue
			}
			if topicUncommitted == nil {
				if uncommitted == nil {
					uncommitted = make(map[string]map[int32]int64, len(g.uncommitted))
				}
				topicUncommitted = uncommitted[topic]
				if topicUncommitted == nil {
					topicUncommitted = make(map[int32]int64, len(partitions))
					uncommitted[topic] = topicUncommitted
				}
			}
			topicUncommitted[partition] = uncommit.head
		}
	}
	return uncommitted
}

// Commit commits the given offsets for a group, calling onDone if non-nil once
// the commit response is received. If uncommitted is empty or the client is
// not consuming as a group, this is function returns immediately.
//
// If autocommitting is enabled, this function blocks autocommitting while
// until this function is complete and the onDone has returned.
//
// Note that this function ensures absolute ordering of commit requests by
// canceling prior requests and ensuring they are done before executing a new
// one. This means, for absolute control, you can use this function to
// periodically commit async and then issue a final sync commit before
// quitting. This differs from the Java async commit, which does not retry
// requests to avoid trampling on future commits.
//
// If using autocommitting, autocommitting will resume once this is complete,
// committing only if the client's internal uncommitted offsets counters are
// higher than the known last commit.
func (cl *Client) Commit(
	ctx context.Context,
	uncommitted map[string]map[int32]int64,
	onDone func(*kmsg.OffsetCommitRequest, *kmsg.OffsetCommitResponse, error),
) {
	if onDone == nil {
		onDone = func(_ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, _ error) {}
	}
	cl.consumer.mu.Lock()
	defer cl.consumer.mu.Unlock()
	if cl.consumer.typ != consumerTypeGroup {
		onDone(new(kmsg.OffsetCommitRequest), new(kmsg.OffsetCommitResponse), nil)
		return
	}
	if len(uncommitted) == 0 {
		onDone(new(kmsg.OffsetCommitRequest), new(kmsg.OffsetCommitResponse), nil)
		return
	}

	g := cl.consumer.group
	g.mu.Lock()
	defer g.mu.Unlock()

	g.blockAuto = true
	unblock := func(req *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
		if onDone != nil {
			onDone(req, resp, err)
		}
		g.mu.Lock()
		defer g.mu.Unlock()
		g.blockAuto = false
	}

	g.commit(ctx, uncommitted, unblock)
}

// defaultRevoke commits the last fetched offsets and waits for the commit to
// finish. This is the default onRevoke function which, when combined with the
// default autocommit, ensures we never miss committing everything.
//
// Note that the heartbeat loop invalidates all buffered, unpolled fetches
// before revoking, meaning this truly will commit all polled fetches.
func (g *groupConsumer) defaultRevoke(_ context.Context, _ map[string][]int32) {
	if !g.autocommitDisable {
		wait := make(chan struct{})
		g.cl.Commit(g.ctx, g.getUncommitted(), func(_ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, _ error) {
			close(wait)
		})
		<-wait
	}
}

// commit is the logic for Commit; see Commit's documentation
func (g *groupConsumer) commit(
	ctx context.Context,
	uncommitted map[string]map[int32]int64,
	onDone func(*kmsg.OffsetCommitRequest, *kmsg.OffsetCommitResponse, error),
) {
	if onDone == nil { // note we must always call onDone
		onDone = func(_ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, _ error) {}
	}
	if len(uncommitted) == 0 { // only empty if called thru autocommit / default revoke
		onDone(new(kmsg.OffsetCommitRequest), new(kmsg.OffsetCommitResponse), nil)
		return
	}

	if g.commitCancel != nil {
		g.commitCancel() // cancel any prior commit
	}
	priorDone := g.commitDone

	commitCtx, commitCancel := context.WithCancel(g.ctx) // enable ours to be canceled and waited for
	commitDone := make(chan struct{})

	g.commitCancel = commitCancel
	g.commitDone = commitDone

	memberID := g.memberID
	req := &kmsg.OffsetCommitRequest{
		GroupID:         g.id,
		GenerationID:    g.generation,
		MemberID:        memberID,
		GroupInstanceID: nil, // TODO KIP-345
	}
	// TODO capture epoch for KIP-320 before the goroutine below
	// to avoid race read/write problems

	if ctx.Done() != nil {
		go func() {
			select {
			case <-ctx.Done():
				commitCancel()
			case <-commitCtx.Done():
			}
		}()
	}

	go func() {
		defer close(commitDone) // allow future commits to continue when we are done
		defer commitCancel()
		if priorDone != nil { // wait for any prior request to finish
			<-priorDone
		}

		for topic, partitions := range uncommitted {
			req.Topics = append(req.Topics, kmsg.OffsetCommitRequestTopic{
				Topic: topic,
			})
			reqTopic := &req.Topics[len(req.Topics)-1]
			for partition, offset := range partitions {
				reqTopic.Partitions = append(reqTopic.Partitions, kmsg.OffsetCommitRequestTopicPartition{
					Partition:   partition,
					Offset:      offset,
					LeaderEpoch: -1, // TODO KIP-320,
					Metadata:    &memberID,
				})
			}
		}

		var kresp kmsg.Response
		var err error
		if len(req.Topics) > 0 {
			kresp, err = g.cl.Request(commitCtx, req)
		}
		if err != nil {
			onDone(req, nil, err)
			return
		}
		resp := kresp.(*kmsg.OffsetCommitResponse)
		g.updateCommitted(req, resp)
		onDone(req, resp, nil)
	}()
}
