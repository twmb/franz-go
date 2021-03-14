package kgo

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
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

// Balancers sets the group balancers to use for dividing topic partitions
// among group members, overriding the defaults.
//
// The current default is [cooperative-sticky].
//
// For balancing, Kafka chooses the first protocol that all group members agree
// to support.
//
// Note that if you want to opt in to cooperative-sticky rebalancing,
// cooperative group balancing is incompatible with eager (classical)
// rebalancing and requires a careful rollout strategy (see KIP-429).
func Balancers(balancers ...GroupBalancer) GroupOpt {
	return groupOpt{func(cfg *groupConsumer) { cfg.balancers = balancers }}
}

// SessionTimeout sets how long a member the group can go between heartbeats,
// overriding the default 10,000ms. If a member does not heartbeat in this
// timeout, the broker will remove the member from the group and initiate a
// rebalance.
//
// This corresponds to Kafka's session.timeout.ms setting and must be within
// the broker's group.min.session.timeout.ms and group.max.session.timeout.ms.
func SessionTimeout(timeout time.Duration) GroupOpt {
	return groupOpt{func(cfg *groupConsumer) { cfg.sessionTimeout = timeout }}
}

// RebalanceTimeout sets how long group members are allowed to take when a a
// rebalance has begun, overriding the default 60,000ms. This timeout is how
// long all members are allowed to complete work and commit offsets, minus the
// time it took to detect the rebalance (from a heartbeat).
//
// Kafka uses the largest rebalance timeout of all members in the group. If a
// member does not rejoin within this timeout, Kafka will kick that member from
// the group.
//
// This corresponds to Kafka's rebalance.timeout.ms.
func RebalanceTimeout(timeout time.Duration) GroupOpt {
	return groupOpt{func(cfg *groupConsumer) { cfg.rebalanceTimeout = timeout }}
}

// HeartbeatInterval sets how long a group member goes between heartbeats to
// Kafka, overriding the default 3,000ms.
//
// Kafka uses heartbeats to ensure that a group member's session stays active.
// This value can be any value lower than the session timeout, but should be no
// higher than 1/3rd the session timeout.
//
// This corresponds to Kafka's heartbeat.interval.ms.
func HeartbeatInterval(interval time.Duration) GroupOpt {
	return groupOpt{func(cfg *groupConsumer) { cfg.heartbeatInterval = interval }}
}

// RequireStableFetchOffsets sets the group consumer to require "stable" fetch
// offsets before consuming from the group. Proposed in KIP-447 and introduced
// in Kafka 2.5.0, stable offsets are important when consuming from partitions
// that a transactional producer could be committing to.
//
// With this option, Kafka will block group consumers from fetching offsets for
// partitions that are in an active transaction.
//
// Because this can block consumption, it is strongly recommended to set
// transactional timeouts to a small value (10s) rather than the default 60s.
// Lowering the transactional timeout will reduce the chance that consumers are
// entirely blocked.
func RequireStableFetchOffsets() GroupOpt {
	return groupOpt{func(cfg *groupConsumer) { cfg.requireStable = true }}
}

// OnAssigned sets the function to be called when a group is joined after
// partitions are assigned before fetches for those partitions begin.
//
// This function combined with OnRevoked should not exceed the rebalance
// interval. It is possible for the group, immediately after finishing a
// balance, to re-enter a new balancing session.
//
// The OnAssigned function is passed the group's context, which is only
// canceled if the group is left or the client is closed.
func OnAssigned(onAssigned func(context.Context, map[string][]int32)) GroupOpt {
	return groupOpt{func(cfg *groupConsumer) { cfg.onAssigned = onAssigned }}
}

// OnRevoked sets the function to be called once this group member has
// partitions revoked.
//
// This function combined with OnAssigned should not exceed the rebalance
// interval. It is possible for the group, immediately after finishing a
// balance, to re-enter a new balancing session.
//
// If autocommit is enabled, the default OnRevoked is a blocking commit all
// offsets. The reason for a blocking commit is so that no later commit cancels
// the blocking commit. If the commit in OnRevoked were canceled, then the
// rebalance would proceed immediately, the commit that canceled the blocking
// commit would fail, and duplicates could be consumed after the rebalance
// completes.
//
// The OnRevoked function is passed the group's context, which is only canceled
// if the group is left or the client is closed.
//
// OnRevoked function is called at the end of a group session even if there are
// no partitions being revoked.
//
// If you are committing offsets manually (have disabled autocommitting), it is
// highly recommended to do a proper blocking commit in OnRevoked.
func OnRevoked(onRevoked func(context.Context, map[string][]int32)) GroupOpt {
	return groupOpt{func(cfg *groupConsumer) { cfg.onRevoked = onRevoked }}
}

// OnLost sets the function to be called on "fatal" group errors, such as
// IllegalGeneration, UnknownMemberID, and authentication failures. This
// function differs from OnRevoked in that it is unlikely that commits will
// succeed when partitions are outright lost, whereas commits likely will
// succeed when revoking partitions.
//
// If not set, OnRevoked is used.
func OnLost(onLost func(context.Context, map[string][]int32)) GroupOpt {
	return groupOpt{func(cfg *groupConsumer) { cfg.onLost = onLost }}
}

// DisableAutoCommit disable auto committing.
func DisableAutoCommit() GroupOpt {
	return groupOpt{func(cfg *groupConsumer) { cfg.autocommitDisable = true }}
}

// AutoCommitInterval sets how long to go between autocommits, overriding the
// default 5s.
func AutoCommitInterval(interval time.Duration) GroupOpt {
	return groupOpt{func(cfg *groupConsumer) { cfg.autocommitInterval = interval }}
}

// InstanceID sets the group consumer's instance ID, switching the group member
// from "dynamic" to "static".
//
// Prior to Kafka 2.3.0, joining a group gave a group member a new member ID.
// The group leader could not tell if this was a rejoining member. Thus, any
// join caused the group to rebalance.
//
// Kafka 2.3.0 introduced the concept of an instance ID, which can persist
// across restarts. This allows for avoiding many costly rebalances and allows
// for stickier rebalancing for rejoining members (since the ID for balancing
// stays the same). The main downsides are that you, the user of a client, have
// to manage instance IDs properly, and that it may take longer to rebalance in
// the event that a client legitimately dies.
//
// When using an instance ID, the client does NOT send a leave group request
// when closing. This allows for the client ot restart with the same instance
// ID and rejoin the group to avoid a rebalance. It is strongly recommended to
// increase the session timeout enough to allow time for the restart (remember
// that the default session timeout is 10s).
//
// To actually leave the group, you must use an external admin command that
// issues a leave group request on behalf of this instance ID (see kcl), or you
// can manually use the kmsg package with a proper LeaveGroupRequest.
//
// NOTE: Leaving a group with an instance ID is only supported in Kafka 2.4.0+.
func InstanceID(id string) GroupOpt {
	return groupOpt{func(cfg *groupConsumer) { cfg.instanceID = &id }}
}

// GroupProtocol sets the group's join protocol, overriding the default value
// "consumer". The only reason to override this is if you are implementing
// custom join and sync group logic.
func GroupProtocol(protocol string) GroupOpt {
	return groupOpt{func(cfg *groupConsumer) { cfg.protocol = protocol }}
}

type groupConsumer struct {
	c  *consumer // used to change consumer state; generally c.mu is grabbed on access
	cl *Client   // used for running requests / adding to topics map

	ctx        context.Context
	cancel     func()
	manageDone chan struct{} // closed once when the manage goroutine quits

	/////////////////////////
	// configuration block //
	/////////////////////////

	id          string              // group we are in
	instanceID  *string             // optional, our instance ID
	topics      map[string]struct{} // topics we are interested in
	balancers   []GroupBalancer     // balancers we can use
	protocol    string              // "consumer" by default, expected to never be overridden
	cooperative bool                // whether all balancers are cooperative

	sessionTimeout    time.Duration
	rebalanceTimeout  time.Duration
	heartbeatInterval time.Duration
	requireStable     bool

	onAssigned func(context.Context, map[string][]int32)
	onRevoked  func(context.Context, map[string][]int32)
	onLost     func(context.Context, map[string][]int32)

	autocommitDisable  bool // true if autocommit was disabled or we are transactional
	autocommitInterval time.Duration

	///////////////////////
	// configuration end //
	///////////////////////

	// regexTopics is configuration, but used exclusively with reSeen,
	// which is updated in findNewAssignments. If our assignment is for
	// regular expressions, then we put every topic that we have passed
	// against all our regex into reSeen. This avoids us re-evaluating
	// topics in our regex on future metadata assignments.
	regexTopics bool
	reSeen      map[string]struct{}

	// Full lock grabbed in BlockingCommitOffsets, read lock grabbed in
	// CommitOffsets, this lock ensures that only one blocking commit can
	// happen at once, and if it is happening, no other commit can be
	// happening.
	blockingCommitMu sync.RWMutex

	rejoinCh chan struct{} // cap 1; sent to if subscription changes (regex)

	// The following two are only updated in the manager / join&sync loop
	lastAssigned map[string][]int32 // only updated in join&sync loop
	nowAssigned  map[string][]int32 // only updated in join&sync loop

	groupExtraTopics map[string]struct{} // TODO TODO TODO

	// leader is whether we are the leader right now. This is set to false
	//
	//  - set to false at the beginning of a join group session
	//  - set to true if join group response indicates we are leader
	//  - read on metadata updates in findNewAssignments
	leader atomicBool

	// Set to true when ending a transaction committing transaction
	// offsets, and then set to false immediately after before calling
	// EndTransaction.
	offsetsAddedToTxn bool

	//////////////
	// mu block //
	//////////////
	mu sync.Mutex

	// using is updated when finding new assignments, we always add to this
	// if we want to consume a topic (or see there are more potential
	// partitions). Only the leader can trigger a new group session if there
	// are simply more partitions for existing topics.
	//
	// This is read when joining a group or leaving a group.
	using map[string]int // topics *we* are currently using => # partitions known in that topic

	// uncommitted is read and updated all over:
	// - updated before PollFetches returns
	// - updated when directly setting offsets (to rewind, for transactions)
	// - emptied when leaving a group
	// - updated when revoking
	// - updated after fetching offsets once we receive our group assignment
	// - updated after we commit
	// - read when getting uncommitted or committed
	uncommitted uncommitted

	// memberID and generation are written to in the join and sync loop,
	// and mostly read within that loop. The reason these two are under the
	// mutex is because they are read during commits, which can happen at
	// any arbitrary moment. It is **recommended** to be done within the
	// context of a group session, but (a) users may have some unique use
	// cases, and (b) the onRevoke hook may take longer than a user
	// expects, which would rotate a session.
	memberID   string
	generation int32

	// commitCancel and commitDone are set under mu before firing off an
	// async commit request. If another commit happens, it cancels the
	// prior commit, waits for the prior to be done, and then starts its
	// own.
	commitCancel func()
	commitDone   chan struct{}

	// blockAuto is set and cleared in {,Blocking}CommitOffsets to block
	// autocommitting if autocommitting is active. This ensures that an
	// autocommit does not cancel the user's manual commit.
	blockAuto bool

	dying bool // set when closing, read in findNewAssignments
}

// LeaveGroup leaves a group if in one. Calling the client's Close function
// also leaves a group, so this is only necessary to call if you plan to leave
// the group and continue using the client.
//
// If you have configured the group with an InstanceID, this does not leave the
// group. With instance IDs, it is expected that clients will restart and
// re-use the same instance ID. To leave a group using an instance ID, you must
// manually issue a kmsg.LeaveGroupRequest or use an external tool (kafka
// scripts or kcl).
func (cl *Client) LeaveGroup() {
	c := &cl.consumer
	c.assignMu.Lock()
	_, wait := cl.consumer.unset()
	c.assignMu.Unlock()
	wait()
}

// AssignGroup assigns a group to consume from, overriding any prior
// assignment.
//
// To leave a group, you can AssignGroup with an empty group, or just close the
// client. If you are using instance IDs, the client does not explicitly leave
// the group and instead you must issue a `kmsg.LeaveGroupRequest` manually (as
// expected when using instance IDs).
//
// It is recommended to do one final blocking commit before leaving a group.
func (cl *Client) AssignGroup(group string, opts ...GroupOpt) {
	c := &cl.consumer

	c.assignMu.Lock()
	defer c.assignMu.Unlock()

	if wasDead := c.unsetAndWait(); wasDead {
		return
	}

	ctx, cancel := context.WithCancel(cl.ctx)
	g := &groupConsumer{
		c:  c,
		cl: cl,

		ctx:        ctx,
		cancel:     cancel,
		manageDone: make(chan struct{}),

		id: group,

		balancers: []GroupBalancer{
			CooperativeStickyBalancer(),
		},
		protocol:    "consumer",
		cooperative: true, // default yes, potentially canceled below by our balancers

		using:    make(map[string]int),
		rejoinCh: make(chan struct{}, 1),
		reSeen:   make(map[string]struct{}),

		sessionTimeout:    10000 * time.Millisecond,
		rebalanceTimeout:  60000 * time.Millisecond,
		heartbeatInterval: 3000 * time.Millisecond,

		autocommitInterval: 5 * time.Second,
	}
	if c.cl.cfg.txnID == nil {
		g.onRevoked = g.defaultRevoke
	} else {
		g.autocommitDisable = true
	}
	for _, opt := range opts {
		opt.apply(g)
	}
	if len(group) == 0 || len(g.topics) == 0 || c.dead {
		return
	}
	for _, balancer := range g.balancers {
		g.cooperative = g.cooperative && balancer.isCooperative()
	}

	c.storeGroup(g)

	// Ensure all topics exist so that we will fetch their metadata.
	if !g.regexTopics {
		topics := make([]string, 0, len(g.topics))
		for topic := range g.topics {
			topics = append(topics, topic)
		}
		cl.storeTopics(topics)
	}

	if !g.autocommitDisable && g.autocommitInterval > 0 {
		g.cl.cfg.logger.Log(LogLevelInfo, "beginning autocommit loop")
		go g.loopCommit()
	}

	cl.triggerUpdateMetadata()
}

// Manages the group consumer's join / sync / heartbeat / fetch offset flow.
//
// Once a group is assigned, we fire a metadata request for all topics the
// assignment specified interest in. Only after we finally have some topic
// metadata do we join the group, and once joined, this management runs in a
// dedicated goroutine until the group is left.
func (g *groupConsumer) manage() {
	defer close(g.manageDone)
	g.cl.cfg.logger.Log(LogLevelInfo, "beginning to manage the group lifecycle")

	var consecutiveErrors int
	for {
		err := g.joinAndSync()
		if err == nil {
			if err = g.setupAssignedAndHeartbeat(); err != nil {
				if err == kerr.RebalanceInProgress {
					err = nil
				}
			}
		}
		if err == nil {
			consecutiveErrors = 0
			continue
		}

		if g.onLost != nil {
			g.onLost(g.ctx, g.nowAssigned)
		} else if g.onRevoked != nil {
			g.onRevoked(g.ctx, g.nowAssigned)
		}

		// If we are eager, we should have invalidated everything
		// before getting here, but we do so doubly just in case.
		//
		// If we are cooperative, the join and sync could have failed
		// during the cooperative rebalance where we were still
		// consuming. We need to invalidate everything.
		{
			g.c.mu.Lock()
			g.c.assignPartitions(nil, assignInvalidateAll)
			g.mu.Lock()     // before allowing poll to touch uncommitted, lock the group
			g.c.mu.Unlock() // now part of poll can continue
			g.uncommitted = nil
			g.mu.Unlock()

			g.nowAssigned = nil
			g.lastAssigned = nil

			g.leader.set(false)
		}

		if err == context.Canceled { // context was canceled, quit now
			return
		}

		// Waiting for the backoff is a good time to update our
		// metadata; maybe the error is from stale metadata.
		consecutiveErrors++
		backoff := g.cl.cfg.retryBackoff(consecutiveErrors)
		g.cl.cfg.logger.Log(LogLevelError, "join and sync loop errored",
			"err", err,
			"consecutive_errors", consecutiveErrors,
			"backoff", backoff,
		)
		deadline := time.Now().Add(backoff)
		g.cl.waitmeta(g.ctx, backoff)
		after := time.NewTimer(time.Until(deadline))
		select {
		case <-g.ctx.Done():
			after.Stop()
			return
		case <-after.C:
		}
	}
}

func (g *groupConsumer) leave() (wait func()) {
	g.cancel()

	// If g.using is nonzero before this check, then a manage goroutine has
	// started. If not, it will never start because we set dying.
	g.mu.Lock()
	g.dying = true
	wasManaging := len(g.using) > 0
	g.mu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)

		if wasManaging {
			// We want to wait for the manage goroutine to be done
			// so that we call the user's on{Assign,RevokeLost}.
			<-g.manageDone
		}

		if g.instanceID == nil {
			g.cl.cfg.logger.Log(LogLevelInfo,
				"leaving group",
				"group", g.id,
				"memberID", g.memberID, // lock not needed now since nothing can change it (manageDone)
			)
			(&kmsg.LeaveGroupRequest{
				Group:    g.id,
				MemberID: g.memberID,
				Members: []kmsg.LeaveGroupRequestMember{{
					MemberID: g.memberID,
					// no instance ID
				}},
			}).RequestWith(g.cl.ctx, g.cl)
		}
	}()

	return func() { <-done }
}

// returns the difference of g.nowAssigned and g.lastAssigned.
func (g *groupConsumer) diffAssigned() (added, lost map[string][]int32) {
	if g.lastAssigned == nil {
		return g.nowAssigned, nil
	}

	added = make(map[string][]int32, len(g.nowAssigned))
	lost = make(map[string][]int32, len(g.nowAssigned))

	// First, we diff lasts: any topic in last but not now is lost,
	// otherwise, (1) new partitions are added, (2) common partitions are
	// ignored, and (3) partitions no longer in now are lost.
	lasts := make(map[int32]struct{}, 100)
	for topic, lastPartitions := range g.lastAssigned {
		nowPartitions, exists := g.nowAssigned[topic]
		if !exists {
			lost[topic] = lastPartitions
			continue
		}

		for _, lastPartition := range lastPartitions {
			lasts[lastPartition] = struct{}{}
		}

		// Anything now that does not exist in last is new,
		// otherwise it is in common and we ignore it.
		for _, nowPartition := range nowPartitions {
			if _, exists := lasts[nowPartition]; !exists {
				added[topic] = append(added[topic], nowPartition)
			} else {
				delete(lasts, nowPartition)
			}
		}

		// Anything remanining in last does not exist now
		// and is thus lost.
		for last := range lasts {
			lost[topic] = append(lost[topic], last)
			delete(lasts, last) // reuse lasts
		}
	}

	// Finally, any new topics in now assigned are strictly added.
	for topic, nowPartitions := range g.nowAssigned {
		if _, exists := g.lastAssigned[topic]; !exists {
			added[topic] = nowPartitions
		}
	}

	return added, lost
}

type revokeStage int8

const (
	revokeLastSession = iota
	revokeThisSession
)

// revoke calls onRevoked for partitions that this group member is losing and
// updates the uncommitted map after the revoke.
//
// For eager consumers, this simply revokes g.assigned. This will only be
// called at the end of a group session.
//
// For cooperative consumers, this either
//
//     (1) if revoking lost partitions from a prior session (i.e., after sync),
//         this revokes the passed in lost
//     (2) if revoking at the end of a session, this revokes topics that the
//         consumer is no longer interested in consuming (TODO, actually, only
//         once we allow subscriptions to change without leaving the group).
//
// Lastly, for cooperative consumers, this must selectively delete what was
// lost from the uncommitted map.
func (g *groupConsumer) revoke(stage revokeStage, lost map[string][]int32) {
	if !g.cooperative { // stage == revokeThisSession if not cooperative
		g.cl.cfg.logger.Log(LogLevelInfo, "eager consumer revoking prior assigned partitions", "revoking", g.nowAssigned)
		if g.onRevoked != nil {
			g.onRevoked(g.ctx, g.nowAssigned)
		}
		g.nowAssigned = nil

		// We are setting uncommitted to nil _after_ the heartbeat loop
		// already called assignPartitions(nil, assignInvalidateAll).
		// After nilling uncommitted here, nothing should recreate
		// uncommitted until a future fetch after the group is
		// rejoined. This _can_ be broken with a manual SetOffsets or
		// with {,Blocking}CommitOffsets but we explicitly document not
		// to do that outside the context of a live group session.
		g.mu.Lock()
		g.uncommitted = nil
		g.mu.Unlock()
		return
	}

	switch stage {
	case revokeLastSession:
		// we use lost in this case

	case revokeThisSession:
		// lost is nil for cooperative assigning. Instead, we determine
		// lost by finding subscriptions we are no longer interested in.
		//
		// TODO only relevant when we allow AssignGroup with the same
		// group to change subscriptions (also we must delete the
		// unused partitions from nowAssigned).
	}

	if len(lost) > 0 {
		// We must now stop fetching anything we lost and invalidate
		// any buffered fetches before falling into onRevoked.
		//
		// We want to invalidate buffered fetches since they may
		// contain partitions that we lost, and we do not want a future
		// poll to return those fetches.
		lostOffsets := make(map[string]map[int32]Offset, len(lost))

		for lostTopic, lostPartitions := range lost {
			lostPartitionOffsets := make(map[int32]Offset, len(lostPartitions))
			for _, lostPartition := range lostPartitions {
				lostPartitionOffsets[lostPartition] = Offset{}
			}
			lostOffsets[lostTopic] = lostPartitionOffsets
		}

		// We must invalidate before revoking and before updating
		// uncommitted, because we want any commits in onRevoke to be
		// for the final polled offsets. We do not want to allow the
		// logical race of allowing fetches for revoked partitions
		// after a revoke but before an invalidation.
		g.c.mu.Lock()
		g.c.assignPartitions(lostOffsets, assignInvalidateMatching)
		g.c.mu.Unlock()
	}

	if len(lost) > 0 || stage == revokeThisSession {
		if len(lost) == 0 {
			g.cl.cfg.logger.Log(LogLevelInfo, "cooperative consumer calling onRevoke at the end of a session even though no partitions were lost")
		} else {
			g.cl.cfg.logger.Log(LogLevelInfo, "cooperative consumer calling onRevoke", "lost", lost, "stage", stage)
		}
		if g.onRevoked != nil {
			g.onRevoked(g.ctx, lost)
		}
	}

	if len(lost) == 0 { // if we lost nothing, do nothing
		return
	}

	defer g.rejoin() // cooperative consumers rejoin after they revoking what they lost

	// The block below deletes everything lost from our uncommitted map.
	// All commits should be **completed** by the time this runs. An async
	// commit can undo what we do below. The default revoke runs a blocking
	// commit.
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.uncommitted == nil {
		return
	}
	for lostTopic, lostPartitions := range lost {
		uncommittedPartitions := g.uncommitted[lostTopic]
		if uncommittedPartitions == nil {
			continue
		}
		for _, lostPartition := range lostPartitions {
			delete(uncommittedPartitions, lostPartition)
		}
		if len(uncommittedPartitions) == 0 {
			delete(g.uncommitted, lostTopic)
		}
	}
	if len(g.uncommitted) == 0 {
		g.uncommitted = nil
	}

}

// assignRevokeSession aids in sequencing prerevoke/assign/revoke.
type assignRevokeSession struct {
	prerevokeDone chan struct{}
	assignDone    chan struct{}
	revokeDone    chan struct{}
}

func newAssignRevokeSession() *assignRevokeSession {
	return &assignRevokeSession{
		prerevokeDone: make(chan struct{}),
		assignDone:    make(chan struct{}),
		revokeDone:    make(chan struct{}),
	}
}

// For cooperative consumers, the first thing a cooperative consumer does is to
// diff its last assignment and its new assignment and revoke anything lost.
// We call this a "prerevoke".
func (s *assignRevokeSession) prerevoke(g *groupConsumer, lost map[string][]int32) <-chan struct{} {
	go func() {
		defer close(s.prerevokeDone)
		if g.cooperative && len(lost) > 0 {
			g.revoke(revokeLastSession, lost)
		}
	}()
	return s.prerevokeDone
}

func (s *assignRevokeSession) assign(g *groupConsumer, newAssigned map[string][]int32) <-chan struct{} {
	go func() {
		defer close(s.assignDone)
		<-s.prerevokeDone
		if g.onAssigned != nil {
			// We always call on assigned, even if nothing new is
			// assigned. This allows consumers to know that
			// assignment is done and do setup logic.
			g.onAssigned(g.ctx, newAssigned)
		}
	}()
	return s.assignDone
}

// At the end of a group session, before we leave the heartbeat loop, we call
// revoke. For non-cooperative consumers, this revokes everything in the
// current session, and before revoking, we invalidate all partitions.  For the
// cooperative consumer, this does nothing but does notify the client that a
// revoke has begun / the group session is ending.
//
// This may not run before returning from the heartbeat loop: if we encounter a
// fatal error, we return before revoking so that we can instead call onLost in
// the manage loop.
func (s *assignRevokeSession) revoke(g *groupConsumer) <-chan struct{} {
	go func() {
		defer close(s.revokeDone)
		<-s.assignDone
		if g.onRevoked != nil {
			g.revoke(revokeThisSession, nil)
		}
	}()
	return s.revokeDone
}

// This chunk of code "pre" revokes lost partitions for the cooperative
// consumer and then begins heartbeating while fetching offsets. This returns
// when heartbeating errors (or if fetch offsets errors).
//
// Before returning, this function ensures that
//  - onAssigned is complete
//    - which ensures that pre revoking is complete
//  - fetching is complete
//  - heartbeating is complete
func (g *groupConsumer) setupAssignedAndHeartbeat() error {
	hbErrCh := make(chan error, 1)
	fetchErrCh := make(chan error, 1)

	s := newAssignRevokeSession()
	added, lost := g.diffAssigned()
	g.cl.cfg.logger.Log(LogLevelInfo, "new group session begun", "added", added, "lost", lost)
	s.prerevoke(g, lost) // for cooperative consumers

	// Since we have joined the group, we immediately begin heartbeating.
	// This will continue until the heartbeat errors, the group is killed,
	// or the fetch offsets below errors.
	ctx, cancel := context.WithCancel(g.ctx)
	go func() {
		defer cancel() // potentially kill offset fetching
		g.cl.cfg.logger.Log(LogLevelInfo, "beginning heartbeat loop")
		hbErrCh <- g.heartbeat(fetchErrCh, s)
	}()

	// We immediately begin fetching offsets. We want to wait until the
	// fetch function returns, since it assumes within it that another
	// assign cannot happen (it assigns partitions itself). Returning
	// before the fetch completes would be not good.
	//
	// The difference between fetchDone and fetchErrCh is that fetchErrCh
	// can kill heartbeating, or signal it to continue, while fetchDone
	// is specifically used for this function's return.
	fetchDone := make(chan struct{})
	defer func() { <-fetchDone }()
	if len(added) > 0 {
		go func() {
			defer close(fetchDone)
			defer close(fetchErrCh)
			g.cl.cfg.logger.Log(LogLevelInfo, "fetching offsets for added partitions", "added", added)
			fetchErrCh <- g.fetchOffsets(ctx, added)
		}()
	} else {
		close(fetchDone)
		close(fetchErrCh)
	}

	// Before we return, we also want to ensure that the user's onAssign is
	// done.
	//
	// Ensuring assigning is done ensures two things:
	//
	// * that we wait for for prerevoking to be done, which updates the
	// uncommitted field. Waiting for that ensures that a rejoin and poll
	// does not have weird concurrent interaction.
	//
	// * that our onLost will not be concurrent with onAssign
	//
	// We especially need to wait here because heartbeating may not
	// necessarily run onRevoke before returning (because of a fatal
	// error).
	s.assign(g, added)
	defer func() { <-s.assignDone }()

	// Finally, we simply return whatever the heartbeat error is. This will
	// be the fetch offset error if that function is what killed this.
	return <-hbErrCh
}

// heartbeat issues heartbeat requests to Kafka for the duration of a group
// session.
//
// This function begins before fetching offsets to allow the consumer's
// onAssigned to be called before fetching. If the eventual offset fetch
// errors, we continue heartbeating until onRevoked finishes and our metadata
// is updated. If the error is not RebalanceInProgress, we return immediately.
//
// If the offset fetch is successful, then we basically sit in this function
// until a heartbeat errors or we, being the leader, decide to re-join.
func (g *groupConsumer) heartbeat(fetchErrCh <-chan error, s *assignRevokeSession) error {
	ticker := time.NewTicker(g.heartbeatInterval)
	defer ticker.Stop()

	// We issue one heartbeat quickly if we are cooperative because
	// cooperative consumers rejoin the group immediately, and we want to
	// detect that in 500ms rather than 3s.
	var cooperativeFastCheck <-chan time.Time
	if g.cooperative {
		cooperativeFastCheck = time.After(500 * time.Millisecond)
	}

	var metadone, revoked <-chan struct{}
	var heartbeat, didMetadone, didRevoke bool
	var lastErr error

	ctxCh := g.ctx.Done()

	for {
		var err error
		heartbeat = false
		select {
		case <-cooperativeFastCheck:
			heartbeat = true
		case <-ticker.C:
			heartbeat = true
		case <-g.rejoinCh:
			// If a metadata update changes our subscription,
			// we just pretend we are rebalancing.
			err = kerr.RebalanceInProgress
		case err = <-fetchErrCh:
			fetchErrCh = nil
		case <-metadone:
			metadone = nil
			didMetadone = true
		case <-revoked:
			revoked = nil
			didRevoke = true
		case <-ctxCh:
			// Even if the group is left, we need to wait for our
			// revoke to finish before returning, otherwise the
			// manage goroutine will race with us setting
			// nowAssigned.
			ctxCh = nil
			err = context.Canceled
		}

		if heartbeat {
			g.cl.cfg.logger.Log(LogLevelDebug, "heartbeating")
			req := &kmsg.HeartbeatRequest{
				Group:      g.id,
				Generation: g.generation,
				MemberID:   g.memberID,
				InstanceID: g.instanceID,
			}
			var resp *kmsg.HeartbeatResponse
			if resp, err = req.RequestWith(g.ctx, g.cl); err == nil {
				err = kerr.ErrorForCode(resp.ErrorCode)
			}
			g.cl.cfg.logger.Log(LogLevelDebug, "heartbeat complete", "err", err)
		}

		// The first error either triggers a clean revoke and metadata
		// update or it returns immediately. If we triggered the
		// revoke, we wait for it to complete regardless of any future
		// error.
		if didMetadone && didRevoke {
			g.cl.cfg.logger.Log(LogLevelInfo, "heartbeat loop complete", "err", lastErr)
			return lastErr
		}

		if err == nil {
			continue
		}

		if lastErr == nil {
			g.cl.cfg.logger.Log(LogLevelInfo, "heartbeat errored", "err", err)
		} else {
			g.cl.cfg.logger.Log(LogLevelInfo, "heartbeat errored again while waiting for user revoke to finish", "err", err)
		}

		// Since we errored, we must revoke.
		if !didRevoke && revoked == nil {
			// If we are an eager consumer, we stop fetching all of
			// our current partitions as we will be revoking them.
			if !g.cooperative {
				g.c.mu.Lock()
				g.c.assignPartitions(nil, assignInvalidateAll)
				g.c.mu.Unlock()
			}

			// If our error is not from rebalancing, then we
			// encountered IllegalGeneration or UnknownMemberID or
			// our context closed all of which are unexpected and
			// unrecoverable.
			//
			// We return early rather than revoking and updating
			// metadata; the groupConsumer's manage function will
			// call onLost with all partitions.
			//
			// setupAssignedAndHeartbeat still waits for onAssigned
			// to be done so that we avoid calling onLost
			// concurrently.
			if err != kerr.RebalanceInProgress && revoked == nil {
				return err
			}

			// Now we call the user provided revoke callback, even
			// if cooperative: if cooperative, this only revokes
			// partitions we no longer want to consume.
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

// rejoin is called after a cooperative member revokes what it lost at the
// beginning of a session, or if we are leader and detect new partitions to
// consume.
func (g *groupConsumer) rejoin() {
	select {
	case g.rejoinCh <- struct{}{}:
	default:
	}
}

// Joins and then syncs, issuing the two slow requests in goroutines to allow
// for group cancelation to return early.
func (g *groupConsumer) joinAndSync() error {
	g.cl.cfg.logger.Log(LogLevelInfo, "joining group")
	g.leader.set(false)

start:
	select {
	case <-g.rejoinCh: // drain to avoid unnecessary rejoins
	default:
	}

	var (
		joinReq = &kmsg.JoinGroupRequest{
			Group:                  g.id,
			SessionTimeoutMillis:   int32(g.sessionTimeout.Milliseconds()),
			RebalanceTimeoutMillis: int32(g.rebalanceTimeout.Milliseconds()),
			ProtocolType:           g.protocol,
			MemberID:               g.memberID,
			InstanceID:             g.instanceID,
			Protocols:              g.joinGroupProtocols(),
		}

		joinResp *kmsg.JoinGroupResponse
		err      error
		joined   = make(chan struct{})
	)

	go func() {
		defer close(joined)
		joinResp, err = joinReq.RequestWith(g.ctx, g.cl)
	}()

	select {
	case <-joined:
	case <-g.ctx.Done():
		return g.ctx.Err() // group killed
	}
	if err != nil {
		return err
	}

	restart, protocol, plan, err := g.handleJoinResp(joinResp)
	if restart {
		goto start
	}
	if err != nil {
		g.cl.cfg.logger.Log(LogLevelWarn, "join group failed", "err", err)
		return err
	}

	var (
		syncReq = &kmsg.SyncGroupRequest{
			Group:           g.id,
			Generation:      g.generation,
			MemberID:        g.memberID,
			InstanceID:      g.instanceID,
			ProtocolType:    &g.protocol,
			Protocol:        &protocol,
			GroupAssignment: plan.intoAssignment(), // nil unless we are the leader
		}

		syncResp *kmsg.SyncGroupResponse
		synced   = make(chan struct{})
	)

	g.cl.cfg.logger.Log(LogLevelInfo, "syncing", "protocol_type", g.protocol, "protocol", protocol)
	go func() {
		defer close(synced)
		syncResp, err = syncReq.RequestWith(g.ctx, g.cl)
	}()

	select {
	case <-synced:
	case <-g.ctx.Done():
		return g.ctx.Err()
	}
	if err != nil {
		return err
	}

	if err = g.handleSyncResp(syncResp, plan); err != nil {
		if err == kerr.RebalanceInProgress {
			g.cl.cfg.logger.Log(LogLevelInfo, "sync failed with RebalanceInProgress, rejoining")
			goto start
		}
		g.cl.cfg.logger.Log(LogLevelWarn, "sync group failed", "err", err)
		return err
	}

	return nil
}

func (g *groupConsumer) handleJoinResp(resp *kmsg.JoinGroupResponse) (restart bool, protocol string, plan balancePlan, err error) {
	if err = kerr.ErrorForCode(resp.ErrorCode); err != nil {
		switch err {
		case kerr.MemberIDRequired:
			g.mu.Lock()
			g.memberID = resp.MemberID // KIP-394
			g.mu.Unlock()
			g.cl.cfg.logger.Log(LogLevelInfo, "join returned MemberIDRequired, rejoining with response's MemberID", "memberID", resp.MemberID)
			return true, "", nil, nil
		case kerr.UnknownMemberID:
			g.mu.Lock()
			g.memberID = ""
			g.mu.Unlock()
			g.cl.cfg.logger.Log(LogLevelInfo, "join returned UnknownMemberID, rejoining without a member id")
			return true, "", nil, nil
		}
		return // Request retries as necesary, so this must be a failure
	}

	// Concurrent committing, while erroneous to do at the moment, could
	// race with this function. We need to lock setting these two fields.
	g.mu.Lock()
	g.memberID = resp.MemberID
	g.generation = resp.Generation
	g.mu.Unlock()

	if resp.Protocol != nil {
		protocol = *resp.Protocol
	}

	leader := resp.LeaderID == resp.MemberID
	if leader {
		g.leader.set(true)
		g.cl.cfg.logger.Log(LogLevelInfo, "joined, balancing group",
			"memberID", g.memberID,
			"instanceID", g.instanceID,
			"generation", g.generation,
			"balance_protocol", protocol,
			"leader", true,
		)

		plan, err = g.balanceGroup(protocol, resp.Members)
		g.cl.cfg.logger.Log(LogLevelDebug, "balanced", "plan", plan)
		if err != nil {
			return
		}

	} else {
		g.cl.cfg.logger.Log(LogLevelInfo, "joined",
			"memberID", g.memberID,
			"instanceID", g.instanceID,
			"generation", g.generation,
			"leader", false,
		)
	}
	return
}

func (g *groupConsumer) handleSyncResp(resp *kmsg.SyncGroupResponse, plan balancePlan) error {
	if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
		return err
	}

	kassignment := new(kmsg.GroupMemberAssignment)
	if err := kassignment.ReadFrom(resp.MemberAssignment); err != nil {
		g.cl.cfg.logger.Log(LogLevelError, "sync assignment parse failed", "err", err)
		return err
	}

	g.cl.cfg.logger.Log(LogLevelDebug, "synced", "assigned", kassignment.Topics)

	// Past this point, we will fall into the setupAssigned prerevoke code,
	// meaning for cooperative, we will revoke what we need to.
	if g.cooperative {
		g.lastAssigned = g.nowAssigned
	}
	g.nowAssigned = make(map[string][]int32)
	for _, topic := range kassignment.Topics {
		g.nowAssigned[topic.Topic] = topic.Partitions
	}
	g.cl.cfg.logger.Log(LogLevelInfo, "synced successfully", "assigned", g.nowAssigned)
	return nil
}

func (g *groupConsumer) joinGroupProtocols() []kmsg.JoinGroupRequestProtocol {
	g.mu.Lock()
	topics := make([]string, 0, len(g.using))
	for topic := range g.using {
		topics = append(topics, topic)
	}
	g.mu.Unlock()
	var protos []kmsg.JoinGroupRequestProtocol
	for _, balancer := range g.balancers {
		protos = append(protos, kmsg.JoinGroupRequestProtocol{
			Name: balancer.protocolName(),
			Metadata: balancer.metaFor(
				topics,
				g.nowAssigned,
				g.generation,
			),
		})
	}
	return protos
}

// fetchOffsets is issued once we join a group to see what the prior commits
// were for the partitions we were assigned.
func (g *groupConsumer) fetchOffsets(ctx context.Context, newAssigned map[string][]int32) error {
start:
	req := kmsg.OffsetFetchRequest{
		Group:         g.id,
		RequireStable: g.requireStable,
	}
	for topic, partitions := range newAssigned {
		req.Topics = append(req.Topics, kmsg.OffsetFetchRequestTopic{
			Topic:      topic,
			Partitions: partitions,
		})
	}

	var (
		resp *kmsg.OffsetFetchResponse
		err  error
	)

	fetchDone := make(chan struct{})
	go func() {
		defer close(fetchDone)
		resp, err = req.RequestWith(ctx, g.cl)
	}()
	select {
	case <-fetchDone:
	case <-ctx.Done():
		err = ctx.Err()
	}
	if err != nil {
		g.cl.cfg.logger.Log(LogLevelError, "fetch offsets failed with non-retriable error", "err", err)
		return err
	}

	offsets := make(map[string]map[int32]Offset)
	for _, rTopic := range resp.Topics {
		topicOffsets := make(map[int32]Offset)
		offsets[rTopic.Topic] = topicOffsets
		for _, rPartition := range rTopic.Partitions {
			if err = kerr.ErrorForCode(rPartition.ErrorCode); err != nil {
				// KIP-447: Unstable offset commit means there is a
				// pending transaction that should be committing soon.
				// We sleep for 1s and retry fetching offsets.
				if err == kerr.UnstableOffsetCommit {
					g.cl.cfg.logger.Log(LogLevelInfo, "fetch offsets failed with UnstableOffsetCommit, waiting 1s and retrying")
					select {
					case <-ctx.Done():
					case <-time.After(time.Second):
						goto start
					}
				}
				return err
			}
			offset := Offset{
				at:    rPartition.Offset,
				epoch: -1,
			}
			if resp.Version >= 5 { // KIP-320
				offset.epoch = rPartition.LeaderEpoch
			}
			if rPartition.Offset == -1 {
				offset = g.cl.cfg.resetOffset
			}
			topicOffsets[rPartition.Partition] = offset
		}
	}

	clientTopics := g.c.cl.loadTopics()
	for fetchedTopic := range offsets {
		if _, exists := clientTopics[fetchedTopic]; !exists {
			delete(offsets, fetchedTopic)
			g.cl.cfg.logger.Log(LogLevelError, "BUG! member was assigned topic that we did not ask for in AssignGroup! skipping assigning this topic!", "topic", fetchedTopic)
		}
	}

	// Lock for assign and then updating uncommitted.
	g.c.mu.Lock()
	defer g.c.mu.Unlock()
	g.mu.Lock()
	defer g.mu.Unlock()

	// Eager: we already invalidated everything; nothing to re-invalidate.
	// Cooperative: assign without invalidating what we are consuming.
	g.c.assignPartitions(offsets, assignWithoutInvalidating)

	// We need to update the uncommited map so that SetOffsets(Committed)
	// does not rewind before the committed offsets we just fetched.
	if g.uncommitted == nil {
		g.uncommitted = make(uncommitted, 10)
	}
	for topic, partitions := range offsets {
		topicUncommitted := g.uncommitted[topic]
		if topicUncommitted == nil {
			topicUncommitted = make(map[int32]uncommit, 20)
			g.uncommitted[topic] = topicUncommitted
		}
		for partition, offset := range partitions {
			if offset.at < 0 {
				continue // not yet committed
			}
			committed := EpochOffset{
				Epoch:  offset.epoch,
				Offset: offset.at,
			}
			topicUncommitted[partition] = uncommit{
				head:      committed,
				committed: committed,
			}
		}
	}

	if g.cl.cfg.logger.Level() >= LogLevelDebug {
		g.cl.cfg.logger.Log(LogLevelDebug, "fetched committed offsets", "fetched", offsets)
	} else {
		g.cl.cfg.logger.Log(LogLevelInfo, "fetched committed offsets")
	}
	return nil
}

// findNewAssignments updates topics the group wants to use and other metadata.
// We only grab the group mu at the end if we need to.
//
// This joins the group if
//  - the group has never been joined
//  - new topics are found for consuming (changing this consumer's join metadata)
//
// Additionally, if the member is the leader, this rejoins the group if the
// leader notices new partitions in an existing topic.
//
// This does not rejoin if the leader notices a partition is lost, which is
// finicky.
func (g *groupConsumer) findNewAssignments(topics map[string]*topicPartitions) {
	type change struct {
		isNew bool
		delta int
	}

	var numNewTopics int
	toChange := make(map[string]change, len(topics))
	for topic, topicPartitions := range topics {
		numPartitions := len(topicPartitions.load().partitions)
		// If we are already using this topic, add that it changed if
		// there are more partitions than we were using prior.
		if used, exists := g.using[topic]; exists {
			if added := numPartitions - used; added > 0 {
				toChange[topic] = change{delta: added}
			}
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

		// We only track using the topic if there are partitions for
		// it; if there are none, then the topic was set by _us_ as "we
		// want to load the metadata", but the topic was not returned
		// in the metadata (or it was returned with an error).
		if useTopic && numPartitions > 0 {
			if g.regexTopics && topicPartitions.load().isInternal {
				continue
			}
			toChange[topic] = change{isNew: true, delta: numPartitions}
			numNewTopics++
		}

	}

	if len(toChange) == 0 {
		return
	}

	g.mu.Lock()
	defer g.mu.Unlock()

	if g.dying {
		return
	}

	wasManaging := len(g.using) != 0
	for topic, change := range toChange {
		g.using[topic] += change.delta
	}

	if !wasManaging {
		go g.manage()
		return
	}

	if numNewTopics > 0 || g.leader.get() {
		g.rejoin()
	}
}

// uncommit tracks the latest offset polled (+1) and the latest commit.
// The reason head is just past the latest offset is because we want
// to commit TO an offset, not BEFORE an offset.
type uncommit struct {
	head      EpochOffset
	committed EpochOffset
}

// EpochOffset combines a record offset with the leader epoch the broker
// was at when the record was written.
type EpochOffset struct {
	Epoch  int32
	Offset int64
}

type uncommitted map[string]map[int32]uncommit

// updateUncommitted sets the latest uncommitted offset.
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
				final := partition.Records[len(partition.Records)-1]

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
				uncommit := topicOffsets[partition.Partition]
				// Our new head points just past the final consumed offset,
				// that is, if we rejoin, this is the offset to begin at.
				newOffset := final.Offset + 1
				uncommit.head = EpochOffset{
					final.LeaderEpoch, // -1 if old message / unknown
					newOffset,
				}
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

	if req.Generation != g.generation {
		return
	}
	if g.uncommitted == nil || // just in case
		len(req.Topics) != len(resp.Topics) { // bad kafka
		g.cl.cfg.logger.Log(LogLevelError, fmt.Sprintf("Kafka replied to our OffsetCommitRequest incorrectly! Num topics in request: %d, in reply: %d, we cannot handle this!", len(req.Topics), len(resp.Topics)))
		return
	}

	sort.Slice(req.Topics, func(i, j int) bool {
		return req.Topics[i].Topic < req.Topics[j].Topic
	})
	sort.Slice(resp.Topics, func(i, j int) bool {
		return resp.Topics[i].Topic < resp.Topics[j].Topic
	})

	for i := range resp.Topics {
		reqTopic := &req.Topics[i]
		respTopic := &resp.Topics[i]
		topic := g.uncommitted[respTopic.Topic]
		if topic == nil || // just in case
			reqTopic.Topic != respTopic.Topic || // bad kafka
			len(reqTopic.Partitions) != len(respTopic.Partitions) { // same
			g.cl.cfg.logger.Log(LogLevelError, fmt.Sprintf("Kafka replied to our OffsetCommitRequest incorrectly! Topic at request index %d: %s, reply at index: %s; num partitions on request topic: %d, in reply: %d, we cannot handle this!", i, reqTopic.Topic, respTopic.Topic, len(reqTopic.Partitions), len(respTopic.Partitions)))
			continue
		}

		sort.Slice(reqTopic.Partitions, func(i, j int) bool {
			return reqTopic.Partitions[i].Partition < reqTopic.Partitions[j].Partition
		})
		sort.Slice(respTopic.Partitions, func(i, j int) bool {
			return respTopic.Partitions[i].Partition < respTopic.Partitions[j].Partition
		})

		for i := range respTopic.Partitions {
			reqPart := &reqTopic.Partitions[i]
			respPart := &respTopic.Partitions[i]
			uncommit, exists := topic[respPart.Partition]
			if !exists { // just in case
				continue
			}
			if reqPart.Partition != respPart.Partition { // bad kafka
				g.cl.cfg.logger.Log(LogLevelError, fmt.Sprintf("Kafka replied to our OffsetCommitRequest incorrectly! Topic %s partition %d != resp partition %d", reqTopic.Topic, reqPart.Partition, respPart.Partition))
				continue
			}
			if respPart.ErrorCode != 0 {
				g.cl.cfg.logger.Log(LogLevelWarn, "unable to commit offset for topic partition", "topic", reqTopic.Topic, "partition", reqPart.Partition, "error_code", respPart.ErrorCode)
				continue
			}

			uncommit.committed = EpochOffset{
				reqPart.LeaderEpoch,
				reqPart.Offset,
			}
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
			g.cl.cfg.logger.Log(LogLevelDebug, "autocommitting")
			g.commit(context.Background(), g.getUncommittedLocked(true), func(_ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
				if err != nil {
					if err != context.Canceled {
						g.cl.cfg.logger.Log(LogLevelError, "autocommit failed", "err", err)
					} else {
						g.cl.cfg.logger.Log(LogLevelDebug, "autocommit canceled")
					}
					return
				}
				for _, topic := range resp.Topics {
					for _, partition := range topic.Partitions {
						if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
							g.cl.cfg.logger.Log(LogLevelError, "in autocommit: unable to commit offsets for topic partition",
								"topic", topic.Topic,
								"partition", partition.Partition)
						}
					}
				}
			})
		}
		g.mu.Unlock()
	}
}

// SetOffsets, for consumer groups, sets any matching offsets in setOffsets to
// the given epoch/offset. Partitions that are not specified are not set. It is
// invalid to set topics that were not yet returned from a PollFetches.
//
// If using transactions, it is advised to just use a GroupTransactSession and
// avoid this function entirely.
//
// It is strongly recommended to use this function outside of the context of a
// PollFetches loop and only when you know the group is not revoked (i.e.,
// block any concurrent revoke while issuing this call). Any other usage is
// prone to odd interactions.
func (cl *Client) SetOffsets(setOffsets map[string]map[int32]EpochOffset) {
	if len(setOffsets) == 0 {
		return
	}

	// We assignPartitions before returning, so we grab the consumer lock
	// first to preserve consumer mu => group mu ordering.
	c := &cl.consumer
	c.mu.Lock()
	defer c.mu.Unlock()

	g, ok := c.loadGroup()
	if !ok {
		return
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	clientTopics := cl.loadTopics()

	// The gist of what follows:
	//
	// We need to set uncommitted.committed; that is the guarantee of this
	// function. However, if, for everything we are setting, the head equals
	// the commit, then we do not need to actually invalidate our current
	// assignments or buffered fetches.
	//
	// We only initialize the assigns map if we need to invalidate.
	var assigns map[string]map[int32]Offset
	if g.uncommitted == nil {
		g.uncommitted = make(uncommitted)
	}
	for topic, partitions := range setOffsets {
		if clientTopics[topic].load() == nil {
			continue // trying to set a topic that was not assigned...
		}
		topicUncommitted := g.uncommitted[topic]
		if topicUncommitted == nil {
			topicUncommitted = make(map[int32]uncommit)
			g.uncommitted[topic] = topicUncommitted
		}
		var topicAssigns map[int32]Offset
		for partition, epochOffset := range partitions {
			// If we are setting the offset to the head, then we do
			// not need to invalidate anything we have buffered.
			// Ideal optimization for transactions.
			current, exists := topicUncommitted[partition]
			if exists && current.head == epochOffset {
				current.committed = epochOffset
				topicUncommitted[partition] = current
				continue
			}
			if topicAssigns == nil {
				topicAssigns = make(map[int32]Offset, len(partitions))
			}
			topicAssigns[partition] = Offset{
				at:    epochOffset.Offset,
				epoch: epochOffset.Epoch,
			}
			topicUncommitted[partition] = uncommit{
				head:      epochOffset,
				committed: epochOffset,
			}
		}
		if len(topicAssigns) > 0 {
			if assigns == nil {
				assigns = make(map[string]map[int32]Offset, 10)
			}
			assigns[topic] = topicAssigns
		}
	}

	if len(assigns) == 0 {
		return
	}

	c.assignPartitions(assigns, assignSetMatching)
}

// UncommittedOffsets returns the latest uncommitted offsets. Uncommitted
// offsets are always updated on calls to PollFetches.
//
// If there are no uncommitted offsets, this returns nil.
//
// Note that, if manually committing, you should be careful with committing
// during group rebalances. You must ensure you commit before the group's
// session timeout is reached, otherwise this client will be kicked from the
// group and the commit will fail.
//
// If using a cooperative balancer, commits while consuming during rebalancing
// may fail with REBALANCE_IN_PROGRESS.
func (cl *Client) UncommittedOffsets() map[string]map[int32]EpochOffset {
	g, ok := cl.consumer.loadGroup()
	if !ok {
		return nil
	}
	return g.getUncommitted()
}

// CommittedOffsets returns the latest committed offsets. Committed offsets are
// updated from commits or from joining a group and fetching offsets.
//
// If there are no committed offsets, this returns nil.
func (cl *Client) CommittedOffsets() map[string]map[int32]EpochOffset {
	g, ok := cl.consumer.loadGroup()
	if !ok {
		return nil
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	return g.getUncommittedLocked(false)
}

func (g *groupConsumer) getUncommitted() map[string]map[int32]EpochOffset {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.getUncommittedLocked(true)
}

func (g *groupConsumer) getUncommittedLocked(head bool) map[string]map[int32]EpochOffset {
	if g.uncommitted == nil {
		return nil
	}

	var uncommitted map[string]map[int32]EpochOffset
	for topic, partitions := range g.uncommitted {
		var topicUncommitted map[int32]EpochOffset
		for partition, uncommit := range partitions {
			if head && uncommit.head == uncommit.committed {
				continue
			}
			if topicUncommitted == nil {
				if uncommitted == nil {
					uncommitted = make(map[string]map[int32]EpochOffset, len(g.uncommitted))
				}
				topicUncommitted = uncommitted[topic]
				if topicUncommitted == nil {
					topicUncommitted = make(map[int32]EpochOffset, len(partitions))
					uncommitted[topic] = topicUncommitted
				}
			}
			if head {
				topicUncommitted[partition] = uncommit.head
			} else {
				topicUncommitted[partition] = uncommit.committed
			}
		}
	}
	return uncommitted
}

// BlockingCommitOffsets cancels any active CommitOffsets, begins a commit that
// cannot be canceled, and waits for that commit to complete. This function
// will not return until the commit is done and the onDone callback is
// complete.
//
// The purpose of this function is for use in OnRevoke or committing before
// leaving a group.
//
// For OnRevoke, you do not want to have a commit in OnRevoke canceled, because
// once the commit is done, rebalancing will continue. If you cancel an
// OnRevoke commit and commit after the revoke, you will be committing for a
// stale session, the commit will be dropped, and you will likely doubly
// process records.
//
// For more information about committing, see the documentation for
// CommitOffsets.
func (cl *Client) BlockingCommitOffsets(
	ctx context.Context,
	uncommitted map[string]map[int32]EpochOffset,
	onDone func(*kmsg.OffsetCommitRequest, *kmsg.OffsetCommitResponse, error),
) {
	done := make(chan struct{})
	defer func() { <-done }()

	cl.cfg.logger.Log(LogLevelDebug, "in BlockingCommitOffsets", "with", uncommitted)
	defer cl.cfg.logger.Log(LogLevelDebug, "left BlockingCommitOffsets")

	if onDone == nil {
		onDone = func(_ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, _ error) {}
	}

	g, ok := cl.consumer.loadGroup()
	if !ok {
		onDone(new(kmsg.OffsetCommitRequest), new(kmsg.OffsetCommitResponse), ErrNotGroup)
		close(done)
		return
	}
	if len(uncommitted) == 0 {
		onDone(new(kmsg.OffsetCommitRequest), new(kmsg.OffsetCommitResponse), nil)
		close(done)
		return
	}

	g.blockingCommitMu.Lock() // block all other concurrent commits until our OnDone is done.

	unblockCommits := func(req *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
		defer close(done)
		defer g.blockingCommitMu.Unlock()
		onDone(req, resp, err)
	}

	g.mu.Lock()
	go func() {
		defer g.mu.Unlock()

		g.blockAuto = true
		unblockAuto := func(req *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
			unblockCommits(req, resp, err)
			g.mu.Lock()
			defer g.mu.Unlock()
			g.blockAuto = false
		}

		g.commit(ctx, uncommitted, unblockAuto)
	}()
}

// CommitOffsets commits the given offsets for a group, calling onDone with the
// commit request and either the response or an error if the response was not
// issued. If uncommitted is empty or the client is not consuming as a group,
// onDone is called with (nil, nil, nil) and this function returns immediately.
// It is OK if onDone is nil, but you will not know if your commit succeeded.
//
// If autocommitting is enabled, this function blocks autocommitting until this
// function is complete and the onDone has returned.
//
// This function itself does not wait for the commit to finish. By default,
// this function is an asyncronous commit. You can use onDone to make it sync.
//
// Note that this function ensures absolute ordering of commit requests by
// canceling prior requests and ensuring they are done before executing a new
// one. This means, for absolute control, you can use this function to
// periodically commit async and then issue a final sync commit before quitting
// (this is the behavior of autocommiting and using the default revoke). This
// differs from the Java async commit, which does not retry requests to avoid
// trampling on future commits.
//
// If using autocommitting, autocommitting will resume once this is complete.
//
// It is invalid to use this function to commit offsets for a transaction.
//
// It is highly recommended to check the response's partition's error codes if
// the response is non-nil. While unlikely, individual partitions can error.
// This is most likely to happen if a commit occurs too late in a rebalance
// event.
//
// If manually committing, you want to set OnRevoked to commit syncronously
// using BlockingCommitOffsets. Otherwise if committing async OnRevoked may
// return and a new group session may start before the commit is issued,
// leading to the commit being ignored and leading to duplicate messages.
func (cl *Client) CommitOffsets(
	ctx context.Context,
	uncommitted map[string]map[int32]EpochOffset,
	onDone func(*kmsg.OffsetCommitRequest, *kmsg.OffsetCommitResponse, error),
) {
	cl.cfg.logger.Log(LogLevelDebug, "in CommitOffsets", "with", uncommitted)
	defer cl.cfg.logger.Log(LogLevelDebug, "left CommitOffsets")
	if onDone == nil {
		onDone = func(_ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, _ error) {}
	}

	g, ok := cl.consumer.loadGroup()
	if !ok {
		onDone(new(kmsg.OffsetCommitRequest), new(kmsg.OffsetCommitResponse), ErrNotGroup)
		return
	}
	if len(uncommitted) == 0 {
		onDone(new(kmsg.OffsetCommitRequest), new(kmsg.OffsetCommitResponse), nil)
		return
	}

	g.blockingCommitMu.RLock() // block BlockingCommit, but allow other concurrent Commit to cancel us

	unblockSyncCommit := func(req *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
		defer g.blockingCommitMu.RUnlock()
		onDone(req, resp, err)
	}

	g.mu.Lock()
	go func() {
		defer g.mu.Unlock()

		g.blockAuto = true
		unblockAuto := func(req *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
			unblockSyncCommit(req, resp, err)
			g.mu.Lock()
			defer g.mu.Unlock()
			g.blockAuto = false
		}

		g.commit(ctx, uncommitted, unblockAuto)
	}()
}

// defaultRevoke commits the last fetched offsets and waits for the commit to
// finish. This is the default onRevoked function which, when combined with the
// default autocommit, ensures we never miss committing everything.
//
// Note that the heartbeat loop invalidates all buffered, unpolled fetches
// before revoking, meaning this truly will commit all polled fetches.
func (g *groupConsumer) defaultRevoke(_ context.Context, _ map[string][]int32) {
	if !g.autocommitDisable {
		un := g.getUncommitted()
		g.cl.BlockingCommitOffsets(g.ctx, un, func(_ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
			if err != nil {
				if err != ErrNotGroup && err != context.Canceled {
					g.cl.cfg.logger.Log(LogLevelError, "default revoke BlockingCommitOffsets failed", "err", err)
				}
				return
			}
			for _, topic := range resp.Topics {
				for _, partition := range topic.Partitions {
					if err := kerr.ErrorForCode(partition.ErrorCode); err != nil {
						g.cl.cfg.logger.Log(LogLevelError, "in revoke: unable to commit offsets for topic partition",
							"topic", topic.Topic,
							"partition", partition.Partition)
					}
				}
			}
		})
	}
}

// commit is the logic for Commit; see Commit's documentation
//
// This is called under the groupConsumer's lock.
func (g *groupConsumer) commit(
	ctx context.Context,
	uncommitted map[string]map[int32]EpochOffset,
	onDone func(*kmsg.OffsetCommitRequest, *kmsg.OffsetCommitResponse, error),
) {
	if onDone == nil { // note we must always call onDone
		onDone = func(_ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, _ error) {}
	}
	if len(uncommitted) == 0 { // only empty if called thru autocommit / default revoke
		onDone(new(kmsg.OffsetCommitRequest), new(kmsg.OffsetCommitResponse), nil)
		return
	}

	priorCancel := g.commitCancel
	priorDone := g.commitDone

	commitCtx, commitCancel := context.WithCancel(g.ctx) // enable ours to be canceled and waited for
	commitDone := make(chan struct{})

	g.commitCancel = commitCancel
	g.commitDone = commitDone

	req := &kmsg.OffsetCommitRequest{
		Group:      g.id,
		Generation: g.generation,
		MemberID:   g.memberID,
		InstanceID: g.instanceID,
	}

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
			select {
			case <-priorDone:
			default:
				g.cl.cfg.logger.Log(LogLevelDebug, "canceling prior commit to issue another")
				priorCancel()
				<-priorDone
			}
		}
		g.cl.cfg.logger.Log(LogLevelDebug, "issuing commit", "uncommitted", uncommitted)

		for topic, partitions := range uncommitted {
			req.Topics = append(req.Topics, kmsg.OffsetCommitRequestTopic{
				Topic: topic,
			})
			reqTopic := &req.Topics[len(req.Topics)-1]
			for partition, eo := range partitions {
				reqTopic.Partitions = append(reqTopic.Partitions, kmsg.OffsetCommitRequestTopicPartition{
					Partition:   partition,
					Offset:      eo.Offset,
					LeaderEpoch: eo.Epoch, // KIP-320
					Metadata:    &req.MemberID,
				})
			}
		}

		resp, err := req.RequestWith(commitCtx, g.cl)
		if err != nil {
			onDone(req, nil, err)
			return
		}
		g.updateCommitted(req, resp)
		onDone(req, resp, nil)
	}()
}
