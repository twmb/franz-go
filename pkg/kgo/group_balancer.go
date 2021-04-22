package kgo

import (
	"errors"
	"fmt"
	"sort"
	"strings"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo/internal/sticky"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// GroupBalancer balances topics and partitions among group members.
type GroupBalancer interface {
	// protocolName returns the name of the protocol, e.g. roundrobin,
	// range, sticky.
	protocolName() string // "sticky"

	// metaFor returns the userdata to use in JoinGroup, given the topic
	// interests and the current assignment.
	metaFor(
		interests []string,
		currentAssignment map[string][]int32,
		generation int32,
	) []byte

	// balance balances topics and partitions among group members.
	//
	// The input members are guaranteed to be sorted by member ID, and
	// each member's topics are guaranteed to be sorted.
	balance(members []groupMember, topics map[string]int32) balancePlan

	// isCooperative returns if this is a cooperative balance strategy.
	isCooperative() bool
}

// groupMember is a member id and the topics that member is interested in.
type groupMember struct {
	id       groupMemberID
	version  int16
	topics   []string
	userdata []byte

	owned []kmsg.GroupMemberMetadataOwnedPartition
}

func (m *groupMember) balanceInterests() string {
	var sb strings.Builder
	sb.WriteString("interested topics: ")
	fmt.Fprintf(&sb, "%v", m.topics)
	sb.WriteString(", previously owned: ")
	for i, owned := range m.owned {
		fmt.Fprintf(&sb, "%s%v", owned.Topic, owned.Partitions)
		if i < len(m.owned) {
			sb.WriteString(", ")
		}
	}
	return sb.String()
}

type groupMemberID struct {
	memberID    string
	instanceID  string
	hasInstance bool
}

func (id *groupMemberID) String() string {
	if id.hasInstance {
		return id.memberID + "(" + id.instanceID + ")"
	}
	return id.memberID
}

func (me groupMemberID) less(other groupMemberID) bool {
	if me.hasInstance && other.hasInstance {
		return me.instanceID < other.instanceID
	} else if me.hasInstance {
		return true
	} else if other.hasInstance {
		return false
	} else {
		return me.memberID < other.memberID
	}
}

// balancePlan is the result of balancing topic partitions among members.
//
// member id => topic => partitions
type balancePlan map[groupMemberID]map[string][]int32

func (p balancePlan) String() string {
	var sb strings.Builder

	var membersWritten int
	for member, topics := range p {
		membersWritten++
		sb.WriteString(member.String())
		sb.WriteString("{")

		var topicsWritten int
		for topic, partitions := range topics {
			fmt.Fprintf(&sb, "%s%v", topic, partitions)
			topicsWritten++
			if topicsWritten < len(topics) {
				sb.WriteString(", ")
			}
		}

		sb.WriteString("}")
		if membersWritten < len(p) {
			sb.WriteString(", ")
		}
	}

	return sb.String()
}

func newBalancePlan(members []groupMember) balancePlan {
	plan := make(map[groupMemberID]map[string][]int32, len(members))
	for i := range members {
		plan[members[i].id] = make(map[string][]int32)
	}
	return plan
}

func (plan balancePlan) addPartition(member groupMemberID, topic string, partition int32) {
	memberPlan := plan[member]
	memberPlan[topic] = append(memberPlan[topic], partition)
}
func (plan balancePlan) addPartitions(member groupMemberID, topic string, partitions []int32) {
	memberPlan := plan[member]
	memberPlan[topic] = append(memberPlan[topic], partitions...)
}

// intoAssignment translates a balance plan to the kmsg equivalent type.
func (plan balancePlan) intoAssignment() []kmsg.SyncGroupRequestGroupAssignment {
	kassignments := make([]kmsg.SyncGroupRequestGroupAssignment, 0, len(plan))
	for member, assignment := range plan {
		var kassignment kmsg.GroupMemberAssignment
		for topic, partitions := range assignment {
			kassignment.Topics = append(kassignment.Topics, kmsg.GroupMemberAssignmentTopic{
				Topic:      topic,
				Partitions: partitions,
			})
		}
		kassignments = append(kassignments, kmsg.SyncGroupRequestGroupAssignment{
			MemberID:         member.memberID,
			MemberAssignment: kassignment.AppendTo(nil),
		})
	}
	return kassignments
}

// balanceGroup returns a balancePlan from a join group response.
func (g *groupConsumer) balanceGroup(proto string, kmembers []kmsg.JoinGroupResponseMember) (balancePlan, error) {
	members, err := parseGroupMembers(kmembers)
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, errors.New("invalidly empty balance members")
	}
	sort.Slice(members, func(i, j int) bool {
		return members[i].id.less(members[j].id) // guarantee sorted members
	})

	myTopics := g.tps.load()
	allTopics := make(map[string]struct{})
	var needMeta bool

	g.cl.cfg.logger.Log(LogLevelInfo, "balancing group as leader")
	for i := range members {
		m := &members[i]
		sort.Strings(m.topics) // guarantee sorted topics
		g.cl.cfg.logger.Log(LogLevelInfo, "balance group member", "id", m.id.String(), "interests", m.balanceInterests())

		for _, topic := range m.topics {
			allTopics[topic] = struct{}{}
			if _, exists := myTopics[topic]; !exists {
				needMeta = true
			}
		}
	}

	topicPartitionCount := make(map[string]int32, len(myTopics))
	for topic, data := range myTopics {
		topicPartitionCount[topic] = int32(len(data.load().partitions))
	}
	if needMeta {
		g.cl.cfg.logger.Log(LogLevelInfo, "group members indicated interest in topics the leader is not assigned, fetching metadata for all group topics")
		var metaTopics []string
		for topic := range allTopics {
			metaTopics = append(metaTopics, topic)
		}

		_, resp, err := g.cl.fetchMetadataForTopics(g.ctx, false, metaTopics)
		if err != nil {
			return nil, fmt.Errorf("unable to fetch metadata for group topics: %v", err)
		}
		for i := range resp.Topics {
			t := &resp.Topics[i]
			if t.ErrorCode != 0 {
				g.cl.cfg.logger.Log(LogLevelWarn, "metadata resp in balance for topic has error, skipping...", "topic", t.Topic, "err", kerr.ErrorForCode(t.ErrorCode))
				continue
			}
			topicPartitionCount[t.Topic] = int32(len(t.Partitions))
		}
	}

	for _, balancer := range g.balancers {
		if balancer.protocolName() == proto {
			plan := balancer.balance(members, topicPartitionCount)
			g.cl.cfg.logger.Log(LogLevelInfo, "balanced", "plan", plan.String())
			return plan, nil
		}
	}
	return nil, errors.New("unable to balance: none of our balances have a name equal to the balancer chosen for balancing")
}

// parseGroupMembers takes the raw data in from a join group response and
// returns the parsed group members.
func parseGroupMembers(kmembers []kmsg.JoinGroupResponseMember) ([]groupMember, error) {
	members := make([]groupMember, 0, len(kmembers))
	for _, kmember := range kmembers {
		var meta kmsg.GroupMemberMetadata
		if err := meta.ReadFrom(kmember.ProtocolMetadata); err != nil {
			return nil, fmt.Errorf("unable to read member metadata: %v", err)
		}
		id := groupMemberID{
			memberID: kmember.MemberID,
		}
		if kmember.InstanceID != nil {
			id.instanceID = *kmember.InstanceID
			id.hasInstance = true
		}
		members = append(members, groupMember{
			id:       id,
			version:  meta.Version,
			topics:   meta.Topics,
			userdata: meta.UserData,
			owned:    meta.OwnedPartitions,
		})
	}
	return members, nil
}

func basicMetaFor(interests []string) []byte {
	return (&kmsg.GroupMemberMetadata{
		Version: 0,
		Topics:  interests,
	}).AppendTo(nil)
}

///////////////////
// Balance Plans //
///////////////////

// RoundRobinBalancer returns a group balancer that evenly maps topics and
// partitions to group members.
//
// Suppose there are two members M0 and M1, two topics t0 and t1, and each
// topic has three partitions p0, p1, and p2. The partition balancing will be
//
//     M0: [t0p0, t0p2, t1p1]
//     M1: [t0p1, t1p0, t1p2]
//
// If all members subscribe to all topics equally, the roundrobin balancer
// will give a perfect balance. However, if topic subscriptions are quite
// unequal, the roundrobin balancer may lead to a bad balance. See KIP-49
// for one example (note that the fair strategy mentioned in KIP-49 does
// not exist).
//
// This is equivalent to the Java roundrobin balancer.
func RoundRobinBalancer() GroupBalancer {
	return new(roundRobinBalancer)
}

type roundRobinBalancer struct{}

func (*roundRobinBalancer) protocolName() string { return "roundrobin" }
func (*roundRobinBalancer) isCooperative() bool  { return false }
func (*roundRobinBalancer) metaFor(interests []string, _ map[string][]int32, _ int32) []byte {
	return basicMetaFor(interests)
}
func (*roundRobinBalancer) balance(members []groupMember, topics map[string]int32) balancePlan {
	// Get all the topics all members are subscribed to.
	memberTopics := make(map[string]struct{}, len(topics))
	for i := range members {
		for _, topic := range members[i].topics {
			memberTopics[topic] = struct{}{}
		}
	}

	type topicPartition struct {
		topic     string
		partition int32
	}
	var nparts int
	for _, partitions := range topics {
		nparts += int(partitions)
	}
	// Order all partitions available to balance, filtering out those that
	// no members are subscribed to.
	allParts := make([]topicPartition, 0, nparts)
	for topic := range memberTopics {
		for partition := int32(0); partition < topics[topic]; partition++ {
			allParts = append(allParts, topicPartition{
				topic,
				partition,
			})
		}
	}
	sort.Slice(allParts, func(i, j int) bool {
		l, r := allParts[i], allParts[j]
		return l.topic < r.topic || l.topic == r.topic && l.partition < r.partition
	})

	plan := newBalancePlan(members)
	// While parts are unassigned, assign them.
	var memberIdx int
	for len(allParts) > 0 {
		next := allParts[0]
		allParts = allParts[1:]

		// The Java roundrobin strategy walks members circularly until
		// a member can take this partition, and then starts the next
		// partition where the circular iterator left off.
	assigned:
		for {
			member := members[memberIdx]
			memberIdx = (memberIdx + 1) % len(members)
			for _, topic := range member.topics {
				if topic == next.topic {
					plan.addPartition(member.id, next.topic, next.partition)
					break assigned
				}
			}
		}
	}

	return plan
}

// RangeBalancer returns a group balancer that, per topic, maps partitions to
// group members. Since this works on a topic level, uneven partitions per
// topic to the number of members can lead to slight partition consumption
// disparities.
//
// Suppose there are two members M0 and M1, two topics t0 and t1, and each
// topic has three partitions p0, p1, and p2. The partition balancing will be
//
//     M0: [t0p0, t0p1, t1p0, t1p1]
//     M1: [t0p2, t1p2]
//
// This is equivalent to the Java range balancer.
func RangeBalancer() GroupBalancer {
	return new(rangeBalancer)
}

type rangeBalancer struct{}

func (*rangeBalancer) protocolName() string { return "range" }
func (*rangeBalancer) isCooperative() bool  { return false }
func (*rangeBalancer) metaFor(interests []string, _ map[string][]int32, _ int32) []byte {
	return basicMetaFor(interests)
}
func (*rangeBalancer) balance(members []groupMember, topics map[string]int32) balancePlan {
	topics2PotentialConsumers := make(map[string][]groupMemberID)
	for i := range members {
		member := &members[i]
		for _, topic := range member.topics {
			topics2PotentialConsumers[topic] = append(topics2PotentialConsumers[topic], member.id)
		}
	}

	plan := newBalancePlan(members)
	for topic, potentialConsumers := range topics2PotentialConsumers {
		sort.Slice(potentialConsumers, func(i, j int) bool {
			return potentialConsumers[i].less(potentialConsumers[j])
		})

		numPartitions := topics[topic]
		partitions := make([]int32, numPartitions)
		for i := range partitions {
			partitions[i] = int32(i)
		}
		numParts := len(partitions)
		div, rem := numParts/len(potentialConsumers), numParts%len(potentialConsumers)

		var consumerIdx int
		for len(partitions) > 0 {
			num := div
			if rem > 0 {
				num++
				rem--
			}

			member := potentialConsumers[consumerIdx]
			plan.addPartitions(member, topic, partitions[:num])

			consumerIdx++
			partitions = partitions[num:]
		}
	}

	return plan
}

// StickyBalancer returns a group balancer that ensures minimal partition
// movement on group changes while also ensuring optimal balancing.
//
// Suppose there are three members M0, M1, and M3, and two topics t0 and t1
// each with three partitions p0, p1, and p2. If the initial balance plan looks
// like
//
//     M0: [t0p0, t0p1, t0p2]
//     M1: [t1p0, t1p1, t1p2]
//     M2: [t2p0, t2p2, t2p2]
//
// If M2 disappears, both roundrobin and range would have mostly destructive
// reassignments.
//
// Range would result in
//
//     M0: [t0p0, t0p1, t1p0, t1p1, t2p0, t2p1]
//     M1: [t0p2, t1p2, t2p2]
//
// which is imbalanced and has 3 partitions move from members that did not need
// to move (t0p2, t1p0, t1p1).
//
// RoundRobin would result in
//
//     M0: [t0p0, t0p2, t1p1, t2p0, t2p2]
//     M1: [t0p1, t1p0, t1p2, t2p1]
//
// which is balanced, but has 2 partitions move when they do not need to
// (t0p1, t1p1).
//
// Sticky balancing results in
//
//     M0: [t0p0, t0p1, t0p2, t2p0, t2p2]
//     M1: [t1p0, t1p1, t1p2, t2p1]
//
// which is balanced and does not cause any unnecessary partition movement.
// The actual t2 partitions may not be in that exact combination, but they
// will be balanced.
//
// An advantage of the sticky consumer is that it allows API users to
// potentially avoid some cleanup until after the consumer knows which
// partitions it is losing when it gets its new assignment. Users can
// then only cleanup state for partitions that changed, which will be
// minimal (see KIP-54; this client also includes the KIP-351 bugfix).
//
// Note that this API implements the sticky partitioning quite differently from
// the Java implementation. The Java implementaiton is difficult to reason
// about and has many edge cases that result in non-optimal balancing (albeit,
// you likely have to be trying to hit those edge cases). This API uses a
// different algorithm (A*) to ensure optimal balancing while being an order of
// magnitude faster.
//
// Since the new strategy is a strict improvement over the Java strategy, it is
// entirely compatible. Any Go client sharing a group with a Java client will
// not have its decisions undone on leadership change from a Go consumer to a
// Java one. Java balancers do not apply the strategy it comes up with if it
// deems the balance score equal to or worse than the original score (the score
// being effectively equal to the standard deviation of the mean number of
// assigned partitions). This Go sticky balancer is optimal and extra sticky.
// Thus, the Java balancer will never back out of a strategy from this
// balancer.
func StickyBalancer() GroupBalancer {
	return &stickyBalancer{cooperative: false}
}

type stickyBalancer struct {
	cooperative bool
}

func (s *stickyBalancer) protocolName() string {
	if s.cooperative {
		return "cooperative-sticky"
	}
	return "sticky"
}
func (s *stickyBalancer) isCooperative() bool { return s.cooperative }
func (s *stickyBalancer) metaFor(interests []string, currentAssignment map[string][]int32, generation int32) []byte {
	meta := kmsg.GroupMemberMetadata{
		Version: 0,
		Topics:  interests,
	}
	if s.cooperative {
		meta.Version = 1
	}
	stickyMeta := kmsg.StickyMemberMetadata{
		Generation: generation,
	}
	for topic, partitions := range currentAssignment {
		if s.cooperative {
			meta.OwnedPartitions = append(meta.OwnedPartitions, kmsg.GroupMemberMetadataOwnedPartition{
				Topic:      topic,
				Partitions: partitions,
			})
		}
		stickyMeta.CurrentAssignment = append(stickyMeta.CurrentAssignment,
			kmsg.StickyMemberMetadataCurrentAssignment{
				Topic:      topic,
				Partitions: partitions,
			})
	}
	meta.UserData = stickyMeta.AppendTo(nil)
	return meta.AppendTo(nil)

}
func (s *stickyBalancer) balance(members []groupMember, topics map[string]int32) balancePlan {
	stickyMembers := make([]sticky.GroupMember, 0, len(members))
	for i := range members {
		member := &members[i]
		stickyMembers = append(stickyMembers, sticky.GroupMember{
			ID:       member.id.memberID,
			Topics:   member.topics,
			UserData: member.userdata,
		})
	}

	// Since our input into balancing is already sorted by instance ID,
	// the sticky strategy does not need to worry about instance IDs at all.
	// See my (slightly rambling) comment on KAFKA-8432.
	stickyPlan := sticky.Balance(stickyMembers, topics)

	// Annoyingly though, we do have to map the members given by the sticky
	// plan back into our memberID+instanceID, even though the instance ID
	// is not needed past this point.
	plan := balancePlan(make(map[groupMemberID]map[string][]int32, len(members)))
	for memberID, topics := range stickyPlan {
		for i := range members {
			member := &members[i]
			if member.id.memberID == memberID {
				plan[member.id] = topics
				break
			}
		}
	}
	if s.cooperative {
		s.adjustCooperative(members, plan)
	}
	return plan
}

// CooperativeStickyBalancer performs the sticky balancing strategy, but
// additionally opts the consumer group into "cooperative" rebalancing.
//
// Cooperative rebalancing differs from "eager" (the original) rebalancing in
// that group members do not stop processing partitions during the rebalance.
// Instead, once they receive their new assignment, each member determines
// which partitions it needs to revoke. If any, they send a new join request
// (before syncing), and the process starts over. This should ultimately end up
// in only two join rounds, with the major benefit being that processing never
// needs to stop.
//
// NOTE once a group is collectively using cooperative balancing, it is unsafe
// to have a member join the group that does not support cooperative balancing.
// If the only-eager member is elected leader, it will not know of the new
// multiple join strategy and things will go awry. Thus, once a group is
// entirely on cooperative rebalancing, it cannot go back.
//
// Migrating an eager group to cooperative balancing requires two rolling
// bounce deploys. The first deploy should add the cooperative-sticky strategy
// as an option (that is, each member goes from using one balance strategy to
// two). During this deploy, Kafka will tell leaders to continue using the old
// eager strategy, since the old eager strategy is the only one in common among
// all members. The second rolling deploy removes the old eager strategy. At
// this point, Kafka will tell the leader to use cooperative-sticky balancing.
// During this roll, all members in the group that still have both strategies
// continue to be eager and give up all of their partitions every rebalance.
// However, once a member only has cooperative-sticky, it can begin using this
// new strategy and things will work correctly. See KIP-429 for more details.
func CooperativeStickyBalancer() GroupBalancer {
	return &stickyBalancer{cooperative: true}
}

// adjustCooperative performs the final adjustment to the plan for cooperative
// sticky assigning.
//
// Over the plan, remove all partitions that migrated from one member (where it
// was assigned) to a new member (where it is now planned).
//
// This allows the assigned members to revoke and rejoin, which will then do
// another rebalance where the partitions will now be on the free list to be
// assigned.
//
// The implementation below is likely a bit slower than the Java version, due
// to the Java version having the input members as maps and the input
// partitions as a single "topic partition" type. Ideally, our much better
// sticky balancing implementation more than makes up for the speed difference.
func (*stickyBalancer) adjustCooperative(members []groupMember, plan balancePlan) {
	allAdded := make(map[string]map[int32]groupMemberID, 100)
	allRevoked := make(map[string]map[int32]struct{}, 100)

	addT := func(t string) map[int32]groupMemberID {
		addT := allAdded[t]
		if addT == nil {
			addT = make(map[int32]groupMemberID, 20)
			allAdded[t] = addT
		}
		return addT
	}
	revokeT := func(t string) map[int32]struct{} {
		revokeT := allRevoked[t]
		if revokeT == nil {
			revokeT = make(map[int32]struct{}, 20)
			allRevoked[t] = revokeT
		}
		return revokeT
	}

	tmap := make(map[string]struct{}) // reusable topic existence map
	pmap := make(map[int32]struct{})  // reusable partitions existence map

	// First, on all members, we find what was added and what was removed
	// to and from that member.
	for i := range members {
		member := &members[i]

		planned := plan[member.id]

		// added   := planned - current
		// revoked := current - planned

		for ptopic := range planned { // set existence for all planned topics
			tmap[ptopic] = struct{}{}
		}
		for _, otopic := range member.owned { // over all prior owned topics,
			topic := otopic.Topic
			delete(tmap, topic)
			ppartitions, exists := planned[topic]
			if !exists { // any topic that is no longer planned was entirely revoked,
				allRevokedT := revokeT(topic)
				for _, opartition := range otopic.Partitions {
					allRevokedT[opartition] = struct{}{}
				}
				continue
			}
			// calculate what was added by creating a planned existence map,
			// then removing what was owned, and anything that remains is new,
			for _, ppartition := range ppartitions {
				pmap[ppartition] = struct{}{}
			}
			for _, opartition := range otopic.Partitions {
				delete(pmap, opartition)
			}
			if len(pmap) > 0 {
				allAddedT := addT(topic)
				for ppartition := range pmap {
					delete(pmap, ppartition)
					allAddedT[ppartition] = member.id
				}
			}
			// then calculate removal by creating owned existence map,
			// then removing what was planned, anything remaining was revoked.
			for _, opartition := range otopic.Partitions {
				pmap[opartition] = struct{}{}
			}
			for _, ppartition := range ppartitions {
				delete(pmap, ppartition)
			}
			if len(pmap) > 0 {
				allRevokedT := revokeT(topic)
				for opartition := range pmap {
					delete(pmap, opartition)
					allRevokedT[opartition] = struct{}{}
				}
			}
		}
		for ptopic := range tmap { // finally, anything remaining in tmap is a new planned topic.
			delete(tmap, ptopic)
			allAddedT := addT(ptopic)
			for _, ppartition := range planned[ptopic] {
				allAddedT[ppartition] = member.id
			}
		}

	}

	// Over all revoked, if the revoked partition was added to a different
	// member, we remove that partition from the new member.
	for topic, rpartitions := range allRevoked {
		atopic, exists := allAdded[topic]
		if !exists {
			continue
		}
		for rpartition := range rpartitions {
			amember, exists := atopic[rpartition]
			if !exists {
				continue
			}

			ptopics := plan[amember]
			ppartitions := ptopics[topic]
			for i, ppartition := range ppartitions {
				if ppartition == rpartition {
					ppartitions[i] = ppartitions[len(ppartitions)-1]
					ppartitions = ppartitions[:len(ppartitions)-1]
					break
				}
			}
			if len(ppartitions) > 0 {
				ptopics[topic] = ppartitions
			} else {
				delete(ptopics, topic)
			}
		}
	}
}
