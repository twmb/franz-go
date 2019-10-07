package kgo

import (
	"fmt"
	"sort"

	"github.com/twmb/kgo/pkg/kgo/internal/sticky"
	"github.com/twmb/kgo/pkg/kmsg"
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
	balance(members []groupMember, topics map[string][]int32) balancePlan
}

// groupMember is a member id and the topics that member is interested in.
type groupMember struct {
	id       string
	version  int16
	topics   []string
	userdata []byte
}

// balancePlan is the result of balancing topic partitions among members.
//
// member id => topic => partitions
type balancePlan map[string]map[string][]int32

func newBalancePlan(members []groupMember) balancePlan {
	plan := make(map[string]map[string][]int32, len(members))
	for _, member := range members {
		plan[member.id] = make(map[string][]int32)
	}
	return plan
}

func (plan balancePlan) addPartition(member, topic string, partition int32) {
	memberPlan := plan[member]
	memberPlan[topic] = append(memberPlan[topic], partition)
}
func (plan balancePlan) addPartitions(member, topic string, partitions []int32) {
	memberPlan := plan[member]
	memberPlan[topic] = append(memberPlan[topic], partitions...)
}

// intoAssignment translates a balance plan to the kmsg equivalent type.
func (plan balancePlan) intoAssignment() []kmsg.SyncGroupRequestGroupAssignment {
	kassignments := make([]kmsg.SyncGroupRequestGroupAssignment, len(plan))
	for member, assignment := range plan {
		var kassignment kmsg.GroupMemberAssignment
		for topic, partitions := range assignment {
			kassignment.Topics = append(kassignment.Topics, kmsg.GroupMemberAssignmentTopic{
				Topic:      topic,
				Partitions: partitions,
			})
		}
		kassignments = append(kassignments, kmsg.SyncGroupRequestGroupAssignment{
			MemberID:         member,
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
		return nil, ErrInvalidResp
	}
	sort.Slice(members, func(i, j int) bool {
		return members[i].id < members[j].id // guarantee sorted members
	})
	for _, member := range members {
		sort.Strings(member.topics) // guarantee sorted topics
	}

	for _, balancer := range g.balancers {
		if balancer.protocolName() == proto {
			return balancer.balance(members, g.cl.loadShortTopics()), nil
		}
	}
	return nil, ErrInvalidResp
}

// parseGroupMembers takes the raw data in from a join group response and
// returns the parsed group members.
func parseGroupMembers(kmembers []kmsg.JoinGroupResponseMember) ([]groupMember, error) {
	members := make([]groupMember, 0, len(kmembers))
	for _, kmember := range kmembers {
		var meta kmsg.GroupMemberMetadata
		if err := meta.ReadFrom(kmember.MemberMetadata); err != nil {
			return nil, fmt.Errorf("unable to read member metadata: %v", err)
		}
		members = append(members, groupMember{
			id:       kmember.MemberID,
			version:  meta.Version,
			topics:   meta.Topics,
			userdata: meta.UserData,
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
func (*roundRobinBalancer) metaFor(interests []string, _ map[string][]int32, _ int32) []byte {
	return basicMetaFor(interests)
}
func (*roundRobinBalancer) balance(members []groupMember, topics map[string][]int32) balancePlan {
	// Get all the topics all members are subscribed to.
	memberTopics := make(map[string]struct{}, len(topics))
	for _, member := range members {
		for _, topic := range member.topics {
			memberTopics[topic] = struct{}{}
		}
	}

	type topicPartition struct {
		topic     string
		partition int32
	}
	var nparts int
	for _, partitions := range topics {
		nparts += len(partitions)
	}
	// Order all partitions available to balance, filtering out those that
	// no members are subscribed to.
	allParts := make([]topicPartition, 0, nparts)
	for topic := range memberTopics {
		for _, partition := range topics[topic] {
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
func (*rangeBalancer) metaFor(interests []string, _ map[string][]int32, _ int32) []byte {
	return basicMetaFor(interests)
}
func (*rangeBalancer) balance(members []groupMember, topics map[string][]int32) balancePlan {
	topics2PotentialConsumers := make(map[string][]string)
	for _, member := range members {
		for _, topic := range member.topics {
			topics2PotentialConsumers[topic] = append(topics2PotentialConsumers[topic], member.id)
		}
	}

	plan := newBalancePlan(members)
	for topic, potentialConsumers := range topics2PotentialConsumers {
		sort.Strings(potentialConsumers)

		partitions := topics[topic]
		numParts := len(partitions)
		div, rem := numParts/len(members), numParts%len(members)

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
	return new(stickyBalancer)
}

type stickyBalancer struct{}

func (*stickyBalancer) protocolName() string { return "sticky" }
func (*stickyBalancer) metaFor(interests []string, currentAssignment map[string][]int32, generation int32) []byte {
	meta := kmsg.GroupMemberMetadata{
		Version: 1,
		Topics:  interests,
	}
	stickyMeta := kmsg.StickyMemberMetadata{
		Generation: generation,
	}
	for topic, partitions := range currentAssignment {
		stickyMeta.CurrentAssignment = append(stickyMeta.CurrentAssignment,
			kmsg.StickyMemberMetadataCurrentAssignment{
				Topic:      topic,
				Partitions: partitions,
			})
	}
	meta.UserData = stickyMeta.AppendTo(nil, 1) // we are using v1 sticky data
	return meta.AppendTo(nil)

}
func (*stickyBalancer) balance(members []groupMember, topics map[string][]int32) balancePlan {
	stickyMembers := make([]sticky.GroupMember, 0, len(members))
	for _, member := range members {
		stickyMembers = append(stickyMembers, sticky.GroupMember{
			ID:       member.id,
			Version:  member.version,
			Topics:   member.topics,
			UserData: member.userdata,
		})
	}

	return balancePlan(sticky.Balance(stickyMembers, topics))
}
