package kgo

import (
	"fmt"
	"sort"

	"github.com/twmb/kgo/kmsg"
)

// GroupBalancer balances topics and partitions among group members.
type GroupBalancer interface {
	// protocolName returns the name of the protocol, e.g. roundrobin,
	// range, sticky.
	protocolName() string // "roundrobin"

	// balance balances topics and partitions among group members.
	balance(members []groupMember, topics map[string][]int32) balancePlan
}

// groupMember is a member id and the topics that member is interested in.
type groupMember struct {
	id     string
	topics []string
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
func (c *consumer) balanceGroup(kmembers []kmsg.JoinGroupResponseMember) (balancePlan, error) {
	members, err := parseGroupMembers(kmembers)
	if err != nil {
		return nil, err
	}
	if len(members) == 0 {
		return nil, fmt.Errorf("NO MEMBERS") // TODO nice err
	}
	sort.Slice(members, func(i, j int) bool {
		return members[i].id < members[j].id
	})
	for _, member := range members {
		sort.Strings(member.topics)
	}

	return c.group.balancer.balance(members, c.client.loadShortTopics()), nil
}

// parseGroupMembers takes the raw data in from a join group response and
// returns the parsed group members.
func parseGroupMembers(kmembers []kmsg.JoinGroupResponseMember) ([]groupMember, error) {
	members := make([]groupMember, 0, len(kmembers))
	for _, kmember := range kmembers {
		var meta kmsg.GroupMemberMetadata
		if err := meta.ReadFrom(kmember.MemberMetadata); err != nil {
			return nil, fmt.Errorf("unable to read member metadata: %v", err) // TODO nice err
		}
		members = append(members, groupMember{
			id:     kmember.MemberID,
			topics: meta.Topics,
		})
	}
	return members, nil
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
// This is equivalent to the Java roundrobin balancer.
func RoundRobinBalancer() GroupBalancer {
	return new(roundRobinBalancer)
}

type roundRobinBalancer struct{}

func (*roundRobinBalancer) protocolName() string { return "roundrobin" }
func (*roundRobinBalancer) balance(members []groupMember, topics map[string][]int32) balancePlan {
	type topicPartition struct {
		topic     string
		partition int32
	}
	var allPartitions []topicPartition

	for topic, partitions := range topics { // layout all topic partitions
		for _, partition := range partitions {
			allPartitions = append(allPartitions, topicPartition{
				topic:     topic,
				partition: partition,
			})
		}
	}

	sort.Slice(allPartitions, func(i, j int) bool { // order them
		l, r := allPartitions[i], allPartitions[j]
		if l.topic == r.topic {
			return l.partition < r.partition
		}
		return l.topic < r.topic
	})

	plan := newBalancePlan(members)

	var memberIdx int
	for _, next := range allPartitions { // then assign them to the members, easy enough
		member := members[memberIdx]
		plan.addPartition(member.id, next.topic, next.partition)
		memberIdx = (memberIdx + 1) % len(members)
	}

	return plan
}

// RangeBalancer returns a group balancer that, per topic, maps partitions to
// group members. Since this works on a topic level, uneven partitions per
// topic to the number of members can lead to slight partition consumption
// disparities..
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
func (*rangeBalancer) balance(members []groupMember, topics map[string][]int32) balancePlan {
	type topicPartitions struct {
		topic      string
		partitions []int32
	}
	var allTopics []topicPartitions

	for topic, partitions := range topics {
		sort.Slice(partitions, func(i, j int) bool { return partitions[i] < partitions[j] })
		allTopics = append(allTopics, topicPartitions{
			topic:      topic,
			partitions: partitions,
		})
	}

	sort.Slice(allTopics, func(i, j int) bool {
		return allTopics[i].topic < allTopics[j].topic
	})

	plan := newBalancePlan(members)

	for _, parts := range allTopics {
		numParts := len(parts.partitions)
		div, rem := numParts/len(members), numParts%len(members)

		var memberIdx int
		for len(parts.partitions) > 0 {
			num := div
			if rem > 0 {
				num++
				rem--
			}

			member := members[memberIdx]
			plan.addPartitions(member.id, parts.topic, parts.partitions[:num])

			memberIdx++
			parts.partitions = parts.partitions[num:]
		}
	}

	return plan
}
