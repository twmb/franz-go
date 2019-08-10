package kgo

import (
	"fmt"
	"sort"

	"github.com/twmb/kgo/kmsg"
)

// we always use "consumer"
type GroupBalancer interface {
	protocolName() string // "roundrobin"

	balance(members []groupMember, topics map[string]*topicPartitions) balancePlan
}

func balancerMetadata(topics []string, userdata []byte) []byte {
	return (&kmsg.GroupMemberMetadata{
		Version:  0,
		Topics:   topics,
		UserData: userdata,
	}).AppendTo(nil)
}

func RoundRobinBalancer() GroupBalancer {
	return new(roundRobinBalancer) // TODO package level singleton
}

type roundRobinBalancer struct{}

func (*roundRobinBalancer) protocolName() string { return "roundrobin" }
func (*roundRobinBalancer) balance(members []groupMember, topics map[string]*topicPartitions) balancePlan {
	type topicPartition struct {
		topic     string
		partition int32
	}
	var allPartitions []topicPartition

	for topic, topicPartitions := range topics {
		for _, partition := range topicPartitions.load().partitions {
			allPartitions = append(allPartitions, topicPartition{
				topic:     topic,
				partition: partition,
			})
		}
	}

	sort.Slice(allPartitions, func(i, j int) bool {
		l, r := allPartitions[i], allPartitions[j]
		if l.topic == r.topic {
			return l.partition < r.partition
		}
		return l.topic < r.topic
	})

	plan := newBalancePlan(members)

	var memberIdx int
	for _, next := range allPartitions {
		member := members[memberIdx]
		plan.add(member.id, next.topic, next.partition)
		memberIdx = (memberIdx + 1) % len(members)
	}

	return plan
}

type groupMember struct {
	id     string
	topics []string
} // TODO userdata

type balancePlan map[string]map[string][]int32

func newBalancePlan(members []groupMember) balancePlan {
	plan := make(map[string]map[string][]int32, len(members))
	for _, member := range members {
		plan[member.id] = make(map[string][]int32)
	}
	return plan
}

func (plan balancePlan) add(member, topic string, partition int32) {
	memberPlan := plan[member]
	memberPlan[topic] = append(memberPlan[topic], partition)
}

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

func (c *consumer) balanceGroup(kmembers []kmsg.JoinGroupResponseMember) (balancePlan, error) {
	members, err := parseGroupMembers(kmembers)
	if err != nil {
		return nil, err
	}
	sort.Slice(members, func(i, j int) bool {
		return members[i].id < members[j].id
	})
	for _, member := range members {
		sort.Strings(member.topics)
	}

	return c.group.balancer.balance(members, c.client.loadTopics()), nil
}

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
