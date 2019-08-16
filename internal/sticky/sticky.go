// Package sticky provides the overcomplicated Java sticky partitioning
// strategy for Kafka.
package sticky

import (
	"container/heap"
	"reflect"
	"sort"

	"github.com/google/btree"
	"github.com/twmb/go-sliceheap"

	"github.com/twmb/kgo/kmsg"
)

const defaultGeneration = -1

type GroupMember struct {
	ID string

	Version  int16
	Topics   []string
	UserData []byte
}

type Plan map[string]map[string][]int32

type memberPartitions map[string][]topicPartition

type consumerPartitions struct {
	member     string
	partitions []topicPartition
}

func (l consumerPartitions) less(r consumerPartitions) bool {
	return len(l.partitions) > len(r.partitions) ||
		len(l.partitions) == len(r.partitions) &&
			l.member > r.member
}

func (l consumerPartitions) Less(r btree.Item) bool {
	return l.less(r.(consumerPartitions))
}

func (m memberPartitions) intoConsumersPartitions() []consumerPartitions {
	var consumersPartitions []consumerPartitions
	for member, partitions := range m {
		consumersPartitions = append(consumersPartitions, consumerPartitions{
			member,
			partitions,
		})
	}
	return consumersPartitions
}

func (m memberPartitions) btreeByConsumersPartitions() *btree.BTree {
	bt := btree.New(8)
	for _, consumerPartitions := range m.intoConsumersPartitions() {
		bt.ReplaceOrInsert(consumerPartitions)
	}
	return bt
}

func (m memberPartitions) heapByConsumersPartitions() sliceheap.Heap {
	consumersPartitions := m.intoConsumersPartitions()
	return sliceheap.On(&consumersPartitions, func(i, j int) bool {
		l, r := consumersPartitions[i], consumersPartitions[j]
		return l.less(r)
	})
}

// cloneFilteringNonExistentPartitions returns a copy of mps with
// all partitions that no longer exist in the client removed.
func (mps memberPartitions) deepCloneFilteringNonExistentPartitions(partitions2AllPotentialMembers partitionMembers) memberPartitions {
	filtered := make(memberPartitions, len(mps))
	for member, partitions := range mps {
		var endIdx int
		for curIdx, partition := range partitions {
			if _, exists := partitions2AllPotentialMembers[partition]; !exists {
				continue
			}
			partitions[endIdx] = partitions[curIdx]
			endIdx++
		}
		filtered[member] = append([]topicPartition(nil), partitions[:endIdx]...)
	}
	return filtered
}

type partitionMembers map[topicPartition][]string

func (orig partitionMembers) cloneKeys() map[topicPartition]struct{} {
	dup := make(map[topicPartition]struct{}, len(orig))
	for partition, _ := range orig {
		dup[partition] = struct{}{}
	}
	return dup
}

func (orig partitionMembers) shallowClone() partitionMembers {
	dup := make(partitionMembers, len(orig))
	for partition, members := range orig {
		dup[partition] = members
	}
	return dup
}

type topicPartition struct {
	topic     string
	partition int32
}

type memberGeneration struct {
	memberID   string
	generation int32
}

func Balance(members []GroupMember, topics map[string][]int32) Plan {
	// Parse the member metadata for figure out what everybody was doing.
	//
	// The plan is members => what they were consuming.
	// The stragglers are rejoining members that had stale metadata => their double consumption.
	plan, priorPlanStragglers := parseMemberMetadata(members)
	isFreshAssignment := len(plan) == 0

	// For potential later movement, we create two big maps.
	partitions2AllPotentialConsumers := make(partitionMembers) // each partition => each consumer that could potentially consume it
	consumers2AllPotentialPartitions := make(memberPartitions) // each member (consumer) => partitions it could potential consume

	// We start with initializing our partitions => consumers map as "all
	// partitions have no consumers".
	for topic, partitions := range topics {
		for _, partition := range partitions {
			topicPartition := topicPartition{topic, partition}
			partitions2AllPotentialConsumers[topicPartition] = nil
		}
	}

	// Now we initialize the consumers => partitions map with all group
	// members saying "all members are consuming nothing".
	for _, member := range members {
		consumers2AllPotentialPartitions[member.ID] = nil
		// Immediately after setting that up, we go through all topics the
		// member indicated they want to consume from, filter out ones that
		// our client does not know about, and then set up the two way
		// mapping.
		for _, topic := range member.Topics {
			partitions, exists := topics[topic]
			if !exists {
				continue
			}
			// Member is interested in a topic, and our client knows of the topic?
			// Set that the member wants to consume it, and that it can be consumed
			// by this member.
			for _, partition := range partitions {
				topicPartition := topicPartition{topic, partition}
				consumers2AllPotentialPartitions[member.ID] = append(consumers2AllPotentialPartitions[member.ID], topicPartition)
				partitions2AllPotentialConsumers[topicPartition] = append(partitions2AllPotentialConsumers[topicPartition], member.ID)
			}
		}
		// Lastly, if this is a new member, the plan everything is
		// using will not know of it. We add that it is consuming nothing
		// in that plan here.
		if _, exists := plan[member.ID]; !exists {
			plan[member.ID] = nil
		}
	}

	// Maps all currently consumed partitions to the member that is
	// currently consuming them.
	partitionConsumers := make(map[topicPartition]string)
	// To build a list of unassigned partitions, we visit all partitions
	// in the current plan and, if they still exist and the prior consumer
	// no longer wants to consume them, we track it as unassigned.
	// After, we add all new partitions.
	unvisitedPartitions := partitions2AllPotentialConsumers.cloneKeys()
	var unassignedPartitions []topicPartition

	for member, partitions := range plan {
		var keepIdx int
		for _, partition := range partitions {
			// If this partition no longer exists at all, likely due to the
			// topic being deleted, we remove the partition from the member.
			if _, exists := partitions2AllPotentialConsumers[partition]; !exists {
				continue
			}

			delete(unvisitedPartitions, partition)
			partitionConsumers[partition] = member

			thisMemberIdx := sort.Search(len(members), func(i int) bool { return members[i].ID >= member })
			thisMember := members[thisMemberIdx]
			if !strsHas(thisMember.Topics, partition.topic) {
				unassignedPartitions = append(unassignedPartitions, partition)
				continue
			}

			partitions[keepIdx] = partition
			keepIdx++
		}
		plan[member] = partitions[:keepIdx]
	}
	for unvisited := range unvisitedPartitions {
		unassignedPartitions = append(unassignedPartitions, unvisited)
	}

	// The last two helpers we build before balancing are partitions sorted
	// by move preference, and the current plan by num partitions.
	//
	// The former is ordered from partitions we most likely want to move
	// to least. See the sortPartitionsByMovePreference function for more
	// documentation.
	partitionsByMovePreference := sortPartitionsByMovePreference(
		plan,
		priorPlanStragglers,
		isFreshAssignment,
		partitions2AllPotentialConsumers,
		consumers2AllPotentialPartitions,
	)
	// For the planByNumPartitions, we use a btree heap since we
	// will be accessing both the min and max often as well as ranging from
	// smallest to largest.
	planByNumPartitions := plan.btreeByConsumersPartitions()

	balance(
		plan,
		priorPlanStragglers,
		partitionsByMovePreference,
		unassignedPartitions,
		planByNumPartitions,
		consumers2AllPotentialPartitions,
		partitions2AllPotentialConsumers,
		partitionConsumers,
	)

	return nil
}

func strsHas(search []string, needle string) bool {
	for _, check := range search {
		if check == needle {
			return true
		}
	}
	return false
}

func partitionsHas(search []topicPartition, needle topicPartition) bool {
	for _, check := range search {
		if check == needle {
			return true
		}
	}
	return false
}

// parseMemberMetadata parses all member metadata and returns the current group
// plan as aggregated from all sticky partition prior sent plans.
//
// The second return, priorPlanStragglers, tracks partitions that have at least
// two members thinking they are consuming the same partition. The member with
// the higher generation is returned in currentPlan, the older generation goes
// into the previous stragglers.
//
// The previous stragglers are eventually used as a hint of "these may be good
// partitions to move if necessary".
func parseMemberMetadata(members []GroupMember) (
	plan memberPartitions,
	priorPlanStragglers map[topicPartition]memberGeneration,
) {
	// all partitions => members that are consuming those partitions
	// Each partition should only have one consumer, but a flaky member
	// could rejoin with an old generation (stale user data) and say it
	// is consuming something a different member is. See KIP-341.
	partitionConsumersByGeneration := make(map[topicPartition][]memberGeneration)

	for _, member := range members {
		memberPlan, generation := deserializeUserData(member.Version, member.UserData)
		memberGeneration := memberGeneration{
			member.ID,
			generation,
		}
		for _, topicPartition := range memberPlan {
			partitionConsumers := partitionConsumersByGeneration[topicPartition]
			var doublyConsumed bool
			for _, otherConsumer := range partitionConsumers { // expected to be very few if any others
				if otherConsumer.generation == generation {
					doublyConsumed = true
					break
				}
			}
			// Two members should not be consuming the same topic and partition
			// within the same generation. If see this, we drop the second.
			if doublyConsumed {
				continue
			}
			partitionConsumers = append(partitionConsumers, memberGeneration)
			partitionConsumersByGeneration[topicPartition] = partitionConsumers
		}
	}

	plan = make(memberPartitions)
	priorPlanStragglers = make(map[topicPartition]memberGeneration)

	for partition, partitionConsumers := range partitionConsumersByGeneration {
		sort.Slice(partitionConsumers, func(i, j int) bool {
			return partitionConsumers[i].generation >= partitionConsumers[j].generation
		})

		memberID := partitionConsumers[0].memberID
		plan[memberID] = append(plan[memberID], partition)
		if len(partitionConsumers) > 1 {
			priorPlanStragglers[partition] = partitionConsumers[1]
		}
	}

	return plan, priorPlanStragglers
}

// deserializeUserData returns the topic partitions a member was consuming and
// the join generation it was consuming from.
//
// If anything fails or we do not understand the userdata parsing generation,
// we return empty defaults. The member will just be assumed to have no
// history.
func deserializeUserData(version int16, userdata []byte) (memberPlan []topicPartition, generation int32) {
	generation = defaultGeneration
	switch version {
	case 0:
		var v0 kmsg.StickyMemberMetadataV0
		if err := v0.ReadFrom(userdata); err != nil {
			return nil, 0
		}
		for _, topicAssignment := range v0.CurrentAssignment {
			for _, partition := range topicAssignment.Partitions {
				memberPlan = append(memberPlan, topicPartition{
					topicAssignment.Topic,
					partition,
				})
			}
		}
	case 1:
		var v1 kmsg.StickyMemberMetadataV1
		if err := v1.ReadFrom(userdata); err != nil {
			return nil, 0
		}
		generation = v1.Generation
		for _, topicAssignment := range v1.CurrentAssignment {
			for _, partition := range topicAssignment.Partitions {
				memberPlan = append(memberPlan, topicPartition{
					topicAssignment.Topic,
					partition,
				})
			}
		}
	}

	return memberPlan, generation
}

// sortPartitionsByMovePreference does a bunch of logic to try to order
// partitions in a certain way to be moved later.
//
// Inexplicably, there are two potential ways of sorting:
// 1) ascending order of partition by the number of consumers it has
// 2) ascending order of partition from the most loaded consumer
// The phrasings of these two orderings are expanded below.
//
// The second sorting only kicks in if every group member can consume
// the same partitions. Any imbalance causes the first sorting.
//
// Thus, the future usage of the returned sorting is hard to reason
// about.
func sortPartitionsByMovePreference(
	plan memberPartitions,
	priorPlanStragglers map[topicPartition]memberGeneration,
	isFreshAssignment bool,
	partitions2AllPotentialConsumers partitionMembers,
	consumers2AllPotentialPartitions memberPartitions,
) (
	partitionsByMovePreference []topicPartition,
) {
	// We avoid adding partitions to the move preference if the partition
	// only has one potential consumer.
	//
	// This effectively takes the place of the fixedPartitions loop in
	// balance in the Java code.
	maybeAddPartition := func(partition topicPartition, potentialConsumers []string) {
		if len(potentialConsumers) > 1 {
			partitionsByMovePreference = append(partitionsByMovePreference, partition)
		}
	}

	// If this is a fresh assignment, or if not all consumers can consume
	// the same partitions, then we simply sort by how many consumers can
	// consume partitions. That is, partitions with fewer potential
	// consumers are the most likely to be moved.
	//
	// Note that the Java code compares all values for both maps, but
	// I am pretty sure that if one map has all equal values, the other
	// must. I am leaving in both comparisons because may as well.
	if isFreshAssignment ||
		!mapValuesEq(partitions2AllPotentialConsumers) ||
		!mapValuesEq(consumers2AllPotentialPartitions) {

		for partition, potentialConsumers := range partitions2AllPotentialConsumers {
			maybeAddPartition(partition, potentialConsumers)
		}
		sort.Slice(partitionsByMovePreference, func(i, j int) bool {
			l, r := partitionsByMovePreference[i], partitionsByMovePreference[j]

			lPotentials := len(partitions2AllPotentialConsumers[l])
			rPotentials := len(partitions2AllPotentialConsumers[r])

			return lPotentials < rPotentials ||
				lPotentials == rPotentials &&
					(l.topic < r.topic ||
						l.topic == r.topic &&
							l.partition < r.partition)
		})
		return partitionsByMovePreference
	}

	// This is not a fresh assignment, and all consumers can equally
	// consume all partitions.
	//
	// Our sort by move preference here will be partitions on consumers
	// that are consuming the most. That is, partitions on consumers
	// consuming the most are the most likely to be moved.
	consumersByNumPartitions := plan.
		deepCloneFilteringNonExistentPartitions(partitions2AllPotentialConsumers).
		heapByConsumersPartitions()
	heap.Init(consumersByNumPartitions)

	// Every loop below peels off a partition from the consumer with the
	// largest amount of partitions. Preference is given to partitions
	// that a straggler thinks they own, otherwise, it's just the first.
	unvisitedPartitions := partitions2AllPotentialConsumers.shallowClone()
	for consumersByNumPartitions.Len() > 0 {
		consumerWithMostPartitions := consumersByNumPartitions.Peek().(*consumerPartitions)
		partitions := consumerWithMostPartitions.partitions
		var useIdx int
		for idx, partition := range partitions {
			if _, exists := priorPlanStragglers[partition]; exists {
				useIdx = idx
				break
			}
		}

		usePartition := partitions[useIdx]
		potentialConsumers := unvisitedPartitions[usePartition]
		delete(unvisitedPartitions, usePartition)

		maybeAddPartition(usePartition, potentialConsumers)

		if useIdx == 0 { // common case
			partitions = partitions[1:]
		} else { // less common: shift from right to preserve ordering
			partitions = append(partitions[:useIdx], partitions[useIdx+1:]...)
		}
		if len(partitions) > 0 {
			consumerWithMostPartitions.partitions = partitions
			heap.Fix(consumersByNumPartitions, 0)
		} else {
			heap.Pop(consumersByNumPartitions)
		}
	}
	for unvisited, potentialConsumers := range unvisitedPartitions {
		maybeAddPartition(unvisited, potentialConsumers)
	}

	return partitionsByMovePreference
}

func mapValuesEq(m interface{}) bool {
	iter := reflect.ValueOf(m).MapRange()
	if !iter.Next() {
		return true
	}
	first := iter.Value().Interface()
	for iter.Next() {
		next := iter.Value().Interface()
		if !reflect.DeepEqual(first, next) {
			return false
		}
	}
	return true
}

func balance(
	plan memberPartitions,
	priorPlanStragglers map[topicPartition]memberGeneration,
	partitionsByMovePreference []topicPartition,
	unassignedPartitions []topicPartition,
	planByNumPartitions *btree.BTree,
	consumers2AllPotentialPartitions memberPartitions,
	partitions2AllPotentialConsumers partitionMembers,
	partitionConsumers map[topicPartition]string,
) {
	// First, we assign all unassigned partitions to consumers.
	for _, partition := range unassignedPartitions {
		if _, exists := partitions2AllPotentialConsumers[partition]; !exists {
			continue
		}
		//assignPartition(
		//	partition,
		//	plan,
		//	planByNumPartitions,
		//	partitionConsumers,
		//	consumers2AllPotentialPartitions,
		//)
	}
}

/*
func assignPartition(
	partition topicPartition,
	plan memberPartitions,
	planByNumPartitions *btree.BTree,
	partitionConsumers map[topicPartition]string,
	consumers2AllPotentialPartitions memberPartitions,
) {
	var fix Item
	var fixWith consumerPartitions
	planByNumPartitions.Ascend(func(item btree.Item) bool {
		memberWithFewestPartitions := item.(consumerPartitions)
		member := memberWithFewestPartitions.member
		memberPotentials := consumers2AllPotentialPartitions[member]
		if !partitionsHas(memberPotentials, partition) {
			return true
		}
		fix = Item
		fixWith = consumerPartitions{
			member: member,
		}
		plan[member] = append(plan[member], partition)
		partitionConsumers[partition] = member
		return false
	})
}
*/
