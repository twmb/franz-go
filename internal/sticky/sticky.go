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

type balancer struct {
	// members are the members in play for this balance.
	//
	// This is built in newBalancer mapping member IDs to the GroupMember.
	members map[string]GroupMember

	// topics are the topic names and partitions that the client knows of
	// and passed to be used for balancing.
	//
	// This is repeatedly used for filtering topics that members indicate
	// they can consume but that our client does not know of.
	topics map[string][]int32

	// plan is the plan that we are building to balance partitions.
	//
	// This is initialized with data from the userdata each group member
	// is sending with the join. After, we use this to move partitions
	// around or assign new partitions.
	plan membersPartitions

	// planByNumPartitions orders plan member partitions by the number of
	// partitions each member is consuming.
	//
	// The nodes in the btree reference values in plan, meaning updates in
	// this field are visible in plan.
	planByNumPartitions *btree.BTree

	// priorPlanStragglers is related to plan above. This tracks partitions
	// that have at least two members thinking they are consuming the same
	// partition. The member with the higher generation is stuck into plan,
	// the one with the lower is stuck here.
	//
	// This is used eventually as a hint of "these may be good partitions
	// to move if necessary".
	priorPlanStragglers map[topicPartition]memberGeneration

	// isFreshAssignment tracks whether this is the first join for a group.
	// This is true if no member has userdata (plan is empty)
	isFreshAssignment bool

	// partitionConsumers maps all possible partitions to consume to the
	// members that are consuming them.
	//
	// We initialize this from our plan and modify it during reassignment.
	partitionConsumers map[topicPartition]string

	// unassignedPartitions tracks all partitions to be consumed that are
	// currently not assigned to a member.
	//
	// This is initalized at the same time as partitionConsumers and is
	// drained during reassignment.
	unassignedPartitions []topicPartition

	// partitionsByMovePreference orders partitions from most-want-to-move
	// to least.
	//
	// There are two potential orderings: if this is a fresh assignment, or
	// not every member can consume the same, the we order by partitions
	// with the fewest consumers (then topic, partition fallback). This
	// ordering allows partitions that are least likely to be assigned to
	// be assigned first.
	//
	// If everything can consume the same and this is not a fresh
	// assignment, then we order by partitions from consumers consuming the
	// most. That is, ordered by partitions on the most consuming members
	// to the least.
	//
	// The varying sort ordering makes usage of this hard to reason about.
	//
	// Note that we leave out partitions that only have one potential
	// consumer.
	partitionsByMovePreference []topicPartition

	// consumers2AllPotentialPartitions maps each member to all of the
	// partitions it theoretically could consume. This is repeatedly used
	// during assignment to see if a partition we want to move can be moved
	// to a member.
	//
	// (maps each partition => each member that could consume it)
	//
	// This is built once and never modified thereafter.
	consumers2AllPotentialPartitions staticMembersPartitions

	// partitions2AllPotentialConsumers maps each partition to a member
	// that could theoretically consume it. This is repeatedly used during
	// assignment to see which members could consume a partition we want to
	// move.
	//
	// (maps each member => each partition it could consume)
	//
	// This is built once and never modified thereafter.
	partitions2AllPotentialConsumers staticPartitionMembers
}

func newBalancer(members []GroupMember, topics map[string][]int32) *balancer {
	b := &balancer{
		members: make(map[string]GroupMember, len(members)),
		topics:  topics,

		plan:                make(membersPartitions),
		priorPlanStragglers: make(map[topicPartition]memberGeneration),

		partitionConsumers: make(map[topicPartition]string),

		partitions2AllPotentialConsumers: make(staticPartitionMembers),
		consumers2AllPotentialPartitions: make(staticMembersPartitions),
	}
	for _, member := range members {
		b.members[member.ID] = member
	}
	return b
}

type topicPartition struct {
	topic     string
	partition int32
}

type memberGeneration struct {
	memberID   string
	generation int32
}

// staticMembersPartitions is like membersPartitions below, but is used only
// for consumers2AllPotentialPartitions. The value is built once and never
// changed. Essentially, this is a clearer type.
type staticMembersPartitions map[string][]topicPartition

// membersPartitions maps members to a pointer of their partitions.  We use a
// pointer so that modifications through memberWithPartitions can be seen in
// any membersPartitions map.
type membersPartitions map[string]*[]topicPartition

// memberWithPartitions ties a member to a pointer to its partitions.
//
// This is generally used for sorting purposes.
type memberWithPartitions struct {
	member     string
	partitions *[]topicPartition
}

func (l memberWithPartitions) less(r memberWithPartitions) bool {
	return len(*l.partitions) > len(*r.partitions) ||
		len(*l.partitions) == len(*r.partitions) &&
			l.member > r.member
}

func (l memberWithPartitions) Less(r btree.Item) bool {
	return l.less(r.(memberWithPartitions))
}

func (m membersPartitions) intoConsumersPartitions() []memberWithPartitions {
	var consumersPartitions []memberWithPartitions
	for member, partitions := range m {
		consumersPartitions = append(consumersPartitions, memberWithPartitions{
			member,
			partitions,
		})
	}
	return consumersPartitions
}

func (m membersPartitions) btreeByConsumersPartitions() *btree.BTree {
	bt := btree.New(8)
	for _, memberWithPartitions := range m.intoConsumersPartitions() {
		bt.ReplaceOrInsert(memberWithPartitions)
	}
	return bt
}

func (m membersPartitions) heapByConsumersPartitions() sliceheap.Heap {
	consumersPartitions := m.intoConsumersPartitions()
	return sliceheap.On(&consumersPartitions, func(i, j int) bool {
		l, r := consumersPartitions[i], consumersPartitions[j]
		return l.less(r)
	})
}

func (mps membersPartitions) deepClone() membersPartitions {
	clone := make(membersPartitions, len(mps))
	for member, partitions := range mps {
		dup := append([]topicPartition(nil), *partitions...)
		clone[member] = &dup
	}
	return clone
}

// cloneFilteringNonExistentPartitions returns a copy of mps with
// all partitions that no longer exist in the client removed.
func (mps membersPartitions) deepCloneFilteringNonExistentPartitions(
	partitions2AllPotentialMembers staticPartitionMembers,
) membersPartitions {
	filtered := make(membersPartitions, len(mps))
	for member, partitions := range mps {
		var clonePartitions []topicPartition
		for _, partition := range *partitions {
			if _, exists := partitions2AllPotentialMembers[partition]; !exists {
				continue
			}
			clonePartitions = append(clonePartitions, partition)
		}
		filtered[member] = &clonePartitions
	}
	return filtered
}

// staticPartitionMember is the same as partitionMembers, but we type name it
// to imply immutability in reading. All mutable uses go through cloneKeys
// or shallowClone.
type staticPartitionMembers map[topicPartition][]string

type partitionMembers map[topicPartition][]string

func (orig staticPartitionMembers) cloneKeys() map[topicPartition]struct{} {
	dup := make(map[topicPartition]struct{}, len(orig))
	for partition, _ := range orig {
		dup[partition] = struct{}{}
	}
	return dup
}

func (orig staticPartitionMembers) shallowClone() partitionMembers {
	dup := make(partitionMembers, len(orig))
	for partition, members := range orig {
		dup[partition] = members
	}
	return dup
}

func Balance(members []GroupMember, topics map[string][]int32) Plan {
	// Code below relies on members to be sorted. It should be: that is the
	// contract of the Balance interface. But, just in case.
	sort.Slice(members, func(i, j int) bool { return members[i].ID < members[j].ID })

	b := newBalancer(members, topics)

	// Parse the member metadata for figure out what everybody was doing.
	b.parseMemberMetadata()
	b.initAllConsumersPartitions()
	// For planByNumPartitions, we use a btree heap since we will be
	// accessing both the min and max often as well as ranging from
	// smallest to largest.
	//
	// We init this after initAllConsumersPartitions, which can add new
	// members that were not in the prior plan.
	b.planByNumPartitions = b.plan.btreeByConsumersPartitions()
	b.determineUnassignedPartitions()
	b.sortPartitionsByMovePreference()

	b.balance()

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

// parseMemberMetadata parses all member userdata to initialize plan and
// priorPlanStragglers.
func (b *balancer) parseMemberMetadata() {
	// all partitions => members that are consuming those partitions
	// Each partition should only have one consumer, but a flaky member
	// could rejoin with an old generation (stale user data) and say it
	// is consuming something a different member is. See KIP-341.
	partitionConsumersByGeneration := make(map[topicPartition][]memberGeneration)

	for _, member := range b.members {
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

	for partition, partitionConsumers := range partitionConsumersByGeneration {
		sort.Slice(partitionConsumers, func(i, j int) bool {
			return partitionConsumers[i].generation > partitionConsumers[j].generation
		})

		memberID := partitionConsumers[0].memberID
		memberPartitions := b.plan[memberID]
		if memberPartitions == nil {
			memberPartitions = new([]topicPartition)
			b.plan[memberID] = memberPartitions
		}
		*memberPartitions = append(*memberPartitions, partition)
		if len(partitionConsumers) > 1 {
			b.priorPlanStragglers[partition] = partitionConsumers[1]
		}
	}

	b.isFreshAssignment = len(b.plan) == 0
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

// initAllConsumersPartitions initializes the two "2All" fields in our
// balancer.
//
// Note that the Java code puts topic partitions that no member is interested
// in into partitions2AllPotentialConsumers. This provides no benefit to any
// part of our balancing and, at worse, could change our partitions by move
// preference unnecessarily.
func (b *balancer) initAllConsumersPartitions() {
	for _, member := range b.members {
		for _, topic := range member.Topics {
			partitions, exists := b.topics[topic]
			if !exists {
				continue
			}
			for _, partition := range partitions {
				topicPartition := topicPartition{topic, partition}
				b.consumers2AllPotentialPartitions[member.ID] = append(b.consumers2AllPotentialPartitions[member.ID], topicPartition)
				b.partitions2AllPotentialConsumers[topicPartition] = append(b.partitions2AllPotentialConsumers[topicPartition], member.ID)
			}
		}
		// Lastly, if this is a new member, the plan everything is
		// using will not know of it. We add that it is consuming nothing
		// in that plan here.
		if _, exists := b.plan[member.ID]; !exists {
			b.plan[member.ID] = new([]topicPartition)
		}
	}
}

// determineUnassignedPartitions does what the name says.
//
// Partitions that a member was consuming but is no longer interested in, as
// well as new partitions that nobody was consuming, are unassigned.
func (b *balancer) determineUnassignedPartitions() {
	// To build a list of unassigned partitions, we visit all partitions
	// in the current plan and, if they still exist and the prior consumer
	// no longer wants to consume them, we track it as unassigned.
	// After, we add all new partitions.
	unvisitedPartitions := b.partitions2AllPotentialConsumers.cloneKeys()

	for member, partitions := range b.plan {
		var keepIdx int
		for _, partition := range *partitions {
			// If this partition no longer exists at all, likely due to the
			// topic being deleted, we remove the partition from the member.
			if _, exists := b.partitions2AllPotentialConsumers[partition]; !exists {
				continue
			}

			delete(unvisitedPartitions, partition)
			b.partitionConsumers[partition] = member

			if !strsHas(b.members[member].Topics, partition.topic) {
				b.unassignedPartitions = append(b.unassignedPartitions, partition)
				continue
			}

			(*partitions)[keepIdx] = partition
			keepIdx++
		}
		*partitions = (*partitions)[:keepIdx]
	}
	for unvisited := range unvisitedPartitions {
		b.unassignedPartitions = append(b.unassignedPartitions, unvisited)
	}
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
func (b *balancer) sortPartitionsByMovePreference() {
	// We avoid adding partitions to the move preference if the partition
	// only has one potential consumer.
	//
	// This effectively takes the place of the fixedPartitions loop in
	// balance in the Java code.
	maybeAddPartition := func(partition topicPartition, potentialConsumers []string) {
		if len(potentialConsumers) > 1 {
			b.partitionsByMovePreference = append(b.partitionsByMovePreference, partition)
		}
	}

	// If not everything consumes the same or this is a fresh assignment,
	// we order by least-likely-to-be-consumed.
	if b.isFreshAssignment || !b.memberSubscriptionsIdentical() {
		for partition, potentialConsumers := range b.partitions2AllPotentialConsumers {
			maybeAddPartition(partition, potentialConsumers)
		}
		sort.Slice(b.partitionsByMovePreference, func(i, j int) bool {
			l, r := b.partitionsByMovePreference[i], b.partitionsByMovePreference[j]

			lPotentials := len(b.partitions2AllPotentialConsumers[l])
			rPotentials := len(b.partitions2AllPotentialConsumers[r])

			return lPotentials < rPotentials ||
				lPotentials == rPotentials &&
					(l.topic < r.topic ||
						l.topic == r.topic &&
							l.partition < r.partition)
		})
	}

	// This is not a fresh assignment, and all consumers can equally
	// consume all partitions.
	//
	// Our sort by move preference here will be partitions on consumers
	// that are consuming the most. That is, partitions on consumers
	// consuming the most are the most likely to be moved.
	consumersByNumPartitions := b.plan.
		deepCloneFilteringNonExistentPartitions(b.partitions2AllPotentialConsumers).
		heapByConsumersPartitions()
	heap.Init(consumersByNumPartitions)

	// Every loop below peels off a partition from the consumer with the
	// largest amount of partitions. Preference is given to partitions
	// that a straggler thinks they own, otherwise, it's just the first.
	unvisitedPartitions := b.partitions2AllPotentialConsumers.shallowClone()
	for consumersByNumPartitions.Len() > 0 {
		consumerWithMostPartitions := consumersByNumPartitions.Peek().(*memberWithPartitions)
		partitions := consumerWithMostPartitions.partitions
		var useIdx int
		for idx, partition := range *partitions {
			if _, exists := b.priorPlanStragglers[partition]; exists {
				useIdx = idx
				break
			}
		}

		usePartition := (*partitions)[useIdx]
		potentialConsumers := unvisitedPartitions[usePartition]
		delete(unvisitedPartitions, usePartition)

		maybeAddPartition(usePartition, potentialConsumers)

		if useIdx == 0 { // common case
			*partitions = (*partitions)[1:]
		} else { // less common: shift from right to preserve ordering
			*partitions = append((*partitions)[:useIdx], (*partitions)[useIdx+1:]...)
		}
		if len(*partitions) > 0 {
			heap.Fix(consumersByNumPartitions, 0)
		} else {
			heap.Pop(consumersByNumPartitions)
		}
	}
	for unvisited, potentialConsumers := range unvisitedPartitions {
		maybeAddPartition(unvisited, potentialConsumers)
	}
}

// Returns whether each member can consume the same partitions.
// The Java code also checks consumers2, but it also stuffs partitions that no
// members can consume into partitions2, which returns false unnecessarily.
// With our code, the maps should be reverse identical.
func (b *balancer) memberSubscriptionsIdentical() bool {
	var firstMembers []string
	var firstSet bool
	for _, members := range b.partitions2AllPotentialConsumers {
		sort.Strings(members)
		if !firstSet {
			firstMembers = members
			firstSet = true
			continue
		}
		if !reflect.DeepEqual(members, firstMembers) {
			return false
		}
	}
	return true
}

func (b *balancer) balance() {
	// First, we assign all unassigned partitions to consumers.
	for _, partition := range b.unassignedPartitions {
		if _, exists := b.partitions2AllPotentialConsumers[partition]; !exists {
			continue
		}
		b.assignPartition(partition)
	}

	// Next, we eliminate from reassignment all members that cannot have
	// their now assigned partition set changed.
	fixedMembers := make(membersPartitions)
	for member, partitions := range b.plan {
		if !b.canMemberParticipateInMove(member, *partitions) {
			fixedMembers[member] = partitions
			b.planByNumPartitions.Delete(memberWithPartitions{
				member,
				partitions,
			})
			delete(b.plan, member)
		}
	}

	// preBalancePlan := plan.deepClone()
}

// assignPartition looks for the first member that can assume this unassigned
// partition, in order from members with smallest partitions, and assigns
// the partition to it.
func (b *balancer) assignPartition(unassigned topicPartition) {
	b.planByNumPartitions.Ascend(func(item btree.Item) bool {
		memberWithFewestPartitions := item.(memberWithPartitions)
		member := memberWithFewestPartitions.member
		memberPotentials := b.consumers2AllPotentialPartitions[member]
		if !partitionsHas(memberPotentials, unassigned) {
			return true
		}

		// Before we change the sort order, delete this item from our
		// btree. If we edo this after changing the order, the tree
		// will not be able to delete the item.
		b.planByNumPartitions.Delete(item)
		partitions := memberWithFewestPartitions.partitions
		*partitions = append(*partitions, unassigned)
		// Add the item back to its new sorted position.
		b.planByNumPartitions.ReplaceOrInsert(memberWithFewestPartitions)

		b.partitionConsumers[unassigned] = member
		return false
	})
}

func (b *balancer) isBalanced() bool {
	minConsumer := b.planByNumPartitions.Min().(memberWithPartitions)
	maxConsumer := b.planByNumPartitions.Max().(memberWithPartitions)
	// If the delta between the min and the max consumer's partition's
	// is 0 or 1, we are balanced.
	if len(*minConsumer.partitions) >= len(*maxConsumer.partitions)-1 {
		return true
	}

	// Across all members, across the partitions a member could have, if
	// any of those partitions are on a member that has _more_ partitions,
	// then this is not balanced.
	//
	// Note that we check one more case than the Java code, but it is
	// not detrimental.
	balanced := true
	b.planByNumPartitions.Ascend(func(item btree.Item) bool {
		current := item.(memberWithPartitions)
		currentMember := current.member
		currentPartitions := *current.partitions

		possiblePartitions := b.consumers2AllPotentialPartitions[currentMember]
		maxPartitions := len(possiblePartitions)

		if len(currentPartitions) == maxPartitions {
			return true
		}

		comparedMembers := make(map[string]struct{})

		for _, partition := range currentPartitions {
			otherMember := b.partitionConsumers[partition]
			if otherMember == currentMember {
				continue
			}
			if _, comparedMember := comparedMembers[otherMember]; comparedMember {
				continue
			}
			comparedMembers[otherMember] = struct{}{}

			otherPartitions := *b.plan[otherMember]

			if len(currentPartitions) < len(otherPartitions)-1 {
				balanced = false
				return false
			}
			if len(currentPartitions) == len(otherPartitions) {
				return true
			}

			// If this member is consuming ONE less than another member, and that
			// member has a partition we can have, we still may be balanced.
			//
			// If the union of what both of these members could consume is equal
			// to that they are consuming now, then moving the partition from
			// the other member to this member would have no benefit. The other
			// member can consume nothing more than the partition we are considering
			// for moving, so moving the partition will just cause this same
			// imbalance in the other direction.
			//
			// Note that the Java code does not do this check, but it is not
			// detrimental.
			otherPossiblePartitions := b.consumers2AllPotentialPartitions[otherMember]
			possiblePartitionsUnion := make(map[topicPartition]struct{}, len(currentPartitions)+len(otherPartitions))
			for _, partition := range possiblePartitions {
				possiblePartitionsUnion[partition] = struct{}{}
			}
			for _, partition := range otherPossiblePartitions {
				possiblePartitionsUnion[partition] = struct{}{}
			}
			if len(currentPartitions)+len(otherPartitions) == len(possiblePartitionsUnion) {
				return true
			}

			balanced = false
			return false
		}
		return true
	})
	return balanced
}

// A member can only participate in reassignment if it has more partitions it
// could potentially consume or if any of the partitions on it can be consumed
// by a different member.
func (b *balancer) canMemberParticipateInMove(
	memberID string,
	memberPartitions []topicPartition,
) bool {
	maxPartitions := len(b.consumers2AllPotentialPartitions[memberID])
	if len(memberPartitions) < maxPartitions {
		return true
	}
	for _, partition := range memberPartitions {
		potentialConsumers := b.partitions2AllPotentialConsumers[partition]
		if len(potentialConsumers) > 1 {
			return true
		}
	}
	return false
}

func (b *balancer) doReassignments() {
	//didReassign := false
	modified := true
	for modified {
		modified = false
	}
}
