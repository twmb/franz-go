// Package sticky provides the overcomplicated Java sticky partitioning
// strategy for Kafka, with modifications made to be stickier and fairer.
//
// For some points on how Java's strategy is flawed, see
// https://github.com/Shopify/sarama/pull/1416/files/b29086bdaae0da7ce71eae3f854d50685fd6b631#r315005878
package sticky

import (
	"reflect"
	"sort"

	"github.com/twmb/go-rbtree"

	"github.com/twmb/kgo/kmsg"
)

// Sticky partitioning has two versions, the latter from KIP-341 preventing a
// bug. The second version introduced generations with the default generation
// from the first generation's consumers defaulting to -1.

const defaultGeneration = -1

type GroupMember struct {
	ID string

	Version  int16
	Topics   []string
	UserData []byte
}

type groupMember struct {
	id       string
	version  int16
	topics   map[string]struct{}
	userData []byte
}

type Plan map[string]map[string][]int32

type balancer struct {
	// members are the members in play for this balance.
	//
	// This is built in newBalancer mapping member IDs to the GroupMember.
	members map[string]groupMember

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
	planByNumPartitions *rbtree.Tree

	// isFreshAssignment tracks whether this is the first join for a group.
	// This is true if no member has userdata (plan is empty)
	isFreshAssignment bool
	// subscriptionsAreIdentical tracks if every member can consume the
	// same partitions. If true, this makes the isBalanced check much
	// simpler.
	subscriptionsAreIdentical bool

	// partitionConsumers maps all possible partitions to consume to the
	// members that are consuming them.
	//
	// We initialize this from our plan and modify it during reassignment.
	// We use this to know what member we are stealing partitions from.
	partitionConsumers map[topicPartition]string

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

type topicPartition struct {
	topic     string
	partition int32
}

func newBalancer(members []GroupMember, topics map[string][]int32) *balancer {
	b := &balancer{
		members: make(map[string]groupMember, len(members)),
		topics:  topics,

		plan: make(membersPartitions),

		partitionConsumers: make(map[topicPartition]string),

		partitions2AllPotentialConsumers: make(staticPartitionMembers),
		consumers2AllPotentialPartitions: make(staticMembersPartitions),
	}
	for _, member := range members {
		gm := groupMember{
			id:       member.ID,
			version:  member.Version,
			topics:   make(map[string]struct{}, len(member.Topics)),
			userData: member.UserData,
		}
		for _, topic := range member.Topics {
			gm.topics[topic] = struct{}{}
		}
		b.members[gm.id] = gm
	}
	return b
}

func (b *balancer) into() Plan {
	plan := make(Plan, len(b.plan))
	for member, partitions := range b.plan {
		topics, exists := plan[member]
		if !exists {
			topics = make(map[string][]int32)
			plan[member] = topics
		}
		for partition := range partitions {
			topics[partition.topic] = append(topics[partition.topic], partition.partition)
		}
	}
	return plan
}

// staticMembersPartitions is like membersPartitions below, but is used only
// for consumers2AllPotentialPartitions. The value is built once and never
// changed. Essentially, this is a clearer type.
type staticMembersPartitions map[string]map[topicPartition]struct{}

// membersPartitions maps members to a pointer of their partitions.
type membersPartitions map[string]map[topicPartition]struct{}

// memberWithPartitions ties a member to a pointer to its partitions.
// The partitions is the same map that is in membersPartitions.
type memberWithPartitions struct {
	member     string
	partitions map[topicPartition]struct{}
}

func (l memberWithPartitions) less(r memberWithPartitions) bool {
	return len(l.partitions) < len(r.partitions) ||
		len(l.partitions) == len(r.partitions) &&
			l.member < r.member
}

func (l memberWithPartitions) Less(r rbtree.Item) bool {
	return l.less(r.(memberWithPartitions))
}

func (m membersPartitions) intoConsumersPartitions() []memberWithPartitions {
	var consumersPartitions []memberWithPartitions
	for member, partitions := range m {
		consumersPartitions = append(consumersPartitions,
			memberWithPartitions{
				member,
				partitions,
			},
		)
	}
	return consumersPartitions
}

func (m membersPartitions) rbtreeByConsumersPartitions() *rbtree.Tree {
	var t rbtree.Tree
	for _, memberWithPartitions := range m.intoConsumersPartitions() {
		t.Insert(memberWithPartitions)
	}
	return &t
}

// staticPartitionMember is the same as partitionMembers, but we type name it
// to imply immutability in reading. All mutable uses go through cloneKeys
// or shallowClone.
type staticPartitionMembers map[topicPartition]map[string]struct{}

func (orig staticPartitionMembers) cloneKeys() map[topicPartition]struct{} {
	dup := make(map[topicPartition]struct{}, len(orig))
	for partition := range orig {
		dup[partition] = struct{}{}
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
	b.planByNumPartitions = b.plan.rbtreeByConsumersPartitions()
	b.assignUnassignedPartitions()

	b.balance()

	return b.into()
}

// parseMemberMetadata parses all member userdata to initialize the prior plan.
func (b *balancer) parseMemberMetadata() {
	type memberGeneration struct {
		member     string
		generation int32
	}

	// all partitions => members that are consuming those partitions
	// Each partition should only have one consumer, but a flaky member
	// could rejoin with an old generation (stale user data) and say it
	// is consuming something a different member is. See KIP-341.
	partitionConsumersByGeneration := make(map[topicPartition][]memberGeneration)

	for _, member := range b.members {
		memberPlan, generation := deserializeUserData(member.version, member.userData)
		memberGeneration := memberGeneration{
			member.id,
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

		member := partitionConsumers[0].member
		memberPartitions := b.plan[member]
		if memberPartitions == nil {
			memberPartitions = make(map[topicPartition]struct{})
			b.plan[member] = memberPartitions
		}
		memberPartitions[partition] = struct{}{}
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
		for topic := range member.topics {
			partitions, exists := b.topics[topic]
			if !exists {
				continue
			}
			for _, partition := range partitions {
				consumerPotentialPartitions := b.consumers2AllPotentialPartitions[member.id]
				if consumerPotentialPartitions == nil {
					consumerPotentialPartitions = make(map[topicPartition]struct{})
					b.consumers2AllPotentialPartitions[member.id] = consumerPotentialPartitions
				}

				topicPartition := topicPartition{topic, partition}
				partitionPotentialConsumers := b.partitions2AllPotentialConsumers[topicPartition]
				if partitionPotentialConsumers == nil {
					partitionPotentialConsumers = make(map[string]struct{})
					b.partitions2AllPotentialConsumers[topicPartition] = partitionPotentialConsumers
				}

				consumerPotentialPartitions[topicPartition] = struct{}{}
				partitionPotentialConsumers[member.id] = struct{}{}
			}
		}
		// Lastly, if this is a new member, the plan everything is
		// using will not know of it. We add that it is consuming nothing
		// in that plan here.
		if _, exists := b.plan[member.id]; !exists {
			b.plan[member.id] = make(map[topicPartition]struct{})
		}
	}

	b.setIfMemberSubscriptionsIdentical()
}

// Determines whether each member can consume the same partitions.
//
// The Java code also checks consumers2, but it also stuffs partitions that no
// members can consume into partitions2, which returns false unnecessarily.
// With our code, the maps should be reverse identical.
func (b *balancer) setIfMemberSubscriptionsIdentical() {
	var firstMembers map[string]struct{}
	var firstSet bool
	for _, members := range b.partitions2AllPotentialConsumers {
		if !firstSet {
			firstMembers = members
			firstSet = true
			continue
		}
		if !reflect.DeepEqual(members, firstMembers) {
			return
		}
	}
	b.subscriptionsAreIdentical = true
}

// assignUnassignedPartitions does what the name says.
//
// Partitions that a member was consuming but is no longer interested in, as
// well as new partitions that nobody was consuming, are unassigned.
func (b *balancer) assignUnassignedPartitions() {
	// To build a list of unassigned partitions, we visit all partitions
	// in the current plan and, if they still exist and the prior consumer
	// no longer wants to consume them, we track it as unassigned.
	// After, we add all new partitions.
	unvisitedPartitions := b.partitions2AllPotentialConsumers.cloneKeys()

	var unassignedPartitions []topicPartition
	for member, partitions := range b.plan {
		for partition := range partitions {
			// If this partition no longer exists at all, likely due to the
			// topic being deleted, we remove the partition from the member.
			if _, exists := b.partitions2AllPotentialConsumers[partition]; !exists {
				delete(partitions, partition)
				continue
			}

			delete(unvisitedPartitions, partition)

			if _, memberStillWantsTopic := b.members[member].topics[partition.topic]; !memberStillWantsTopic {
				unassignedPartitions = append(unassignedPartitions, partition)
				delete(partitions, partition)
				continue
			}

			b.partitionConsumers[partition] = member
		}
	}
	for unvisited := range unvisitedPartitions {
		unassignedPartitions = append(unassignedPartitions, unvisited)
	}

	// With our list of unassigned partitions, if the partition can be
	// assigned, we assign it to the least loaded member.
	for _, partition := range unassignedPartitions {
		if _, exists := b.partitions2AllPotentialConsumers[partition]; !exists {
			continue
		}
		b.assignPartition(partition)
	}
}

func (b *balancer) balance() {
	b.shuffle()
}

// assignPartition looks for the first member that can assume this unassigned
// partition, in order from members with smallest partitions, and assigns
// the partition to it.
func (b *balancer) assignPartition(unassigned topicPartition) {
	for it := rbtree.IterAt(b.planByNumPartitions.Min()); it.Ok(); it.Right() {
		memberWithFewestPartitions := it.Item().(memberWithPartitions)
		member := memberWithFewestPartitions.member
		memberPotentials := b.consumers2AllPotentialPartitions[member]
		if _, memberCanConsumePartition := memberPotentials[unassigned]; !memberCanConsumePartition {
			continue
		}

		memberWithFewestPartitions.partitions[unassigned] = struct{}{}
		b.partitionConsumers[unassigned] = member
		b.planByNumPartitions.Fix(it.Node()) // can do since Item contains pointer
		return
	}
}

// shuffle loops trying to move partitions until the plan is balanced
// or until no moves happen.
//
// O(M * P^2) i.e. not great.
func (b *balancer) shuffle() {
	for it := rbtree.IterAt(b.planByNumPartitions.Min()); it.Ok(); it.Right() { // O(M)
		leastLoaded := it.Item().(memberWithPartitions)
		myMember := leastLoaded.member
		myPartitions := leastLoaded.partitions

		var mostHavingMember string
		var mostHavingNum int
		var stealPart topicPartition
		for wantPart := range b.consumers2AllPotentialPartitions[myMember] { // O(P)
			current := b.partitionConsumers[wantPart]
			if current == myMember {
				continue
			}
			currentPartitions := b.plan[current]
			if len(currentPartitions) > mostHavingNum &&
				len(currentPartitions) > len(myPartitions)+1 {

				mostHavingMember = current
				mostHavingNum = len(currentPartitions)
				stealPart = wantPart
			}
		}

		if mostHavingMember != "" {
			b.reassignPartition(stealPart, mostHavingMember, myMember)
			it.Reset(rbtree.Before(b.planByNumPartitions.Min()))
		}
	}
}

// reassignPartition reassigns a partition from srcMember to dstMember, potentially
// undoing a prior move if this detects a partition when there-and-back.
// 2*O(log members)
func (b *balancer) reassignPartition(partition topicPartition, srcMember, dstMember string) {
	oldPartitions := b.plan[srcMember]
	newPartitions := b.plan[dstMember]

	// Remove the elements from our btree before we change the sort order.
	// We have to delete both now rather than fixing both below because we
	// change two sort orders.
	srcNode := b.planByNumPartitions.Delete(
		b.planByNumPartitions.Find(memberWithPartitions{
			srcMember,
			oldPartitions,
		}))

	dstNode := b.planByNumPartitions.Delete(
		b.planByNumPartitions.Find(memberWithPartitions{
			dstMember,
			newPartitions,
		}))

	delete(oldPartitions, partition)
	newPartitions[partition] = struct{}{}

	// Now add back the changed elements to our btree.
	b.planByNumPartitions.Reinsert(srcNode)
	b.planByNumPartitions.Reinsert(dstNode)

	// Finally, update which member is consuming the partition.
	b.partitionConsumers[partition] = dstMember
}

// Map of dependents down,
// If we see there is a dependent down,
// Steal up,
// Reset at dependent
