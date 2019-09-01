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

	stealGraph graph
}

type topicPartition struct {
	topic     string
	partition int32
}

func (t topicPartition) isZero() bool {
	return len(t.topic) == 0
}

func newBalancer(members []GroupMember, topics map[string][]int32) *balancer {
	b := &balancer{
		members: make(map[string]groupMember, len(members)),
		topics:  topics,

		plan: make(membersPartitions),

		partitionConsumers: make(map[topicPartition]string),

		partitions2AllPotentialConsumers: make(staticPartitionMembers),
		consumers2AllPotentialPartitions: make(staticMembersPartitions),

		stealGraph: newGraph(),
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

// memberPartitions contains partitions for a member.
type memberPartitions map[topicPartition]struct{}

// membersPartitions maps members to their partitions.
type membersPartitions map[string]memberPartitions

type partitionLevel struct {
	level   int
	members membersPartitions
}

func (b *balancer) fixMemberLevel(
	src *rbtree.Node,
	member string,
	partitions memberPartitions,
) {
	b.removeLevelingMember(src, member)
	newLevel := len(partitions)
	b.planByNumPartitions.FindWithOrInsertWith(
		func(n *rbtree.Node) int { return newLevel - n.Item.(partitionLevel).level },
		func() rbtree.Item { return newPartitionLevel(newLevel) },
	).Item.(partitionLevel).members[member] = partitions
}

func (b *balancer) removeLevelingMember(
	src *rbtree.Node,
	member string,
) {
	currentLevel := src.Item.(partitionLevel)
	delete(currentLevel.members, member)
	if len(currentLevel.members) == 0 {
		b.planByNumPartitions.Delete(src)
	}
}

func (l partitionLevel) Less(r rbtree.Item) bool {
	return l.level < r.(partitionLevel).level
}

func newPartitionLevel(level int) partitionLevel {
	return partitionLevel{level, make(membersPartitions)}
}

func (m membersPartitions) rbtreeByLevel() *rbtree.Tree {
	var t rbtree.Tree
	for member, partitions := range m {
		level := len(partitions)
		t.FindWithOrInsertWith(
			func(n *rbtree.Node) int { return level - n.Item.(partitionLevel).level },
			func() rbtree.Item { return newPartitionLevel(level) },
		).Item.(partitionLevel).members[member] = partitions
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
	b.planByNumPartitions = b.plan.rbtreeByLevel()
	b.assignUnassignedPartitions()
	b.initStealGraph()

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
		partitions := b.plan[member]
		if partitions == nil {
			partitions = make(memberPartitions)
			b.plan[member] = partitions
		}
		partitions[partition] = struct{}{}
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

func (b *balancer) initStealGraph() {
	for member, partitions := range b.plan {
		b.stealGraph.add(member, partitions)
	}

	for member := range b.plan {
		for potential := range b.consumers2AllPotentialPartitions[member] {
			owner := b.partitionConsumers[potential]
			if owner == member {
				continue
			}
			b.stealGraph.link(member, potential)
		}
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
		level := it.Item().(partitionLevel)
		for member, partitions := range level.members {
			memberPotentials := b.consumers2AllPotentialPartitions[member]
			if _, memberCanConsumePartition := memberPotentials[unassigned]; !memberCanConsumePartition {
				continue
			}
			partitions[unassigned] = struct{}{}
			b.partitionConsumers[unassigned] = member
			b.fixMemberLevel(it.Node(), member, partitions)
			return
		}
	}
}

func (b *balancer) findStealPartition(
	member string,
	partitions map[topicPartition]struct{},
	potentials map[topicPartition]struct{},
) (
	steal topicPartition,
) {

	if len(partitions) == len(potentials) {
		return
	}

	var stealeeNum int
	for want := range potentials { // O(P); maybe switch to ordered heap ? Would be M log(M) ?
		other := b.partitionConsumers[want]
		if other == member {
			continue
		}
		otherNum := len(b.plan[other])

		if otherNum > stealeeNum && otherNum > len(partitions)+1 {
			steal, stealeeNum = want, otherNum
		}
	}
	return
}

// shuffle loops trying to move partitions until the plan is balanced
// or until no moves happen.
//
// O(M * P^2) i.e. not great.
func (b *balancer) shuffle() {
	// O(lg(level disparity))?, which is at most P (all partitions in one)?
	for min := b.planByNumPartitions.Min(); min != nil; min = b.planByNumPartitions.Min() {
		level := min.Item.(partitionLevel)
		for len(level.members) > 0 {
			for member, partitions := range level.members {

				// TODO evaluate whether we should "findStealPartition" at all
				// vs. just using the stealGraph
				// Would have to change A* to search all nodes at a match rather
				// than returning on first match so that we can use the node with
				// the most partitions. This is necessary to preserve stickiness.
				// With the current approach, we stop at first match since all
				// first matches are +2 only guaranteed. If we remove the heap
				// part, then the first match may steal from a node that could
				// be sticky; we would need to iterate over all at level to keep
				// stickiness by only taking from most loaded.
				potentials := b.consumers2AllPotentialPartitions[member]
				steal := b.findStealPartition(member, partitions, potentials)

				// If we found a member we can steal from, we do so and fix
				// our position and the stealees position in the level heap.
				if !steal.isZero() {
					b.reassignPartition(member, steal)
					continue
				}

				// If we could not steal from a big imbalance, we may be
				// have a transitive steal.
				stealPath, found := b.stealGraph.findSteal(member)
				if found {
					for _, segment := range stealPath {
						b.reassignPartition(segment.dst, segment.part)
					}
					continue
				}

				// If we did not have a transitive steal, this member
				// will never grow past this level and can be removed
				// from consideration.
				delete(level.members, member)
				if len(level.members) == 0 {
					b.planByNumPartitions.Delete(b.planByNumPartitions.Min())
				}
			}
		}
	}
}

func (b *balancer) reassignPartition(dst string, partition topicPartition) {
	src := b.partitionConsumers[partition]
	b.partitionConsumers[partition] = dst

	srcPartitions := b.plan[src]
	dstPartitions := b.plan[dst]

	delete(srcPartitions, partition)
	dstPartitions[partition] = struct{}{}

	b.fixMemberLevel(
		b.planByNumPartitions.FindWith(func(n *rbtree.Node) int {
			return len(srcPartitions) + 1 - n.Item.(partitionLevel).level
		}),
		src,
		srcPartitions,
	)
	b.fixMemberLevel(
		b.planByNumPartitions.FindWith(func(n *rbtree.Node) int {
			return len(dstPartitions) - 1 - n.Item.(partitionLevel).level
		}),
		dst,
		dstPartitions,
	)

	b.stealGraph.changeOwnership(src, dst, partition)
}
