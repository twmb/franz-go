// Package sticky provides the overcomplicated Java sticky partitioning
// strategy for Kafka, with modifications made to be stickier and fairer.
//
// For some points on how Java's strategy is flawed, see
// https://github.com/Shopify/sarama/pull/1416/files/b29086bdaae0da7ce71eae3f854d50685fd6b631#r315005878
package sticky

import (
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
	planByNumPartitions *rbtree.Tree

	stealGraph graph
}

type topicPartition struct {
	topic     string
	partition int32
}

func newBalancer(members []GroupMember, topics map[string][]int32) *balancer {
	b := &balancer{
		members: make(map[string]GroupMember, len(members)),
		topics:  topics,

		plan: make(membersPartitions),

		stealGraph: newGraph(),
	}
	for _, member := range members {
		b.members[member.ID] = member
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
	// For planByNumPartitions, we use a btree heap since we will be
	// accessing both the min and max often as well as ranging from
	// smallest to largest.
	//
	// We init this after initAllConsumersPartitions, which can add new
	// members that were not in the prior plan.
	b.assignUnassignedAndInitGraph()
	b.planByNumPartitions = b.plan.rbtreeByLevel()

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

		member := partitionConsumers[0].member
		partitions := b.plan[member]
		if partitions == nil {
			partitions = make(memberPartitions)
			b.plan[member] = partitions
		}
		partitions[partition] = struct{}{}
	}
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

func (b *balancer) assignUnassignedAndInitGraph() {
	var nparts int
	for partitions := range b.topics {
		nparts += len(partitions)
	}

	partitionPotentials := make(map[topicPartition][]string, nparts)

	// First, over all members in this assignment, map each partition to
	// the members that can consume it. We will use this for assigning.
	for _, member := range b.members {
		// If this is a new member, reserve it in our plan.
		if _, exists := b.plan[member.ID]; !exists {
			b.plan[member.ID] = make(map[topicPartition]struct{})
		}
		for _, topic := range member.Topics {
			for _, partition := range b.topics[topic] {
				tp := topicPartition{topic, partition}
				if potentials, exists := partitionPotentials[tp]; !exists {
					partitionPotentials[tp] = append(make([]string, 0, len(b.members)), member.ID)
				} else {
					partitionPotentials[tp] = append(potentials, member.ID)
				}
			}
		}
	}

	// Next, over the prior plan, un-map deleted topics or topics that
	// members no longer want. This is where we determine what is now
	// unassigned.
	unassigned := make(map[topicPartition]struct{}, nparts)
	for partition := range partitionPotentials {
		unassigned[partition] = struct{}{}
	}
	partitionConsumers := make(map[topicPartition]string, nparts)
	for member, partitions := range b.plan {
		for partition := range partitions {
			if _, exists := partitionPotentials[partition]; !exists { // topic baleted
				delete(unassigned, partition)
				delete(partitions, partition)
				continue
			}
			memberTopics := b.members[member].Topics
			var memberStillWantsTopic bool
			for _, memberTopic := range memberTopics {
				if memberTopic == partition.topic {
					memberStillWantsTopic = true
					break
				}
			}
			if !memberStillWantsTopic {
				delete(partitions, partition)
				continue
			}
			delete(unassigned, partition)
			partitionConsumers[partition] = member
		}
	}

	// We now assign everything we know is not currently assigned.
	for partition := range unassigned {
		potentials := partitionPotentials[partition]
		if len(potentials) == 0 {
			continue
		}
		assigned := b.assignPartition(partition, potentials)
		partitionConsumers[partition] = assigned
	}

	// Lastly, with everything assigned, we build our steal graph for balancing.
	for member, partitions := range b.plan {
		b.stealGraph.add(member, partitions)
	}
	for partition, potentials := range partitionPotentials {
		owner := partitionConsumers[partition]
		for _, potential := range potentials {
			if owner == potential {
				continue
			}
			b.stealGraph.link(potential, partition)
		}
	}
}

// assignPartition looks for the first member that can assume this unassigned
// partition, in order from members with smallest partitions, and assigns
// the partition to it.
func (b *balancer) assignPartition(unassigned topicPartition, potentials []string) string {
	var minMember string
	var minPartitions memberPartitions
	for _, potential := range potentials {
		partitions := b.plan[potential]
		if minPartitions == nil || len(partitions) < len(minPartitions) {
			minMember = potential
			minPartitions = partitions
		}
	}

	minPartitions[unassigned] = struct{}{}
	return minMember
}

func (b *balancer) balance() {
	b.shuffle()
}

// shuffle loops trying to move partitions until the plan is balanced
// or until no moves happen.
//
// O(M * P^2) i.e. not great.
func (b *balancer) shuffle() {
	// O(lg(level disparity))?, which is at most P (all partitions in one)?
	for min := b.planByNumPartitions.Min(); b.planByNumPartitions.Len() > 1; min = b.planByNumPartitions.Min() {
		level := min.Item.(partitionLevel)
		for len(level.members) > 0 {
			for member := range level.members {
				// If we could not steal from a big imbalance, we may be
				// have a transitive steal.
				stealPath, found := b.stealGraph.findSteal(member)
				if found {
					for _, segment := range stealPath {
						b.reassignPartition(segment.src, segment.dst, segment.part)
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

func (b *balancer) reassignPartition(src, dst string, partition topicPartition) {
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
