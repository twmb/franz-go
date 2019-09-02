// Package sticky provides sticky partitioning strategy for Kafka,
// with a complete overhaul to be more understandable and optimal.
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
	// This is built in newBalancer mapping member IDs to the GroupMember.
	members map[string]GroupMember

	// topics are the topic names and partitions that the client knows of
	// and passed to be used for balancing.
	topics map[string][]int32

	// partBuf and partBuf at contain the backing topicPartitions that
	// are referenced in the algorithm, reducing thousands of small
	// allocations to one large one.
	partBuf   []topicPartition
	partBufAt int

	// Stales tracks partitions that are doubly subscribed in this join
	// where one of the subscribers is on an old generation.
	//
	// The newer generation goes into plan directly, the older gets
	// stuffed here.
	stales map[*topicPartition]string

	// plan is what we are building and balancing.
	plan membersPartitions

	// planByNumPartitions orders plan members into partition count levels.
	//
	// The nodes in the tree reference values in plan, meaning updates in
	// this field are visible in plan.
	planByNumPartitions *rbtree.Tree

	// stealGraph is a graphical representation of members and partitions
	// they want to steal.
	stealGraph graph
}

type topicPartition struct {
	topic     string
	partition int32
}

func newBalancer(members []GroupMember, topics map[string][]int32) *balancer {
	var nparts int
	for _, partitions := range topics {
		nparts += len(partitions)
	}

	b := &balancer{
		members: make(map[string]GroupMember, len(members)),
		topics:  topics,
		partBuf: make([]topicPartition, nparts),
		stales:  make(map[*topicPartition]string),
		plan:    make(membersPartitions, len(members)),
	}
	b.stealGraph = newGraph(b.plan, nparts)

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

func (b *balancer) newPartitionPointer(p topicPartition) *topicPartition {
	b.partBuf[b.partBufAt] = p
	r := &b.partBuf[b.partBufAt]
	b.partBufAt++
	return r
}

// memberPartitions contains partitions for a member.
type memberPartitions map[*topicPartition]struct{}

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

func Balance(members []GroupMember, topics map[string][]int32) Plan {
	b := newBalancer(members, topics)
	b.parseMemberMetadata()
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
			// If the topic no longer exists in our topics, no sense keeping
			// it around here only to be deleted later.
			if _, exists := b.topics[topicPartition.topic]; !exists {
				continue
			}
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

		tpp := b.newPartitionPointer(partition)
		partitions[tpp] = struct{}{}

		if len(partitionConsumers) > 1 {
			b.stales[tpp] = partitionConsumers[1].member
		}
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

// assignUnassignedAndInitGraph is a long function that assigns unassigned
// functions to the least loaded members and initializes our steal graph.
//
// Doing so requires a bunch of metadata, and in the process we want to remove
// partitions from the plan that no longer exist in the client.
func (b *balancer) assignUnassignedAndInitGraph() {
	partitionPointers := make(map[topicPartition]*topicPartition, cap(b.partBuf))
	for _, partitions := range b.plan {
		for partition := range partitions {
			partitionPointers[*partition] = partition
		}
	}

	partitionPotentials := make(map[*topicPartition][]string, cap(b.partBuf))

	// First, over all members in this assignment, map each partition to
	// the members that can consume it. We will use this for assigning.
	for _, member := range b.members {
		// If this is a new member, reserve it in our plan.
		if _, exists := b.plan[member.ID]; !exists {
			b.plan[member.ID] = make(memberPartitions, 100)
		}
		for _, topic := range member.Topics {
			for _, partition := range b.topics[topic] {
				tp := topicPartition{topic, partition}
				tpp, exists := partitionPointers[tp]
				if !exists {
					tpp = b.newPartitionPointer(tp)
					partitionPointers[tp] = tpp
				}
				potentials, exists := partitionPotentials[tpp]
				if !exists {
					potentials = make([]string, 0, len(b.members))
				}
				potentials = append(potentials, member.ID)
				partitionPotentials[tpp] = potentials
			}
		}
	}

	// Next, over the prior plan, un-map deleted topics or topics that
	// members no longer want. This is where we determine what is now
	// unassigned.
	unassigned := make(map[*topicPartition]struct{}, nparts)
	for _, partition := range partitionPointers {
		unassigned[partition] = struct{}{}
	}
	partitionConsumers := make(map[*topicPartition]string, nparts)
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

	b.tryRestickyStales(unassigned, partitionPotentials, partitionConsumers)

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
	b.stealGraph.cxns = partitionConsumers
	for member := range b.plan {
		b.stealGraph.add(member)
	}
	for partition, potentials := range partitionPotentials {
		for _, potential := range potentials {
			b.stealGraph.link(potential, partition)
		}
	}
}

// tryRestickyStales is a pre-assigning step where, for all stale members,
// we give partitions back to them if the partition is currently on an
// over loaded member or unassigned.
//
// This effectively re-stickies members before we balance further.
func (b *balancer) tryRestickyStales(
	unassigned map[*topicPartition]struct{},
	partitionPotentials map[*topicPartition][]string,
	partitionConsumers map[*topicPartition]string,
) {
	for stale, lastOwner := range b.stales {
		potentials := partitionPotentials[stale]
		if len(potentials) == 0 {
			continue
		}
		var canTake bool
		for _, potential := range potentials {
			if potential == lastOwner {
				canTake = true
			}
		}
		if !canTake {
			return
		}

		if _, isUnassigned := unassigned[stale]; isUnassigned {
			b.plan[lastOwner][stale] = struct{}{}
			delete(unassigned, stale)
		}

		currentOwner := partitionConsumers[stale]
		lastOwnerPartitions := b.plan[lastOwner]
		currentOwnerPartitions := b.plan[currentOwner]
		if len(lastOwnerPartitions)+1 < len(currentOwnerPartitions) {
			delete(currentOwnerPartitions, stale)
			lastOwnerPartitions[stale] = struct{}{}
		}
	}
}

// assignPartition looks for the least loaded member that can take this
// partition and assigns it to that member.
func (b *balancer) assignPartition(unassigned *topicPartition, potentials []string) string {
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

// balance loops trying to move partitions until the plan is as balanced
// as it can be.
func (b *balancer) balance() {
	for min := b.planByNumPartitions.Min(); b.planByNumPartitions.Len() > 1; min = b.planByNumPartitions.Min() {
		level := min.Item.(partitionLevel)
		// If this max level is within one of this level, then nothing
		// can steal down so we return early.
		if b.planByNumPartitions.Max().Item.(partitionLevel).level <= level.level+1 {
			return
		}
		// We continually loop over this level until every member is
		// static (deleted) or bumped up a level. It is possible for a
		// member to bump itself up only to have a different in this
		// level steal from it and bump that original member back down,
		// which is why we do not just loop once over level.members.
		for len(level.members) > 0 {
			for member := range level.members {
				if stealPath, found := b.stealGraph.findSteal(member); found {
					for _, segment := range stealPath {
						b.reassignPartition(segment.src, segment.dst, segment.part)
					}
					continue
				}

				// If we could not find a steal path, this
				// member is not static (will never grow).
				delete(level.members, member)
				if len(level.members) == 0 {
					b.planByNumPartitions.Delete(b.planByNumPartitions.Min())
				}
			}
		}
	}
}

func (b *balancer) reassignPartition(src, dst string, partition *topicPartition) {
	srcPartitions := b.plan[src]
	dstPartitions := b.plan[dst]

	oldSrcLevel := len(srcPartitions)
	oldDstLevel := len(dstPartitions)

	delete(srcPartitions, partition)
	dstPartitions[partition] = struct{}{}

	b.fixMemberLevel(
		b.planByNumPartitions.FindWith(func(n *rbtree.Node) int {
			return oldSrcLevel - n.Item.(partitionLevel).level
		}),
		src,
		srcPartitions,
	)
	b.fixMemberLevel(
		b.planByNumPartitions.FindWith(func(n *rbtree.Node) int {
			return oldDstLevel - n.Item.(partitionLevel).level
		}),
		dst,
		dstPartitions,
	)

	b.stealGraph.changeOwnership(partition, dst)
}
