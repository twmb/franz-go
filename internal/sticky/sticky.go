// Package sticky provides the overcomplicated Java sticky partitioning
// strategy for Kafka, with modifications made to be stickier and fairer.
//
// For some points on how Java's strategy is flawed, see
// https://github.com/Shopify/sarama/pull/1416/files/b29086bdaae0da7ce71eae3f854d50685fd6b631#r315005878
package sticky

import (
	"container/list"
	"fmt"
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
	plus1sBase []topicPartition,
) (
	steal topicPartition,
	plus1s []topicPartition,
) {
	plus1s = plus1sBase[:0]

	if len(partitions) == len(potentials) {
		return
	}

	var stealeeNum int
	for want := range potentials { // O(P)
		other := b.partitionConsumers[want]
		if other == member {
			continue
		}
		otherNum := len(b.plan[other])

		if otherNum > stealeeNum && otherNum > len(partitions)+1 {
			steal, stealeeNum = want, otherNum
		}
		if steal.isZero() && otherNum == len(partitions)+1 {
			plus1s = append(plus1s, want)
		}
	}
	return
}

// shuffle loops trying to move partitions until the plan is balanced
// or until no moves happen.
//
// O(M * P^2) i.e. not great.
func (b *balancer) shuffle() {
	deps := newDependents()

	var plus1s []topicPartition

iter:
	// O(lg(level disparity))?, which is at most P (all partitions in one)?
	for it := rbtree.IterAt(b.planByNumPartitions.Min()); it.Ok(); it.Right() {
		level := it.Item().(partitionLevel)
		fmt.Println("on level", level.level)
		for member, partitions := range level.members {
			fmt.Println("on", member)

			potentials := b.consumers2AllPotentialPartitions[member]
			var steal topicPartition
			steal, plus1s = b.findStealPartition(member, partitions, potentials, plus1s[:0])

			// If we found a member we can steal from, we do so and fix
			// our position and the stealees position in the level heap.
			if !steal.isZero() {
				b.reassignPartition(member, steal)

				// We only need to reset iterating if we cleared this level,
				// otherwise we can just continue looping on this level.
				if len(level.members) == 0 {
					it.Reset(rbtree.Into(b.planByNumPartitions.Min()))
					continue iter
				}
				continue
			}

			// We found nothing obvious to steal to steal.
			//
			// If we see a +1, and we have a chain, we steal.
			//
			// Otherwise, we now will delete ourself from iter
			// consideration. If we have all we can have and no
			// dependents, we continue; else, we register ourself
			// dependent on everything that we could steal from.

			if len(plus1s) > 0 && deps.memberHasDependents(member) {
				b.reassignPartition(member, plus1s[0])

				deps.bubbleDown(b, member)

				if len(level.members) == 0 {
					it.Reset(rbtree.Into(b.planByNumPartitions.Min()))
					continue iter
				}
				continue
			}

			//b.removeLevelingMember(it.Node(), member)
			if len(partitions) == len(potentials) &&
				!deps.memberHasDependents(member) {
				continue
			}

		}
	}
}

func (b *balancer) reassignPartition(dst string, partition topicPartition) {
	src := b.partitionConsumers[partition]
	b.partitionConsumers[partition] = dst

	srcPartitions := b.plan[src]
	dstPartitions := b.plan[dst]

	fmt.Printf("reassigning %s from %s to %s\n", partition.topic, src, dst)

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
}

type dependents struct {
	chains map[string]*chain
}

func newDependents() *dependents {
	return &dependents{
		chains: make(map[string][]*chain),
	}
}

func (deps *dependents) memberHasDependents(member string) bool {
	return len(deps.chains[member]) > 0
}

func (deps *dependents) bubbleDown(b *balancer, src string) {
	srcChains := deps.chains[src]
start:
	minChain := srcChains[0]
	for _, chain := range srcChains[1:] {
		if chain.minParts < minChain.minParts {
			minChain = chain
		}
	}

	dstElem := minChain.list.Front()
	dst := dstElem.Value.(string)

bubble:
	fmt.Printf("bubble down, src %s, dst %s\n", src, dst)
	var take topicPartition
	for take = range b.consumers2AllPotentialPartitions[src] {
		if _, canTake := b.partitions2AllPotentialConsumers[take][dst]; canTake {
			break
		}
	}

	// If the chain we chose actually cannot take anything, then a
	// different equal chain under us took whatever this one could have.
	//
	// We delete this chain, since this member will never have another
	// partition for this chain, and we go back to the start to work on the
	// other equal chain.
	//
	// Can this be transitively messed up?
	if take.isZero() {
		srcChains = srcChains[1:]
		deps.chains[src] = srcChains
		goto start
	}

	b.reassignPartition(dst, take)
	dstElem = dstElem.Next()
	if dstElem != nil {
		src = dst
		dst = dstElem.Value.(string)
		if len(b.plan[dst]) != minChain.minParts {
			goto bubble
		}
	}

	if len(b.plan[dst]) > minChain.minParts {
		minChain.minParts++
	}
	minChain.totParts++
}

type chain struct {
	list     list.List
	minParts int
	totParts int

	possibleParts map[topicPartition]struct{}
}

func newChain() *chain {
	c := &chain{
		possibleParts: make(map[topicPartition]struct{}),
	}
	c.list.Init()
	return c
}

func (c *chain) pushMember(member string, numParts int, possiblePartitions map[topicPartition]struct{}) {
	c.list.PushBack(member)
	if c.minParts == 0 {
		c.minParts = numParts
	}
	c.totParts += numParts
	for possiblePartition := range possiblePartitions {
		c.possibleParts[possiblePartition] = struct{}{}
	}
}

func (c *chain) maxParts() int {
	return len(c.possibleParts)
}
