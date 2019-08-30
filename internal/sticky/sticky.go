// Package sticky provides the overcomplicated Java sticky partitioning
// strategy for Kafka, with modifications made to be stickier and fairer.
//
// For some points on how Java's strategy is flawed, see
// https://github.com/Shopify/sarama/pull/1416/files/b29086bdaae0da7ce71eae3f854d50685fd6b631#r315005878
package sticky

import (
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

func (b *balancer) moveMemberLevel(
	src *rbtree.Node,
	member string,
	partitions memberPartitions,
) {
	currentLevel := src.Item.(partitionLevel)
	delete(currentLevel.members, member)
	if len(currentLevel.members) == 0 {
		b.planByNumPartitions.Delete(src)
	}
	newLevel := len(partitions)
	b.planByNumPartitions.FindWithOrInsertWith(
		func(n *rbtree.Node) int { return newLevel - n.Item.(partitionLevel).level },
		func() rbtree.Item { return newPartitionLevel(newLevel) },
	).Item.(partitionLevel).members[member] = partitions
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
			b.moveMemberLevel(it.Node(), member, partitions)
			return
		}
	}
}

// shuffle loops trying to move partitions until the plan is balanced
// or until no moves happen.
//
// O(M * P^2) i.e. not great.
func (b *balancer) shuffle() {
	//var deps dependents
	//deps.reset()

iter:
	for it := rbtree.IterAt(b.planByNumPartitions.Min()); it.Ok(); it.Right() { // O(M)
		level := it.Item().(partitionLevel)
		fmt.Println("on level", level.level)
		for member, partitions := range level.members {
			fmt.Println("on", member)

			var (
				mostMember     string
				mostPartitions memberPartitions

				toSteal topicPartition

				//equals []memberPartition
				//plus1s []memberPartition
			)

			for wantPart := range b.consumers2AllPotentialPartitions[member] { // O(P)
				other := b.partitionConsumers[wantPart]
				if other == member {
					continue
				}
				otherPartitions := b.plan[other]

				if len(otherPartitions) > len(mostPartitions) &&
					len(otherPartitions) > len(partitions)+1 {

					mostMember = other
					mostPartitions = otherPartitions
					toSteal = wantPart
				}

				if len(mostPartitions) > len(partitions)+1 {
					continue
				}

				/*
					if len(otherPartitions) == len(partitions) {
						equals = append(equals, memberPartition{other, wantPart})
					} else if len(otherPartitions) == len(partitions)+1 {
						plus1s = append(plus1s, memberPartition{other, wantPart})
					}
				*/
			}

			if mostMember != "" {
				b.reassignPartition(
					it.Node(),
					mostMember,
					mostPartitions,
					member,
					partitions,
					toSteal,
				)
				it.Reset(rbtree.Under(b.planByNumPartitions.Min()))
				//deps.reset()
				continue iter
			}

			/*
				if len(plus1s) > 0 {
					if deps.hasDependent(member) {
						deps.bubbleDown(b, plus1s[0], member)
						it.Reset(rbtree.Under(b.planByNumPartitions.Min()))
						continue
					} else {
						for _, plus1 := range plus1s {
							deps.addDependent(plus1, member)
						}
					}

				} else if len(equals) > 0 {
					if deps.hasDependent(member) {
						for _, equal := range equals {
							deps.addDependent(equal, member)
						}
					}
				}

				if len(deps.order) > 0 {
					found := b.planByNumPartitions.Find(memberWithPartitions{
						deps.order[0],
						b.plan[deps.order[0]],
					})
					it.Reset(rbtree.Under(found))
					fmt.Println("resetting before", deps.order[0], "(right:", it.PeekRight(), ", found:", found, ")")
					deps.order = deps.order[1:]
				}
			*/
		}
	}
}

// If equal,
// If has downstream (save yes at top of loop)
// Register down[equal] = append(down[equal], self) (addDependent(self, equal)

// If +1,
// If has downstream, hasDependent(self)
// steal to self, bubble down from self

// If +1,
// If not has downstream,
// addDependent(self, other)

// reassignPartition reassigns a partition from srcMember to dstMember, potentially
// undoing a prior move if this detects a partition when there-and-back.
// 2*O(log members)
func (b *balancer) reassignPartition(
	dstStartNode *rbtree.Node,
	srcMember string,
	srcPartitions memberPartitions,
	dstMember string,
	dstPartitions memberPartitions,
	steal topicPartition,
) {
	fmt.Printf("reassigning %s from %s to %s\n", steal.topic, srcMember, dstMember)

	dstPartitions[steal] = struct{}{}
	b.moveMemberLevel(dstStartNode, dstMember, dstPartitions)

	srcStartNode := b.planByNumPartitions.FindWith(func(n *rbtree.Node) int {
		return len(srcPartitions) - n.Item.(partitionLevel).level
	})
	delete(srcPartitions, steal)
	b.moveMemberLevel(srcStartNode, srcMember, srcPartitions)

	b.partitionConsumers[steal] = dstMember
}

/*

type dependents struct {
	// down: FROM key, we WANT the values
	down map[string][]memberPartition
	// up: FROM key, the values (values) WANT something from us
	up map[string][]string

	order []string
}

func (d *dependents) reset() {
	fmt.Print("clearing dependents\n\n")
	if d.down == nil {
		d.down = make(map[string][]memberPartition)
		d.up = make(map[string][]string)
		return
	}
	for down := range d.down {
		delete(d.down, down)
	}
	for up := range d.up {
		delete(d.up, up)
	}
	d.order = d.order[:0]
}

type memberPartition struct {
	member string
	part   topicPartition
}

func (d *dependents) addDependent(src memberPartition, dst string) {
	if _, srcIsAlreadyDown := d.down[src.member]; srcIsAlreadyDown {
		fmt.Printf("skipping adding dependent %s wanting %s; src is already down\n", dst, src.member)
		return
	}

	fmt.Printf("dependent: %s wants %s from %s\n", dst, src.member, src.part.topic)
	_, isNewSrc := d.up[src.member]
	d.down[dst] = append(d.down[dst], src)
	d.up[src.member] = append(d.up[src.member], dst)

	if isNewSrc {
		fmt.Println("added", src.member, "to order")
		// TODO need to order BY LEVEL
		// So really,
		// Order planByNumPartition
		// Group partition levels,
		// then we can change order here.
		d.order = append(d.order, src.member)
	}
}

func (d *dependents) hasDependent(src string) bool {
	fmt.Printf("%s has dependent? %v\n", src, len(d.up[src]) > 0)
	return len(d.up[src]) > 0
}

func (d *dependents) bubbleDown(b *balancer, src memberPartition, dst string) {
	defer d.reset()

start:
	fmt.Printf("bubbling %s from %s to %s\n", src.part.topic, src.member, dst)

	b.reassignPartition(src.part, src.member, dst)
	// dst just stole,
	// so we try to bubble down from dst,
	// who is dependent on dst?

	// First, to prevent circles, delete that anything is dependent on the
	// src we just stole from.
	delete(d.up, src.member)

	furtherDependents, exists := d.up[dst]
	if !exists {
		return
	}

	dst = furtherDependents[0]
	src = d.down[dst][0]
	goto start
}
*/
