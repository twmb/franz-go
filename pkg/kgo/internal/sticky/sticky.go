// Package sticky provides sticky partitioning strategy for Kafka, with a
// complete overhaul to be faster, more understandable, and optimal.
//
// For some points on how Java's strategy is flawed, see
// https://github.com/Shopify/sarama/pull/1416/files/b29086bdaae0da7ce71eae3f854d50685fd6b631#r315005878
package sticky

import (
	"math"
	"sort"

	"github.com/twmb/go-rbtree"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// Sticky partitioning has two versions, the latter from KIP-341 preventing a
// bug. The second version introduced generations with the default generation
// from the first generation's consumers defaulting to -1.

// We can support up to 65533 members; two slots are reserved.
// We can support up to 4,294,967,295 partitions.
// I expect a server to fall over before reaching either of these numbers.

// GroupMember is a Kafka group member.
type GroupMember struct {
	ID       string
	Topics   []string
	UserData []byte
}

// Plan is the plan this package came up with (member => topic => partitions).
type Plan map[string]map[string][]int32

type balancer struct {
	// members are the members in play for this balance.
	// This is built in newBalancer mapping member IDs to the GroupMember.
	members []GroupMember

	// memberNums and memberNames map member names to numbers (and back).
	// We use numbers throughout balancing for a significant speed boost.
	memberNames   []string
	memberNums    map[string]uint16
	nextMemberNum uint16

	// topics are the topic names and partitions that the client knows of
	// and passed to be used for assigning unassigned partitions.
	topics map[string]int32

	// Similar to memberNums and memberNames above, partNums and partNums
	// map topic partitions to numbers and back. This provides significant
	// speed boosts.
	partNames   []topicPartition
	partNums    map[string]partNumStart
	nextPartNum uint32

	// Stales tracks partNums that are doubly subscribed in this join
	// where one of the subscribers is on an old generation.
	//
	// The newer generation goes into plan directly, the older gets
	// stuffed here.
	stales map[uint32]uint16 // partNum => stale memberNum

	// plan is what we are building and balancing.
	plan membersPartitions

	// planByNumPartitions orders plan members into partition count levels.
	//
	// The nodes in the tree reference values in plan, meaning updates in
	// this field are visible in plan.
	planByNumPartitions rbtree.Tree

	// if the subscriptions are complex (all members do _not_ consume the
	// same partitions), then we build a graph and use that for assigning.
	isComplex bool

	// stealGraph is a graphical representation of members and partitions
	// they want to steal.
	stealGraph graph
}

type topicPartition struct {
	topic     string
	partition int32
}

func newBalancer(members []GroupMember, topics map[string]int32) *balancer {
	var nparts int
	for _, partitions := range topics {
		nparts += int(partitions)
	}

	b := &balancer{
		members:     make([]GroupMember, len(members)),
		memberNums:  make(map[string]uint16, len(members)),
		memberNames: make([]string, len(members)),
		plan:        make(membersPartitions, len(members)),
		topics:      topics,
		partNames:   make([]topicPartition, nparts),
		partNums:    make(map[string]partNumStart, len(topics)),
		stales:      make(map[uint32]uint16),
	}

	evenDivvy := nparts/len(members) + 1
	for _, member := range members {
		num := b.memberNum(member.ID)
		b.members[num] = member
		b.plan[num] = make(memberPartitions, 0, evenDivvy)
	}
	return b
}

func (b *balancer) into() Plan {
	plan := make(Plan, len(b.plan))
	for memberNum, partNums := range b.plan {
		name := b.memberName(uint16(memberNum))
		topics, exists := plan[name]
		if !exists {
			topics = make(map[string][]int32, 20)
			plan[name] = topics
		}
		for _, partNum := range partNums {
			partition := b.partName(partNum)
			topicPartitions := topics[partition.topic]
			if len(topicPartitions) == 0 {
				topicPartitions = make([]int32, 0, 40)
			}
			topicPartitions = append(topicPartitions, partition.partition)
			topics[partition.topic] = topicPartitions
		}
	}
	return plan
}

type partNumStart struct {
	num   uint32
	total int32
}

func (b *balancer) partNum(topic string, partition int32) (uint32, int) {
	start, exists := b.partNums[topic]
	if !exists {
		start = partNumStart{
			num:   b.nextPartNum,
			total: b.topics[topic],
		}
		b.partNums[topic] = start
		b.nextPartNum += uint32(start.total)
		for partition, num := int32(0), start.num; partition < start.total; partition, num = partition+1, num+1 {
			b.partNames[num] = topicPartition{topic, partition}
		}
	}
	num := start.num + uint32(partition)
	return num, int(start.total)
}

func (b *balancer) partName(num uint32) topicPartition {
	return b.partNames[num]
}

func (b *balancer) memberNum(name string) uint16 {
	num, exists := b.memberNums[name]
	if !exists {
		num = b.nextMemberNum
		b.nextMemberNum++
		b.memberNums[name] = num
		b.memberNames[num] = name
	}
	return num
}

func (b *balancer) memberName(num uint16) string {
	return b.memberNames[num]
}

func (m *memberPartitions) remove(needle uint32) {
	s := *m
	var d int
	for i, check := range s {
		if check == needle {
			d = i
			break
		}
	}
	s[d] = s[len(s)-1]
	*m = s[:len(s)-1]
}

func (m *memberPartitions) takeEnd() uint32 {
	s := *m
	r := s[len(s)-1]
	*m = s[:len(s)-1]
	return r
}

func (m *memberPartitions) add(partNum uint32) {
	*m = append(*m, partNum)
}

func (m *memberPartitions) len() int {
	return len(*m)
}

// memberPartitions contains partitions for a member.
type memberPartitions []uint32

// membersPartitions maps members to their partitions.
type membersPartitions []memberPartitions

type partitionLevel struct {
	level   int
	members []uint16
}

// partitionLevel's members field used to be a map, but removing it gains a
// slight perf boost at the cost of removing members being O(M).
// Even with the worse complexity, scanning a short list can be faster
// than managing a map, and we expect groups to not be _too_ large.
func (l *partitionLevel) removeMember(memberNum uint16) {
	for i, v := range l.members {
		if v == memberNum {
			l.members[i] = l.members[len(l.members)-1]
			l.members = l.members[:len(l.members)-1]
			return
		}
	}
}

func (b *balancer) findLevel(level int) *partitionLevel {
	return b.planByNumPartitions.FindWithOrInsertWith(
		func(n *rbtree.Node) int { return level - n.Item.(*partitionLevel).level },
		func() rbtree.Item { return newPartitionLevel(level) },
	).Item.(*partitionLevel)
}

func (b *balancer) fixMemberLevel(
	src *rbtree.Node,
	memberNum uint16,
	partNums memberPartitions,
) {
	b.removeLevelingMember(src, memberNum)
	newLevel := len(partNums)
	partLevel := b.findLevel(newLevel)
	partLevel.members = append(partLevel.members, memberNum)
}

func (b *balancer) removeLevelingMember(
	src *rbtree.Node,
	memberNum uint16,
) {
	level := src.Item.(*partitionLevel)
	level.removeMember(memberNum)
	if len(level.members) == 0 {
		b.planByNumPartitions.Delete(src)
	}
}

func (l *partitionLevel) Less(r rbtree.Item) bool {
	return l.level < r.(*partitionLevel).level
}

func newPartitionLevel(level int) *partitionLevel {
	return &partitionLevel{level: level}
}

func (b *balancer) initPlanByNumPartitions() {
	for memberNum, partNums := range b.plan {
		partLevel := b.findLevel(len(partNums))
		partLevel.members = append(partLevel.members, uint16(memberNum))
	}
}

// Balance performs sticky partitioning for the given group members and topics,
// returning the determined plan.
func Balance(members []GroupMember, topics map[string]int32) Plan {
	if len(members) == 0 {
		return make(Plan)
	}
	b := newBalancer(members, topics)
	if cap(b.partNames) == 0 {
		return b.into()
	}
	b.parseMemberMetadata()
	b.assignUnassignedAndInitGraph()
	b.initPlanByNumPartitions()
	b.balance()
	return b.into()
}

// parseMemberMetadata parses all member userdata to initialize the prior plan.
func (b *balancer) parseMemberMetadata() {
	// all partitions => members that are consuming those partitions
	// Each partition should only have one consumer, but a flaky member
	// could rejoin with an old generation (stale user data) and say it
	// is consuming something a different member is. See KIP-341.
	partitionConsumersByGeneration := make([][]memberGeneration, cap(b.partNames))
	partitionConsumersBuf := make([]memberGeneration, cap(b.partNames))

	for _, member := range b.members {
		memberPlan, generation := deserializeUserData(member.UserData)
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
			partNum, _ := b.partNum(topicPartition.topic, topicPartition.partition)
			partitionConsumers := partitionConsumersByGeneration[partNum]
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
			if len(partitionConsumers) == 0 {
				partitionConsumers = partitionConsumersBuf[:0:1]
				partitionConsumersBuf = partitionConsumersBuf[1:]
			}
			partitionConsumersByGeneration[partNum] = append(partitionConsumers, memberGeneration)
		}
	}

	var mgs memberGenerations
	for partNum, partitionConsumers := range partitionConsumersByGeneration {
		mgs = memberGenerations(partitionConsumers)
		sort.Sort(&mgs)
		if len(partitionConsumers) == 0 {
			continue
		}

		memberNum := b.memberNum(partitionConsumers[0].member)
		partNums := &b.plan[memberNum]
		partNums.add(uint32(partNum))

		if len(partitionConsumers) > 1 {
			b.stales[uint32(partNum)] = b.memberNum(partitionConsumers[1].member)
		}
	}
}

type memberGeneration struct {
	member     string
	generation int32
}

// for alloc avoidance since it is easy enough.
type memberGenerations []memberGeneration

func (m *memberGenerations) Len() int           { return len(*m) }
func (m *memberGenerations) Less(i, j int) bool { s := *m; return s[i].generation > s[j].generation }
func (m *memberGenerations) Swap(i, j int)      { s := *m; s[i], s[j] = s[j], s[i] }

// deserializeUserData returns the topic partitions a member was consuming and
// the join generation it was consuming from.
//
// If anything fails or we do not understand the userdata parsing generation,
// we return empty defaults. The member will just be assumed to have no
// history.
func deserializeUserData(userdata []byte) (memberPlan []topicPartition, generation int32) {
	s := kmsg.StickyMemberMetadata{
		Generation: -1,
	}
	if err := s.ReadFrom(userdata); err != nil {
		return nil, 0
	}
	generation = s.Generation
	for _, topicAssignment := range s.CurrentAssignment {
		for _, partition := range topicAssignment.Partitions {
			memberPlan = append(memberPlan, topicPartition{
				topicAssignment.Topic,
				partition,
			})
		}
	}

	return memberPlan, generation
}

// assignUnassignedAndInitGraph is a long function that assigns unassigned
// partitions to the least loaded members and initializes our steal graph.
//
// Doing so requires a bunch of metadata, and in the process we want to remove
// partitions from the plan that no longer exist in the client.
func (b *balancer) assignUnassignedAndInitGraph() {
	// First, over all members in this assignment, map each partition to
	// the members that can consume it. We will use this for assigning.
	//
	// To do this mapping efficiently, we first map each topic to the
	// memberNums that can consume those topics, and then use the results
	// below in the partition mapping. Doing this two step process allows
	// for a 10x speed boost rather than ranging over all partitions many
	// times.
	membersBufs := make([]uint16, len(b.topics)*len(b.members))
	topicPotentials := make(map[string][]uint16, len(b.topics))
	for memberNum, member := range b.members {
		for _, topic := range member.Topics {
			if _, exists := b.topics[topic]; !exists {
				continue
			}
			memberNums := topicPotentials[topic]
			if cap(memberNums) == 0 {
				memberNums = membersBufs[:0:len(b.memberNums)]
				membersBufs = membersBufs[len(b.memberNums):]
			}
			topicPotentials[topic] = append(memberNums, uint16(memberNum))
		}
	}

	for topic, topicMembers := range topicPotentials {
		last := b.topics[topic]
		for partition := int32(0); partition < last; partition++ {
			b.partNum(topic, partition)
		}

		// If the number of members interested in this topic is not the
		// same as the number of members in this group, then **other**
		// members are interested in other topics and not this one, and
		// we must go to complex balancing.
		//
		// We could accidentally fall into isComplex if any member is
		// not interested in anything, but realistically we do not
		// expect members to join with no interests.
		if len(topicMembers) != len(b.members) {
			b.isComplex = true
		}
	}

	// Next, over the prior plan, un-map deleted topics or topics that
	// members no longer want. This is where we determine what is now
	// unassigned.
	partitionConsumers := make([]uint16, cap(b.partNames)) // partNum => consuming member
	for i := range partitionConsumers {
		partitionConsumers[i] = unassignedPart
	}
	for memberNum := range b.plan {
		partNums := &b.plan[memberNum]
		for _, partNum := range *partNums {
			if len(topicPotentials[b.partName(partNum).topic]) == 0 { // all prior subscriptions stopped wanting this partition
				partitionConsumers[partNum] = deletedPart
				partNums.remove(partNum)
				continue
			}
			memberTopics := b.members[memberNum].Topics
			var memberStillWantsTopic bool
			partition := b.partName(partNum)
			for _, memberTopic := range memberTopics {
				if memberTopic == partition.topic {
					memberStillWantsTopic = true
					break
				}
			}
			if !memberStillWantsTopic {
				partNums.remove(partNum)
				continue
			}
			partitionConsumers[partNum] = uint16(memberNum)
		}
	}

	b.tryRestickyStales(topicPotentials, partitionConsumers)
	for _, potentials := range topicPotentials {
		(&membersByPartitions{potentials, b.plan}).init()
	}

	// We now assign everything we know is not currently assigned.
	for partNum, owner := range partitionConsumers {
		if owner != unassignedPart {
			continue
		}
		potentials := topicPotentials[b.partName(uint32(partNum)).topic]
		if len(potentials) == 0 {
			continue
		}
		assigned := potentials[0]
		b.plan[assigned].add(uint32(partNum))
		(&membersByPartitions{potentials, b.plan}).fix0()
		partitionConsumers[partNum] = assigned
	}

	// Lastly, with everything assigned, we build our steal graph for
	// balancing if needed.
	if b.isComplex {
		b.stealGraph = b.newGraph(partitionConsumers, topicPotentials)
	}
}

const (
	// deletedPart and unassignedPart are fake member numbers that we use
	// to track if a partition is deleted or unassigned.
	//
	// deletedPart is technically unneeded; if no member wants a partition,
	// no member will be seen as a potential for taking it, so tracking
	// that it was deleted is unnecessary. We do though just to be
	// explicit.
	//
	// unassignedPart is the default of partitions until we process what
	// members say they were assigned prior.
	deletedPart    = math.MaxUint16
	unassignedPart = math.MaxUint16 - 1
)

// tryRestickyStales is a pre-assigning step where, for all stale members,
// we give partitions back to them if the partition is currently on an
// over loaded member or unassigned.
//
// This effectively re-stickies members before we balance further.
func (b *balancer) tryRestickyStales(
	topicPotentials map[string][]uint16,
	partitionConsumers []uint16,
) {
	for staleNum, lastOwnerNum := range b.stales {
		potentials := topicPotentials[b.partName(staleNum).topic] // there must be a potential consumer if we are here
		var canTake bool
		for _, potentialNum := range potentials {
			if potentialNum == lastOwnerNum {
				canTake = true
			}
		}
		if !canTake {
			return
		}

		// The part cannot be unassigned here; a stale member
		// would just have it. The part also cannot be deleted;
		// if it is, there are no potential consumers and the
		// logic above continues before getting here. The part
		// must be on a different owner (cannot be lastOwner),
		// otherwise it would not be a lastOwner in the stales
		// map; it would just be the current owner.
		currentOwner := partitionConsumers[staleNum]
		lastOwnerPartitions := &b.plan[lastOwnerNum]
		currentOwnerPartitions := &b.plan[currentOwner]
		if lastOwnerPartitions.len()+1 < currentOwnerPartitions.len() {
			currentOwnerPartitions.remove(staleNum)
			lastOwnerPartitions.add(staleNum)
		}
	}
}

// While assigning, we keep members per topic heap sorted by the number of
// partitions they are currently consuming. This allows us to have quick
// assignment vs. always scanning to see the min loaded member.
//
// Our process is to init the heap and then always fix the 0th index after
// making it larger, so we only ever need to sift down.
type membersByPartitions struct {
	members    []uint16
	partitions membersPartitions
}

func (m *membersByPartitions) len() int { return len(m.members) }
func (m *membersByPartitions) swap(i, j int) {
	m.members[i], m.members[j] = m.members[j], m.members[i]
}
func (m *membersByPartitions) less(i, j int) bool {
	return m.partitions[m.members[i]].len() < m.partitions[m.members[j]].len()
}
func (m *membersByPartitions) init() {
	n := m.len()
	for i := n/2 - 1; i >= 0; i-- {
		m.down(i, n)
	}
}
func (m *membersByPartitions) fix0() {
	m.down(0, m.len())
}
func (m *membersByPartitions) down(i0, n int) bool {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 >= n || j1 < 0 { // j1 < 0 after int overflow
			break
		}
		j := j1 // left child
		if j2 := j1 + 1; j2 < n && m.less(j2, j1) {
			j = j2 // = 2*i + 2  // right child
		}
		if !m.less(j, i) {
			break
		}
		m.swap(i, j)
		i = j
	}
	return i > i0
}

// balance loops trying to move partitions until the plan is as balanced
// as it can be.
func (b *balancer) balance() {
	if b.isComplex {
		b.balanceComplex()
		return
	}

	// If all partitions are consumed equally, we have a very easy
	// algorithm to balance: while the min and max levels are separated
	// by over two, take from the top and give to the bottom.
	min := b.planByNumPartitions.Min().Item.(*partitionLevel)
	max := b.planByNumPartitions.Max().Item.(*partitionLevel)
	for {
		if max.level <= min.level+1 {
			return
		}

		minRem := min.members
		maxRem := max.members
		for len(minRem) > 0 && len(maxRem) > 0 {
			dst := minRem[0]
			src := maxRem[0]

			minRem = minRem[1:]
			maxRem = maxRem[1:]

			srcPartitions := &b.plan[src]
			dstPartitions := &b.plan[dst]

			dstPartitions.add(srcPartitions.takeEnd())
		}

		nextUp := b.findLevel(min.level + 1)
		nextDown := b.findLevel(max.level - 1)

		upEnd := len(min.members) - len(minRem)
		downEnd := len(max.members) - len(maxRem)

		nextUp.members = append(nextUp.members, min.members[:upEnd]...)
		nextDown.members = append(nextDown.members, max.members[:downEnd]...)

		min.members = min.members[upEnd:]
		max.members = max.members[downEnd:]

		if len(min.members) == 0 {
			b.planByNumPartitions.Delete(b.planByNumPartitions.Min())
			min = b.planByNumPartitions.Min().Item.(*partitionLevel)
		}
		if len(max.members) == 0 {
			b.planByNumPartitions.Delete(b.planByNumPartitions.Max())
			max = b.planByNumPartitions.Max().Item.(*partitionLevel)
		}
	}
}

func (b *balancer) balanceComplex() {
	for min := b.planByNumPartitions.Min(); b.planByNumPartitions.Len() > 1; min = b.planByNumPartitions.Min() {
		level := min.Item.(*partitionLevel)
		// If this max level is within one of this level, then nothing
		// can steal down so we return early.
		if b.planByNumPartitions.Max().Item.(*partitionLevel).level <= level.level+1 {
			return
		}
		// We continually loop over this level until every member is
		// static (deleted) or bumped up a level.
		for len(level.members) > 0 {
			memberNum := level.members[0]
			if stealPath, found := b.stealGraph.findSteal(memberNum); found {
				for _, segment := range stealPath {
					b.reassignPartition(segment.src, segment.dst, segment.part)
				}
				continue
			}

			// If we could not find a steal path, this
			// member is not static (will never grow).
			level.removeMember(memberNum)
			if len(level.members) == 0 {
				b.planByNumPartitions.Delete(b.planByNumPartitions.Min())
			}
		}
	}
}

func (b *balancer) reassignPartition(src, dst uint16, partNum uint32) {
	srcPartitions := &b.plan[src]
	dstPartitions := &b.plan[dst]

	oldSrcLevel := srcPartitions.len()
	oldDstLevel := dstPartitions.len()

	srcPartitions.remove(partNum)
	dstPartitions.add(partNum)

	b.fixMemberLevel(
		b.planByNumPartitions.FindWith(func(n *rbtree.Node) int {
			return oldSrcLevel - n.Item.(*partitionLevel).level
		}),
		src,
		*srcPartitions,
	)
	b.fixMemberLevel(
		b.planByNumPartitions.FindWith(func(n *rbtree.Node) int {
			return oldDstLevel - n.Item.(*partitionLevel).level
		}),
		dst,
		*dstPartitions,
	)

	b.stealGraph.changeOwnership(partNum, dst)
}
