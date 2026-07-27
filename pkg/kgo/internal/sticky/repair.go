package sticky

// This file rearranges which member holds which partition, without changing
// how many partitions anybody holds, so that as many as possible stay with the
// member that already had them.
//
// balance gets the counts right: it stops only once no member can take from
// one two levels above it, which is exactly the condition for the load to be
// optimal. But it picks which partition to move by whichever the search
// reached first, and among the many assignments sharing those optimal counts,
// some leave far more partitions where they were than others.
//
// Seen as a flow, the question is small. All partitions of a topic are
// interchangeable except for who holds them, so the whole thing collapses onto
// a table of topics by members, where a cell is a count. Moving one unit from
// one cell to another and one back is a trade that leaves both members' totals
// untouched, so any rearrangement preserving the counts is a set of cycles
// through that table. The plan we already have is a feasible flow, so there is
// nothing to construct -- only cycles that would keep more partitions in place
// to find and apply, and on a plan that is already best there are none.

// stickyCell is one topic-member pair the member may hold: how many partitions
// of the topic it holds now, and how many of those it came in with.
type stickyCell struct {
	topic  uint32
	member uint16
	x      int32
	held   int32
}

// arrivals is what a cell contributes to the number of partitions that had to
// move: everything it holds beyond what it came in with.
func (c *stickyCell) arrivals() int32 {
	if c.x > c.held {
		return c.x - c.held
	}
	return 0
}

// costToAdd is what holding one more of this topic would cost in partitions
// moved: nothing while the member is still under what it came in with, since
// that unit can be one it already had.
func (c *stickyCell) costToAdd() int32 {
	if c.x < c.held {
		return 0
	}
	return 1
}

// costToDrop is what giving up one of this topic would cost. Negative when the
// member holds more than it came in with, because that unit is one that had to
// move and now does not.
func (c *stickyCell) costToDrop() int32 {
	if c.x > c.held {
		return -1
	}
	return 0
}

// maximizeStickiness trades partitions between members until no trade would
// leave more of them where they already were.
//
// Only the complex path needs this. When every member subscribes to every
// topic, any member may hold any partition, so balance moving only from
// members above their quota already moves the fewest partitions possible.
func (b *balancer) maximizeStickiness() {
	if !b.isComplex {
		return
	}

	cells, cellAt := b.stickyCells()
	if len(cells) == 0 {
		return
	}

	// Cancelling one cycle can expose another, so keep going until none is
	// left. Each cancellation strictly reduces the number of partitions
	// that had to move, and that count cannot go below zero, so this ends.
	for {
		cycle := findStickyCycle(cells, len(b.topicInfos), len(b.members))
		if cycle == nil {
			break
		}
		for _, step := range cycle {
			cells[step.cell].x += step.delta
		}
	}

	b.realizeStickiness(cells, cellAt)
}

// stickyCells builds the table: one cell per topic a member may hold, holding
// what it has now and what it arrived with. Members that came in with nothing
// still get cells, since they can be traded into.
func (b *balancer) stickyCells() ([]stickyCell, map[uint64]int32) {
	cellAt := make(map[uint64]int32, len(b.members))
	var cells []stickyCell

	for memberNum := range b.plan {
		for _, topicNum := range b.stealGraph.out[memberNum] {
			key := uint64(topicNum)<<32 | uint64(memberNum)
			if _, seen := cellAt[key]; seen {
				continue
			}
			cellAt[key] = int32(len(cells))
			cells = append(cells, stickyCell{topic: topicNum, member: uint16(memberNum)})
		}
	}

	// What each member holds now.
	for memberNum := range b.plan {
		for _, partNum := range b.plan[memberNum] {
			topicNum := b.partOwners[partNum]
			if at, ok := cellAt[uint64(topicNum)<<32|uint64(memberNum)]; ok {
				cells[at].x++
			}
		}
	}

	// And what it came in with, which is every partition it arrived
	// holding rather than only the ones it still has. A member that lost
	// four of a topic it came in with five of can take three of them back
	// if its count allows; counting only what it still holds would say it
	// had nothing to take back and no trade would ever look worthwhile.
	for partNum, cxn := range b.stealGraph.cxns {
		if cxn.originalNum == unassignedPart {
			continue
		}
		topicNum := b.partOwners[partNum]
		if at, ok := cellAt[uint64(topicNum)<<32|uint64(cxn.originalNum)]; ok {
			cells[at].held++
		}
	}
	return cells, cellAt
}

// stickyStep is one leg of a trade: a cell gaining or losing a partition.
type stickyStep struct {
	cell  int32
	delta int32
}

// findStickyCycle looks for a rotation through the table that leaves more
// partitions where they were. Nodes are topics and members; an arc from a
// topic to a member means that member takes one more of it, and back again
// means it gives one up. A cycle alternates between the two, so every member
// on it hands over exactly as much as it receives.
//
// Bellman-Ford over every node at once, which finds a negative cycle anywhere
// in the table rather than only one reachable from some chosen start.
func findStickyCycle(cells []stickyCell, ntopics, nmembers int) []stickyStep {
	const (
		unset = -1
		inf   = 1 << 30
	)
	nodes := ntopics + nmembers
	dist := make([]int32, nodes)
	viaCell := make([]int32, nodes)
	viaFrom := make([]int32, nodes)
	for i := range viaCell {
		viaCell[i] = unset
		viaFrom[i] = unset
	}

	memberNode := func(m uint16) int32 { return int32(ntopics) + int32(m) }

	// Every node starts reachable at zero, which is the standard way to
	// hunt a negative cycle without picking a source.
	var last int32 = unset
	for pass := 0; pass <= nodes; pass++ {
		last = unset
		for i := range cells {
			c := &cells[i]
			t, m := int32(c.topic), memberNode(c.member)

			// The member takes one more of this topic.
			if d := dist[t] + c.costToAdd(); d < dist[m] {
				dist[m], viaCell[m], viaFrom[m] = d, int32(i), t
				last = m
			}
			// The member gives one up, which it can only do if it has one.
			if c.x > 0 {
				if d := dist[m] + c.costToDrop(); d < dist[t] {
					dist[t], viaCell[t], viaFrom[t] = d, int32(i), m
					last = t
				}
			}
		}
		if last == unset {
			return nil // settled, so no cycle
		}
	}

	// Still relaxing after a full pass per node means a negative cycle is
	// reachable from here; walking back that many times lands inside it.
	at := last
	for range nodes {
		at = viaFrom[at]
	}

	var cycle []stickyStep
	for node := at; ; {
		cell := viaCell[node]
		// Reaching a member means it took one more; reaching a topic
		// means the member on the other side gave one up.
		if node >= int32(ntopics) {
			cycle = append(cycle, stickyStep{cell, +1})
		} else {
			cycle = append(cycle, stickyStep{cell, -1})
		}
		node = viaFrom[node]
		if node == at {
			break
		}
	}
	return cycle
}

// realizeStickiness rewrites the plan to match the table, giving each member
// as many of the partitions it arrived with as its new count allows and
// filling the rest from whatever nobody claimed.
func (b *balancer) realizeStickiness(cells []stickyCell, cellAt map[uint64]int32) {
	// Group each topic's partitions by who arrived holding them, not by
	// who holds them now: the table may well say a member should end up
	// with one it lost, and it can only take it back if we look past the
	// current owner.
	type pool struct {
		mine map[uint16][]int32
		rest []int32
	}
	pools := make(map[uint32]*pool, len(b.topicInfos))
	for memberNum := range b.plan {
		for _, partNum := range b.plan[memberNum] {
			topicNum := b.partOwners[partNum]
			p := pools[topicNum]
			if p == nil {
				p = &pool{mine: make(map[uint16][]int32)}
				pools[topicNum] = p
			}
			if orig := b.stealGraph.cxns[partNum].originalNum; orig != unassignedPart {
				p.mine[orig] = append(p.mine[orig], partNum)
			} else {
				p.rest = append(p.rest, partNum)
			}
		}
	}

	for i := range b.plan {
		b.plan[i] = b.plan[i][:0]
	}

	// Everybody first takes back as many of its own as its count allows.
	owed := make([]int32, len(cells))
	for i := range cells {
		c := &cells[i]
		owed[i] = c.x
		p := pools[c.topic]
		if p == nil {
			continue
		}
		mine := p.mine[c.member]
		take := min(int(owed[i]), len(mine))
		for _, partNum := range mine[:take] {
			b.plan[c.member].add(partNum)
		}
		p.mine[c.member] = mine[take:]
		owed[i] -= int32(take)
	}

	// Then whatever is still owed comes from what nobody reclaimed.
	for i := range cells {
		c := &cells[i]
		p := pools[c.topic]
		if owed[i] == 0 || p == nil {
			continue
		}
		for owed[i] > 0 && len(p.rest) > 0 {
			b.plan[c.member].add(p.rest[len(p.rest)-1])
			p.rest = p.rest[:len(p.rest)-1]
			owed[i]--
		}
		for other, left := range p.mine {
			for owed[i] > 0 && len(left) > 0 {
				b.plan[c.member].add(left[len(left)-1])
				left = left[:len(left)-1]
				owed[i]--
			}
			p.mine[other] = left
			if owed[i] == 0 {
				break
			}
		}
	}

	// The graph's record of who holds what has to follow, since it is what
	// tells a later balance where a partition came from.
	for memberNum := range b.plan {
		for _, partNum := range b.plan[memberNum] {
			b.stealGraph.cxns[partNum].memberNum = uint16(memberNum)
		}
	}
}
