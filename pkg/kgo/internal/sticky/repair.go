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
	loads := make([]int32, len(b.members))
	for i := range cells {
		loads[cells[i].member] += cells[i].x
	}

	// Cancelling one cycle can expose another, so keep going until none is
	// left. Each cancellation strictly reduces the number of partitions
	// that had to move, and that count cannot go below zero, so this ends.
	for {
		cycle := findStickyCycle(cells, loads, len(b.topicInfos), len(b.members))
		if cycle == nil {
			break
		}
		for _, step := range cycle {
			cells[step.cell].x += step.delta
			loads[cells[step.cell].member] += step.delta
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

// findStickyCycle looks for a rotation through the table that either evens the
// load out or, failing that, leaves more partitions where they already were.
//
// Nodes are topics, members, and one node standing for the group as a whole.
// A topic to a member means that member takes one more of it; back again means
// it gives one up. A member to the group node means it sheds a partition
// outright and another picks one up, which is the only way a cycle can change
// what anybody holds in total.
//
// Load is priced so that the k'th partition a member holds costs W*(2k-1),
// making the total W*k^2. Shifting one partition from a member holding a to
// one holding b then costs W*(2b-2a+2): negative when b is two or more below
// a, which is exactly when balancing would have moved it anyway; zero when b
// is one below, so two members a level apart may trade freely; and positive
// otherwise. With W larger than any churn a cycle could save, the load vector
// can never be worsened to keep a partition in place, and churn decides only
// among rearrangements the load is indifferent to.
func findStickyCycle(cells []stickyCell, loads []int32, ntopics, nmembers int) []stickyStep {
	const unset = -1
	nodes := ntopics + nmembers + 1
	group := int32(ntopics + nmembers)

	// Any cycle alternates, so it holds at most one step per node, and each
	// step moves churn by at most one.
	weight := int64(2*nodes + 2)

	dist := make([]int64, nodes)
	viaCell := make([]int32, nodes)
	viaFrom := make([]int32, nodes)
	for i := range viaCell {
		viaCell[i] = unset
		viaFrom[i] = unset
	}

	memberNode := func(m uint16) int32 { return int32(ntopics) + int32(m) }
	// Taking the k+1'th partition, and giving up the k'th.
	costToLoad := func(m uint16) int64 { return weight * int64(2*loads[m]+1) }
	costToShed := func(m uint16) int64 { return -weight * int64(2*loads[m]-1) }

	var last int32 = unset
	for pass := 0; pass <= nodes; pass++ {
		last = unset
		for i := range cells {
			c := &cells[i]
			t, m := int32(c.topic), memberNode(c.member)

			if d := dist[t] + int64(c.costToAdd()); d < dist[m] {
				dist[m], viaCell[m], viaFrom[m] = d, int32(i), t
				last = m
			}
			if c.x > 0 {
				if d := dist[m] + int64(c.costToDrop()); d < dist[t] {
					dist[t], viaCell[t], viaFrom[t] = d, int32(i), m
					last = t
				}
			}
		}
		// Load leaving one member and arriving at another, which is what
		// lets two members a level apart trade their counts.
		for m := range nmembers {
			node := memberNode(uint16(m))
			// Toward the group node is this member taking on one more
			// partition overall; away from it is giving one up.
			if d := dist[node] + costToLoad(uint16(m)); d < dist[group] {
				dist[group], viaCell[group], viaFrom[group] = d, unset, node
				last = group
			}
			if loads[m] > 0 {
				if d := dist[group] + costToShed(uint16(m)); d < dist[node] {
					dist[node], viaCell[node], viaFrom[node] = d, unset, group
					last = node
				}
			}
		}
		if last == unset {
			return nil
		}
	}

	at := last
	for range nodes {
		at = viaFrom[at]
	}

	var cycle []stickyStep
	for node := at; ; {
		if cell := viaCell[node]; cell != unset {
			// Reaching a member means it took one more of the topic;
			// reaching a topic means the member gave one up.
			if node >= int32(ntopics) && node != group {
				cycle = append(cycle, stickyStep{cell, +1})
			} else {
				cycle = append(cycle, stickyStep{cell, -1})
			}
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
