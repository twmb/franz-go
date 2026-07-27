package sticky

import (
	"math"
	"math/bits"
	"slices"
)

// This file rearranges which member holds which partition once balancing has
// settled how many each one holds, so that the assignment is the best of the
// many that share those counts.
//
// balance gets the counts right: it stops only once no member can take from one
// two levels above it, which is exactly the condition for the load to be
// optimal. But it picks which partition to move by whichever the search reached
// first, and it never revisits one carried over from the previous assignment.
// Among the assignments sharing optimal counts, some leave far more partitions
// where they were than others, and some place far more of them in the rack they
// are led from.
//
// Seen as a flow the question is small. Partitions nothing can tell apart are
// interchangeable, so the whole thing collapses onto a table of those groups by
// members, where a cell is a count. Adding one to a cell and taking one from
// another is a rotation, and every rearrangement is a set of them. The plan we
// already have is a feasible flow, so there is nothing to construct -- only
// rotations that would lower the price to find and apply, and on a plan that is
// already the cheapest there are none.
//
// What the price is decides what the repair optimizes, and the three things it
// weighs are ranked by how far apart their weights are set: balance first, then
// rack, then stickiness. Same framework, one term each.

// stickyRow is a group of partitions that are interchangeable to every member:
// nobody prefers one over another for any reason the repair prices. Which
// partitions those are depends on what can tell them apart. Racks can, since
// one is a local fetch and the other crosses zones. Topics can only when
// members subscribe to different sets -- with uniform subscriptions every
// member can take any topic, so the rack alone decides.
type stickyRow struct {
	class int32
	rack  uint16
}

// stickyCell is one place partitions can sit: a row, and a member that may
// hold them.
type stickyCell struct {
	row     int32
	member  uint16
	x       int32 // partitions of this row the member holds
	held    int32 // how many of them it arrived holding
	offrack bool  // the member is not in this row's rack
}

// costToAdd is what one more partition here costs: a move, unless the member
// is still below what it came in with, plus a zone crossing if it is not in
// this row's rack.
func (c *stickyCell) costToAdd(rackw int64) int64 {
	var cost int64
	if c.x >= c.held {
		cost = 1
	}
	if c.offrack {
		cost += rackw
	}
	return cost
}

// costToDrop is what giving one up here saves.
func (c *stickyCell) costToDrop(rackw int64) int64 {
	var cost int64
	if c.x > c.held {
		cost = -1
	}
	if c.offrack {
		cost -= rackw
	}
	return cost
}

// room is how many more units can move through this cell in the given
// direction before the price of the next one changes -- crossing what the
// member came in with, or emptying the cell.
func (c *stickyCell) room(delta int32) int32 {
	if delta > 0 {
		if c.x < c.held {
			return c.held - c.x
		}
		return math.MaxInt32
	}
	if c.x > c.held {
		return c.x - c.held
	}
	return c.x
}

// stickyTable is the rows-by-members table of counts the repair works on.
// Cells are grouped by row, so a row's cells are one contiguous span.
type stickyTable struct {
	rows     []stickyRow
	cells    []stickyCell
	rowStart []int32 // row r owns cells[rowStart[r]:rowStart[r+1]]
	nmembers int32

	// A partition's row is looked up rather than stored: these tables are
	// keyed by topic and rack, so they stay small where one per partition
	// would be megabytes on a large group.
	classOf []int32
	rowAt   []int32
	stride  int32
	racks   []uint16

	// Set when some partition is held by a member other than the one that
	// arrived with it. Counts alone cannot see that -- two members each
	// holding the other's partition looks settled -- so it decides whether
	// the plan has to be rewritten at all.
	crossed bool

	// Exactly one of these is set: a dense index of every row by every
	// member, or a map holding only the pairs that exist.
	idx []int32
	at  map[uint64]int32
}

// rowOf is the row a partition belongs to.
func (t *stickyTable) rowOf(partNum int32, topic uint32) int32 {
	var rack int32
	if t.racks != nil {
		rack = int32(t.racks[partNum])
	}
	return t.rowAt[t.classOf[topic]*t.stride+rack]
}

func (t *stickyTable) cellAt(row int32, member uint16) int32 {
	if t.idx != nil {
		return t.idx[row*t.nmembers+int32(member)]
	}
	if at, ok := t.at[uint64(row)<<32|uint64(member)]; ok {
		return at
	}
	return -1
}

func (b *balancer) rackOf(partNum int32) uint16 {
	if b.partRacks == nil {
		return noRack
	}
	return b.partRacks[partNum]
}

// repairAssignment rearranges who holds what until nothing would improve, in
// the order balance, then rack, then stickiness.
//
// Balancing already gets the counts right, but it picks which partition to move
// by whichever the search reached first, and it never revisits a partition
// carried over from the previous assignment. Both leave room: the same optimal
// counts can be reached by arrangements that keep very different numbers of
// partitions in place, and rack placement rots as leadership moves.
//
// Everything is priced and the cheapest arrangement wins. A partition that
// moves costs 1. One sitting in the wrong rack costs rackw, set above anything
// stickiness could save, so no number of retained partitions can buy a zone
// crossing. A member's k'th partition costs loadw*(2k-1), making its load cost
// loadw*k^2 -- so shifting one partition from a member holding a to one holding
// b costs loadw*(2b-2a+2), which is negative exactly when balancing would have
// moved it anyway, zero when they sit one level apart, and positive otherwise.
// loadw is set above anything rack and stickiness together could save, so the
// spread of partitions is never worsened for either.
func (b *balancer) repairAssignment() {
	// With uniform subscriptions and no racks there is nothing to find:
	// every member can take from every topic, so balancing already lands on
	// an arrangement no rotation improves. Skipping spares the common case
	// a table it would only scan once and discard.
	if !b.isComplex && b.partRacks == nil {
		return
	}

	t := b.stickyTable()
	if len(t.cells) == 0 {
		return
	}
	loads := make([]int32, len(b.members))
	for i := range t.cells {
		loads[t.cells[i].member] += t.cells[i].x
	}

	// A rotation alternates rows and members, and runs through the group
	// node at most once, so it is no longer than this. Each leg can save at
	// most one partition's worth of stickiness, so a rack crossing priced
	// above the whole rotation can never be bought with retained partitions,
	// and a load shift priced above the whole rotation again can never be
	// bought with either.
	turns := int64(2*min(len(t.rows), len(b.members)) + 2)
	rackw := turns + 1
	loadw := rackw * (turns + 1)

	// Cancelling one improvement can expose another. Each one strictly
	// lowers a whole-number cost that cannot fall below zero, so this ends.
	f := newStickyFinder(len(t.rows), len(b.members))
	var moved bool
	for {
		cycles := f.find(t.cells, loads, rackw, loadw)
		if len(cycles) == 0 {
			break
		}
		moved = true
		for _, cycle := range cycles {
			// Every edge on the rotation holds the same price for this
			// many more units, so they all move at once rather than one
			// per search. A rotation that shifts load is the exception:
			// a member's price rises with every partition it takes on,
			// so only one can move before it has to be re-priced.
			n := int32(1)
			if !cycle.shiftsLoad {
				n = math.MaxInt32
				for _, step := range cycle.steps {
					n = min(n, t.cells[step.cell].room(step.delta))
				}
			}
			for _, step := range cycle.steps {
				t.cells[step.cell].x += step.delta * n
				loads[t.cells[step.cell].member] += step.delta * n
			}
		}
	}

	// Nothing to rewrite if no rotation fired and everybody already holds
	// what it arrived with. What is left over was unassigned, and one
	// unassigned partition of a row is worth exactly what another is.
	if !moved && !t.crossed {
		return
	}
	b.realizeAssignment(t)
}

// topicClasses groups topics whose subscribers are exactly the same set. Two
// such topics are interchangeable: every member that may hold one may hold the
// other, and the repair prices nothing else about a topic. Collapsing them is
// what keeps the table small -- a group where one member of two thousand reads
// one extra topic is otherwise a table of every topic by every member, when
// there are really only two kinds of topic in it.
//
// Returns the class of each topic and each class's members.
func (b *balancer) topicClasses() ([]int32, [][]uint64) {
	nwords := (len(b.members) + 63) / 64
	if !b.isComplex {
		all := make([]uint64, nwords)
		for m := range b.members {
			all[m/64] |= 1 << (m % 64)
		}
		return make([]int32, len(b.topicNames)), [][]uint64{all}
	}

	subs := make([]uint64, len(b.topicNames)*nwords)
	for m := range b.members {
		for _, topic := range b.members[m].Topics {
			if topicNum, ok := b.topicNums[topic]; ok {
				subs[int(topicNum)*nwords+m/64] |= 1 << (m % 64)
			}
		}
	}

	classOf := make([]int32, len(b.topicNames))
	var classSubs [][]uint64
	byHash := make(map[uint64][]int32)
	for topicNum := range classOf {
		mine := subs[topicNum*nwords : (topicNum+1)*nwords]
		h := uint64(14695981039346656037)
		for _, w := range mine {
			h = (h ^ w) * 1099511628211
		}
		class := int32(-1)
		for _, c := range byHash[h] {
			if slices.Equal(classSubs[c], mine) {
				class = c
				break
			}
		}
		if class < 0 {
			class = int32(len(classSubs))
			classSubs = append(classSubs, mine)
			byHash[h] = append(byHash[h], class)
		}
		classOf[topicNum] = class
	}
	return classOf, classSubs
}

// stickyTable builds the table: which partitions are interchangeable, and which
// members may hold them.
func (b *balancer) stickyTable() *stickyTable {
	classOf, classSubs := b.topicClasses()
	nmembers := int32(len(b.plan))
	t := &stickyTable{
		nmembers: nmembers,
		classOf:  classOf,
		stride:   int32(b.nRacks) + 1,
		racks:    b.partRacks,
	}

	t.rowAt = make([]int32, int32(len(classSubs))*t.stride)
	for i := range t.rowAt {
		t.rowAt[i] = -1
	}
	for p := range b.origOwner {
		class, rack := classOf[b.partOwners[p]], b.rackOf(int32(p))
		key := class*t.stride + int32(rack)
		if t.rowAt[key] < 0 {
			t.rowAt[key] = int32(len(t.rows))
			t.rows = append(t.rows, stickyRow{class, rack})
		}
	}

	// A member may hold a row if it subscribes to the row's class. Where
	// most members may hold most rows, finding a cell is arithmetic; where
	// the table is mostly holes, a dense index of it would dwarf the cells
	// themselves.
	var ncells int64
	for r := range t.rows {
		for _, w := range classSubs[t.rows[r].class] {
			ncells += int64(bits.OnesCount64(w))
		}
	}
	grid := int64(len(t.rows)) * int64(nmembers)
	t.cells = make([]stickyCell, 0, ncells)
	t.rowStart = make([]int32, len(t.rows)+1)
	if grid <= 4*ncells {
		t.idx = make([]int32, grid)
	} else {
		t.at = make(map[uint64]int32, ncells)
	}
	for r := range t.rows {
		t.rowStart[r] = int32(len(t.cells))
		subs := classSubs[t.rows[r].class]
		for m := range nmembers {
			at := int32(-1)
			if subs[m/64]&(1<<(m%64)) != 0 {
				at = int32(len(t.cells))
				t.cells = append(t.cells, stickyCell{
					row:    int32(r),
					member: uint16(m),
					offrack: b.partRacks != nil && t.rows[r].rack != noRack &&
						b.memberRacks[m] != noRack && t.rows[r].rack != b.memberRacks[m],
				})
			}
			if t.idx != nil {
				t.idx[int32(r)*nmembers+m] = at
			} else if at >= 0 {
				t.at[uint64(r)<<32|uint64(m)] = at
			}
		}
	}
	t.rowStart[len(t.rows)] = int32(len(t.cells))

	for m := range b.plan {
		for _, p := range b.plan[m] {
			if orig := b.origOwner[p]; orig != unassignedPart && orig != uint16(m) {
				t.crossed = true
			}
			if at := t.cellAt(t.rowOf(p, b.partOwners[p]), uint16(m)); at >= 0 {
				t.cells[at].x++
			}
		}
	}
	// What a member came in with is everything it arrived holding, not only
	// what it still has: one that lost four of a row it had five of can take
	// three back, and counting only current holdings makes every cell look
	// settled so no trade ever looks worthwhile.
	for p, orig := range b.origOwner {
		if orig == unassignedPart {
			continue
		}
		if at := t.cellAt(t.rowOf(int32(p), b.partOwners[p]), orig); at >= 0 {
			t.cells[at].held++
		}
	}
	return t
}

// stickyStep is one leg of a rearrangement: a cell gaining or losing one.
type stickyStep struct {
	cell  int32
	delta int32
}

// stickyCycle is one rearrangement: a rotation through the table that lowers
// the total price. shiftsLoad is set when it runs through the group node, which
// is what makes it change how much anybody holds in total.
type stickyCycle struct {
	steps      []stickyStep
	shiftsLoad bool
}

// stickyFinder looks for a rotation through the table that lowers the total
// price. Nodes are rows, members, and one node standing for the group. A row to
// a member means that member takes one more from it; back again means it gives
// one up. A member to the group node and out to another is how load moves
// between them, which is the only way a rotation changes what anybody holds in
// total.
//
// Bellman-Ford from every node at once, so an improvement is found wherever it
// is rather than only where some chosen start can reach.
type stickyFinder struct {
	nrows, nmembers int
	group           int32
	dist            []int64
	viaCell         []int32
	viaFrom         []int32
	seen            []int32
	roots           []int32
	cycles          []stickyCycle
}

const stickyUnset = -1

func newStickyFinder(nrows, nmembers int) *stickyFinder {
	nodes := nrows + nmembers + 1
	return &stickyFinder{
		nrows:    nrows,
		nmembers: nmembers,
		group:    int32(nrows + nmembers),
		dist:     make([]int64, nodes),
		viaCell:  make([]int32, nodes),
		viaFrom:  make([]int32, nodes),
		seen:     make([]int32, nodes),
	}
}

func (f *stickyFinder) memberNode(m uint16) int32 { return int32(f.nrows) + int32(m) }

// onCycles collects a node from every loop in the chain of predecessors. Any
// loop there is a rotation that lowers the price, so the search can stop at the
// first sweep that closes one rather than running a pass per node -- and take
// all of them, since a node has only one predecessor and so no two loops can
// share one. Loops that miss the group node leave every member's holdings
// where they were, one partition in for each out, so cancelling one cannot
// change what another is worth.
func (f *stickyFinder) onCycles(roots []int32) []int32 {
	for i := range f.seen {
		f.seen[i] = stickyUnset
	}
	for start := range int32(len(f.seen)) {
		if f.seen[start] != stickyUnset {
			continue
		}
		node := start
		for node != stickyUnset && f.seen[node] == stickyUnset {
			f.seen[node] = start
			node = f.viaFrom[node]
		}
		if node != stickyUnset && f.seen[node] == start {
			roots = append(roots, node)
		}
	}
	return roots
}

func (f *stickyFinder) find(cells []stickyCell, loads []int32, rackw, loadw int64) []stickyCycle {
	nodes := len(f.dist)
	clear(f.dist)
	for i := range f.viaCell {
		f.viaCell[i] = stickyUnset
		f.viaFrom[i] = stickyUnset
	}

	costToLoad := func(m uint16) int64 { return loadw * int64(2*loads[m]+1) }
	costToShed := func(m uint16) int64 { return -loadw * int64(2*loads[m]-1) }

	for pass := 0; pass <= nodes; pass++ {
		moved := false
		for i := range cells {
			c := &cells[i]
			r, m := c.row, f.memberNode(c.member)
			if d := f.dist[r] + c.costToAdd(rackw); d < f.dist[m] {
				f.dist[m], f.viaCell[m], f.viaFrom[m] = d, int32(i), r
				moved = true
			}
			if c.x > 0 {
				if d := f.dist[m] + c.costToDrop(rackw); d < f.dist[r] {
					f.dist[r], f.viaCell[r], f.viaFrom[r] = d, int32(i), m
					moved = true
				}
			}
		}
		for m := range f.nmembers {
			node := f.memberNode(uint16(m))
			// Toward the group node is this member taking on one more
			// partition overall; away from it is giving one up.
			if d := f.dist[node] + costToLoad(uint16(m)); d < f.dist[f.group] {
				f.dist[f.group], f.viaCell[f.group], f.viaFrom[f.group] = d, stickyUnset, node
				moved = true
			}
			if loads[m] > 0 {
				if d := f.dist[f.group] + costToShed(uint16(m)); d < f.dist[node] {
					f.dist[node], f.viaCell[node], f.viaFrom[node] = d, stickyUnset, f.group
					moved = true
				}
			}
		}
		if !moved {
			return nil
		}
		if f.roots = f.onCycles(f.roots[:0]); len(f.roots) > 0 {
			f.cycles = f.cycles[:0]
			for _, at := range f.roots {
				f.cycles = append(f.cycles, f.extract(at))
			}
			return f.cycles
		}
	}
	return nil
}

func (f *stickyFinder) extract(at int32) stickyCycle {
	var cycle stickyCycle
	for node := at; ; {
		if node == f.group {
			cycle.shiftsLoad = true
		}
		if cell := f.viaCell[node]; cell != stickyUnset {
			if node >= int32(f.nrows) && node != f.group {
				cycle.steps = append(cycle.steps, stickyStep{cell, +1})
			} else {
				cycle.steps = append(cycle.steps, stickyStep{cell, -1})
			}
		}
		node = f.viaFrom[node]
		if node == at {
			break
		}
	}
	return cycle
}

// realizeAssignment rewrites the plan to match the table, giving each member as
// many of the partitions it arrived with as its count allows and filling the
// rest from whatever nobody reclaimed.
func (b *balancer) realizeAssignment(t *stickyTable) {
	// Bucket each partition under the cell its prior owner would sit in,
	// not the one holding it now: the table may well say a member should
	// end up with one it lost, and it can only take it back if we look past
	// the current owner. Partitions nobody has a claim on go in a pile per
	// row for anyone to draw from.
	claim := func(p int32) int32 {
		if orig := b.origOwner[p]; orig != unassignedPart {
			return t.cellAt(t.rowOf(p, b.partOwners[p]), orig)
		}
		return -1
	}
	owned := make([]int32, len(t.cells)+1)
	free := make([]int32, len(t.rows)+1)
	var nparts int32
	for m := range b.plan {
		for _, p := range b.plan[m] {
			if c := claim(p); c >= 0 {
				owned[c]++
			} else {
				free[t.rowOf(p, b.partOwners[p])]++
			}
			nparts++
		}
	}
	ownAt, freeAt := make([]int32, len(owned)), make([]int32, len(free))
	var sum int32
	for i := range owned {
		ownAt[i], sum = sum, sum+owned[i]
	}
	for i := range free {
		freeAt[i], sum = sum, sum+free[i]
	}
	parts := make([]int32, nparts)
	fill, freeFill := slices.Clone(ownAt), slices.Clone(freeAt)
	for m := range b.plan {
		for _, p := range b.plan[m] {
			if c := claim(p); c >= 0 {
				parts[fill[c]] = p
				fill[c]++
			} else {
				row := t.rowOf(p, b.partOwners[p])
				parts[freeFill[row]] = p
				freeFill[row]++
			}
		}
	}

	for i := range b.plan {
		b.plan[i] = b.plan[i][:0]
	}

	// Everybody first keeps as much of what they came with as their count
	// allows; only then is the shortfall made up from the piles.
	owed := make([]int32, len(t.cells))
	for i := range t.cells {
		owed[i] = t.cells[i].x
		for owed[i] > 0 && ownAt[i] < fill[i] {
			b.plan[t.cells[i].member].add(parts[ownAt[i]])
			ownAt[i]++
			owed[i]--
		}
	}
	for r := range t.rows {
		leftover := t.rowStart[r]
		for i := t.rowStart[r]; i < t.rowStart[r+1]; i++ {
			for owed[i] > 0 {
				var p int32
				switch {
				case freeAt[r] < freeFill[r]:
					p, freeAt[r] = parts[freeAt[r]], freeAt[r]+1
				default:
					for leftover < t.rowStart[r+1] && ownAt[leftover] == fill[leftover] {
						leftover++
					}
					if leftover == t.rowStart[r+1] {
						owed[i] = 0
						continue
					}
					p, ownAt[leftover] = parts[ownAt[leftover]], ownAt[leftover]+1
				}
				b.plan[t.cells[i].member].add(p)
				owed[i]--
			}
		}
	}
}
