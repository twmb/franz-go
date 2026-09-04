package sticky

import (
	"math"
	"math/bits"
	"slices"
)

// Balancing settles how many partitions each member holds but not which.
// Among the assignments with those counts, some keep far more partitions
// where they were, and some place far more in the rack they are led from.
// The repair below rearranges the plan to the best of them: balance first,
// then rack, then stickiness.
//
// Partitions nobody can tell apart form a row, and the plan collapses to a
// table of how many of each row each member holds. A rotation through the
// table adds one to some cells and takes one from others. The plan is
// cheapest when no rotation lowers its price, so we cancel rotations that
// do until none remain, then rewrite the plan from the table.

// stickyRow is a set of partitions no member can tell apart: they have the
// same subscribers and are led from the same rack.
type stickyRow struct {
	class int32
	rack  uint16
}

// stickyCell counts how many partitions of one row one member holds.
type stickyCell struct {
	row     int32
	member  uint16
	x       int32 // partitions of this row the member holds
	held    int32 // how many of them it arrived holding
	offrack bool  // the member is not in this row's rack
}

// costToAdd prices one more partition here: a move unless the member is
// still below what it arrived with, plus a zone crossing if it is off rack.
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

// costToDrop prices giving one up here, which is what the last one cost.
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

// room is how many partitions can move through this cell in the given
// direction before the price of the next one changes.
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

// stickyTable is the rows by members table the repair works on. Cells are
// grouped by row.
type stickyTable struct {
	rows     []stickyRow
	cells    []stickyCell
	rowStart []int32 // row r owns cells[rowStart[r]:rowStart[r+1]]
	nmembers int32

	// A partition's row is looked up from its topic's class and its rack
	// rather than stored per partition.
	owners  []uint32
	classOf []int32
	rowAt   []int32
	stride  int32
	racks   []uint16

	// crossed is set if some partition is held by a member other than the
	// one that arrived with it. Counts cannot see that: two members each
	// holding the other's partition look settled.
	crossed bool

	// One of these indexes cells by (row, member): the dense one when the
	// table is mostly full, the map when it is mostly holes.
	idx []int32
	at  map[uint64]int32
}

func (t *stickyTable) rowOf(partNum int32) int32 {
	var rack int32
	if t.racks != nil {
		rack = int32(t.racks[partNum])
	}
	return t.rowAt[t.classOf[t.owners[partNum]]*t.stride+rack]
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

// repairAssignment rearranges which partitions each member holds until no
// rotation through the table lowers the price. Balance outranks rack, which
// outranks stickiness: load only ever shifts between members one level
// apart, and a zone crossing costs more than a whole rotation can save in
// stickiness.
func (b *balancer) repairAssignment() {
	// With uniform subscriptions and no racks, balancing already leaves
	// nothing to trade: every member can hold every partition.
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

	// A rotation gains or loses at most one partition of stickiness per
	// cell it passes through and passes each row at most once, so this is
	// more than any rotation can save in stickiness.
	rackw := int64(2*min(len(t.rows), len(b.members)) + 1)

	f := newStickyFinder(len(t.rows), len(b.members))
	var moved bool
	for cycles := f.find(t.cells, loads, rackw); len(cycles) > 0; cycles = f.find(t.cells, loads, rackw) {
		moved = true
		for _, cycle := range cycles {
			t.apply(cycle, loads)
		}
	}

	// If nothing rotated and everybody still holds what it arrived with,
	// the plan already is what the table says.
	if !moved && !t.crossed {
		return
	}
	b.realizeAssignment(t)
}

// apply moves partitions around one rotation. Every cell on it holds the
// same price for as many partitions as its tightest cell has room for, so
// they all move at once. A rotation that shifts load moves one: it changes
// which levels two members sit on, so the next search has to re-price it.
func (t *stickyTable) apply(cycle stickyCycle, loads []int32) {
	n := int32(1)
	if !cycle.shiftsLoad {
		n = math.MaxInt32
		for _, step := range cycle.steps {
			n = min(n, t.cells[step.cell].room(step.delta))
		}
	}
	for _, step := range cycle.steps {
		c := &t.cells[step.cell]
		c.x += step.delta * n
		loads[c.member] += step.delta * n
	}
}

// topicClasses groups topics whose subscribers are exactly the same set:
// any member that may hold one may hold the other, and nothing else about
// a topic is priced. Two thousand members reading the same topics with one
// of them reading one extra is two classes, not a row per topic.
//
// Returns the class of each topic and each class's members as a bitset.
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

// stickyTable builds the table of rows by the members that may hold them.
func (b *balancer) stickyTable() *stickyTable {
	classOf, classSubs := b.topicClasses()
	nmembers := int32(len(b.plan))
	t := &stickyTable{
		nmembers: nmembers,
		owners:   b.partOwners,
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

	// A member may hold a row if it subscribes to the row's class. We index
	// cells densely unless the table is mostly holes, where a dense index
	// would dwarf the cells themselves.
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
		row := t.rows[r]
		subs := classSubs[row.class]
		for m := range nmembers {
			at := int32(-1)
			if subs[m/64]&(1<<(m%64)) != 0 {
				at = int32(len(t.cells))
				offrack := row.rack != noRack && b.memberRacks[m] != noRack && row.rack != b.memberRacks[m]
				t.cells = append(t.cells, stickyCell{row: int32(r), member: uint16(m), offrack: offrack})
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
			if at := t.cellAt(t.rowOf(p), uint16(m)); at >= 0 {
				t.cells[at].x++
			}
		}
	}
	// held counts everything a member arrived with, not only what it still
	// has: a member that lost four of a row it had five of can take three
	// back, and counting only current holdings makes every cell look settled.
	for p, orig := range b.origOwner {
		if orig == unassignedPart {
			continue
		}
		if at := t.cellAt(t.rowOf(int32(p)), orig); at >= 0 {
			t.cells[at].held++
		}
	}
	return t
}

// stickyStep is one leg of a rotation: a cell gaining or losing one.
type stickyStep struct {
	cell  int32
	delta int32
}

// stickyCycle is one rotation that lowers the price. shiftsLoad is set if
// it moves load between members, which only happens one level at a time.
type stickyCycle struct {
	steps      []stickyStep
	shiftsLoad bool
}

// stickyFinder searches the table for a rotation that lowers the price.
// Nodes are rows, members, and one node per level of load. A row to a
// member means the member takes one more of the row; the member back to
// the row means it gives one up. A member to its level's node and out to a
// member one level below shifts one partition of load between them, which
// leaves the balance exactly as it was.
//
// This is Bellman-Ford from every node at once. The predecessor chains
// close a cycle exactly when some rotation lowers the price.
type stickyFinder struct {
	nrows, nmembers int
	levels          []int32 // level node i is for load levels[i]
	dist            []int64
	viaCell         []int32
	viaFrom         []int32
	seen            []int32
	roots           []int32
	cycles          []stickyCycle
}

const stickyUnset = -1

func newStickyFinder(nrows, nmembers int) *stickyFinder {
	nodes := nrows + 2*nmembers // at most one level per member
	return &stickyFinder{
		nrows:    nrows,
		nmembers: nmembers,
		dist:     make([]int64, nodes),
		viaCell:  make([]int32, nodes),
		viaFrom:  make([]int32, nodes),
		seen:     make([]int32, nodes),
	}
}

func (f *stickyFinder) memberNode(m uint16) int32 { return int32(f.nrows) + int32(m) }

func (f *stickyFinder) levelNode(load int32) (int32, bool) {
	i, ok := slices.BinarySearch(f.levels, load)
	return int32(f.nrows + f.nmembers + i), ok
}

func (f *stickyFinder) find(cells []stickyCell, loads []int32, rackw int64) []stickyCycle {
	// Load only shifts to a member one level below, so each level some
	// member is on gets a node: members one below enter it by taking, and
	// members on it leave it by giving one up.
	f.levels = f.levels[:0]
	for _, load := range loads {
		if load > 0 {
			f.levels = append(f.levels, load)
		}
	}
	slices.Sort(f.levels)
	f.levels = slices.Compact(f.levels)

	nodes := f.nrows + f.nmembers + len(f.levels)
	clear(f.dist[:nodes])
	for i := range nodes {
		f.viaCell[i] = stickyUnset
		f.viaFrom[i] = stickyUnset
	}

	for range nodes {
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
			// Taking one more puts a member on the level above; a member
			// on that level giving one up comes down to this one.
			if take, ok := f.levelNode(loads[m] + 1); ok && f.dist[node] < f.dist[take] {
				f.dist[take], f.viaCell[take], f.viaFrom[take] = f.dist[node], stickyUnset, node
				moved = true
			}
			if loads[m] > 0 {
				if shed, _ := f.levelNode(loads[m]); f.dist[shed] < f.dist[node] {
					f.dist[node], f.viaCell[node], f.viaFrom[node] = f.dist[shed], stickyUnset, shed
					moved = true
				}
			}
		}
		if !moved {
			return nil
		}
		if f.roots = f.onCycles(f.roots[:0], nodes); len(f.roots) > 0 {
			f.cycles = f.cycles[:0]
			for _, at := range f.roots {
				f.cycles = append(f.cycles, f.extract(at))
			}
			return f.cycles
		}
	}

	// Unreachable: a distance can only still fall after a pass per node
	// if its predecessor chain has closed a cycle, which the check above
	// would have found.
	return nil
}

// onCycles returns one node from every cycle in the predecessor chains.
// Each node has one predecessor, so no two cycles share a node, and every
// one of them is a rotation that lowers the price.
func (f *stickyFinder) onCycles(roots []int32, nodes int) []int32 {
	seen := f.seen[:nodes]
	for i := range seen {
		seen[i] = stickyUnset
	}
	for start := range int32(nodes) {
		if seen[start] != stickyUnset {
			continue
		}
		node := start
		for node != stickyUnset && seen[node] == stickyUnset {
			seen[node] = start
			node = f.viaFrom[node]
		}
		if node != stickyUnset && seen[node] == start {
			roots = append(roots, node)
		}
	}
	return roots
}

func (f *stickyFinder) extract(at int32) stickyCycle {
	var cycle stickyCycle
	for node := at; ; {
		switch {
		case node >= int32(f.nrows+f.nmembers): // a level node
			cycle.shiftsLoad = true
		case f.viaCell[node] == stickyUnset: // a member reached from a level node
		case node >= int32(f.nrows): // a member taking one from a row
			cycle.steps = append(cycle.steps, stickyStep{f.viaCell[node], +1})
		default: // a row a member gave one back to
			cycle.steps = append(cycle.steps, stickyStep{f.viaCell[node], -1})
		}
		if node = f.viaFrom[node]; node == at {
			return cycle
		}
	}
}

// realizeAssignment rewrites the plan to the table's counts, giving each
// member as many of the partitions it arrived with as its count allows and
// filling the rest from whatever nobody took back.
func (b *balancer) realizeAssignment(t *stickyTable) {
	// Bucket every partition under the cell of the member that arrived
	// with it, or under a pile for its row if nobody did, laid out so a
	// row's buckets are contiguous with the pile last. Bucketing by prior
	// owner rather than current holder is what lets a member take back
	// what it lost.
	bucketOf := func(p int32) int32 {
		row := t.rowOf(p)
		if orig := b.origOwner[p]; orig != unassignedPart {
			if at := t.cellAt(row, orig); at >= 0 {
				return at + row
			}
		}
		return t.rowStart[row+1] + row
	}
	nbuckets := len(t.cells) + len(t.rows)
	start := make([]int32, nbuckets+1)
	for m := range b.plan {
		for _, p := range b.plan[m] {
			start[bucketOf(p)+1]++
		}
	}
	for i := 1; i <= nbuckets; i++ {
		start[i] += start[i-1]
	}
	parts := make([]int32, start[nbuckets])
	next := slices.Clone(start[:nbuckets])
	for m := range b.plan {
		for _, p := range b.plan[m] {
			bk := bucketOf(p)
			parts[next[bk]] = p
			next[bk]++
		}
	}

	// Every member first takes what it arrived with as far as its count
	// allows. Whatever is then left in a bucket is a partition its owner
	// cannot take back, so shortfalls are filled from the row's buckets in
	// order without caring which.
	for i := range b.plan {
		b.plan[i] = b.plan[i][:0]
	}
	copy(next, start)
	owed := make([]int32, len(t.cells))
	for i := range t.cells {
		c, bk := &t.cells[i], int32(i)+t.cells[i].row
		n := min(c.x, start[bk+1]-next[bk])
		b.plan[c.member] = append(b.plan[c.member], parts[next[bk]:next[bk]+n]...)
		next[bk] += n
		owed[i] = c.x - n
	}
	for r := range t.rows {
		bk, end := t.rowStart[r]+int32(r), t.rowStart[r+1]+int32(r)
		for i := t.rowStart[r]; i < t.rowStart[r+1]; i++ {
			for owed[i] > 0 {
				for bk <= end && next[bk] == start[bk+1] {
					bk++
				}
				if bk > end {
					break // rows hold exactly what their cells count; cannot happen
				}
				n := min(owed[i], start[bk+1]-next[bk])
				member := t.cells[i].member
				b.plan[member] = append(b.plan[member], parts[next[bk]:next[bk]+n]...)
				next[bk] += n
				owed[i] -= n
			}
		}
	}
}
