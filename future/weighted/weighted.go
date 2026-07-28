// Package weighted explores what survives of the franz-go sticky balancer's
// exactness when items stop being unit-weight.
//
// The unit-weight repair works because a rotation that preserves counts also
// preserves load, so load bookkeeping can live entirely on one extra node and
// every other edge prices only stickiness. With weights that stops being true:
// a worker that gains an item of weight 2 and gives up one of weight 3 has
// changed its load, and a cycle visits that worker exactly once with one edge
// in and one out, so there is no third edge on which to price the difference.
//
// So the move set splits in two:
//
//   - Rotations within one weight class. Every worker on the cycle gains and
//     loses the same weight, so load is preserved exactly, and a cycle through
//     the transfer node moves exactly one item's weight between two workers.
//     Pricing is exact and negative-cycle cancelling applies unchanged.
//
//   - Exchanges across weight classes. Structurally inexpressible as a cycle
//     in that graph. They need their own move, priced directly.
//
// Neither is a formulation trick: minimizing squared load over two workers
// eligible for everything is PARTITION, so no polynomial move set can be
// exactly optimal unless P = NP. What this package measures is how much is
// actually lost, as a function of how many distinct weights there are.
package weighted

import (
	"fmt"
	"slices"
	"sort"
)

// Item is one assignable thing.
type Item struct {
	Weight   int64
	Eligible []int // workers that may hold it
	Prior    int   // worker that arrived holding it, or -1
}

// Instance is one assignment problem.
type Instance struct {
	Items   []Item
	Workers int
}

// Cost is the objective, ranked: balance first, then stickiness. Balance is the
// sum of squared worker loads, which is minimized exactly when load is as even
// as the eligibility constraints allow. Moves counts items not with the worker
// that arrived holding them.
type Cost struct {
	Squares int64
	Moves   int64
}

func (c Cost) Less(o Cost) bool {
	if c.Squares != o.Squares {
		return c.Squares < o.Squares
	}
	return c.Moves < o.Moves
}

func (c Cost) String() string { return fmt.Sprintf("squares=%d moves=%d", c.Squares, c.Moves) }

// Eval scores an assignment of item => worker.
func (in *Instance) Eval(at []int) Cost {
	loads := make([]int64, in.Workers)
	var moves int64
	for i, w := range at {
		loads[w] += in.Items[i].Weight
		if in.Items[i].Prior >= 0 && in.Items[i].Prior != w {
			moves++
		}
	}
	var squares int64
	for _, l := range loads {
		squares += l * l
	}
	return Cost{squares, moves}
}

func (in *Instance) eligible(item, worker int) bool {
	return slices.Contains(in.Items[item].Eligible, worker)
}

// --- the table -------------------------------------------------------------

// row groups items nothing can tell apart: same eligibility, same weight.
type row struct {
	weight   int64
	eligible []int
	items    []int
}

type cell struct {
	row, worker int
	x, held     int
}

type table struct {
	in    *Instance
	rows  []row
	cells []cell
	at    map[[2]int]int // (row, worker) => cell
}

func newTable(in *Instance) *table {
	t := &table{in: in, at: make(map[[2]int]int)}
	byKey := make(map[string]int)
	for i := range in.Items {
		el := slices.Clone(in.Items[i].Eligible)
		sort.Ints(el)
		key := fmt.Sprint(in.Items[i].Weight, el)
		r, ok := byKey[key]
		if !ok {
			r = len(t.rows)
			byKey[key] = r
			t.rows = append(t.rows, row{weight: in.Items[i].Weight, eligible: el})
		}
		t.rows[r].items = append(t.rows[r].items, i)
	}
	for r := range t.rows {
		for _, w := range t.rows[r].eligible {
			t.at[[2]int{r, w}] = len(t.cells)
			t.cells = append(t.cells, cell{row: r, worker: w})
		}
	}
	return t
}

func (t *table) cellAt(r, w int) int {
	if c, ok := t.at[[2]int{r, w}]; ok {
		return c
	}
	return -1
}

// load returns each worker's total weight implied by the current counts.
func (t *table) loads() []int64 {
	loads := make([]int64, t.in.Workers)
	for i := range t.cells {
		c := &t.cells[i]
		loads[c.worker] += int64(c.x) * t.rows[c.row].weight
	}
	return loads
}

// fill sets counts from an assignment, and records what each worker arrived
// holding so stickiness can be priced.
func (t *table) fill(at []int) {
	for i := range t.cells {
		t.cells[i].x, t.cells[i].held = 0, 0
	}
	rowOf := make([]int, len(t.in.Items))
	for r := range t.rows {
		for _, item := range t.rows[r].items {
			rowOf[item] = r
		}
	}
	for item, w := range at {
		if c := t.cellAt(rowOf[item], w); c >= 0 {
			t.cells[c].x++
		}
	}
	for item := range t.in.Items {
		if p := t.in.Items[item].Prior; p >= 0 {
			if c := t.cellAt(rowOf[item], p); c >= 0 {
				t.cells[c].held++
			}
		}
	}
}

// realize turns counts back into an assignment, giving each worker as many of
// the items it arrived with as its count allows before drawing from the pool.
func (t *table) realize() []int {
	at := make([]int, len(t.in.Items))
	for i := range at {
		at[i] = -1
	}
	for r := range t.rows {
		owed := make(map[int]int)
		for _, w := range t.rows[r].eligible {
			owed[w] = t.cells[t.cellAt(r, w)].x
		}
		var pool []int
		for _, item := range t.rows[r].items {
			p := t.in.Items[item].Prior
			if p >= 0 && owed[p] > 0 {
				at[item] = p
				owed[p]--
				continue
			}
			pool = append(pool, item)
		}
		for _, item := range pool {
			for w, n := range owed {
				if n > 0 {
					at[item] = w
					owed[w]--
					break
				}
			}
		}
	}
	return at
}

// --- the repair ------------------------------------------------------------

const noCycle = -1

// Repair is the weighted generalization: cancel improving rotations within each
// weight class until none remain, then take any improving cross-weight
// exchange, and repeat until neither fires.
func Repair(in *Instance, at []int) []int {
	t := newTable(in)
	at = slices.Clone(at)
	loadw := int64(len(in.Items) + 1) // balance outranks stickiness

	for {
		t.fill(at)
		moved := false
		for _, w := range t.weights() {
			for t.cancelOne(w, loadw) {
				moved = true
			}
		}
		if moved {
			at = t.realize()
		}
		if swapped := crossWeightSwap(in, at, loadw); swapped {
			moved = true
		}
		if !moved {
			return at
		}
	}
}

func (t *table) weights() []int64 {
	var ws []int64
	for r := range t.rows {
		if !slices.Contains(ws, t.rows[r].weight) {
			ws = append(ws, t.rows[r].weight)
		}
	}
	slices.Sort(ws)
	return ws
}

// cancelOne finds one improving rotation among rows of the given weight and
// applies it. Every worker on such a cycle gains and loses exactly one item of
// that weight, so load is preserved -- except through the transfer node, which
// moves exactly one item's weight from one worker to another and is priced for
// it. That is what keeps the pricing exact within a class.
func (t *table) cancelOne(weight, loadw int64) bool {
	var rows []int
	for r := range t.rows {
		if t.rows[r].weight == weight {
			rows = append(rows, r)
		}
	}
	if len(rows) == 0 {
		return false
	}
	rowIdx := make(map[int]int, len(rows))
	for i, r := range rows {
		rowIdx[r] = i
	}
	nw := t.in.Workers
	nodes := len(rows) + nw + 1
	transfer := len(rows) + nw
	loads := t.loads()

	dist := make([]int64, nodes)
	viaCell := make([]int, nodes)
	viaFrom := make([]int, nodes)
	for i := range viaCell {
		viaCell[i], viaFrom[i] = noCycle, noCycle
	}
	workerNode := func(w int) int { return len(rows) + w }

	// Convex load: f(L) = L^2, so taking on weight costs f(L+w)-f(L) and
	// shedding saves f(L-w)-f(L). The zero-cost band that tolerance ratios
	// approximate is exactly where these two sum to zero.
	takeOn := func(w int) int64 { return loadw * (2*loads[w]*weight + weight*weight) }
	shed := func(w int) int64 { return loadw * (-2*loads[w]*weight + weight*weight) }

	last := noCycle
	for pass := 0; pass <= nodes; pass++ {
		relaxed := false
		for ci := range t.cells {
			c := &t.cells[ci]
			ri, ok := rowIdx[c.row]
			if !ok {
				continue
			}
			m := workerNode(c.worker)
			addCost := int64(0)
			if c.x >= c.held {
				addCost = 1
			}
			if d := dist[ri] + addCost; d < dist[m] {
				dist[m], viaCell[m], viaFrom[m] = d, ci, ri
				relaxed, last = true, m
			}
			if c.x > 0 {
				dropCost := int64(0)
				if c.x > c.held {
					dropCost = -1
				}
				if d := dist[m] + dropCost; d < dist[ri] {
					dist[ri], viaCell[ri], viaFrom[ri] = d, ci, m
					relaxed, last = true, ri
				}
			}
		}
		for w := range nw {
			node := workerNode(w)
			if d := dist[node] + takeOn(w); d < dist[transfer] {
				dist[transfer], viaCell[transfer], viaFrom[transfer] = d, noCycle, node
				relaxed, last = true, transfer
			}
			if loads[w] >= weight {
				if d := dist[transfer] + shed(w); d < dist[node] {
					dist[node], viaCell[node], viaFrom[node] = d, noCycle, transfer
					relaxed, last = true, node
				}
			}
		}
		if !relaxed {
			return false
		}
		if at := predCycle(viaFrom); at != noCycle {
			return t.apply(at, viaCell, viaFrom, rows, transfer)
		}
	}
	if last == noCycle {
		return false
	}
	at := last
	for range nodes {
		at = viaFrom[at]
	}
	return t.apply(at, viaCell, viaFrom, rows, transfer)
}

// predCycle returns a node whose predecessor chain loops, or noCycle.
func predCycle(viaFrom []int) int {
	seen := make([]int, len(viaFrom))
	for i := range seen {
		seen[i] = noCycle
	}
	for start := range viaFrom {
		if seen[start] != noCycle {
			continue
		}
		node := start
		for node != noCycle && seen[node] == noCycle {
			seen[node] = start
			node = viaFrom[node]
		}
		if node != noCycle && seen[node] == start {
			return node
		}
	}
	return noCycle
}

func (t *table) apply(start int, viaCell, viaFrom []int, rows []int, transfer int) bool {
	type step struct{ cell, delta int }
	var steps []step
	for node := start; ; {
		if c := viaCell[node]; c != noCycle {
			if node >= len(rows) && node != transfer {
				steps = append(steps, step{c, +1})
			} else {
				steps = append(steps, step{c, -1})
			}
		}
		node = viaFrom[node]
		if node == start {
			break
		}
	}
	if len(steps) == 0 {
		return false
	}
	for _, s := range steps {
		t.cells[s.cell].x += s.delta
		if t.cells[s.cell].x < 0 {
			return false
		}
	}
	return true
}

// crossWeightSwap takes one improving exchange of two items of different
// weights. These cannot be expressed as a rotation, so they get their own move.
func crossWeightSwap(in *Instance, at []int, loadw int64) bool {
	scalar := func(c Cost) int64 { return loadw*c.Squares + c.Moves }
	best := scalar(in.Eval(at))
	bi, bj := -1, -1
	for i := range in.Items {
		for j := i + 1; j < len(in.Items); j++ {
			if at[i] == at[j] || in.Items[i].Weight == in.Items[j].Weight {
				continue
			}
			if !in.eligible(i, at[j]) || !in.eligible(j, at[i]) {
				continue
			}
			at[i], at[j] = at[j], at[i]
			if s := scalar(in.Eval(at)); s < best {
				best, bi, bj = s, i, j
			}
			at[i], at[j] = at[j], at[i]
		}
	}
	if bi < 0 {
		return false
	}
	at[bi], at[bj] = at[bj], at[bi]
	return true
}

// --- baselines -------------------------------------------------------------

// Greedy is the shape most systems ship: place largest-first on the least
// loaded eligible worker, then accept any single move that improves things.
// This is roughly the guarantee a tolerance-band scheduler reaches.
func Greedy(in *Instance) []int {
	at := make([]int, len(in.Items))
	for i := range at {
		at[i] = -1
	}
	order := make([]int, len(in.Items))
	for i := range order {
		order[i] = i
	}
	slices.SortStableFunc(order, func(a, b int) int {
		return int(in.Items[b].Weight - in.Items[a].Weight)
	})
	loads := make([]int64, in.Workers)
	for _, item := range order {
		best, bestLoad := -1, int64(1)<<62
		// Prefer the worker that arrived with it when that is no worse.
		for _, w := range in.Items[item].Eligible {
			l := loads[w]
			if in.Items[item].Prior == w {
				l--
			}
			if l < bestLoad {
				best, bestLoad = w, l
			}
		}
		at[item] = best
		loads[best] += in.Items[item].Weight
	}
	singleMoveLocalSearch(in, at)
	return at
}

func singleMoveLocalSearch(in *Instance, at []int) {
	loadw := int64(len(in.Items) + 1)
	scalar := func(c Cost) int64 { return loadw*c.Squares + c.Moves }
	for {
		best, bi, bw := scalar(in.Eval(at)), -1, -1
		for i := range in.Items {
			was := at[i]
			for _, w := range in.Items[i].Eligible {
				if w == was {
					continue
				}
				at[i] = w
				if s := scalar(in.Eval(at)); s < best {
					best, bi, bw = s, i, w
				}
			}
			at[i] = was
		}
		if bi < 0 {
			return
		}
		at[bi] = bw
	}
}

// BruteForce is the true lexicographic optimum. Exponential; small inputs only.
func BruteForce(in *Instance) []int {
	at := make([]int, len(in.Items))
	best := make([]int, len(in.Items))
	bestCost := Cost{1 << 62, 1 << 62}
	var rec func(int)
	rec = func(i int) {
		if i == len(in.Items) {
			if c := in.Eval(at); c.Less(bestCost) {
				bestCost = c
				copy(best, at)
			}
			return
		}
		for _, w := range in.Items[i].Eligible {
			at[i] = w
			rec(i + 1)
		}
	}
	rec(0)
	return best
}

// crossWeightRotate takes one improving rotation of k items across k distinct
// workers, where the items may have different weights. A same-weight rotation
// is a cycle in the flow graph; a mixed-weight one is not, because it changes
// each worker's load and a cycle has no edge left on which to price that. So
// they are enumerated directly. k=2 is a swap.
func crossWeightRotate(in *Instance, at []int, loadw int64, k int) bool {
	scalar := func(c Cost) int64 { return loadw*c.Squares + c.Moves }
	best := scalar(in.Eval(at))
	var bestPick []int
	pick := make([]int, k)

	var rec func(start, depth int)
	rec = func(start, depth int) {
		if depth == k {
			// Rotate: item pick[0] goes where pick[1] was, and so on.
			was := make([]int, k)
			for i := range k {
				was[i] = at[pick[i]]
			}
			for i := range k {
				if !in.eligible(pick[i], was[(i+1)%k]) {
					return
				}
			}
			for i := range k {
				at[pick[i]] = was[(i+1)%k]
			}
			if s := scalar(in.Eval(at)); s < best {
				best = s
				bestPick = slices.Clone(pick)
			}
			for i := range k {
				at[pick[i]] = was[i]
			}
			return
		}
		for i := start; i < len(in.Items); i++ {
			// Distinct workers, else this is not a rotation.
			ok := true
			for d := range depth {
				if at[pick[d]] == at[i] {
					ok = false
					break
				}
			}
			if !ok {
				continue
			}
			pick[depth] = i
			rec(i+1, depth+1)
		}
	}
	rec(0, 0)

	if bestPick == nil {
		return false
	}
	was := make([]int, k)
	for i := range k {
		was[i] = at[bestPick[i]]
	}
	for i := range k {
		at[bestPick[i]] = was[(i+1)%k]
	}
	return true
}

// RepairK is Repair with cross-weight rotations up to length maxk.
func RepairK(in *Instance, at []int, maxk int) []int {
	t := newTable(in)
	at = slices.Clone(at)
	loadw := int64(len(in.Items) + 1)
	for {
		t.fill(at)
		moved := false
		for _, w := range t.weights() {
			for t.cancelOne(w, loadw) {
				moved = true
			}
		}
		if moved {
			at = t.realize()
		}
		for k := 2; k <= maxk; k++ {
			if crossWeightRotate(in, at, loadw, k) {
				moved = true
				break
			}
		}
		if !moved {
			return at
		}
	}
}

// FractionalBound is the whole weighted problem's lower bound, computed exactly
// by the unit-weight machinery.
//
// Model an item of weight w as w unit tokens sharing its eligibility, and then
// *drop* the requirement that they stay together. That relaxation is strictly
// more permissive, so its optimum can only be below the real one -- and it is a
// pure unit-weight instance, which is the case we solve exactly. So the thing
// that makes the weighted problem hard is precisely the co-location constraint,
// and removing it hands back a bound rather than nothing.
//
// This is what makes the loss measurable per instance instead of merely
// asserted: an assignment can be reported as within a proven distance of
// optimal even where the optimum itself is out of reach.
func FractionalBound(in *Instance) int64 {
	tokens := &Instance{Workers: in.Workers}
	for i := range in.Items {
		for range in.Items[i].Weight {
			tokens.Items = append(tokens.Items, Item{
				Weight:   1,
				Eligible: in.Items[i].Eligible,
				Prior:    -1,
			})
		}
	}
	at := Greedy(tokens)
	at = Repair(tokens, at)
	return tokens.Eval(at).Squares
}
