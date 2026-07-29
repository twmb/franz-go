package weighted

import "slices"

// equalWeightExchange trades one item for a set of others of exactly the same
// total weight, between two workers.
//
// This is the move divisibility asks for and rotations cannot express. A
// rotation trades one item for one item, so it can only ever move weight
// between workers in the sizes that already exist. When weights divide each
// other, one item of a size is worth exactly several of a smaller size, and
// the useful trade is one-for-several -- which changes how many items each
// worker holds and is therefore not a cycle in any assignment graph.
//
// Both sides keep their load unchanged, so this reaches arrangements that
// preserve a load vector no sequence of load-preserving rotations can.
func equalWeightExchange(in *Instance, at []int, loadw int64, maxSet int) bool {
	scalar := func(c Cost) int64 { return loadw*c.Squares + c.Moves }
	best := scalar(in.Eval(at))
	var bestOne int
	var bestSet []int

	byWorker := make([][]int, in.Workers)
	for i := range in.Items {
		byWorker[at[i]] = append(byWorker[at[i]], i)
	}

	for one := range in.Items {
		a := at[one]
		want := in.Items[one].Weight
		for b := range in.Workers {
			if b == a || !in.eligible(one, b) {
				continue
			}
			// Find a set on b of exactly the same total weight, every
			// member of which may move to a.
			var set []int
			var rec func(start int, left int64)
			rec = func(start int, left int64) {
				if bestSet != nil && len(set) == 0 {
					// keep scanning; cheap guard against deep work
				}
				if left == 0 && len(set) > 0 {
					for i := range set {
						at[set[i]] = a
					}
					at[one] = b
					if s := scalar(in.Eval(at)); s < best {
						best, bestOne, bestSet = s, one, slices.Clone(set)
					}
					at[one] = a
					for i := range set {
						at[set[i]] = b
					}
					return
				}
				if len(set) >= maxSet || left < 0 {
					return
				}
				for idx := start; idx < len(byWorker[b]); idx++ {
					j := byWorker[b][idx]
					if in.Items[j].Weight > left || !in.eligible(j, a) {
						continue
					}
					set = append(set, j)
					rec(idx+1, left-in.Items[j].Weight)
					set = set[:len(set)-1]
				}
			}
			rec(0, want)
		}
	}

	if bestSet == nil {
		return false
	}
	a := at[bestOne]
	b := at[bestSet[0]]
	for _, j := range bestSet {
		at[j] = a
	}
	at[bestOne] = b
	return true
}

// RepairX is RepairK plus equal-weight one-for-several exchanges.
func RepairX(in *Instance, at []int, maxk, maxSet int) []int {
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
		if !moved && equalWeightExchange(in, at, loadw, maxSet) {
			moved = true
		}
		if !moved {
			return at
		}
	}
}

// ScaleDecompose solves the divisible case the way the structure suggests:
// scale by scale, largest first. Within one scale every item weighs the same,
// so that sub-problem is the unit-weight problem this whole line of work solves
// exactly -- the loads already placed by larger scales are just a starting
// offset, and the cost stays convex in how many more a worker takes.
//
// The open question this answers is whether the sequence is optimal, i.e.
// whether committing the large items before looking at the small ones can be
// regretted. For making change with divisible coins it cannot. Here each scale
// is solved exhaustively so that only the decomposition is under test, not the
// quality of the per-scale solver.
func ScaleDecompose(in *Instance) []int {
	var scales []int64
	for i := range in.Items {
		if !slices.Contains(scales, in.Items[i].Weight) {
			scales = append(scales, in.Items[i].Weight)
		}
	}
	slices.Sort(scales)
	slices.Reverse(scales)

	at := make([]int, len(in.Items))
	for i := range at {
		at[i] = -1
	}
	loads := make([]int64, in.Workers)

	for _, w := range scales {
		var idx []int
		for i := range in.Items {
			if in.Items[i].Weight == w {
				idx = append(idx, i)
			}
		}
		best := int64(1) << 62
		bestPick := make([]int, len(idx))
		pick := make([]int, len(idx))
		var rec func(k int)
		rec = func(k int) {
			if k == len(idx) {
				var sq int64
				for _, l := range loads {
					sq += l * l
				}
				// Tie-break on stickiness within the scale.
				var moves int64
				for j, i := range idx {
					if in.Items[i].Prior >= 0 && in.Items[i].Prior != pick[j] {
						moves++
					}
				}
				if c := sq*int64(len(in.Items)+1) + moves; c < best {
					best = c
					copy(bestPick, pick)
				}
				return
			}
			for _, worker := range in.Items[idx[k]].Eligible {
				pick[k] = worker
				loads[worker] += w
				rec(k + 1)
				loads[worker] -= w
			}
		}
		rec(0)
		for j, i := range idx {
			at[i] = bestPick[j]
			loads[bestPick[j]] += w
		}
	}
	return at
}
