package weighted

import "slices"

// The bound is the whole problem: search only prunes what it can prove
// worthless, so how far proof reaches is decided by how much the bound knows.
//
// Pouring the unplaced weight out level concedes two things. It lets weight
// land on workers not allowed to hold it, and it splits items. This file fixes
// the first.

// Eligibility turns out not to be where the bound loses. An earlier version of
// this file added the one fact eligibility gives for free -- items that may only
// go to workers in some set must all land there, so that set holds at least
// what it holds now plus everything trapped in it -- taking the best such bound
// over every distinct eligible set, every single worker, and every pairwise
// union. Measured against the level-pour bound alone it pruned nothing at all:
// identical node counts at ten, twenty and forty items, for four times the time
// at three hundred. Where eligibility is at all generous almost no weight is
// truly trapped, and for the set of all workers the bound is weaker than
// pouring level, since pouring respects loads already above the level and a
// flat average does not.
//
// So the concession that costs is the other one: pouring level splits items.
// With forty items over six workers the split optimum is essentially perfectly
// even, far below anything reachable while items stay whole, and a bound that
// far below the truth cannot discard anything.

// workerClasses groups workers that no item can tell apart: for every item,
// either both are eligible or neither is. Two such workers holding the same
// load make identical subtrees, so only one need be explored.
func workerClasses(in *Instance) []int {
	class := make([]int, in.Workers)
	for w := range in.Workers {
		class[w] = w
		for v := range w {
			same := true
			for i := range in.Items {
				ew := slices.Contains(in.Items[i].Eligible, w)
				ev := slices.Contains(in.Items[i].Eligible, v)
				if ew != ev {
					same = false
					break
				}
				// A prior owner distinguishes them too.
				if in.Items[i].Prior == w || in.Items[i].Prior == v {
					same = false
					break
				}
			}
			if same {
				class[w] = class[v]
				break
			}
		}
	}
	return class
}
