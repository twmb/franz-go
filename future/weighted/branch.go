package weighted

import (
	"slices"
	"sort"
)

// This file pursues exactness for weights that are unrelated to each other --
// no common factor, no bounded ratio, nothing.
//
// That cannot be had in the worst case: two workers eligible for everything and
// squared-load cost is PARTITION. But worst case is the wrong target. Cutting
// stock is NP-hard and is solved to proven optimality at industrial sizes every
// day, because a search that can prove a branch worthless never explores it.
//
// So the question is not whether an exact algorithm exists in general -- it does
// not -- but how large an instance can be closed with proof. That depends
// almost entirely on the bound.

// waterfillBound is the least squared load the unplaced weight could possibly
// produce, ignoring which workers are eligible for it and ignoring that items
// cannot be split. Both make it optimistic, which is what a bound must be.
//
// Given the loads so far, spreading the remainder to level the lowest workers
// first is optimal for a convex cost, so the bound is found by pouring the
// remaining weight in until it reaches a common level.
func waterfillBound(loads []int64, remaining int64) int64 {
	s := slices.Clone(loads)
	sort.Slice(s, func(i, j int) bool { return s[i] < s[j] })

	// Find the level the remainder reaches, filling from the lowest up.
	k, level := 1, s[0]
	for k <= len(s) {
		var need int64
		if k < len(s) {
			need = int64(k)*(s[k]-s[k-1]) - 0
		}
		if k == len(s) || remaining < need {
			// Everything left fits below the next step.
			level = s[k-1] + remaining/int64(k)
			rem := remaining % int64(k)
			var sq int64
			for i := range s {
				l := s[i]
				if i < k {
					l = level
				}
				sq += l * l
			}
			// The leftover that does not divide evenly must sit
			// somewhere, and one unit above the level is cheapest.
			if rem > 0 {
				sq += 2*level*rem + rem*rem/int64(max(1, int(rem)))
			}
			return sq
		}
		remaining -= need
		k++
	}
	return 0
}

// BranchAndBound returns a provably optimal assignment, or gives up after a
// node budget and reports that it did not prove anything. Items are taken
// heaviest first, since a heavy item placed badly is detected soonest.
func BranchAndBound(in *Instance, incumbent []int, budget int) (best []int, proved bool, nodes int) {
	loadw := int64(len(in.Items) + 1)
	scalar := func(c Cost) int64 { return loadw*c.Squares + c.Moves }

	order := make([]int, len(in.Items))
	for i := range order {
		order[i] = i
	}
	slices.SortStableFunc(order, func(a, b int) int {
		return int(in.Items[b].Weight - in.Items[a].Weight)
	})

	suffix := make([]int64, len(order)+1) // weight still unplaced
	for i := len(order) - 1; i >= 0; i-- {
		suffix[i] = suffix[i+1] + in.Items[order[i]].Weight
	}

	class := workerClasses(in)

	best = slices.Clone(incumbent)
	bestVal := scalar(in.Eval(best))
	at := make([]int, len(in.Items))
	for i := range at {
		at[i] = -1
	}
	loads := make([]int64, in.Workers)
	var moves int64

	gaveUp := false
	var rec func(k int)
	rec = func(k int) {
		if gaveUp {
			return
		}
		if nodes++; nodes > budget {
			gaveUp = true
			return
		}
		if k == len(order) {
			var sq int64
			for _, l := range loads {
				sq += l * l
			}
			if v := loadw*sq + moves; v < bestVal {
				bestVal = v
				copy(best, at)
			}
			return
		}
		// Nothing below this node can beat the incumbent if even a
		// perfect spread of what is left cannot.
		if loadw*waterfillBound(loads, suffix[k])+moves >= bestVal {
			return
		}
		item := order[k]
		// Workers nothing can tell apart, holding the same load, make
		// identical subtrees; one stands for all of them.
		var tried []int64
		for _, w := range in.Items[item].Eligible {
			dup := false
			for _, seen := range tried {
				if seen == int64(class[w])<<40|loads[w] {
					dup = true
					break
				}
			}
			if dup {
				continue
			}
			tried = append(tried, int64(class[w])<<40|loads[w])
			at[item] = w
			loads[w] += in.Items[item].Weight
			add := int64(0)
			if in.Items[item].Prior >= 0 && in.Items[item].Prior != w {
				add = 1
			}
			moves += add
			rec(k + 1)
			moves -= add
			loads[w] -= in.Items[item].Weight
			at[item] = -1
		}
	}
	rec(0)
	return best, !gaveUp, nodes
}
