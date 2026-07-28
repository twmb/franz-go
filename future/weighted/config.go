package weighted

import "slices"

// This file changes what the variables are.
//
// Everything else here assigns items to workers, so the variables are item-
// worker pairs and there are as many of them as there are items. That is what
// makes uneven weights hard: a worker's load is a weighted sum of its variables,
// and the convex cost of that sum cannot be split into per-unit pieces unless
// every unit moves the load equally.
//
// The other formulation, which goes back to Gilmore and Gomory's cutting stock
// work, asks a different question: not where each item goes, but how many of
// each *type* a worker ends up holding. The variables become configurations,
// and items disappear from the problem -- only their type counts remain. The
// size of the search stops depending on how many items there are and starts
// depending on how many kinds of item there are, which for a workload with a
// handful of distinct sizes is a different problem altogether.
//
// This is the same quantity the equivalence-class collapse already computes.
// There it made an exactly solvable problem small enough to solve in a
// rebalance; here it decides whether an intractable one is tractable at all.

// configType is one kind of item: a weight and how many there are.
type configType struct {
	weight int64
	count  int
}

// ExactByConfigurations solves the uniform-eligibility case exactly by choosing
// each worker's type counts rather than each item's worker. Cost is the sum of
// squared loads. Returns the optimal cost.
//
// The state is the vector of type counts still unplaced, so the work depends on
// the product of the counts and the worker count -- never on which item is
// which. Where every item has a distinct weight this is no better than anything
// else, and it is not meant to be: it is exact precisely when types are few.
func ExactByConfigurations(weights []int64, workers int) int64 {
	var types []configType
	for _, w := range weights {
		if i := slices.IndexFunc(types, func(t configType) bool { return t.weight == w }); i >= 0 {
			types[i].count++
		} else {
			types = append(types, configType{weight: w, count: 1})
		}
	}
	slices.SortFunc(types, func(a, b configType) int { return int(b.weight - a.weight) })

	remaining := make([]int, len(types))
	for i := range types {
		remaining[i] = types[i].count
	}

	// key packs the remaining counts and the workers left into a string,
	// which is enough because counts are small wherever this is worth using.
	memo := make(map[string]int64)
	key := func(rem []int, left int) string {
		b := make([]byte, 0, len(rem)*3+3)
		for _, c := range rem {
			b = append(b, byte(c), ',')
		}
		return string(append(b, byte(left)))
	}

	var solve func(rem []int, left int) int64
	solve = func(rem []int, left int) int64 {
		if left == 1 {
			var load int64
			for i, c := range rem {
				load += int64(c) * types[i].weight
			}
			return load * load
		}
		k := key(rem, left)
		if v, ok := memo[k]; ok {
			return v
		}

		best := int64(1) << 62
		take := make([]int, len(types))
		// Enumerate this worker's configuration. Workers are
		// interchangeable, so requiring each to take no more of the
		// first type than the previous would prune the symmetry; the
		// counts here are small enough not to need it.
		var pick func(ti int, load int64)
		pick = func(ti int, load int64) {
			if ti == len(types) {
				next := make([]int, len(rem))
				for i := range rem {
					next[i] = rem[i] - take[i]
				}
				if c := load*load + solve(next, left-1); c < best {
					best = c
				}
				return
			}
			for n := 0; n <= rem[ti]; n++ {
				take[ti] = n
				pick(ti+1, load+int64(n)*types[ti].weight)
			}
			take[ti] = 0
		}
		pick(0, 0)

		memo[k] = best
		return best
	}
	return solve(remaining, workers)
}
