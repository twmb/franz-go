package weighted

import (
	"slices"
	"sort"
)

// This file compares against the shape of scheduler that placement systems
// actually ship, on the workloads they actually have.
//
// The pattern in both CockroachDB's allocator and TiKV's placement driver is
// the same: pick the most loaded store, pick the least loaded one, move a
// single item, and accept the move only if the source still outranks the target
// afterwards by more than a tolerance. The tolerance is there to stop the two
// from trading an item back and forth forever.
//
// Two properties follow from that shape, and both are measurable. It moves one
// item at a time, so it can never perform an exchange -- and an exchange is the
// only way to improve an assignment whose loads are already as even as single
// moves can make them. And it has no notion of what a move costs, so the
// tolerance is doing two jobs at once: expressing how uneven is acceptable, and
// expressing how much churn is acceptable, which are not the same thing.

// SingleMoveScheduler models that shape. tolerance is in units of load, in the
// role tolerantSizeRatio plays: a move must improve the gap by more than this
// or it is refused.
//
// Returns the assignment and how many items it moved.
func SingleMoveScheduler(in *Instance, at []int, tolerance int64) ([]int, int) {
	at = slices.Clone(at)
	loads := make([]int64, in.Workers)
	for i, w := range at {
		loads[w] += in.Items[i].Weight
	}

	moves := 0
	for range 100000 {
		// Most loaded store with something movable, least loaded store.
		order := make([]int, in.Workers)
		for i := range order {
			order[i] = i
		}
		sort.Slice(order, func(a, b int) bool { return loads[order[a]] > loads[order[b]] })

		did := false
		for _, src := range order {
			for i := len(order) - 1; i >= 0; i-- {
				dst := order[i]
				if dst == src || loads[dst] >= loads[src] {
					continue
				}
				// Move one item from src to dst, if any may go.
				for item := range in.Items {
					if at[item] != src || !containsInt(in.Items[item].Eligible, dst) {
						continue
					}
					w := in.Items[item].Weight
					// The acceptance rule: after the move the source
					// must still be no worse than the target, with the
					// tolerance keeping near-equal stores from trading.
					if loads[src]-w >= loads[dst]+w+tolerance {
						at[item] = dst
						loads[src] -= w
						loads[dst] += w
						moves++
						did = true
						break
					}
				}
				if did {
					break
				}
			}
			if did {
				break
			}
		}
		if !did {
			break
		}
	}
	return at, moves
}

// Imbalance is what an operator watches: the spread between the busiest and
// quietest worker, as a fraction of the average.
func Imbalance(in *Instance, at []int) float64 {
	loads := make([]int64, in.Workers)
	var total int64
	for i, w := range at {
		loads[w] += in.Items[i].Weight
		total += in.Items[i].Weight
	}
	lo, hi := loads[0], loads[0]
	for _, l := range loads {
		lo = min(lo, l)
		hi = max(hi, l)
	}
	avg := float64(total) / float64(in.Workers)
	if avg == 0 {
		return 0
	}
	return float64(hi-lo) / avg
}

// MovesFrom counts how many items ended up somewhere other than where they
// started, which is what a rebalance costs the cluster.
func MovesFrom(start, end []int) int {
	n := 0
	for i := range start {
		if start[i] != end[i] {
			n++
		}
	}
	return n
}

// FlowOnlyRepair is the priced repair with the rotations that a flow can
// express, and nothing that has to be enumerated over items. Everything it does
// costs table cells rather than items, which is the difference between running
// on a cluster and not.
func FlowOnlyRepair(in *Instance, at []int) []int {
	t := newTable(in)
	at = slices.Clone(at)
	loadw := int64(len(in.Items) + 1)
	for range 200 {
		t.fill(at)
		moved := false
		for _, w := range t.weights() {
			for t.cancelOne(w, loadw) {
				moved = true
			}
		}
		if !moved {
			return at
		}
		at = t.realize()
	}
	return at
}
