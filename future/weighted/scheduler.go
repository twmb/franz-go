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
	return FlowRepairPow(in, at, 2, 1)
}

// FlowRepairPow prices load as load^pow, in units where one unit of load is
// `scale` of weight. Scaling keeps a steep power from running off the end of an
// int64: a worker holding sixteen thousand costs 6.5e16 at the fourth power
// before the weight against churn is even applied.
func FlowRepairPow(in *Instance, at []int, pow int, scale int64) []int {
	t := newTable(in)
	at = slices.Clone(at)
	if scale > 1 {
		for i := range t.rows {
			t.rows[i].weight = max(1, t.rows[i].weight/scale)
		}
	}
	// A rotation is at most this long, and each leg saves at most one move,
	// so pricing load above it keeps balance ahead of churn.
	loadw := int64(2*min(len(t.rows), in.Workers) + 3)
	for range 200 {
		t.fill(at)
		moved := false
		for _, w := range t.weights() {
			for t.cancelOnePow(w, loadw, pow) {
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

// FlowThenPolish does the two things each approach is good at, in the order
// that lets each do it.
//
// The flow finds the moves that only work in combination -- give this away and
// take that, at the same time -- which is what a scheduler considering one move
// at a time can never accept, because neither half improves anything alone.
// What it does not do is fine levelling: it works on groups of interchangeable
// items and stops when no group can be rearranged, which can leave a store a
// little heavy in a way one more single move would fix.
//
// Single moves are exactly that fine levelling, and they are cheap once the
// hard part is done, because there is almost nothing left to find.
func FlowThenPolish(in *Instance, at []int) []int {
	at = FlowOnlyRepair(in, at)
	singleMoveLocalSearch(in, at)
	return at
}

// PeakShave levels heavily loaded workers against lighter ones, by a single
// move or by trading one item for another, until neither helps.
//
// It exists because sum of squares and the spread between busiest and quietest
// are not the same objective. Squares is content to leave one worker heavy if
// several others come down, which is right for total work and wrong for the
// worker that is actually the bottleneck.
//
// It supplies the move the flow structurally cannot, too: the flow rearranges
// within a weight class, so it can never answer an imbalance of thirty by
// trading a ninety for a sixty.
//
// The busiest worker is often not allowed to hand anything to the quietest, so
// pairing only those two stalls immediately wherever placement is restricted.
// Instead the heaviest few are each considered against the lightest several
// they can actually exchange with, and the best of those is taken.
func PeakShave(in *Instance, at []int, rounds int) []int {
	at = slices.Clone(at)
	loads := make([]int64, in.Workers)
	live := make([]bool, in.Workers)
	for i, w := range at {
		loads[w] += in.Items[i].Weight
		for _, e := range in.Items[i].Eligible {
			live[e] = true
		}
	}

	const heavy, light = 6, 12
	order := make([]int, 0, in.Workers)
	for w := range in.Workers {
		if live[w] {
			order = append(order, w)
		}
	}

	for range rounds {
		sort.Slice(order, func(a, b int) bool { return loads[order[a]] > loads[order[b]] })
		b := make([][]int, in.Workers)
		for i := range in.Items {
			b[at[i]] = append(b[at[i]], i)
		}

		bestGain, bestMove := int64(0), -1
		bestSwap := [2]int{-1, -1}
		var bestHi, bestLo int
		for hi := 0; hi < heavy && hi < len(order); hi++ {
			src := order[hi]
			for lo := 0; lo < light && lo < len(order); lo++ {
				dst := order[len(order)-1-lo]
				if dst == src || loads[dst] >= loads[src] {
					continue
				}
				gap := loads[src] - loads[dst]
				for _, i := range b[src] {
					if !containsInt(in.Items[i].Eligible, dst) {
						continue
					}
					w := in.Items[i].Weight
					if g := gap - abs64(gap-2*w); g > bestGain {
						bestGain, bestMove, bestSwap = g, i, [2]int{-1, -1}
						bestHi, bestLo = src, dst
					}
					for _, j := range b[dst] {
						if !containsInt(in.Items[j].Eligible, src) {
							continue
						}
						d := w - in.Items[j].Weight
						if d <= 0 {
							continue
						}
						if g := gap - abs64(gap-2*d); g > bestGain {
							bestGain, bestMove, bestSwap = g, -1, [2]int{i, j}
							bestHi, bestLo = src, dst
						}
					}
				}
			}
		}

		switch {
		case bestMove >= 0:
			w := in.Items[bestMove].Weight
			at[bestMove] = bestLo
			loads[bestHi] -= w
			loads[bestLo] += w
		case bestSwap[0] >= 0:
			i, j := bestSwap[0], bestSwap[1]
			d := in.Items[i].Weight - in.Items[j].Weight
			at[i], at[j] = bestLo, bestHi
			loads[bestHi] -= d
			loads[bestLo] += d
		default:
			return at
		}
	}
	return at
}

func abs64(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

// FlowThenShave does the chains first and the peak second: the flow reaches an
// arrangement single moves cannot, and the shave levels what the flow's weight
// classes leave behind.
func FlowThenShave(in *Instance, at []int) []int {
	return PeakShave(in, FlowOnlyRepair(in, at), 20000)
}
