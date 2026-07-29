package weighted

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// fairCluster builds the case that separates the two approaches, rather than
// the case where anything works.
//
// A single move can always fix imbalance when every item may go anywhere: take
// from the fullest, give to the emptiest. It cannot fix imbalance when what the
// fullest store holds is only allowed on stores that are themselves full, while
// those stores hold something that could move on. That needs two moves decided
// together, and a scheduler that accepts a move only if it improves things by
// itself will refuse both halves of it.
//
// Every item starts where it is, and that is the baseline stickiness is
// measured against -- not some earlier state.
func fairCluster(rng *rand.Rand, nitems, nstores, ngroups int) (*Instance, []int) {
	in := &Instance{Workers: nstores}
	start := make([]int, nitems)

	// Stores fall into overlapping groups, as they do when disk class,
	// capacity, or locality restricts where something may live. Group g
	// spans a window of stores, and windows overlap their neighbours, so
	// weight can travel between groups only by passing through the overlap.
	width := max(2, nstores/ngroups+1)
	groups := make([][]int, ngroups)
	for g := range ngroups {
		lo := g * (nstores - width) / max(1, ngroups-1)
		for s := lo; s < lo+width && s < nstores; s++ {
			groups[g] = append(groups[g], s)
		}
	}

	for i := range nitems {
		w := int64(64 + rng.Intn(64))
		if rng.Intn(5) == 0 {
			w = int64(1 + rng.Intn(64))
		}
		g := rng.Intn(ngroups)
		el := groups[g]
		// Start skewed: everything piles onto the first store of its
		// group, which is what a decommission or a fresh split leaves.
		home := el[0]
		if rng.Intn(100) < 40 {
			home = el[rng.Intn(len(el))]
		}
		in.Items = append(in.Items, Item{Weight: w, Eligible: el, Prior: home})
		start[i] = home
	}
	return in, start
}

func liveImbalance(in *Instance, at []int) float64 {
	loads := make([]int64, in.Workers)
	used := make([]bool, in.Workers)
	var total int64
	for i, w := range at {
		loads[w] += in.Items[i].Weight
		total += in.Items[i].Weight
	}
	// A store nobody may use is not part of the balance question.
	for i := range in.Items {
		for _, e := range in.Items[i].Eligible {
			used[e] = true
		}
	}
	lo, hi, n := int64(1)<<62, int64(0), 0
	for w := range in.Workers {
		if !used[w] {
			continue
		}
		lo = min(lo, loads[w])
		hi = max(hi, loads[w])
		n++
	}
	if n == 0 {
		return 0
	}
	return float64(hi-lo) / (float64(total) / float64(n))
}

func TestFairHeadToHead(t *testing.T) {
	for _, tc := range []struct{ items, stores, groups int }{
		{500, 12, 4}, {2000, 24, 6}, {8000, 48, 8},
	} {
		rng := rand.New(rand.NewSource(int64(tc.items)))
		in, start := fairCluster(rng, tc.items, tc.stores, tc.groups)
		var avgItem int64
		for i := range in.Items {
			avgItem += in.Items[i].Weight
		}
		avgItem /= int64(len(in.Items))

		type row struct {
			name    string
			imb     float64
			moves   int
			squares int64
			d       time.Duration
		}
		var rows []row
		for _, mult := range []int64{0, 1} {
			s := time.Now()
			got, _ := SingleMoveScheduler(in, start, mult*avgItem)
			rows = append(rows, row{fmt.Sprintf("single-move, tol=%dx", mult),
				liveImbalance(in, got), MovesFrom(start, got), in.Eval(got).Squares, time.Since(s)})
		}
		s := time.Now()
		got := FlowOnlyRepair(in, start)
		rows = append(rows, row{"priced repair (flow)", liveImbalance(in, got), MovesFrom(start, got), in.Eval(got).Squares, time.Since(s)})

		t.Logf("=== %d items, %d stores, %d overlapping eligibility groups ===", tc.items, tc.stores, tc.groups)
		t.Logf("  %-24s %-11s %-8s %-16s %s", "algorithm", "imbalance", "moves", "sum squares", "time")
		t.Logf("  %-24s %-11.4f %-8d %-16d -", "starting state", liveImbalance(in, start), 0, in.Eval(start).Squares)
		for _, r := range rows {
			t.Logf("  %-24s %-11.4f %-8d %-16d %v", r.name, r.imb, r.moves, r.squares, r.d.Round(time.Microsecond))
		}
	}
}
