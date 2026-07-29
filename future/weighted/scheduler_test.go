package weighted

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"
	"time"
)

// cluster builds a placement workload: many items of uneven size over stores,
// with a store added or removed so there is real work to do, and locality
// restricting where some items may live.
func cluster(rng *rand.Rand, nitems, nstores int, churn bool) (*Instance, []int) {
	nzones := 3
	in := &Instance{Workers: nstores}
	for i := range nitems {
		// Sizes cluster near a maximum, as ranges do after splitting,
		// with a tail of small ones.
		w := int64(64 + rng.Intn(64))
		if rng.Intn(5) == 0 {
			w = int64(1 + rng.Intn(64))
		}
		var el []int
		// Two thirds of items may live anywhere; the rest are pinned to
		// a zone, which is what locality constraints do.
		if rng.Intn(3) == 0 {
			zone := rng.Intn(nzones)
			for s := range nstores {
				if s%nzones == zone {
					el = append(el, s)
				}
			}
		} else {
			for s := range nstores {
				el = append(el, s)
			}
		}
		prior := el[rng.Intn(len(el))]
		_ = i
		in.Items = append(in.Items, Item{Weight: w, Eligible: el, Prior: prior})
	}
	start := make([]int, nitems)
	for i := range in.Items {
		start[i] = in.Items[i].Prior
	}
	if churn {
		// A store goes away: everything on the last store is displaced
		// onto the first one that will take it, which is the state a
		// rebalance actually starts from.
		gone := nstores - 1
		for i := range in.Items {
			if start[i] == gone {
				for _, e := range in.Items[i].Eligible {
					if e != gone {
						start[i] = e
						break
					}
				}
			}
		}
		for i := range in.Items {
			in.Items[i].Eligible = slices.DeleteFunc(slices.Clone(in.Items[i].Eligible), func(e int) bool { return e == gone })
			if len(in.Items[i].Eligible) == 0 {
				in.Items[i].Eligible = []int{0}
				start[i] = 0
			}
			if in.Items[i].Prior == gone {
				in.Items[i].Prior = -1
			}
		}
	}
	return in, start
}

func TestSchedulerHeadToHead(t *testing.T) {
	for _, tc := range []struct{ items, stores int }{
		{500, 12}, {2000, 24}, {8000, 48},
	} {
		rng := rand.New(rand.NewSource(int64(tc.items)))
		in, start := cluster(rng, tc.items, tc.stores, true)

		var avgItem int64
		for i := range in.Items {
			avgItem += in.Items[i].Weight
		}
		avgItem /= int64(len(in.Items))

		type result struct {
			name     string
			imb      float64
			moves    int
			squares  int64
			duration time.Duration
		}
		var results []result

		// The shipped shape, at a few tolerances. Tolerance is in load
		// units; theirs is a multiple of the average item size.
		for _, mult := range []int64{0, 1, 2} {
			s := time.Now()
			got, _ := SingleMoveScheduler(in, start, mult*avgItem)
			d := time.Since(s)
			results = append(results, result{
				fmt.Sprintf("single-move, tolerance=%dx avg item", mult),
				Imbalance(in, got), MovesFrom(start, got), in.Eval(got).Squares, d,
			})
		}

		s := time.Now()
		got := FlowOnlyRepair(in, start)
		d := time.Since(s)
		results = append(results, result{"priced repair, flow only", Imbalance(in, got), MovesFrom(start, got), in.Eval(got).Squares, d})

		t.Logf("=== %d items over %d stores, one store removed ===", tc.items, tc.stores)
		t.Logf("  %-34s %-12s %-9s %-14s %s", "algorithm", "imbalance", "moves", "sum squares", "time")
		t.Logf("  %-34s %-12.4f %-9d %-14d %v", "starting state", Imbalance(in, start), 0, in.Eval(start).Squares, time.Duration(0))
		for _, r := range results {
			t.Logf("  %-34s %-12.4f %-9d %-14d %v", r.name, r.imb, r.moves, r.squares, r.duration.Round(time.Microsecond))
		}
	}
}
