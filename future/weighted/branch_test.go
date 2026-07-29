package weighted

import (
	"math/rand"
	"testing"
	"time"
)

// TestBranchAndBoundExactness asks whether search that can prove a branch
// worthless closes what local search leaves open, on weights with nothing in
// common.
func TestBranchAndBoundExactness(t *testing.T) {
	const trials = 2000
	for _, span := range []int{8, 40, 200, 5000} {
		var localExact, bbExact, bbProved, totalNodes int
		for seed := range trials {
			rng := rand.New(rand.NewSource(int64(seed*13 + span)))
			in := &Instance{Workers: 3}
			for range 8 {
				var el []int
				for w := range 3 {
					if rng.Intn(100) < 70 {
						el = append(el, w)
					}
				}
				if len(el) == 0 {
					el = append(el, rng.Intn(3))
				}
				prior := -1
				if rng.Intn(100) < 70 {
					prior = el[rng.Intn(len(el))]
				}
				in.Items = append(in.Items, Item{Weight: int64(1 + rng.Intn(span)), Eligible: el, Prior: prior})
			}
			s := make([]int, len(in.Items))
			for i := range in.Items {
				if p := in.Items[i].Prior; p >= 0 {
					s[i] = p
				} else {
					s[i] = in.Items[i].Eligible[rng.Intn(len(in.Items[i].Eligible))]
				}
			}
			opt := in.Eval(BruteForce(in))
			local := RepairX(in, s, 3, 3)
			if in.Eval(local) == opt {
				localExact++
			}
			bb, proved, nodes := BranchAndBound(in, local, 1_000_000)
			totalNodes += nodes
			if proved {
				bbProved++
			}
			if in.Eval(bb) == opt {
				bbExact++
			}
		}
		f := float64(trials)
		t.Logf("weights 1..%-4d | local search exact %6.2f%% | branch&bound exact %6.2f%% proved %6.2f%% | avg nodes %d",
			span, 100*float64(localExact)/f, 100*float64(bbExact)/f, 100*float64(bbProved)/f, totalNodes/trials)
	}
}

// TestBranchAndBoundScale asks how far proof reaches on unrelated weights.
func TestBranchAndBoundScale(t *testing.T) {
	for _, tc := range []struct{ items, workers int }{
		{10, 3}, {20, 4}, {40, 6}, {80, 8}, {150, 10}, {300, 12},
	} {
		rng := rand.New(rand.NewSource(int64(tc.items)))
		in := &Instance{Workers: tc.workers}
		for range tc.items {
			var el []int
			for w := range tc.workers {
				if rng.Intn(100) < 70 {
					el = append(el, w)
				}
			}
			if len(el) == 0 {
				el = append(el, rng.Intn(tc.workers))
			}
			prior := -1
			if rng.Intn(100) < 70 {
				prior = el[rng.Intn(len(el))]
			}
			in.Items = append(in.Items, Item{Weight: int64(1 + rng.Intn(100000)), Eligible: el, Prior: prior})
		}
		s := make([]int, len(in.Items))
		for i := range in.Items {
			if p := in.Items[i].Prior; p >= 0 {
				s[i] = p
			} else {
				s[i] = in.Items[i].Eligible[rng.Intn(len(in.Items[i].Eligible))]
			}
		}
		local := RepairX(in, s, 3, 3)
		start := time.Now()
		bb, proved, nodes := BranchAndBound(in, local, 20_000_000)
		el := time.Since(start)
		improved := in.Eval(local).Squares - in.Eval(bb).Squares
		t.Logf("items=%3d workers=%2d weights 1..100000 | proved optimal=%-5v in %-11v nodes=%-10d | improved on local search by %d squared units",
			tc.items, tc.workers, proved, el.Round(time.Millisecond), nodes, improved)
	}
}
