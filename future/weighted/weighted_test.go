package weighted

import (
	"fmt"
	"math/rand"
	"testing"
)

// gen builds a random instance with exactly nweights distinct item weights.
func gen(rng *rand.Rand, nitems, nworkers, nweights int, priors bool) *Instance {
	return genPatterns(rng, nitems, nworkers, nweights, 0, priors)
}

// genPatterns builds an instance with npatterns distinct eligibility sets. Zero
// means every item gets its own random set, which is the pathological case: no
// two items are interchangeable and the table cannot collapse at all.
func genPatterns(rng *rand.Rand, nitems, nworkers, nweights, npatterns int, priors bool) *Instance {
	var patterns [][]int
	for range npatterns {
		var el []int
		for w := range nworkers {
			if rng.Intn(100) < 70 {
				el = append(el, w)
			}
		}
		if len(el) == 0 {
			el = append(el, rng.Intn(nworkers))
		}
		patterns = append(patterns, el)
	}
	weights := make([]int64, nweights)
	for i := range weights {
		weights[i] = int64(1 + rng.Intn(6))
	}
	in := &Instance{Workers: nworkers}
	for range nitems {
		var el []int
		if npatterns > 0 {
			el = patterns[rng.Intn(npatterns)]
		} else {
			for w := range nworkers {
				if rng.Intn(100) < 70 {
					el = append(el, w)
				}
			}
			if len(el) == 0 {
				el = append(el, rng.Intn(nworkers))
			}
		}
		prior := -1
		if priors && rng.Intn(100) < 70 {
			prior = el[rng.Intn(len(el))]
		}
		in.Items = append(in.Items, Item{
			Weight:   weights[rng.Intn(nweights)],
			Eligible: el,
			Prior:    prior,
		})
	}
	return in
}

// start is the assignment the repair is handed: each item with its prior owner
// where it has one, else anywhere eligible. Deliberately unbalanced, which is
// what a real rebalance sees after membership changes.
func start(in *Instance, rng *rand.Rand) []int {
	at := make([]int, len(in.Items))
	for i := range in.Items {
		if p := in.Items[i].Prior; p >= 0 {
			at[i] = p
			continue
		}
		at[i] = in.Items[i].Eligible[rng.Intn(len(in.Items[i].Eligible))]
	}
	return at
}

// TestGapVersusDistinctWeights is the experiment: how much exactness is lost as
// the number of distinct weights grows, for the weighted repair versus the
// single-move local search most systems ship.
func TestGapVersusDistinctWeights(t *testing.T) {
	const trials = 3000
	for _, nweights := range []int{1, 2, 3, 5} {
		var repairOpt, greedyOpt, n int
		var repairWorseBal, greedyWorseBal int
		var repairSq, greedySq, repairMv, greedyMv float64
		for seed := range trials {
			rng := rand.New(rand.NewSource(int64(seed*97 + nweights)))
			in := gen(rng, 7, 3, nweights, true)
			s := start(in, rng)

			opt := in.Eval(BruteForce(in))
			rep := in.Eval(Repair(in, s))
			gre := in.Eval(Greedy(in))

			n++
			if rep == opt {
				repairOpt++
			} else if rep.Squares > opt.Squares {
				repairWorseBal++
			}
			if gre == opt {
				greedyOpt++
			} else if gre.Squares > opt.Squares {
				greedyWorseBal++
			}
			repairSq += float64(rep.Squares-opt.Squares) / float64(opt.Squares)
			greedySq += float64(gre.Squares-opt.Squares) / float64(opt.Squares)
			repairMv += float64(rep.Moves - opt.Moves)
			greedyMv += float64(gre.Moves - opt.Moves)
			if opt.Less(rep) == false && rep.Less(opt) {
				t.Fatalf("repair beat brute force, impossible: opt=%v rep=%v", opt, rep)
			}
		}
		f := float64(n)
		t.Logf("weights=%d | repair: optimal %5.1f%%  balance-subopt %4.1f%%  excess squares %+5.3f%%  excess moves %+.3f | greedy+1move: optimal %5.1f%%  balance-subopt %4.1f%%  excess squares %+5.3f%%  excess moves %+.3f",
			nweights,
			100*float64(repairOpt)/f, 100*float64(repairWorseBal)/f, 100*repairSq/f, repairMv/f,
			100*float64(greedyOpt)/f, 100*float64(greedyWorseBal)/f, 100*greedySq/f, greedyMv/f)
	}
}

// TestUnitWeightStaysExact is the control: at one distinct weight the problem is
// the one franz-go actually solves, and the repair must be exactly optimal.
func TestUnitWeightStaysExact(t *testing.T) {
	const trials = 20000
	var bad int
	for seed := range trials {
		rng := rand.New(rand.NewSource(int64(seed)))
		in := gen(rng, 7, 3, 1, true)
		for i := range in.Items {
			in.Items[i].Weight = 1
		}
		s := start(in, rng)
		opt, rep := in.Eval(BruteForce(in)), in.Eval(Repair(in, s))
		if rep != opt {
			if bad < 5 {
				t.Errorf("seed %d: unit weights not optimal: opt=%v repair=%v", seed, opt, rep)
			}
			bad++
		}
	}
	if bad > 0 {
		t.Fatalf("%d/%d unit-weight instances suboptimal", bad, trials)
	}
	t.Logf("%d unit-weight instances, all exactly optimal", trials)
}

// TestBalanceNeverWorsened checks the ranking holds: the repair must never
// trade balance away for stickiness, whatever the weights.
func TestBalanceNeverWorsened(t *testing.T) {
	const trials = 5000
	for seed := range trials {
		rng := rand.New(rand.NewSource(int64(seed * 31)))
		in := gen(rng, 8, 4, 3, true)
		s := start(in, rng)
		before, after := in.Eval(s), in.Eval(Repair(in, s))
		if after.Squares > before.Squares {
			t.Fatalf("seed %d: repair worsened balance %v => %v", seed, before, after)
		}
	}
}

// TestRowCollapse measures the thing that makes the unit case affordable, and
// whether it survives weights: rows are (eligibility, weight) pairs, so adding
// weights multiplies the table.
func TestRowCollapse(t *testing.T) {
	rng := rand.New(rand.NewSource(1))
	t.Log("realistic: a handful of distinct eligibility patterns")
	for _, nweights := range []int{1, 2, 4, 8} {
		in := genPatterns(rng, 4000, 40, nweights, 3, false)
		tbl := newTable(in)
		t.Logf("  items=%d workers=%d patterns=3 weights=%d => rows=%d cells=%d",
			len(in.Items), in.Workers, nweights, len(tbl.rows), len(tbl.cells))
	}
	t.Log("pathological: every item its own eligibility set, nothing collapses")
	for _, nweights := range []int{1, 8} {
		in := genPatterns(rng, 4000, 40, nweights, 0, false)
		tbl := newTable(in)
		t.Logf("  items=%d workers=%d patterns=per-item weights=%d => rows=%d cells=%d",
			len(in.Items), in.Workers, nweights, len(tbl.rows), len(tbl.cells))
	}
}

func ExampleCost() {
	fmt.Println(Cost{10, 2})
	// Output: squares=10 moves=2
}

// TestLongerCrossWeightRotations asks whether the gap left at multiple weights
// is the theory or just too small a move set.
func TestLongerCrossWeightRotations(t *testing.T) {
	const trials = 3000
	for _, nweights := range []int{2, 3, 5} {
		var opt2, opt3, opt4, n int
		var sq2, sq3, sq4 float64
		for seed := range trials {
			rng := rand.New(rand.NewSource(int64(seed*97 + nweights)))
			in := gen(rng, 7, 3, nweights, true)
			s := start(in, rng)
			opt := in.Eval(BruteForce(in))
			for _, tc := range []struct {
				k   int
				hit *int
				sq  *float64
			}{{2, &opt2, &sq2}, {3, &opt3, &sq3}, {4, &opt4, &sq4}} {
				got := in.Eval(RepairK(in, s, tc.k))
				if got == opt {
					*tc.hit++
				}
				*tc.sq += float64(got.Squares-opt.Squares) / float64(opt.Squares)
				if got.Less(opt) {
					t.Fatalf("beat brute force, impossible")
				}
			}
			n++
		}
		f := float64(n)
		t.Logf("weights=%d | 2-swap %5.1f%% (+%.3f%%) | +3-rotations %5.1f%% (+%.3f%%) | +4-rotations %5.1f%% (+%.3f%%)",
			nweights, 100*float64(opt2)/f, 100*sq2/f, 100*float64(opt3)/f, 100*sq3/f, 100*float64(opt4)/f, 100*sq4/f)
	}
}

// TestFractionalBoundTightness asks how much a certificate is worth: the bound
// is computable at any size, the true optimum is not.
func TestFractionalBoundTightness(t *testing.T) {
	const trials = 3000
	for _, nweights := range []int{1, 2, 3, 5} {
		var n, boundEqOpt int
		var boundGap, trueGap, provenGap float64
		for seed := range trials {
			rng := rand.New(rand.NewSource(int64(seed*97 + nweights)))
			in := gen(rng, 7, 3, nweights, true)
			s := start(in, rng)
			opt := in.Eval(BruteForce(in)).Squares
			got := in.Eval(RepairK(in, s, 3)).Squares
			lb := FractionalBound(in)
			if lb > opt {
				t.Fatalf("bound %d exceeds true optimum %d, not a bound", lb, opt)
			}
			if lb == opt {
				boundEqOpt++
			}
			boundGap += float64(opt-lb) / float64(opt) // how loose the bound is
			trueGap += float64(got-opt) / float64(opt) // how far we really are
			provenGap += float64(got-lb) / float64(lb) // what we can prove
			n++
		}
		f := float64(n)
		t.Logf("weights=%d | bound is exact in %5.1f%% | bound looseness %+.3f%% | true gap %+.3f%% | provable gap %+.3f%%",
			nweights, 100*float64(boundEqOpt)/f, 100*boundGap/f, 100*trueGap/f, 100*provenGap/f)
	}
}
