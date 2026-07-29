package weighted

import (
	"math"
	"math/rand"
	"testing"
)

// TestDivisibilityRestoresExactness tests the structural hypothesis: the unit
// case is exact because load is a count, and counts can be exchanged freely --
// any item for any other. Arbitrary weights destroy that. Divisible weights,
// where each divides the next, should restore it, because one item of a size is
// exactly exchangeable for several of the next size down.
//
// If that holds, arbitrary weights are the wrong thing to fight: quantizing
// onto a divisible chain buys back exactness, and costs only the quantization.
func TestDivisibilityRestoresExactness(t *testing.T) {
	sets := []struct {
		name string
		w    []int64
	}{
		{"arbitrary {3,7,11}", []int64{3, 7, 11}},
		{"arbitrary {5,9,13}", []int64{5, 9, 13}},
		{"divisible {1,2,4}", []int64{1, 2, 4}},
		{"divisible {2,4,8}", []int64{2, 4, 8}},
		{"divisible {1,3,9}", []int64{1, 3, 9}},
		{"divisible {3,6,12}", []int64{3, 6, 12}},
		{"near-divisible {1,2,5}", []int64{1, 2, 5}},
		{"near-divisible {1,3,8}", []int64{1, 3, 8}},
		{"uniform {4,4,4}", []int64{4, 4, 4}},
	}
	const nitems, nworkers, trials = 9, 3, 4000
	for _, set := range sets {
		var repairExact, greedyExact int
		var excess float64
		for seed := range trials {
			rng := rand.New(rand.NewSource(int64(seed)))
			in := &Instance{Workers: nworkers}
			all := make([]int, nworkers)
			for w := range nworkers {
				all[w] = w
			}
			for range nitems {
				in.Items = append(in.Items, Item{
					Weight:   set.w[rng.Intn(len(set.w))],
					Eligible: all,
					Prior:    -1,
				})
			}
			opt := in.Eval(BruteForce(in)).Squares
			rep := in.Eval(RepairK(in, Greedy(in), 3)).Squares
			gre := in.Eval(Greedy(in)).Squares
			if rep == opt {
				repairExact++
			}
			if gre == opt {
				greedyExact++
			}
			excess += float64(rep-opt) / float64(opt)
		}
		f := float64(trials)
		t.Logf("%-22s | repair exact %6.2f%% | greedy exact %6.2f%% | repair excess %+.4f%%",
			set.name, 100*float64(repairExact)/f, 100*float64(greedyExact)/f, 100*excess/f)
	}
}

// TestDivisibilityWithStickiness is the real test: priors present, so both
// objectives are live, and eligibility is restricted, so it is not the
// textbook partitioning problem. If divisibility only bought balance, it would
// show up here as balance exact but the pair not.
func TestDivisibilityWithStickiness(t *testing.T) {
	sets := []struct {
		name string
		w    []int64
	}{
		{"arbitrary {3,7,11}", []int64{3, 7, 11}},
		{"arbitrary {5,9,13}", []int64{5, 9, 13}},
		{"divisible {1,2,4}", []int64{1, 2, 4}},
		{"divisible {2,4,8}", []int64{2, 4, 8}},
		{"divisible {1,3,9}", []int64{1, 3, 9}},
		{"uniform {4,4,4}", []int64{4, 4, 4}},
	}
	const nitems, nworkers, trials = 8, 3, 4000
	for _, set := range sets {
		var bothExact, balExact int
		for seed := range trials {
			rng := rand.New(rand.NewSource(int64(seed * 13)))
			in := &Instance{Workers: nworkers}
			for range nitems {
				var el []int
				for w := range nworkers {
					if rng.Intn(100) < 70 {
						el = append(el, w)
					}
				}
				if len(el) == 0 {
					el = append(el, rng.Intn(nworkers))
				}
				prior := -1
				if rng.Intn(100) < 70 {
					prior = el[rng.Intn(len(el))]
				}
				in.Items = append(in.Items, Item{
					Weight:   set.w[rng.Intn(len(set.w))],
					Eligible: el,
					Prior:    prior,
				})
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
			got := in.Eval(RepairK(in, s, 3))
			if got == opt {
				bothExact++
			}
			if got.Squares == opt.Squares {
				balExact++
			}
		}
		f := float64(trials)
		t.Logf("%-20s | balance+stickiness exact %6.2f%% | balance exact %6.2f%%",
			set.name, 100*float64(bothExact)/f, 100*float64(balExact)/f)
	}
}

// TestOneForManyExchange checks whether the move divisibility implies is what
// was missing.
func TestOneForManyExchange(t *testing.T) {
	sets := []struct {
		name string
		w    []int64
	}{
		{"arbitrary {3,7,11}", []int64{3, 7, 11}},
		{"divisible {1,2,4}", []int64{1, 2, 4}},
		{"divisible {2,4,8}", []int64{2, 4, 8}},
		{"divisible {1,3,9}", []int64{1, 3, 9}},
		{"divisible {1,2,4,8}", []int64{1, 2, 4, 8}},
	}
	const nitems, nworkers, trials = 8, 3, 3000
	for _, set := range sets {
		var rotOnly, withX, balX int
		for seed := range trials {
			rng := rand.New(rand.NewSource(int64(seed * 13)))
			in := &Instance{Workers: nworkers}
			for range nitems {
				var el []int
				for w := range nworkers {
					if rng.Intn(100) < 70 {
						el = append(el, w)
					}
				}
				if len(el) == 0 {
					el = append(el, rng.Intn(nworkers))
				}
				prior := -1
				if rng.Intn(100) < 70 {
					prior = el[rng.Intn(len(el))]
				}
				in.Items = append(in.Items, Item{Weight: set.w[rng.Intn(len(set.w))], Eligible: el, Prior: prior})
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
			if in.Eval(RepairK(in, s, 3)) == opt {
				rotOnly++
			}
			gotX := in.Eval(RepairX(in, s, 3, 3))
			if gotX == opt {
				withX++
			}
			if gotX.Squares == opt.Squares {
				balX++
			}
		}
		f := float64(trials)
		t.Logf("%-20s | rotations only %6.2f%% | + one-for-many %6.2f%% | balance alone %6.2f%%",
			set.name, 100*float64(rotOnly)/f, 100*float64(withX)/f, 100*float64(balX)/f)
	}
}

// TestScaleDecomposition asks whether committing large items before small ones
// can be regretted, which is what would make the divisible case exactly
// solvable by a sequence of unit-weight solves.
func TestScaleDecomposition(t *testing.T) {
	sets := []struct {
		name string
		w    []int64
	}{
		{"arbitrary {3,7,11}", []int64{3, 7, 11}},
		{"divisible {1,2,4}", []int64{1, 2, 4}},
		{"divisible {2,4,8}", []int64{2, 4, 8}},
		{"divisible {1,3,9}", []int64{1, 3, 9}},
		{"divisible {1,2,4,8}", []int64{1, 2, 4, 8}},
	}
	const nitems, nworkers, trials = 8, 3, 3000
	for _, set := range sets {
		var both, bal int
		for seed := range trials {
			rng := rand.New(rand.NewSource(int64(seed * 13)))
			in := &Instance{Workers: nworkers}
			for range nitems {
				var el []int
				for w := range nworkers {
					if rng.Intn(100) < 70 {
						el = append(el, w)
					}
				}
				if len(el) == 0 {
					el = append(el, rng.Intn(nworkers))
				}
				prior := -1
				if rng.Intn(100) < 70 {
					prior = el[rng.Intn(len(el))]
				}
				in.Items = append(in.Items, Item{Weight: set.w[rng.Intn(len(set.w))], Eligible: el, Prior: prior})
			}
			opt := in.Eval(BruteForce(in))
			got := in.Eval(ScaleDecompose(in))
			if got == opt {
				both++
			}
			if got.Squares == opt.Squares {
				bal++
			}
		}
		f := float64(trials)
		t.Logf("%-20s | scale-decompose balance exact %6.2f%% | balance+stickiness %6.2f%%",
			set.name, 100*float64(bal)/f, 100*float64(both)/f)
	}
}

// TestLaminarEligibility isolates the interaction. Divisibility fixes weights
// when every worker is eligible for everything; the flow fixes eligibility when
// every item weighs the same. Neither fixes them together -- so the question is
// whether eligibility that is itself structured behaves better than arbitrary
// eligibility.
//
// Laminar means the eligible sets are nested or disjoint, never partially
// overlapping: the shape a hierarchy produces. It is what real deployments
// have -- region containing zone containing rack, or a subscription pattern
// matching a prefix of another. Arbitrary overlap is the thing that does not
// occur naturally.
func TestLaminarEligibility(t *testing.T) {
	const nworkers = 6
	// A three-level hierarchy over six workers: everything, two halves,
	// three pairs. Every pair of these sets is nested or disjoint.
	laminar := [][]int{
		{0, 1, 2, 3, 4, 5},
		{0, 1, 2}, {3, 4, 5},
		{0, 1}, {2, 3}, {4, 5},
	}
	sets := []struct {
		name string
		w    []int64
	}{
		{"arbitrary {3,7,11}", []int64{3, 7, 11}},
		{"divisible {1,2,4}", []int64{1, 2, 4}},
		{"divisible {2,4,8}", []int64{2, 4, 8}},
		{"uniform {4,4,4}", []int64{4, 4, 4}},
	}
	const nitems, trials = 8, 3000
	for _, laminarOn := range []bool{true, false} {
		for _, set := range sets {
			var both, bal int
			for seed := range trials {
				rng := rand.New(rand.NewSource(int64(seed * 13)))
				in := &Instance{Workers: nworkers}
				for range nitems {
					var el []int
					if laminarOn {
						el = laminar[rng.Intn(len(laminar))]
					} else {
						for w := range nworkers {
							if rng.Intn(100) < 50 {
								el = append(el, w)
							}
						}
						if len(el) == 0 {
							el = append(el, rng.Intn(nworkers))
						}
					}
					prior := -1
					if rng.Intn(100) < 70 {
						prior = el[rng.Intn(len(el))]
					}
					in.Items = append(in.Items, Item{Weight: set.w[rng.Intn(len(set.w))], Eligible: el, Prior: prior})
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
				got := in.Eval(RepairX(in, s, 3, 3))
				if got == opt {
					both++
				}
				if got.Squares == opt.Squares {
					bal++
				}
			}
			f := float64(trials)
			kind := "laminar   "
			if !laminarOn {
				kind = "arbitrary "
			}
			t.Logf("%seligibility | %-20s | balance exact %6.2f%% | balance+stickiness %6.2f%%",
				kind, set.name, 100*float64(bal)/f, 100*float64(both)/f)
		}
	}
}

// TestQuantizeToDivisible is the decisive test for the whole divisible line.
// Divisibility is a property real weights do not have, so it is only worth
// anything if arbitrary weights can be moved onto a divisible chain, solved
// exactly there, and the answer carried back.
//
// The catch is arithmetic: consecutive members of an integer divisible chain
// differ by a factor of at least two, so rounding to the nearest one can be off
// by up to about 41% per item. Local search on the true weights is off by about
// a tenth of a percent. The transformation has to be worth that trade.
func TestQuantizeToDivisible(t *testing.T) {
	pow2 := func(w int64) int64 {
		best, bestErr := int64(1), math.Inf(1)
		for p := int64(1); p <= 1<<20; p *= 2 {
			if e := math.Abs(math.Log2(float64(w) / float64(p))); e < bestErr {
				best, bestErr = p, e
			}
		}
		return best
	}
	const nitems, nworkers, trials = 8, 3, 3000
	for _, span := range []int{8, 40, 200} {
		var directExact, quantExact int
		var directExcess, quantExcess float64
		for seed := range trials {
			rng := rand.New(rand.NewSource(int64(seed*13 + span)))
			in := &Instance{Workers: nworkers}
			for range nitems {
				var el []int
				for w := range nworkers {
					if rng.Intn(100) < 70 {
						el = append(el, w)
					}
				}
				if len(el) == 0 {
					el = append(el, rng.Intn(nworkers))
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

			// Arm A: work on the true weights, accept inexactness.
			direct := in.Eval(RepairX(in, s, 3, 3))

			// Arm B: move onto a divisible chain, solve there exactly,
			// carry the assignment back and score it truthfully.
			q := &Instance{Workers: in.Workers}
			for i := range in.Items {
				it := in.Items[i]
				it.Weight = pow2(it.Weight)
				q.Items = append(q.Items, it)
			}
			quant := in.Eval(BruteForce(q))

			if direct == opt {
				directExact++
			}
			if quant == opt {
				quantExact++
			}
			directExcess += float64(direct.Squares-opt.Squares) / float64(opt.Squares)
			quantExcess += float64(quant.Squares-opt.Squares) / float64(opt.Squares)
		}
		f := float64(trials)
		t.Logf("weights 1..%-3d | direct on true weights: exact %6.2f%% excess %+7.4f%% | quantize+solve exactly: exact %6.2f%% excess %+7.4f%%",
			span,
			100*float64(directExact)/f, 100*directExcess/f,
			100*float64(quantExact)/f, 100*quantExcess/f)
	}
}
