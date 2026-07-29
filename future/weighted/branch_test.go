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

// TestExactTwoWorkersAtScale isolates which dimension is actually the barrier.
// Weights are unrelated and huge; only the worker count is small.
func TestExactTwoWorkersAtScale(t *testing.T) {
	for _, nitems := range []int{20, 100, 1000} {
		rng := rand.New(rand.NewSource(int64(nitems)))
		in := &Instance{Workers: 2}
		for range nitems {
			el := []int{0, 1}
			if r := rng.Intn(10); r == 0 {
				el = []int{0}
			} else if r == 1 {
				el = []int{1}
			}
			in.Items = append(in.Items, Item{Weight: int64(1 + rng.Intn(100000)), Eligible: el, Prior: -1})
		}
		s := make([]int, len(in.Items))
		for i := range in.Items {
			s[i] = in.Items[i].Eligible[0]
		}
		start := time.Now()
		opt, ok := ExactTwoWorkers(in)
		el := time.Since(start)
		local := in.Eval(RepairX(in, s, 3, 3)).Squares
		t.Logf("items=%4d workers=2 weights 1..100000 | exact=%v in %-10v | local search excess %+.9f%%",
			nitems, ok, el.Round(time.Microsecond), 100*float64(local-opt)/float64(opt))
	}
}

// TestPairwiseCertificate measures what surviving every pair is worth: how
// often an assembly that no pair can improve is in fact globally optimal.
func TestPairwiseCertificate(t *testing.T) {
	const trials = 3000
	for _, workers := range []int{3, 4, 5} {
		var certified, certifiedAndOptimal, optimal int
		for seed := range trials {
			rng := rand.New(rand.NewSource(int64(seed*13 + workers)))
			in := &Instance{Workers: workers}
			for range 8 {
				var el []int
				for w := range workers {
					if rng.Intn(100) < 70 {
						el = append(el, w)
					}
				}
				if len(el) == 0 {
					el = append(el, rng.Intn(workers))
				}
				in.Items = append(in.Items, Item{Weight: int64(1 + rng.Intn(1000)), Eligible: el, Prior: -1})
			}
			s := make([]int, len(in.Items))
			for i := range in.Items {
				s[i] = in.Items[i].Eligible[rng.Intn(len(in.Items[i].Eligible))]
			}
			got := RepairX(in, s, 3, 3)
			isOpt := in.Eval(got).Squares == in.Eval(BruteForce(in)).Squares
			cert, _ := PairwiseOptimal(in, got)
			if isOpt {
				optimal++
			}
			if cert {
				certified++
				if isOpt {
					certifiedAndOptimal++
				}
			}
		}
		f := float64(trials)
		var precision float64
		if certified > 0 {
			precision = 100 * float64(certifiedAndOptimal) / float64(certified)
		}
		t.Logf("workers=%d | local search optimal %6.2f%% | pair-certified %6.2f%% | of those, actually optimal %6.2f%%",
			workers, 100*float64(optimal)/f, 100*float64(certified)/f, precision)
	}
}
