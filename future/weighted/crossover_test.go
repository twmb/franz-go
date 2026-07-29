package weighted

import (
	"math/rand"
	"testing"
	"time"
)

// TestEligibilityCrossover finds where the two approaches change places.
//
// The whole margin comes from an item being unable to go wherever it likes. So
// the question for any real system is not which algorithm is better in the
// abstract but how restricted its placement actually is -- and that has a
// number. Here eligibility is swept from every store allowed down to a small
// window, holding everything else fixed.
func TestEligibilityCrossover(t *testing.T) {
	const nitems, nstores = 4000, 32
	for _, frac := range []float64{1.0, 0.75, 0.5, 0.33, 0.25, 0.15, 0.1} {
		width := max(2, int(float64(nstores)*frac))
		rng := rand.New(rand.NewSource(11))
		in := &Instance{Workers: nstores}
		start := make([]int, nitems)
		for i := range nitems {
			w := int64(64 + rng.Intn(64))
			if rng.Intn(5) == 0 {
				w = int64(1 + rng.Intn(64))
			}
			lo := rng.Intn(nstores - width + 1)
			var el []int
			for s := lo; s < lo+width; s++ {
				el = append(el, s)
			}
			// Skewed start, as a decommission or a burst of splits leaves.
			home := el[0]
			if rng.Intn(100) < 40 {
				home = el[rng.Intn(len(el))]
			}
			in.Items = append(in.Items, Item{Weight: w, Eligible: el, Prior: home})
			start[i] = home
		}

		s := time.Now()
		sm, _ := SingleMoveScheduler(in, start, 0)
		smTime := time.Since(s)
		s = time.Now()
		fr := FlowOnlyRepair(in, start)
		frTime := time.Since(s)
		s = time.Now()
		fp := FlowThenPolish(in, start)
		fpTime := time.Since(s)

		smImb, frImb, fpImb := liveImbalance(in, sm), liveImbalance(in, fr), liveImbalance(in, fp)
		verdict := "single-move"
		if fpImb < smImb*0.98 {
			verdict = "FLOW+POLISH WINS"
		} else if fpImb < smImb*1.02 {
			verdict = "tie"
		}
		t.Logf("allowed %4.0f%% (%2d/%d) | single-move %.4f/%-9v | flow %.4f/%-9v | flow+polish %.4f/%-9v | %s",
			100*frac, width, nstores,
			smImb, smTime.Round(time.Millisecond),
			frImb, frTime.Round(time.Millisecond),
			fpImb, fpTime.Round(time.Millisecond), verdict)
	}
}

// TestSteeperLoadCost asks whether a load price that rises faster closes the
// gap where every store is allowed, without giving up the gap where few are.
func TestSteeperLoadCost(t *testing.T) {
	const nitems, nstores = 4000, 32
	for _, frac := range []float64{1.0, 0.5, 0.25, 0.1} {
		width := max(2, int(float64(nstores)*frac))
		rng := rand.New(rand.NewSource(11))
		in := &Instance{Workers: nstores}
		start := make([]int, nitems)
		for i := range nitems {
			w := int64(64 + rng.Intn(64))
			if rng.Intn(5) == 0 {
				w = int64(1 + rng.Intn(64))
			}
			lo := rng.Intn(nstores - width + 1)
			var el []int
			for s := lo; s < lo+width; s++ {
				el = append(el, s)
			}
			home := el[0]
			if rng.Intn(100) < 40 {
				home = el[rng.Intn(len(el))]
			}
			in.Items = append(in.Items, Item{Weight: w, Eligible: el, Prior: home})
			start[i] = home
		}
		s0 := time.Now()
		sm, _ := SingleMoveScheduler(in, start, 0)
		smT := time.Since(s0)
		s0 = time.Now()
		fs := FlowThenShave(in, start)
		fsT := time.Since(s0)
		s0 = time.Now()
		sh := PeakShave(in, start, 20000)
		shT := time.Since(s0)
		t.Logf("allowed %4.0f%% | single-move %.4f/%-8v moves=%-5d | shave-only %.4f/%-8v moves=%-5d | flow+shave %.4f/%-8v moves=%-5d",
			100*frac,
			liveImbalance(in, sm), smT.Round(time.Millisecond), MovesFrom(start, sm),
			liveImbalance(in, sh), shT.Round(time.Millisecond), MovesFrom(start, sh),
			liveImbalance(in, fs), fsT.Round(time.Millisecond), MovesFrom(start, fs))
	}
}
