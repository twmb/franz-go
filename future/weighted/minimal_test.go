package weighted

import (
	"fmt"
	"testing"
)

// TestMinimalCounterexample searches for the smallest instance where the repair
// misses the optimum, with weights {1,2} and restricted eligibility -- one step
// from the case that is exact. Aggregate percentages say a barrier exists;
// only a specific instance says what it is.
func TestMinimalCounterexample(t *testing.T) {
	type found struct {
		in       *Instance
		start    []int
		got, opt Cost
	}
	var best *found

	// Enumerate every instance of a given size: each item is weight 1 or 2,
	// with a nonempty eligible set, and starts on some eligible worker.
	for nitems := 2; nitems <= 5 && best == nil; nitems++ {
		for nworkers := 2; nworkers <= 3 && best == nil; nworkers++ {
			elig := [][]int{}
			for mask := 1; mask < 1<<nworkers; mask++ {
				var e []int
				for w := range nworkers {
					if mask&(1<<w) != 0 {
						e = append(e, w)
					}
				}
				elig = append(elig, e)
			}
			// Each item: (weight index, eligibility index, prior index).
			type spec struct{ w, e, p int }
			specs := []spec{}
			for wi := range 2 {
				for ei := range elig {
					for p := range len(elig[ei]) + 1 {
						specs = append(specs, spec{wi, ei, p})
					}
				}
			}
			idx := make([]int, nitems)
			var rec func(k int) bool
			rec = func(k int) bool {
				if k == nitems {
					in := &Instance{Workers: nworkers}
					for _, si := range idx {
						s := specs[si]
						prior := -1
						if s.p < len(elig[s.e]) {
							prior = elig[s.e][s.p]
						}
						in.Items = append(in.Items, Item{
							Weight:   int64(s.w + 1),
							Eligible: elig[s.e],
							Prior:    prior,
						})
					}
					st := make([]int, nitems)
					for i := range in.Items {
						if p := in.Items[i].Prior; p >= 0 {
							st[i] = p
						} else {
							st[i] = in.Items[i].Eligible[0]
						}
					}
					opt := in.Eval(BruteForce(in))
					got := in.Eval(RepairX(in, st, 4, 4))
					if got != opt {
						best = &found{in, st, got, opt}
						return true
					}
					return false
				}
				for i := range specs {
					idx[k] = i
					if rec(k + 1) {
						return true
					}
				}
				return false
			}
			rec(0)
		}
	}

	if best == nil {
		t.Log("no counterexample at these sizes -- the repair is exact for weights {1,2} up to 5 items / 3 workers")
		return
	}

	in := best.in
	t.Logf("MINIMAL COUNTEREXAMPLE: %d items, %d workers", len(in.Items), in.Workers)
	for i := range in.Items {
		t.Logf("  item %d: weight=%d eligible=%v prior=%d start=%d",
			i, in.Items[i].Weight, in.Items[i].Eligible, in.Items[i].Prior, best.start[i])
	}
	t.Logf("  repair reached %v, optimum is %v", best.got, best.opt)
	opt := BruteForce(in)
	got := RepairX(in, best.start, 4, 4)
	show := func(name string, at []int) {
		loads := make([]int64, in.Workers)
		for i, w := range at {
			loads[w] += in.Items[i].Weight
		}
		t.Logf("  %-8s assignment=%v loads=%v", name, at, loads)
	}
	show("optimum", opt)
	show("repair", got)
	var diff []string
	for i := range in.Items {
		if opt[i] != got[i] {
			diff = append(diff, fmt.Sprintf("item%d: %d=>%d", i, got[i], opt[i]))
		}
	}
	t.Logf("  the move the repair cannot see: %v", diff)
}
