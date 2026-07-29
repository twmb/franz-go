package weighted

// PairwiseOptimal reports whether an assignment cannot be improved by
// reconsidering any two workers together, and how much the best pair rewrite
// would save if it can.
//
// Two workers are the case that is exactly solvable at any size and for any
// weights, so this asks the strongest question that can still be answered
// exactly: take the items sitting on a pair, forget how they are split, and
// find the best split there is. An assignment that survives every pair is not
// proven globally optimal -- three workers can hold an improvement no pair of
// them sees -- but every claim it makes is exact, which is more than a
// relaxation bound offers.
func PairwiseOptimal(in *Instance, at []int) (bool, int64) {
	var worst int64
	ok := true
	for a := range in.Workers {
		for b := a + 1; b < in.Workers; b++ {
			sub := &Instance{Workers: 2}
			var cur [2]int64
			for i := range in.Items {
				if at[i] != a && at[i] != b {
					continue
				}
				var el []int
				if containsInt(in.Items[i].Eligible, a) {
					el = append(el, 0)
				}
				if containsInt(in.Items[i].Eligible, b) {
					el = append(el, 1)
				}
				if len(el) == 0 {
					continue
				}
				sub.Items = append(sub.Items, Item{Weight: in.Items[i].Weight, Eligible: el, Prior: -1})
				if at[i] == a {
					cur[0] += in.Items[i].Weight
				} else {
					cur[1] += in.Items[i].Weight
				}
			}
			if len(sub.Items) == 0 {
				continue
			}
			best, solved := ExactTwoWorkers(sub)
			if !solved {
				continue
			}
			have := cur[0]*cur[0] + cur[1]*cur[1]
			if have > best {
				ok = false
				if have-best > worst {
					worst = have - best
				}
			}
		}
	}
	return ok, worst
}
