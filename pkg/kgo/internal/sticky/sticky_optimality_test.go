package sticky

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"
)

// bruteForceOptimum enumerates every legal assignment of parts to eligible
// members and returns the lexicographically smallest sorted-descending load
// vector along with the greatest stickiness achievable at that load vector.
//
// Sorted-descending lexicographic order is the right comparison: an optimal
// semi-matching minimizes every separable convex function of the load vector
// at once, and that is equivalent to being lexicographically minimal here.
func bruteForceOptimum(eligible [][]int, prior []int, nmembers int) (bestLoads []int, bestSticky int) {
	loads := make([]int, nmembers)
	var rec func(part, sticky int)
	rec = func(part, sticky int) {
		if part == len(eligible) {
			vec := slices.Clone(loads)
			slices.Sort(vec)
			slices.Reverse(vec)
			switch {
			case bestLoads == nil, slices.Compare(vec, bestLoads) < 0:
				bestLoads, bestSticky = vec, sticky
			case slices.Compare(vec, bestLoads) == 0 && sticky > bestSticky:
				bestSticky = sticky
			}
			return
		}
		if len(eligible[part]) == 0 { // nobody may hold it; it stays unassigned
			rec(part+1, sticky)
			return
		}
		for _, m := range eligible[part] {
			loads[m]++
			if prior[part] == m {
				rec(part+1, sticky+1)
			} else {
				rec(part+1, sticky)
			}
			loads[m]--
		}
	}
	rec(0, 0)
	return bestLoads, bestSticky
}

// TestBalanceIsLoadOptimalButNotStickyOptimal brute forces small instances to
// separate the two claims the balancer makes.
//
// The load claim holds: the balancer always reaches the optimal load vector,
// because it terminates only when no cost-reducing path remains, which is
// exactly the optimality criterion for a semi-matching.
//
// The stickiness claim is weaker than it looks. srcIsOriginal is a preference
// in the path heap, not a cost being minimized, so among the many assignments
// that achieve the optimal load vector the balancer picks a good one rather
// than the one that keeps the most partitions where they already were.
func TestBalanceIsLoadOptimalButNotStickyOptimal(t *testing.T) {
	t.Parallel()

	var instances, stickyShortfalls, totalShortfall, atQuota, byQuota int
	var worst string

	for seed := int64(0); seed < 6000; seed++ {
		rng := rand.New(rand.NewSource(seed))

		ntopics := 2 + rng.Intn(2)
		topics := make(map[string]int32, ntopics)
		var names []string
		var nparts int
		for i := range ntopics {
			n := 1 + rng.Intn(3)
			name := fmt.Sprintf("t%d", i)
			topics[name] = int32(n)
			names = append(names, name)
			nparts += n
		}
		if nparts > 7 { // keep the brute force enumeration cheap
			continue
		}

		nmembers := 2 + rng.Intn(3)
		subs := make([][]string, nmembers)
		for i := range nmembers {
			for _, name := range names {
				if rng.Intn(2) == 0 {
					subs[i] = append(subs[i], name)
				}
			}
			if len(subs[i]) == 0 {
				subs[i] = []string{names[rng.Intn(len(names))]}
			}
		}

		var parts []topicPartition
		for _, name := range names {
			for p := int32(0); p < topics[name]; p++ {
				parts = append(parts, topicPartition{name, p})
			}
		}
		eligible := make([][]int, len(parts))
		for i, part := range parts {
			for m := range nmembers {
				if slices.Contains(subs[m], part.topic) {
					eligible[i] = append(eligible[i], m)
				}
			}
		}

		// A random prior assignment, which is what the balancer will try to
		// stay close to. Some partitions are deliberately left unowned.
		prior := make([]int, len(parts))
		priorPlans := make([]map[string][]int32, nmembers)
		for i := range priorPlans {
			priorPlans[i] = make(map[string][]int32)
		}
		for i, part := range parts {
			if len(eligible[i]) == 0 || rng.Intn(4) == 0 {
				prior[i] = -1
				continue
			}
			m := eligible[i][rng.Intn(len(eligible[i]))]
			prior[i] = m
			priorPlans[m][part.topic] = append(priorPlans[m][part.topic], part.partition)
		}

		members := make([]GroupMember, nmembers)
		for i := range nmembers {
			members[i] = GroupMember{
				ID:       fmt.Sprintf("m%d", i),
				Topics:   subs[i],
				UserData: udEncode(1, 1, priorPlans[i]),
			}
		}

		instances++
		plan := Balance(members, topics)

		gotLoads := make([]int, nmembers)
		gotLoadsByMember := make([]int, nmembers)
		var gotSticky int
		for i := range nmembers {
			id := fmt.Sprintf("m%d", i)
			for _, partitions := range plan[id] {
				gotLoads[i] += len(partitions)
			}
			gotLoadsByMember[i] = gotLoads[i]
			gotSticky += getStickiness(id, plan[id], members)
		}
		slices.Sort(gotLoads)
		slices.Reverse(gotLoads)

		// The oracle maximizes stickiness at the quotas the balancer chose;
		// the brute force may also permute quotas between members. So
		// kept <= oracle <= bestSticky, and the two differences separate
		// "picked the wrong partitions" from "helped the wrong member".
		quotas := make(map[string]int, nmembers)
		priorByID := make(map[string]map[string][]int32, nmembers)
		subsByID := make(map[string][]string, nmembers)
		for i := range nmembers {
			id := fmt.Sprintf("m%d", i)
			quotas[id] = gotLoadsByMember[i]
			priorByID[id] = priorPlans[i]
			subsByID[id] = subs[i]
		}
		oracle, _ := maxStickinessAt(topics, subsByID, priorByID, quotas)

		bestLoads, bestSticky := bruteForceOptimum(eligible, prior, nmembers)

		if slices.Compare(gotLoads, bestLoads) != 0 {
			t.Fatalf("seed %d: load vector %v is not the optimum %v", seed, gotLoads, bestLoads)
		}
		if oracle >= 0 {
			if oracle > bestSticky {
				t.Fatalf("seed %d: oracle %d exceeds brute-force optimum %d", seed, oracle, bestSticky)
			}
			if gotSticky > oracle {
				t.Fatalf("seed %d: kept %d exceeds oracle %d at the same quotas", seed, gotSticky, oracle)
			}
			atQuota += oracle - gotSticky
			byQuota += bestSticky - oracle
		}
		if gotSticky < bestSticky {
			stickyShortfalls++
			totalShortfall += bestSticky - gotSticky
			if worst == "" {
				var sb []byte
				sb = fmt.Appendf(sb, "seed %d: kept %d of a possible %d at the same optimal load vector %v\n",
					seed, gotSticky, bestSticky, bestLoads)
				for i := range nmembers {
					sb = fmt.Appendf(sb, "  m%d subscribes %v, previously held", i, subs[i])
					for j, part := range parts {
						if prior[j] == i {
							sb = fmt.Appendf(sb, " %s/%d", part.topic, part.partition)
						}
					}
					sb = fmt.Appendf(sb, ", was planned %v\n", plan[fmt.Sprintf("m%d", i)])
				}
				worst = string(sb)
			}
		}
	}

	t.Logf("%d instances; the load vector was optimal in all of them", instances)
	t.Logf("stickiness fell short of the achievable maximum in %d (%.1f%%), by %d partitions in total",
		stickyShortfalls, 100*float64(stickyShortfalls)/float64(instances), totalShortfall)
	t.Logf("of that shortfall: %d from picking the wrong partitions at the chosen quotas, %d from choosing the wrong quotas",
		atQuota, byQuota)
	if worst != "" {
		t.Logf("first shortfall:\n%s", worst)
	}
	if stickyShortfalls == 0 {
		t.Errorf("expected to find at least one instance where stickiness is not maximal")
	}
}
