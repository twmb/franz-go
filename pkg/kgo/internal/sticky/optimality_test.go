package sticky

import (
	"fmt"
	"math/rand"
	"os"
	"slices"
	"strconv"
	"testing"
)

// bruteForceOptimum tries every assignment of the partitions to their
// eligible members and returns the best load vector, sorted descending and
// compared lexicographically, then the fewest partitions held off rack at
// that load vector, then the most partitions any assignment at both keeps
// in place. A rack of 0 is no rack: a partition is off rack only when both
// it and its member have a rack and they differ.
func bruteForceOptimum(eligible [][]int, prior, partRack, memberRack []int, nmembers int) (bestLoads []int, bestOff, bestSticky int) {
	loads := make([]int, nmembers)
	var rec func(part, off, sticky int)
	rec = func(part, off, sticky int) {
		if part == len(eligible) {
			vec := slices.Clone(loads)
			slices.Sort(vec)
			slices.Reverse(vec)
			c := 0
			if bestLoads != nil {
				c = slices.Compare(vec, bestLoads)
			}
			if bestLoads == nil || c < 0 || c == 0 && (off < bestOff || off == bestOff && sticky > bestSticky) {
				bestLoads, bestOff, bestSticky = vec, off, sticky
			}
			return
		}
		if len(eligible[part]) == 0 { // nobody may hold it; it stays unassigned
			rec(part+1, off, sticky)
			return
		}
		for _, m := range eligible[part] {
			loads[m]++
			o, s := off, sticky
			if partRack[part] != 0 && memberRack[m] != 0 && partRack[part] != memberRack[m] {
				o++
			}
			if prior[part] == m {
				s++
			}
			rec(part+1, o, s)
			loads[m]--
		}
	}
	rec(0, 0, 0)
	return bestLoads, bestOff, bestSticky
}

// TestBalanceIsOptimal brute forces small random groups and checks that
// balancing reaches the optimal load vector, then the fewest off rack
// partitions at that load vector, then keeps as many partitions in place
// as any assignment at both could. A quarter of the groups are uniform, a
// quarter have no racks, and some have over 64 members with a few
// subscribers per topic, so that the subscriber bitsets span two words.
// Set STICKY_SEEDS to hunt further than the default.
func TestBalanceIsOptimal(t *testing.T) {
	t.Parallel()

	seeds := int64(6000)
	if env := os.Getenv("STICKY_SEEDS"); env != "" {
		n, err := strconv.ParseInt(env, 10, 64)
		if err != nil {
			t.Fatalf("STICKY_SEEDS: %v", err)
		}
		seeds = n
	}
	rackNames := []string{"", "a", "b", "c"}

	var instances int
	for seed := int64(0); seed < seeds; seed++ {
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
		nracks := rng.Intn(4)                // 0 is a group with no racks at all
		uniform := rng.Intn(4) == 0          // every member subscribes to every topic
		wide := !uniform && rng.Intn(8) == 0 // over 64 members, three per topic
		nmembers := 2 + rng.Intn(3)
		if wide {
			nmembers = 64 + rng.Intn(4)
		}
		if nparts > 7 || wide && nparts > 5 { // keep the enumeration cheap
			continue
		}

		subs := make([][]string, nmembers)
		memberRack := make([]int, nmembers)
		for i := range nmembers {
			memberRack[i] = rng.Intn(nracks + 1)
			if wide {
				continue
			}
			for _, name := range names {
				if uniform || rng.Intn(2) == 0 {
					subs[i] = append(subs[i], name)
				}
			}
			if len(subs[i]) == 0 {
				subs[i] = []string{names[rng.Intn(len(names))]}
			}
		}
		if wide {
			for _, name := range names {
				for range 3 {
					if m := rng.Intn(nmembers); !slices.Contains(subs[m], name) {
						subs[m] = append(subs[m], name)
					}
				}
			}
		}

		var parts []topicPartition
		partIdx := make(map[topicPartition]int)
		var partRack []int
		var partitionRacks map[string][]string
		if nracks > 0 {
			partitionRacks = make(map[string][]string, ntopics)
		}
		for _, name := range names {
			rs := make([]string, topics[name])
			for p := range topics[name] {
				partIdx[topicPartition{name, p}] = len(parts)
				parts = append(parts, topicPartition{name, p})
				r := rng.Intn(nracks + 1)
				partRack = append(partRack, r)
				rs[p] = rackNames[r]
			}
			if nracks > 0 {
				partitionRacks[name] = rs
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

		// A random prior assignment, with some partitions left unowned.
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
				Rack:     rackNames[memberRack[i]],
			}
		}

		instances++
		plan := BalanceWithRacks(members, topics, partitionRacks)

		// A uniform group without racks skips the repair. Check that the
		// repair would have found nothing to do.
		if b := balanced(members, topics, partitionRacks); !b.isComplex && b.partRacks == nil {
			tbl := b.stickyTable()
			loads := make([]int32, len(b.members))
			for _, c := range tbl.cells {
				loads[c.member] += c.x
			}
			f := newStickyFinder(int(tbl.nrows), len(b.members))
			if cycles := f.find(tbl.cells, loads); len(cycles) > 0 {
				t.Fatalf("seed %d: the repair is skipped for a uniform group without racks but would make %d rotations", seed, len(cycles))
			}
		}

		gotLoads := make([]int, nmembers)
		var gotOff, gotSticky int
		seen := make([]bool, len(parts))
		for i := range nmembers {
			for topic, ps := range plan[fmt.Sprintf("m%d", i)] {
				for _, p := range ps {
					idx, ok := partIdx[topicPartition{topic, p}]
					if !ok || seen[idx] {
						t.Fatalf("seed %d: partition %s/%d is unknown or assigned twice", seed, topic, p)
					}
					seen[idx] = true
					gotLoads[i]++
					if partRack[idx] != 0 && memberRack[i] != 0 && partRack[idx] != memberRack[i] {
						gotOff++
					}
					if prior[idx] == i {
						gotSticky++
					}
				}
			}
		}
		for i := range parts {
			if !seen[i] && len(eligible[i]) > 0 {
				t.Fatalf("seed %d: partition %s/%d has subscribers but was not assigned", seed, parts[i].topic, parts[i].partition)
			}
		}
		slices.Sort(gotLoads)
		slices.Reverse(gotLoads)

		bestLoads, bestOff, bestSticky := bruteForceOptimum(eligible, prior, partRack, memberRack, nmembers)

		describe := func() string {
			var sb []byte
			for i := range nmembers {
				if len(subs[i]) == 0 {
					continue
				}
				sb = fmt.Appendf(sb, "  m%d rack=%q subscribes %v, held", i, rackNames[memberRack[i]], subs[i])
				for j, part := range parts {
					if prior[j] == i {
						sb = fmt.Appendf(sb, " %s/%d", part.topic, part.partition)
					}
				}
				sb = fmt.Appendf(sb, ", planned %v\n", plan[fmt.Sprintf("m%d", i)])
			}
			for j, part := range parts {
				sb = fmt.Appendf(sb, "  %s/%d rack=%q\n", part.topic, part.partition, rackNames[partRack[j]])
			}
			return string(sb)
		}
		switch {
		case slices.Compare(gotLoads, bestLoads) != 0:
			t.Fatalf("seed %d: load vector %v is not the optimum %v\n%s", seed, gotLoads, bestLoads, describe())
		case gotOff != bestOff:
			t.Fatalf("seed %d: %d partitions off rack, the optimum at load vector %v has %d\n%s", seed, gotOff, bestLoads, bestOff, describe())
		case gotSticky != bestSticky:
			t.Fatalf("seed %d: kept %d partitions in place, the optimum at load vector %v with %d off rack keeps %d\n%s", seed, gotSticky, bestLoads, bestOff, bestSticky, describe())
		}
	}
	t.Logf("%d instances load, rack, and sticky optimal", instances)
}
