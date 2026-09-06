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
// compared lexicographically, with the most partitions any assignment at
// that load vector keeps in place.
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

// TestBalanceIsLoadAndStickyOptimal brute forces small random instances and
// checks that balancing reaches the optimal load vector and, at that load
// vector, keeps as many partitions in place as any assignment could. Set
// STICKY_SEEDS to hunt further than the default.
func TestBalanceIsLoadAndStickyOptimal(t *testing.T) {
	t.Parallel()

	seeds := int64(6000)
	if env := os.Getenv("STICKY_SEEDS"); env != "" {
		n, err := strconv.ParseInt(env, 10, 64)
		if err != nil {
			t.Fatalf("STICKY_SEEDS: %v", err)
		}
		seeds = n
	}

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
		if nparts > 7 { // keep the enumeration cheap
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
			}
		}

		instances++
		plan := Balance(members, topics)

		gotLoads := make([]int, nmembers)
		var gotSticky int
		for i := range nmembers {
			id := fmt.Sprintf("m%d", i)
			for _, partitions := range plan[id] {
				gotLoads[i] += len(partitions)
			}
			gotSticky += getStickiness(id, plan[id], members)
		}
		slices.Sort(gotLoads)
		slices.Reverse(gotLoads)

		bestLoads, bestSticky := bruteForceOptimum(eligible, prior, nmembers)
		if slices.Compare(gotLoads, bestLoads) != 0 {
			t.Fatalf("seed %d: load vector %v is not the optimum %v", seed, gotLoads, bestLoads)
		}
		if gotSticky != bestSticky {
			var sb []byte
			for i := range nmembers {
				sb = fmt.Appendf(sb, "  m%d subscribes %v, held", i, subs[i])
				for j, part := range parts {
					if prior[j] == i {
						sb = fmt.Appendf(sb, " %s/%d", part.topic, part.partition)
					}
				}
				sb = fmt.Appendf(sb, ", planned %v\n", plan[fmt.Sprintf("m%d", i)])
			}
			t.Fatalf("seed %d: kept %d partitions in place, optimum at load vector %v keeps %d\n%s", seed, gotSticky, bestLoads, bestSticky, sb)
		}
	}
	t.Logf("%d instances load and sticky optimal", instances)
}

// A min-cost max-flow, far too slow to run in the balancer but exactly what
// a test wants: the true optimum to compare against.
type mcmfEdge struct {
	to, rev  int
	capacity int
	cost     int
}

type mcmf struct{ g [][]mcmfEdge }

func newMCMF(n int) *mcmf { return &mcmf{g: make([][]mcmfEdge, n)} }

func (f *mcmf) addEdge(from, to, capacity, cost int) {
	f.g[from] = append(f.g[from], mcmfEdge{to, len(f.g[to]), capacity, cost})
	f.g[to] = append(f.g[to], mcmfEdge{from, len(f.g[from]) - 1, 0, -cost})
}

// run returns the max flow from s to t and its minimum cost, by repeatedly
// augmenting along a cheapest path (SPFA, since residual costs go negative).
func (f *mcmf) run(s, t int) (flow, cost int) {
	const inf = 1 << 30
	n := len(f.g)
	dist := make([]int, n)
	inQueue := make([]bool, n)
	prevNode := make([]int, n)
	prevEdge := make([]int, n)
	for {
		for i := range dist {
			dist[i] = inf
			prevNode[i] = -1
			inQueue[i] = false
		}
		dist[s] = 0
		queue := []int{s}
		inQueue[s] = true
		for len(queue) > 0 {
			v := queue[0]
			queue = queue[1:]
			inQueue[v] = false
			for i, e := range f.g[v] {
				if e.capacity <= 0 || dist[v]+e.cost >= dist[e.to] {
					continue
				}
				dist[e.to] = dist[v] + e.cost
				prevNode[e.to] = v
				prevEdge[e.to] = i
				if !inQueue[e.to] {
					inQueue[e.to] = true
					queue = append(queue, e.to)
				}
			}
		}
		if dist[t] == inf {
			return flow, cost
		}
		push := inf
		for v := t; v != s; v = prevNode[v] {
			push = min(push, f.g[prevNode[v]][prevEdge[v]].capacity)
		}
		for v := t; v != s; v = prevNode[v] {
			e := &f.g[prevNode[v]][prevEdge[v]]
			e.capacity -= push
			f.g[v][e.rev].capacity += push
		}
		flow += push
		cost += push * dist[t]
	}
}

// maxStickinessAt returns the most partitions any assignment could leave
// where they were, given each member must end with exactly its quota.
//
// Partitions of a topic are interchangeable except for who holds them, so
// each (topic, member) pair gets two arcs: one of capacity "partitions this
// member already holds of this topic" at cost 0, and one unbounded at cost
// 1. The minimum cost is then the fewest partitions that have to move.
func maxStickinessAt(
	topics map[string]int32,
	subs map[string][]string,
	prior map[string]map[string][]int32,
	quotas map[string]int,
) (maxKept, assigned int) {
	topicNums := make(map[string]int, len(topics))
	var topicNames []string
	for topic := range topics {
		topicNums[topic] = len(topicNames)
		topicNames = append(topicNames, topic)
	}
	slices.Sort(topicNames)
	for i, topic := range topicNames {
		topicNums[topic] = i
	}

	memberNums := make(map[string]int, len(quotas))
	var memberNames []string
	for member := range quotas {
		memberNames = append(memberNames, member)
	}
	slices.Sort(memberNames)
	for i, member := range memberNames {
		memberNums[member] = i
	}

	nt, nm := len(topicNames), len(memberNames)
	source, sink := nt+nm, nt+nm+1
	f := newMCMF(nt + nm + 2)

	// Only topics somebody subscribes to can be assigned at all.
	wanted := make([]bool, nt)
	for _, mine := range subs {
		for _, topic := range mine {
			wanted[topicNums[topic]] = true
		}
	}
	for i, topic := range topicNames {
		if wanted[i] {
			f.addEdge(source, i, int(topics[topic]), 0)
			assigned += int(topics[topic])
		}
	}
	for i, member := range memberNames {
		f.addEdge(nt+i, sink, quotas[member], 0)
	}
	// A subscription list may repeat a topic; a second pair of arcs would
	// hand the member twice the cost-free capacity it has.
	seen := make(map[[2]int]bool, len(subs))
	for member, mine := range subs {
		mi := memberNums[member]
		for _, topic := range mine {
			ti := topicNums[topic]
			if seen[[2]int{ti, mi}] {
				continue
			}
			seen[[2]int{ti, mi}] = true
			held := len(prior[member][topic])
			if held > 0 {
				f.addEdge(ti, nt+mi, held, 0)
			}
			f.addEdge(ti, nt+mi, int(topics[topic]), 1)
		}
	}

	flow, cost := f.run(source, sink)
	if flow != assigned {
		return -1, assigned // quotas not satisfiable; caller should notice
	}
	return assigned - cost, assigned
}

// TestStickinessAgainstOptimum checks realistic shapes, far larger than the
// brute force can reach, against the flow: at the per-member counts the
// balancer chose, it must keep exactly as many partitions in place as any
// assignment could.
func TestStickinessAgainstOptimum(t *testing.T) {
	t.Parallel()

	for _, s := range []struct {
		name       string
		nt, np, nm int
		divergent  bool // one extra member subscribes to a prefix of the topics
		double     bool // half the members are brand new
		subsPer    int  // if >0, each member subscribes to only this many topics
	}{
		{"classic/rejoin", 20, 120, 40, false, false, 0},
		{"classic/rejoin+divergent", 20, 120, 40, true, false, 0},
		{"classic/doubled+divergent", 20, 120, 20, true, true, 0},
		{"manytopics/rejoin+divergent", 120, 12, 40, true, false, 0},
		{"manytopics/doubled+divergent", 120, 12, 20, true, true, 0},
		{"wide/doubled+divergent", 40, 60, 20, true, true, 0},
		{"subset/rejoin", 40, 40, 40, false, false, 6},
		{"subset/doubled", 40, 40, 20, false, true, 6},
		{"subset/doubled+divergent", 40, 40, 20, true, true, 6},
		{"subset/narrow+doubled", 60, 50, 30, false, true, 3},
		{"subset/wide-overlap+doubled", 30, 60, 20, false, true, 12},
	} {
		rng := rand.New(rand.NewSource(1))

		topics := make(map[string]int32, s.nt)
		all := make([]string, 0, s.nt)
		for i := range s.nt {
			name := fmt.Sprintf("topic-%d", i)
			topics[name] = int32(s.np)
			all = append(all, name)
		}

		// A first balance to produce a realistic prior assignment.
		pick := func(i int) []string {
			if s.subsPer <= 0 {
				return all
			}
			mine := make([]string, 0, s.subsPer)
			for j := range s.subsPer {
				mine = append(mine, all[(i*s.subsPer+j*7+rng.Intn(3))%len(all)])
			}
			return mine
		}
		memberTopics := make([][]string, 0, 2*s.nm)
		for i := range 2 * s.nm {
			memberTopics = append(memberTopics, pick(i))
		}
		seed := make([]GroupMember, 0, s.nm)
		for i := range s.nm {
			seed = append(seed, GroupMember{ID: fmt.Sprintf("c-%d", i), Topics: memberTopics[i]})
		}
		prior := Balance(seed, topics)

		members := make([]GroupMember, 0, 2*s.nm+1)
		subs := make(map[string][]string)
		for i := range s.nm {
			id := fmt.Sprintf("c-%d", i)
			members = append(members, GroupMember{ID: id, Topics: memberTopics[i], UserData: udEncode(1, 1, prior[id])})
			subs[id] = memberTopics[i]
		}
		if s.double {
			for i := range s.nm {
				id := fmt.Sprintf("new-%d", i)
				members = append(members, GroupMember{ID: id, Topics: memberTopics[s.nm+i]})
				subs[id] = memberTopics[s.nm+i]
			}
		}
		if s.divergent {
			n := 1 + rng.Intn(len(all)-1)
			members = append(members, GroupMember{ID: "odd", Topics: all[:n]})
			subs["odd"] = all[:n]
		}

		plan := Balance(members, topics)

		quotas := make(map[string]int, len(members))
		priorPlans := make(map[string]map[string][]int32, len(members))
		var kept, total int
		for _, m := range members {
			for _, ps := range plan[m.ID] {
				quotas[m.ID] += len(ps)
			}
			total += quotas[m.ID]
			pp := make(map[string][]int32)
			flat, _ := deserializeUserData(m.UserData, nil)
			for _, tp := range flat {
				pp[tp.topic] = append(pp[tp.topic], tp.partition)
			}
			priorPlans[m.ID] = pp
			kept += getStickiness(m.ID, plan[m.ID], members)
		}

		best, _ := maxStickinessAt(topics, subs, priorPlans, quotas)
		t.Logf("%-30s parts=%-6d kept=%-6d optimum=%-6d", s.name, total, kept, best)
		switch {
		case best < 0:
			t.Errorf("%s: the balancer's own per-member counts are not satisfiable", s.name)
		case kept > best:
			t.Errorf("%s: kept %d exceeds the flow optimum %d; the oracle is wrong", s.name, kept, best)
		case kept < best:
			t.Errorf("%s: kept %d of %d partitions in place, the optimum keeps %d", s.name, kept, total, best)
		}
	}
}
