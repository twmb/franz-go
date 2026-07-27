package sticky

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"
)

// This file proves, rather than assumes, that balancing keeps as many
// partitions where they already were as any assignment could at the same
// per-member counts.
//
// The proof is a min-cost max-flow, which is far too slow to run in the
// balancer itself -- on a wide group it costs twenty times the entire balance
// -- but is exactly the right thing for a test: it computes the true optimum
// so the balancer's result can be compared against it instead of against a
// guess.

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

// maxStickinessAt returns the greatest number of partitions any assignment
// could leave where they already were, given that each member must end with
// exactly the number of partitions it was given.
//
// Partitions of one topic are interchangeable except for who holds them, so
// the whole question collapses onto a topic-by-member count table. Each
// (topic, member) cell gets two arcs: one of capacity "partitions this member
// already holds of this topic" at cost 0, and one unbounded at cost 1. The
// minimum cost is then the fewest partitions that have to move, and every
// count in the table is realizable back into actual partitions because the
// members' held sets are disjoint.
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
	// A member may list a topic more than once -- subscriptions arrive from
	// other group members' metadata, so they are arbitrary input. Adding a
	// second pair of arcs for a repeat would hand that member twice the
	// cost-free capacity it actually has and inflate the optimum.
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

type stickyShape struct {
	name       string
	nt, np, nm int
	divergent  bool
	double     bool // half the members are brand new
	subsPer    int  // if >0, each member subscribes to only this many topics

	// maxGapPct is the most of the assignment this shape may leave on the
	// table versus the true optimum. Every shape measures zero now that
	// trades run after balancing; the small allowance is only slack for
	// the run to run variation in topic numbering, which comes from map
	// iteration order.
	maxGapPct float64
}

func TestStickinessAgainstOptimum(t *testing.T) {
	shapes := []stickyShape{
		{"classic/rejoin", 20, 120, 40, false, false, 0, 0.5},
		{"classic/rejoin+divergent", 20, 120, 40, true, false, 0, 0.5},
		{"classic/doubled+divergent", 20, 120, 20, true, true, 0, 0.5},
		{"manytopics/rejoin+divergent", 120, 12, 40, true, false, 0, 0.5},
		{"manytopics/doubled+divergent", 120, 12, 20, true, true, 0, 0.5},
		{"wide/doubled+divergent", 40, 60, 20, true, true, 0, 0.5},
		// Heterogeneous subscriptions, where eligibility actually binds.
		{"subset/rejoin", 40, 40, 40, false, false, 6, 0.5},
		{"subset/doubled", 40, 40, 20, false, true, 6, 0.5},
		{"subset/doubled+divergent", 40, 40, 20, true, true, 6, 0.5},
		{"subset/narrow+doubled", 60, 50, 30, false, true, 3, 0.5},
		{"subset/wide-overlap+doubled", 30, 60, 20, false, true, 12, 0.5},
	}

	var certifiedShapes int
	for _, s := range shapes {
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
		for _, m := range members {
			n := 0
			for _, ps := range plan[m.ID] {
				n += len(ps)
			}
			quotas[m.ID] = n
			pp := make(map[string][]int32)
			flat, _ := deserializeUserData(m.UserData, nil)
			for _, tp := range flat {
				pp[tp.topic] = append(pp[tp.topic], tp.partition)
			}
			priorPlans[m.ID] = pp
		}

		var kept, ceiling, total int
		for _, m := range members {
			assigned := 0
			for _, ps := range plan[m.ID] {
				assigned += len(ps)
			}
			total += assigned

			// held: partitions this member previously had that it still
			// subscribes to. Anything else it could never have kept.
			held := 0
			priorPlan, _ := deserializeUserData(m.UserData, nil)
			wants := make(map[string]bool, len(m.Topics))
			for _, topic := range m.Topics {
				wants[topic] = true
			}
			for _, tp := range priorPlan {
				if wants[tp.topic] {
					held++
				}
			}

			kept += getStickiness(m.ID, plan[m.ID], members)
			ceiling += min(assigned, held)
		}

		best, _ := maxStickinessAt(topics, subs, priorPlans, quotas)
		gap := best - kept

		// The certificate must never claim optimal when the flow found
		// room to improve. It is allowed to decline to certify an
		// optimal plan; it is not allowed to be wrong.
		if certified := certifiesOptimal(subs, priorPlans, plan); certified && gap > 0 {
			t.Errorf("%s: certificate claimed optimal, flow found %d partitions of room", s.name, gap)
		} else if certified {
			certifiedShapes++
		}
		t.Logf("%-30s parts=%-6d kept=%-6d optimum=%-6d gap=%-5d (%.2f%%)  [loose ceiling=%d]",
			s.name, total, kept, best, gap, 100*float64(gap)/float64(total), ceiling)
		switch {
		case best < 0:
			t.Errorf("%s: the balancer's own per-member counts are not satisfiable", s.name)
		case kept > best:
			t.Errorf("%s: kept %d exceeds the proven optimum %d -- the oracle is wrong", s.name, kept, best)
		case float64(gap) > s.maxGapPct/100*float64(total):
			t.Errorf("%s: left %d of %d partitions (%.2f%%) on the table, more than the %.1f%% this shape is allowed",
				s.name, gap, total, 100*float64(gap)/float64(total), s.maxGapPct)
		}
	}
}

// certifiesOptimal reports whether the plan provably keeps as many partitions
// in place as any assignment could at the same per-member counts, without
// building a flow at all.
//
// Falling short of optimal takes one of two forms, and this rules out both.
//
// The counts per member and topic could be improvable. Improving them means a
// trade: some member gives back one of a topic it has more of than it started
// with and takes back one of a topic it has less of, so its total is unchanged
// and balance survives. A member that did not both gain somewhere and lose
// somewhere cannot take part in such a trade, and a cycle needs every member
// on it to be both a giver and a taker -- so if nobody is both, there is no
// trade of any length.
//
// Or the counts could be right while the wrong partitions were picked inside a
// cell: a member allotted three of a topic it held five of should be holding
// three it already had. That is what the second loop checks.
//
// One directional: true is proof, false only means go build the flow.
func certifiesOptimal(
	subs map[string][]string,
	prior map[string]map[string][]int32,
	plan map[string]map[string][]int32,
) bool {
	for member, held := range prior {
		var gained, lost bool
		got := plan[member]
		for _, topic := range subs[member] {
			switch n, h := len(got[topic]), len(held[topic]); {
			case n > h:
				gained = true
			case n < h:
				lost = true
			}
		}
		if gained && lost {
			return false
		}
		for _, topic := range subs[member] {
			mine := make(map[int32]bool, len(held[topic]))
			for _, p := range held[topic] {
				mine[p] = true
			}
			var kept int
			for _, p := range got[topic] {
				if mine[p] {
					kept++
				}
			}
			if kept < min(len(got[topic]), len(held[topic])) {
				return false
			}
		}
	}
	return true
}

// TestCertificateNeverLies checks the cheap optimality proof against the flow
// over many random instances, since a certificate that is ever wrong is worse
// than no certificate.
func TestCertificateNeverLies(t *testing.T) {
	t.Parallel()

	var certified, optimal int
	for seed := int64(0); seed < 3000; seed++ {
		rng := rand.New(rand.NewSource(seed))

		ntopics := 1 + rng.Intn(5)
		topics := make(map[string]int32, ntopics)
		var names []string
		for i := range ntopics {
			name := fmt.Sprintf("t%d", i)
			topics[name] = int32(1 + rng.Intn(6))
			names = append(names, name)
		}
		nmembers := 2 + rng.Intn(5)
		subs := make(map[string][]string, nmembers)
		members := make([]GroupMember, 0, nmembers)
		priorPlans := make(map[string]map[string][]int32, nmembers)
		for i := range nmembers {
			id := fmt.Sprintf("m%d", i)
			var mine []string
			for _, name := range names {
				if rng.Intn(2) == 0 {
					mine = append(mine, name)
				}
			}
			if len(mine) == 0 {
				mine = []string{names[rng.Intn(len(names))]}
			}
			subs[id] = mine
			priorPlans[id] = make(map[string][]int32)
			members = append(members, GroupMember{ID: id, Topics: mine})
		}

		// A partition has one owner, so priors must be disjoint. Claiming
		// one partition for several members would let the flow retain it
		// more than once and report an optimum nobody can reach.
		for _, topic := range names {
			for p := int32(0); p < topics[topic]; p++ {
				var eligible []string
				for id, mine := range subs {
					if slices.Contains(mine, topic) {
						eligible = append(eligible, id)
					}
				}
				if len(eligible) == 0 || rng.Intn(3) == 0 {
					continue
				}
				slices.Sort(eligible)
				owner := eligible[rng.Intn(len(eligible))]
				priorPlans[owner][topic] = append(priorPlans[owner][topic], p)
			}
		}
		for i := range members {
			members[i].UserData = udEncode(1, 1, priorPlans[members[i].ID])
		}

		plan := Balance(members, topics)
		quotas := make(map[string]int, nmembers)
		var kept int
		for _, m := range members {
			n := 0
			for _, ps := range plan[m.ID] {
				n += len(ps)
			}
			quotas[m.ID] = n
			kept += getStickiness(m.ID, plan[m.ID], members)
		}
		best, _ := maxStickinessAt(topics, subs, priorPlans, quotas)
		if best < 0 {
			continue
		}
		if best == kept {
			optimal++
		}
		if certifiesOptimal(subs, priorPlans, plan) {
			certified++
			if best > kept {
				t.Fatalf("seed %d: certificate claimed optimal, flow found %d more", seed, best-kept)
			}
		}
	}
	t.Logf("certificate proved %d of %d optimal plans outright, and was never wrong", certified, optimal)
	if certified == 0 {
		t.Error("the certificate never fired; it is not being exercised")
	}
}
