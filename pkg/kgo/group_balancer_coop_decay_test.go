package kgo

import (
	"fmt"
	"math/rand"
	"slices"
	"sort"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// Cooperative rebalancing needs two rounds to move a partition: the first
// round's plan is stripped of everything that has to be revoked first, the
// owner revokes and rejoins, and the second round hands the partition over.
// That is fine as long as the loop terminates. This file drives the real
// balance => AdjustCooperative => rejoin loop to answer whether it converges,
// oscillates, or drifts, and how the answer changes as a group ages.

// coopGroup is a cooperative-sticky group carried across rebalances. It runs
// the same path a real leader runs: build each member's join metadata from
// what it currently owns, hand the whole set to the sticky balancer, let
// AdjustCooperative strip the transfers, and give everyone the result as their
// new assignment.
type coopGroup struct {
	rng *rand.Rand

	topics    map[string]int32
	tnames    []string
	rackNames []string
	partRacks map[string][]string

	ids    []string
	racks  map[string]string
	owned  map[string]map[string][]int32
	gen    int32
	nextID int
}

func newCoopGroup(seed int64, ntopics int, nparts int32, nmembers int, racks []string) *coopGroup {
	g := &coopGroup{
		rng:       rand.New(rand.NewSource(seed)),
		topics:    make(map[string]int32, ntopics),
		rackNames: racks,
		racks:     make(map[string]string),
		owned:     make(map[string]map[string][]int32),
	}
	if len(racks) > 0 {
		g.partRacks = make(map[string][]string, ntopics)
	}
	for i := range ntopics {
		name := fmt.Sprintf("t%d", i)
		g.topics[name] = nparts
		g.tnames = append(g.tnames, name)
		if g.partRacks != nil {
			rs := make([]string, nparts)
			for j := range rs {
				rs[j] = racks[g.rng.Intn(len(racks))]
			}
			g.partRacks[name] = rs
		}
	}
	sort.Strings(g.tnames)
	for range nmembers {
		g.addMember()
	}
	return g
}

func (g *coopGroup) addMember() string {
	id := fmt.Sprintf("c-%d", g.nextID)
	if len(g.rackNames) > 0 {
		g.racks[id] = g.rackNames[g.nextID%len(g.rackNames)]
	}
	g.nextID++
	g.ids = append(g.ids, id)
	return id
}

func (g *coopGroup) dropMember(i int) {
	id := g.ids[i]
	g.ids = append(g.ids[:i], g.ids[i+1:]...)
	delete(g.owned, id)
	delete(g.racks, id)
}

// replaceKeepingRack swaps a member for a new one in the same rack. Round
// robin placement alone drifts the group toward one rack holding more members
// than its share of partition leaders, which caps locality for reasons that
// have nothing to do with the balancer.
func (g *coopGroup) replaceKeepingRack(i int) {
	rack := g.racks[g.ids[i]]
	g.dropMember(i)
	id := g.addMember()
	if rack != "" {
		g.racks[id] = rack
	}
}

func (g *coopGroup) moveLeaders(pct int) {
	if g.partRacks == nil || pct == 0 {
		return
	}
	for _, t := range g.tnames {
		rs := g.partRacks[t]
		for i := range rs {
			if g.rng.Intn(100) < pct {
				rs[i] = g.rackNames[g.rng.Intn(len(g.rackNames))]
			}
		}
	}
}

func (g *coopGroup) nparts() int {
	var n int
	for _, p := range g.topics {
		n += int(p)
	}
	return n
}

// coopRound is one join+sync of the cooperative loop.
type coopRound struct {
	revoked    int // partitions a member had to give up, which forces another round
	added      int // partitions a member picked up
	unassigned int // partitions nobody is consuming until the next round
	rack       float64
}

func (g *coopGroup) rebalance(t *testing.T) coopRound {
	t.Helper()

	sb := &stickyBalancer{cooperative: true}
	g.gen++

	members := make([]kmsg.JoinGroupResponseMember, 0, len(g.ids))
	for _, id := range g.ids {
		meta := sb.JoinGroupMetadata(g.tnames, g.owned[id], g.gen)
		if rack := g.racks[id]; rack != "" {
			// Mirror the rack injection the group consumer does on
			// top of whatever the balancer wrote.
			var m kmsg.ConsumerMemberMetadata
			if err := m.ReadFrom(meta); err != nil {
				t.Fatalf("reading back join metadata: %v", err)
			}
			rack := rack
			m.Rack = &rack
			if m.Version < 3 {
				m.Version = 3
			}
			meta = m.AppendTo(nil)
		}
		members = append(members, kmsg.JoinGroupResponseMember{
			MemberID:         id,
			ProtocolMetadata: meta,
		})
	}
	sortJoinMembers(members)

	mb, _, err := sb.MemberBalancer(members)
	if err != nil {
		t.Fatalf("member balancer: %v", err)
	}
	cb := mb.(*ConsumerBalancer)
	cb.partitionRacks = g.partRacks

	isa, err := cb.BalanceOrError(g.topics)
	if err != nil {
		t.Fatalf("balance: %v", err)
	}
	plan := isa.(*BalancePlan).AsMemberIDMap()

	var r coopRound
	seen := make(map[string]map[int32]bool, len(g.topics))
	rackHits := 0
	for _, id := range g.ids {
		was := g.owned[id]
		now := plan[id]
		for topic, parts := range now {
			for _, p := range parts {
				if seen[topic] == nil {
					seen[topic] = make(map[int32]bool)
				}
				if seen[topic][p] {
					t.Fatalf("%s/%d assigned twice", topic, p)
				}
				seen[topic][p] = true
				if g.partRacks != nil && g.partRacks[topic][p] == g.racks[id] {
					rackHits++
				}
			}
			r.added += len(parts) - countIn(was[topic], parts)
		}
		for topic, parts := range was {
			r.revoked += len(parts) - countIn(parts, now[topic])
		}
	}
	for topic, n := range g.topics {
		for p := range n {
			if !seen[topic][p] {
				r.unassigned++
			}
		}
	}
	if g.partRacks != nil {
		r.rack = 100 * float64(rackHits) / float64(g.nparts())
	}

	// Everyone applies the assignment they were given; whoever lost
	// something revokes it and rejoins, which is what drives the loop.
	next := make(map[string]map[string][]int32, len(g.ids))
	for _, id := range g.ids {
		cp := make(map[string][]int32, len(plan[id]))
		for topic, parts := range plan[id] {
			if len(parts) > 0 {
				cp[topic] = slices.Clone(parts)
			}
		}
		next[id] = cp
	}
	g.owned = next
	return r
}

// eagerRack is the rack locality the plain (eager) sticky balancer reaches
// from the group's current state. Cooperative reaches its plan through a
// stripped intermediate round, so this is the number to hold it against: if
// the strip defeats the rack trade pass, cooperative settles lower.
func (g *coopGroup) eagerRack(t *testing.T) float64 {
	t.Helper()
	if g.partRacks == nil {
		return 0
	}
	sb := &stickyBalancer{cooperative: false}
	members := make([]kmsg.JoinGroupResponseMember, 0, len(g.ids))
	for _, id := range g.ids {
		meta := sb.JoinGroupMetadata(g.tnames, g.owned[id], g.gen)
		var m kmsg.ConsumerMemberMetadata
		if err := m.ReadFrom(meta); err != nil {
			t.Fatalf("reading back join metadata: %v", err)
		}
		rack := g.racks[id]
		m.Rack = &rack
		m.Version = 3
		members = append(members, kmsg.JoinGroupResponseMember{
			MemberID:         id,
			ProtocolMetadata: m.AppendTo(nil),
		})
	}
	sortJoinMembers(members)
	mb, _, err := sb.MemberBalancer(members)
	if err != nil {
		t.Fatalf("member balancer: %v", err)
	}
	cb := mb.(*ConsumerBalancer)
	cb.partitionRacks = g.partRacks
	isa, err := cb.BalanceOrError(g.topics)
	if err != nil {
		t.Fatalf("balance: %v", err)
	}
	var hits int
	for id, topics := range isa.(*BalancePlan).AsMemberIDMap() {
		for topic, parts := range topics {
			for _, p := range parts {
				if g.partRacks[topic][p] == g.racks[id] {
					hits++
				}
			}
		}
	}
	return 100 * float64(hits) / float64(g.nparts())
}

// countIn counts how many of want appear in have.
func countIn(want, have []int32) int {
	if len(want) == 0 || len(have) == 0 {
		return 0
	}
	set := make(map[int32]struct{}, len(have))
	for _, p := range have {
		set[p] = struct{}{}
	}
	var n int
	for _, p := range want {
		if _, ok := set[p]; ok {
			n++
		}
	}
	return n
}

// converge runs the join loop until no member has to revoke anything, which is
// the condition under which no member rejoins and the group settles.
func (g *coopGroup) converge(t *testing.T, maxRounds int) []coopRound {
	t.Helper()
	var rs []coopRound
	for range maxRounds {
		r := g.rebalance(t)
		rs = append(rs, r)
		if r.revoked == 0 {
			return rs
		}
	}
	return rs
}

// TestCooperativeConvergence drives balance => AdjustCooperative => rejoin
// until nobody revokes, once per membership or leadership event, and reports
// how many rounds and how much movement each event cost as the group ages.
//
// A group that never settles shows up as rounds hitting the cap; a group that
// gets worse with age shows up as rounds or handoffs climbing.
func TestCooperativeConvergence(t *testing.T) {
	t.Parallel()

	const (
		ntopics   = 12
		nparts    = 30
		nmembers  = 12
		nevents   = 25
		maxRounds = 20
	)
	racks := []string{"a", "b", "c"}

	for _, s := range []struct {
		name      string
		racks     []string
		leaderPct int
		event     func(g *coopGroup, n int)
		maxSettle int // rounds the last third of events may average
	}{
		// Controls: nothing happens, so the very first round must have
		// nothing to revoke.
		{name: "control_static", maxSettle: 1},
		{name: "control_static_racked", racks: racks, maxSettle: 1},

		{
			name: "1out1in",
			event: func(g *coopGroup, n int) {
				g.dropMember(g.rng.Intn(len(g.ids)))
				g.addMember()
			},
			maxSettle: 3,
		},
		{
			name:  "1out1in_racked_same_rack",
			racks: racks,
			event: func(g *coopGroup, n int) {
				g.replaceKeepingRack(g.rng.Intn(len(g.ids)))
			},
			maxSettle: 4,
		},
		{
			name:  "1out1in_racked_drifting_racks",
			racks: racks,
			event: func(g *coopGroup, n int) {
				g.dropMember(g.rng.Intn(len(g.ids)))
				g.addMember()
			},
			maxSettle: 4,
		},
		{
			// The realistic rack case: a broker restart moves some
			// leaders, then the cluster is quiet again. The trade
			// pass wants to move partitions; cooperative has to
			// revoke them first. Does it settle?
			name:      "leaders_move_10pct",
			racks:     racks,
			leaderPct: 10,
			maxSettle: 4,
		},
		{
			name:      "leaders_move_40pct",
			racks:     racks,
			leaderPct: 40,
			maxSettle: 5,
		},
		{
			// Scaling up is the case that genuinely needs the extra
			// round: a joining member can only be given partitions
			// that somebody else first revokes.
			name: "grow_then_shrink",
			event: func(g *coopGroup, n int) {
				if n%2 == 1 {
					g.addMember()
				} else {
					g.dropMember(g.rng.Intn(len(g.ids)))
				}
			},
			maxSettle: 3,
		},
		{
			name:  "grow_then_shrink_racked",
			racks: racks,
			event: func(g *coopGroup, n int) {
				if n%2 == 1 {
					g.addMember()
				} else {
					g.dropMember(g.rng.Intn(len(g.ids)))
				}
			},
			maxSettle: 4,
		},
		{
			// A deployment scaling out and back in, three at a time.
			name: "bulk_scaleout",
			event: func(g *coopGroup, n int) {
				if n%2 == 1 {
					for range 3 {
						g.addMember()
					}
				} else {
					for range 3 {
						g.dropMember(g.rng.Intn(len(g.ids)))
					}
				}
			},
			maxSettle: 3,
		},
	} {
		g := newCoopGroup(17, ntopics, nparts, nmembers, s.racks)
		// Settle the initial assignment before measuring.
		if rs := g.converge(t, maxRounds); len(rs) >= maxRounds {
			t.Fatalf("coop/%s: initial assignment did not settle in %d rounds", s.name, maxRounds)
		}

		t.Logf("coop/%s\nevent rounds revoked  added unassigned  rack%%  eagerrack%%", s.name)
		var settles []int
		var stuck bool
		var sumRack, sumEager float64
		for n := 1; n <= nevents; n++ {
			g.moveLeaders(s.leaderPct)
			if s.event != nil {
				s.event(g, n)
			}
			rs := g.converge(t, maxRounds)
			var revoked, added, unassigned int
			for _, r := range rs {
				revoked += r.revoked
				added += r.added
				unassigned += r.unassigned
			}
			settles = append(settles, len(rs))
			eager := g.eagerRack(t)
			sumRack, sumEager = sumRack+rs[len(rs)-1].rack, sumEager+eager
			t.Logf("%5d %6d %7d %6d %10d %7.1f %11.1f", n, len(rs), revoked, added, unassigned, rs[len(rs)-1].rack, eager)
			if len(rs) >= maxRounds {
				t.Errorf("coop/%s: event %d never settled in %d rounds; the group rebalances forever", s.name, n, maxRounds)
				stuck = true
				break
			}
		}
		if stuck {
			continue
		}

		third := max(1, len(settles)/3)
		meanInt := func(xs []int) float64 {
			var s int
			for _, x := range xs {
				s += x
			}
			return float64(s) / float64(len(xs))
		}
		first := meanInt(settles[:third])
		last := meanInt(settles[len(settles)-third:])
		t.Logf("coop/%s: rounds to settle %.2f => %.2f over %d events; settled rack locality %.1f%% where an eager balance of the same state reaches %.1f%%",
			s.name, first, last, nevents, sumRack/float64(nevents), sumEager/float64(nevents))
		if last > float64(s.maxSettle) {
			t.Errorf("coop/%s: settling took %.2f rounds on average over the last third of events (first third %.2f), above the %d allowance",
				s.name, last, first, s.maxSettle)
		}
	}
}

// TestCooperativeSteadyStateChurn asks what the cooperative loop costs when
// the cluster never goes quiet: leaders keep moving every single rebalance, so
// the rack trade pass always has something it wants to move and every round
// revokes something. The group can never reach "nobody revokes", so the
// question is whether the per-round cost holds steady or ratchets up.
func TestCooperativeSteadyStateChurn(t *testing.T) {
	t.Parallel()

	const (
		ntopics  = 12
		nparts   = 30
		nmembers = 12
		nrounds  = 40
	)
	racks := []string{"a", "b", "c"}

	for _, s := range []struct {
		name      string
		leaderPct int
	}{
		{name: "control_leaders_still", leaderPct: 0},
		{name: "leaders_move_2pct_every_round", leaderPct: 2},
		{name: "leaders_move_10pct_every_round", leaderPct: 10},
	} {
		g := newCoopGroup(29, ntopics, nparts, nmembers, racks)
		g.converge(t, 20)

		t.Logf("coopsteady/%s\nround revoked  added unassigned  rack%%", s.name)
		var rs []coopRound
		for n := 1; n <= nrounds; n++ {
			g.moveLeaders(s.leaderPct)
			r := g.rebalance(t)
			rs = append(rs, r)
			t.Logf("%5d %7d %6d %10d %7.1f", n, r.revoked, r.added, r.unassigned, r.rack)
		}

		third := max(1, len(rs)/3)
		mean := func(rs []coopRound, f func(coopRound) float64) float64 {
			var s float64
			for _, r := range rs {
				s += f(r)
			}
			return s / float64(len(rs))
		}
		first, last := rs[:third], rs[len(rs)-third:]
		t.Logf("coopsteady/%s: revoked %.1f => %.1f | unassigned %.1f => %.1f | rack%% %.1f => %.1f",
			s.name,
			mean(first, func(r coopRound) float64 { return float64(r.revoked) }),
			mean(last, func(r coopRound) float64 { return float64(r.revoked) }),
			mean(first, func(r coopRound) float64 { return float64(r.unassigned) }),
			mean(last, func(r coopRound) float64 { return float64(r.unassigned) }),
			mean(first, func(r coopRound) float64 { return r.rack }),
			mean(last, func(r coopRound) float64 { return r.rack }),
		)

		if s.leaderPct == 0 {
			for _, r := range rs {
				if r.revoked != 0 || r.unassigned != 0 {
					t.Errorf("coopsteady/%s: a quiet cluster revoked %d and left %d partitions unconsumed", s.name, r.revoked, r.unassigned)
					break
				}
			}
		}
	}
}
