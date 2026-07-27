package sticky

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"
)

// This file asks, of properties other than rack locality, the question that
// found the rack decay: does the property get monotonically worse across
// SUCCESSIVE rebalances, rather than being wrong on any single one? Every
// harness here runs many rounds against a mutating cluster and reports the
// metric per round; every scenario list carries a control that must hold flat
// so a decaying line can be told apart from noise.

// decayCluster is a group plus its topics carried across rebalances. Each
// round mutates the cluster (members come and go, topics grow, leaders move)
// and then rebalances from the previous plan, exactly as a real group does.
type decayCluster struct {
	rng *rand.Rand

	topics    map[string]int32
	tnames    []string
	rackNames []string
	partRacks map[string][]string // nil when the cluster has no rack info

	ids    []string
	subs   map[string][]string // member => topics; nil entry means all topics
	mrack  map[string]string
	nextID int
	gen    int
	rackRR int

	// subPool plus subSlot let a replacement member inherit exactly the
	// subscription of the member it replaces, so the group's multiset of
	// subscriptions is identical every round and any drift measured is the
	// balancer's, not the scenario's.
	subPool [][]string
	subSlot map[string]int

	claims map[string]map[string][]int32 // what each member tells the leader it holds
	gens   map[string]int                // the generation each member claims
	actual map[topicPartition]string     // who the leader last put where
}

func newDecayCluster(seed int64, ntopics int, nparts int32, nmembers int, racks []string) *decayCluster {
	c := &decayCluster{
		rng:       rand.New(rand.NewSource(seed)),
		topics:    make(map[string]int32, ntopics),
		rackNames: racks,
		subs:      make(map[string][]string),
		claims:    make(map[string]map[string][]int32),
		gens:      make(map[string]int),
		actual:    make(map[topicPartition]string),
		subSlot:   make(map[string]int),
		mrack:     make(map[string]string),
		gen:       1,
	}
	if len(racks) > 0 {
		c.partRacks = make(map[string][]string, ntopics)
	}
	for range ntopics {
		c.addTopic(nparts)
	}
	for range nmembers {
		c.addMember()
	}
	return c
}

func (c *decayCluster) addTopic(nparts int32) string {
	name := fmt.Sprintf("t%d", len(c.tnames))
	c.topics[name] = nparts
	c.tnames = append(c.tnames, name)
	if c.partRacks != nil {
		rs := make([]string, nparts)
		for i := range rs {
			rs[i] = c.rackNames[c.rng.Intn(len(c.rackNames))]
		}
		c.partRacks[name] = rs
	}
	return name
}

func (c *decayCluster) growTopic(name string, by int32) {
	c.topics[name] += by
	if c.partRacks != nil {
		rs := c.partRacks[name]
		for range by {
			rs = append(rs, c.rackNames[c.rng.Intn(len(c.rackNames))])
		}
		c.partRacks[name] = rs
	}
}

func (c *decayCluster) addMember() string {
	id := fmt.Sprintf("c-%d", c.nextID)
	c.nextID++
	c.ids = append(c.ids, id)
	c.gens[id] = c.gen
	if len(c.rackNames) > 0 {
		c.mrack[id] = c.rackNames[c.rackRR%len(c.rackNames)]
		c.rackRR++
	}
	return id
}

// replaceKeepingRack swaps a member for a new one in the same rack, so the
// group's rack composition is identical every round. Round robin placement
// alone does not hold it: dropping members at random and adding them in
// rotation drifts the group toward one rack having more members than its
// share of partition leaders, which caps locality no matter what the
// balancer does.
func (c *decayCluster) replaceKeepingRack(i int) string {
	rack := c.mrack[c.ids[i]]
	c.dropMember(i)
	id := c.addMember()
	c.mrack[id] = rack
	return id
}

func (c *decayCluster) dropMember(i int) string {
	id := c.ids[i]
	c.ids = append(c.ids[:i], c.ids[i+1:]...)
	delete(c.claims, id)
	delete(c.gens, id)
	delete(c.subs, id)
	delete(c.subSlot, id)
	delete(c.mrack, id)
	return id
}

// useSubPool gives every current member a distinct random subscription and
// remembers which slot each holds, so replaceKeepingSub can hand a slot on.
func (c *decayCluster) useSubPool() {
	c.subPool = make([][]string, len(c.ids))
	for i, id := range c.ids {
		c.subPool[i] = randomSubset(c.rng, c.tnames)
		c.subs[id] = c.subPool[i]
		c.subSlot[id] = i
	}
}

// replaceKeepingSub swaps a member for a brand new one that subscribes to
// exactly what the departing member did.
func (c *decayCluster) replaceKeepingSub(i int) string {
	slot := c.subSlot[c.ids[i]]
	c.dropMember(i)
	id := c.addMember()
	c.subSlot[id] = slot
	c.subs[id] = c.subPool[slot]
	return id
}

// bounce is a member restarting: same ID, but it comes back having forgotten
// everything it held.
func (c *decayCluster) bounce(i int) string {
	id := c.ids[i]
	c.claims[id] = nil
	return id
}

func (c *decayCluster) moveLeaders(pct int) {
	if c.partRacks == nil || pct == 0 {
		return
	}
	for _, t := range c.tnames {
		rs := c.partRacks[t]
		for i := range rs {
			if c.rng.Intn(100) < pct {
				rs[i] = c.rackNames[c.rng.Intn(len(c.rackNames))]
			}
		}
	}
}

func (c *decayCluster) rackOf(id string) string { return c.mrack[id] }

// rackSpread is how many members sit in each rack, which caps how much
// locality is reachable at all.
func (c *decayCluster) rackSpread() []int {
	counts := make([]int, len(c.rackNames))
	for _, id := range c.ids {
		for i, r := range c.rackNames {
			if c.mrack[id] == r {
				counts[i]++
			}
		}
	}
	return counts
}

func (c *decayCluster) topicsFor(id string) []string {
	if s, ok := c.subs[id]; ok && s != nil {
		return s
	}
	return c.tnames
}

func (c *decayCluster) nparts() int {
	var n int
	for _, p := range c.topics {
		n += int(p)
	}
	return n
}

// round is one rebalance's worth of measurements.
type round struct {
	n        int
	nparts   int
	nmembers int

	hands  int // partitions whose consumer changed since the last plan
	floor  int // fewest such changes any balanced plan could have made
	fresh  int // partitions that had no consumer going in
	lo, hi int // partitions held by the least and most loaded member

	spread float64 // mean distinct topics a member holds
	conc   float64 // worst member's share of its load coming from one topic
	rack   float64 // percent of partitions consumed from the leader's rack
}

func (c *decayCluster) rebalance(t *testing.T, n int, uniform bool) round {
	t.Helper()

	members := make([]GroupMember, 0, len(c.ids))
	for _, id := range c.ids {
		members = append(members, GroupMember{
			ID:       id,
			Topics:   c.topicsFor(id),
			UserData: udEncode(1, c.gens[id], c.claims[id]),
			Rack:     c.rackOf(id),
		})
	}

	prior := c.actual
	plan := BalanceWithRacks(members, c.topics, c.partRacks)

	r := round{n: n, nparts: c.nparts(), nmembers: len(c.ids)}
	owners := make(map[topicPartition]string, r.nparts)
	var (
		total     int
		held      = make(map[string]int, len(c.ids))
		topicsPer = make(map[string]int, len(c.ids))
		worstConc float64
		rackHits  int
	)
	for member, topics := range plan {
		var mine, biggest int
		for topic, parts := range topics {
			if len(parts) == 0 {
				continue
			}
			topicsPer[member]++
			mine += len(parts)
			biggest = max(biggest, len(parts))
			for _, p := range parts {
				tp := topicPartition{topic, p}
				if _, dup := owners[tp]; dup {
					t.Fatalf("round %d: %s/%d assigned twice", n, topic, p)
				}
				owners[tp] = member
				if c.partRacks != nil && c.partRacks[topic][p] == c.rackOf(member) {
					rackHits++
				}
			}
		}
		total += mine
		held[member] = mine
		if mine > 0 {
			worstConc = max(worstConc, float64(biggest)/float64(mine))
		}
	}
	if total != r.nparts {
		t.Fatalf("round %d: %d partitions assigned, want %d", n, total, r.nparts)
	}

	r.lo, r.hi = r.nparts, 0
	var sumTopics int
	for _, id := range c.ids {
		r.lo, r.hi = min(r.lo, held[id]), max(r.hi, held[id])
		sumTopics += topicsPer[id]
	}
	r.spread = float64(sumTopics) / float64(len(c.ids))
	r.conc = worstConc
	if c.partRacks != nil {
		r.rack = 100 * float64(rackHits) / float64(r.nparts)
	}
	if uniform && r.hi-r.lo > 1 {
		t.Errorf("round %d: partition counts span %d..%d", n, r.lo, r.hi)
	}

	for tp, now := range owners {
		was, had := prior[tp]
		switch {
		case !had:
			r.fresh++
		case was != now:
			r.hands++
		}
	}
	// A brand new partition is never "retained" by minMoves, so it shows up
	// in its count the same as a partition that had to move. hands only
	// counts partitions that already had a consumer, so the floor has to
	// drop the new ones to be comparable.
	r.floor = max(0, minMoves(prior, c.ids, c.topics)-r.fresh)

	// Roll forward: the plan becomes both what everyone is consuming and
	// what everyone will claim next round.
	c.actual = owners
	next := make(map[string]map[string][]int32, len(plan))
	for member, topics := range plan {
		cp := make(map[string][]int32, len(topics))
		for topic, parts := range topics {
			cp[topic] = slices.Clone(parts)
		}
		next[member] = cp
	}
	c.claims = next
	c.gen++
	for _, id := range c.ids {
		c.gens[id] = c.gen
	}
	return r
}

// minMoves is the fewest partitions that must change consumer to get from
// prior to any evenly balanced assignment over the live members. It ignores
// what the balancer can know (a member that forgot its assignment still counts
// as holding it) and ignores subscriptions, so it is a true lower bound only
// for uniform subscriptions.
func minMoves(prior map[topicPartition]string, ids []string, topics map[string]int32) int {
	live := make(map[string]bool, len(ids))
	for _, id := range ids {
		live[id] = true
	}
	held := make(map[string]int, len(ids))
	var total int
	for topic, n := range topics {
		for p := range n {
			total++
			if o, ok := prior[topicPartition{topic, p}]; ok && live[o] {
				held[o]++
			}
		}
	}
	if len(ids) == 0 {
		return 0
	}
	q, r := total/len(ids), total%len(ids)
	hs := make([]int, 0, len(ids))
	for _, id := range ids {
		hs = append(hs, held[id])
	}
	slices.SortFunc(hs, func(a, b int) int { return b - a })
	// The r members allowed the extra partition should be the ones already
	// holding the most, else we understate what can be retained.
	var retain int
	for i, h := range hs {
		lim := q
		if i < r {
			lim = q + 1
		}
		retain += min(h, lim)
	}
	return total - retain
}

// reportRounds prints every round so a trend is visible, then the mean of the
// first and last third so a trend is unmistakable.
func reportRounds(t *testing.T, name string, rs []round, cols ...string) {
	t.Helper()
	has := func(c string) bool { return slices.Contains(cols, c) }

	var hdr string
	hdr = "round parts mbrs"
	if has("hands") {
		hdr += "  hands  floor excess  fresh"
	}
	if has("span") {
		hdr += "   span"
	}
	if has("spread") {
		hdr += " topics/mbr  worstconc"
	}
	if has("rack") {
		hdr += "   rack%"
	}
	t.Logf("%s\n%s", name, hdr)
	for _, r := range rs {
		line := fmt.Sprintf("%5d %5d %4d", r.n, r.nparts, r.nmembers)
		if has("hands") {
			line += fmt.Sprintf(" %6d %6d %6d %6d", r.hands, r.floor, r.hands-r.floor, r.fresh)
		}
		if has("span") {
			line += fmt.Sprintf("  %3d-%-3d", r.lo, r.hi)
		}
		if has("spread") {
			line += fmt.Sprintf(" %10.2f %10.3f", r.spread, r.conc)
		}
		if has("rack") {
			line += fmt.Sprintf(" %7.1f", r.rack)
		}
		t.Log(line)
	}

	third := max(1, len(rs)/3)
	mean := func(rs []round, f func(round) float64) float64 {
		var s float64
		for _, r := range rs {
			s += f(r)
		}
		return s / float64(len(rs))
	}
	first, last := rs[:third], rs[len(rs)-third:]
	t.Logf("%s trend over %d rounds: hands %.1f => %.1f | excess %.1f => %.1f | topics/mbr %.2f => %.2f | worstconc %.3f => %.3f | rack%% %.1f => %.1f",
		name, len(rs),
		mean(first, func(r round) float64 { return float64(r.hands) }),
		mean(last, func(r round) float64 { return float64(r.hands) }),
		mean(first, func(r round) float64 { return float64(r.hands - r.floor) }),
		mean(last, func(r round) float64 { return float64(r.hands - r.floor) }),
		mean(first, func(r round) float64 { return r.spread }),
		mean(last, func(r round) float64 { return r.spread }),
		mean(first, func(r round) float64 { return r.conc }),
		mean(last, func(r round) float64 { return r.conc }),
		mean(first, func(r round) float64 { return r.rack }),
		mean(last, func(r round) float64 { return r.rack }),
	)
}

func meanOf(rs []round, f func(round) float64) float64 {
	var s float64
	for _, r := range rs {
		s += f(r)
	}
	return s / float64(len(rs))
}

// TestChurnDecay asks whether a group gets less sticky as it ages: with the
// same membership event repeated every round, does the number of partitions
// that change consumer grow?
//
// floor is the fewest changes any balanced assignment could have made from the
// previous one, so excess = hands - floor is churn the balancer chose rather
// than churn the event forced. A balancer that decays shows excess climbing.
func TestChurnDecay(t *testing.T) {
	t.Parallel()

	const (
		ntopics  = 12
		nparts   = 30
		nmembers = 12
		nrounds  = 24
	)

	for _, s := range []struct {
		name      string
		racks     []string
		leaderPct int
		event     func(c *decayCluster, n int)
		maxExcess float64 // mean excess over the last third
	}{
		// Controls: nothing changes, so nothing may move.
		{name: "control_static", maxExcess: 0},
		{
			name:      "control_static_racked",
			racks:     []string{"a", "b", "c"},
			maxExcess: 0,
		},

		// One member out, one in, every round: the leaver's partitions
		// are exactly what the joiner needs, so a perfect balancer
		// moves only those.
		{
			name: "1out1in_every_round",
			event: func(c *decayCluster, n int) {
				c.dropMember(c.rng.Intn(len(c.ids)))
				c.addMember()
			},
			maxExcess: 2,
		},
		// A rolling restart: shrink one round, grow the next.
		{
			name: "shrink_then_grow",
			event: func(c *decayCluster, n int) {
				if n%2 == 0 {
					c.dropMember(c.rng.Intn(len(c.ids)))
				} else {
					c.addMember()
				}
			},
			maxExcess: 2,
		},
		// A pod restarts: same ID, no memory of what it held.
		{
			name: "bounce_same_id",
			event: func(c *decayCluster, n int) {
				c.bounce(c.rng.Intn(len(c.ids)))
			},
			maxExcess: 2,
		},
		// Racked versions of the same, plus leaders moving underneath.
		{
			// The replacement lands in the rack the departing member
			// was in, so the group's rack composition never drifts
			// and locality is not capped by the scenario.
			name:      "1out1in_racked_same_rack",
			racks:     []string{"a", "b", "c"},
			maxExcess: 40,
			event: func(c *decayCluster, n int) {
				c.replaceKeepingRack(c.rng.Intn(len(c.ids)))
			},
		},
		{
			name:      "1out1in_racked_drifting_racks",
			racks:     []string{"a", "b", "c"},
			maxExcess: 40,
			event: func(c *decayCluster, n int) {
				c.dropMember(c.rng.Intn(len(c.ids)))
				c.addMember()
			},
		},
		{
			name:      "static_members_leaders_move_10pct",
			racks:     []string{"a", "b", "c"},
			leaderPct: 10,
			maxExcess: 60,
		},
	} {
		c := newDecayCluster(11, ntopics, nparts, nmembers, s.racks)
		var rs []round
		c.rebalance(t, 0, true) // settle
		for n := 1; n <= nrounds; n++ {
			c.moveLeaders(s.leaderPct)
			if s.event != nil {
				s.event(c, n)
			}
			rs = append(rs, c.rebalance(t, n, true))
		}
		cols := []string{"hands", "span", "spread"}
		if s.racks != nil {
			cols = append(cols, "rack")
		}
		reportRounds(t, "churn/"+s.name, rs, cols...)

		third := max(1, len(rs)/3)
		firstExcess := meanOf(rs[:third], func(r round) float64 { return float64(r.hands - r.floor) })
		lastExcess := meanOf(rs[len(rs)-third:], func(r round) float64 { return float64(r.hands - r.floor) })
		if lastExcess > s.maxExcess {
			t.Errorf("churn/%s: excess churn averaged %.1f partitions/round over the last third (first third %.1f), above the %.1f allowance; the group is getting less sticky as it ages",
				s.name, lastExcess, firstExcess, s.maxExcess)
		}
	}
}

// TestTopicGrowthDecay asks what happens as topics gain partitions, which
// Kafka topics do and never undo. Adding partitions should cost exactly the
// placement of the new partitions: no partition that already had a consumer
// needs to change hands, because handing the new ones to the least loaded
// members keeps the group balanced by construction.
func TestTopicGrowthDecay(t *testing.T) {
	t.Parallel()

	const (
		ntopics  = 12
		nparts   = 12
		nmembers = 12
		nrounds  = 24
	)

	for _, s := range []struct {
		name      string
		racks     []string
		event     func(c *decayCluster, n int)
		maxExcess float64
	}{
		{name: "control_no_growth", maxExcess: 0},
		{
			name: "grow_every_topic_by_2",
			event: func(c *decayCluster, n int) {
				for _, t := range c.tnames {
					c.growTopic(t, 2)
				}
			},
			maxExcess: 2,
		},
		{
			name: "grow_one_topic_by_12",
			event: func(c *decayCluster, n int) {
				c.growTopic(c.tnames[c.rng.Intn(len(c.tnames))], 12)
			},
			maxExcess: 2,
		},
		{
			name: "new_topic_each_round",
			event: func(c *decayCluster, n int) {
				c.addTopic(12)
			},
			maxExcess: 2,
		},
		{
			name: "grow_and_churn",
			event: func(c *decayCluster, n int) {
				c.growTopic(c.tnames[c.rng.Intn(len(c.tnames))], 6)
				if n%3 == 0 {
					c.dropMember(c.rng.Intn(len(c.ids)))
					c.addMember()
				}
			},
			maxExcess: 3,
		},
		{
			name:  "grow_every_topic_racked",
			racks: []string{"a", "b", "c"},
			event: func(c *decayCluster, n int) {
				for _, t := range c.tnames {
					c.growTopic(t, 2)
				}
			},
			maxExcess: 30,
		},
	} {
		c := newDecayCluster(23, ntopics, nparts, nmembers, s.racks)
		var rs []round
		c.rebalance(t, 0, true)
		for n := 1; n <= nrounds; n++ {
			if s.event != nil {
				s.event(c, n)
			}
			rs = append(rs, c.rebalance(t, n, true))
		}
		cols := []string{"hands", "span", "spread"}
		if s.racks != nil {
			cols = append(cols, "rack")
		}
		reportRounds(t, "growth/"+s.name, rs, cols...)

		third := max(1, len(rs)/3)
		firstExcess := meanOf(rs[:third], func(r round) float64 { return float64(r.hands - r.floor) })
		lastExcess := meanOf(rs[len(rs)-third:], func(r round) float64 { return float64(r.hands - r.floor) })
		if lastExcess > s.maxExcess {
			t.Errorf("growth/%s: excess churn averaged %.1f partitions/round over the last third (first third %.1f), above the %.1f allowance",
				s.name, lastExcess, firstExcess, s.maxExcess)
		}
	}
}

// TestTopicSpreadDecay watches how a member's load is distributed across
// topics as the group ages. Partition count balance is not load balance: a
// member that ends up holding one whole topic and nothing else is badly
// overloaded if that topic is the hot one. Nothing in the balancer re-examines
// topic spread once partitions are placed, so it fits the shape of a property
// set once while the thing it depends on keeps moving.
func TestTopicSpreadDecay(t *testing.T) {
	t.Parallel()

	const (
		ntopics  = 12
		nparts   = 24
		nmembers = 12
		nrounds  = 40
	)

	for _, s := range []struct {
		name  string
		event func(c *decayCluster, n int)
	}{
		{name: "control_static"},
		{
			name: "1out1in_every_round",
			event: func(c *decayCluster, n int) {
				c.dropMember(c.rng.Intn(len(c.ids)))
				c.addMember()
			},
		},
		{
			name: "bounce_every_round",
			event: func(c *decayCluster, n int) {
				c.bounce(c.rng.Intn(len(c.ids)))
			},
		},
		{
			name: "grow_one_topic",
			event: func(c *decayCluster, n int) {
				c.growTopic(c.tnames[c.rng.Intn(len(c.tnames))], 6)
			},
		},
	} {
		c := newDecayCluster(31, ntopics, nparts, nmembers, nil)
		var rs []round
		c.rebalance(t, 0, true)
		for n := 1; n <= nrounds; n++ {
			if s.event != nil {
				s.event(c, n)
			}
			rs = append(rs, c.rebalance(t, n, true))
		}
		reportRounds(t, "spread/"+s.name, rs, "hands", "span", "spread")

		third := max(1, len(rs)/3)
		firstSpread := meanOf(rs[:third], func(r round) float64 { return r.spread })
		lastSpread := meanOf(rs[len(rs)-third:], func(r round) float64 { return r.spread })
		firstConc := meanOf(rs[:third], func(r round) float64 { return r.conc })
		lastConc := meanOf(rs[len(rs)-third:], func(r round) float64 { return r.conc })
		t.Logf("spread/%s: topics per member %.2f => %.2f of %d; worst single-topic share %.3f => %.3f",
			s.name, firstSpread, lastSpread, len(c.tnames), firstConc, lastConc)
		// A member holding over half its load from one topic out of a
		// dozen means partition balance has stopped tracking load.
		if lastConc > 0.5 {
			t.Errorf("spread/%s: worst member draws %.0f%% of its partitions from a single topic (was %.0f%%); partition balance has drifted away from topic balance",
				s.name, 100*lastConc, 100*firstConc)
		}
	}
}

// TestStaleRejoinDecay repeats the KIP-341 path: a member that missed
// rebalances rejoins still claiming, at its old generation, partitions that
// have since moved on. tryRestickyStales hands those partitions back when the
// current owner is overloaded. Doing that once is fine; the question is
// whether doing it every round leaves a residue.
//
// A stale claim only differs from a current one if assignments actually moved
// in between, so every scenario here also churns membership. The control is
// the same churn with nobody lagging.
func TestStaleRejoinDecay(t *testing.T) {
	t.Parallel()

	const (
		ntopics  = 8
		nparts   = 24
		nmembers = 8
		nrounds  = 72
	)

	type snapshot struct {
		gen    int
		claims map[string]map[string][]int32
	}

	for _, s := range []struct {
		name      string
		lag       int  // how many generations behind the lagging member rejoins
		sameOne   bool // always the same member lags, or a random one
		maxExcess float64
	}{
		{name: "control_churn_no_stales", maxExcess: 1},
		{name: "one_member_lags_2_gens", lag: 2, sameOne: true, maxExcess: 12},
		{name: "random_member_lags_2_gens", lag: 2, maxExcess: 12},
		{name: "one_member_lags_5_gens", lag: 5, sameOne: true, maxExcess: 12},
	} {
		c := newDecayCluster(41, ntopics, nparts, nmembers, nil)
		c.rebalance(t, 0, true)

		var history []snapshot
		save := func() {
			cp := make(map[string]map[string][]int32, len(c.claims))
			for m, topics := range c.claims {
				tc := make(map[string][]int32, len(topics))
				for topic, parts := range topics {
					tc[topic] = slices.Clone(parts)
				}
				cp[m] = tc
			}
			history = append(history, snapshot{gen: c.gen, claims: cp})
		}
		save()

		var rs []round
		var stales, staleParts int
		for n := 1; n <= nrounds; n++ {
			// The group size has to actually change: swapping one
			// member for another leaves everybody else holding
			// exactly what they held, so an old claim would be
			// identical to the current one and nothing would ever
			// be doubly claimed. Shrinking and growing redivides
			// the whole group every round. Index 0 never leaves so
			// it can be the one that lags.
			if n%2 == 1 {
				c.dropMember(1 + c.rng.Intn(len(c.ids)-1))
			} else {
				c.addMember()
			}
			if s.lag > 0 && len(history) > s.lag {
				old := history[len(history)-1-s.lag]
				id := c.ids[0]
				if !s.sameOne {
					id = c.ids[c.rng.Intn(len(c.ids))]
				}
				if oldClaims, ok := old.claims[id]; ok {
					c.claims[id] = oldClaims
					c.gens[id] = old.gen
					stales++
				}
			}
			staleParts += countDoubleClaims(c)
			rs = append(rs, c.rebalance(t, n, true))
			save()
		}
		reportRounds(t, "stale/"+s.name, rs, "hands", "span", "spread")
		t.Logf("stale/%s: %d rounds had a lagging rejoin, producing %d doubly claimed partitions in total", s.name, stales, staleParts)
		if s.lag > 0 && staleParts == 0 {
			t.Errorf("stale/%s: no partition was ever doubly claimed, so the KIP-341 path was never exercised", s.name)
		}

		third := max(1, len(rs)/3)
		firstExcess := meanOf(rs[:third], func(r round) float64 { return float64(r.hands - r.floor) })
		lastExcess := meanOf(rs[len(rs)-third:], func(r round) float64 { return float64(r.hands - r.floor) })
		if lastExcess > s.maxExcess {
			t.Errorf("stale/%s: excess churn averaged %.1f partitions/round over the last third (first third %.1f), above the %.1f allowance",
				s.name, lastExcess, firstExcess, s.maxExcess)
		}
	}
}

// countDoubleClaims is how many partitions two members both claim with one on
// an older generation, which is exactly what lands in the balancer's stales
// map. It exists so the stale scenarios can prove they reach the KIP-341 path
// rather than quietly measuring nothing.
func countDoubleClaims(c *decayCluster) int {
	members := make([]GroupMember, 0, len(c.ids))
	for _, id := range c.ids {
		members = append(members, GroupMember{
			ID:       id,
			Topics:   c.topicsFor(id),
			UserData: udEncode(1, c.gens[id], c.claims[id]),
		})
	}
	b := newBalancer(members, c.topics, nil)
	b.parseMemberMetadata()
	return len(b.stales)
}

// TestComplexPathDecay runs the same questions down the complex (non-uniform
// subscription) path, which balances through the steal graph rather than the
// level tree. Subscriptions are not uniform, so minMoves is not a valid floor
// here and only observed churn, balance, and topic spread are reported.
//
// The metric to watch is topics per member against what a fresh balance of the
// same final state would hold. Partition count balance is not load balance: a
// member holding half its partitions from one topic is badly loaded if that is
// the busy topic, and nothing in the balancer ever re-examines the spread of a
// partition it is already holding.
func TestComplexPathDecay(t *testing.T) {
	t.Parallel()

	const (
		ntopics  = 12
		nparts   = 20
		nmembers = 12
		nrounds  = 48
	)

	for _, s := range []struct {
		name  string
		event func(c *decayCluster, n int)
	}{
		{name: "control_static"},
		{
			// The replacement subscribes to exactly what the member
			// it replaces did, so the group's multiset of
			// subscriptions is byte for byte the same every round
			// and any drift is the balancer's.
			name: "1out1in_same_subs",
			event: func(c *decayCluster, n int) {
				c.replaceKeepingSub(c.rng.Intn(len(c.ids)))
			},
		},
		{
			// A restart that comes back wanting something else,
			// which is what a regex consumer looks like as topics
			// come and go.
			name: "1out1in_new_subs",
			event: func(c *decayCluster, n int) {
				c.dropMember(c.rng.Intn(len(c.ids)))
				id := c.addMember()
				c.subs[id] = randomSubset(c.rng, c.tnames)
			},
		},
		{
			name: "grow_one_topic",
			event: func(c *decayCluster, n int) {
				c.growTopic(c.tnames[c.rng.Intn(len(c.tnames))], 8)
			},
		},
		{
			name: "resubscribe_every_round",
			event: func(c *decayCluster, n int) {
				id := c.ids[c.rng.Intn(len(c.ids))]
				c.subs[id] = randomSubset(c.rng, c.tnames)
			},
		},
	} {
		c := newDecayCluster(53, ntopics, nparts, nmembers, nil)
		c.useSubPool()
		var rs []round
		c.rebalance(t, 0, false)
		for n := 1; n <= nrounds; n++ {
			if s.event != nil {
				s.event(c, n)
			}
			// New members need a subscription too.
			for _, id := range c.ids {
				if _, ok := c.subs[id]; !ok {
					c.subs[id] = randomSubset(c.rng, c.tnames)
				}
			}
			rs = append(rs, c.rebalance(t, n, false))
		}
		reportRounds(t, "complex/"+s.name, rs, "hands", "span", "spread")

		third := max(1, len(rs)/3)
		firstSpan := meanOf(rs[:third], func(r round) float64 { return float64(r.hi - r.lo) })
		lastSpan := meanOf(rs[len(rs)-third:], func(r round) float64 { return float64(r.hi - r.lo) })
		firstHands := meanOf(rs[:third], func(r round) float64 { return float64(r.hands) })
		lastHands := meanOf(rs[len(rs)-third:], func(r round) float64 { return float64(r.hands) })
		lastSpread := meanOf(rs[len(rs)-third:], func(r round) float64 { return r.spread })
		freshTopics, freshConc := freshSpread(c)
		t.Logf("complex/%s: span %.2f => %.2f, hands %.1f => %.1f, holds %.2f topics/mbr where a fresh balance of the same state holds %.2f (forgone %.2f, worst conc fresh %.3f)",
			s.name, firstSpan, lastSpan, firstHands, lastHands, lastSpread, freshTopics, freshTopics-lastSpread, freshConc)

		// The aged plan should not hold materially fewer topics per
		// member than a from-scratch balance of the same state: any gap
		// is concentration the previous plan handed forward.
		if freshTopics-lastSpread > 3 {
			t.Errorf("complex/%s: the carried plan holds %.2f topics per member where a fresh balance holds %.2f; topic spread is rotting across rebalances",
				s.name, lastSpread, freshTopics)
		}
	}
}

// planSpread reports, over a balancer's in-progress plan, the mean number of
// distinct topics a member holds and the worst member's share of its load
// coming from one topic.
func planSpread(b *balancer) (meanTopics, worstConc float64) {
	var sumTopics int
	seen := make(map[uint32]int)
	for memberNum := range b.plan {
		clear(seen)
		for _, partNum := range b.plan[memberNum] {
			seen[b.partOwners[partNum]]++
		}
		sumTopics += len(seen)
		var biggest, mine int
		for _, n := range seen {
			biggest = max(biggest, n)
			mine += n
		}
		if mine > 0 {
			worstConc = max(worstConc, float64(biggest)/float64(mine))
		}
	}
	return float64(sumTopics) / float64(len(b.plan)), worstConc
}

// freshSpread is the topic spread a from-scratch balance of the cluster's
// current members and topics achieves -- the spread available if nothing were
// carried over from the previous plan.
func freshSpread(c *decayCluster) (meanTopics, worstConc float64) {
	members := make([]GroupMember, 0, len(c.ids))
	for _, id := range c.ids {
		members = append(members, GroupMember{ID: id, Topics: c.topicsFor(id)})
	}
	b := newBalancer(members, c.topics, nil)
	b.parseMemberMetadata()
	b.assignUnassignedAndInitGraph()
	b.initPlanByNumPartitions()
	b.balance()
	return planSpread(b)
}

// TestComplexSpreadStage narrows the complex path's topic concentration to a
// stage. It runs the same churn as TestComplexPathDecay but drives the
// balancer by hand so the plan can be measured after unassigned partitions are
// placed and again after the steal graph has balanced.
//
// The simple path deliberately spreads what it steals: it sorts a member's
// partitions by partition-then-topic before taking from the end so that steals
// come off every topic evenly (sticky.go sortMemberByLiteralPartNum, and the
// comment above the steal loop in balance). The complex path has no
// counterpart -- findSteal takes o.free[0] or o.owned[0], the first partition
// of whichever topic reached that member (graph.go findSteal), and pathHeap
// ranks candidates by stickiness, level, distance, and member number, never by
// what the stealer already holds a lot of. Sticky carries the result into the
// next round, so the concentration compounds.
func TestComplexSpreadStage(t *testing.T) {
	t.Parallel()

	const (
		ntopics  = 12
		nparts   = 20
		nmembers = 12
		nrounds  = 30
	)

	for _, s := range []struct {
		name  string
		event func(c *decayCluster, n int)
	}{
		{name: "control_static"},
		{
			name: "1out1in_same_subs",
			event: func(c *decayCluster, n int) {
				c.replaceKeepingSub(c.rng.Intn(len(c.ids)))
			},
		},
		{
			name: "1out1in_new_subs",
			event: func(c *decayCluster, n int) {
				c.dropMember(c.rng.Intn(len(c.ids)))
				id := c.addMember()
				c.subs[id] = randomSubset(c.rng, c.tnames)
			},
		},
	} {
		c := newDecayCluster(53, ntopics, nparts, nmembers, nil)
		c.useSubPool()

		run := func() (subbed, assignTopics, assignConc, balTopics, balConc float64) {
			for _, id := range c.ids {
				if _, ok := c.subs[id]; !ok {
					c.subs[id] = randomSubset(c.rng, c.tnames)
				}
			}
			members := make([]GroupMember, 0, len(c.ids))
			var nsubs int
			for _, id := range c.ids {
				topics := c.topicsFor(id)
				nsubs += len(topics)
				members = append(members, GroupMember{
					ID:       id,
					Topics:   topics,
					UserData: udEncode(1, c.gen, c.claims[id]),
				})
			}
			b := newBalancer(members, c.topics, nil)
			b.parseMemberMetadata()
			b.assignUnassignedAndInitGraph()
			if !b.isComplex {
				t.Fatalf("%s: subscriptions were uniform, this test needs the complex path", s.name)
			}
			assignTopics, assignConc = planSpread(b)
			b.initPlanByNumPartitions()
			b.balance()
			balTopics, balConc = planSpread(b)

			plan := b.into()
			next := make(map[string]map[string][]int32, len(plan))
			for member, topics := range plan {
				cp := make(map[string][]int32, len(topics))
				for topic, parts := range topics {
					cp[topic] = slices.Clone(parts)
				}
				next[member] = cp
			}
			c.claims = next
			c.gen++
			return float64(nsubs) / float64(len(c.ids)), assignTopics, assignConc, balTopics, balConc
		}

		run() // settle
		t.Logf("complexstage/%s\nround subbed  afterAssign(topics conc)  afterBalance(topics conc)", s.name)
		type row struct{ subbed, at, ac, bt, bc float64 }
		var rows []row
		for n := 1; n <= nrounds; n++ {
			if s.event != nil {
				s.event(c, n)
			}
			subbed, at, ac, bt, bc := run()
			rows = append(rows, row{subbed, at, ac, bt, bc})
			t.Logf("%5d %6.2f %12.2f %8.3f %14.2f %8.3f", n, subbed, at, ac, bt, bc)
		}

		third := max(1, len(rows)/3)
		mean := func(rs []row, f func(row) float64) float64 {
			var s float64
			for _, r := range rs {
				s += f(r)
			}
			return s / float64(len(rs))
		}
		first, last := rows[:third], rows[len(rows)-third:]
		freshTopics, freshConc := freshSpread(c)
		t.Logf("complexstage/%s: subscribed topics/mbr %.2f => %.2f | held after assign %.2f => %.2f | held after balance %.2f => %.2f | worst conc after balance %.3f => %.3f | a fresh balance of the final state holds %.2f topics/mbr at conc %.3f",
			s.name,
			mean(first, func(r row) float64 { return r.subbed }), mean(last, func(r row) float64 { return r.subbed }),
			mean(first, func(r row) float64 { return r.at }), mean(last, func(r row) float64 { return r.at }),
			mean(first, func(r row) float64 { return r.bt }), mean(last, func(r row) float64 { return r.bt }),
			mean(first, func(r row) float64 { return r.bc }), mean(last, func(r row) float64 { return r.bc }),
			freshTopics, freshConc,
		)
	}
}

// TestBalanceDeterminism asks whether the plan is a function of the input.
// The balancer numbers topics by ranging a Go map, so the internal partition
// layout differs between two calls on identical input. If that leaks into the
// plan, then every rebalance -- and in particular one run by a newly elected
// leader -- shuffles partitions for no reason, and the group pays churn for
// nothing.
func TestBalanceDeterminism(t *testing.T) {
	t.Parallel()

	const (
		ntopics  = 12
		nparts   = 30
		nmembers = 12
	)
	racks := []string{"a", "b", "c"}

	for _, s := range []struct {
		name    string
		racks   []string
		perturb func(c *decayCluster)
		maxDiff int
	}{
		{name: "settled", maxDiff: 0},
		{name: "settled_racked", racks: racks, maxDiff: 0},
		{
			name:    "after_a_member_left",
			perturb: func(c *decayCluster) { c.dropMember(0) },
			maxDiff: 360,
		},
		{
			name:    "after_a_member_left_racked",
			racks:   racks,
			perturb: func(c *decayCluster) { c.dropMember(0) },
			maxDiff: 360,
		},
		{
			name:    "after_leaders_moved",
			racks:   racks,
			perturb: func(c *decayCluster) { c.moveLeaders(10) },
			maxDiff: 360,
		},
	} {
		c := newDecayCluster(83, ntopics, nparts, nmembers, s.racks)
		c.rebalance(t, 0, true)
		c.rebalance(t, 1, true)
		if s.perturb != nil {
			s.perturb(c)
		}

		members := make([]GroupMember, 0, len(c.ids))
		for _, id := range c.ids {
			members = append(members, GroupMember{
				ID:       id,
				Topics:   c.topicsFor(id),
				UserData: udEncode(1, c.gens[id], c.claims[id]),
				Rack:     c.rackOf(id),
			})
		}

		// Twenty balances of byte-identical input. Any disagreement
		// between them is the map ordering showing through.
		base := planOwners(BalanceWithRacks(members, c.topics, c.partRacks))
		worst := 0
		loMoves, hiMoves := c.nparts(), 0
		for range 20 {
			owners := planOwners(BalanceWithRacks(members, c.topics, c.partRacks))
			var diff, moves int
			for tp, who := range owners {
				if base[tp] != who {
					diff++
				}
				if was, had := c.actual[tp]; had && was != who {
					moves++
				}
			}
			worst = max(worst, diff)
			loMoves, hiMoves = min(loMoves, moves), max(hiMoves, moves)
		}
		t.Logf("determinism/%s: %d of %d partitions land on a different member across repeated balances of identical input; those balances move %d..%d partitions off their previous consumer",
			s.name, worst, c.nparts(), loMoves, hiMoves)
		if worst > s.maxDiff {
			t.Errorf("determinism/%s: %d partitions differ across identical balances, above the %d allowance", s.name, worst, s.maxDiff)
		}
	}
}

// planOwners flattens a plan to partition => member.
func planOwners(plan Plan) map[topicPartition]string {
	owners := make(map[topicPartition]string)
	for member, topics := range plan {
		for topic, parts := range topics {
			for _, p := range parts {
				owners[topicPartition{topic, p}] = member
			}
		}
	}
	return owners
}

// planRackHits counts partitions sitting on a member in the leader's rack.
func planRackHits(b *balancer) int {
	var n int
	for memberNum := range b.plan {
		has := b.memberRacks[memberNum]
		if has == noRack {
			continue
		}
		for _, partNum := range b.plan[memberNum] {
			if b.partRacks[partNum] == has {
				n++
			}
		}
	}
	return n
}

// TestRackTradeSaturation asks whether one improveRackLocality pass takes
// every trade that is available to it. The pass walks each ordered rack pair
// once, merging the same-topic runs and then looking a short way ahead for a
// cross-topic partner, so it can leave a tradeable pair on the table. Running
// it repeatedly to a fixpoint says how much it left.
//
// The second question is how much locality is available at all: a from-scratch
// balance of the same cluster is the ceiling, and the gap to it is what
// stickiness costs.
func TestRackTradeSaturation(t *testing.T) {
	t.Parallel()

	const (
		ntopics  = 12
		nparts   = 30
		nmembers = 12
		nrounds  = 24
	)
	racks := []string{"a", "b", "c"}

	for _, s := range []struct {
		name      string
		leaderPct int
		event     func(c *decayCluster, n int)
	}{
		{name: "control_static"},
		{name: "leaders_move_10pct", leaderPct: 10},
		{
			name: "1out1in_same_rack",
			event: func(c *decayCluster, n int) {
				c.replaceKeepingRack(c.rng.Intn(len(c.ids)))
			},
		},
		{
			name: "1out1in_drifting_racks",
			event: func(c *decayCluster, n int) {
				c.dropMember(c.rng.Intn(len(c.ids)))
				c.addMember()
			},
		},
		{
			name:      "1out1in_same_rack_leaders_move_10pct",
			leaderPct: 10,
			event: func(c *decayCluster, n int) {
				c.replaceKeepingRack(c.rng.Intn(len(c.ids)))
			},
		},
	} {
		c := newDecayCluster(67, ntopics, nparts, nmembers, racks)
		c.rebalance(t, 0, true)

		t.Logf("racksat/%s\nround  noTrade  onePass  fixpoint  freshBalance   (percent of partitions on the leader's rack)", s.name)
		var sumOne, sumSat, sumFresh, sumNone float64
		for n := 1; n <= nrounds; n++ {
			c.moveLeaders(s.leaderPct)
			if s.event != nil {
				s.event(c, n)
			}
			members := make([]GroupMember, 0, len(c.ids))
			for _, id := range c.ids {
				members = append(members, GroupMember{
					ID:       id,
					Topics:   c.topicsFor(id),
					UserData: udEncode(1, c.gen, c.claims[id]),
					Rack:     c.rackOf(id),
				})
			}
			total := float64(c.nparts())

			b := newBalancer(members, c.topics, c.partRacks)
			b.parseMemberMetadata()
			b.assignUnassignedAndInitGraph()
			b.initPlanByNumPartitions()
			b.balance()
			none := 100 * float64(planRackHits(b)) / total
			b.improveRackLocality()
			one := 100 * float64(planRackHits(b)) / total
			for range 10 {
				b.improveRackLocality()
			}
			sat := 100 * float64(planRackHits(b)) / total

			fb := newBalancer(freshMembers(c), c.topics, c.partRacks)
			fb.parseMemberMetadata()
			fb.assignUnassignedAndInitGraph()
			fb.initPlanByNumPartitions()
			fb.balance()
			fb.improveRackLocality()
			fresh := 100 * float64(planRackHits(fb)) / total

			t.Logf("%5d %8.1f %8.1f %9.1f %13.1f", n, none, one, sat, fresh)
			sumNone, sumOne, sumSat, sumFresh = sumNone+none, sumOne+one, sumSat+sat, sumFresh+fresh

			// Carry the once-traded plan forward, which is what the
			// balancer actually emits.
			b2 := newBalancer(members, c.topics, c.partRacks)
			b2.parseMemberMetadata()
			b2.assignUnassignedAndInitGraph()
			b2.initPlanByNumPartitions()
			b2.balance()
			b2.improveRackLocality()
			plan := b2.into()
			next := make(map[string]map[string][]int32, len(plan))
			for member, topics := range plan {
				cp := make(map[string][]int32, len(topics))
				for topic, parts := range topics {
					cp[topic] = slices.Clone(parts)
				}
				next[member] = cp
			}
			c.claims = next
			c.gen++
		}
		nr := float64(nrounds)
		t.Logf("racksat/%s means: before trading %.1f%%, one pass %.1f%%, traded to a fixpoint %.1f%%, fresh balance %.1f%% (one pass leaves %.1f points on the table; stickiness costs %.1f points); final members per rack %v",
			s.name, sumNone/nr, sumOne/nr, sumSat/nr, sumFresh/nr, sumSat/nr-sumOne/nr, sumFresh/nr-sumOne/nr, c.rackSpread())
		if sumSat/nr-sumOne/nr > 0.01 {
			t.Errorf("racksat/%s: trading to a fixpoint reaches %.1f%% where one pass reaches %.1f%%; improveRackLocality is not taking every trade available to it",
				s.name, sumSat/nr, sumOne/nr)
		}
	}
}

// freshMembers is the cluster's members with no memory of a prior assignment.
func freshMembers(c *decayCluster) []GroupMember {
	members := make([]GroupMember, 0, len(c.ids))
	for _, id := range c.ids {
		members = append(members, GroupMember{
			ID:     id,
			Topics: c.topicsFor(id),
			Rack:   c.rackOf(id),
		})
	}
	return members
}

// randomSubset picks a nonempty subset of names, biased large so that groups
// stay mostly overlapping the way real ones do.
func randomSubset(rng *rand.Rand, names []string) []string {
	var out []string
	for _, n := range names {
		if rng.Intn(4) != 0 {
			out = append(out, n)
		}
	}
	if len(out) == 0 {
		out = append(out, names[rng.Intn(len(names))])
	}
	return out
}
