package sticky

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"
)

// TestRackLocalityDecay rebalances a group 25 times while partition leaders
// move between racks, and requires that rack locality ends exactly where a
// fresh balance of the final cluster would put it. Before the repair, only
// partitions arriving unassigned were ever placed by rack, and locality
// decayed to what random placement gives.
func TestRackLocalityDecay(t *testing.T) {
	t.Parallel()

	const (
		ntopics  = 12
		nparts   = 30
		nmembers = 12
		nrounds  = 25
	)
	racks := []string{"a", "b", "c"}

	for _, churn := range []struct {
		name          string
		leaderMovePct int // percent of partitions whose leader moves each round
		memberChurn   bool
	}{
		{"leaders_move_2pct", 2, false},
		{"leaders_move_10pct", 10, false},
		{"leaders_move_10pct+member_churn", 10, true},
		{"static_cluster", 0, false},
	} {
		rng := rand.New(rand.NewSource(7))

		topics := make(map[string]int32, ntopics)
		var all []string
		for i := range ntopics {
			name := fmt.Sprintf("t%d", i)
			topics[name] = nparts
			all = append(all, name)
		}
		partitionRacks := make(map[string][]string, ntopics)
		for _, topic := range all {
			rs := make([]string, nparts)
			for i := range rs {
				rs[i] = racks[rng.Intn(len(racks))]
			}
			partitionRacks[topic] = rs
		}

		members := make([]GroupMember, 0, nmembers)
		for i := range nmembers {
			members = append(members, GroupMember{
				ID:     fmt.Sprintf("c-%d", i),
				Topics: all,
				Rack:   racks[i%len(racks)],
			})
		}

		plan := BalanceWithRacks(members, topics, partitionRacks)
		total := ntopics * nparts
		first := countRackMatches(plan, members, partitionRacks)

		nextID := nmembers
		for round := range nrounds {
			for _, topic := range all {
				rs := partitionRacks[topic]
				for i := range rs {
					if rng.Intn(100) < churn.leaderMovePct {
						rs[i] = racks[rng.Intn(len(racks))]
					}
				}
			}
			if churn.memberChurn && round%5 == 4 {
				drop := rng.Intn(len(members))
				members = append(members[:drop], members[drop+1:]...)
				members = append(members, GroupMember{
					ID:     fmt.Sprintf("c-%d", nextID),
					Topics: all,
					Rack:   racks[nextID%len(racks)],
				})
				nextID++
			}

			for i := range members {
				members[i].UserData = udEncode(1, 1, plan[members[i].ID])
			}
			plan = BalanceWithRacks(members, topics, partitionRacks)
		}
		last := countRackMatches(plan, members, partitionRacks)

		fresh := make([]GroupMember, 0, len(members))
		for _, m := range members {
			m.UserData = nil
			fresh = append(fresh, m)
		}
		avail := countRackMatches(BalanceWithRacks(fresh, topics, partitionRacks), fresh, partitionRacks)

		t.Logf("%-32s first=%d/%d after %d rounds=%d fresh=%d", churn.name, first, total, nrounds, last, avail)
		if last != avail {
			t.Errorf("%s: %d of %d rack local after rebalances, a fresh balance gets %d", churn.name, last, total, avail)
		}

		counts := make(map[string]int, len(members))
		for _, m := range members {
			for _, ps := range plan[m.ID] {
				counts[m.ID] += len(ps)
			}
		}
		lo, hi := total, 0
		for _, n := range counts {
			lo, hi = min(lo, n), max(hi, n)
		}
		if hi-lo > 1 {
			t.Errorf("%s: partition counts span %d..%d", churn.name, lo, hi)
		}
	}
}

func benchRepairSetup(nt, np, nm, mismatchPct int) (*balancer, [][]int32) {
	rng := rand.New(rand.NewSource(3))
	racks := []string{"a", "b", "c"}

	topics := make(map[string]int32, nt)
	var all []string
	for i := range nt {
		name := fmt.Sprintf("topic-%d", i)
		topics[name] = int32(np)
		all = append(all, name)
	}
	partitionRacks := make(map[string][]string, nt)
	for _, topic := range all {
		rs := make([]string, np)
		for i := range rs {
			rs[i] = racks[rng.Intn(len(racks))]
		}
		partitionRacks[topic] = rs
	}
	members := make([]GroupMember, 0, nm)
	for i := range nm {
		members = append(members, GroupMember{
			ID: fmt.Sprintf("c-%d", i), Topics: all, Rack: racks[i%len(racks)],
		})
	}

	b := newBalancer(members, topics, partitionRacks)
	b.parseMemberMetadata()
	b.assignUnassignedAndInitGraph()
	b.initPlanByNumPartitions()
	b.balance()

	// Scramble some leader racks, standing in for leadership having moved
	// since the last rebalance.
	for i := range b.partRacks {
		if rng.Intn(100) < mismatchPct {
			b.partRacks[i] = uint16(1 + rng.Intn(len(racks)))
		}
	}

	snapshot := make([][]int32, len(b.plan))
	for i := range b.plan {
		snapshot[i] = slices.Clone(b.plan[i])
	}
	return b, snapshot
}

// BenchmarkRepair measures the repair on its own, since a whole balance
// allocates enough that its collector noise swamps a pass this small.
func BenchmarkRepair(b *testing.B) {
	for _, sz := range []struct{ nt, np, nm, mismatch int }{
		{100, 250, 100, 0},  // nothing to do
		{100, 250, 100, 10}, // a rolling restart's worth of drift
		{100, 250, 100, 50}, // badly decayed
		{20, 1250, 100, 10}, // same partitions, fewer topics
	} {
		name := fmt.Sprintf("t%d_p%d_m%d_drift%d", sz.nt, sz.np, sz.nm, sz.mismatch)
		bal, snapshot := benchRepairSetup(sz.nt, sz.np, sz.nm, sz.mismatch)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				for i := range snapshot {
					bal.plan[i] = append(bal.plan[i][:0], snapshot[i]...)
				}
				b.StartTimer()
				bal.repairAssignment()
			}
		})
	}
}
