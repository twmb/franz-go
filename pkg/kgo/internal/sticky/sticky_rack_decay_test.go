package sticky

import (
	"fmt"
	"math/rand"
	"testing"
)

// TestRackLocalityDecay guards against rack placement rotting away.
//
// Rack placement is only ever applied to partitions that are unassigned. A
// partition carried over from the previous assignment keeps its owner without
// being re-checked. Meanwhile leadership genuinely moves between brokers --
// rolling restarts, preferred leader election, reassignment -- so a partition
// that was rack-correct when assigned can silently become wrong.
//
// Before the repair priced rack placement, locality fell from 96% to 33% over
// 25 rebalances at 10% leadership movement -- 33% being what random assignment
// gives with three racks, so it decayed the whole way to no rack awareness at
// all. The floors below are well under what the repair achieves, so they catch
// that regression without pinning exact numbers.

func TestRackLocalityDecay(t *testing.T) {
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
		minLocality   float64
	}{
		{"leaders_move_2pct", 2, false, 75},
		{"leaders_move_10pct", 10, false, 65},
		{"leaders_move_10pct+member_churn", 10, true, 65},
		{"static_cluster", 0, false, 90},
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
		first := 100 * float64(countRackMatches(plan, members, partitionRacks)) / float64(total)

		var last float64
		nextID := nmembers
		for round := range nrounds {
			// Leadership moves: some partitions end up led from a
			// different rack than the one they were placed for.
			for _, topic := range all {
				rs := partitionRacks[topic]
				for i := range rs {
					if rng.Intn(100) < churn.leaderMovePct {
						rs[i] = racks[rng.Intn(len(racks))]
					}
				}
			}
			// Optionally one member leaves and another joins.
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

			next := make([]GroupMember, 0, len(members))
			for _, m := range members {
				m.UserData = udEncode(1, 1, plan[m.ID])
				next = append(next, m)
			}
			members = next
			plan = BalanceWithRacks(members, topics, partitionRacks)
			last = 100 * float64(countRackMatches(plan, members, partitionRacks)) / float64(total)
		}

		// What a fresh balance of the final cluster state would achieve --
		// the locality that is available if nothing were carried over.
		fresh := make([]GroupMember, 0, len(members))
		for _, m := range members {
			m.UserData = nil
			fresh = append(fresh, m)
		}
		freshPlan := BalanceWithRacks(fresh, topics, partitionRacks)
		avail := 100 * float64(countRackMatches(freshPlan, fresh, partitionRacks)) / float64(total)

		t.Logf("%-32s first=%5.1f%%  after %d rounds=%5.1f%%  a fresh balance would get=%5.1f%%  (forgone %4.1f points)",
			churn.name, first, nrounds, last, avail, avail-last)
		if last < churn.minLocality {
			t.Errorf("%s: locality fell to %.1f%%, below the %.1f%% floor; rack placement is rotting across rebalances",
				churn.name, last, churn.minLocality)
		}

		// Trading partitions must never change how many anybody holds.
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
			t.Errorf("%s: partition counts span %d..%d; trading broke the balance it must preserve", churn.name, lo, hi)
		}
	}
}
