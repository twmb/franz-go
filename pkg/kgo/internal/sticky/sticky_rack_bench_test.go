package sticky

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"
)

// improveRackLocality is measured directly rather than as a delta on a whole
// balance: a balance of this size allocates enough that its own collector
// noise swamps a pass this small. Cost tracks how much drift there is to
// repair, which is the point -- a group whose leaders have not moved pays
// almost nothing.

func benchRackSetup(nt, np, nm, mismatchPct int) (*balancer, [][]int32) {
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

	// Deliberately scramble some placements so the pass has real work,
	// standing in for leadership having moved since the last rebalance.
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

func BenchmarkImproveRackLocality(b *testing.B) {
	for _, sz := range []struct{ nt, np, nm, mismatch int }{
		{100, 250, 100, 0},  // nothing to do -- the steady state
		{100, 250, 100, 10}, // a rolling restart's worth of drift
		{100, 250, 100, 50}, // badly decayed
		{20, 1250, 100, 10}, // same partitions, fewer topics
	} {
		name := fmt.Sprintf("t%d_p%d_m%d_drift%d", sz.nt, sz.np, sz.nm, sz.mismatch)
		bal, snapshot := benchRackSetup(sz.nt, sz.np, sz.nm, sz.mismatch)
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				for i := range snapshot {
					bal.plan[i] = append(bal.plan[i][:0], snapshot[i]...)
				}
				b.StartTimer()
				bal.improveRackLocality()
			}
		})
	}
}

// For scale: what the whole balance costs on the same shapes.
func BenchmarkWholeBalanceForScale(b *testing.B) {
	for _, sz := range []struct{ nt, np, nm int }{
		{100, 250, 100},
		{20, 1250, 100},
	} {
		rng := rand.New(rand.NewSource(3))
		racks := []string{"a", "b", "c"}
		topics := make(map[string]int32, sz.nt)
		var all []string
		for i := range sz.nt {
			n := fmt.Sprintf("topic-%d", i)
			topics[n] = int32(sz.np)
			all = append(all, n)
		}
		partitionRacks := make(map[string][]string, sz.nt)
		for _, topic := range all {
			rs := make([]string, sz.np)
			for i := range rs {
				rs[i] = racks[rng.Intn(len(racks))]
			}
			partitionRacks[topic] = rs
		}
		var members []GroupMember
		for i := range sz.nm {
			members = append(members, GroupMember{
				ID: fmt.Sprintf("c-%d", i), Topics: all, Rack: racks[i%len(racks)],
			})
		}
		b.Run(fmt.Sprintf("t%d_p%d_m%d", sz.nt, sz.np, sz.nm), func(b *testing.B) {
			for b.Loop() {
				BalanceWithRacks(members, topics, partitionRacks)
			}
		})
	}
}
