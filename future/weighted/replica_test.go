package weighted

import (
	"math/rand"
	"testing"
	"time"
)

// Replica placement, which is the shape CockroachDB actually has: every range
// is stored several times over, on distinct stores, spread across localities.
//
// The reduction is the point. A range needing one replica in each of three
// localities is three placements, each choosing a store within its own
// locality, and stores belong to exactly one locality -- so replicas of a range
// land on distinct stores without anything having to enforce it. Each placement
// is one item whose eligible set is the stores of its locality, which is
// precisely the model already built. Replicas do not need new machinery; they
// need the eligible sets to be locality memberships.
//
// What that means for the flow is that it survives: the constraint coupling a
// range's replicas is discharged by the partition, not carried into the cost,
// so the cost stays per placement and per store and the problem stays one the
// flow can express.
func replicaCluster(rng *rand.Rand, nranges, nloc, perLoc, replicas int, narrow bool) (*Instance, []int) {
	nstores := nloc * perLoc
	in := &Instance{Workers: nstores}
	var start []int

	storesOf := func(l int) []int {
		var s []int
		for i := range perLoc {
			s = append(s, l*perLoc+i)
		}
		return s
	}

	for range nranges {
		size := int64(64 + rng.Intn(64))
		if rng.Intn(5) == 0 {
			size = int64(1 + rng.Intn(64))
		}
		// One replica in each of the first `replicas` localities.
		for l := range replicas {
			el := storesOf(l)
			if narrow {
				// Disk class or capacity narrows it further, which is
				// what a heterogeneous cluster looks like.
				w := max(2, perLoc/3)
				lo := rng.Intn(perLoc - w + 1)
				el = el[lo : lo+w]
			}
			home := el[0]
			if rng.Intn(100) < 40 {
				home = el[rng.Intn(len(el))]
			}
			in.Items = append(in.Items, Item{Weight: size, Eligible: el, Prior: home})
			start = append(start, home)
		}
	}
	return in, start
}

func TestReplicaPlacement(t *testing.T) {
	for _, narrow := range []bool{false, true} {
		for _, tc := range []struct{ nranges, nloc, perLoc, replicas int }{
			{1500, 3, 8, 3},
			{4000, 3, 16, 3},
		} {
			rng := rand.New(rand.NewSource(5))
			in, start := replicaCluster(rng, tc.nranges, tc.nloc, tc.perLoc, tc.replicas, narrow)

			s := time.Now()
			sm, _ := SingleMoveScheduler(in, start, 0)
			smT := time.Since(s)
			s = time.Now()
			fs := FlowThenShave(in, start)
			fsT := time.Since(s)

			kind := "any store in locality"
			if narrow {
				kind = "narrowed within locality"
			}
			t.Logf("%-26s | %d ranges x%d replicas over %d stores | single-move %.4f/%-8v moves=%-5d | flow+shave %.4f/%-8v moves=%-5d",
				kind, tc.nranges, tc.replicas, tc.nloc*tc.perLoc,
				liveImbalance(in, sm), smT.Round(time.Millisecond), MovesFrom(start, sm),
				liveImbalance(in, fs), fsT.Round(time.Millisecond), MovesFrom(start, fs))

			// Replicas of one range must be on distinct stores; check the
			// partition really does guarantee it.
			for r := range tc.nranges {
				seen := map[int]bool{}
				for k := range tc.replicas {
					s := fs[r*tc.replicas+k]
					if seen[s] {
						t.Fatalf("range %d has two replicas on store %d", r, s)
					}
					seen[s] = true
				}
			}
		}
	}
}
