package sticky

import (
	"fmt"
	"math/rand"
	"slices"
	"testing"
)

// TestOwnerIndexStaysConsistent checks that the incrementally maintained
// per-topic owner index still describes the real assignment after a balance
// has moved partitions around.
//
// The index is only ever read mid-search, so a maintenance bug would not show
// up as a wrong plan -- it would show up as the search quietly failing to see
// an edge, which the balance-quality tests can only catch by luck. This
// compares it against cxns directly.
func TestOwnerIndexStaysConsistent(t *testing.T) {
	t.Parallel()

	var complexBalances int
	for seed := int64(0); seed < 2000; seed++ {
		rng := rand.New(rand.NewSource(seed))

		ntopics := 1 + rng.Intn(6)
		topics := make(map[string]int32, ntopics)
		var names []string
		for i := range ntopics {
			name := fmt.Sprintf("t%d", i)
			topics[name] = int32(1 + rng.Intn(8))
			names = append(names, name)
		}

		nmembers := 2 + rng.Intn(6)
		members := make([]GroupMember, nmembers)
		for i := range nmembers {
			var subs []string
			for _, name := range names {
				if rng.Intn(2) == 0 {
					subs = append(subs, name)
				}
			}
			if len(subs) == 0 {
				subs = []string{names[rng.Intn(len(names))]}
			}
			// Give some members a prior plan so partitions start spread
			// unevenly and the balance actually has to move things.
			prior := make(map[string][]int32)
			for _, topic := range subs {
				if rng.Intn(3) == 0 {
					for p := int32(0); p < topics[topic]; p++ {
						if rng.Intn(2) == 0 {
							prior[topic] = append(prior[topic], p)
						}
					}
				}
			}
			members[i] = GroupMember{
				ID:       fmt.Sprintf("m%d", i),
				Topics:   subs,
				UserData: udEncode(1, 1, prior),
			}
		}

		b := newBalancer(members, topics, nil)
		if cap(b.partOwners) == 0 {
			continue
		}
		b.parseMemberMetadata()
		b.assignUnassignedAndInitGraph()
		b.initPlanByNumPartitions()
		b.balance()
		if !b.isComplex {
			continue
		}
		complexBalances++

		g := &b.stealGraph
		type key struct {
			topic  uint32
			member uint16
		}
		got := make(map[key][]int32)
		for topicNum, owners := range g.topicOwners {
			for _, o := range owners {
				for _, edge := range o.free {
					if g.cxns[edge].originalNum == o.member {
						t.Fatalf("seed %d: part %d is in %d's free bucket but %d started with it", seed, edge, o.member, o.member)
					}
					got[key{uint32(topicNum), o.member}] = append(got[key{uint32(topicNum), o.member}], edge)
				}
				for _, edge := range o.owned {
					if g.cxns[edge].originalNum != o.member {
						t.Fatalf("seed %d: part %d is in %d's owned bucket but %d did not start with it", seed, edge, o.member, o.member)
					}
					got[key{uint32(topicNum), o.member}] = append(got[key{uint32(topicNum), o.member}], edge)
				}
			}
		}

		want := make(map[key][]int32)
		for edge, cxn := range g.cxns {
			if cxn.memberNum == unassignedPart {
				continue
			}
			k := key{b.partOwners[edge], cxn.memberNum}
			want[k] = append(want[k], int32(edge))
		}

		if len(got) != len(want) {
			t.Fatalf("seed %d: index has %d (topic, owner) pairs, assignment has %d", seed, len(got), len(want))
		}
		for k, wantParts := range want {
			gotParts := got[k]
			slices.Sort(gotParts)
			slices.Sort(wantParts)
			if !slices.Equal(gotParts, wantParts) {
				t.Fatalf("seed %d: topic %d member %d: index has %v, assignment has %v", seed, k.topic, k.member, gotParts, wantParts)
			}
		}
	}

	if complexBalances < 100 {
		t.Errorf("only %d complex balances exercised; the index was barely tested", complexBalances)
	}
	t.Logf("index verified against the assignment after %d complex balances", complexBalances)
}
