package sticky

import (
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// A member that lost a partition to a higher-generation claimant is recorded
// in b.stales. If the higher-generation claimant no longer subscribes to the
// topic, the un-map loop leaves the partition unassigned -- the stale entry
// must then hand the partition back to the stale member, not index
// b.plan[unassignedPart].
func TestStaleUnassignedRestickies(t *testing.T) {
	t.Parallel()

	members := []GroupMember{
		{ID: "a", Topics: []string{"t"}, UserData: newUD().setGeneration(1).assign("t", 0).encode()},
		{ID: "b", Topics: []string{"u"}, UserData: newUD().setGeneration(2).assign("t", 0).encode()},
	}

	plan := Balance(members, map[string]int32{"t": 1, "u": 1})

	if got := plan["a"]["t"]; len(got) != 1 || got[0] != 0 {
		t.Errorf("expected a to be re-stickied t/0, got %v", got)
	}
	if got := plan["b"]["u"]; len(got) != 1 || got[0] != 0 {
		t.Errorf("expected b to keep u/0, got %v", got)
	}
}

// When tryRestickyStales moves a partition from the current owner back to the
// stale member, the partitionConsumers slice that later seeds the complex
// path's steal graph must move with it. If it does not, a steal of the moved
// partition resolves to the old owner, and memberPartitions.remove on a list
// that does not contain the partition silently removes a different partition:
// the emitted plan then assigns one partition to two members and another to
// none.
func TestStaleMoveKeepsGraphConsistent(t *testing.T) {
	t.Parallel()

	members := []GroupMember{
		{ID: "a", Topics: []string{"t"}, UserData: newUD().setGeneration(1).assign("t", 0).encode()},
		{ID: "b", Topics: []string{"t"}, UserData: newUD().setGeneration(2).assign("t", 0, 1, 2).encode()},
		{ID: "c", Topics: []string{"t", "u"}}, // u forces the complex path
	}

	plan := Balance(members, map[string]int32{"t": 3, "u": 0})

	seen := make(map[int32]string)
	for member, topics := range plan {
		for _, p := range topics["t"] {
			if prev, ok := seen[p]; ok {
				t.Errorf("t/%d assigned to both %s and %s", p, prev, member)
			}
			seen[p] = member
		}
	}
	for p := range int32(3) {
		if _, ok := seen[p]; !ok {
			t.Errorf("t/%d assigned to nobody", p)
		}
	}
}

// One stale entry whose last owner no longer subscribes to the claimed topic
// must not abort processing of the other stale entries. b.stales is a map, so
// an early return makes WHICH stales were re-stickied iteration-order random:
// the same input balances differently across runs. We loop to make the
// pre-fix order-dependence overwhelming.
func TestStaleSkipDoesNotAbortOthers(t *testing.T) {
	t.Parallel()

	for i := range 64 {
		members := []GroupMember{
			// c2 and a both subscribe to v and are equally loaded; if
			// the v/0 stale entry is skipped, the unassigned pass
			// tie-breaks v/0 onto c2 (first member) instead.
			{ID: "c2", Topics: []string{"v", "z"}, UserData: newUD().setGeneration(2).assign("z", 0).encode()},
			{ID: "a", Topics: []string{"v", "w"}, UserData: newUD().setGeneration(1).assign("w", 0).assign("v", 0).encode()},
			// x claims t/0 at an old generation but no longer
			// subscribes to t: its stale entry is unsatisfiable.
			{ID: "x", Topics: []string{"xw"}, UserData: newUD().setGeneration(1).assign("t", 0).assign("xw", 0).encode()},
			{ID: "y", Topics: []string{"t"}, UserData: newUD().setGeneration(2).assign("t", 0).encode()},
			// bq claims v/0 at generation 2 but no longer subscribes
			// to v, leaving v/0 unassigned with a stale entry for a.
			{ID: "bq", Topics: []string{"q"}, UserData: newUD().setGeneration(2).assign("v", 0).assign("q", 0).encode()},
		}

		plan := Balance(members, map[string]int32{
			"v": 1, "z": 1, "w": 1, "q": 1, "t": 1, "xw": 1,
		})

		if got := plan["a"]["v"]; len(got) != 1 || got[0] != 0 {
			t.Fatalf("iter %d: expected a to be re-stickied v/0, got %v (full plan %v)", i, got, plan)
		}
	}
}

// Member metadata is arbitrary input from other group members: a negative
// claimed partition must be ignored, not index partition state at a negative
// offset (leader panic) or alias into a neighboring topic's partition range.
func TestNegativeClaimedPartitionNoPanic(t *testing.T) {
	t.Parallel()

	members := []GroupMember{
		{
			ID: "a", Topics: []string{"t"},
			Cooperative: true, Generation: 1,
			Owned: []kmsg.ConsumerMemberMetadataOwnedPartition{
				{Topic: "t", Partitions: []int32{-1, 0}},
			},
		},
		{ID: "b", Topics: []string{"t"}, UserData: newUD().setGeneration(1).assign("t", -2).encode()},
	}

	plan := Balance(members, map[string]int32{"t": 2})

	var total int
	for _, topics := range plan {
		total += len(topics["t"])
	}
	if total != 2 {
		t.Errorf("expected both partitions assigned exactly once, got plan %v", plan)
	}
}
