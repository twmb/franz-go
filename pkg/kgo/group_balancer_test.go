package kgo

import (
	"testing"

	"github.com/google/go-cmp/cmp"

	"github.com/twmb/kafka-go/pkg/kmsg"
)

// This simple test hits every branch of cooperative-sticky's adjustCooperative
// by:
//
//   1) having partitions migrating from one member to another
//   2) having a whole topic migrate from one member to another
//   3) adding new partitions in the plan (new topic wanted for consuming, or an eager member)
//   4) completely deleting partitions from the plan (topic no longer wanted for consuming)
//   5) having a member that is still on eager
//   6) having two members that think they own the same partitions (similar to KIP-341)
//
// Thus while it is an ugly test, it is effective.
func Test_stickyAdjustCooperative(t *testing.T) {
	id := func(name string) groupMemberID { return groupMemberID{memberID: name} }
	assn := func(in map[string][]int32) []kmsg.GroupMemberMetadataOwnedPartition {
		var ks []kmsg.GroupMemberMetadataOwnedPartition
		for topic, partitions := range in {
			ks = append(ks, kmsg.GroupMemberMetadataOwnedPartition{
				Topic:      topic,
				Partitions: partitions,
			})
		}
		return ks
	}

	members := []groupMember{
		{id: id("a"),
			owned: assn(map[string][]int32{
				"t1":      {1, 2, 3, 4},
				"tmove":   {1, 2},
				"tdelete": {1, 2},
			})},

		{id: id("b"),
			owned: assn(map[string][]int32{
				"t2": {1, 2, 3},
			})},

		{id: id("c")}, // eager member: nothing owned

		{id: id("d"), // also thinks it owned t1 (similar to KIP-341)
			owned: assn(map[string][]int32{
				"t1": {1, 2, 3, 4},
			})},
	}

	inPlan := map[groupMemberID]map[string][]int32{
		id("a"): map[string][]int32{
			"t1":   {1, 4},
			"t2":   {3},
			"tnew": {1, 2},
		},
		id("b"): map[string][]int32{
			"t2":    {2},
			"tnew":  {3, 4},
			"tmove": {1},
		},
		id("c"): map[string][]int32{
			"t1":    {3},
			"t2":    {1},
			"tnew":  {5},
			"tmove": {2},
		},
		id("d"): map[string][]int32{
			"t1": {2},
		},
	}

	expPlan := map[groupMemberID]map[string][]int32{
		id("a"): map[string][]int32{
			"t1":   {1, 4},
			"tnew": {1, 2},
		},
		id("b"): map[string][]int32{
			"t2":   {2},
			"tnew": {3, 4},
		},
		id("c"): map[string][]int32{
			"tnew": {5},
		},
		id("d"): map[string][]int32{
			"t1": {2},
		},
	}

	(new(stickyBalancer)).adjustCooperative(members, inPlan)

	if diff := cmp.Diff(inPlan, expPlan, cmp.AllowUnexported()); diff != "" {
		t.Error(diff)
	}
}
