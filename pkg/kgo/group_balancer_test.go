package kgo

import (
	"reflect"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// This simple test hits every branch of cooperative-sticky's adjustCooperative
// by:
//
//  1. having partitions migrating from one member to another
//  2. having a whole topic migrate from one member to another
//  3. adding new partitions in the plan (new topic wanted for consuming, or an eager member)
//  4. completely deleting partitions from the plan (topic no longer wanted for consuming)
//  5. having a member that is still on eager
//  6. having two members that think they own the same partitions (similar to KIP-341)
//
// Thus while it is an ugly test, it is effective.
func Test_stickyAdjustCooperative(t *testing.T) {
	assn := func(in map[string][]int32) []kmsg.ConsumerMemberMetadataOwnedPartition {
		var ks []kmsg.ConsumerMemberMetadataOwnedPartition
		for topic, partitions := range in {
			ks = append(ks, kmsg.ConsumerMemberMetadataOwnedPartition{
				Topic:      topic,
				Partitions: partitions,
			})
		}
		return ks
	}

	b := &ConsumerBalancer{
		members: []kmsg.JoinGroupResponseMember{
			{MemberID: "a"},
			{MemberID: "b"},
			{MemberID: "c"},
			{MemberID: "d"},
		},
		metadatas: []kmsg.ConsumerMemberMetadata{
			{OwnedPartitions: assn(map[string][]int32{
				"t1":      {1, 2, 3, 4},
				"tmove":   {1, 2},
				"tdelete": {1, 2},
			})},
			{OwnedPartitions: assn(map[string][]int32{
				"t2": {1, 2, 3},
			})},
			{}, // eager member, nothing owned
			{OwnedPartitions: assn(map[string][]int32{
				"t1": {1, 2, 3, 4},
			})},
		},
	}

	inPlan := map[string]map[string][]int32{
		"a": {
			"t1":   {1, 4},
			"t2":   {3},
			"tnew": {1, 2},
		},
		"b": {
			"t2":    {2},
			"tnew":  {3, 4},
			"tmove": {1},
		},
		"c": {
			"t1":    {3},
			"t2":    {1},
			"tnew":  {5},
			"tmove": {2},
		},
		"d": {
			"t1": {2},
		},
	}

	expPlan := map[string]map[string][]int32{
		"a": {
			"t1":   {1, 4},
			"tnew": {1, 2},
		},
		"b": {
			"t2":   {2},
			"tnew": {3, 4},
		},
		"c": {
			"tnew": {5},
		},
		"d": {
			"t1": {2},
		},
	}

	(&BalancePlan{inPlan}).AdjustCooperative(b)

	if !reflect.DeepEqual(inPlan, expPlan) {
		t.Errorf("got plan != exp\ngot: %#v\nexp: %#v\n", inPlan, expPlan)
	}
}

func TestRangeBalancerRackAware(t *testing.T) {
	t.Parallel()

	rackA := "rackA"
	rackB := "rackB"

	members := []kmsg.JoinGroupResponseMember{
		{MemberID: "A"},
		{MemberID: "B"},
	}
	metadatas := []kmsg.ConsumerMemberMetadata{
		{Topics: []string{"t1"}, Rack: &rackA},
		{Topics: []string{"t1"}, Rack: &rackB},
	}

	rb := &rangeBalancer{}
	b := &ConsumerBalancer{
		b:         rb,
		members:   members,
		metadatas: metadatas,
		topics:    map[string]struct{}{"t1": {}},
		partitionRacks: map[string][]string{
			"t1": {"rackA", "rackB", "rackA", "rackB"},
		},
	}

	topics := map[string]int32{"t1": 4}
	plan := rb.Balance(b, topics)
	bp := plan.(*BalancePlan)

	// With rack-aware, A (rackA) should get rackA partitions (0,2)
	// and B (rackB) should get rackB partitions (1,3).
	aParts := bp.plan["A"]["t1"]
	bParts := bp.plan["B"]["t1"]
	if len(aParts) != 2 || len(bParts) != 2 {
		t.Fatalf("expected 2 partitions each, got A=%v B=%v", aParts, bParts)
	}

	for _, p := range aParts {
		if b.partitionRacks["t1"][p] != "rackA" {
			t.Errorf("A (rackA) got non-rackA partition %d", p)
		}
	}
	for _, p := range bParts {
		if b.partitionRacks["t1"][p] != "rackB" {
			t.Errorf("B (rackB) got non-rackB partition %d", p)
		}
	}
}

func TestRangeBalancerNoRacks(t *testing.T) {
	t.Parallel()
	// Without rack info, range balancer should work as before.
	members := []kmsg.JoinGroupResponseMember{
		{MemberID: "A"},
		{MemberID: "B"},
	}
	metadatas := []kmsg.ConsumerMemberMetadata{
		{Topics: []string{"t1"}},
		{Topics: []string{"t1"}},
	}

	rb := &rangeBalancer{}
	b := &ConsumerBalancer{
		b:         rb,
		members:   members,
		metadatas: metadatas,
		topics:    map[string]struct{}{"t1": {}},
	}

	topics := map[string]int32{"t1": 4}
	plan := rb.Balance(b, topics)
	bp := plan.(*BalancePlan)

	// Standard range: A gets [0,1], B gets [2,3].
	aParts := bp.plan["A"]["t1"]
	bParts := bp.plan["B"]["t1"]
	if !reflect.DeepEqual(aParts, []int32{0, 1}) {
		t.Errorf("expected A=[0,1], got %v", aParts)
	}
	if !reflect.DeepEqual(bParts, []int32{2, 3}) {
		t.Errorf("expected B=[2,3], got %v", bParts)
	}
}

// rangeBalance is a test helper that builds a ConsumerBalancer and runs the
// range balancer. Each member is {ID, Topics, Rack} as a simple struct.
type rangeMember struct {
	id     string
	topics []string
	rack   string // empty means no rack
}

func rangeBalance(members []rangeMember, topics map[string]int32, partitionRacks map[string][]string) *BalancePlan {
	rb := &rangeBalancer{}
	jMembers := make([]kmsg.JoinGroupResponseMember, len(members))
	metas := make([]kmsg.ConsumerMemberMetadata, len(members))
	allTopics := make(map[string]struct{})
	for i, m := range members {
		jMembers[i] = kmsg.JoinGroupResponseMember{MemberID: m.id}
		metas[i] = kmsg.ConsumerMemberMetadata{Topics: m.topics}
		if m.rack != "" {
			r := m.rack
			metas[i].Rack = &r
		}
		for _, t := range m.topics {
			allTopics[t] = struct{}{}
		}
	}
	b := &ConsumerBalancer{
		b:              rb,
		members:        jMembers,
		metadatas:      metas,
		topics:         allTopics,
		partitionRacks: partitionRacks,
	}
	return rb.Balance(b, topics).(*BalancePlan)
}

func TestRangeBalancerRackNoMatchingRacks(t *testing.T) {
	t.Parallel()
	// Partition racks don't match any consumer rack: phase 1 assigns
	// nothing, phase 2 does standard range.
	members := []rangeMember{
		{"A", []string{"t1"}, "rackX"},
		{"B", []string{"t1"}, "rackY"},
	}
	bp := rangeBalance(members, map[string]int32{"t1": 4}, map[string][]string{
		"t1": {"rackA", "rackB", "rackA", "rackB"},
	})

	// Falls through to standard range: A=[0,1], B=[2,3].
	if !reflect.DeepEqual(bp.plan["A"]["t1"], []int32{0, 1}) {
		t.Errorf("A: want [0,1], got %v", bp.plan["A"]["t1"])
	}
	if !reflect.DeepEqual(bp.plan["B"]["t1"], []int32{2, 3}) {
		t.Errorf("B: want [2,3], got %v", bp.plan["B"]["t1"])
	}
}

func TestRangeBalancerRackMixedMemberRacks(t *testing.T) {
	t.Parallel()
	// A has a rack, B has no rack. Phase 1 assigns rack-matched
	// partitions to A up to quota. B gets remainder in phase 2.
	members := []rangeMember{
		{"A", []string{"t1"}, "rackA"},
		{"B", []string{"t1"}, ""},
	}
	bp := rangeBalance(members, map[string]int32{"t1": 4}, map[string][]string{
		"t1": {"rackA", "rackB", "rackA", "rackB"},
	})

	// A's quota is 2. Phase 1: A gets p0(rackA), p2(rackA) = 2.
	// Phase 2: B gets p1, p3 (the unassigned ones).
	aParts := bp.plan["A"]["t1"]
	bParts := bp.plan["B"]["t1"]
	if len(aParts) != 2 || len(bParts) != 2 {
		t.Fatalf("want 2 each, got A=%v B=%v", aParts, bParts)
	}
	// A should have the rackA partitions (0 and 2).
	for _, p := range aParts {
		rack := map[string][]string{"t1": {"rackA", "rackB", "rackA", "rackB"}}["t1"][p]
		if rack != "rackA" {
			t.Errorf("A(rackA) got partition %d with rack %s", p, rack)
		}
	}
}

func TestRangeBalancerRackShortRackSlice(t *testing.T) {
	t.Parallel()
	// partitionRacks has fewer entries than the actual partition count.
	// Partitions beyond the rack slice should fall through to phase 2.
	members := []rangeMember{
		{"A", []string{"t1"}, "rackA"},
		{"B", []string{"t1"}, "rackB"},
	}
	bp := rangeBalance(members, map[string]int32{"t1": 6}, map[string][]string{
		"t1": {"rackA", "rackB"}, // only 2 entries for 6 partitions
	})

	// Phase 1: A gets p0(rackA), B gets p1(rackB). Each has 1/3 quota used.
	// Phase 2: remaining 4 partitions distributed by range.
	// A quota=3, used=1, needs 2 more. B quota=3, used=1, needs 2 more.
	aTotal := len(bp.plan["A"]["t1"])
	bTotal := len(bp.plan["B"]["t1"])
	if aTotal != 3 || bTotal != 3 {
		t.Errorf("want A=3 B=3, got A=%d B=%d", aTotal, bTotal)
	}
}

func TestRangeBalancerRackOddPartitions(t *testing.T) {
	t.Parallel()
	// 5 partitions, 2 consumers: div=2, rem=1. First consumer gets 3.
	// With racks: the remainder partition should still respect range order.
	members := []rangeMember{
		{"A", []string{"t1"}, "rackA"},
		{"B", []string{"t1"}, "rackB"},
	}
	bp := rangeBalance(members, map[string]int32{"t1": 5}, map[string][]string{
		"t1": {"rackA", "rackB", "rackA", "rackB", "rackA"},
	})

	// A(rackA) quota=3: phase 1 grabs p0,p2,p4 (all rackA). Done.
	// B(rackB) quota=2: phase 1 grabs p1,p3 (both rackB). Done.
	aParts := bp.plan["A"]["t1"]
	bParts := bp.plan["B"]["t1"]
	if len(aParts) != 3 || len(bParts) != 2 {
		t.Fatalf("want A=3 B=2, got A=%v B=%v", aParts, bParts)
	}
}

func TestRangeBalancerRackMultiTopicDisjoint(t *testing.T) {
	t.Parallel()
	// Two topics with different subscriptions and different rack layouts.
	// t1: A,B subscribe; all partitions rackA.
	// t2: B,C subscribe; all partitions rackB.
	members := []rangeMember{
		{"A", []string{"t1"}, "rackA"},
		{"B", []string{"t1", "t2"}, "rackB"},
		{"C", []string{"t2"}, "rackA"},
	}
	bp := rangeBalance(members,
		map[string]int32{"t1": 4, "t2": 4},
		map[string][]string{
			"t1": {"rackA", "rackA", "rackA", "rackA"},
			"t2": {"rackB", "rackB", "rackB", "rackB"},
		},
	)

	// t1: A(rackA) quota=2, gets p0,p1 via phase 1. B(rackB) quota=2,
	//     no rack match in phase 1, gets p2,p3 via phase 2.
	// t2: B(rackB) quota=2, gets p0,p1 via phase 1. C(rackA) quota=2,
	//     no rack match, gets p2,p3 via phase 2.
	if len(bp.plan["A"]["t1"]) != 2 {
		t.Errorf("A t1: want 2, got %v", bp.plan["A"]["t1"])
	}
	if len(bp.plan["B"]["t1"]) != 2 {
		t.Errorf("B t1: want 2, got %v", bp.plan["B"]["t1"])
	}
	if len(bp.plan["B"]["t2"]) != 2 {
		t.Errorf("B t2: want 2, got %v", bp.plan["B"]["t2"])
	}
	if len(bp.plan["C"]["t2"]) != 2 {
		t.Errorf("C t2: want 2, got %v", bp.plan["C"]["t2"])
	}
}

func TestNewConsumerBalancerIssue493(t *testing.T) {
	m := kmsg.NewConsumerMemberMetadata()
	m.Version = 0
	m.Topics = []string{"foo"}
	protoMeta := m.AppendTo(nil)
	protoMeta[1] = 1
	member := kmsg.NewJoinGroupResponseMember()
	member.MemberID = "test"
	member.ProtocolMetadata = protoMeta
	_, err := NewConsumerBalancer(nil, []kmsg.JoinGroupResponseMember{member})
	if err != nil {
		t.Errorf("got unexpected error: %v", err)
	}
}
