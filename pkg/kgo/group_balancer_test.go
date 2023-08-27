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
