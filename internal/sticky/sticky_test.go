package sticky

import (
	"fmt"
	"testing"
)

func Test_stickyBalanceStrategy_Plan(t *testing.T) {
	for _, test := range []struct {
		name    string
		members []GroupMember
		topics  map[string][]int32
	}{
		{
			name: "one consumer, no topics",
			members: []GroupMember{
				{ID: "A"},
			},
		},

		{
			name: "one consumer, non existing topic",
			members: []GroupMember{
				{ID: "A", Topics: []string{"t1"}},
			},
		},

		{
			name: "one consumer, one topic",
			members: []GroupMember{
				{ID: "A", Topics: []string{"t1"}},
			},
			topics: map[string][]int32{
				"t1": []int32{0, 1, 2},
			},
		},

		{
			name: "only assigns from subscribed topics",
			members: []GroupMember{
				{ID: "A", Topics: []string{"t1"}},
			},
			topics: map[string][]int32{
				"t1": []int32{0, 1, 2},
				"t2": []int32{0, 1, 2},
			},
		},

		{
			name: "one consumer multiple topics",
			members: []GroupMember{
				{ID: "A", Topics: []string{"t1", "t2"}},
			},
			topics: map[string][]int32{
				"t1": []int32{0},
				"t2": []int32{0, 1},
			},
		},

		{
			name: "two consumers, one topic with one partition",
			members: []GroupMember{
				{ID: "A", Topics: []string{"t1"}},
				{ID: "B", Topics: []string{"t1"}},
			},
			topics: map[string][]int32{
				"t1": []int32{0},
			},
		},

		{
			name: "two consumers, one topic with two partitions",
			members: []GroupMember{
				{ID: "A", Topics: []string{"t1"}},
				{ID: "B", Topics: []string{"t1"}},
			},
			topics: map[string][]int32{
				"t1": []int32{0, 1},
			},
		},

		{
			name: "multiple consumers with mixed topic subscriptions",
			members: []GroupMember{
				{ID: "A", Topics: []string{"t1"}},
				{ID: "B", Topics: []string{"t1", "t2"}},
				{ID: "C", Topics: []string{"t1"}},
			},
			topics: map[string][]int32{
				"t1": []int32{0, 1, 2},
				"t2": []int32{0, 1},
			},
		},

		{
			name: "two consumers with two topics and six partitions",
			members: []GroupMember{
				{ID: "A", Topics: []string{"t1", "t2"}},
				{ID: "B", Topics: []string{"t1", "t2"}},
			},
			topics: map[string][]int32{
				"t1": []int32{0, 1, 2},
				"t2": []int32{0, 1, 2},
			},
		},

		{
			name: "three consumers (two old, one new) with one topic and 12 partitions",
			members: []GroupMember{
				{ID: "A", Topics: []string{"t1"},
					UserData: oldUD().
						assign("t1", 4, 11, 8, 5, 9, 2).
						encode()},
				{ID: "B", Topics: []string{"t1"},
					UserData: oldUD().
						assign("t1", 1, 3, 0, 7, 10, 6).
						encode()},
			},
			topics: map[string][]int32{
				"t1": []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
			},
		},

		{
			name: "three consumers (two old, one new) with one topic and 13 partitions",
			members: []GroupMember{
				{ID: "A", Topics: []string{"t1"},
					UserData: oldUD().
						assign("t1", 4, 11, 8, 5, 9, 2, 6).
						encode()},
				{ID: "B", Topics: []string{"t1"},
					UserData: oldUD().
						assign("t1", 1, 3, 0, 7, 10, 12).
						encode()},
				{ID: "C", Topics: []string{"t1"}},
			},
			topics: map[string][]int32{
				"t1": []int32{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
			},
		},

		{
			name: "one consumer that is no longer subscribed to topic is was consuming",
			members: []GroupMember{
				{ID: "A", Topics: []string{"t2"},
					Version: 1,
					UserData: newUD().
						assign("t1", 0).
						encode()},
			},
			topics: map[string][]int32{
				"t1": []int32{0},
				"t2": []int32{0},
			},
		},

		{
			name: "two consumers, one no longer consuming what it was",
			members: []GroupMember{
				{ID: "A", Topics: []string{"t2"},
					Version: 1,
					UserData: newUD().
						assign("t1", 0).
						encode()},
				{ID: "B", Topics: []string{"t1", "t2"},
					Version: 1,
					UserData: newUD().
						assign("t1", 1).
						encode()},
			},
			topics: map[string][]int32{
				"t1": []int32{0, 1},
				"t2": []int32{0, 1},
			},
		},

		{
			// A -> 1, can take all
			// B -> 2, 3, 4
			// C -> 5, 6, 7, 8, 9
			//
			// Bad would be stealing a partition from B:
			// A -> 1, 2, 5
			// B -> 3, 4
			// C -> 6, 7, 8, 9
			//
			// Ideal:
			// A -> 1, 5, 6
			// B -> 2, 3, 4
			// C -> 7, 8, 9
			name: "bigly disbalancy 1",
			members: []GroupMember{
				{ID: "A", Topics: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"},
					Version: 1,
					UserData: newUD().
						assign("1", 0).
						encode()},
				{ID: "B", Topics: []string{"2", "3", "4"},
					Version: 1,
					UserData: newUD().
						assign("2", 0).
						assign("3", 0).
						assign("4", 0).
						encode()},
				{ID: "C", Topics: []string{"5", "6", "7", "8", "9"},
					Version: 1,
					UserData: newUD().
						assign("5", 0).
						assign("6", 0).
						assign("7", 0).
						assign("8", 0).
						assign("9", 0).
						encode()},
			},
			topics: map[string][]int32{
				"1": []int32{0},
				"2": []int32{0},
				"3": []int32{0},
				"4": []int32{0},
				"5": []int32{0},
				"6": []int32{0},
				"7": []int32{0},
				"8": []int32{0},
				"9": []int32{0},
			},
		},

		{
			// A -> 1, [in all]
			// B -> 2, [in 2, 3, 4]
			// D -> 3, 4, 5, 6, 7, 8, 9
			//
			// A -> 1, 9, 8, 7
			// B -> 2,
			// D -> 3, 4, 5, 6
			//
			// A -> 1, 9, 8, 7
			// B -> 2, 3
			// D -> 4, 5, 6
			//
			// Ideal:
			// A -> 1, 9, 8
			// B -> 2, 3, 4
			// C -> 5, 6, 7
			name: "bigly disbalancy 2",
			members: []GroupMember{
				{ID: "A", Topics: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"},
					Version: 1,
					UserData: newUD().
						assign("1", 0).
						encode()},
				{ID: "B", Topics: []string{"2", "3", "4"},
					Version: 1,
					UserData: newUD().
						assign("2", 0).
						encode()},
				{ID: "C", Topics: []string{"3", "4", "5", "6", "7", "8", "9"},
					Version: 1,
					UserData: newUD().
						assign("3", 0).
						assign("4", 0).
						assign("5", 0).
						assign("6", 0).
						assign("7", 0).
						assign("8", 0).
						assign("9", 0).
						encode()},
			},
			topics: map[string][]int32{
				"1": []int32{0},
				"2": []int32{0},
				"3": []int32{0},
				"4": []int32{0},
				"5": []int32{0},
				"6": []int32{0},
				"7": []int32{0},
				"8": []int32{0},
				"9": []int32{0},
			},
		},

		{
			// A -> 1, [in all]
			// B -> 2, [in 2, 3, 4]
			// D -> 3, 4, 5, 6, 7, 8, 9
			//
			// A -> 1, 9, 8, 7
			// B -> 2,
			// D -> 3, 4, 5, 6
			//
			// A -> 1, 9, 8, 7
			// B -> 2, 3
			// D -> 4, 5, 6
			//
			// Ideal:
			// A -> 1, 9, 8
			// B -> 2, 3, 4
			// C -> 5, 6, 7
			name: "bigly disbalancy 3",
			members: []GroupMember{
				{ID: "A", Topics: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"},
					Version: 1,
					UserData: newUD().
						assign("1", 0).
						encode()},
				{ID: "B", Topics: []string{"2", "3", "4"},
					Version: 1,
					UserData: newUD().
						assign("2", 0).
						encode()},
				{ID: "C", Topics: []string{"3", "4", "5", "6", "7", "8", "9"},
					Version: 1,
					UserData: newUD().
						assign("3", 0).
						assign("4", 0).
						assign("5", 0).
						assign("6", 0).
						assign("7", 0).
						assign("8", 0).
						assign("9", 0).
						encode()},
			},
			topics: map[string][]int32{
				"1": []int32{0},
				"2": []int32{0},
				"3": []int32{0},
				"4": []int32{0},
				"5": []int32{0},
				"6": []int32{0},
				"7": []int32{0},
				"8": []int32{0},
				"9": []int32{0},
			},
		},

		{
			// Start:
			// A -> 1 2 3
			// B ->
			// C -> 5 6 7 8 9
			// D ->
			//
			// Ideal:
			// A -> 1 2
			// B -> 3
			// C -> 5 6 7
			// D -> 8 9
			name: "back and forth 1",
			members: []GroupMember{
				{ID: "A", Topics: []string{"1", "2", "3"},
					Version: 1,
					UserData: newUD().
						assign("1", 0).
						assign("2", 0).
						assign("3", 0).
						encode()},
				{ID: "B", Topics: []string{"1", "2", "3"},
					Version: 1,
					UserData: newUD().
						encode()},
				{ID: "C", Topics: []string{"5", "6", "7", "8", "9"},
					Version: 1,
					UserData: newUD().
						assign("5", 0).
						assign("6", 0).
						assign("7", 0).
						assign("8", 0).
						assign("9", 0).
						encode()},
				{ID: "D", Topics: []string{"5", "6", "7", "8", "9"},
					Version: 1,
					UserData: newUD().
						encode()},
			},
			topics: map[string][]int32{
				"1": []int32{0},
				"2": []int32{0},
				"3": []int32{0},
				"4": []int32{0},
				"5": []int32{0},
				"6": []int32{0},
				"7": []int32{0},
				"8": []int32{0},
				"9": []int32{0},
			},
		},

		{
			// Start:
			// A -> 1 2 3 4
			// B -> 5
			// C ->
			// D -> 6 7 8 9 a b
			// E ->
			//
			// Ideal:
			// A -> 1 4
			// B -> 2 5
			// C -> 3
			// D -> 6 7 8
			// E -> 9 a b
			name: "back and forth 2",
			members: []GroupMember{
				{ID: "A", Topics: []string{"1", "2", "3", "4"},
					Version: 1,
					UserData: newUD().
						assign("1", 0).
						assign("2", 0).
						assign("3", 0).
						assign("4", 0).
						encode()},
				{ID: "B", Topics: []string{"2", "3", "4", "5"},
					Version: 1,
					UserData: newUD().
						assign("5", 0).
						encode()},
				{ID: "C", Topics: []string{"1", "3", "4", "5"},
					Version: 1,
					UserData: newUD().
						encode()},
				{ID: "D", Topics: []string{"6", "7", "8", "9", "a", "b"},
					Version: 1,
					UserData: newUD().
						assign("6", 0).
						assign("7", 0).
						assign("8", 0).
						assign("9", 0).
						assign("a", 0).
						assign("b", 0).
						encode()},
				{ID: "E", Topics: []string{"6", "7", "8", "9", "a", "b"},
					Version: 1,
					UserData: newUD().
						encode()},
			},
			topics: map[string][]int32{
				"1": []int32{0},
				"2": []int32{0},
				"3": []int32{0},
				"4": []int32{0},
				"5": []int32{0},
				"6": []int32{0},
				"7": []int32{0},
				"8": []int32{0},
				"9": []int32{0},
				"a": []int32{0},
				"b": []int32{0},
			},
		},

		{
			// Start:
			// A -> 1 2
			// B -> 3 4
			// C -> 5 6
			// D -> 7 8
			// E -> 9
			// F -> a b c d e
			// G ->
			//
			//
			// Ideal:
			// A -> 1 2
			// B -> 3 4
			// C -> 5 6
			// D -> 7 8
			// E -> 9
			// F -> a b c
			// G -> d e
			name: "back and forth 3",
			members: []GroupMember{
				{ID: "A", Topics: []string{"9", "1", "2"},
					Version: 1,
					UserData: newUD().
						assign("1", 0).
						assign("2", 0).
						encode()},
				{ID: "B", Topics: []string{"2", "3", "4"},
					Version: 1,
					UserData: newUD().
						assign("3", 0).
						assign("4", 0).
						encode()},
				{ID: "C", Topics: []string{"4", "5", "6"},
					Version: 1,
					UserData: newUD().
						assign("5", 0).
						assign("6", 0).
						encode()},
				{ID: "D", Topics: []string{"6", "7", "8"},
					Version: 1,
					UserData: newUD().
						assign("7", 0).
						assign("8", 0).
						encode()},
				{ID: "E", Topics: []string{"8", "9", "1"},
					Version: 1,
					UserData: newUD().
						assign("9", 0).
						encode()},
				{ID: "F", Topics: []string{"a", "b", "c", "d", "e"},
					Version: 1,
					UserData: newUD().
						assign("a", 0).
						assign("b", 0).
						assign("c", 0).
						assign("d", 0).
						assign("e", 0).
						encode()},
				{ID: "G", Topics: []string{"a", "b", "c", "d", "e"},
					Version: 1,
					UserData: newUD().
						encode()},
			},
			topics: map[string][]int32{
				"1": []int32{0},
				"2": []int32{0},
				"3": []int32{0},
				"4": []int32{0},
				"5": []int32{0},
				"6": []int32{0},
				"7": []int32{0},
				"8": []int32{0},
				"9": []int32{0},
				"a": []int32{0},
				"b": []int32{0},
				"c": []int32{0},
				"d": []int32{0},
				"e": []int32{0},
			},
		},

		{
			// Start:
			// A -> 1 2
			// B -> 3 4
			// C -> 5
			// D -> a b c d e
			// E ->
			//
			// A takes all,
			// B takes 1-5
			// C takes 3-5
			// D, E take a-e
			//
			// Ideal:
			// A -> 1 e
			// B -> 2 3
			// C -> 4 5
			// D -> a b
			// E -> c d
			name: "complicated steals",
			members: []GroupMember{
				{ID: "A", Topics: []string{"1", "2", "3", "4", "5", "a", "b", "c", "d", "e"},
					Version: 1,
					UserData: newUD().
						assign("1", 0).
						assign("2", 0).
						encode()},
				{ID: "B", Topics: []string{"1", "2", "3", "4", "5"},
					Version: 1,
					UserData: newUD().
						assign("3", 0).
						assign("4", 0).
						encode()},
				{ID: "C", Topics: []string{"3", "4", "5"},
					Version: 1,
					UserData: newUD().
						assign("5", 0).
						encode()},
				{ID: "D", Topics: []string{"a", "b", "c", "d", "e"},
					Version: 1,
					UserData: newUD().
						assign("a", 0).
						assign("b", 0).
						assign("c", 0).
						assign("d", 0).
						assign("e", 0).
						encode()},
				{ID: "E", Topics: []string{"a", "b", "c", "d", "e"},
					Version: 1,
					UserData: newUD().
						encode()},
			},
			topics: map[string][]int32{
				"1": []int32{0},
				"2": []int32{0},
				"3": []int32{0},
				"4": []int32{0},
				"5": []int32{0},
				"6": []int32{0},
				"7": []int32{0},
				"8": []int32{0},
				"9": []int32{0},
				"a": []int32{0},
				"b": []int32{0},
				"c": []int32{0},
				"d": []int32{0},
				"e": []int32{0},
			},
		},

		//
	} {

		t.Run(test.name, func(t *testing.T) {
			plan := Balance(test.members, test.topics)
			testEqualDivvy(t, plan)
			for member, topics := range plan {
				fmt.Printf("%s: %v\n", member, topics)
			}
		})
	}
}
