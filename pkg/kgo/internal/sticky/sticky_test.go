package sticky

import (
	"fmt"
	"math/rand"
	"testing"
)

func Test_stickyBalanceStrategy_Plan(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name         string
		members      []GroupMember
		topics       map[string]int32
		unusedTopics []string
		nsticky      int
	}{
		{
			name:    "no members, no plan",
			members: []GroupMember{},
			topics: map[string]int32{
				"a": 1,
			},
		},

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
			topics: map[string]int32{
				"t1": 3,
			},
		},

		{
			name: "only assigns from subscribed topics",
			members: []GroupMember{
				{ID: "A", Topics: []string{"t1"}},
			},
			topics: map[string]int32{
				"t1": 3,
				"t2": 3,
			},
			unusedTopics: []string{"t2"},
		},

		{
			name: "one consumer multiple topics",
			members: []GroupMember{
				{ID: "A", Topics: []string{"t1", "t2"}},
			},
			topics: map[string]int32{
				"t1": 1,
				"t2": 2,
			},
		},

		{
			name: "two consumers, one topic with one partition",
			members: []GroupMember{
				{ID: "A", Topics: []string{"t1"}},
				{ID: "B", Topics: []string{"t1"}},
			},
			topics: map[string]int32{
				"t1": 1,
			},
		},

		{
			name: "two consumers, one topic with two partitions",
			members: []GroupMember{
				{ID: "A", Topics: []string{"t1"}},
				{ID: "B", Topics: []string{"t1"}},
			},
			topics: map[string]int32{
				"t1": 2,
			},
		},

		{
			name: "multiple consumers with mixed topic subscriptions",
			members: []GroupMember{
				{ID: "A", Topics: []string{"t1"}},
				{ID: "B", Topics: []string{"t1", "t2"}},
				{ID: "C", Topics: []string{"t1"}},
			},
			topics: map[string]int32{
				"t1": 3,
				"t2": 2,
			},
		},

		{
			name: "two consumers with two topics and six partitions",
			members: []GroupMember{
				{ID: "A", Topics: []string{"t1", "t2"}},
				{ID: "B", Topics: []string{"t1", "t2"}},
			},
			topics: map[string]int32{
				"t1": 3,
				"t2": 3,
			},
		},

		{
			name: "three consumers (two old, one new) with one topic and 12 partitions",
			members: []GroupMember{
				{
					ID: "A", Topics: []string{"t1"},
					UserData: oldUD().
						assign("t1", 4, 11, 8, 5, 9, 2).
						encode(),
				},
				{
					ID: "B", Topics: []string{"t1"},
					UserData: oldUD().
						assign("t1", 1, 3, 0, 7, 10, 6).
						encode(),
				},
				{ID: "C", Topics: []string{"t1"}},
			},
			topics: map[string]int32{
				"t1": 12,
			},
			nsticky: 8,
		},

		{
			name: "three consumers (two old, one new) with one topic and 13 partitions",
			members: []GroupMember{
				{
					ID: "A", Topics: []string{"t1"},
					UserData: oldUD().
						assign("t1", 4, 11, 8, 5, 9, 2, 6).
						encode(),
				},
				{
					ID: "B", Topics: []string{"t1"},
					UserData: oldUD().
						assign("t1", 1, 3, 0, 7, 10, 12).
						encode(),
				},
				{ID: "C", Topics: []string{"t1"}},
			},
			topics: map[string]int32{
				"t1":     13,
				"unused": 13,
			},
			unusedTopics: []string{"unused"},
			nsticky:      9,
		},

		{
			name: "one consumer that is no longer subscribed to topic is was consuming",
			members: []GroupMember{
				{
					ID: "A", Topics: []string{"t2"},
					UserData: newUD().
						assign("t1", 0).
						encode(),
				},
			},
			topics: map[string]int32{
				"t1": 1,
				"t2": 1,
			},
			unusedTopics: []string{"t1"},
		},

		{
			name: "two consumers, one no longer consuming what it was",
			members: []GroupMember{
				{
					ID: "A", Topics: []string{"t2"},
					UserData: newUD().
						assign("t1", 0).
						encode(),
				},
				{
					ID: "B", Topics: []string{"t1", "t2"},
					UserData: newUD().
						assign("t1", 1).
						encode(),
				},
			},
			topics: map[string]int32{
				"t1": 2,
				"t2": 2,
			},
			nsticky: 1,
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
				{
					ID: "A", Topics: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"},
					UserData: newUD().
						assign("1", 0).
						encode(),
				},
				{
					ID: "B", Topics: []string{"2", "3", "4"},
					UserData: newUD().
						assign("2", 0).
						assign("3", 0).
						assign("4", 0).
						encode(),
				},
				{
					ID: "C", Topics: []string{"5", "6", "7", "8", "9"},
					UserData: newUD().
						assign("5", 0).
						assign("6", 0).
						assign("7", 0).
						assign("8", 0).
						assign("9", 0).
						encode(),
				},
			},
			topics: map[string]int32{
				"1": 1,
				"2": 1,
				"3": 1,
				"4": 1,
				"5": 1,
				"6": 1,
				"7": 1,
				"8": 1,
				"9": 1,
			},
			nsticky: 7,
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
				{
					ID: "A", Topics: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"},
					UserData: newUD().
						assign("1", 0).
						encode(),
				},
				{
					ID: "B", Topics: []string{"2", "3", "4"},
					UserData: newUD().
						assign("2", 0).
						encode(),
				},
				{
					ID: "C", Topics: []string{"3", "4", "5", "6", "7", "8", "9"},
					UserData: newUD().
						assign("3", 0).
						assign("4", 0).
						assign("5", 0).
						assign("6", 0).
						assign("7", 0).
						assign("8", 0).
						assign("9", 0).
						encode(),
				},
			},
			topics: map[string]int32{
				"1": 1,
				"2": 1,
				"3": 1,
				"4": 1,
				"5": 1,
				"6": 1,
				"7": 1,
				"8": 1,
				"9": 1,
			},
			nsticky: 5,
		},

		{
			// A -> 1, [in all]
			// B -> 2, [in 2, 3, 4]
			// D -> 3, 4, 5, 6, 7, 8, 9
			//
			// Ideal:
			// A -> 1, 9, 8
			// B -> 2, 3, 4
			// C -> 5, 6, 7
			name: "bigly disbalancy 3",
			members: []GroupMember{
				{
					ID: "A", Topics: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9"},
					UserData: newUD().
						assign("1", 0).
						encode(),
				},
				{
					ID: "B", Topics: []string{"2", "3", "4"},
					UserData: newUD().
						assign("2", 0).
						encode(),
				},
				{
					ID: "C", Topics: []string{"3", "4", "5", "6", "7", "8", "9"},
					UserData: newUD().
						assign("3", 0).
						assign("4", 0).
						assign("5", 0).
						assign("6", 0).
						assign("7", 0).
						assign("8", 0).
						assign("9", 0).
						encode(),
				},
			},
			topics: map[string]int32{
				"1": 1,
				"2": 1,
				"3": 1,
				"4": 1,
				"5": 1,
				"6": 1,
				"7": 1,
				"8": 1,
				"9": 1,
			},
			nsticky: 5,
		},

		{
			// Start:
			// A -> 1 2
			// B -> 3 4
			// C -> 5
			// D -> a b c d e
			// E ->
			//
			// Ideal:
			// A -> 1 e
			// B -> 2 3
			// C -> 4 5
			// D -> a b
			// E -> c d
			name: "complicated steals",
			members: []GroupMember{
				{
					ID: "A", Topics: []string{"1", "2", "3", "4", "5", "a", "b", "c", "d", "e"},
					UserData: newUD().
						assign("1", 0).
						assign("2", 0).
						encode(),
				},
				{
					ID: "B", Topics: []string{"1", "2", "3", "4", "5"},
					UserData: newUD().
						assign("3", 0).
						assign("4", 0).
						encode(),
				},
				{
					ID: "C", Topics: []string{"3", "4", "5"},
					UserData: newUD().
						assign("5", 0).
						encode(),
				},
				{
					ID: "D", Topics: []string{"a", "b", "c", "d", "e"},
					UserData: newUD().
						assign("a", 0).
						assign("b", 0).
						assign("c", 0).
						assign("d", 0).
						assign("e", 0).
						encode(),
				},
				{
					ID: "E", Topics: []string{"a", "b", "c", "d", "e"},
					UserData: newUD().
						encode(),
				},
			},
			topics: map[string]int32{
				"1": 1,
				"2": 1,
				"3": 1,
				"4": 1,
				"5": 1,
				"6": 1,
				"7": 1,
				"8": 1,
				"9": 1,
				"a": 1,
				"b": 1,
				"c": 1,
				"d": 1,
				"e": 1,
			},
			nsticky: 5,
		},

		{
			// Start:
			// A: [1 2 3]
			// B: [1 2 3 4 5 6]
			// C: [4 5 6 7 8 9]
			// D: [6 7 8 9 a b c d e f]
			// E: [6 7 8 9 a b c d e f]
			//
			// A -> 1 2
			// B -> 3
			// C -> 4 5
			// D -> 6 7
			// E -> 8 9 a b c d e f
			//
			// Ideal:
			// A -> 1 2 3
			// B -> 4 5 6
			// C -> 7 8 9
			// D -> a b c
			// E -> d e f
			name: "big disbalance to equal",
			members: []GroupMember{
				{
					ID: "A", Topics: []string{"1", "2", "3"},
					UserData: newUD().
						assign("1", 0).
						assign("2", 0).
						encode(),
				},
				{
					ID: "B", Topics: []string{"1", "2", "3", "4", "5", "6"},
					UserData: newUD().
						assign("3", 0).
						encode(),
				},
				{
					ID: "C", Topics: []string{"4", "5", "6", "7", "8", "9"},
					UserData: newUD().
						assign("4", 0).
						assign("5", 0).
						encode(),
				},
				{
					ID: "D", Topics: []string{"6", "7", "8", "9", "a", "b", "c", "d", "e", "f"},
					UserData: newUD().
						assign("6", 0).
						assign("7", 0).
						encode(),
				},
				{
					ID: "E", Topics: []string{"6", "7", "8", "9", "a", "b", "c", "d", "e", "f"},
					UserData: newUD().
						assign("8", 0).
						assign("9", 0).
						assign("a", 0).
						assign("b", 0).
						assign("c", 0).
						assign("d", 0).
						assign("e", 0).
						assign("f", 0).
						encode(),
				},
			},
			topics: map[string]int32{
				"1": 1,
				"2": 1,
				"3": 1,
				"4": 1,
				"5": 1,
				"6": 1,
				"7": 1,
				"8": 1,
				"9": 1,
				"a": 1,
				"b": 1,
				"c": 1,
				"d": 1,
				"e": 1,
				"f": 1,
			},
			nsticky: 5,
		},

		//
	} {
		t.Run(test.name, func(t *testing.T) {
			plan := Balance(test.members, test.topics)
			testEqualDivvy(t, plan, test.nsticky, test.members)
			testPlanUsage(t, plan, test.topics, test.unusedTopics)
		})
	}
}

// For imbalanced plans, we can have multiple results per partition count.
// resultOptions aids in these results; see help_test.
type resultOptions struct {
	candidates []string
	times      int
}

func TestImbalanced(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		members []GroupMember
		topics  map[string]int32
		nsticky int
		balance map[int]resultOptions
	}{
		{
			// Start:
			// A -> [1 2 3]
			// B -> [1 2 3]
			// C -> [5 6 7 8 9]
			// D -> [5 6 7 8 9]
			//
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
				{
					ID: "A", Topics: []string{"1", "2", "3"},
					UserData: newUD().
						assign("1", 0).
						assign("2", 0).
						assign("3", 0).
						encode(),
				},
				{
					ID: "B", Topics: []string{"1", "2", "3"},
					UserData: newUD().
						encode(),
				},
				{
					ID: "C", Topics: []string{"5", "6", "7", "8", "9"},
					UserData: newUD().
						assign("5", 0).
						assign("6", 0).
						assign("7", 0).
						assign("8", 0).
						assign("9", 0).
						encode(),
				},
				{
					ID: "D", Topics: []string{"5", "6", "7", "8", "9"},
					UserData: newUD().
						encode(),
				},
			},
			topics: map[string]int32{
				"1": 1,
				"2": 1,
				"3": 1,
				"4": 1,
				"5": 1,
				"6": 1,
				"7": 1,
				"8": 1,
				"9": 1,
			},
			nsticky: 2 + 3,
			balance: map[int]resultOptions{
				2: {[]string{"A", "D"}, 2},
				1: {[]string{"B"}, 1},
				3: {[]string{"C"}, 1},
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
				{
					ID: "A", Topics: []string{"1", "2", "3", "4"},
					UserData: newUD().
						assign("1", 0).
						assign("2", 0).
						assign("3", 0).
						assign("4", 0).
						encode(),
				},
				{
					ID: "B", Topics: []string{"2", "3", "4", "5"},
					UserData: newUD().
						assign("5", 0).
						encode(),
				},
				{
					ID: "C", Topics: []string{"1", "3", "4", "5"},
					UserData: newUD().
						encode(),
				},
				{
					ID: "D", Topics: []string{"6", "7", "8", "9", "a", "b"},
					UserData: newUD().
						assign("6", 0).
						assign("7", 0).
						assign("8", 0).
						assign("9", 0).
						assign("a", 0).
						assign("b", 0).
						encode(),
				},
				{
					ID: "E", Topics: []string{"6", "7", "8", "9", "a", "b"},
					UserData: newUD().
						encode(),
				},
			},
			topics: map[string]int32{
				"1": 1,
				"2": 1,
				"3": 1,
				"4": 1,
				"5": 1,
				"6": 1,
				"7": 1,
				"8": 1,
				"9": 1,
				"a": 1,
				"b": 1,
			},
			nsticky: 2 + 1 + 3,
			balance: map[int]resultOptions{ // B and C could alternate
				2: {[]string{"A", "B", "C"}, 2},
				1: {[]string{"B", "C"}, 1},
				3: {[]string{"D", "E"}, 2},
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
				{
					ID: "A", Topics: []string{"9", "1", "2"},
					UserData: newUD().
						assign("1", 0).
						assign("2", 0).
						encode(),
				},
				{
					ID: "B", Topics: []string{"2", "3", "4"},
					UserData: newUD().
						assign("3", 0).
						assign("4", 0).
						encode(),
				},
				{
					ID: "C", Topics: []string{"4", "5", "6"},
					UserData: newUD().
						assign("5", 0).
						assign("6", 0).
						encode(),
				},
				{
					ID: "D", Topics: []string{"6", "7", "8"},
					UserData: newUD().
						assign("7", 0).
						assign("8", 0).
						encode(),
				},
				{
					ID: "E", Topics: []string{"8", "9", "1"},
					UserData: newUD().
						assign("9", 0).
						encode(),
				},
				{
					ID: "F", Topics: []string{"a", "b", "c", "d", "e"},
					UserData: newUD().
						assign("a", 0).
						assign("b", 0).
						assign("c", 0).
						assign("d", 0).
						assign("e", 0).
						encode(),
				},
				{
					ID: "G", Topics: []string{"a", "b", "c", "d", "e"},
					UserData: newUD().
						encode(),
				},
			},
			topics: map[string]int32{
				"1": 1,
				"2": 1,
				"3": 1,
				"4": 1,
				"5": 1,
				"6": 1,
				"7": 1,
				"8": 1,
				"9": 1,
				"a": 1,
				"b": 1,
				"c": 1,
				"d": 1,
				"e": 1,
			},
			nsticky: 12,
			balance: map[int]resultOptions{
				2: {[]string{"A", "B", "C", "D", "G"}, 5},
				1: {[]string{"E"}, 1},
				3: {[]string{"F"}, 1},
			},
		},

		{
			// Start:
			// A: [1 2 3 4]
			// B: [1 2 3 4 5 6 7 8 9 a b c d]
			// C: [5 6 7 8 9 a b c d]
			// D: [5 6 7 8 9 a b c d e f g h i j]
			// E: [e]
			// F: [f]
			// G: [g]
			// H: [h]
			// I: [i]
			// J: [j]
			//
			// A ->
			// B -> 1 2 3 4
			// C -> 5 6 7 8 9 a b c d
			// D -> e f g h i j
			// E ->
			// F ->
			// G ->
			// H ->
			// I ->
			// J ->
			//
			// Ideal:
			// A -> 1 2 3
			// B -> 4 c d
			// C -> 5 6 7 b
			// D -> 8 9 a
			// E -> e
			// F -> f
			// G -> g
			// H -> h
			// I -> i
			// J -> j
			name: "odd pyramid",
			members: []GroupMember{
				{
					ID: "A", Topics: []string{"1", "2", "3", "4"},
					UserData: newUD().
						encode(),
				},
				{
					ID: "B", Topics: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d"},
					UserData: newUD().
						assign("1", 0).
						assign("2", 0).
						assign("3", 0).
						assign("4", 0).
						encode(),
				},
				{
					ID: "C", Topics: []string{"5", "6", "7", "8", "9", "a", "b", "c", "d"},
					UserData: newUD().
						assign("5", 0).
						assign("6", 0).
						assign("7", 0).
						assign("8", 0).
						assign("9", 0).
						assign("a", 0).
						assign("b", 0).
						assign("c", 0).
						assign("d", 0).
						encode(),
				},
				{
					ID: "D", Topics: []string{"5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j"},
					UserData: newUD().
						assign("e", 0).
						assign("f", 0).
						assign("g", 0).
						assign("h", 0).
						assign("i", 0).
						assign("j", 0).
						encode(),
				},
				{
					ID: "E", Topics: []string{"e"},
					UserData: newUD().
						encode(),
				},
				{
					ID: "F", Topics: []string{"f"},
					UserData: newUD().
						encode(),
				},
				{
					ID: "G", Topics: []string{"g"},
					UserData: newUD().
						encode(),
				},
				{
					ID: "H", Topics: []string{"h"},
					UserData: newUD().
						encode(),
				},
				{
					ID: "I", Topics: []string{"i"},
					UserData: newUD().
						encode(),
				},
				{
					ID: "J", Topics: []string{"j"},
					UserData: newUD().
						encode(),
				},
			},
			topics: map[string]int32{
				"1": 1,
				"2": 1,
				"3": 1,
				"4": 1,
				"5": 1,
				"6": 1,
				"7": 1,
				"8": 1,
				"9": 1,
				"a": 1,
				"b": 1,
				"c": 1,
				"d": 1,
				"e": 1,
				"f": 1,
				"g": 1,
				"h": 1,
				"i": 1,
				"j": 1,
			},
			nsticky: 1 + 4,
			balance: map[int]resultOptions{
				3: {[]string{"A", "B", "D"}, 3},
				4: {[]string{"C"}, 1},
				1: {[]string{"E", "F", "G", "H", "I", "J"}, 6},
			},
		},

		{
			// Start:
			// A: [1 2 3]
			// B: [3 4 5]
			// C: [3 4 5 6 7 8 9 a b c]
			// D: [3 4 5 6 7 8 9 a b c z]
			//
			// A -> 1 2
			// B ->
			// C -> 3 4 5
			// D -> 6 7 8 9 a b c
			//
			// Ideal:
			// A -> 1 2
			// B -> 3 4 5
			// C -> 7 8 9
			// D -> 9 a b c
			name: "chain stop and noexist drop",
			members: []GroupMember{
				{
					ID: "A", Topics: []string{"1", "2", "3"},
					UserData: newUD().
						assign("1", 0).
						assign("2", 0).
						encode(),
				},
				{
					ID: "B", Topics: []string{"3", "4", "5"},
					UserData: newUD().
						encode(),
				},
				{
					ID: "C", Topics: []string{"3", "4", "5", "6", "7", "8", "9", "a", "b", "c"},
					UserData: newUD().
						assign("3", 0).
						assign("4", 0).
						assign("5", 0).
						encode(),
				},
				{
					ID: "D", Topics: []string{
						"3", "4", "5", "6", "7", "8", "9", "a", "b", "c",
						"x",
					}, // x does not exist; hits continue branch in assign
					UserData: newUD().
						assign("6", 0).
						assign("7", 0).
						assign("8", 0).
						assign("9", 0).
						assign("a", 0).
						assign("b", 0).
						assign("c", 0).
						assign("z", 0). // no longer exists; dropped in parseMemberMetadata
						encode(),
				},
			},
			topics: map[string]int32{
				"1": 1,
				"2": 1,
				"3": 1,
				"4": 1,
				"5": 1,
				"6": 1,
				"7": 1,
				"8": 1,
				"9": 1,
				"a": 1,
				"b": 1,
				"c": 1,
			},
			nsticky: 2 + 4,
			balance: map[int]resultOptions{
				2: {[]string{"A", "B"}, 1},
				3: {[]string{"A", "B", "C"}, 2},
				4: {[]string{"D"}, 1},
			},
		},

		{
			name: "unequal",
			members: []GroupMember{
				{ID: "0", Topics: []string{"0"}},
				{ID: "1", Topics: []string{"1"}},
			},
			topics: map[string]int32{
				"1": 2,
			},
			nsticky: 0,
			balance: map[int]resultOptions{
				0: {[]string{"0"}, 1},
				2: {[]string{"1"}, 1},
			},
		},

		{
			// Start:
			// A: [1 2 3 4]
			// B: [1 2 3 4 5 6 7 8 9 a b c d e f g]
			// C: [5 6 7 8 9 a]
			// D: [b c d e f g]
			//
			// A ->
			// B -> 1 2 3 4
			// C -> 5 6 7 8 9 a
			// D -> b c d e f g
			//
			//
			// Ideal:
			// A -> 1 2 3 4
			// B -> 5 6 b c
			// C -> 7 8 9 a
			// D -> d e f g
			name: "graph reachy",
			members: []GroupMember{
				{
					ID: "A", Topics: []string{"1", "2", "3", "4"},
					UserData: newUD().
						encode(),
				},
				{
					ID: "B", Topics: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f", "g"},
					UserData: newUD().
						assign("1", 0).
						assign("2", 0).
						assign("3", 0).
						assign("4", 0).
						encode(),
				},
				{
					ID: "C", Topics: []string{"5", "6", "7", "8", "9", "a"},
					UserData: newUD().
						assign("5", 0).
						assign("6", 0).
						assign("7", 0).
						assign("8", 0).
						assign("9", 0).
						assign("a", 0).
						encode(),
				},
				{
					ID: "D", Topics: []string{"b", "c", "d", "e", "f", "g"},
					UserData: newUD().
						assign("b", 0).
						assign("c", 0).
						assign("d", 0).
						assign("e", 0).
						assign("f", 0).
						assign("g", 0).
						encode(),
				},
			},
			topics: map[string]int32{
				"1": 1,
				"2": 1,
				"3": 1,
				"4": 1,
				"5": 1,
				"6": 1,
				"7": 1,
				"8": 1,
				"9": 1,
				"a": 1,
				"b": 1,
				"c": 1,
				"d": 1,
				"e": 1,
				"f": 1,
				"g": 1,
			},
			nsticky: 8,
			balance: map[int]resultOptions{
				4: {[]string{"A", "B", "C", "D"}, 4},
			},
		},

		{
			// This test specifically triggers a condition where B
			// will still from A and then A will steal back from B.
			// The problem is that we want A to re-steal its
			// partition, rather than lose stickiness.
			// Start:
			// A: [1 2 3 4 5 6]
			// B: [1 2 3 4 5 6]
			// C: [5 6 7 8]
			// D: [7 8 9 a b c]
			// E: [9 a b c d e f g h i]
			// F: [7 8 9 a b c d e f g h i]
			//
			// A -> 0 1 2 3 4 5 6
			// B -> 7 8 9 a b
			// C ->
			// D -> c d
			// E -> e f
			// F -> g h i j k l m n o p q r s t
			//
			// Ideal:
			//
			// A => [1 2 3 4 6]
			// B => [0 5 7 a b]
			// C => [8 9 c e f]
			// D => [d h j p s]
			// E => [g i l n q]
			// F => [k m o r t]
			// Less ideal is the same balance, but with lost stickiness.
			name: "imbalance hard",
			members: []GroupMember{
				{
					ID: "A", Topics: []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b"},
					UserData: newUD().
						assign("0", 0).
						assign("1", 0).
						assign("2", 0).
						assign("3", 0).
						assign("4", 0).
						assign("5", 0).
						assign("6", 0).
						encode(),
				},
				{
					ID: "B", Topics: []string{"0", "1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b"},
					UserData: newUD().
						assign("7", 0).
						assign("8", 0).
						assign("9", 0).
						assign("a", 0).
						assign("b", 0).
						encode(),
				},
				{
					ID: "C", Topics: []string{"7", "8", "9", "a", "b", "c", "d", "e", "f"},
					UserData: newUD().
						encode(),
				},
				{
					ID: "D", Topics: []string{"c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t"},
					UserData: newUD().
						assign("c", 0).
						assign("d", 0).
						encode(),
				},
				{
					ID: "E", Topics: []string{"c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t"},
					UserData: newUD().
						assign("e", 0).
						assign("f", 0).
						encode(),
				},
				{
					ID: "F", Topics: []string{"c", "d", "e", "f", "g", "h", "i", "j", "k", "l", "m", "n", "o", "p", "q", "r", "s", "t"},
					UserData: newUD().
						assign("g", 0).
						assign("h", 0).
						assign("i", 0).
						assign("j", 0).
						assign("k", 0).
						assign("l", 0).
						assign("m", 0).
						assign("n", 0).
						assign("o", 0).
						assign("p", 0).
						assign("q", 0).
						assign("r", 0).
						assign("s", 0).
						assign("t", 0).
						encode(),
				},
			},
			topics: map[string]int32{
				"0": 1,
				"1": 1,
				"2": 1,
				"3": 1,
				"4": 1,
				"5": 1,
				"6": 1,
				"7": 1,
				"8": 1,
				"9": 1,
				"a": 1,
				"b": 1,
				"c": 1,
				"d": 1,
				"e": 1,
				"f": 1,
				"g": 1,
				"h": 1,
				"i": 1,
				"j": 1,
				"k": 1,
				"l": 1,
				"m": 1,
				"n": 1,
				"o": 1,
				"p": 1,
				"q": 1,
				"r": 1,
				"s": 1,
				"t": 1,
			},
			nsticky: 14,
			balance: map[int]resultOptions{
				5: {[]string{"A", "B", "C", "D", "E", "F"}, 6},
			},
		},

		//
	} {
		t.Run(test.name, func(t *testing.T) {
			plan := Balance(test.members, test.topics)
			testStickyResult(t, plan, test.members, test.nsticky, test.balance)
			testPlanUsage(t, plan, test.topics, nil)
		})
	}
}

func TestMultiGenerational(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name    string
		members []GroupMember
		topics  map[string]int32
		nsticky int
		balance map[int]resultOptions
	}{
		{
			// When old generation cannot take back.
			//
			// Start:
			// A -> 1 2 (gen 1, unassigned in gen 2, no longer interested)
			// B -> 3 4 (gen 2)
			// C -> 1 2 5 (gen 2)
			//
			// Ideal:
			// A -> 4
			// B -> 3
			// C -> 1 2 5
			name: "!canTake branch in resticky",
			members: []GroupMember{
				{
					ID: "A", Topics: []string{"3", "4"},
					UserData: newUD().
						assign("1", 0).
						assign("2", 0).
						setGeneration(1).
						encode(),
				},
				{
					ID: "B", Topics: []string{"3", "4"},
					UserData: newUD().
						assign("3", 0).
						assign("4", 0).
						setGeneration(2).
						encode(),
				},
				{
					ID: "C", Topics: []string{"1", "2", "5"},
					UserData: newUD().
						assign("1", 0).
						assign("2", 0).
						assign("5", 0).
						setGeneration(2).
						encode(),
				},
			},
			topics: map[string]int32{
				"1": 1,
				"2": 1,
				"3": 1,
				"4": 1,
				"5": 1,
			},
			nsticky: 4,
			balance: map[int]resultOptions{
				1: {[]string{"A", "B"}, 2},
				3: {[]string{"C"}, 1},
			},
		},

		//
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			plan := Balance(test.members, test.topics)
			testStickyResult(t, plan, test.members, test.nsticky, test.balance)
			testPlanUsage(t, plan, test.topics, nil)
		})
	}
}

func Test_stickyBalanceStrategy_Plan_KIP54_ExampleOne(t *testing.T) {
	t.Parallel()
	// PLAN 1
	members := []GroupMember{
		{ID: "A", Topics: []string{"1", "2", "3", "4"}},
		{ID: "B", Topics: []string{"1", "2", "3", "4"}},
		{ID: "C", Topics: []string{"1", "2", "3", "4"}},
	}
	topics := map[string]int32{
		"1": 2,
		"2": 2,
		"3": 2,
		"4": 2,
	}
	plan1 := Balance(members, topics)
	testEqualDivvy(t, plan1, 0, members)
	testPlanUsage(t, plan1, topics, nil)

	// PLAN 2
	members[1].UserData = udEncode(0, 0, plan1["B"])
	members[2].UserData = udEncode(0, 0, plan1["C"])
	nsticky := 8 - partitionsForMember(plan1["A"])
	members = members[1:]
	plan2 := Balance(members, topics)
	testEqualDivvy(t, plan2, nsticky, members)
	testPlanUsage(t, plan2, topics, nil)
}

func Test_stickyBalanceStrategy_Plan_KIP54_ExampleTwo(t *testing.T) {
	t.Parallel()
	// PLAN 1
	members := []GroupMember{
		{ID: "1", Topics: []string{"1"}},
		{ID: "2", Topics: []string{"1", "2"}},
		{ID: "3", Topics: []string{"1", "2", "3"}},
	}
	topics := map[string]int32{
		"1": 1,
		"2": 2,
		"3": 3,
	}
	plan1 := Balance(members, topics)
	balance1 := map[int]resultOptions{
		1: {[]string{"1"}, 1},
		2: {[]string{"2"}, 1},
		3: {[]string{"3"}, 1},
	}
	testStickyResult(t, plan1, members, 0, balance1)
	testPlanUsage(t, plan1, topics, nil)
	if len(plan1["1"]["1"]) != 1 || len(plan1["2"]["2"]) != 2 || len(plan1["3"]["3"]) != 3 {
		t.Error("Incorrect distribution of topic partition assignments")
	}

	// PLAN 2
	members[1].UserData = udEncode(0, 0, plan1["2"])
	members[2].UserData = udEncode(0, 0, plan1["3"])
	members = members[1:]
	plan2 := Balance(members, topics)
	balance2 := map[int]resultOptions{
		3: {[]string{"2", "3"}, 2},
	}
	testStickyResult(t, plan2, members, 5, balance2)
	testPlanUsage(t, plan2, topics, nil)
	if len(plan2["2"]["1"]) != 1 || len(plan2["2"]["2"]) != 2 || len(plan2["3"]["3"]) != 3 {
		t.Error("Incorrect distribution of topic partition assignments")
	}
}

func Test_stickyBalanceStrategy_Plan_KIP54_ExampleThree(t *testing.T) {
	t.Parallel()
	// PLAN 1
	members := []GroupMember{
		{ID: "1", Topics: []string{"1", "2"}},
		{ID: "2", Topics: []string{"1", "2"}},
	}
	topics := map[string]int32{
		"1": 2,
		"2": 2,
	}
	plan1 := Balance(members, topics)
	balance1 := map[int]resultOptions{
		2: {[]string{"1", "2"}, 2},
	}
	testStickyResult(t, plan1, members, 0, balance1)
	testPlanUsage(t, plan1, topics, nil)

	// PLAN 2
	members[1].UserData = udEncode(0, 0, plan1["2"])
	members = append(members, GroupMember{
		ID: "3", Topics: []string{"1", "2"},
	})
	plan2 := Balance(members, topics)
	balance2 := map[int]resultOptions{
		1: {[]string{"1", "3"}, 2},
		2: {[]string{"2"}, 1},
	}
	testStickyResult(t, plan2, members, 2, balance2)
	testPlanUsage(t, plan2, topics, nil)
}

func Test_stickyBalanceStrategy_Plan_AddRemoveConsumerOneTopic(t *testing.T) {
	t.Parallel()
	// PLAN 1
	members := []GroupMember{
		{ID: "1", Topics: []string{"T"}},
	}
	topics := map[string]int32{
		"T": 3,
	}
	plan1 := Balance(members, topics)
	balance1 := map[int]resultOptions{
		3: {[]string{"1"}, 1},
	}
	testStickyResult(t, plan1, members, 0, balance1)
	testPlanUsage(t, plan1, topics, nil)

	// PLAN 2
	members[0].UserData = udEncode(0, 0, plan1["1"])
	members = append(members, GroupMember{
		ID: "2", Topics: []string{"T"},
	})
	plan2 := Balance(members, topics)
	balance2 := map[int]resultOptions{
		1: {[]string{"2"}, 1},
		2: {[]string{"1"}, 1},
	}
	testStickyResult(t, plan2, members, 2, balance2)
	testPlanUsage(t, plan2, topics, nil)

	// PLAN 3
	members[1].UserData = udEncode(0, 0, plan2["2"])
	members = members[1:]
	plan3 := Balance(members, topics)
	balance3 := map[int]resultOptions{
		3: {[]string{"2"}, 1},
	}
	testStickyResult(t, plan3, members, 1, balance3)
	testPlanUsage(t, plan3, topics, nil)
}

func Test_stickyBalanceStrategy_Plan_AssignmentWithMultipleGenerations1(t *testing.T) {
	t.Parallel()
	topics := map[string]int32{"1": 6}
	members := []GroupMember{
		{ID: "1", Topics: []string{"1"}},
		{ID: "2", Topics: []string{"1"}},
		{ID: "3", Topics: []string{"1"}},
	}
	plan1 := Balance(members, topics)
	testEqualDivvy(t, plan1, 0, members)
	testPlanUsage(t, plan1, topics, nil)

	// PLAN 2
	members[0].UserData = udEncode(1, 1, plan1["1"])
	members[1].UserData = udEncode(1, 1, plan1["2"])
	members = members[:2] // delete 3

	plan2 := Balance(members, topics)
	testEqualDivvy(t, plan2, 4, members)
	testPlanUsage(t, plan2, topics, nil)

	// PLAN 3
	members = members[1:] // delete 1
	members[0].UserData = udEncode(1, 2, plan2["2"])
	members = append(members, GroupMember{
		ID:       "3",
		Topics:   []string{"1"},
		UserData: udEncode(1, 1, plan1["3"]),
	})

	plan3 := Balance(members, topics)
	testEqualDivvy(t, plan3, 4, members)
	testPlanUsage(t, plan3, topics, nil)
}

func Test_stickyBalanceStrategy_Plan_AssignmentWithMultipleGenerations2(t *testing.T) {
	t.Parallel()
	topics := map[string]int32{"1": 6}
	members := []GroupMember{
		{ID: "1", Topics: []string{"1"}},
		{ID: "2", Topics: []string{"1"}},
		{ID: "3", Topics: []string{"1"}},
	}
	plan1 := Balance(members, topics)
	testEqualDivvy(t, plan1, 0, members)
	testPlanUsage(t, plan1, topics, nil)

	// PLAN 2
	members[1].UserData = udEncode(1, 1, plan1["2"])
	ogMembers := members
	members = members[1:2]

	plan2 := Balance(members, topics)
	testEqualDivvy(t, plan2, 2, members)
	testPlanUsage(t, plan2, topics, nil)

	// PLAN 3
	members = ogMembers
	members[0].UserData = udEncode(1, 1, plan1["1"])
	members[1].UserData = udEncode(1, 2, plan2["2"])
	members[2].UserData = udEncode(1, 1, plan1["3"])
	plan3 := Balance(members, topics)

	testEqualDivvy(t, plan3, 6, members)
	testPlanUsage(t, plan3, topics, nil)
}

func Test_stickyAddEqualMove(t *testing.T) {
	t.Parallel()
	topics := map[string]int32{"foo": 16, "bar": 16}
	members := []GroupMember{
		{ID: "1", Topics: []string{"foo", "bar"}},
	}
	plan1 := Balance(members, topics)

	// PLAN 2
	members[0].UserData = udEncode(1, 1, plan1["1"])
	members = append(members, GroupMember{
		ID: "2", Topics: []string{"foo", "bar"},
	})

	plan2 := Balance(members, topics)
	testEqualDivvy(t, plan2, 16, members)
	testPlanUsage(t, plan2, topics, nil)

	if len(plan2["1"]["foo"]) != 8 || len(plan2["1"]["bar"]) != 8 ||
		len(plan2["2"]["foo"]) != 8 || len(plan2["2"]["bar"]) != 8 {
		t.Errorf("bad distribution: %v", plan2)
	}
}

func Test_stickyTwoJoinEqualBalance(t *testing.T) {
	t.Parallel()
	topics := map[string]int32{"foo": 16, "bar": 16}
	members := []GroupMember{
		{ID: "1", Topics: []string{"foo", "bar"}},
		{ID: "2", Topics: []string{"foo", "bar"}},
	}
	plan := Balance(members, topics)
	if len(plan["1"]["foo"]) != 8 || len(plan["1"]["bar"]) != 8 ||
		len(plan["2"]["foo"]) != 8 || len(plan["2"]["bar"]) != 8 {
		t.Errorf("bad distribution: %v", plan)
	}
}

func Test_stickyBalanceStrategy_Plan_AssignmentWithConflictingPreviousGenerations(t *testing.T) {
	t.Parallel()

	topics := map[string]int32{"1": 6}
	members := []GroupMember{
		{
			ID: "1", Topics: []string{"1"},
			UserData: newUD().
				assign("1", 0, 1, 4).
				setGeneration(1).
				encode(),
		},
		{
			ID: "2", Topics: []string{"1"},
			UserData: newUD().
				assign("1", 0, 2, 3). // double zero consumption... OK I guess...
				setGeneration(1).
				encode(),
		},
		{
			ID: "3", Topics: []string{"1"},
			UserData: newUD().
				assign("1", 3, 4, 5).
				setGeneration(2).
				encode(),
		},
	}

	plan := Balance(members, topics)

	testEqualDivvy(t, plan, 6, members)
	testPlanUsage(t, plan, topics, nil)
}

func Test_stickyBalanceStrategy_Plan_SchemaBackwardCompatibility(t *testing.T) {
	t.Parallel()
	topics := map[string]int32{"1": 3}
	members := []GroupMember{
		{
			ID: "1", Topics: []string{"1"},
			UserData: newUD().
				assign("1", 0, 2).
				setGeneration(1).
				encode(),
		},
		{
			ID: "2", Topics: []string{"1"},
			UserData: oldUD().
				assign("1", 1).
				encode(),
		},
		{ID: "3", Topics: []string{"1"}},
	}

	plan := Balance(members, topics)
	testEqualDivvy(t, plan, 2, members)
	testPlanUsage(t, plan, topics, nil)
}

func Test_stickyBalanceStrategy_Plan_ConflictingPreviousAssignments(t *testing.T) {
	t.Parallel()
	topics := map[string]int32{"1": 2}
	members := []GroupMember{
		{
			ID: "1", Topics: []string{"1"},
			UserData: newUD().
				assign("1", 0, 1).
				encode(),
		},
		{
			ID: "2", Topics: []string{"1"},
			UserData: newUD().
				assign("1", 0, 1).
				encode(),
		},
	}
	plan := Balance(members, topics)
	testEqualDivvy(t, plan, 2, members)
	testPlanUsage(t, plan, topics, nil)
}

func Test_EnsurePartitionsAssignedToHighestGeneration(t *testing.T) {
	t.Parallel()

	topics := map[string]int32{
		"topic":  3,
		"topic2": 3,
		"topic3": 3,
	}

	currentGeneration := 10

	members := []GroupMember{
		{
			ID:     "consumer1",
			Topics: []string{"topic", "topic2", "topic3"},
			UserData: newUD().
				assign("topic", 0).
				assign("topic2", 0).
				assign("topic3", 0).
				setGeneration(currentGeneration).
				encode(),
		},
		{
			ID:     "consumer2",
			Topics: []string{"topic", "topic2", "topic3"},
			UserData: newUD().
				assign("topic", 1).
				assign("topic2", 1).
				assign("topic3", 1).
				setGeneration(currentGeneration - 1).
				encode(),
		},
		{
			ID:     "consumer3",
			Topics: []string{"topic", "topic2", "topic3"},
			UserData: newUD().
				assign("topic2", 1). // Conflicts with consumer2 (lower generation, loses)
				assign("topic3", 0). // Conflicts with consumer1 (lower generation, loses)
				assign("topic3", 2). // No conflict
				setGeneration(currentGeneration - 2).
				encode(),
		},
	}

	plan := Balance(members, topics)

	// Due to generation-based conflict resolution:
	// - consumer1 keeps all 3 (highest generation) = 3 sticky
	// - consumer2 keeps all 3 (second highest) = 3 sticky
	// - consumer3 loses topic2-1 and topic3-0, keeps only topic3-2 = 1 sticky
	// Total expected sticky: 7
	balance := map[int]resultOptions{
		3: {[]string{"consumer1", "consumer2", "consumer3"}, 3},
	}

	testStickyResult(t, plan, members, 7, balance)
	testPlanUsage(t, plan, topics, nil)
}

func Test_NoReassignmentOnCurrentMembers(t *testing.T) {
	t.Parallel()

	topics := map[string]int32{
		"topic":  3,
		"topic1": 3,
		"topic2": 3,
		"topic3": 3,
	}

	currentGeneration := 10

	members := []GroupMember{
		{
			ID:     "consumer1",
			Topics: []string{"topic", "topic2", "topic3", "topic1"},
			UserData: newUD().
				setGeneration(-1). // DEFAULT_GENERATION
				encode(),
		},
		{
			ID:     "consumer2",
			Topics: []string{"topic", "topic2", "topic3", "topic1"},
			UserData: newUD().
				assign("topic", 0).
				assign("topic2", 0).
				assign("topic1", 0).
				setGeneration(currentGeneration - 1).
				encode(),
		},
		{
			ID:     "consumer3",
			Topics: []string{"topic", "topic2", "topic3", "topic1"},
			UserData: newUD().
				assign("topic3", 2).
				assign("topic2", 2).
				assign("topic1", 1).
				setGeneration(currentGeneration - 2).
				encode(),
		},
		{
			ID:     "consumer4",
			Topics: []string{"topic", "topic2", "topic3", "topic1"},
			UserData: newUD().
				assign("topic3", 1).
				assign("topic", 1, 2).
				setGeneration(currentGeneration - 3).
				encode(),
		},
	}

	plan := Balance(members, topics)

	// All existing assignments should be preserved (9 partitions already assigned)
	// Consumer1 gets the remaining 3 unassigned partitions
	testEqualDivvy(t, plan, 9, members)
	testPlanUsage(t, plan, topics, nil)
}

func Test_OwnedPartitionsInvalidatedForConsumerWithMultipleGeneration(t *testing.T) {
	t.Parallel()

	topics := map[string]int32{
		"topic":  3,
		"topic2": 3,
	}

	currentGeneration := 10

	members := []GroupMember{
		{
			ID:     "consumer1",
			Topics: []string{"topic", "topic2"},
			UserData: newUD().
				assign("topic", 0).
				assign("topic2", 1).
				assign("topic", 1).
				setGeneration(currentGeneration).
				encode(),
		},
		{
			ID:     "consumer2",
			Topics: []string{"topic", "topic2"},
			UserData: newUD().
				assign("topic", 0).  // Conflicts with consumer1
				assign("topic2", 1). // Conflicts with consumer1
				assign("topic2", 2).
				setGeneration(currentGeneration - 2).
				encode(),
		},
	}

	plan := Balance(members, topics)

	// Consumer1 has higher generation so keeps conflicting partitions
	// Both should get 3 partitions each for balance
	testEqualDivvy(t, plan, 4, members)
	testPlanUsage(t, plan, topics, nil)
}

func TestLarge(t *testing.T) {
	t.Parallel()
	{
		plan := Balance(large.members, large.topics)
		testPlanUsage(t, plan, large.topics, nil)
	}
	{
		plan := Balance(largeImbalanced.members, largeImbalanced.topics)
		testPlanUsage(t, plan, largeImbalanced.topics, nil)
	}
}

const (
	topicNum     = 100
	partitionNum = 200
	memberNum    = 100
)

var racks = []string{"rackA", "rackB", "rackC"}

func makeLargeBalance(withImbalance bool) generatedInput {
	rng := rand.New(rand.NewSource(0))
	var allTopics []string
	topics := make(map[string]int32)
	partitionRacks := make(map[string][]string)
	var totalPartitions int
	for i := range topicNum {
		n := rng.Intn(partitionNum * 5 / 2)
		totalPartitions += n
		topic := fmt.Sprintf("topic%d", i)
		topics[topic] = int32(n)
		allTopics = append(allTopics, topic)
		tr := make([]string, n)
		for j := range tr {
			tr[j] = racks[j%len(racks)]
		}
		partitionRacks[topic] = tr
	}

	var members []GroupMember
	for i := range memberNum {
		members = append(members, GroupMember{
			ID:     fmt.Sprintf("consumer%d", i),
			Topics: allTopics,
			Rack:   racks[i%len(racks)],
		})
	}
	if withImbalance {
		members = append(members, GroupMember{
			ID:     "imbalance",
			Topics: []string{"topic0"},
			Rack:   racks[0],
		})
	}
	return generatedInput{
		members,
		topics,
		partitionRacks,
		totalPartitions,
	}
}

func makeLargeBalanceWithExisting(withImbalance bool) generatedInput {
	input := makeLargeBalance(withImbalance)
	plan := Balance(input.members, input.topics)

	oldMembers := input.members
	input.members = input.members[:0]
	for i := range topicNum {
		consumer := fmt.Sprintf("consumer%d", i)
		input.members = append(input.members, GroupMember{
			ID:       consumer,
			Topics:   oldMembers[i].Topics, // evaluated before overwrite
			Rack:     oldMembers[i].Rack,
			UserData: udEncode(1, 1, plan[consumer]),
		})
	}
	return input
}

var (
	large           = makeLargeBalance(false)
	largeImbalanced = makeLargeBalance(true)

	largeWithExisting           = makeLargeBalanceWithExisting(false)
	largeWithExistingImbalanced = makeLargeBalanceWithExisting(true)
)

func BenchmarkLarge(b *testing.B) {
	for _, bench := range []struct {
		name  string
		input generatedInput
	}{
		{"fresh", large},
		{"existing", largeWithExisting},
		{"imbalanced", largeImbalanced},
		{"existing_imbalanced", largeWithExistingImbalanced},
	} {
		members := bench.input.members
		if bench.name == "existing" || bench.name == "existing_imbalanced" {
			members = members[1:]
		}
		b.Run(bench.name+"/no_rack", func(b *testing.B) {
			for b.Loop() {
				Balance(members, bench.input.topics)
			}
		})
		b.Run(bench.name+"/rack", func(b *testing.B) {
			for b.Loop() {
				BalanceWithRacks(members, bench.input.topics, bench.input.partitionRacks)
			}
		})
	}
}

type generatedInput struct {
	members        []GroupMember
	topics         map[string]int32
	partitionRacks map[string][]string

	totalPartitions int
}

func makeJavaPlan(topicCount, partitionCount, consumerCount int, imbalanced bool) generatedInput {
	p := generatedInput{
		topics:         make(map[string]int32),
		partitionRacks: make(map[string][]string),
	}
	var allTopics []string

	for i := range topicCount {
		topic := fmt.Sprintf("t%d", i)
		allTopics = append(allTopics, topic)
		p.topics[topic] = int32(partitionCount)
		p.totalPartitions += partitionCount
		tr := make([]string, partitionCount)
		for j := range tr {
			tr[j] = racks[j%len(racks)]
		}
		p.partitionRacks[topic] = tr
	}

	for i := range consumerCount {
		p.members = append(p.members, GroupMember{
			ID:     fmt.Sprintf("c%d", i),
			Topics: allTopics,
			Rack:   racks[i%len(racks)],
		})
	}

	if imbalanced {
		p.members = append(p.members, GroupMember{
			ID:     fmt.Sprintf("c%d", consumerCount),
			Topics: allTopics[:1],
			Rack:   racks[0],
		})
	}

	return p
}

var (
	javaLarge          = makeJavaPlan(500, 2000, 2000, false)
	javaLargeImbalance = makeJavaPlan(500, 2000, 2000, true)

	javaMedium          = makeJavaPlan(50, 1000, 1000, false)
	javaMediumImbalance = makeJavaPlan(50, 1000, 1000, true)

	javaSmall          = makeJavaPlan(50, 800, 800, false)
	javaSmallImbalance = makeJavaPlan(50, 800, 800, true)
)

func BenchmarkJava(b *testing.B) {
	for _, bench := range []struct {
		name  string
		input generatedInput
	}{
		{"large", javaLarge},
		{"large_imbalance", javaLargeImbalance},
		{"medium", javaMedium},
		{"medium_imbalance", javaMediumImbalance},
		{"small", javaSmall},
		{"small_imbalance", javaSmallImbalance},
	} {
		b.Run(bench.name+"/no_rack", func(b *testing.B) {
			for b.Loop() {
				Balance(bench.input.members, bench.input.topics)
			}
		})
		b.Run(bench.name+"/rack", func(b *testing.B) {
			for b.Loop() {
				BalanceWithRacks(bench.input.members, bench.input.topics, bench.input.partitionRacks)
			}
		})
	}
}

// countRackMatches counts how many assigned partitions are on the same
// rack as the member consuming them.
func countRackMatches(plan Plan, members []GroupMember, partitionRacks map[string][]string) int {
	memberRack := make(map[string]string)
	for _, m := range members {
		memberRack[m.ID] = m.Rack
	}
	var matches int
	for member, topics := range plan {
		rack := memberRack[member]
		if rack == "" {
			continue
		}
		for topic, partitions := range topics {
			racks := partitionRacks[topic]
			for _, p := range partitions {
				if int(p) < len(racks) && racks[p] == rack {
					matches++
				}
			}
		}
	}
	return matches
}

func TestRackAwareBasic(t *testing.T) {
	t.Parallel()
	// Two members in different racks, two topics with two partitions
	// each. Partition leaders are split across racks.
	members := []GroupMember{
		{ID: "A", Topics: []string{"t1", "t2"}, Rack: "rackA"},
		{ID: "B", Topics: []string{"t1", "t2"}, Rack: "rackB"},
	}
	topics := map[string]int32{"t1": 2, "t2": 2}
	partitionRacks := map[string][]string{
		"t1": {"rackA", "rackB"},
		"t2": {"rackA", "rackB"},
	}

	plan := BalanceWithRacks(members, topics, partitionRacks)
	testPlanUsage(t, plan, topics, nil)
	testEqualDivvy(t, plan, 0, members)

	matches := countRackMatches(plan, members, partitionRacks)
	if matches != 4 {
		t.Errorf("expected 4 rack matches, got %d", matches)
	}
}

func TestRackAwareBalanceOverLocality(t *testing.T) {
	t.Parallel()
	// Three members: two in rackA, one in rackB. All partitions in
	// rackA. Balance must still be maintained - we should not overload
	// rackA members just because they match.
	members := []GroupMember{
		{ID: "A1", Topics: []string{"t1"}, Rack: "rackA"},
		{ID: "A2", Topics: []string{"t1"}, Rack: "rackA"},
		{ID: "B1", Topics: []string{"t1"}, Rack: "rackB"},
	}
	topics := map[string]int32{"t1": 6}
	partitionRacks := map[string][]string{
		"t1": {"rackA", "rackA", "rackA", "rackA", "rackA", "rackA"},
	}

	plan := BalanceWithRacks(members, topics, partitionRacks)
	testPlanUsage(t, plan, topics, nil)
	testEqualDivvy(t, plan, 0, members)

	// Each member should get exactly 2 partitions (6/3).
	for member, memberTopics := range plan {
		n := partitionsForMember(memberTopics)
		if n != 2 {
			t.Errorf("member %s has %d partitions, want 2", member, n)
		}
	}
}

func TestRackAwareNoRackInfo(t *testing.T) {
	t.Parallel()
	// Members have racks but no partition rack info - should behave
	// identically to Balance().
	members := []GroupMember{
		{ID: "A", Topics: []string{"t1"}, Rack: "rackA"},
		{ID: "B", Topics: []string{"t1"}, Rack: "rackB"},
	}
	topics := map[string]int32{"t1": 4}

	plan := BalanceWithRacks(members, topics, nil)
	testPlanUsage(t, plan, topics, nil)
	testEqualDivvy(t, plan, 0, members)

	planNoRack := Balance(members, topics)
	testPlanUsage(t, planNoRack, topics, nil)
	testEqualDivvy(t, planNoRack, 0, members)
}

func TestRackAwareNoMemberRack(t *testing.T) {
	t.Parallel()
	// Partition racks present but no member has a rack - should
	// fall back to normal assignment.
	members := []GroupMember{
		{ID: "A", Topics: []string{"t1"}},
		{ID: "B", Topics: []string{"t1"}},
	}
	topics := map[string]int32{"t1": 4}
	partitionRacks := map[string][]string{
		"t1": {"rackA", "rackA", "rackB", "rackB"},
	}

	plan := BalanceWithRacks(members, topics, partitionRacks)
	testPlanUsage(t, plan, topics, nil)
	testEqualDivvy(t, plan, 0, members)
}

func TestRackAwareMixedRacks(t *testing.T) {
	t.Parallel()
	// Some members have racks, some don't. Members with racks should
	// get rack-matched partitions; member without rack gets the rest.
	members := []GroupMember{
		{ID: "A", Topics: []string{"t1"}, Rack: "rackA"},
		{ID: "B", Topics: []string{"t1"}, Rack: "rackB"},
		{ID: "C", Topics: []string{"t1"}},
	}
	topics := map[string]int32{"t1": 6}
	partitionRacks := map[string][]string{
		"t1": {"rackA", "rackA", "rackB", "rackB", "rackA", "rackB"},
	}

	plan := BalanceWithRacks(members, topics, partitionRacks)
	testPlanUsage(t, plan, topics, nil)
	testEqualDivvy(t, plan, 0, members)

	// Each member gets 2 partitions. A and B should have rack-matched
	// partitions since there are enough.
	matches := countRackMatches(plan, members, partitionRacks)
	if matches < 4 {
		t.Errorf("expected at least 4 rack matches (A and B each 2), got %d", matches)
	}
}

func TestRackAwareComplex(t *testing.T) {
	t.Parallel()
	// Complex subscriptions: members subscribe to different topics.
	members := []GroupMember{
		{ID: "A", Topics: []string{"t1", "t2"}, Rack: "rackA"},
		{ID: "B", Topics: []string{"t1"}, Rack: "rackB"},
		{ID: "C", Topics: []string{"t2"}, Rack: "rackA"},
	}
	topics := map[string]int32{"t1": 4, "t2": 4}
	partitionRacks := map[string][]string{
		"t1": {"rackA", "rackB", "rackA", "rackB"},
		"t2": {"rackA", "rackB", "rackA", "rackB"},
	}

	plan := BalanceWithRacks(members, topics, partitionRacks)
	testPlanUsage(t, plan, topics, nil)

	// All 8 partitions should be assigned.
	total := 0
	for _, memberTopics := range plan {
		total += partitionsForMember(memberTopics)
	}
	if total != 8 {
		t.Errorf("expected 8 total partitions assigned, got %d", total)
	}

	matches := countRackMatches(plan, members, partitionRacks)
	if matches < 4 {
		t.Errorf("expected at least 4 rack matches, got %d", matches)
	}
}

func TestRackAwareSticky(t *testing.T) {
	t.Parallel()
	// First balance assigns with rack awareness. Second balance
	// (simulating a rebalance) should preserve sticky assignments.
	members := []GroupMember{
		{ID: "A", Topics: []string{"t1"}, Rack: "rackA"},
		{ID: "B", Topics: []string{"t1"}, Rack: "rackB"},
	}
	topics := map[string]int32{"t1": 4}
	partitionRacks := map[string][]string{
		"t1": {"rackA", "rackA", "rackB", "rackB"},
	}

	plan1 := BalanceWithRacks(members, topics, partitionRacks)
	testPlanUsage(t, plan1, topics, nil)
	testEqualDivvy(t, plan1, 0, members)

	// Build members with prior assignment for second balance.
	members2 := make([]GroupMember, len(members))
	for i, m := range members {
		members2[i] = GroupMember{
			ID:       m.ID,
			Topics:   m.Topics,
			Rack:     m.Rack,
			UserData: newUD().setGeneration(1).assign("t1", plan1[m.ID]["t1"]...).encode(),
		}
	}

	plan2 := BalanceWithRacks(members2, topics, partitionRacks)
	testPlanUsage(t, plan2, topics, nil)
	testEqualDivvy(t, plan2, 4, members2) // all 4 partitions should be sticky
}

func TestRackAwareNewMember(t *testing.T) {
	t.Parallel()
	// Start with 2 members, then add a third. The new member should
	// preferentially get rack-matched partitions.
	members := []GroupMember{
		{ID: "A", Topics: []string{"t1"}, Rack: "rackA"},
		{ID: "B", Topics: []string{"t1"}, Rack: "rackB"},
	}
	topics := map[string]int32{"t1": 6}
	partitionRacks := map[string][]string{
		"t1": {"rackA", "rackB", "rackA", "rackB", "rackA", "rackB"},
	}

	plan1 := BalanceWithRacks(members, topics, partitionRacks)
	testPlanUsage(t, plan1, topics, nil)

	// Add a new member in rackA. Rebalance should give it rack-matched
	// partitions where possible.
	members2 := []GroupMember{
		{
			ID: "A", Topics: []string{"t1"}, Rack: "rackA",
			UserData: newUD().setGeneration(1).assign("t1", plan1["A"]["t1"]...).encode(),
		},
		{
			ID: "B", Topics: []string{"t1"}, Rack: "rackB",
			UserData: newUD().setGeneration(1).assign("t1", plan1["B"]["t1"]...).encode(),
		},
		{ID: "C", Topics: []string{"t1"}, Rack: "rackA"},
	}

	plan2 := BalanceWithRacks(members2, topics, partitionRacks)
	testPlanUsage(t, plan2, topics, nil)
	testEqualDivvy(t, plan2, 4, members2) // 4 of 6 partitions should be sticky

	// C is in rackA, verify at least one of its partitions matches rackA.
	cParts := plan2["C"]["t1"]
	rackAMatches := 0
	for _, p := range cParts {
		if partitionRacks["t1"][p] == "rackA" {
			rackAMatches++
		}
	}
	if rackAMatches == 0 {
		t.Errorf("new member C in rackA got no rackA partitions: %v", cParts)
	}
}

func TestRackAwareManyMembers(t *testing.T) {
	t.Parallel()
	// Larger scale test: 10 members across 3 racks, 30 partitions.
	members := make([]GroupMember, 10)
	racks := []string{"rackA", "rackB", "rackC"}
	for i := range members {
		members[i] = GroupMember{
			ID:     fmt.Sprintf("M%d", i),
			Topics: []string{"t1"},
			Rack:   racks[i%3],
		}
	}
	topics := map[string]int32{"t1": 30}
	partitionRacks := map[string][]string{
		"t1": make([]string, 30),
	}
	for i := range partitionRacks["t1"] {
		partitionRacks["t1"][i] = racks[i%3]
	}

	plan := BalanceWithRacks(members, topics, partitionRacks)
	testPlanUsage(t, plan, topics, nil)
	testEqualDivvy(t, plan, 0, members)

	matches := countRackMatches(plan, members, partitionRacks)
	// 4 members in rackA (i%3==0), 3 each in rackB/rackC. 10
	// partitions per rack. rackA has 12 slots but only 10 matching
	// partitions; rackB/rackC each have 9 slots but 10 matching
	// partitions. Max rack matches = 10 + 9 + 9 = 28.
	if matches != 28 {
		t.Errorf("expected 28 rack matches, got %d", matches)
	}
}

func TestRackAwarePyramidSubscriptions(t *testing.T) {
	t.Parallel()
	// Pyramid subscriptions where A subscribes broadly, others to
	// subsets. Rack assignments alternate across partitions. The
	// complex path must handle rack-aware pre-assignment across
	// varying subscription widths without breaking balance.
	//
	// Subscriptions (pyramid shape):
	//   1: A, B
	//   2: A, B
	//   3: A, B, C
	//   4: A, B, C
	//   5: A, B, C, D
	//   6: A, B, C, D
	//   7: A, C, D
	//   8: A, C, D
	//
	// 8 partitions, 4 members, 2 each. Rack alternates A/B per topic.
	// With 4 rackA partitions (1,3,5,7) and 4 rackB (2,4,6,8), the
	// two rackA members (A,C) and two rackB members (B,D) should each
	// get rack-matched partitions despite the subscription constraints.
	members := []GroupMember{
		{ID: "A", Topics: []string{"1", "2", "3", "4", "5", "6", "7", "8"}, Rack: "rackA"},
		{ID: "B", Topics: []string{"1", "2", "3", "4", "5", "6"}, Rack: "rackB"},
		{ID: "C", Topics: []string{"3", "4", "5", "6", "7", "8"}, Rack: "rackA"},
		{ID: "D", Topics: []string{"5", "6", "7", "8"}, Rack: "rackB"},
	}
	topics := map[string]int32{
		"1": 1, "2": 1, "3": 1, "4": 1,
		"5": 1, "6": 1, "7": 1, "8": 1,
	}
	partitionRacks := map[string][]string{
		"1": {"rackA"}, "2": {"rackB"}, "3": {"rackA"}, "4": {"rackB"},
		"5": {"rackA"}, "6": {"rackB"}, "7": {"rackA"}, "8": {"rackB"},
	}

	plan := BalanceWithRacks(members, topics, partitionRacks)
	testPlanUsage(t, plan, topics, nil)
	testEqualDivvy(t, plan, 0, members)

	matches := countRackMatches(plan, members, partitionRacks)
	if matches < 6 {
		t.Errorf("expected at least 6 rack matches, got %d", matches)
	}
}

func TestRackAwareThreeRacksHomeTopics(t *testing.T) {
	t.Parallel()
	// Three racks, each member has a "home" topic (same rack as member)
	// plus a shared topic with partitions spread across all racks.
	// Each home topic can only be consumed by its member, so the member
	// must get all 3 of its home partitions. The shared topic's 6
	// partitions (2 per rack) should be rack-matched: each member gets
	// the 2 shared partitions in their rack.
	//
	// 15 partitions, 3 members, 5 each.
	members := []GroupMember{
		{ID: "A", Topics: []string{"home_a", "shared"}, Rack: "rackA"},
		{ID: "B", Topics: []string{"home_b", "shared"}, Rack: "rackB"},
		{ID: "C", Topics: []string{"home_c", "shared"}, Rack: "rackC"},
	}
	topics := map[string]int32{"home_a": 3, "home_b": 3, "home_c": 3, "shared": 6}
	partitionRacks := map[string][]string{
		"home_a": {"rackA", "rackA", "rackA"},
		"home_b": {"rackB", "rackB", "rackB"},
		"home_c": {"rackC", "rackC", "rackC"},
		"shared": {"rackA", "rackA", "rackB", "rackB", "rackC", "rackC"},
	}

	plan := BalanceWithRacks(members, topics, partitionRacks)
	testPlanUsage(t, plan, topics, nil)
	testEqualDivvy(t, plan, 0, members)

	matches := countRackMatches(plan, members, partitionRacks)
	// Each member gets all 3 home partitions (rack-matched = 9 guaranteed).
	// The 6 shared partitions are split by the complex path; map iteration
	// order can cause some shared partitions to land on a mismatched rack.
	if matches < 9 {
		t.Errorf("expected at least 9 rack matches, got %d", matches)
	}
}

func TestRackAwareSimpleMultiPartition(t *testing.T) {
	t.Parallel()
	// Simple path: all members subscribe to all topics. Three
	// multi-partition topics with rotated rack patterns so that
	// each rack has exactly 6 partitions across all topics. One
	// member per rack, 6 partitions each - perfect rack matching
	// should be achievable.
	members := []GroupMember{
		{ID: "A", Topics: []string{"t1", "t2", "t3"}, Rack: "rackA"},
		{ID: "B", Topics: []string{"t1", "t2", "t3"}, Rack: "rackB"},
		{ID: "C", Topics: []string{"t1", "t2", "t3"}, Rack: "rackC"},
	}
	topics := map[string]int32{"t1": 6, "t2": 6, "t3": 6}
	partitionRacks := map[string][]string{
		"t1": {"rackA", "rackA", "rackB", "rackB", "rackC", "rackC"},
		"t2": {"rackB", "rackB", "rackC", "rackC", "rackA", "rackA"},
		"t3": {"rackC", "rackC", "rackA", "rackA", "rackB", "rackB"},
	}

	plan := BalanceWithRacks(members, topics, partitionRacks)
	testPlanUsage(t, plan, topics, nil)
	testEqualDivvy(t, plan, 0, members)

	matches := countRackMatches(plan, members, partitionRacks)
	// 6 partitions per rack, one member per rack, 6 each. Perfect = 18.
	if matches < 14 {
		t.Errorf("expected at least 14 rack matches, got %d", matches)
	}
}

func TestRackAwareOverlappingTopics(t *testing.T) {
	t.Parallel()
	// Four members with overlapping subscriptions across three topics
	// that have very different rack layouts:
	//   t1 (4 partitions, all rackA) - only A,B subscribe
	//   t2 (4 partitions, mixed)     - all four subscribe
	//   t3 (4 partitions, all rackB) - only C,D subscribe
	//
	// The shared t2 creates tension: its rackA partitions should go to
	// A or C (rackA members), its rackB to B or D. But subscription
	// constraints force A,B to split t1 and C,D to split t3.
	//
	// 12 partitions, 4 members, 3 each.
	members := []GroupMember{
		{ID: "A", Topics: []string{"t1", "t2"}, Rack: "rackA"},
		{ID: "B", Topics: []string{"t1", "t2"}, Rack: "rackB"},
		{ID: "C", Topics: []string{"t2", "t3"}, Rack: "rackA"},
		{ID: "D", Topics: []string{"t2", "t3"}, Rack: "rackB"},
	}
	topics := map[string]int32{"t1": 4, "t2": 4, "t3": 4}
	partitionRacks := map[string][]string{
		"t1": {"rackA", "rackA", "rackA", "rackA"},
		"t2": {"rackA", "rackB", "rackA", "rackB"},
		"t3": {"rackB", "rackB", "rackB", "rackB"},
	}

	plan := BalanceWithRacks(members, topics, partitionRacks)
	testPlanUsage(t, plan, topics, nil)
	testEqualDivvy(t, plan, 0, members)

	matches := countRackMatches(plan, members, partitionRacks)
	// A(rackA) should get t1 rackA partitions + a t2 rackA partition.
	// D(rackB) should get t3 rackB partitions + a t2 rackB partition.
	// B gets some t1 (rackA, no match) + t2 rackB. C gets t2 rackA + t3 (no match).
	// Max achievable: 8.
	if matches < 6 {
		t.Errorf("expected at least 6 rack matches, got %d", matches)
	}
}

func TestRackAwareMemberLeaves(t *testing.T) {
	t.Parallel()
	// Round 1: three members balanced with rack awareness.
	// Round 2: one member leaves. Its partitions become unassigned and
	// the rack-aware pre-assignment should place them on rack-matched
	// remaining members.
	//
	// t1 has 6 partitions alternating rackA/rackB. In round 1,
	// A(rackA) and C(rackA) share the rackA partitions, B(rackB) gets
	// the rackB ones. When C leaves, its rackA partitions should flow
	// to A (also rackA), and B keeps its rackB partitions.
	members1 := []GroupMember{
		{ID: "A", Topics: []string{"t1"}, Rack: "rackA"},
		{ID: "B", Topics: []string{"t1"}, Rack: "rackB"},
		{ID: "C", Topics: []string{"t1"}, Rack: "rackA"},
	}
	topics := map[string]int32{"t1": 6}
	partitionRacks := map[string][]string{
		"t1": {"rackA", "rackB", "rackA", "rackB", "rackA", "rackB"},
	}

	plan1 := BalanceWithRacks(members1, topics, partitionRacks)
	testPlanUsage(t, plan1, topics, nil)
	testEqualDivvy(t, plan1, 0, members1)

	// C leaves. A and B carry prior assignments.
	members2 := []GroupMember{
		{
			ID: "A", Topics: []string{"t1"}, Rack: "rackA",
			UserData: newUD().assign("t1", plan1["A"]["t1"]...).encode(),
		},
		{
			ID: "B", Topics: []string{"t1"}, Rack: "rackB",
			UserData: newUD().assign("t1", plan1["B"]["t1"]...).encode(),
		},
	}

	plan2 := BalanceWithRacks(members2, topics, partitionRacks)
	testPlanUsage(t, plan2, topics, nil)
	testEqualDivvy(t, plan2, 4, members2) // A and B each keep their 2 from round 1

	matches := countRackMatches(plan2, members2, partitionRacks)
	// In round 1, the last rackB partition goes to A (rackA) via normal
	// fallback since B hits maxQuota first. Stickiness preserves that
	// mismatch in round 2, limiting rack matches to 4 instead of 6.
	if matches < 4 {
		t.Errorf("expected at least 4 rack matches, got %d", matches)
	}
}

func TestRackAwareTopicGrowth(t *testing.T) {
	t.Parallel()
	// Topic grows from 4 to 8 partitions. Prior assignments (already
	// rack-matched) should stay sticky. New partitions should also
	// be rack-matched via the rack-aware pre-assignment phase.
	members := []GroupMember{
		{
			ID: "A", Topics: []string{"t1"}, Rack: "rackA",
			UserData: newUD().assign("t1", 0, 2).encode(),
		},
		{
			ID: "B", Topics: []string{"t1"}, Rack: "rackB",
			UserData: newUD().assign("t1", 1, 3).encode(),
		},
	}
	topics := map[string]int32{"t1": 8}
	partitionRacks := map[string][]string{
		"t1": {"rackA", "rackB", "rackA", "rackB", "rackA", "rackB", "rackA", "rackB"},
	}

	plan := BalanceWithRacks(members, topics, partitionRacks)
	testPlanUsage(t, plan, topics, nil)
	testEqualDivvy(t, plan, 4, members) // prior 4 partitions stay sticky

	matches := countRackMatches(plan, members, partitionRacks)
	// Prior: A has p0(rackA),p2(rackA) = 2 matches. B has p1(rackB),p3(rackB) = 2.
	// New: p4(rackA),p6(rackA) -> A; p5(rackB),p7(rackB) -> B.
	// Total: 8 matches.
	if matches < 6 {
		t.Errorf("expected at least 6 rack matches, got %d", matches)
	}
}

func TestRackAwareImbalancedSubs(t *testing.T) {
	t.Parallel()
	// Members have constrained subscriptions: B can only consume t1,
	// C can only consume t2. A bridges both topics. Rack-aware must
	// work within these subscription constraints.
	//
	// t1: 4 partitions [rackA, rackB, rackA, rackB] - consumed by A,B
	// t2: 2 partitions [rackA, rackB]                - consumed by A,C
	//
	// 6 partitions, 3 members, 2 each.
	// B(rackB) should get rackB partitions from t1.
	// A(rackA) should get rackA partitions from t1.
	// C(rackA) should get rackA partition from t2 (plus one rackB).
	members := []GroupMember{
		{ID: "A", Topics: []string{"t1", "t2"}, Rack: "rackA"},
		{ID: "B", Topics: []string{"t1"}, Rack: "rackB"},
		{ID: "C", Topics: []string{"t2"}, Rack: "rackA"},
	}
	topics := map[string]int32{"t1": 4, "t2": 2}
	partitionRacks := map[string][]string{
		"t1": {"rackA", "rackB", "rackA", "rackB"},
		"t2": {"rackA", "rackB"},
	}

	plan := BalanceWithRacks(members, topics, partitionRacks)
	testPlanUsage(t, plan, topics, nil)
	testEqualDivvy(t, plan, 0, members)

	matches := countRackMatches(plan, members, partitionRacks)
	if matches < 4 {
		t.Errorf("expected at least 4 rack matches, got %d", matches)
	}
}

func TestRackAwareSparseRacks(t *testing.T) {
	t.Parallel()
	// Some partitions have no rack info (empty string), some have a rack
	// that no member matches, and partitionRacks references a topic not
	// in the topics map. Tests defensive branches in initRacks and
	// assignRackAware's simple path.
	members := []GroupMember{
		{ID: "A", Topics: []string{"t1", "t2"}, Rack: "rackA"},
		{ID: "B", Topics: []string{"t1", "t2"}, Rack: "rackB"},
	}
	topics := map[string]int32{"t1": 4, "t2": 3}
	partitionRacks := map[string][]string{
		"t1":        {"rackA", "", "rackC", "rackB"},      // p1 empty, p2 unmatched rack
		"t2":        {"rackA", "rackB", "rackA", "extra"}, // index 3 exceeds t2's 3 partitions
		"no_such_t": {"rackA"},                            // topic not in topics map
	}

	plan := BalanceWithRacks(members, topics, partitionRacks)
	testPlanUsage(t, plan, topics, nil)
	testEqualDivvy(t, plan, 0, members)

	matches := countRackMatches(plan, members, partitionRacks)
	if matches < 3 {
		t.Errorf("expected at least 3 rack matches, got %d", matches)
	}
}

func TestRackAwareSparseRacksComplex(t *testing.T) {
	t.Parallel()
	// Different subscriptions with sparse rack info and prior
	// assignments. Exercises the complex path's skip-already-assigned
	// branch, empty-rack branch, and unmatched-rack branch.
	members := []GroupMember{
		{
			ID: "A", Topics: []string{"t1", "t2"}, Rack: "rackA",
			UserData: newUD().assign("t1", 0).assign("t2", 1).encode(),
		},
		{
			ID: "B", Topics: []string{"t1"}, Rack: "rackB",
			UserData: newUD().assign("t1", 1).encode(),
		},
		{
			ID: "C", Topics: []string{"t2"}, Rack: "rackA",
			UserData: newUD().assign("t2", 0).encode(),
		},
	}
	topics := map[string]int32{"t1": 4, "t2": 4}
	partitionRacks := map[string][]string{
		"t1": {"rackA", "", "rackC", "rackB"}, // p1 empty, p2 unmatched
		"t2": {"", "rackA", "", "rackB"},      // p0,p2 empty

	}

	plan := BalanceWithRacks(members, topics, partitionRacks)
	testPlanUsage(t, plan, topics, nil)

	total := 0
	for _, mt := range plan {
		total += partitionsForMember(mt)
	}
	if total != 8 {
		t.Errorf("total partitions %d, want 8", total)
	}
}
