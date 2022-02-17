package sticky

import (
	"fmt"
	"math/rand"
	"runtime"
	"testing"
	"time"
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

func makeLargeBalance(withImbalance bool) generatedInput {
	rng := rand.New(rand.NewSource(0))
	var allTopics []string
	topics := make(map[string]int32)
	var totalPartitions int
	for i := 0; i < topicNum; i++ {
		n := rng.Intn(partitionNum * 5 / 2)
		totalPartitions += n
		topic := fmt.Sprintf("topic%d", i)
		topics[topic] = int32(n)
		allTopics = append(allTopics, topic)
	}

	var members []GroupMember
	for i := 0; i < memberNum; i++ {
		members = append(members, GroupMember{
			ID:     fmt.Sprintf("consumer%d", i),
			Topics: allTopics,
		})
	}
	if withImbalance {
		members = append(members, GroupMember{
			ID:     "imbalance",
			Topics: []string{"topic0"},
		})
	}
	return generatedInput{
		members,
		topics,
		totalPartitions,
	}
}

func makeLargeBalanceWithExisting(withImbalance bool) generatedInput {
	input := makeLargeBalance(withImbalance)
	plan := Balance(input.members, input.topics)

	oldMembers := input.members
	input.members = input.members[:0]
	for i := 0; i < topicNum; i++ {
		consumer := fmt.Sprintf("consumer%d", i)
		input.members = append(input.members, GroupMember{
			ID:       consumer,
			Topics:   oldMembers[i].Topics, // evaluated before overwrite
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
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		Balance(large.members, large.topics)
	}
}

func BenchmarkLargeWithExisting(b *testing.B) {
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		Balance(largeWithExisting.members[1:], largeWithExisting.topics)
	}
}

func BenchmarkLargeImbalanced(b *testing.B) {
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		Balance(largeImbalanced.members, largeImbalanced.topics)
	}
}

func BenchmarkLargeWithExistingImbalanced(b *testing.B) {
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		Balance(largeWithExistingImbalanced.members[1:], largeWithExistingImbalanced.topics)
	}
}

type generatedInput struct {
	members []GroupMember
	topics  map[string]int32

	totalPartitions int
}

func makeJavaPlan(topicCount, partitionCount, consumerCount int, imbalanced bool) generatedInput {
	p := generatedInput{topics: make(map[string]int32)}
	var allTopics []string

	for i := 0; i < topicCount; i++ {
		topic := fmt.Sprintf("t%d", i)
		allTopics = append(allTopics, topic)
		p.topics[topic] = int32(partitionCount)
		p.totalPartitions += partitionCount
	}

	for i := 0; i < consumerCount; i++ {
		p.members = append(p.members, GroupMember{
			ID:     fmt.Sprintf("c%d", i),
			Topics: allTopics,
		})
	}

	if imbalanced {
		p.members = append(p.members, GroupMember{
			ID:     fmt.Sprintf("c%d", consumerCount),
			Topics: allTopics[:1],
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
		b.Run(bench.name, func(b *testing.B) {
			start := time.Now()
			for n := 0; n < b.N; n++ {
				Balance(bench.input.members, bench.input.topics)
				runtime.GC()
				runtime.GC()
			}
			b.Logf("avg %v per %d balances of %d members and %d total partitions",
				time.Since(start)/time.Duration(b.N),
				b.N,
				len(bench.input.members),
				bench.input.totalPartitions,
			)
		})
	}
}
