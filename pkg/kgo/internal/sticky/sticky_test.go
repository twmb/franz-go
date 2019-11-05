package sticky

import (
	"fmt"
	"math/rand"
	"testing"
)

func Test_stickyBalanceStrategy_Plan(t *testing.T) {
	for _, test := range []struct {
		name         string
		members      []GroupMember
		topics       map[string][]int32
		unusedTopics []string
		nsticky      int
	}{
		{
			name:    "no members, no plan",
			members: []GroupMember{},
			topics: map[string][]int32{
				"a": {0},
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
			topics: map[string][]int32{
				"t1": {0, 1, 2},
			},
		},

		{
			name: "only assigns from subscribed topics",
			members: []GroupMember{
				{ID: "A", Topics: []string{"t1"}},
			},
			topics: map[string][]int32{
				"t1": {0, 1, 2},
				"t2": {0, 1, 2},
			},
			unusedTopics: []string{"t2"},
		},

		{
			name: "one consumer multiple topics",
			members: []GroupMember{
				{ID: "A", Topics: []string{"t1", "t2"}},
			},
			topics: map[string][]int32{
				"t1": {0},
				"t2": {0, 1},
			},
		},

		{
			name: "two consumers, one topic with one partition",
			members: []GroupMember{
				{ID: "A", Topics: []string{"t1"}},
				{ID: "B", Topics: []string{"t1"}},
			},
			topics: map[string][]int32{
				"t1": {0},
			},
		},

		{
			name: "two consumers, one topic with two partitions",
			members: []GroupMember{
				{ID: "A", Topics: []string{"t1"}},
				{ID: "B", Topics: []string{"t1"}},
			},
			topics: map[string][]int32{
				"t1": {0, 1},
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
				"t1": {0, 1, 2},
				"t2": {0, 1},
			},
		},

		{
			name: "two consumers with two topics and six partitions",
			members: []GroupMember{
				{ID: "A", Topics: []string{"t1", "t2"}},
				{ID: "B", Topics: []string{"t1", "t2"}},
			},
			topics: map[string][]int32{
				"t1": {0, 1, 2},
				"t2": {0, 1, 2},
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
				{ID: "C", Topics: []string{"t1"}},
			},
			topics: map[string][]int32{
				"t1": {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11},
			},
			nsticky: 8,
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
				"t1":     {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
				"unused": {0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12},
			},
			unusedTopics: []string{"unused"},
			nsticky:      9,
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
				"t1": {0},
				"t2": {0},
			},
			unusedTopics: []string{"t1"},
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
				"t1": {0, 1},
				"t2": {0, 1},
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
				"1": {0},
				"2": {0},
				"3": {0},
				"4": {0},
				"5": {0},
				"6": {0},
				"7": {0},
				"8": {0},
				"9": {0},
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
				"1": {0},
				"2": {0},
				"3": {0},
				"4": {0},
				"5": {0},
				"6": {0},
				"7": {0},
				"8": {0},
				"9": {0},
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
				"1": {0},
				"2": {0},
				"3": {0},
				"4": {0},
				"5": {0},
				"6": {0},
				"7": {0},
				"8": {0},
				"9": {0},
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
				"1": {0},
				"2": {0},
				"3": {0},
				"4": {0},
				"5": {0},
				"6": {0},
				"7": {0},
				"8": {0},
				"9": {0},
				"a": {0},
				"b": {0},
				"c": {0},
				"d": {0},
				"e": {0},
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
				{ID: "A", Topics: []string{"1", "2", "3"},
					Version: 1,
					UserData: newUD().
						assign("1", 0).
						assign("2", 0).
						encode()},
				{ID: "B", Topics: []string{"1", "2", "3", "4", "5", "6"},
					Version: 1,
					UserData: newUD().
						assign("3", 0).
						encode()},
				{ID: "C", Topics: []string{"4", "5", "6", "7", "8", "9"},
					Version: 1,
					UserData: newUD().
						assign("4", 0).
						assign("5", 0).
						encode()},
				{ID: "D", Topics: []string{"6", "7", "8", "9", "a", "b", "c", "d", "e", "f"},
					Version: 1,
					UserData: newUD().
						assign("6", 0).
						assign("7", 0).
						encode()},
				{ID: "E", Topics: []string{"6", "7", "8", "9", "a", "b", "c", "d", "e", "f"},
					Version: 1,
					UserData: newUD().
						assign("8", 0).
						assign("9", 0).
						assign("a", 0).
						assign("b", 0).
						assign("c", 0).
						assign("d", 0).
						assign("e", 0).
						assign("f", 0).
						encode()},
			},
			topics: map[string][]int32{
				"1": {0},
				"2": {0},
				"3": {0},
				"4": {0},
				"5": {0},
				"6": {0},
				"7": {0},
				"8": {0},
				"9": {0},
				"a": {0},
				"b": {0},
				"c": {0},
				"d": {0},
				"e": {0},
				"f": {0},
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
	for _, test := range []struct {
		name    string
		members []GroupMember
		topics  map[string][]int32
		nsticky int
		balance map[int]resultOptions
	}{

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
				"1": {0},
				"2": {0},
				"3": {0},
				"4": {0},
				"5": {0},
				"6": {0},
				"7": {0},
				"8": {0},
				"9": {0},
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
				"1": {0},
				"2": {0},
				"3": {0},
				"4": {0},
				"5": {0},
				"6": {0},
				"7": {0},
				"8": {0},
				"9": {0},
				"a": {0},
				"b": {0},
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
				"1": {0},
				"2": {0},
				"3": {0},
				"4": {0},
				"5": {0},
				"6": {0},
				"7": {0},
				"8": {0},
				"9": {0},
				"a": {0},
				"b": {0},
				"c": {0},
				"d": {0},
				"e": {0},
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
				{ID: "A", Topics: []string{"1", "2", "3", "4"},
					Version: 1,
					UserData: newUD().
						encode()},
				{ID: "B", Topics: []string{"1", "2", "3", "4", "5", "6", "7", "8", "9", "a", "b", "c", "d"},
					Version: 1,
					UserData: newUD().
						assign("1", 0).
						assign("2", 0).
						assign("3", 0).
						assign("4", 0).
						encode()},
				{ID: "C", Topics: []string{"5", "6", "7", "8", "9", "a", "b", "c", "d"},
					Version: 1,
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
						encode()},
				{ID: "D", Topics: []string{"5", "6", "7", "8", "9", "a", "b", "c", "d", "e", "f", "g", "h", "i", "j"},
					Version: 1,
					UserData: newUD().
						assign("e", 0).
						assign("f", 0).
						assign("g", 0).
						assign("h", 0).
						assign("i", 0).
						assign("j", 0).
						encode()},
				{ID: "E", Topics: []string{"e"},
					Version: 1,
					UserData: newUD().
						encode()},
				{ID: "F", Topics: []string{"f"},
					Version: 1,
					UserData: newUD().
						encode()},
				{ID: "G", Topics: []string{"g"},
					Version: 1,
					UserData: newUD().
						encode()},
				{ID: "H", Topics: []string{"h"},
					Version: 1,
					UserData: newUD().
						encode()},
				{ID: "I", Topics: []string{"i"},
					Version: 1,
					UserData: newUD().
						encode()},
				{ID: "J", Topics: []string{"j"},
					Version: 1,
					UserData: newUD().
						encode()},
			},
			topics: map[string][]int32{
				"1": {0},
				"2": {0},
				"3": {0},
				"4": {0},
				"5": {0},
				"6": {0},
				"7": {0},
				"8": {0},
				"9": {0},
				"a": {0},
				"b": {0},
				"c": {0},
				"d": {0},
				"e": {0},
				"f": {0},
				"g": {0},
				"h": {0},
				"i": {0},
				"j": {0},
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
				{ID: "A", Topics: []string{"1", "2", "3"},
					Version: 1,
					UserData: newUD().
						assign("1", 0).
						assign("2", 0).
						encode()},
				{ID: "B", Topics: []string{"3", "4", "5"},
					Version: 1,
					UserData: newUD().
						encode()},
				{ID: "C", Topics: []string{"3", "4", "5", "6", "7", "8", "9", "a", "b", "c"},
					Version: 1,
					UserData: newUD().
						assign("3", 0).
						assign("4", 0).
						assign("5", 0).
						encode()},
				{ID: "D", Topics: []string{"3", "4", "5", "6", "7", "8", "9", "a", "b", "c",
					"x"}, // x does not exist; hits continue branch in assign
					Version: 1,
					UserData: newUD().
						assign("6", 0).
						assign("7", 0).
						assign("8", 0).
						assign("9", 0).
						assign("a", 0).
						assign("b", 0).
						assign("c", 0).
						assign("z", 0). // no longer exists; dropped in parseMemberMetadata
						encode()},
			},
			topics: map[string][]int32{
				"1": {0},
				"2": {0},
				"3": {0},
				"4": {0},
				"5": {0},
				"6": {0},
				"7": {0},
				"8": {0},
				"9": {0},
				"a": {0},
				"b": {0},
				"c": {0},
			},
			nsticky: 2 + 4,
			balance: map[int]resultOptions{
				2: {[]string{"A", "B"}, 1},
				3: {[]string{"A", "B", "C"}, 2},
				4: {[]string{"D"}, 1},
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
	for _, test := range []struct {
		name    string
		members []GroupMember
		topics  map[string][]int32
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
				{ID: "A", Topics: []string{"3", "4"},
					Version: 1,
					UserData: newUD().
						assign("1", 0).
						assign("2", 0).
						setGeneration(1).
						encode()},
				{ID: "B", Topics: []string{"3", "4"},
					Version: 1,
					UserData: newUD().
						assign("3", 0).
						assign("4", 0).
						setGeneration(2).
						encode()},
				{ID: "C", Topics: []string{"1", "2", "5"},
					Version: 1,
					UserData: newUD().
						assign("1", 0).
						assign("2", 0).
						assign("5", 0).
						setGeneration(2).
						encode()},
			},
			topics: map[string][]int32{
				"1": {0},
				"2": {0},
				"3": {0},
				"4": {0},
				"5": {0},
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
	// PLAN 1
	members := []GroupMember{
		{ID: "A", Topics: []string{"1", "2", "3", "4"}},
		{ID: "B", Topics: []string{"1", "2", "3", "4"}},
		{ID: "C", Topics: []string{"1", "2", "3", "4"}},
	}
	topics := map[string][]int32{
		"1": {0, 1},
		"2": {0, 1},
		"3": {0, 1},
		"4": {0, 1},
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
	// PLAN 1
	members := []GroupMember{
		{ID: "1", Topics: []string{"1"}},
		{ID: "2", Topics: []string{"1", "2"}},
		{ID: "3", Topics: []string{"1", "2", "3"}},
	}
	topics := map[string][]int32{
		"1": {0},
		"2": {0, 1},
		"3": {0, 1, 2},
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
	// PLAN 1
	members := []GroupMember{
		{ID: "1", Topics: []string{"1", "2"}},
		{ID: "2", Topics: []string{"1", "2"}},
	}
	topics := map[string][]int32{
		"1": {0, 1},
		"2": {0, 1},
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
	// PLAN 1
	members := []GroupMember{
		{ID: "1", Topics: []string{"T"}},
	}
	topics := map[string][]int32{
		"T": {0, 1, 2},
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
	topics := map[string][]int32{"1": {0, 1, 2, 3, 4, 5}}
	members := []GroupMember{
		{ID: "1", Topics: []string{"1"}, Version: 1},
		{ID: "2", Topics: []string{"1"}, Version: 1},
		{ID: "3", Topics: []string{"1"}, Version: 1},
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
		Version:  1,
		UserData: udEncode(1, 1, plan1["3"]),
	})

	plan3 := Balance(members, topics)
	testEqualDivvy(t, plan3, 4, members)
	testPlanUsage(t, plan3, topics, nil)
}

func Test_stickyBalanceStrategy_Plan_AssignmentWithMultipleGenerations2(t *testing.T) {
	topics := map[string][]int32{"1": {0, 1, 2, 3, 4, 5}}
	members := []GroupMember{
		{ID: "1", Topics: []string{"1"}, Version: 1},
		{ID: "2", Topics: []string{"1"}, Version: 1},
		{ID: "3", Topics: []string{"1"}, Version: 1},
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

	topics := map[string][]int32{"1": {0, 1, 2, 3, 4, 5}}
	members := []GroupMember{
		{ID: "1", Topics: []string{"1"},
			Version: 1,
			UserData: newUD().
				assign("1", 0, 1, 4).
				setGeneration(1).
				encode()},
		{ID: "2", Topics: []string{"1"},
			Version: 1,
			UserData: newUD().
				assign("1", 0, 2, 3). // double zero consumption... OK I guess...
				setGeneration(1).
				encode()},
		{ID: "3", Topics: []string{"1"},
			Version: 1,
			UserData: newUD().
				assign("1", 3, 4, 5).
				setGeneration(2).
				encode()},
	}

	plan := Balance(members, topics)

	testEqualDivvy(t, plan, 6, members)
	testPlanUsage(t, plan, topics, nil)
}

func Test_stickyBalanceStrategy_Plan_SchemaBackwardCompatibility(t *testing.T) {
	topics := map[string][]int32{"1": {0, 1, 2}}
	members := []GroupMember{
		{ID: "1", Topics: []string{"1"},
			Version: 1,
			UserData: newUD().
				assign("1", 0, 2).
				setGeneration(1).
				encode()},
		{ID: "2", Topics: []string{"1"},
			Version: 0,
			UserData: oldUD().
				assign("1", 1).
				encode()},
		{ID: "3", Topics: []string{"1"}},
	}

	plan := Balance(members, topics)
	testEqualDivvy(t, plan, 2, members)
	testPlanUsage(t, plan, topics, nil)
}

func Test_stickyBalanceStrategy_Plan_ConflictingPreviousAssignments(t *testing.T) {
	topics := map[string][]int32{"1": {0, 1}}
	members := []GroupMember{
		{ID: "1", Topics: []string{"1"},
			Version: 1,
			UserData: newUD().
				assign("1", 0, 1).
				encode()},
		{ID: "2", Topics: []string{"1"},
			Version: 1,
			UserData: newUD().
				assign("1", 0, 1).
				encode()},
	}
	plan := Balance(members, topics)
	testEqualDivvy(t, plan, 2, members)
	testPlanUsage(t, plan, topics, nil)
}

func TestLarge(t *testing.T) {
	{
		members, topics := makeLargeBalance(t, false)
		plan := Balance(members, topics)
		testPlanUsage(t, plan, topics, nil)
	}
	{
		members, topics := makeLargeBalance(t, true)
		plan := Balance(members, topics)
		testPlanUsage(t, plan, topics, nil)
	}
}

const topicNum = 100
const partitionNum = 200

func makeLargeBalance(tb testing.TB, withImbalance bool) ([]GroupMember, map[string][]int32) {
	rng := rand.New(rand.NewSource(0))
	var members []GroupMember
	for i := 0; i < topicNum; i++ {
		topics := make([]string, topicNum)
		for j := range topics {
			topics[j] = fmt.Sprintf("topic%d", j)
		}
		members = append(members, GroupMember{
			ID:     fmt.Sprintf("consumer%d", i),
			Topics: topics,
		})
	}
	if withImbalance {
		members = append(members, GroupMember{
			ID:     "imbalance",
			Topics: []string{"topic0"},
		})
	}

	// now we make topicNum topics
	topics := make(map[string][]int32)
	var totalPartitions int
	for i := 0; i < topicNum; i++ {
		n := rng.Intn(partitionNum * 5 / 2)
		totalPartitions += n
		partitions := make([]int32, n)
		for j := range partitions {
			partitions[j] = int32(j)
		}
		topics[fmt.Sprintf("topic%d", i)] = partitions
	}
	tb.Logf("%d total partitions; %d total members", totalPartitions, len(members))
	return members, topics
}

func makeLargeBalanceWithExisting(tb testing.TB, withImbalance bool) ([]GroupMember, map[string][]int32) {
	members, topics := makeLargeBalance(tb, withImbalance)
	plan := Balance(members, topics)

	oldMembers := members
	members = members[:0]
	for i := 0; i < topicNum; i++ {
		consumer := fmt.Sprintf("consumer%d", i)
		members = append(members, GroupMember{
			ID:       consumer,
			Version:  1,
			Topics:   oldMembers[i].Topics, // evaluated before overwrite
			UserData: udEncode(1, 1, plan[consumer]),
		})
	}
	return members, topics
}

func BenchmarkLarge(b *testing.B) {
	members, topics := makeLargeBalance(b, false)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		Balance(members, topics)
	}
}

func BenchmarkLargeWithExisting(b *testing.B) {
	members, topics := makeLargeBalanceWithExisting(b, false)
	members = members[1:]
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		Balance(members, topics)
	}
}

func BenchmarkLargeImbalanced(b *testing.B) {
	members, topics := makeLargeBalance(b, true)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		Balance(members, topics)
	}
}

func BenchmarkLargeWithExistingImbalanced(b *testing.B) {
	members, topics := makeLargeBalanceWithExisting(b, true)
	members = members[1:]
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		Balance(members, topics)
	}
}
