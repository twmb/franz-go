package kfake

import (
	"slices"
	"testing"
)

func TestConfigDefaults(t *testing.T) {
	t.Parallel()
	exceptions := map[string]struct{}{
		"broker.id":               {},
		"broker.rack":             {},
		"kfake.is_internal":       {},
		"sasl.enabled.mechanisms": {},
		"super.users":             {},
	}
	for k := range validTopicConfigs {
		if _, ok := configDefaults[k]; !ok {
			if _, ok := exceptions[k]; !ok {
				t.Errorf("configDefaults missing %q", k)
			}
		}
	}
}

func TestValidServerAssignor(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		ok   bool
	}{
		{"uniform", true},
		{"range", true},
		{"simple", false},
		{"", false},
		{"unknown", false},
	}
	for _, tt := range tests {
		if got := validServerAssignor(tt.name); got != tt.ok {
			t.Errorf("validServerAssignor(%q) = %v, want %v", tt.name, got, tt.ok)
		}
	}
}

// testTopic is one topic for testGroup: the ID the test wants it to
// have, and how many partitions it has.
type testTopic struct {
	id    uuid
	parts int32
}

// testGroup builds a group whose cluster knows the given topics, with
// the given assignor and members, for testing computeTargetAssignment.
func testGroup(assignor string, topics map[string]testTopic, members map[string][]string) *group {
	c := new(Cluster)
	c.data.c = c
	c.data.tps = make(tps[partData], len(topics))
	c.data.t2id = make(map[string]uuid, len(topics))
	c.data.id2t = make(map[uuid]string, len(topics))
	for topic, tt := range topics {
		ps := make(map[int32]*partData, tt.parts)
		for p := int32(0); p < tt.parts; p++ {
			ps[p] = new(partData)
		}
		c.data.tps[topic] = ps
		c.data.t2id[topic] = tt.id
		c.data.id2t[tt.id] = topic
	}
	g := &group{
		c:               c,
		assignorName:    assignor,
		consumerMembers: make(map[string]*consumerMember, len(members)),
		partitionEpochs: make(map[uuid]map[int32]partitionOwner),
	}
	for mid, topics := range members {
		g.consumerMembers[mid] = &consumerMember{
			memberID:         mid,
			subscribedTopics: topics,
			targetAssignment: make(map[uuid][]int32),
		}
	}
	return g
}

func TestAssignUniform(t *testing.T) {
	t.Parallel()
	idA := uuid{1}
	idB := uuid{2}
	g := testGroup("uniform", map[string]testTopic{
		"topicA": {id: idA, parts: 3},
		"topicB": {id: idB, parts: 3},
	}, map[string][]string{
		"m0": {"topicA", "topicB"},
		"m1": {"topicA", "topicB"},
	})
	g.computeTargetAssignment()

	// Uniform round-robin: 6 total partitions across 2 members => 3 each.
	for _, mid := range []string{"m0", "m1"} {
		m := g.consumerMembers[mid]
		total := 0
		for _, ps := range m.targetAssignment {
			total += len(ps)
		}
		if total != 3 {
			t.Errorf("member %s got %d partitions, want 3", mid, total)
		}
	}
}

func TestAssignUniformStickyOnLeave(t *testing.T) {
	t.Parallel()
	id := uuid{1}
	// Start with 5 members all subscribing to the same topic.
	g := testGroup("uniform", map[string]testTopic{
		"topic": {id: id, parts: 31},
	}, map[string][]string{
		"m0": {"topic"},
		"m1": {"topic"},
		"m2": {"topic"},
		"m3": {"topic"},
		"m4": {"topic"},
	})
	g.computeTargetAssignment()

	// Record the initial assignment for all members.
	before := make(map[string][]int32)
	for mid, m := range g.consumerMembers {
		before[mid] = slices.Clone(m.targetAssignment[id])
	}

	// Verify initial: 5 members, 31 partitions => 6,6,6,6,7.
	var total int
	for _, ps := range before {
		total += len(ps)
	}
	if total != 31 {
		t.Fatalf("initial total = %d, want 31", total)
	}

	// Remove m4 (simulating a leave).
	delete(g.consumerMembers, "m4")
	g.computeTargetAssignment()

	// After leave: 4 members, 31 partitions => 7,8,8,8.
	// ALL of each remaining member's old partitions must
	// still be in their new target (sticky).
	for _, mid := range []string{"m0", "m1", "m2", "m3"} {
		m := g.consumerMembers[mid]
		newTarget := m.targetAssignment[id]
		for _, p := range before[mid] {
			if !slices.Contains(newTarget, p) {
				t.Errorf("member %s lost partition %d: before=%v after=%v",
					mid, p, before[mid], newTarget)
			}
		}
	}
}

// TestAssignUniformStickyRapidJoinsThenLeave simulates the ETL
// integration test scenario: members join one-by-one (each
// triggering a target recomputation before any convergence), then
// one member leaves. After convergence, the remaining members'
// targets should be sticky - a leave should only redistribute the
// leaving member's partitions.
func TestAssignUniformStickyRapidJoinsThenLeave(t *testing.T) {
	t.Parallel()
	id := uuid{1}
	g := testGroup("uniform", map[string]testTopic{
		"topic": {id: id, parts: 31},
	}, map[string][]string{})
	allMembers := []string{"m0", "m1", "m2", "m3", "m4"}

	// Simulate rapid joins: each member joins and triggers
	// target recomputation immediately (no convergence between).
	for _, mid := range allMembers {
		g.consumerMembers[mid] = &consumerMember{
			memberID:         mid,
			subscribedTopics: []string{"topic"},
			targetAssignment: make(map[uuid][]int32),
		}
		g.computeTargetAssignment()
	}

	// Record the converged assignment (after all 5 joins).
	converged := make(map[string][]int32)
	for mid, m := range g.consumerMembers {
		converged[mid] = slices.Clone(m.targetAssignment[id])
	}

	// Now remove m4.
	delete(g.consumerMembers, "m4")
	g.computeTargetAssignment()

	// Every remaining member must keep ALL of their converged
	// partitions (sticky). They should only gain partitions,
	// never lose.
	for _, mid := range []string{"m0", "m1", "m2", "m3"} {
		m := g.consumerMembers[mid]
		newTarget := m.targetAssignment[id]
		for _, p := range converged[mid] {
			if !slices.Contains(newTarget, p) {
				t.Errorf("member %s lost partition %d: converged=%v after=%v",
					mid, p, converged[mid], newTarget)
			}
		}
	}
}

// TestAssignRange checks the range assignor: for each topic, the members
// subscribed to it take contiguous runs of its partitions, and the earlier
// members take the remainder.
func TestAssignRange(t *testing.T) {
	t.Parallel()
	idA, idB := uuid{1}, uuid{2}
	for _, tc := range []struct {
		name    string
		topics  map[string]testTopic
		members map[string][]string
		want    map[string]map[string][]int32 // member -> topic -> partitions
	}{
		{
			// 3 partitions over 2 members is 1 each plus 1 for the first.
			name:    "two-members-two-topics",
			topics:  map[string]testTopic{"topicA": {id: idA, parts: 3}, "topicB": {id: idB, parts: 3}},
			members: map[string][]string{"m0": {"topicA", "topicB"}, "m1": {"topicA", "topicB"}},
			want: map[string]map[string][]int32{
				"m0": {"topicA": {0, 1}, "topicB": {0, 1}},
				"m1": {"topicA": {2}, "topicB": {2}},
			},
		},
		{
			name:    "uneven-partitions",
			topics:  map[string]testTopic{"topicA": {id: idA, parts: 7}},
			members: map[string][]string{"m0": {"topicA"}, "m1": {"topicA"}},
			want: map[string]map[string][]int32{
				"m0": {"topicA": {0, 1, 2, 3}},
				"m1": {"topicA": {4, 5, 6}},
			},
		},
		{
			// m2 gets nothing at all, not an empty entry.
			name:    "more-members-than-partitions",
			topics:  map[string]testTopic{"topicA": {id: idA, parts: 2}},
			members: map[string][]string{"m0": {"topicA"}, "m1": {"topicA"}, "m2": {"topicA"}},
			want: map[string]map[string][]int32{
				"m0": {"topicA": {0}},
				"m1": {"topicA": {1}},
				"m2": {},
			},
		},
		{
			// Each topic is split only across the members that asked for it.
			name:    "heterogeneous-subscriptions",
			topics:  map[string]testTopic{"topicA": {id: idA, parts: 4}, "topicB": {id: idB, parts: 4}},
			members: map[string][]string{"m0": {"topicA", "topicB"}, "m1": {"topicA"}, "m2": {"topicB"}},
			want: map[string]map[string][]int32{
				"m0": {"topicA": {0, 1}, "topicB": {0, 1}},
				"m1": {"topicA": {2, 3}},
				"m2": {"topicB": {2, 3}},
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			g := testGroup("range", tc.topics, tc.members)
			g.computeTargetAssignment()

			for mid, wantTopics := range tc.want {
				m := g.consumerMembers[mid]
				if len(m.targetAssignment) != len(wantTopics) {
					t.Errorf("member %s assigned %v, want %v", mid, m.targetAssignment, wantTopics)
					continue
				}
				for topic, wantParts := range wantTopics {
					if got := m.targetAssignment[tc.topics[topic].id]; !slices.Equal(got, wantParts) {
						t.Errorf("member %s %s = %v, want %v", mid, topic, got, wantParts)
					}
				}
			}
		})
	}
}
