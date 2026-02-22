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

// testGroup builds a group with the given assignor and members for
// testing computeTargetAssignment.
func testGroup(assignor string, members map[string][]string, snap topicMetaSnap) *group {
	g := &group{
		assignorName:    assignor,
		consumerMembers: make(map[string]*consumerMember, len(members)),
		partitionEpochs: make(map[uuid]map[int32]int32),
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
	snap := topicMetaSnap{
		"topicA": {id: idA, partitions: 3},
		"topicB": {id: idB, partitions: 3},
	}

	g := testGroup("uniform", map[string][]string{
		"m0": {"topicA", "topicB"},
		"m1": {"topicA", "topicB"},
	}, snap)
	g.computeTargetAssignment(snap)

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

func TestAssignRangeTwoMembersTwoTopics(t *testing.T) {
	t.Parallel()
	idA := uuid{1}
	idB := uuid{2}
	snap := topicMetaSnap{
		"topicA": {id: idA, partitions: 3},
		"topicB": {id: idB, partitions: 3},
	}

	g := testGroup("range", map[string][]string{
		"m0": {"topicA", "topicB"},
		"m1": {"topicA", "topicB"},
	}, snap)
	g.computeTargetAssignment(snap)

	// Range: topicA [0,1] to m0, [2] to m1; topicB [0,1] to m0, [2] to m1.
	// (3 partitions / 2 members = 1 base + 1 extra for first member)
	m0 := g.consumerMembers["m0"]
	m1 := g.consumerMembers["m1"]
	if !slices.Equal(m0.targetAssignment[idA], []int32{0, 1}) {
		t.Errorf("m0 topicA = %v, want [0 1]", m0.targetAssignment[idA])
	}
	if !slices.Equal(m1.targetAssignment[idA], []int32{2}) {
		t.Errorf("m1 topicA = %v, want [2]", m1.targetAssignment[idA])
	}
	if !slices.Equal(m0.targetAssignment[idB], []int32{0, 1}) {
		t.Errorf("m0 topicB = %v, want [0 1]", m0.targetAssignment[idB])
	}
	if !slices.Equal(m1.targetAssignment[idB], []int32{2}) {
		t.Errorf("m1 topicB = %v, want [2]", m1.targetAssignment[idB])
	}
}

func TestAssignRangeUnevenPartitions(t *testing.T) {
	t.Parallel()
	id := uuid{1}
	snap := topicMetaSnap{
		"topic": {id: id, partitions: 7},
	}

	g := testGroup("range", map[string][]string{
		"m0": {"topic"},
		"m1": {"topic"},
	}, snap)
	g.computeTargetAssignment(snap)

	// 7 partitions, 2 members => m0 gets [0,1,2,3], m1 gets [4,5,6].
	m0 := g.consumerMembers["m0"]
	m1 := g.consumerMembers["m1"]
	if !slices.Equal(m0.targetAssignment[id], []int32{0, 1, 2, 3}) {
		t.Errorf("m0 = %v, want [0 1 2 3]", m0.targetAssignment[id])
	}
	if !slices.Equal(m1.targetAssignment[id], []int32{4, 5, 6}) {
		t.Errorf("m1 = %v, want [4 5 6]", m1.targetAssignment[id])
	}
}

func TestAssignRangeMoreMembersThanPartitions(t *testing.T) {
	t.Parallel()
	id := uuid{1}
	snap := topicMetaSnap{
		"topic": {id: id, partitions: 2},
	}

	g := testGroup("range", map[string][]string{
		"m0": {"topic"},
		"m1": {"topic"},
		"m2": {"topic"},
	}, snap)
	g.computeTargetAssignment(snap)

	// 2 partitions, 3 members => m0 gets [0], m1 gets [1], m2 gets nothing.
	m0 := g.consumerMembers["m0"]
	m1 := g.consumerMembers["m1"]
	m2 := g.consumerMembers["m2"]
	if !slices.Equal(m0.targetAssignment[id], []int32{0}) {
		t.Errorf("m0 = %v, want [0]", m0.targetAssignment[id])
	}
	if !slices.Equal(m1.targetAssignment[id], []int32{1}) {
		t.Errorf("m1 = %v, want [1]", m1.targetAssignment[id])
	}
	if len(m2.targetAssignment) != 0 {
		t.Errorf("m2 = %v, want empty", m2.targetAssignment)
	}
}

func TestAssignRangeHeterogeneousSubscriptions(t *testing.T) {
	t.Parallel()
	idA := uuid{1}
	idB := uuid{2}
	snap := topicMetaSnap{
		"topicA": {id: idA, partitions: 4},
		"topicB": {id: idB, partitions: 4},
	}

	// m0 subscribes to both, m1 only topicA, m2 only topicB.
	g := testGroup("range", map[string][]string{
		"m0": {"topicA", "topicB"},
		"m1": {"topicA"},
		"m2": {"topicB"},
	}, snap)
	g.computeTargetAssignment(snap)

	// topicA: subscribed by m0, m1 => m0 gets [0,1], m1 gets [2,3]
	// topicB: subscribed by m0, m2 => m0 gets [0,1], m2 gets [2,3]
	m0 := g.consumerMembers["m0"]
	m1 := g.consumerMembers["m1"]
	m2 := g.consumerMembers["m2"]
	if !slices.Equal(m0.targetAssignment[idA], []int32{0, 1}) {
		t.Errorf("m0 topicA = %v, want [0 1]", m0.targetAssignment[idA])
	}
	if !slices.Equal(m1.targetAssignment[idA], []int32{2, 3}) {
		t.Errorf("m1 topicA = %v, want [2 3]", m1.targetAssignment[idA])
	}
	if !slices.Equal(m0.targetAssignment[idB], []int32{0, 1}) {
		t.Errorf("m0 topicB = %v, want [0 1]", m0.targetAssignment[idB])
	}
	if !slices.Equal(m2.targetAssignment[idB], []int32{2, 3}) {
		t.Errorf("m2 topicB = %v, want [2 3]", m2.targetAssignment[idB])
	}
}

func TestAssignUniformStickyOnLeave(t *testing.T) {
	t.Parallel()
	id := uuid{1}
	snap := topicMetaSnap{
		"topic": {id: id, partitions: 31},
	}

	// Start with 5 members all subscribing to the same topic.
	g := testGroup("uniform", map[string][]string{
		"m0": {"topic"},
		"m1": {"topic"},
		"m2": {"topic"},
		"m3": {"topic"},
		"m4": {"topic"},
	}, snap)
	g.computeTargetAssignment(snap)

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
	g.computeTargetAssignment(snap)

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
	snap := topicMetaSnap{
		"topic": {id: id, partitions: 31},
	}

	g := testGroup("uniform", map[string][]string{}, snap)
	allMembers := []string{"m0", "m1", "m2", "m3", "m4"}

	// Simulate rapid joins: each member joins and triggers
	// target recomputation immediately (no convergence between).
	for _, mid := range allMembers {
		g.consumerMembers[mid] = &consumerMember{
			memberID:         mid,
			subscribedTopics: []string{"topic"},
			targetAssignment: make(map[uuid][]int32),
		}
		g.computeTargetAssignment(snap)
	}

	// Record the converged assignment (after all 5 joins).
	converged := make(map[string][]int32)
	for mid, m := range g.consumerMembers {
		converged[mid] = slices.Clone(m.targetAssignment[id])
	}

	// Now remove m4.
	delete(g.consumerMembers, "m4")
	g.computeTargetAssignment(snap)

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
