package kgo

import (
	"math/bits"
	"testing"
)

// testBackupIter mimics leastBackupInput: it iterates partition indices from
// last to first, returning the configured buffered-record count for each.
type testBackupIter struct{ backups []int64 }

func (i *testBackupIter) Next() (int, int64) {
	last := len(i.backups) - 1
	b := i.backups[last]
	i.backups = i.backups[:last]
	return last, b
}

func (i *testBackupIter) Rem() int { return len(i.backups) }

// The LeastBackupPartitioner's tie-break reservoir sampled every partition
// that was not strictly better than the current best, rather than only equal
// ties: a partition with MORE buffered records seen after the least could
// steal the pick with probability 1/npicked (and then be pinned until the
// next batch roll). The introducing commit's stated condition was "multiple
// partitions have the same backup".
func TestLeastBackupPicksLeast(t *testing.T) {
	t.Parallel()

	tp := LeastBackupPartitioner().ForTopic("t")
	tbp := tp.(TopicBackupPartitioner)
	onb := tp.(TopicPartitionerOnNewBatch)

	// Partition 1 is strictly least backed up; the worse partition 0 is
	// iterated after it (the iterator runs last to first) and pre-fix
	// steals the pick half the time.
	for i := 0; i < 200; i++ {
		onb.OnNewBatch()
		iter := &testBackupIter{backups: []int64{3, 0, 7}}
		if pick := tbp.PartitionByBackup(nil, 3, iter); pick != 1 {
			t.Fatalf("iteration %d: picked partition %d, want least-backed-up partition 1", i, pick)
		}
	}

	// Equal-least ties are chosen at random: over enough rolls with all
	// partitions equal, every partition must be picked at least once.
	seen := make(map[int]bool)
	for i := 0; i < 300; i++ {
		onb.OnNewBatch()
		iter := &testBackupIter{backups: []int64{2, 2, 2}}
		seen[tbp.PartitionByBackup(nil, 3, iter)] = true
	}
	for p := 0; p < 3; p++ {
		if !seen[p] {
			t.Errorf("equal-backup tie-break never picked partition %d over 300 rolls", p)
		}
	}
}

// The adaptive UniformBytesPartitioner re-picks a partition by weighted random
// selection. Two things can leave the weighted loop selecting nothing: a
// floating-point rounding edge (the code's own comment acknowledges this), or
// entering the re-pick with a stale p.onPart that is >= n because the writable
// partition count shrank under a pinned partitioner (a leader election dropping
// partitions from writablePartitions, with the byte threshold not crossed this
// call so onPart was not reset to the -1 sentinel). Pre-fix the rounding
// fallback only fired for onPart == -1, so a stale onPart >= n survived and was
// returned, which doPartition rejects as an out-of-range choice and fails the
// record. The non-adaptive branch (Intn(n)) re-picks unconditionally and never
// had this hole.
//
// We force the "loop selects nothing" path deterministically (independent of
// the rng and Go version) with a backup of -1: backup++ makes the weight
// 1/0 = +Inf, so pick - Inf is never <= 0. This is a stand-in for the
// production rounding fallthrough; the fix must fall back to a valid partition
// in both cases.
func TestAuditUniformBytesAdaptiveShrinkStalePartition(t *testing.T) {
	t.Parallel()

	p := UniformBytesPartitioner(1<<30, true, false, nil).ForTopic("t").(*uniformBytesTopicPartitioner)

	// Simulate a writable-partition shrink: the partitioner was pinned to
	// partition 5, but only 3 partitions are writable now. bytes stays well
	// below the 1<<30 threshold so the byte reset does not clear onPart.
	p.onPart = 5
	p.bytes = 0

	iter := &testBackupIter{backups: []int64{-1, 10, 20}}
	pick := p.PartitionByBackup(new(Record), 3, iter)
	if pick < 0 || pick >= 3 {
		t.Fatalf("adaptive re-pick after a writable-partition shrink returned out-of-range partition %d (n=3): the stale pin survived the no-selection fallback", pick)
	}

	// A normal re-pick (no Inf weight, fresh sentinel) must still land in
	// range - the fix must not regress the common path.
	p.onPart = -1
	p.bytes = 0
	iter = &testBackupIter{backups: []int64{1, 2, 3}}
	if pick := p.PartitionByBackup(new(Record), 3, iter); pick < 0 || pick >= 3 {
		t.Fatalf("adaptive re-pick on the normal path returned out-of-range partition %d (n=3)", pick)
	}
}

// Golden vectors from Kafka's UtilsTest.testMurmur2; murmur2 must match the
// reference implementation exactly for KafkaHasher placement compatibility.
func TestMurmur2KafkaVectors(t *testing.T) {
	t.Parallel()
	for _, c := range []struct {
		in   string
		want int32
	}{
		{"21", -973932308},
		{"foobar", -790332482},
		{"a-little-bit-long-string", -985981536},
		{"a-little-bit-longer-string", -1486304829},
		{"lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8", -58897971},
		{"abc", 479470107},
	} {
		if got := int32(murmur2([]byte(c.in))); got != c.want {
			t.Errorf("murmur2(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}

// SaramaHasher's arithmetic is int-width dependent BY DESIGN: the function
// exists to preserve a historical placement, so each platform's behavior is
// pinned exactly as it has always been (64-bit matches librdkafka's unsigned
// modulo; 32-bit negates wrapped high-bit hashes). See the SaramaHasher doc.
// If this test trips, key->partition mappings remap for existing
// deployments.
func TestSaramaHasherPinnedPlacement(t *testing.T) {
	t.Parallel()
	h := SaramaHasher(func([]byte) uint32 { return 0x80000001 })
	// 64-bit: int(0x80000001) = 2147483649; % 10 = 9.
	// 32-bit: int(0x80000001) = -2147483647; negated, % 10 = 7.
	want := 9
	if bits.UintSize == 32 {
		want = 7
	}
	if got := h(nil, 10); got != want {
		t.Errorf("SaramaHasher high-bit hash partition = %d, want %d (historical %d-bit placement)", got, want, bits.UintSize)
	}
	// A low-bit hash is identical under both interpretations.
	h = SaramaHasher(func([]byte) uint32 { return 12345 })
	if got := h(nil, 10); got != 5 {
		t.Errorf("SaramaHasher low-bit hash partition = %d, want 5", got)
	}
}
