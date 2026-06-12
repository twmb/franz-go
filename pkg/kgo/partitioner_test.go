package kgo

import (
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

// SaramaHasher's documented behavior (librdkafka-consistent: unsigned hash
// mod n, no negation) is what 64-bit platforms have always produced; the
// arithmetic must not depend on int width. A high-bit hash is the
// discriminator: int(uint32(0x80000001)) is positive on 64-bit but negative
// (and was then negated) on 32-bit GOARCH.
func TestSaramaHasherIntWidth(t *testing.T) {
	t.Parallel()
	h := SaramaHasher(func([]byte) uint32 { return 0x80000001 })
	// 0x80000001 = 2147483649; 2147483649 % 10 = 9. The pre-fix 32-bit
	// path computed -(int32(0x80000001)) % 10 = 2147483647 % 10 = 7.
	if got := h(nil, 10); got != 9 {
		t.Errorf("SaramaHasher high-bit hash partition = %d, want 9 (unsigned semantics)", got)
	}
	// A low-bit hash is identical under both interpretations.
	h = SaramaHasher(func([]byte) uint32 { return 12345 })
	if got := h(nil, 10); got != 5 {
		t.Errorf("SaramaHasher low-bit hash partition = %d, want 5", got)
	}
}
