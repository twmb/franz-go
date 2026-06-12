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
