package kadm

import "testing"

// LessBySize previously fell through to Less when the receiver was larger,
// so a larger partition on a lower broker was "less" than a smaller partition
// on a higher broker in both directions, and the BySize sorts returned
// whatever order the map iteration produced.
func TestLessBySize(t *testing.T) {
	large := DescribedLogDirPartition{Broker: 1, Dir: "/a", Topic: "t", Partition: 0, Size: 100}
	small := DescribedLogDirPartition{Broker: 2, Dir: "/a", Topic: "t", Partition: 0, Size: 10}
	if !small.LessBySize(large) {
		t.Error("smaller partition is not less by size than the larger")
	}
	if large.LessBySize(small) {
		t.Error("larger partition is less by size than the smaller")
	}

	ds := DescribedLogDirTopics{"t": {0: large, 1: small}}
	for range 50 { // map order is random; the sort was only sometimes wrong
		if got := ds.SortedBySize(); got[0].Size != 10 || got[1].Size != 100 {
			t.Fatalf("got sizes %d, %d; want 10, 100", got[0].Size, got[1].Size)
		}
	}
}
