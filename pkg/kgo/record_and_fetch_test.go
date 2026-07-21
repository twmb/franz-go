package kgo

import "testing"

// TestEachTopicPreservesTopicID verifies that Fetches.EachTopic carries
// FetchTopic.TopicID through its grouping.
//
// A topic's partitions are led by different brokers, so the same topic is
// returned in separate Fetch entries (one per broker response). EachTopic
// groups those entries by topic name. Before the fix, the multi-fetch
// (len(fs) >= 2) grouping path rebuilt FetchTopic with a hard-coded zero
// TopicID, so any caller reading TopicID got zero whenever more than one
// broker replied -- nearly always in a real cluster, yet never in a
// single-broker test (which hits the len(fs) == 1 path that preserves the
// ID). The grouped TopicID must match the per-fetch TopicID.
func TestEachTopicPreservesTopicID(t *testing.T) {
	t.Parallel()

	fooID := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	barID := [16]byte{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}

	collect := func(fs Fetches) (ids map[string][16]byte, nparts map[string]int) {
		ids = make(map[string][16]byte)
		nparts = make(map[string]int)
		fs.EachTopic(func(ft FetchTopic) {
			ids[ft.Topic] = ft.TopicID
			nparts[ft.Topic] += len(ft.Partitions)
		})
		return
	}

	t.Run("spread across multiple fetches", func(t *testing.T) {
		t.Parallel()
		// "foo" is led on two brokers, so it arrives in two Fetch entries;
		// both carry its TopicID. "bar" is in a single Fetch entry. This is
		// the multi-fetch grouping path (len(fs) == 2).
		fs := Fetches{
			{Topics: []FetchTopic{
				{Topic: "foo", TopicID: fooID, Partitions: []FetchPartition{{Partition: 0}}},
				{Topic: "bar", TopicID: barID, Partitions: []FetchPartition{{Partition: 0}}},
			}},
			{Topics: []FetchTopic{
				{Topic: "foo", TopicID: fooID, Partitions: []FetchPartition{{Partition: 1}}},
			}},
		}
		ids, nparts := collect(fs)
		if ids["foo"] != fooID { // dropped to zero pre-fix
			t.Errorf("foo TopicID: got %v, want %v", ids["foo"], fooID)
		}
		if ids["bar"] != barID {
			t.Errorf("bar TopicID: got %v, want %v", ids["bar"], barID)
		}
		if nparts["foo"] != 2 { // both fetches' partitions grouped
			t.Errorf("foo partitions: got %d, want 2", nparts["foo"])
		}
	})

	t.Run("single fetch preserves id", func(t *testing.T) {
		t.Parallel()
		// The len(fs) == 1 fast path already preserved the ID; guard it
		// against regressions.
		fs := Fetches{
			{Topics: []FetchTopic{
				{Topic: "foo", TopicID: fooID, Partitions: []FetchPartition{{Partition: 0}}},
			}},
		}
		ids, _ := collect(fs)
		if ids["foo"] != fooID {
			t.Errorf("foo TopicID: got %v, want %v", ids["foo"], fooID)
		}
	})

	t.Run("no id stays zero", func(t *testing.T) {
		t.Parallel()
		// A pre-3.1 broker (or a share fetch) returns no TopicID; EachTopic
		// must not fabricate one.
		fs := Fetches{
			{Topics: []FetchTopic{
				{Topic: "foo", Partitions: []FetchPartition{{Partition: 0}}},
			}},
			{Topics: []FetchTopic{
				{Topic: "foo", Partitions: []FetchPartition{{Partition: 1}}},
			}},
		}
		ids, _ := collect(fs)
		if ids["foo"] != ([16]byte{}) {
			t.Errorf("foo TopicID: got %v, want zero", ids["foo"])
		}
	})
}

// TestNewRecordAttrs verifies NewRecordAttrs round-trips through the accessors.
func TestNewRecordAttrs(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name            string
		compressionType uint8
		timestampType   int8
		isTransactional bool
		isControl       bool
	}{
		{"zero", 0, 0, false, false},
		{"snappy create-time", 2, 0, false, false},
		{"zstd log-append-time", 4, 1, false, false},
		{"no-timestamp", 0, -1, false, false},
		{"transactional", 1, 0, true, false},
		{"control", 0, 0, false, true},
		{"all set", 3, 1, true, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := NewRecordAttrs(tc.compressionType, tc.timestampType, tc.isTransactional, tc.isControl)
			if got := a.CompressionType(); got != tc.compressionType {
				t.Errorf("CompressionType: got %d, want %d", got, tc.compressionType)
			}
			if got := a.TimestampType(); got != tc.timestampType {
				t.Errorf("TimestampType: got %d, want %d", got, tc.timestampType)
			}
			if got := a.IsTransactional(); got != tc.isTransactional {
				t.Errorf("IsTransactional: got %t, want %t", got, tc.isTransactional)
			}
			if got := a.IsControl(); got != tc.isControl {
				t.Errorf("IsControl: got %t, want %t", got, tc.isControl)
			}
		})
	}
}
