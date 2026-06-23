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
