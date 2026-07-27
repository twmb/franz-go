package kgo

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"reflect"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

func benchTopicID(n int) [16]byte {
	var id [16]byte
	binary.BigEndian.PutUint64(id[8:], uint64(n)+1) // +1: never the zero ID
	return id
}

func benchLatestTopics(n int) map[string]*metadataTopic {
	latest := make(map[string]*metadataTopic, n)
	for i := range n {
		topic := fmt.Sprintf("benchmark_topic_%06d", i)
		latest[topic] = &metadataTopic{
			topic: topic,
			id:    benchTopicID(i),
		}
	}
	return latest
}

func benchPartitions(n int) []kmsg.MetadataResponseTopicPartition {
	ps := make([]kmsg.MetadataResponseTopicPartition, n)
	for i := range ps {
		ps[i].Partition = int32(i)
		ps[i].Leader = int32(i % 3)
		ps[i].LeaderEpoch = 1
		ps[i].Replicas = []int32{int32(i % 3)}
		ps[i].ISR = []int32{int32(i % 3)}
	}
	return ps
}

func TestMergeTopicIDs(t *testing.T) {
	t.Parallel()

	cl := new(Client)
	ta, tb := benchTopicID(0), benchTopicID(1)

	// An ID-less topic (an old broker, or a topic that failed to load) is
	// never mapped.
	cl.mergeTopicIDs(map[string]*metadataTopic{
		"a": {topic: "a", id: ta},
		"b": {topic: "b", id: tb},
		"c": {topic: "c"},
	})
	want := map[[16]byte]string{ta: "a", tb: "b"}
	if got := cl.id2tMap(); !reflect.DeepEqual(got, want) {
		t.Errorf("after first merge: got %v != want %v", got, want)
	}

	// Re-merging the same response must not rebuild the map. The rebuild
	// always stores a fresh map, so an unchanged stored map proves we
	// skipped it.
	same := map[string]*metadataTopic{
		"b": {topic: "b", id: tb},
		"a": {topic: "a", id: ta},
		"c": {topic: "c"},
	}
	prior := reflect.ValueOf(cl.id2tMap()).Pointer()
	cl.mergeTopicIDs(same)
	if got := reflect.ValueOf(cl.id2tMap()).Pointer(); got != prior {
		t.Error("merging an unchanged response replaced the id2t map")
	}
	if got := cl.id2tMap(); !reflect.DeepEqual(got, want) {
		t.Errorf("after unchanged merge: got %v != want %v", got, want)
	}

	// A recreated topic (same name, new ID) keeps the old mapping until
	// the user purges; a new name is added.
	tc, trecreate := benchTopicID(2), benchTopicID(3)
	cl.mergeTopicIDs(map[string]*metadataTopic{
		"a": {topic: "a", id: trecreate},
		"c": {topic: "c", id: tc},
	})
	want = map[[16]byte]string{ta: "a", tb: "b", tc: "c"}
	if got := cl.id2tMap(); !reflect.DeepEqual(got, want) {
		t.Errorf("after recreate merge: got %v != want %v", got, want)
	}
}

func TestEnsurePartitionsOrdered(t *testing.T) {
	t.Parallel()

	t.Run("ordered", func(t *testing.T) {
		t.Parallel()
		ps := benchPartitions(64)
		if err := ensurePartitionsOrdered(ps); err != nil {
			t.Fatal(err)
		}
		if i := firstUnordered(ps); i >= 0 {
			t.Errorf("partition at index %d is %d", i, ps[i].Partition)
		}
	})

	t.Run("shuffled", func(t *testing.T) {
		t.Parallel()
		ps := benchPartitions(64)
		rand.Shuffle(len(ps), func(i, j int) { ps[i], ps[j] = ps[j], ps[i] })
		if err := ensurePartitionsOrdered(ps); err != nil {
			t.Fatal(err)
		}
		for i := range ps {
			if ps[i].Partition != int32(i) {
				t.Fatalf("partition at index %d is %d", i, ps[i].Partition)
			}
			// Sorting must move the whole partition, not just the
			// number.
			if want := int32(i % 3); ps[i].Leader != want {
				t.Fatalf("partition %d has leader %d != %d", i, ps[i].Leader, want)
			}
		}
	})

	t.Run("empty", func(t *testing.T) {
		t.Parallel()
		if err := ensurePartitionsOrdered(nil); err != nil {
			t.Error(err)
		}
	})

	for _, test := range []struct {
		name string
		ps   []int32
	}{
		{"missing", []int32{0, 1, 3}},
		{"duplicate", []int32{0, 1, 1}},
		{"not_from_zero", []int32{1, 2, 3}},
		{"negative", []int32{-1, 0, 1}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			ps := make([]kmsg.MetadataResponseTopicPartition, len(test.ps))
			for i, p := range test.ps {
				ps[i].Partition = p
			}
			if err := ensurePartitionsOrdered(ps); err == nil {
				t.Errorf("got nil error for partitions %v", test.ps)
			}
		})
	}
}

// BenchmarkMergeTopicIDs measures merging a metadata response's topic IDs into
// the client's ID to name map. Every metadata refresh merges every topic in the
// response; "unchanged" is the steady state where the broker returns the same
// IDs it returned last time.
func BenchmarkMergeTopicIDs(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		latest := benchLatestTopics(n)

		b.Run(fmt.Sprintf("unchanged/%d", n), func(b *testing.B) {
			cl := new(Client)
			cl.mergeTopicIDs(latest)
			b.ReportAllocs()
			for b.Loop() {
				cl.mergeTopicIDs(latest)
			}
		})

		// One recreated topic (same name, new ID) is enough to force
		// the full clone-and-merge on every iteration.
		recreated := make(map[string]*metadataTopic, n)
		for topic, mt := range latest {
			recreated[topic] = mt
		}
		for topic := range recreated {
			recreated[topic] = &metadataTopic{topic: topic, id: benchTopicID(n)}
			break
		}
		b.Run(fmt.Sprintf("changed/%d", n), func(b *testing.B) {
			cl := new(Client)
			cl.mergeTopicIDs(latest)
			b.ReportAllocs()
			for b.Loop() {
				cl.mergeTopicIDs(recreated)
			}
		})
	}
}

// BenchmarkEnsurePartitionsOrdered measures the per-topic partition ordering
// check that runs on every metadata refresh. "ordered" is what a broker
// actually returns; "shuffled" forces the sort.
func BenchmarkEnsurePartitionsOrdered(b *testing.B) {
	for _, n := range []int{100, 1000, 10000} {
		b.Run(fmt.Sprintf("ordered/%d", n), func(b *testing.B) {
			ps := benchPartitions(n)
			b.ReportAllocs()
			for b.Loop() {
				if err := ensurePartitionsOrdered(ps); err != nil {
					b.Fatal(err)
				}
			}
		})

		b.Run(fmt.Sprintf("shuffled/%d", n), func(b *testing.B) {
			shuffled := benchPartitions(n)
			rand.Shuffle(n, func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
			ps := make([]kmsg.MetadataResponseTopicPartition, n)
			b.ReportAllocs()
			for b.Loop() {
				b.StopTimer()
				copy(ps, shuffled)
				b.StartTimer()
				if err := ensurePartitionsOrdered(ps); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkNewPartitions measures building the topicPartitionsData for one
// topic, which a metadata refresh does for every topic the client knows about
// -- including topics that did not change.
func BenchmarkNewPartitions(b *testing.B) {
	cl, err := NewClient(SeedBrokers("127.0.0.1:1")) // metadata never loads; we drive the path directly
	if err != nil {
		b.Fatal(err)
	}
	defer cl.Close()

	for _, n := range []int{100, 1000, 10000} {
		mt := &metadataTopic{
			topic:      "benchmark_topic",
			id:         benchTopicID(0),
			partitions: make([]metadataPartition, n),
		}
		for i := range mt.partitions {
			mt.partitions[i] = metadataPartition{
				topic:     mt.topic,
				topicID:   mt.id,
				partition: int32(i),
				leader:    int32(i % 3),
			}
		}
		for _, kind := range []struct {
			name string
			kind partitionKind
		}{
			{"produce", partitionKindProduce},
			{"consume", partitionKindConsume},
		} {
			b.Run(fmt.Sprintf("%s/%d", kind.name, n), func(b *testing.B) {
				b.ReportAllocs()
				for b.Loop() {
					mt.newPartitions(cl, kind.kind)
				}
			})
		}
	}
}
