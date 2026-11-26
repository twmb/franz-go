package kgo

import (
	"fmt"
	"maps"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// A batch can fail after we build its produce request: failAllRecords takes
// its records, or bumpRepeatedLoadErr marks it failing. AppendTo leaves out
// the batch and any topic with no batch left, and rewrites each count that
// went down. A compact count of 127 or more takes two bytes and a count of
// 126 or less takes one byte, so the 200 to 100 and 130 to 2 rows check that
// AppendTo shifts the rest of the request left. Round-trip the hand-rolled
// encoding through kmsg's generated decoder to pin the wire shape.
func TestProduceRequestFailedBatches(t *testing.T) {
	t.Parallel()

	type counts struct{ ok, failed int } // partitions per topic
	manyTopics := map[string]counts{"a": {1, 0}, "b": {2, 1}}
	for i := range 128 {
		manyTopics[fmt.Sprintf("failed%d", i)] = counts{0, 1}
	}

	for _, test := range []struct {
		name   string
		topics map[string]counts
	}{
		{"healthy and failed", map[string]counts{"a": {2, 2}, "b": {0, 3}}},
		{"partition count 200 to 100", map[string]counts{"a": {100, 100}}},
		{"topic count 130 to 2", manyTopics},
		{"all failed", map[string]counts{"a": {0, 2}, "b": {0, 1}}},
	} {
		for _, version := range []int16{8, 9, 13} { // last non-flexible, first flexible, first with topic IDs
			t.Run(fmt.Sprintf("%s/v%d", test.name, version), func(t *testing.T) {
				req := &produceRequest{version: version, acks: -1, timeout: 1000, producerID: -1}
				want := make(map[string][]int32)
				var id byte
				for topic, c := range test.topics {
					id++
					for p := range int32(c.ok + c.failed) {
						b := &recBatch{wireLength: recordBatchOverhead}
						pr := promisedRec{Record: &Record{Value: []byte("v"), Timestamp: time.Now()}}
						b.appendRecord(pr, b.calculateRecordNumbers(pr.Record))
						switch {
						case p < int32(c.ok):
							want[topic] = append(want[topic], p)
						case p%2 == 0:
							b.records = nil // failAllRecords
						default:
							b.isFailingFromLoadErr = true // bumpRepeatedLoadErr
						}
						req.batches.addBatch(topic, [16]byte{id}, p, 0, b)
					}
				}

				decode := func() map[string]map[int32][]byte {
					var d kmsg.ProduceRequest
					d.SetVersion(version)
					if err := d.ReadFrom(req.AppendTo(nil)); err != nil {
						t.Fatalf("hand-rolled produce request does not decode: %v", err)
					}
					got := make(map[string]map[int32][]byte)
					for _, dt := range d.Topics {
						topic := dt.Topic
						if version >= 13 {
							topic = req.batches.id2t[dt.TopicID]
						}
						got[topic] = make(map[int32][]byte)
						for _, dp := range dt.Partitions {
							if dp.Records == nil {
								t.Errorf("topic %s partition %d: records are null", topic, dp.Partition)
							}
							got[topic][dp.Partition] = dp.Records
						}
					}
					return got
				}

				got := decode()
				if parts := sortedPartitions(got); !reflect.DeepEqual(parts, want) {
					t.Errorf("wrote partitions %v, want %v", parts, want)
				}
				if parts := sortedPartitions(req.batches.bs); !reflect.DeepEqual(parts, want) {
					t.Errorf("request batches are %v, want %v", parts, want)
				}
				if again := decode(); !reflect.DeepEqual(again, got) {
					t.Errorf("a second AppendTo wrote %v, the first wrote %v", again, got)
				}
			})
		}
	}
}

// sortedPartitions returns each topic's partitions, sorted.
func sortedPartitions[V any](m map[string]map[int32]V) map[string][]int32 {
	parts := make(map[string][]int32, len(m))
	for topic, ps := range m {
		parts[topic] = slices.Sorted(maps.Keys(ps))
	}
	return parts
}
