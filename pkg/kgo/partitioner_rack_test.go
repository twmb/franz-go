package kgo

import (
	"reflect"
	"testing"
)

func TestRackEligiblePartitions(t *testing.T) {
	// Partitions 0,2 lead on broker 10 (rack "a"); partition 1 leads on
	// broker 11 (rack "b"); partition 3 leads on broker 12 (no rack).
	p0 := &topicPartition{topicPartitionData: topicPartitionData{leader: 10}}
	p1 := &topicPartition{topicPartitionData: topicPartitionData{leader: 11}}
	p2 := &topicPartition{topicPartitionData: topicPartitionData{leader: 10}}
	p3 := &topicPartition{topicPartitionData: topicPartitionData{leader: 12}}
	writable := []*topicPartition{p0, p1, p2, p3}
	racks := map[int32]string{
		10: "a",
		11: "b",
		// 12 intentionally absent: leader with no known rack.
	}

	t.Run("same-rack subset of writable", func(t *testing.T) {
		got := rackEligiblePartitions(writable, racks, "a")
		want := []*topicPartition{p0, p2}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("rack a: got %v, want %v", got, want)
		}
	})

	t.Run("leader without rack excluded", func(t *testing.T) {
		// Only p3 leads on the rackless broker; asking for any rack must
		// never include it.
		if got := rackEligiblePartitions(writable, racks, "z"); len(got) != 0 {
			t.Errorf("rack z: got %v, want empty", got)
		}
		got := rackEligiblePartitions(writable, racks, "b")
		if want := []*topicPartition{p1}; !reflect.DeepEqual(got, want) {
			t.Errorf("rack b: got %v, want %v", got, want)
		}
	})

	t.Run("empty when none same-rack", func(t *testing.T) {
		if got := rackEligiblePartitions(writable, racks, "nope"); len(got) != 0 {
			t.Errorf("got %v, want empty", got)
		}
	})

	t.Run("nil when no writable partitions", func(t *testing.T) {
		// Only writable (available) partitions are rack-filtered. When
		// none are writable - every partition has a retriable load error -
		// this returns nil and doPartition uses the full partition set
		// unfiltered, matching KIP-1123 rather than pinning to a stale
		// same-rack leader. See the Kafka producer BuiltInPartitioner.
		if got := rackEligiblePartitions(nil, racks, "a"); got != nil {
			t.Errorf("got %v, want nil", got)
		}
	})
}

func TestRackAwarePartitioningOptPlumbing(t *testing.T) {
	var cfg cfg

	// Rack remains a ConsumerOpt but is usable as a plain Opt, so a
	// producer-only client may pass it to NewClient. This asserts that
	// dual use compiles and applies the rack.
	var rackOpt Opt = Rack("r1")
	rackOpt.apply(&cfg)
	if cfg.rack != "r1" {
		t.Errorf("Rack: got rack %q, want %q", cfg.rack, "r1")
	}

	if cfg.rackAwarePartitioning {
		t.Errorf("rackAwarePartitioning should default to false")
	}
	RackAwarePartitioning().apply(&cfg)
	if !cfg.rackAwarePartitioning {
		t.Errorf("RackAwarePartitioning() did not set the field")
	}
}
