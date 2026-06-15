package kgo

import (
	"reflect"
	"testing"
)

func TestRackEligible(t *testing.T) {
	// Partitions 0,2 lead on broker 10 (rack "a"); partition 1 leads on
	// broker 11 (rack "b"); partition 3 leads on broker 12 (no rack).
	p0 := &topicPartition{topicPartitionData: topicPartitionData{leader: 10}}
	p1 := &topicPartition{topicPartitionData: topicPartitionData{leader: 11}}
	p2 := &topicPartition{topicPartitionData: topicPartitionData{leader: 10}}
	p3 := &topicPartition{topicPartitionData: topicPartitionData{leader: 12}}
	mapping := []*topicPartition{p0, p1, p2, p3}
	racks := map[int32]string{
		10: "a",
		11: "b",
		// 12 intentionally absent: leader with no known rack.
	}

	t.Run("same-rack subset", func(t *testing.T) {
		got := rackEligible(nil, mapping, racks, "a")
		want := []*topicPartition{p0, p2}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("rack a: got %v, want %v", got, want)
		}
	})

	t.Run("leader without rack excluded", func(t *testing.T) {
		// Only p3 leads on the rackless broker; asking for any rack must
		// never include it.
		got := rackEligible(nil, mapping, racks, "z")
		if len(got) != 0 {
			t.Errorf("rack z: got %v, want empty", got)
		}
		got = rackEligible(nil, mapping, racks, "b")
		if want := []*topicPartition{p1}; !reflect.DeepEqual(got, want) {
			t.Errorf("rack b: got %v, want %v", got, want)
		}
	})

	t.Run("empty when none same-rack", func(t *testing.T) {
		got := rackEligible(nil, mapping, racks, "nope")
		if len(got) != 0 {
			t.Errorf("got %v, want empty", got)
		}
	})

	t.Run("dst reuse", func(t *testing.T) {
		// Seed dst with capacity and stale contents; rackEligible must
		// reuse the backing array (dst[:0]) and not leak old entries.
		dst := make([]*topicPartition, 0, 8)
		dst = append(dst, p3, p3, p3)
		got := rackEligible(dst, mapping, racks, "a")
		want := []*topicPartition{p0, p2}
		if !reflect.DeepEqual(got, want) {
			t.Errorf("got %v, want %v", got, want)
		}
		if cap(got) != cap(dst) || &got[:1][0] != &dst[:1][0] {
			t.Errorf("expected dst backing array reuse")
		}
	})
}

func TestRackAwarePartitioningOptPlumbing(t *testing.T) {
	var cfg cfg

	// Rack now returns a general Opt (relaxed from ConsumerOpt) so that a
	// producer-only client may set it. This must compile as an Opt and apply
	// the rack.
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
