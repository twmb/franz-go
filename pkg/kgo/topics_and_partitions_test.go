package kgo

import (
	"testing"
)

func TestDelTopicsWithCoexistingPartitionPauses(t *testing.T) {
	// delTopics must clear all=true even when partition-level pauses exist for
	// the same topic. Previously the modified pausedPartitions struct was never
	// written back, leaving all=true in the map when len(pps.m) > 0.
	m := make(pausedTopics)

	// Add a topic-level pause and a partition-level pause for the same topic.
	m.addTopics("topic-a")
	m.addPartitions(map[string][]int32{"topic-a": {0}})

	if !m["topic-a"].all {
		t.Fatal("expected topic-a to be topic-paused after addTopics")
	}

	// Removing the topic-level pause must clear all=true even though partition
	// pauses still exist.
	m.delTopics("topic-a")

	if pps, ok := m["topic-a"]; ok && pps.all {
		t.Fatal("delTopics did not clear all=true when partition-level pauses coexist")
	}

	// The partition-level pause must still be present.
	if _, ok := m["topic-a"].m[0]; !ok {
		t.Fatal("delTopics unexpectedly removed the partition-level pause")
	}
}

// TestKip951MoveAfterPurgeReAddNoPanic verifies that applying a staged
// KIP-951 leader-move hint is safe when the topic was purged and re-added
// between the produce/fetch response that staged the hint and the
// asynchronous doMove that applies it.
//
// A re-added topic starts with zero partitions until metadata loads, so
// pre-fix, modifyP's unchecked d.partitions[partition] panicked with an
// index out of range. And once metadata loads the re-added topic, the
// object at that index belongs to the NEW topic incarnation: migrating it
// based on the old incarnation's hint is wrong (and dereferenced the new
// stub recBuf's nil sink). modifyP now bounds-checks the partition and
// skips when the partition object does not own the staged recBuf/cursor.
func TestKip951MoveAfterPurgeReAddNoPanic(t *testing.T) {
	t.Parallel()

	newCl := func() *Client {
		cl := &Client{
			cfg:             cfg{logger: new(nopLogger)},
			sinksAndSources: make(map[int32]sinkAndSource),
		}
		cl.producer.topics = newTopicsPartitions()
		cl.sinksAndSources[1] = sinkAndSource{}
		return cl
	}

	// Case 1: the re-added topic has no partitions yet (metadata has not
	// loaded). Pre-fix: index out of range panic.
	{
		cl := newCl()
		cl.producer.topics.storeTopics([]string{"t"})

		staged := &recBuf{cl: cl, topic: "t", partition: 5}
		k := &kip951move{
			recBufs: map[*recBuf]topicPartitionData{
				staged: {leader: 1, leaderEpoch: 3},
			},
			brokers: []BrokerMetadata{{NodeID: 1}},
		}
		k.doMove(cl) // pre-fix: panics
	}

	// Case 2: metadata loaded the re-added topic, so the partition is in
	// range but is a different object than the one that staged the hint.
	// The move must be skipped: pre-fix it migrated the stranger (and
	// panicked dereferencing its nil sink).
	{
		cl := newCl()
		cl.producer.topics.storeTopics([]string{"t"})

		stranger := &recBuf{cl: cl, topic: "t", partition: 0}
		tp := &topicPartition{records: stranger}
		data := &topicPartitionsData{
			partitions:         []*topicPartition{tp},
			writablePartitions: []*topicPartition{tp},
		}
		cl.producer.topics.load()["t"].v.Store(data)

		staged := &recBuf{cl: cl, topic: "t", partition: 0}
		k := &kip951move{
			recBufs: map[*recBuf]topicPartitionData{
				staged: {leader: 1, leaderEpoch: 3},
			},
			brokers: []BrokerMetadata{{NodeID: 1}},
		}
		k.doMove(cl)

		after := cl.producer.topics.load()["t"].load()
		if after.partitions[0] != tp {
			t.Error("kip-951 move replaced a partition object it does not own")
		}
		if stranger.sink != nil || stranger.leader != 0 || stranger.leaderEpoch != 0 {
			t.Errorf("kip-951 move mutated a recBuf it does not own: sink=%v leader=%d epoch=%d",
				stranger.sink, stranger.leader, stranger.leaderEpoch)
		}
	}
}
