package kgo

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// Regression test from the client.go Phase-2 audit re-sweep.

// writeTxnMarkersSharder.shard buckets each marker by a key holding every
// per-marker field that is NOT the topic/partition list, then rebuilds the
// marker from that key. CoordinatorEpoch - a v0+ field the broker uses to
// fence stale transaction coordinators (KafkaApis passes it to
// completeTransaction and embeds it into the EndTransactionMarker control
// record written to the partition log) - was omitted from both the bucket key
// and the rebuilt marker, so every sharded WriteTxnMarkers request went out
// with CoordinatorEpoch=0 regardless of the user's value. It is reachable via
// kadm.WriteTxnMarkers, which sets rm.CoordinatorEpoch and then RequestShards.
// This is the WriteTxnMarkers sibling of the round-10 AddPartitionsToTxn
// VerifyOnly fix: a txn sharder silently losing a fencing field.
func TestAuditWriteTxnMarkersPreservesCoordinatorEpoch(t *testing.T) {
	t.Parallel()
	cl := &Client{cfg: defaultCfg()}

	// Seed the metadata cache so the sharder maps foo-0 to a leader without
	// a network fetch (a fresh cache entry short-circuits resolveTopicMeta).
	meta := kmsg.NewPtrMetadataResponse()
	rt := kmsg.NewMetadataResponseTopic()
	rt.Topic = kmsg.StringPtr("foo")
	rp := kmsg.NewMetadataResponseTopicPartition()
	rp.Partition = 0
	rp.Leader = 1
	rt.Partitions = append(rt.Partitions, rp)
	meta.Topics = append(meta.Topics, rt)
	cl.storeCachedMeta(meta, false, nil)

	req := kmsg.NewPtrWriteTxnMarkersRequest()
	m := kmsg.NewWriteTxnMarkersRequestMarker()
	m.ProducerID = 1
	m.ProducerEpoch = 2
	m.Committed = true
	m.CoordinatorEpoch = 5
	mt := kmsg.NewWriteTxnMarkersRequestMarkerTopic()
	mt.Topic = "foo"
	mt.Partitions = []int32{0}
	m.Topics = append(m.Topics, mt)
	req.Markers = append(req.Markers, m)

	sharder := &writeTxnMarkersSharder{cl}
	issues, _, err := sharder.shard(context.Background(), req, nil)
	if err != nil {
		t.Fatalf("shard: %v", err)
	}

	var sawMarker bool
	for _, issue := range issues {
		wreq := issue.req.(*kmsg.WriteTxnMarkersRequest)
		for _, got := range wreq.Markers {
			sawMarker = true
			if got.CoordinatorEpoch != 5 {
				t.Errorf("sharded marker CoordinatorEpoch = %d, want 5 (the user value was dropped to 0)", got.CoordinatorEpoch)
			}
		}
	}
	if !sawMarker {
		t.Fatal("no markers in the sharded requests")
	}
}
