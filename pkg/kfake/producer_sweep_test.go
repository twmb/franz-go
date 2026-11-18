package kfake

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Regression tests for producer.go. Each TestAudit* below fails before its
// corresponding kgo fix.

// EnsureProduceConnectionIsOpen filtered broker IDs < -1 into a kept list but
// then dialed the unfiltered input slice, which the in-place filter had also
// alias-corrupted: (realID, -5) errored with "unknown broker" for an ID the
// filter intended to ignore, while (-5, realID) silently dialed the real
// broker twice. Both orders must succeed, opening only the real broker.
func TestAuditEnsureProduceConnectionIgnoresNegativeIDs(t *testing.T) {
	t.Parallel()
	c := newCluster(t, NumBrokers(1))
	cl := newPlainClient(t, c)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Discover the real broker ID.
	req := kmsg.NewPtrMetadataRequest()
	req.Topics = []kmsg.MetadataRequestTopic{}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("metadata request failed: %v", err)
	}
	if len(resp.Brokers) != 1 {
		t.Fatalf("expected 1 broker in metadata, got %d", len(resp.Brokers))
	}
	id := resp.Brokers[0].NodeID

	for _, brokers := range [][]int32{
		{id, -5},
		{-5, id},
		{-5}, // everything filtered: nothing to open, no error
	} {
		if err := cl.EnsureProduceConnectionIsOpen(ctx, brokers...); err != nil {
			t.Errorf("EnsureProduceConnectionIsOpen(%v) = %v, want nil (IDs < -1 are ignored)", brokers, err)
		}
	}
}

type partitionedHook func(*kgo.Record, int32)

func (h partitionedHook) OnProduceRecordPartitioned(r *kgo.Record, leader int32) { h(r, leader) }

// storePartitionsUpdate buffers a new topic's records before it stores the
// topic's partitions. With ManualFlushing and no linger, buffering a record
// starts no drain. A Flush that started after the buffering and before the
// store did not drain the topic's partitions, so Flush returned only when its
// context ended.
func TestAuditFlushDrainsTopicBeingStored(t *testing.T) {
	t.Parallel()
	const topic = "flush-being-stored"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))

	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	flushing := &msgLogger{msg: "flushing", seen: make(chan struct{}, 1)}
	buffered := make(chan struct{})
	cl := newPlainClient(t, c,
		kgo.ManualFlushing(),
		kgo.ProducerLinger(0),
		kgo.WithLogger(flushing),
		// storePartitionsUpdate runs the hook after buffering the
		// record and before the store. The hook returns once Flush
		// starts.
		kgo.WithHooks(partitionedHook(func(*kgo.Record, int32) {
			close(buffered)
			select {
			case <-flushing.seen:
			case <-ctx.Done():
			}
		})),
	)

	produced := produceCh(ctx, cl, topic, "v")
	select {
	case <-buffered:
	case <-ctx.Done():
		t.Fatal("the record was never partitioned")
	}
	if err := cl.Flush(ctx); err != nil {
		t.Fatalf("Flush did not drain the new topic: %v", err)
	}
	if err := <-produced; err != nil {
		t.Fatal(err)
	}
}
