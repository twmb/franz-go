package kfake

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// Regression tests from the producer.go audit sweep (round 13). Each
// TestAudit* below fails before its corresponding kgo fix.

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
