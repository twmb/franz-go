package kfake

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// The share group assignment goes to a full heartbeat (one carrying
// SubscribedTopicNames) or when this heartbeat changed it, as on a real
// broker. A keepalive does not carry it again.
func TestShareHeartbeatAssignmentDelivery(t *testing.T) {
	t.Parallel()
	const (
		topic = "t"
		group = "g"
	)
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c)
	ctx := context.Background()

	heartbeat := func(epoch int32, subscribe bool) *kmsg.ShareGroupHeartbeatResponse {
		t.Helper()
		req := kmsg.NewPtrShareGroupHeartbeatRequest()
		req.GroupID = group
		req.MemberID = "11111111-2222-3333-4444-555555555555"
		req.MemberEpoch = epoch
		if subscribe {
			req.SubscribedTopicNames = []string{topic}
		}
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		if resp.ErrorCode != 0 {
			t.Fatalf("heartbeat at epoch %d: %v", epoch, kerr.ErrorForCode(resp.ErrorCode))
		}
		return resp
	}

	join := heartbeat(0, true)
	if join.Assignment == nil || len(join.Assignment.TopicPartitions) != 1 {
		t.Fatalf("join returned no assignment: %+v", join.Assignment)
	}
	defer heartbeat(-1, false) // leave
	if keepalive := heartbeat(join.MemberEpoch, false); keepalive.Assignment != nil {
		t.Fatalf("a keepalive carried the assignment again: %+v", keepalive.Assignment)
	}
	if full := heartbeat(join.MemberEpoch, true); full.Assignment == nil || len(full.Assignment.TopicPartitions) != 1 {
		t.Fatalf("a full heartbeat did not carry the assignment: %+v", full.Assignment)
	}
}
