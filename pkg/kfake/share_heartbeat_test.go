package kfake

import (
	"context"
	"errors"
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

// DeleteGroups deletes share groups too: Kafka has no share-specific
// delete API. A share group with a member answers NON_EMPTY_GROUP; an
// empty one is dropped and no longer shows up in ListGroups.
func TestShareGroupDeleteGroups(t *testing.T) {
	t.Parallel()
	const (
		topic = "t"
		group = "g"
	)
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c)
	ctx := context.Background()

	heartbeat := func(epoch int32) {
		t.Helper()
		req := kmsg.NewPtrShareGroupHeartbeatRequest()
		req.GroupID = group
		req.MemberID = "11111111-2222-3333-4444-555555555555"
		req.MemberEpoch = epoch
		if epoch == 0 {
			req.SubscribedTopicNames = []string{topic}
		}
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		if resp.ErrorCode != 0 {
			t.Fatalf("heartbeat at epoch %d: %v", epoch, kerr.ErrorForCode(resp.ErrorCode))
		}
	}
	deleteGroup := func() error {
		t.Helper()
		req := kmsg.NewPtrDeleteGroupsRequest()
		req.Groups = []string{group}
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Groups) != 1 || resp.Groups[0].Group != group {
			t.Fatalf("unexpected DeleteGroups response groups: %+v", resp.Groups)
		}
		return kerr.ErrorForCode(resp.Groups[0].ErrorCode)
	}
	listed := func() bool {
		t.Helper()
		resp, err := kmsg.NewPtrListGroupsRequest().RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		for _, g := range resp.Groups {
			if g.Group == group {
				return true
			}
		}
		return false
	}

	// Give the group partition state so it survives its member leaving:
	// a share group with no members and no partition state is dropped
	// as soon as the last member leaves.
	alter := kmsg.NewPtrAlterShareGroupOffsetsRequest()
	alter.GroupID = group
	at := kmsg.NewAlterShareGroupOffsetsRequestTopic()
	at.Topic = topic
	ap := kmsg.NewAlterShareGroupOffsetsRequestTopicPartition()
	ap.Partition = 0
	at.Partitions = append(at.Partitions, ap)
	alter.Topics = append(alter.Topics, at)
	if resp, err := alter.RequestWith(ctx, cl); err != nil {
		t.Fatal(err)
	} else if resp.ErrorCode != 0 {
		t.Fatalf("alter share group offsets: %v", kerr.ErrorForCode(resp.ErrorCode))
	}

	heartbeat(0) // join
	if err := deleteGroup(); !errors.Is(err, kerr.NonEmptyGroup) {
		t.Fatalf("deleting a share group with a member: got %v, want NON_EMPTY_GROUP", err)
	}
	if !listed() {
		t.Fatal("share group vanished after a refused delete")
	}

	heartbeat(-1) // leave
	if !listed() {
		t.Fatal("empty share group with partition state is not listed")
	}
	if err := deleteGroup(); err != nil {
		t.Fatalf("deleting an empty share group: %v", err)
	}
	if listed() {
		t.Fatal("share group still listed after delete")
	}
	if err := deleteGroup(); !errors.Is(err, kerr.GroupIDNotFound) {
		t.Fatalf("deleting a deleted share group: got %v, want GROUP_ID_NOT_FOUND", err)
	}
}
