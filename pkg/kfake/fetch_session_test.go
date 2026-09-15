package kfake

import (
	"context"
	"testing"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func fetchPartitionErr(t *testing.T, resp *kmsg.FetchResponse, id [16]byte) error {
	t.Helper()
	if resp.ErrorCode != 0 {
		t.Fatalf("fetch failed: %v", kerr.ErrorForCode(resp.ErrorCode))
	}
	for _, rt := range resp.Topics {
		if rt.TopicID != id {
			continue
		}
		if len(rt.Partitions) != 1 {
			t.Fatalf("topic %x has %d partitions in the response, want 1", id, len(rt.Partitions))
		}
		return kerr.ErrorForCode(rt.Partitions[0].ErrorCode)
	}
	t.Fatalf("topic %x is not in the response: %+v", id, resp.Topics)
	return nil
}

// A partition whose topic ID does not resolve stays in the session under its
// ID: every fetch of the session answers it UNKNOWN_TOPIC_ID, as a real
// broker does, until the client forgets it. If the topic is created later,
// the entry resolves and is served.
func TestFetchSessionUnknownID(t *testing.T) {
	t.Parallel()
	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c)
	ctx := context.Background()

	known := c.TopicInfo(topic).TopicID
	unknown := known
	unknown[0]++

	req := fetchByID(0, 0, 100, known, 0)
	ft := kmsg.NewFetchRequestTopic()
	ft.TopicID = unknown
	fp := kmsg.NewFetchRequestTopicPartition()
	fp.PartitionMaxBytes = 1 << 20
	fp.CurrentLeaderEpoch = -1
	ft.Partitions = append(ft.Partitions, fp)
	req.Topics = append(req.Topics, ft)
	full, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if full.SessionID <= 0 {
		t.Fatalf("session not established: err %v", kerr.ErrorForCode(full.ErrorCode))
	}
	if err := fetchPartitionErr(t, full, unknown); err != kerr.UnknownTopicID {
		t.Fatalf("full fetch of the unknown ID got %v, want UNKNOWN_TOPIC_ID", err)
	}

	// An incremental fetch that leaves both entries to the session
	// answers the unknown one again.
	incr, err := fetchByID(full.SessionID, 1, 100, known).RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if err := fetchPartitionErr(t, incr, unknown); err != kerr.UnknownTopicID {
		t.Fatalf("incremental fetch got %v for the unknown ID, want UNKNOWN_TOPIC_ID", err)
	}

	// Forgetting the partition removes it.
	forget := fetchByID(full.SessionID, 2, 100, known)
	fft := kmsg.NewFetchRequestForgottenTopic()
	fft.TopicID = unknown
	fft.Partitions = []int32{0}
	forget.ForgottenTopics = append(forget.ForgottenTopics, fft)
	forgot, err := forget.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if forgot.ErrorCode != 0 {
		t.Fatalf("fetch failed: %v", kerr.ErrorForCode(forgot.ErrorCode))
	}
	for _, rt := range forgot.Topics {
		if rt.TopicID == unknown {
			t.Fatalf("the forgotten unknown ID is still answered: %+v", rt)
		}
	}
}

// A session entry keeps the name it was added under. Once the topic is
// deleted, the entry reaches the log by name and is answered
// UNKNOWN_TOPIC_OR_PARTITION, whether the session or a re-send by ID
// addresses it; only a full fetch, which addresses the topic by ID alone, is
// answered UNKNOWN_TOPIC_ID.
func TestFetchSessionDeletedTopic(t *testing.T) {
	t.Parallel()
	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c)
	ctx := context.Background()

	id := c.TopicInfo(topic).TopicID
	first, err := fetchByID(0, 0, 100, id, 0).RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if first.SessionID <= 0 {
		t.Fatalf("session not established: err %v", kerr.ErrorForCode(first.ErrorCode))
	}

	if err := c.DeleteTopic(topic); err != nil {
		t.Fatalf("delete topic: %v", err)
	}

	incr, err := fetchByID(first.SessionID, 1, 100, id).RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if err := fetchPartitionErr(t, incr, id); err != kerr.UnknownTopicOrPartition {
		t.Fatalf("incremental fetch of the deleted topic got %v, want UNKNOWN_TOPIC_OR_PARTITION", err)
	}
	resend, err := fetchByID(first.SessionID, 2, 100, id, 0).RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if err := fetchPartitionErr(t, resend, id); err != kerr.UnknownTopicOrPartition {
		t.Fatalf("re-sending the deleted topic's ID got %v, want UNKNOWN_TOPIC_OR_PARTITION", err)
	}

	full, err := fetchByID(0, 0, 100, id, 0).RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if err := fetchPartitionErr(t, full, id); err != kerr.UnknownTopicID {
		t.Fatalf("full fetch of the deleted topic's ID got %v, want UNKNOWN_TOPIC_ID", err)
	}
}

// Re-sending a recreated topic's old ID in an incremental fetch matches the
// session's named entry, as on a real broker, which keys session entries by
// ID and keeps the name: the answer stays INCONSISTENT_TOPIC_ID rather than
// becoming UNKNOWN_TOPIC_ID.
func TestFetchSessionRecreatedTopicResend(t *testing.T) {
	t.Parallel()
	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c)
	ctx := context.Background()

	oldID := c.TopicInfo(topic).TopicID
	first, err := fetchByID(0, 0, 100, oldID, 0).RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if first.SessionID <= 0 {
		t.Fatalf("session not established: err %v", kerr.ErrorForCode(first.ErrorCode))
	}
	recreateTopic(t, c, topic)

	resend, err := fetchByID(first.SessionID, 1, 100, oldID, 0).RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if err := fetchPartitionErr(t, resend, oldID); err != kerr.InconsistentTopicID {
		t.Fatalf("re-sending the old ID got %v, want INCONSISTENT_TOPIC_ID", err)
	}
	incr, err := fetchByID(first.SessionID, 2, 100, oldID).RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if err := fetchPartitionErr(t, incr, oldID); err != kerr.InconsistentTopicID {
		t.Fatalf("the session's entry got %v after the re-send, want INCONSISTENT_TOPIC_ID", err)
	}
}
