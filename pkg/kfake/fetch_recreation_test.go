package kfake_test

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// fetchByID builds a topic-ID addressed fetch (the client negotiates v13+).
func fetchByID(sessionID, sessionEpoch, maxWait int32, id [16]byte, partitions ...int32) *kmsg.FetchRequest {
	req := kmsg.NewPtrFetchRequest()
	req.MaxWaitMillis = maxWait
	req.MinBytes = 1
	req.MaxBytes = 1 << 20
	req.SessionID = sessionID
	req.SessionEpoch = sessionEpoch
	if len(partitions) > 0 {
		ft := kmsg.NewFetchRequestTopic()
		ft.TopicID = id
		for _, p := range partitions {
			fp := kmsg.NewFetchRequestTopicPartition()
			fp.Partition = p
			fp.FetchOffset = 0
			fp.PartitionMaxBytes = 1 << 20
			fp.CurrentLeaderEpoch = -1
			ft.Partitions = append(ft.Partitions, fp)
		}
		req.Topics = append(req.Topics, ft)
	}
	return req
}

func recreateTopicRaw(t *testing.T, cl *kgo.Client, topic string) {
	t.Helper()
	ctx := context.Background()
	del := kmsg.NewPtrDeleteTopicsRequest()
	del.TopicNames = []string{topic}
	dt := kmsg.NewDeleteTopicsRequestTopic()
	dt.Topic = kmsg.StringPtr(topic)
	del.Topics = append(del.Topics, dt)
	dresp, err := del.RequestWith(ctx, cl)
	if err == nil {
		err = kerr.ErrorForCode(dresp.Topics[0].ErrorCode)
	}
	if err != nil {
		t.Fatalf("delete topic: %v", err)
	}
	cr := kmsg.NewPtrCreateTopicsRequest()
	ct := kmsg.NewCreateTopicsRequestTopic()
	ct.Topic = topic
	ct.NumPartitions = 1
	ct.ReplicationFactor = 1
	cr.Topics = append(cr.Topics, ct)
	cresp, err := cr.RequestWith(ctx, cl)
	if err == nil {
		err = kerr.ErrorForCode(cresp.Topics[0].ErrorCode)
	}
	if err != nil {
		t.Fatalf("create topic: %v", err)
	}
}

// A fetch with a partition that errors completes at once, like a real
// broker's fetch purgatory, rather than holding the request for MaxWait.
func TestFetchPartitionErrorCompletesImmediately(t *testing.T) {
	t.Parallel()
	const topic = "t"
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	cl := newPlainClient(t, c)
	ctx := context.Background()

	unknown := c.TopicInfo(topic).TopicID
	unknown[0]++
	start := time.Now()
	resp, err := fetchByID(0, 0, 5000, unknown, 0).RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("fetch of an unknown topic ID took %v; want an immediate completion", elapsed)
	}
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected response shape: %+v", resp.Topics)
	}
	if ec := resp.Topics[0].Partitions[0].ErrorCode; ec != kerr.UnknownTopicID.Code {
		t.Errorf("got %v; want UNKNOWN_TOPIC_ID", kerr.ErrorForCode(ec))
	}
}

// A session entry keeps the name it was inserted under. After the topic is
// recreated, an incremental fetch that leaves the entry to the session is
// rejected as INCONSISTENT_TOPIC_ID by the broker hosting the new
// incarnation, while a full fetch by the old ID is UNKNOWN_TOPIC_ID, as on a
// real broker.
func TestFetchSessionRecreatedTopic(t *testing.T) {
	t.Parallel()
	const topic = "t"
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	cl := newPlainClient(t, c)
	ctx := context.Background()

	oldID := c.TopicInfo(topic).TopicID
	first, err := fetchByID(0, 0, 100, oldID, 0).RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if first.ErrorCode != 0 || first.SessionID <= 0 {
		t.Fatalf("session not established: err %v, session %d", kerr.ErrorForCode(first.ErrorCode), first.SessionID)
	}

	recreateTopicRaw(t, cl, topic)
	if c.TopicInfo(topic).TopicID == oldID {
		t.Fatal("recreation kept the topic ID")
	}

	incr, err := fetchByID(first.SessionID, 1, 100, oldID).RequestWith(ctx, cl) // no topics: the session's entry answers
	if err != nil {
		t.Fatal(err)
	}
	if incr.ErrorCode != 0 || len(incr.Topics) != 1 || len(incr.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected incremental response: err %v, topics %+v", kerr.ErrorForCode(incr.ErrorCode), incr.Topics)
	}
	if got := incr.Topics[0].TopicID; got != oldID {
		t.Errorf("incremental response addressed %x; want the session's stale ID %x", got, oldID)
	}
	if ec := incr.Topics[0].Partitions[0].ErrorCode; ec != kerr.InconsistentTopicID.Code {
		t.Errorf("incremental fetch got %v; want INCONSISTENT_TOPIC_ID", kerr.ErrorForCode(ec))
	}

	full, err := fetchByID(0, 0, 100, oldID, 0).RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if len(full.Topics) != 1 || len(full.Topics[0].Partitions) != 1 {
		t.Fatalf("unexpected full response: %+v", full.Topics)
	}
	if ec := full.Topics[0].Partitions[0].ErrorCode; ec != kerr.UnknownTopicID.Code {
		t.Errorf("full fetch got %v; want UNKNOWN_TOPIC_ID", kerr.ErrorForCode(ec))
	}
}
