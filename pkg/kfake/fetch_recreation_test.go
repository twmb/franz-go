package kfake

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
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

// recreateTopic deletes and creates the topic, giving the new incarnation a
// new topic ID.
func recreateTopic(t *testing.T, c *Cluster, topic string) {
	t.Helper()
	recreateTopicN(t, c, topic, 1)
}

// recreateTopicN deletes the topic and creates it again with the given
// number of partitions.
func recreateTopicN(t *testing.T, c *Cluster, topic string, partitions int32) {
	t.Helper()
	if err := c.DeleteTopic(topic); err != nil {
		t.Fatalf("delete topic: %v", err)
	}
	if err := c.CreateTopic(topic, partitions, nil); err != nil {
		t.Fatalf("create topic: %v", err)
	}
}

// A fetch with a partition that errors completes at once, like a real
// broker's fetch purgatory, rather than holding the request for MaxWait. A
// partition that fails before the read leaves the rest to wait.
func TestFetchPartitionErrorCompletesImmediately(t *testing.T) {
	t.Parallel()
	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(2, topic))
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

	// An unknown ID whose bytes spell a fetched topic's name is its own
	// topic.
	const spelled = "abcdefghijklmnop"
	if err := c.CreateTopic(spelled, 1, nil); err != nil {
		t.Fatal(err)
	}
	both := fetchByID(0, -1, 0, c.TopicInfo(spelled).TopicID, 0)
	both.Topics = append(both.Topics, kmsg.FetchRequestTopic{TopicID: [16]byte([]byte(spelled)), Partitions: both.Topics[0].Partitions})
	if resp, err = both.RequestWith(ctx, cl); err != nil {
		t.Fatal(err)
	}
	if len(resp.Topics) != 2 {
		t.Errorf("fetch of %s and the ID that spells it: got %d topics, want 2", spelled, len(resp.Topics))
	}

	// A fault answers as its code: a read error ends the wait for the idle
	// partition next to it, and an unknown partition does not. A share
	// fetch waits the same way.
	known := c.TopicInfo(topic).TopicID
	memberID, _ := joinShareGroupRaw(t, cl, "g", topic)
	share := kmsg.NewPtrShareFetchRequest()
	share.GroupID, share.MemberID, share.MaxRecords = kmsg.StringPtr("g"), &memberID, 500
	share.Topics = []kmsg.ShareFetchRequestTopic{{TopicID: known, Partitions: []kmsg.ShareFetchRequestTopicPartition{{Partition: 0}, {Partition: 1}}}}
	for _, tc := range []struct {
		err     *kerr.Error
		maxWait int32
		wait    bool
	}{{kerr.KafkaStorageError, 5000, false}, {kerr.UnknownTopicOrPartition, 100, true}} {
		h := c.Fault(Fault{Keys: []kmsg.Key{kmsg.Fetch, kmsg.ShareFetch}, Topic: topic, Partitions: []int32{0}, Err: tc.err, Count: -1})
		share.MaxWaitMillis = tc.maxWait
		for _, req := range []kmsg.Request{fetchByID(0, -1, tc.maxWait, known, 0, 1), share} {
			start = time.Now()
			if _, err := cl.Request(ctx, req); err != nil {
				t.Fatal(err)
			}
			if elapsed := time.Since(start); tc.wait && elapsed < 90*time.Millisecond || !tc.wait && elapsed > time.Second {
				t.Errorf("%s with a %s fault took %v, want waiting %v", kmsg.NameForKey(req.Key()), tc.err.Message, elapsed, tc.wait)
			}
		}
		h.Remove()
	}

	// As in Kafka, MaxWait counts from when we handle the request: one
	// held 300ms before we handle it still waits its 200ms.
	share.MaxWaitMillis = 200
	for _, req := range []kmsg.Request{fetchByID(0, -1, 200, known, 1), share} {
		c.ControlKey(req.Key(), func(kmsg.Request) (kmsg.Response, error, bool) {
			c.SleepControl(func() { time.Sleep(300 * time.Millisecond) })
			return nil, nil, false
		})
		start = time.Now()
		if _, err := cl.Request(ctx, req); err != nil {
			t.Fatal(err)
		}
		if elapsed := time.Since(start); elapsed < 450*time.Millisecond {
			t.Errorf("%s held 300ms with a 200ms MaxWait took %v, want at least 500ms", kmsg.NameForKey(req.Key()), elapsed)
		}
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
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
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

	recreateTopic(t, c, topic)
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
