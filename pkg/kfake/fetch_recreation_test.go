package kfake_test

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
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
