package kfake_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// fetchIDs builds a fetch addressing partition 0 of every topic ID, outside
// any fetch session.
func fetchIDs(maxWait int32, ids ...[16]byte) *kmsg.FetchRequest {
	req := kmsg.NewPtrFetchRequest()
	req.MaxWaitMillis = maxWait
	req.MinBytes = 1
	req.MaxBytes = 1 << 20
	req.SessionEpoch = -1
	for _, id := range ids {
		ft := kmsg.NewFetchRequestTopic()
		ft.TopicID = id
		fp := kmsg.NewFetchRequestTopicPartition()
		fp.Partition = 0
		fp.FetchOffset = 0
		fp.PartitionMaxBytes = 1 << 20
		fp.CurrentLeaderEpoch = -1
		ft.Partitions = append(ft.Partitions, fp)
		req.Topics = append(req.Topics, ft)
	}
	return req
}

func fetchIDsAt(t *testing.T, cl *kgo.Client, node int32, req *kmsg.FetchRequest) *kmsg.FetchResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	kresp, err := cl.Broker(int(node)).RetriableRequest(ctx, req)
	if err != nil {
		t.Fatalf("fetch: %v", err)
	}
	return kresp.(*kmsg.FetchResponse)
}

func fetchPart(t *testing.T, resp *kmsg.FetchResponse, id [16]byte) kmsg.FetchResponseTopicPartition {
	t.Helper()
	for _, st := range resp.Topics {
		if st.TopicID == id && len(st.Partitions) == 1 {
			return st.Partitions[0]
		}
	}
	t.Fatalf("topic ID %x missing from fetch response", id)
	return kmsg.FetchResponseTopicPartition{}
}

// listOffsetsAt lists the latest offset of one partition at one node and
// returns the partition's error code.
func listOffsetsAt(t *testing.T, cl *kgo.Client, node int32, topic string, partition int32) int16 {
	t.Helper()
	req := kmsg.NewPtrListOffsetsRequest()
	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewListOffsetsRequestTopicPartition()
	rp.Partition = partition
	rp.Timestamp = -1
	rp.CurrentLeaderEpoch = -1
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	kresp, err := cl.Broker(int(node)).RetriableRequest(ctx, req)
	if err != nil {
		t.Fatalf("list offsets: %v", err)
	}
	resp := kresp.(*kmsg.ListOffsetsResponse)
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("list offsets answered %d topics, want one topic with one partition", len(resp.Topics))
	}
	return resp.Topics[0].Partitions[0].ErrorCode
}

// A fault with a request budget fails a producer until the budget runs out,
// and the record lands once.
func TestFaultProduceCount(t *testing.T) {
	t.Parallel()
	const topic = "t"
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	// An unknown topic error parks the batch until metadata reloads, so
	// the retries only come as fast as we allow metadata to.
	cl := newPlainClient(t, c,
		kgo.RetryBackoffFn(func(int) time.Duration { return 50 * time.Millisecond }),
		kgo.UnknownTopicRetries(10),
		kgo.MetadataMinAge(100*time.Millisecond),
	)

	h := c.AddFault(kfake.Fault{
		Node:      -1,
		Topic:     topic,
		Partition: -1,
		Err:       kerr.UnknownTopicOrPartition,
		Count:     2,
	})
	produceSync(t, cl, &kgo.Record{Topic: topic, Value: []byte("v")})

	if n := h.Fired(); n != 2 {
		t.Errorf("fault fired %d times != 2", n)
	}
	if i := c.PartitionInfo(topic, 0); i.HighWatermark != 1 {
		t.Errorf("high watermark %d != 1", i.HighWatermark)
	}
	h.Remove() // removing an exhausted fault is fine
	if n := h.Fired(); n != 2 {
		t.Errorf("fault fired %d times after removal != 2", n)
	}
}

// A fault on one topic ID fails only that topic; the rest of the fetch is
// answered normally and at once.
func TestFaultFetchByTopicID(t *testing.T) {
	t.Parallel()
	const topic, other = "t", "other"
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic, other))
	recreateTopicRaw(t, newPlainClient(t, c), topic)
	cl := newPlainClient(t, c)

	id := c.TopicInfo(topic).TopicID
	otherID := c.TopicInfo(other).TopicID
	produceSync(t, cl,
		&kgo.Record{Topic: topic, Value: []byte("v")},
		&kgo.Record{Topic: other, Value: []byte("v")},
	)

	h := c.AddFault(kfake.Fault{
		Node:      -1,
		TopicID:   id,
		Partition: -1,
		Err:       kerr.UnknownTopicID,
		Count:     3,
	})
	for i := range 3 {
		start := time.Now()
		resp := fetchIDsAt(t, cl, 0, fetchIDs(30000, id, otherID))
		if p := fetchPart(t, resp, id); p.ErrorCode != kerr.UnknownTopicID.Code {
			t.Fatalf("fetch %d: faulted partition answered %d != %d", i, p.ErrorCode, kerr.UnknownTopicID.Code)
		}
		if p := fetchPart(t, resp, otherID); p.ErrorCode != 0 || len(p.RecordBatches) == 0 {
			t.Fatalf("fetch %d: unfaulted partition answered %d with %d record bytes", i, p.ErrorCode, len(p.RecordBatches))
		}
		if took := time.Since(start); took > 5*time.Second {
			t.Fatalf("fetch %d took %s: a faulted partition must not wait out MaxWait", i, took)
		}
	}
	if n := h.Fired(); n != 3 {
		t.Errorf("fault fired %d times != 3", n)
	}

	resp := fetchIDsAt(t, cl, 0, fetchIDs(30000, id, otherID))
	if p := fetchPart(t, resp, id); p.ErrorCode != 0 || len(p.RecordBatches) == 0 {
		t.Errorf("exhausted fault still failing: %d with %d record bytes", p.ErrorCode, len(p.RecordBatches))
	}
	if n := h.Fired(); n != 3 {
		t.Errorf("fault fired %d times after exhaustion != 3", n)
	}
}

// Keys restricts a fault to one request kind.
func TestFaultKeys(t *testing.T) {
	t.Parallel()
	const topic = "t"
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	cl := newPlainClient(t, c)
	produceSync(t, cl, &kgo.Record{Topic: topic, Value: []byte("v")})

	h := c.AddFault(kfake.Fault{
		Keys:      []int16{int16(kmsg.Produce)},
		Node:      -1,
		Topic:     topic,
		Partition: -1,
		Err:       kerr.PolicyViolation,
	})
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("v")}).FirstErr()
	if !errors.Is(err, kerr.PolicyViolation) {
		t.Fatalf("produce err %v != policy violation", err)
	}

	id := c.TopicInfo(topic).TopicID
	resp := fetchIDsAt(t, cl, 0, fetchIDs(30000, id))
	if p := fetchPart(t, resp, id); p.ErrorCode != 0 || len(p.RecordBatches) == 0 {
		t.Errorf("fetch answered %d with %d record bytes: a produce fault must not touch fetch", p.ErrorCode, len(p.RecordBatches))
	}
	if n := h.Fired(); n != 1 {
		t.Errorf("fault fired %d times != 1", n)
	}
}

// A fault with no budget fires until removed.
func TestFaultRemove(t *testing.T) {
	t.Parallel()
	const topic = "t"
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	cl := newPlainClient(t, c)

	h := c.AddFault(kfake.Fault{
		Keys:      []int16{int16(kmsg.ListOffsets)},
		Node:      -1,
		Topic:     topic,
		Partition: -1,
		Err:       kerr.UnknownTopicOrPartition,
	})
	for i := range 3 {
		if code := listOffsetsAt(t, cl, 0, topic, 0); code != kerr.UnknownTopicOrPartition.Code {
			t.Fatalf("list offsets %d answered %d != %d", i, code, kerr.UnknownTopicOrPartition.Code)
		}
	}
	if n := h.Fired(); n != 3 {
		t.Errorf("fault fired %d times != 3", n)
	}

	h.Remove()
	h.Remove() // twice is fine
	for i := range 2 {
		if code := listOffsetsAt(t, cl, 0, topic, 0); code != 0 {
			t.Fatalf("list offsets %d after removal answered %d != 0", i, code)
		}
	}
	if n := h.Fired(); n != 3 {
		t.Errorf("fault fired %d times after removal != 3", n)
	}
}

// A fault pinned to a node fires only there; Node -1 fires everywhere.
func TestFaultNode(t *testing.T) {
	t.Parallel()
	const topic = "t"
	c := newCluster(t, kfake.NumBrokers(2), kfake.SeedTopics(1, topic))
	cl := newPlainClient(t, c)
	if err := c.MoveTopicPartition(topic, 0, 1); err != nil {
		t.Fatalf("move: %v", err)
	}

	one := c.AddFault(kfake.Fault{
		Keys:      []int16{int16(kmsg.ListOffsets)},
		Node:      0,
		Topic:     topic,
		Partition: -1,
		Err:       kerr.PolicyViolation,
	})
	if code := listOffsetsAt(t, cl, 1, topic, 0); code != 0 {
		t.Errorf("leader node answered %d != 0: a node 0 fault must not fire on node 1", code)
	}
	if n := one.Fired(); n != 0 {
		t.Errorf("node 0 fault fired %d times != 0", n)
	}
	if code := listOffsetsAt(t, cl, 0, topic, 0); code != kerr.PolicyViolation.Code {
		t.Errorf("node 0 answered %d != %d", code, kerr.PolicyViolation.Code)
	}
	if n := one.Fired(); n != 1 {
		t.Errorf("node 0 fault fired %d times != 1", n)
	}
	one.Remove()

	any := c.AddFault(kfake.Fault{
		Keys:      []int16{int16(kmsg.ListOffsets)},
		Node:      -1,
		Topic:     topic,
		Partition: -1,
		Err:       kerr.PolicyViolation,
	})
	for _, node := range []int32{0, 1} {
		if code := listOffsetsAt(t, cl, node, topic, 0); code != kerr.PolicyViolation.Code {
			t.Errorf("node %d answered %d != %d", node, code, kerr.PolicyViolation.Code)
		}
	}
	if n := any.Fired(); n != 2 {
		t.Errorf("any-node fault fired %d times != 2", n)
	}
}
