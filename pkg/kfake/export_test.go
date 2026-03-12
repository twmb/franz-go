package kfake

import (
	"context"
	"fmt"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type testMemFS struct{ fs fs }

func newTestMemFS() *testMemFS { return &testMemFS{fs: newMemFS()} }
func (m *testMemFS) opt() Opt  { return withFS(m.fs) }

func newCluster(t *testing.T, opts ...Opt) *Cluster {
	t.Helper()
	c, err := NewCluster(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func newPlainClient(t *testing.T, c *Cluster, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	opts = append([]kgo.Opt{kgo.SeedBrokers(c.ListenAddrs()...)}, opts...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func newShareConsumer(t *testing.T, c *Cluster, topic, group string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	opts = append([]kgo.Opt{
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics(topic),
		kgo.ShareGroup(group),
		kgo.FetchMaxWait(200 * time.Millisecond),
	}, opts...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// collectRecords polls until at least n records are collected or the timeout
// expires. It fatals if fewer than n records arrive.
func collectRecords(t *testing.T, cl *kgo.Client, n int, timeout time.Duration) []*kgo.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var records []*kgo.Record
	for ctx.Err() == nil && len(records) < n {
		fetches := cl.PollFetches(ctx)
		fetches.EachRecord(func(r *kgo.Record) {
			records = append(records, r)
		})
	}
	if len(records) < n {
		t.Fatalf("collectRecords: wanted %d, got %d (timeout %v)", n, len(records), timeout)
	}
	return records
}

// verifyZeroRecords polls for the given duration and fails if any records
// are returned.
func verifyZeroRecords(t *testing.T, cl *kgo.Client, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var got int
	for ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		got += len(fetches.Records())
	}
	if got > 0 {
		t.Errorf("expected zero records, got %d", got)
	}
}

// produceN creates a plain client, produces n records with values "v0".."vN-1"
// to the given topic, then closes the client.
func produceN(t *testing.T, c *Cluster, topic string, n int) {
	t.Helper()
	cl := newPlainClient(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := range n {
		r := &kgo.Record{Topic: topic, Value: fmt.Appendf(nil, "v%d", i)}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
}

func stringp(s string) *string { return &s }

// produceShareN creates a plain client, sets share.auto.offset.reset=earliest
// for the given group, produces n string records to the topic, and flushes.
func produceShareN(t *testing.T, c *Cluster, topic, group string, n int) {
	t.Helper()
	cl := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
	setShareAutoOffsetReset(t, cl, group)
	for i := range n {
		cl.Produce(context.Background(), kgo.StringRecord(strconv.Itoa(i)), func(_ *kgo.Record, err error) {
			if err != nil {
				t.Errorf("produce %d: %v", i, err)
			}
		})
	}
	if err := cl.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

// joinShareGroupRaw joins a share group via raw heartbeat+metadata and returns
// the member ID and topic UUID.
func joinShareGroupRaw(t *testing.T, cl *kgo.Client, group, topic string) (memberID string, topicID [16]byte) {
	t.Helper()
	hbReq := kmsg.NewPtrShareGroupHeartbeatRequest()
	hbReq.GroupID = group
	hbReq.MemberID = "test-member-1"
	hbReq.MemberEpoch = 0
	hbReq.SubscribedTopicNames = []string{topic}
	hbResp, err := hbReq.RequestWith(context.Background(), cl)
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if hbResp.ErrorCode != 0 {
		t.Fatalf("heartbeat error: %v", kerr.ErrorForCode(hbResp.ErrorCode))
	}
	memberID = *hbResp.MemberID

	metaReq := kmsg.NewPtrMetadataRequest()
	metaReqTopic := kmsg.NewMetadataRequestTopic()
	metaReqTopic.Topic = kmsg.StringPtr(topic)
	metaReq.Topics = append(metaReq.Topics, metaReqTopic)
	metaResp, err := metaReq.RequestWith(context.Background(), cl)
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	topicID = metaResp.Topics[0].TopicID
	return memberID, topicID
}

// rawShareFetch sends a ShareFetch request and returns the response and count
// of acquired records.
func rawShareFetch(t *testing.T, cl *kgo.Client, group, memberID string, topicID [16]byte, epoch int32) (*kmsg.ShareFetchResponse, int) {
	t.Helper()
	sfReq := kmsg.NewPtrShareFetchRequest()
	sfReq.GroupID = &group
	sfReq.MemberID = &memberID
	sfReq.ShareSessionEpoch = epoch
	sfReq.MaxRecords = 500
	sfTopic := kmsg.NewShareFetchRequestTopic()
	sfTopic.TopicID = topicID
	sfPart := kmsg.NewShareFetchRequestTopicPartition()
	sfPart.Partition = 0
	sfPart.PartitionMaxBytes = 1 << 20
	sfTopic.Partitions = append(sfTopic.Partitions, sfPart)
	sfReq.Topics = append(sfReq.Topics, sfTopic)
	sfResp, err := sfReq.RequestWith(context.Background(), cl)
	if err != nil {
		t.Fatalf("ShareFetch: %v", err)
	}
	var acquired int
	for _, rt := range sfResp.Topics {
		for _, rp := range rt.Partitions {
			for _, ab := range rp.AcquiredRecords {
				acquired += int(ab.LastOffset-ab.FirstOffset) + 1
			}
		}
	}
	return sfResp, acquired
}
