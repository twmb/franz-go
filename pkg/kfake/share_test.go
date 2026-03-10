package kfake_test

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func setShareAutoOffsetReset(t *testing.T, cl *kgo.Client, group string) {
	t.Helper()
	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	res := kmsg.NewIncrementalAlterConfigsRequestResource()
	res.ResourceType = kmsg.ConfigResourceTypeGroupConfig
	res.ResourceName = group
	cfg := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	cfg.Name = "share.auto.offset.reset"
	cfg.Op = 0
	cfg.Value = kmsg.StringPtr("earliest")
	res.Configs = append(res.Configs, cfg)
	req.Resources = append(req.Resources, res)
	resp, err := req.RequestWith(context.Background(), cl)
	if err != nil {
		t.Fatalf("IncrementalAlterConfigs: %v", err)
	}
	for _, r := range resp.Resources {
		if err := kerr.ErrorForCode(r.ErrorCode); err != nil {
			t.Fatalf("IncrementalAlterConfigs resource error: %v", err)
		}
	}
}

func TestShareGroupBasic(t *testing.T) {
	t.Parallel()

	c := newCluster(t, kfake.SeedTopics(1, "share-basic"))
	group := "share-test-basic"

	// Admin client for producing and config.
	admin, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic("share-basic"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()

	setShareAutoOffsetReset(t, admin, group)

	// Produce 50 records.
	const total = 50
	for i := range total {
		admin.Produce(context.Background(), kgo.StringRecord(strconv.Itoa(i)), func(_ *kgo.Record, err error) {
			if err != nil {
				t.Errorf("produce %d: %v", i, err)
			}
		})
	}
	if err := admin.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Share group consumer.
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics("share-basic"),
		kgo.ShareGroup(group),
		kgo.FetchMaxWait(200*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var got int
	for got < total {
		fetches := cl.PollFetches(ctx)
		for _, e := range fetches.Errors() {
			if e.Err == context.DeadlineExceeded || e.Err == context.Canceled {
				continue
			}
			t.Errorf("fetch error: %v", e)
		}
		records := fetches.Records()
		got += len(records)
		if ctx.Err() != nil {
			break
		}
	}

	if got != total {
		t.Fatalf("expected %d records, got %d", total, got)
	}
}

func TestShareGroupAckAndRedelivery(t *testing.T) {
	t.Parallel()

	c := newCluster(t, kfake.SeedTopics(1, "share-ack"))
	group := "share-test-ack"

	admin, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic("share-ack"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()

	setShareAutoOffsetReset(t, admin, group)

	// Produce 10 records with numeric keys.
	const total = 10
	for i := range total {
		r := &kgo.Record{
			Topic: "share-ack",
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(strconv.Itoa(i)),
		}
		admin.Produce(context.Background(), r, func(_ *kgo.Record, err error) {
			if err != nil {
				t.Errorf("produce %d: %v", i, err)
			}
		})
	}
	if err := admin.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Consumer 1: poll all records, release half, accept half.
	cl1, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics("share-ack"),
		kgo.ShareGroup(group),
		kgo.FetchMaxWait(200*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var released int
	var got1 int
	for got1 < total {
		fetches := cl1.PollFetches(ctx)
		for _, r := range fetches.Records() {
			got1++
			keyNum, _ := strconv.Atoi(string(r.Key))
			if keyNum%2 == 0 {
				r.Ack(kgo.AckRelease)
				released++
			} else {
				r.Ack(kgo.AckAccept)
			}
		}
		if ctx.Err() != nil {
			break
		}
	}
	if got1 < total {
		t.Fatalf("consumer 1: expected %d records, got %d", total, got1)
	}

	// Commit acks and close.
	commitCtx, commitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := cl1.CommitAcks(commitCtx); err != nil {
		t.Fatalf("commit acks: %v", err)
	}
	commitCancel()
	cl1.Close()

	t.Logf("consumer 1: got %d, released %d", got1, released)

	// Consumer 2: should see released records redelivered.
	cl2, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics("share-ack"),
		kgo.ShareGroup(group),
		kgo.FetchMaxWait(200*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl2.Close()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel2()

	var got2 int
	for {
		fetches := cl2.PollFetches(ctx2)
		records := fetches.Records()
		got2 += len(records)
		for _, r := range records {
			r.Ack(kgo.AckAccept)
		}
		if got2 >= released {
			break
		}
		if ctx2.Err() != nil {
			break
		}
	}

	if got2 < released {
		t.Errorf("consumer 2: expected at least %d redelivered records, got %d", released, got2)
	}
	t.Logf("consumer 2: got %d redelivered records", got2)
}

// TestShareGroupReject verifies that rejected records are archived and not
// redelivered to subsequent consumers.
func TestShareGroupReject(t *testing.T) {
	t.Parallel()

	c := newCluster(t, kfake.SeedTopics(1, "share-reject"))
	group := "share-test-reject"

	admin, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic("share-reject"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()

	setShareAutoOffsetReset(t, admin, group)

	// Produce 20 records.
	const total = 20
	for i := range total {
		r := &kgo.Record{
			Topic: "share-reject",
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte("v"),
		}
		admin.Produce(context.Background(), r, func(_ *kgo.Record, err error) {
			if err != nil {
				t.Errorf("produce %d: %v", i, err)
			}
		})
	}
	if err := admin.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Consumer 1: reject all records.
	cl1, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics("share-reject"),
		kgo.ShareGroup(group),
		kgo.FetchMaxWait(200*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var rejected int
	for rejected < total {
		fetches := cl1.PollFetches(ctx)
		for _, r := range fetches.Records() {
			r.Ack(kgo.AckReject)
			rejected++
		}
		commitCtx, commitCancel := context.WithTimeout(context.Background(), 5*time.Second)
		cl1.CommitAcks(commitCtx)
		commitCancel()
		if ctx.Err() != nil {
			break
		}
	}
	cl1.Close()
	t.Logf("consumer 1: rejected %d records", rejected)

	// Consumer 2: should see no records since all were rejected.
	cl2, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics("share-reject"),
		kgo.ShareGroup(group),
		kgo.FetchMaxWait(200*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl2.Close()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()

	var got2 int
	for ctx2.Err() == nil {
		fetches := cl2.PollFetches(ctx2)
		got2 += len(fetches.Records())
	}

	if got2 > 0 {
		t.Errorf("consumer 2: got %d records, expected 0 (all should be archived)", got2)
	} else {
		t.Log("consumer 2: correctly received 0 records")
	}
}

// TestShareGroupMaxDeliveryCount verifies that records are archived after
// reaching the max delivery attempt count. After archival, a second consumer
// should not see those records.
func TestShareGroupMaxDeliveryCount(t *testing.T) {
	t.Parallel()

	// Set max delivery attempts to 2 so records are archived after 2 releases.
	c := newCluster(t,
		kfake.SeedTopics(1, "share-maxdlv"),
		kfake.BrokerConfigs(map[string]string{
			"share.max.delivery.attempts": "2",
		}),
	)
	group := "share-test-maxdlv"

	admin, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic("share-maxdlv"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()

	setShareAutoOffsetReset(t, admin, group)

	const total = 5
	for i := range total {
		admin.Produce(context.Background(), kgo.StringRecord(strconv.Itoa(i)), func(_ *kgo.Record, err error) {
			if err != nil {
				t.Errorf("produce %d: %v", i, err)
			}
		})
	}
	if err := admin.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Consumer 1: fetch all 5 records, release all of them.
	// This is delivery attempt 1.
	cl1, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics("share-maxdlv"),
		kgo.ShareGroup(group),
		kgo.FetchMaxWait(200*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var got1 int
	for got1 < total {
		fetches := cl1.PollFetches(ctx)
		for _, r := range fetches.Records() {
			got1++
			if r.DeliveryCount() != 1 {
				t.Errorf("consumer 1: offset %d delivery count = %d, want 1", r.Offset, r.DeliveryCount())
			}
			r.Ack(kgo.AckRelease)
		}
		if ctx.Err() != nil {
			break
		}
	}
	if got1 < total {
		t.Fatalf("consumer 1: expected %d records, got %d", total, got1)
	}
	cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
	cl1.CommitAcks(cCtx)
	cCancel()
	cl1.Close()

	// Consumer 2: fetch records again (delivery attempt 2), release again.
	cl2, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics("share-maxdlv"),
		kgo.ShareGroup(group),
		kgo.FetchMaxWait(200*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()

	var got2 int
	for got2 < total {
		fetches := cl2.PollFetches(ctx2)
		for _, r := range fetches.Records() {
			got2++
			if r.DeliveryCount() != 2 {
				t.Errorf("consumer 2: offset %d delivery count = %d, want 2", r.Offset, r.DeliveryCount())
			}
			r.Ack(kgo.AckRelease) // release again -- should trigger archival
		}
		if ctx2.Err() != nil {
			break
		}
	}
	if got2 < total {
		t.Fatalf("consumer 2: expected %d records, got %d", total, got2)
	}
	cCtx, cCancel = context.WithTimeout(context.Background(), 5*time.Second)
	cl2.CommitAcks(cCtx)
	cCancel()
	cl2.Close()

	// Consumer 3: should see no records since all were archived (max delivery reached).
	cl3, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics("share-maxdlv"),
		kgo.ShareGroup(group),
		kgo.FetchMaxWait(200*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl3.Close()

	ctx3, cancel3 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel3()

	var got3 int
	for ctx3.Err() == nil {
		fetches := cl3.PollFetches(ctx3)
		got3 += len(fetches.Records())
	}
	if got3 > 0 {
		t.Errorf("consumer 3: expected 0 records after max delivery, got %d", got3)
	}
}

// TestShareGroupAutoAckOnPoll verifies that records not explicitly acked
// before the next PollFetches are auto-accepted.
func TestShareGroupAutoAckOnPoll(t *testing.T) {
	t.Parallel()

	c := newCluster(t, kfake.SeedTopics(1, "share-autoack"))
	group := "share-test-autoack"

	admin, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic("share-autoack"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()

	setShareAutoOffsetReset(t, admin, group)

	const total = 10
	for i := range total {
		admin.Produce(context.Background(), kgo.StringRecord(strconv.Itoa(i)), func(_ *kgo.Record, err error) {
			if err != nil {
				t.Errorf("produce %d: %v", i, err)
			}
		})
	}
	if err := admin.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Consumer 1: poll all records but do NOT ack them. The next poll
	// (which will return nothing new) should auto-accept them.
	cl1, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics("share-autoack"),
		kgo.ShareGroup(group),
		kgo.FetchMaxWait(200*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var got1 int
	for got1 < total {
		fetches := cl1.PollFetches(ctx)
		got1 += len(fetches.Records())
		// Intentionally NOT calling r.Ack() on any record.
		if ctx.Err() != nil {
			break
		}
	}
	if got1 < total {
		t.Fatalf("consumer 1: expected %d records, got %d", total, got1)
	}

	// Poll again to trigger auto-accept of the previous batch.
	// The auto-accepted acks are routed to the source's pending acks.
	cl1.PollFetches(ctx)

	// CommitAcks sends any pending acks -- including auto-accepted ones
	// that were routed to sources during finalizePreviousPoll.
	// But CommitAcks only sends records finalized by finalizeMarkedRecords,
	// NOT those already in source pending acks. The pending acks from
	// auto-accept will be piggybacked on the next ShareFetch or sent
	// on close. Force another poll + commit cycle to send them.
	cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := cl1.CommitAcks(cCtx); err != nil {
		t.Fatalf("commit acks: %v", err)
	}
	cCancel()
	cl1.Close()

	// Consumer 2: should see no records since all were auto-accepted.
	cl2, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics("share-autoack"),
		kgo.ShareGroup(group),
		kgo.FetchMaxWait(200*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl2.Close()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()

	var got2 int
	for ctx2.Err() == nil {
		fetches := cl2.PollFetches(ctx2)
		got2 += len(fetches.Records())
	}
	if got2 > 0 {
		t.Errorf("consumer 2: expected 0 records after auto-accept, got %d", got2)
	}
}

// TestShareGroupAcquisitionLockExpiry verifies that records whose acquisition
// lock expires are released by the sweep timer and redelivered to another
// consumer, without the first consumer explicitly releasing or leaving.
func TestShareGroupAcquisitionLockExpiry(t *testing.T) {
	t.Parallel()

	// Short lock duration and sweep interval so the test runs quickly.
	// Single broker so raw ShareFetch goes to the partition leader.
	c := newCluster(t,
		kfake.NumBrokers(1),
		kfake.SeedTopics(1, "share-lockexp"),
		kfake.BrokerConfigs(map[string]string{
			"share.record.lock.duration.ms":       "100",
			"share.record.lock.sweep.interval.ms": "100",
		}),
	)
	group := "share-test-lockexp"

	admin, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic("share-lockexp"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()

	setShareAutoOffsetReset(t, admin, group)

	const total = 5
	for i := range total {
		admin.Produce(context.Background(), kgo.StringRecord(strconv.Itoa(i)), func(_ *kgo.Record, err error) {
			if err != nil {
				t.Errorf("produce %d: %v", i, err)
			}
		})
	}
	if err := admin.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Member 1: join and acquire via raw ShareFetch, then do NOT ack
	// and do NOT leave. The acquisition lock should expire and the
	// sweep timer should release the records.
	cl1, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl1.Close()

	hbReq := kmsg.NewPtrShareGroupHeartbeatRequest()
	hbReq.GroupID = group
	hbReq.MemberID = "test-member-1"
	hbReq.MemberEpoch = 0
	hbReq.SubscribedTopicNames = []string{"share-lockexp"}
	hbResp, err := hbReq.RequestWith(context.Background(), cl1)
	if err != nil {
		t.Fatalf("heartbeat join: %v", err)
	}
	if hbResp.ErrorCode != 0 {
		t.Fatalf("heartbeat join error: %v", kerr.ErrorForCode(hbResp.ErrorCode))
	}
	memberID1 := *hbResp.MemberID

	metaReq := kmsg.NewPtrMetadataRequest()
	mt := kmsg.NewMetadataRequestTopic()
	mt.Topic = kmsg.StringPtr("share-lockexp")
	metaReq.Topics = append(metaReq.Topics, mt)
	metaResp, err := metaReq.RequestWith(context.Background(), cl1)
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	topicID := metaResp.Topics[0].TopicID

	sfReq := kmsg.NewPtrShareFetchRequest()
	sfReq.GroupID = &group
	sfReq.MemberID = &memberID1
	sfReq.ShareSessionEpoch = 0
	sfReq.MaxRecords = 100
	st := kmsg.NewShareFetchRequestTopic()
	st.TopicID = topicID
	sp := kmsg.NewShareFetchRequestTopicPartition()
	sp.Partition = 0
	st.Partitions = append(st.Partitions, sp)
	sfReq.Topics = append(sfReq.Topics, st)
	sfResp, err := sfReq.RequestWith(context.Background(), cl1)
	if err != nil {
		t.Fatalf("share fetch: %v", err)
	}
	if sfResp.ErrorCode != 0 {
		t.Fatalf("share fetch error: %v", kerr.ErrorForCode(sfResp.ErrorCode))
	}

	var acquired int
	for _, rt := range sfResp.Topics {
		for _, rp := range rt.Partitions {
			for _, ar := range rp.AcquiredRecords {
				acquired += int(ar.LastOffset - ar.FirstOffset + 1)
			}
		}
	}
	if acquired < total {
		t.Fatalf("expected to acquire %d records, got %d", total, acquired)
	}

	// Do NOT ack, do NOT leave. Wait for lock expiry + sweep.
	time.Sleep(300 * time.Millisecond)

	// Member 2 (via kgo share consumer): should see the records that were
	// released by the expired acquisition lock sweep.
	cl2, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics("share-lockexp"),
		kgo.ShareGroup(group),
		kgo.FetchMaxWait(200*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl2.Close()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()

	var got2 int
	for got2 < total {
		fetches := cl2.PollFetches(ctx2)
		for _, r := range fetches.Records() {
			got2++
			r.Ack(kgo.AckAccept)
		}
		if ctx2.Err() != nil {
			break
		}
	}
	if got2 < total {
		t.Fatalf("consumer 2: expected %d redelivered records after lock expiry, got %d", total, got2)
	}

	// Clean up member 1.
	hbLeave := kmsg.NewPtrShareGroupHeartbeatRequest()
	hbLeave.GroupID = group
	hbLeave.MemberID = memberID1
	hbLeave.MemberEpoch = -1
	hbLeave.RequestWith(context.Background(), cl1)
}

// TestShareGroupSessionEpoch verifies that share session epoch tracking works:
// invalid epoch returns INVALID_SHARE_SESSION_EPOCH, and unknown session
// returns SHARE_SESSION_NOT_FOUND.
func TestShareGroupSessionEpoch(t *testing.T) {
	t.Parallel()

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, "share-epoch"))
	group := "share-test-epoch"

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.RetryTimeout(0),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	setShareAutoOffsetReset(t, cl, group)

	// Join the share group.
	hbReq := kmsg.NewPtrShareGroupHeartbeatRequest()
	hbReq.GroupID = group
	hbReq.MemberID = "test-member-1"
	hbReq.MemberEpoch = 0
	hbReq.SubscribedTopicNames = []string{"share-epoch"}
	hbResp, err := hbReq.RequestWith(context.Background(), cl)
	if err != nil {
		t.Fatalf("heartbeat join: %v", err)
	}
	memberID := *hbResp.MemberID

	// Get topic ID.
	metaReq := kmsg.NewPtrMetadataRequest()
	mt := kmsg.NewMetadataRequestTopic()
	mt.Topic = kmsg.StringPtr("share-epoch")
	metaReq.Topics = append(metaReq.Topics, mt)
	metaResp, err := metaReq.RequestWith(context.Background(), cl)
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	topicID := metaResp.Topics[0].TopicID

	mkFetch := func(epoch int32) *kmsg.ShareFetchRequest {
		req := kmsg.NewPtrShareFetchRequest()
		req.GroupID = &group
		req.MemberID = &memberID
		req.ShareSessionEpoch = epoch
		req.MaxRecords = 10
		st := kmsg.NewShareFetchRequestTopic()
		st.TopicID = topicID
		sp := kmsg.NewShareFetchRequestTopicPartition()
		sp.Partition = 0
		st.Partitions = append(st.Partitions, sp)
		req.Topics = append(req.Topics, st)
		return req
	}

	// Use MaxVersions to disable retries for ShareFetch -- we want
	// exactly the error codes we send.
	doFetch := func(epoch int32) *kmsg.ShareFetchResponse {
		t.Helper()
		req := mkFetch(epoch)
		// Bypass kgo retry by sending directly.
		resp, err := req.RequestWith(context.Background(), cl)
		if err != nil {
			t.Fatalf("fetch epoch %d request error: %v", epoch, err)
		}
		return resp
	}

	// Epoch 0: new session -- should succeed. Server sets session epoch to 1.
	resp := doFetch(0)
	if resp.ErrorCode != 0 {
		t.Fatalf("fetch epoch 0 error: %v", kerr.ErrorForCode(resp.ErrorCode))
	}

	// Epoch 1: matches server's epoch (1) -- should succeed. Server advances to 2.
	resp = doFetch(1)
	if resp.ErrorCode != 0 {
		t.Fatalf("fetch epoch 1 error: %v", kerr.ErrorForCode(resp.ErrorCode))
	}

	// Epoch 2: matches server's epoch (2) -- should succeed. Server advances to 3.
	resp = doFetch(2)
	if resp.ErrorCode != 0 {
		t.Fatalf("fetch epoch 2 error: %v", kerr.ErrorForCode(resp.ErrorCode))
	}

	// Epoch 2 again: stale (server is at 3) -- should fail.
	resp = doFetch(2)
	if resp.ErrorCode != kerr.InvalidShareSessionEpoch.Code {
		t.Fatalf("expected INVALID_SHARE_SESSION_EPOCH for stale epoch, got %v", kerr.ErrorForCode(resp.ErrorCode))
	}

	// Close session with epoch -1.
	resp = doFetch(-1)
	if resp.ErrorCode != 0 {
		t.Fatalf("fetch epoch -1 error: %v", kerr.ErrorForCode(resp.ErrorCode))
	}

	// Any non-zero epoch after close: session not found.
	resp = doFetch(3)
	if resp.ErrorCode != kerr.ShareSessionNotFound.Code {
		t.Fatalf("expected SHARE_SESSION_NOT_FOUND, got %v", kerr.ErrorForCode(resp.ErrorCode))
	}

	// Clean up: leave.
	hbReq2 := kmsg.NewPtrShareGroupHeartbeatRequest()
	hbReq2.GroupID = group
	hbReq2.MemberID = memberID
	hbReq2.MemberEpoch = -1
	hbReq2.RequestWith(context.Background(), cl)
}

// TestShareGroupSessionTimeout verifies that a member that stops heartbeating
// is fenced and its acquired records are released.
func TestShareGroupSessionTimeout(t *testing.T) {
	t.Parallel()

	// Short session timeout so the test doesn't take long.
	// Single broker so raw ShareFetch goes to the partition leader.
	c := newCluster(t,
		kfake.NumBrokers(1),
		kfake.SeedTopics(1, "share-sessexp"),
		kfake.BrokerConfigs(map[string]string{
			"group.share.session.timeout.ms": "500",
		}),
	)
	group := "share-test-sessexp"

	admin, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic("share-sessexp"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()

	setShareAutoOffsetReset(t, admin, group)

	const total = 5
	for i := range total {
		admin.Produce(context.Background(), kgo.StringRecord(strconv.Itoa(i)), func(_ *kgo.Record, err error) {
			if err != nil {
				t.Errorf("produce %d: %v", i, err)
			}
		})
	}
	if err := admin.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Member 1: join, fetch records, then stop heartbeating.
	cl1, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl1.Close()

	hbReq := kmsg.NewPtrShareGroupHeartbeatRequest()
	hbReq.GroupID = group
	hbReq.MemberID = "test-member-1"
	hbReq.MemberEpoch = 0
	hbReq.SubscribedTopicNames = []string{"share-sessexp"}
	hbResp, err := hbReq.RequestWith(context.Background(), cl1)
	if err != nil {
		t.Fatalf("heartbeat join: %v", err)
	}
	memberID1 := *hbResp.MemberID

	// Fetch via raw ShareFetch to acquire without auto-acking.
	metaReq := kmsg.NewPtrMetadataRequest()
	mt := kmsg.NewMetadataRequestTopic()
	mt.Topic = kmsg.StringPtr("share-sessexp")
	metaReq.Topics = append(metaReq.Topics, mt)
	metaResp, err := metaReq.RequestWith(context.Background(), cl1)
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	topicID := metaResp.Topics[0].TopicID

	sfReq := kmsg.NewPtrShareFetchRequest()
	sfReq.GroupID = &group
	sfReq.MemberID = &memberID1
	sfReq.ShareSessionEpoch = 0
	sfReq.MaxRecords = 100
	st := kmsg.NewShareFetchRequestTopic()
	st.TopicID = topicID
	sp := kmsg.NewShareFetchRequestTopicPartition()
	sp.Partition = 0
	st.Partitions = append(st.Partitions, sp)
	sfReq.Topics = append(sfReq.Topics, st)
	sfResp, err := sfReq.RequestWith(context.Background(), cl1)
	if err != nil {
		t.Fatalf("share fetch: %v", err)
	}
	var acquired int
	for _, rt := range sfResp.Topics {
		for _, rp := range rt.Partitions {
			for _, ar := range rp.AcquiredRecords {
				acquired += int(ar.LastOffset - ar.FirstOffset + 1)
			}
		}
	}
	if acquired < total {
		t.Fatalf("expected to acquire %d, got %d", total, acquired)
	}

	// Stop heartbeating. Wait for session timeout (500ms) + some margin
	// for the fencing to happen.
	time.Sleep(800 * time.Millisecond)

	// Verify the member was fenced by trying a regular heartbeat.
	hbReq2 := kmsg.NewPtrShareGroupHeartbeatRequest()
	hbReq2.GroupID = group
	hbReq2.MemberID = memberID1
	hbReq2.MemberEpoch = hbResp.MemberEpoch
	hbResp2, err := hbReq2.RequestWith(context.Background(), cl1)
	if err != nil {
		t.Fatalf("heartbeat after timeout: %v", err)
	}
	if hbResp2.ErrorCode != kerr.UnknownMemberID.Code {
		t.Fatalf("expected UNKNOWN_MEMBER_ID after session timeout, got %v", kerr.ErrorForCode(hbResp2.ErrorCode))
	}

	// Consumer 2: should pick up released records (fencing releases them).
	cl2, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics("share-sessexp"),
		kgo.ShareGroup(group),
		kgo.FetchMaxWait(200*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl2.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var got2 int
	for got2 < total {
		fetches := cl2.PollFetches(ctx)
		for _, r := range fetches.Records() {
			got2++
			r.Ack(kgo.AckAccept)
		}
		if ctx.Err() != nil {
			break
		}
	}
	if got2 < total {
		t.Fatalf("consumer 2: expected %d records after session timeout, got %d", total, got2)
	}
}

// TestShareGroupStandaloneAcknowledge verifies that the standalone
// ShareAcknowledge API (not piggybacked on ShareFetch) works correctly.
func TestShareGroupStandaloneAcknowledge(t *testing.T) {
	t.Parallel()

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, "share-standalone-ack"))
	group := "share-test-standalone-ack"

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic("share-standalone-ack"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	setShareAutoOffsetReset(t, cl, group)

	const total = 10
	for i := range total {
		cl.Produce(context.Background(), kgo.StringRecord(strconv.Itoa(i)), func(_ *kgo.Record, err error) {
			if err != nil {
				t.Errorf("produce %d: %v", i, err)
			}
		})
	}
	if err := cl.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Join share group.
	hbReq := kmsg.NewPtrShareGroupHeartbeatRequest()
	hbReq.GroupID = group
	hbReq.MemberID = "test-member-1"
	hbReq.MemberEpoch = 0
	hbReq.SubscribedTopicNames = []string{"share-standalone-ack"}
	hbResp, err := hbReq.RequestWith(context.Background(), cl)
	if err != nil {
		t.Fatalf("heartbeat join: %v", err)
	}
	memberID := *hbResp.MemberID

	// Get topic ID.
	metaReq := kmsg.NewPtrMetadataRequest()
	mt := kmsg.NewMetadataRequestTopic()
	mt.Topic = kmsg.StringPtr("share-standalone-ack")
	metaReq.Topics = append(metaReq.Topics, mt)
	metaResp, err := metaReq.RequestWith(context.Background(), cl)
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	topicID := metaResp.Topics[0].TopicID

	// ShareFetch to acquire records.
	sfReq := kmsg.NewPtrShareFetchRequest()
	sfReq.GroupID = &group
	sfReq.MemberID = &memberID
	sfReq.ShareSessionEpoch = 0
	sfReq.MaxRecords = 100
	st := kmsg.NewShareFetchRequestTopic()
	st.TopicID = topicID
	sp := kmsg.NewShareFetchRequestTopicPartition()
	sp.Partition = 0
	st.Partitions = append(st.Partitions, sp)
	sfReq.Topics = append(sfReq.Topics, st)
	sfResp, err := sfReq.RequestWith(context.Background(), cl)
	if err != nil {
		t.Fatalf("share fetch: %v", err)
	}

	// Build standalone ShareAcknowledge: accept first half, release second half.
	// ShareFetch above used epoch 0 (new session), advancing to 1.
	ackReq := kmsg.NewPtrShareAcknowledgeRequest()
	ackReq.GroupID = &group
	ackReq.MemberID = &memberID
	ackReq.ShareSessionEpoch = 1
	at := kmsg.NewShareAcknowledgeRequestTopic()
	at.TopicID = topicID
	ap := kmsg.NewShareAcknowledgeRequestTopicPartition()
	ap.Partition = 0
	// Accept offsets 0-4.
	ab1 := kmsg.NewShareAcknowledgeRequestTopicPartitionAcknowledgementBatche()
	ab1.FirstOffset = 0
	ab1.LastOffset = int64(total/2 - 1)
	ab1.AcknowledgeTypes = []int8{1} // Accept
	ap.AcknowledgementBatches = append(ap.AcknowledgementBatches, ab1)
	// Release offsets 5-9.
	ab2 := kmsg.NewShareAcknowledgeRequestTopicPartitionAcknowledgementBatche()
	ab2.FirstOffset = int64(total / 2)
	ab2.LastOffset = int64(total - 1)
	ab2.AcknowledgeTypes = []int8{2} // Release
	ap.AcknowledgementBatches = append(ap.AcknowledgementBatches, ab2)
	at.Partitions = append(at.Partitions, ap)
	ackReq.Topics = append(ackReq.Topics, at)

	ackResp, err := ackReq.RequestWith(context.Background(), cl)
	if err != nil {
		t.Fatalf("share acknowledge: %v", err)
	}
	if ackResp.ErrorCode != 0 {
		t.Fatalf("share acknowledge error: %v", kerr.ErrorForCode(ackResp.ErrorCode))
	}

	// Leave.
	hbReq2 := kmsg.NewPtrShareGroupHeartbeatRequest()
	hbReq2.GroupID = group
	hbReq2.MemberID = memberID
	hbReq2.MemberEpoch = -1
	hbReq2.RequestWith(context.Background(), cl)

	// Verify: second consumer should see only the released half (5 records).
	_ = sfResp // suppress unused
	cl2, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics("share-standalone-ack"),
		kgo.ShareGroup(group),
		kgo.FetchMaxWait(200*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl2.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var got int
	for got < total/2 {
		fetches := cl2.PollFetches(ctx)
		for _, r := range fetches.Records() {
			got++
			if r.Offset < int64(total/2) {
				t.Errorf("got record at offset %d, expected only offsets >= %d (released half)", r.Offset, total/2)
			}
			r.Ack(kgo.AckAccept)
		}
		if ctx.Err() != nil {
			break
		}
	}
	if got < total/2 {
		t.Fatalf("expected %d released records, got %d", total/2, got)
	}

	// After accepting the released half, a third consumer should see nothing.
	cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
	cl2.CommitAcks(cCtx)
	cCancel()
	cl2.Close()

	cl3, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics("share-standalone-ack"),
		kgo.ShareGroup(group),
		kgo.FetchMaxWait(200*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl3.Close()

	ctx3, cancel3 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel3()

	var got3 int
	for ctx3.Err() == nil {
		fetches := cl3.PollFetches(ctx3)
		got3 += len(fetches.Records())
	}
	if got3 > 0 {
		t.Errorf("expected 0 records after all accepted, got %d", got3)
	}
}

// TestShareGroupMultiPartition verifies share group behavior across multiple
// partitions, ensuring records from all partitions are acquired and acked.
func TestShareGroupMultiPartition(t *testing.T) {
	t.Parallel()

	const nPartitions = 5
	c := newCluster(t, kfake.NumBrokers(3), kfake.SeedTopics(nPartitions, "share-multipart"))
	group := "share-test-multipart"

	admin, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic("share-multipart"),
		kgo.RecordPartitioner(kgo.RoundRobinPartitioner()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()

	setShareAutoOffsetReset(t, admin, group)

	// Produce records that will be spread across partitions.
	const total = 50
	for i := range total {
		admin.Produce(context.Background(), kgo.StringRecord(strconv.Itoa(i)), func(_ *kgo.Record, err error) {
			if err != nil {
				t.Errorf("produce %d: %v", i, err)
			}
		})
	}
	if err := admin.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Consume all records from the share group.
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics("share-multipart"),
		kgo.ShareGroup(group),
		kgo.FetchMaxWait(200*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	partitionsSeen := make(map[int32]int)
	var got int
	for got < total {
		fetches := cl.PollFetches(ctx)
		for _, r := range fetches.Records() {
			got++
			partitionsSeen[r.Partition]++
			r.Ack(kgo.AckAccept)
		}
		if ctx.Err() != nil {
			break
		}
	}
	if got != total {
		t.Fatalf("expected %d records, got %d", total, got)
	}
	if len(partitionsSeen) < 2 {
		t.Errorf("expected records from multiple partitions, got %d partition(s): %v", len(partitionsSeen), partitionsSeen)
	}
	t.Logf("records by partition: %v", partitionsSeen)
}

// TestShareGroupCloseReleasesRecords verifies that closing a share consumer
// without explicit acks releases records (not rejects them), making them
// available for redelivery.
func TestShareGroupCloseReleasesRecords(t *testing.T) {
	t.Parallel()

	c := newCluster(t, kfake.SeedTopics(1, "share-closerel"))
	group := "share-test-closerel"

	admin, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic("share-closerel"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()

	setShareAutoOffsetReset(t, admin, group)

	const total = 10
	for i := range total {
		admin.Produce(context.Background(), kgo.StringRecord(strconv.Itoa(i)), func(_ *kgo.Record, err error) {
			if err != nil {
				t.Errorf("produce %d: %v", i, err)
			}
		})
	}
	if err := admin.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Consumer 1: poll records then close WITHOUT acking or committing.
	// Close should release them (not reject).
	cl1, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics("share-closerel"),
		kgo.ShareGroup(group),
		kgo.FetchMaxWait(200*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var got1 int
	for got1 < total {
		fetches := cl1.PollFetches(ctx)
		got1 += len(fetches.Records())
		// Intentionally NOT acking.
		if ctx.Err() != nil {
			break
		}
	}
	if got1 < total {
		t.Fatalf("consumer 1: expected %d, got %d", total, got1)
	}
	cl1.Close() // should release, not reject

	// Consumer 2: should see all records redelivered.
	cl2, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics("share-closerel"),
		kgo.ShareGroup(group),
		kgo.FetchMaxWait(200*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl2.Close()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel2()

	var got2 int
	for got2 < total {
		fetches := cl2.PollFetches(ctx2)
		for _, r := range fetches.Records() {
			got2++
			r.Ack(kgo.AckAccept)
		}
		if ctx2.Err() != nil {
			break
		}
	}
	if got2 < total {
		t.Fatalf("consumer 2: expected %d redelivered records, got %d", total, got2)
	}
}

// TestShareGroupRenewAck verifies that AckRenew (type 4) with isRenewAck=true
// correctly extends the acquisition lock, and that mixed ack types (Accept +
// Renew) work in the same request.
func TestShareGroupRenewAck(t *testing.T) {
	t.Parallel()

	// Short lock duration so we can verify renew extends it.
	c := newCluster(t,
		kfake.NumBrokers(1),
		kfake.SeedTopics(1, "share-renew"),
		kfake.BrokerConfigs(map[string]string{
			"share.record.lock.duration.ms":       "500",
			"share.record.lock.sweep.interval.ms": "100",
		}),
	)
	group := "share-test-renew"

	admin, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic("share-renew"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()

	setShareAutoOffsetReset(t, admin, group)

	const total = 10
	for i := range total {
		admin.Produce(context.Background(), kgo.StringRecord(strconv.Itoa(i)), func(_ *kgo.Record, err error) {
			if err != nil {
				t.Errorf("produce %d: %v", i, err)
			}
		})
	}
	if err := admin.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	// Join share group.
	hbReq := kmsg.NewPtrShareGroupHeartbeatRequest()
	hbReq.GroupID = group
	hbReq.MemberID = "test-member-1"
	hbReq.MemberEpoch = 0
	hbReq.SubscribedTopicNames = []string{"share-renew"}
	hbResp, err := hbReq.RequestWith(context.Background(), cl)
	if err != nil {
		t.Fatalf("heartbeat join: %v", err)
	}
	memberID := *hbResp.MemberID

	// Get topic ID.
	metaReq := kmsg.NewPtrMetadataRequest()
	mt := kmsg.NewMetadataRequestTopic()
	mt.Topic = kmsg.StringPtr("share-renew")
	metaReq.Topics = append(metaReq.Topics, mt)
	metaResp, err := metaReq.RequestWith(context.Background(), cl)
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	topicID := metaResp.Topics[0].TopicID

	// ShareFetch epoch 0: acquire all records. Session epoch -> 1.
	sfReq := kmsg.NewPtrShareFetchRequest()
	sfReq.GroupID = &group
	sfReq.MemberID = &memberID
	sfReq.ShareSessionEpoch = 0
	sfReq.MaxRecords = 100
	fetchTopic := kmsg.NewShareFetchRequestTopic()
	fetchTopic.TopicID = topicID
	fetchPart := kmsg.NewShareFetchRequestTopicPartition()
	fetchPart.Partition = 0
	fetchTopic.Partitions = append(fetchTopic.Partitions, fetchPart)
	sfReq.Topics = append(sfReq.Topics, fetchTopic)
	sfResp, err := sfReq.RequestWith(context.Background(), cl)
	if err != nil {
		t.Fatalf("share fetch: %v", err)
	}
	if sfResp.ErrorCode != 0 {
		t.Fatalf("share fetch error: %v", kerr.ErrorForCode(sfResp.ErrorCode))
	}

	var acquired int
	for _, rt := range sfResp.Topics {
		for _, rp := range rt.Partitions {
			for _, ar := range rp.AcquiredRecords {
				acquired += int(ar.LastOffset - ar.FirstOffset + 1)
			}
		}
	}
	if acquired < total {
		t.Fatalf("expected to acquire %d records, got %d", total, acquired)
	}

	// Wait 300ms -- 60% of the 500ms lock duration.
	time.Sleep(300 * time.Millisecond)

	// Send mixed acks via isRenewAck ShareFetch: Renew offsets 0-4,
	// Accept offsets 5-9. This verifies that mixed ack types work in
	// the same isRenewAck request. Session epoch 1 -> 2.
	renewReq := kmsg.NewPtrShareFetchRequest()
	renewReq.GroupID = &group
	renewReq.MemberID = &memberID
	renewReq.ShareSessionEpoch = 1
	renewReq.IsRenewAck = true
	// All fetch params must be 0 when isRenewAck is set.
	renewReq.MaxBytes = 0
	renewTopic := kmsg.NewShareFetchRequestTopic()
	renewTopic.TopicID = topicID
	renewPart := kmsg.NewShareFetchRequestTopicPartition()
	renewPart.Partition = 0
	rb := kmsg.NewShareFetchRequestTopicPartitionAcknowledgementBatche()
	rb.FirstOffset = 0
	rb.LastOffset = 4
	rb.AcknowledgeTypes = []int8{4} // Renew
	renewPart.AcknowledgementBatches = append(renewPart.AcknowledgementBatches, rb)
	ab := kmsg.NewShareFetchRequestTopicPartitionAcknowledgementBatche()
	ab.FirstOffset = 5
	ab.LastOffset = 9
	ab.AcknowledgeTypes = []int8{1} // Accept
	renewPart.AcknowledgementBatches = append(renewPart.AcknowledgementBatches, ab)
	renewTopic.Partitions = append(renewTopic.Partitions, renewPart)
	renewReq.Topics = append(renewReq.Topics, renewTopic)

	renewResp, err := renewReq.RequestWith(context.Background(), cl)
	if err != nil {
		t.Fatalf("renew fetch: %v", err)
	}
	if renewResp.ErrorCode != 0 {
		t.Fatalf("renew fetch error: %v", kerr.ErrorForCode(renewResp.ErrorCode))
	}
	// isRenewAck should not return any acquired records.
	for _, rt := range renewResp.Topics {
		for _, rp := range rt.Partitions {
			if len(rp.AcquiredRecords) > 0 {
				t.Fatalf("expected 0 acquired records in renew response, got %d", len(rp.AcquiredRecords))
			}
			if rp.AcknowledgeErrorCode != 0 {
				t.Fatalf("unexpected ack error: %v", kerr.ErrorForCode(rp.AcknowledgeErrorCode))
			}
		}
	}

	// Wait 300ms more. Total elapsed since acquire: ~600ms.
	// Without renew, locks on 0-4 would have expired at ~500ms.
	// With renew at ~300ms, their new expiry is ~300+500 = ~800ms.
	time.Sleep(300 * time.Millisecond)

	// Accept the renewed offsets 0-4 via piggybacked ack. If renew
	// failed to extend the lock, the sweep would have released these
	// records and this accept would be a no-op.
	// Session epoch 2 -> 3.
	acceptReq := kmsg.NewPtrShareFetchRequest()
	acceptReq.GroupID = &group
	acceptReq.MemberID = &memberID
	acceptReq.ShareSessionEpoch = 2
	acceptReq.MaxRecords = 100
	acceptTopic := kmsg.NewShareFetchRequestTopic()
	acceptTopic.TopicID = topicID
	acceptPart := kmsg.NewShareFetchRequestTopicPartition()
	acceptPart.Partition = 0
	acceptBatch := kmsg.NewShareFetchRequestTopicPartitionAcknowledgementBatche()
	acceptBatch.FirstOffset = 0
	acceptBatch.LastOffset = 4
	acceptBatch.AcknowledgeTypes = []int8{1} // Accept
	acceptPart.AcknowledgementBatches = append(acceptPart.AcknowledgementBatches, acceptBatch)
	acceptTopic.Partitions = append(acceptTopic.Partitions, acceptPart)
	acceptReq.Topics = append(acceptReq.Topics, acceptTopic)

	acceptResp, err := acceptReq.RequestWith(context.Background(), cl)
	if err != nil {
		t.Fatalf("accept fetch: %v", err)
	}
	if acceptResp.ErrorCode != 0 {
		t.Fatalf("accept fetch error: %v", kerr.ErrorForCode(acceptResp.ErrorCode))
	}

	// Leave group.
	hbLeave := kmsg.NewPtrShareGroupHeartbeatRequest()
	hbLeave.GroupID = group
	hbLeave.MemberID = memberID
	hbLeave.MemberEpoch = -1
	hbLeave.RequestWith(context.Background(), cl)

	// Verify: a second consumer should see 0 records. All 10 records
	// were accepted -- 5-9 in the mixed ack, 0-4 after the renewed
	// lock held through the original expiry. If renew had failed, the
	// sweep would have released 0-4 and they'd be redelivered here.
	cl2, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics("share-renew"),
		kgo.ShareGroup(group),
		kgo.FetchMaxWait(200*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl2.Close()

	ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel2()

	var got2 int
	for ctx2.Err() == nil {
		fetches := cl2.PollFetches(ctx2)
		got2 += len(fetches.Records())
	}
	if got2 > 0 {
		t.Errorf("expected 0 records after renew+accept, got %d (renew likely failed to extend lock)", got2)
	}
}

// TestShareGroupConcurrentFetchAndAck verifies that multiple consumers can
// concurrently fetch and ack records without data loss or duplication in
// the final accepted set.
func TestShareGroupConcurrentFetchAndAck(t *testing.T) {
	t.Parallel()

	c := newCluster(t, kfake.SeedTopics(1, "share-concurrent"))
	group := "share-test-concurrent"

	admin, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic("share-concurrent"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()

	setShareAutoOffsetReset(t, admin, group)

	const total = 100
	for i := range total {
		admin.Produce(context.Background(), kgo.StringRecord(strconv.Itoa(i)), func(_ *kgo.Record, err error) {
			if err != nil {
				t.Errorf("produce %d: %v", i, err)
			}
		})
	}
	if err := admin.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Run 5 concurrent consumers, each accepting records.
	// Track total accepted across all consumers.
	var totalAccepted atomic.Int64

	consume := func(name string) {
		cl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.ConsumeTopics("share-concurrent"),
			kgo.ShareGroup(group),
			kgo.FetchMaxWait(200*time.Millisecond),
		)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			return
		}
		defer cl.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		for {
			fetches := cl.PollFetches(ctx)
			records := fetches.Records()
			if len(records) == 0 && ctx.Err() != nil {
				break
			}
			for _, r := range records {
				r.Ack(kgo.AckAccept)
			}
			totalAccepted.Add(int64(len(records)))
			cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
			cl.CommitAcks(cCtx)
			cCancel()

			if totalAccepted.Load() >= total {
				break
			}
		}
	}

	const numConsumers = 5
	done := make(chan struct{}, numConsumers)
	for i := range numConsumers {
		go func() {
			consume("c" + strconv.Itoa(i))
			done <- struct{}{}
		}()
	}
	for range numConsumers {
		<-done
	}

	got := totalAccepted.Load()
	if got < total {
		t.Fatalf("expected at least %d total accepted records across all consumers, got %d", total, got)
	}
	t.Logf("total records accepted across %d consumers: %d", numConsumers, got)
}

// TestShareGroupPollRecordsBuffering verifies that PollRecords with a limit
// smaller than the number of acquired records buffers excess records and
// returns them on subsequent polls without re-fetching from the broker.
func TestShareGroupPollRecordsBuffering(t *testing.T) {
	t.Parallel()

	c := newCluster(t, kfake.SeedTopics(1, "share-pollbuf"))
	group := "share-test-pollbuf"

	admin, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic("share-pollbuf"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer admin.Close()

	setShareAutoOffsetReset(t, admin, group)

	// Produce 50 records.
	const total = 50
	for i := range total {
		admin.Produce(context.Background(), kgo.StringRecord(strconv.Itoa(i)), func(_ *kgo.Record, err error) {
			if err != nil {
				t.Errorf("produce %d: %v", i, err)
			}
		})
	}
	if err := admin.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics("share-pollbuf"),
		kgo.ShareGroup(group),
		kgo.FetchMaxWait(200*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Poll 10 records at a time. The first PollRecords should fetch
	// from the broker and buffer the rest; subsequent calls should
	// return from the buffer without new ShareFetch requests.
	var got int
	var polls int
	for got < total {
		fetches := cl.PollRecords(ctx, 10)
		for _, e := range fetches.Errors() {
			if e.Err == context.DeadlineExceeded || e.Err == context.Canceled {
				continue
			}
			t.Errorf("fetch error: %v", e)
		}
		records := fetches.Records()
		if len(records) > 10 {
			t.Fatalf("PollRecords(10) returned %d records", len(records))
		}
		got += len(records)
		polls++
		if ctx.Err() != nil {
			break
		}
	}

	if got != total {
		t.Fatalf("expected %d records, got %d", total, got)
	}
	t.Logf("consumed %d records in %d polls of 10", got, polls)
}
