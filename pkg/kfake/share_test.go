package kfake

import (
	"context"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
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

	c := newCluster(t, SeedTopics(1, "share-basic"))
	group := "share-test-basic"

	const total = 50
	produceShareN(t, c, "share-basic", group, total)

	// Share group consumer.
	cl := newShareConsumer(t, c, "share-basic", group)

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

	c := newCluster(t, SeedTopics(1, "share-ack"))
	group := "share-test-ack"

	admin := newPlainClient(t, c, kgo.DefaultProduceTopic("share-ack"))

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
	cl1 := newShareConsumer(t, c, "share-ack", group)

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
	if _, err := cl1.CommitAcks(commitCtx); err != nil {
		t.Fatalf("commit acks: %v", err)
	}
	commitCancel()
	cl1.Close()

	t.Logf("consumer 1: got %d, released %d", got1, released)

	// Consumer 2: should see released records redelivered.
	cl2 := newShareConsumer(t, c, "share-ack", group)

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

	c := newCluster(t, SeedTopics(1, "share-reject"))
	group := "share-test-reject"

	const total = 20
	produceShareN(t, c, "share-reject", group, total)

	// Consumer 1: reject all records.
	cl1 := newShareConsumer(t, c, "share-reject", group)

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
		if _, err := cl1.CommitAcks(commitCtx); err != nil {
			t.Fatal(err)
		}
		commitCancel()
		if ctx.Err() != nil {
			break
		}
	}
	cl1.Close()
	t.Logf("consumer 1: rejected %d records", rejected)

	// Consumer 2: should see no records since all were rejected.
	cl2 := newShareConsumer(t, c, "share-reject", group)
	verifyZeroRecords(t, cl2, 500*time.Millisecond)
}

// TestShareGroupMaxDeliveryCount verifies that records are archived after
// reaching the max delivery attempt count. After archival, a second consumer
// should not see those records.
func TestShareGroupMaxDeliveryCount(t *testing.T) {
	t.Parallel()

	// Set max delivery attempts to 2 so records are archived after 2 releases.
	c := newCluster(t,
		SeedTopics(1, "share-maxdlv"),
		BrokerConfigs(map[string]string{
			"group.share.delivery.count.limit": "2",
		}),
	)
	group := "share-test-maxdlv"

	const total = 5
	produceShareN(t, c, "share-maxdlv", group, total)

	// Consumer 1: fetch all 5 records, release all of them.
	// This is delivery attempt 1.
	cl1 := newShareConsumer(t, c, "share-maxdlv", group)

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
	if _, err := cl1.CommitAcks(cCtx); err != nil {
		t.Fatal(err)
	}
	cCancel()
	cl1.Close()

	// Consumer 2: fetch records again (delivery attempt 2), release again.
	cl2 := newShareConsumer(t, c, "share-maxdlv", group)

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
	if _, err := cl2.CommitAcks(cCtx); err != nil {
		t.Fatal(err)
	}
	cCancel()
	cl2.Close()

	// Consumer 3: should see no records since all were archived (max delivery reached).
	cl3 := newShareConsumer(t, c, "share-maxdlv", group)
	verifyZeroRecords(t, cl3, 500*time.Millisecond)
}

// TestShareGroupAutoAckOnPoll verifies that records not explicitly acked
// before the next PollFetches are auto-accepted.
func TestShareGroupAutoAckOnPoll(t *testing.T) {
	t.Parallel()

	c := newCluster(t, SeedTopics(1, "share-autoack"))
	group := "share-test-autoack"

	const total = 10
	produceShareN(t, c, "share-autoack", group, total)

	// Consumer 1: poll all records but do NOT ack them. The next poll
	// (which will return nothing new) should auto-accept them.
	cl1 := newShareConsumer(t, c, "share-autoack", group)

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
	if _, err := cl1.CommitAcks(cCtx); err != nil {
		t.Fatalf("commit acks: %v", err)
	}
	cCancel()
	cl1.Close()

	// Consumer 2: should see no records since all were auto-accepted.
	cl2 := newShareConsumer(t, c, "share-autoack", group)
	verifyZeroRecords(t, cl2, 500*time.Millisecond)
}

// TestShareGroupAcquisitionLockExpiry verifies that records whose acquisition
// lock expires are released by the sweep timer and redelivered to another
// consumer, without the first consumer explicitly releasing or leaving.
func TestShareGroupAcquisitionLockExpiry(t *testing.T) {
	t.Parallel()

	// Short lock duration and sweep interval so the test runs quickly.
	// Single broker so raw ShareFetch goes to the partition leader.
	c := newCluster(t,
		NumBrokers(1),
		SeedTopics(1, "share-lockexp"),
		BrokerConfigs(map[string]string{
			"group.share.record.lock.duration.ms": "100",
			"share.record.lock.sweep.interval.ms": "100",
		}),
	)
	group := "share-test-lockexp"

	const total = 5
	produceShareN(t, c, "share-lockexp", group, total)

	// Member 1: join and acquire via raw ShareFetch, then do NOT ack
	// and do NOT leave. The acquisition lock should expire and the
	// sweep timer should release the records.
	cl1 := newPlainClient(t, c)
	memberID1, topicID := joinShareGroupRaw(t, cl1, group, "share-lockexp")

	_, acquired := rawShareFetch(t, cl1, group, memberID1, topicID, 0)
	if acquired < total {
		t.Fatalf("expected to acquire %d records, got %d", total, acquired)
	}

	// Do NOT ack, do NOT leave. Wait for lock expiry + sweep.
	time.Sleep(300 * time.Millisecond)

	// Member 2 (via kgo share consumer): should see the records that were
	// released by the expired acquisition lock sweep.
	cl2 := newShareConsumer(t, c, "share-lockexp", group)

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

	c := newCluster(t, NumBrokers(1), SeedTopics(1, "share-epoch"))
	group := "share-test-epoch"

	cl := newPlainClient(t, c, kgo.RetryTimeout(0))

	setShareAutoOffsetReset(t, cl, group)

	// Join the share group.
	memberID, topicID := joinShareGroupRaw(t, cl, group, "share-epoch")

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
		NumBrokers(1),
		SeedTopics(1, "share-sessexp"),
		BrokerConfigs(map[string]string{
			"group.share.session.timeout.ms": "500",
		}),
	)
	group := "share-test-sessexp"

	const total = 5
	produceShareN(t, c, "share-sessexp", group, total)

	// Member 1: join, fetch records, then stop heartbeating.
	cl1 := newPlainClient(t, c)
	memberID1, topicID := joinShareGroupRaw(t, cl1, group, "share-sessexp")

	// Fetch via raw ShareFetch to acquire without auto-acking.
	_, acquired := rawShareFetch(t, cl1, group, memberID1, topicID, 0)
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
	hbReq2.MemberEpoch = 1
	hbResp2, err := hbReq2.RequestWith(context.Background(), cl1)
	if err != nil {
		t.Fatalf("heartbeat after timeout: %v", err)
	}
	if hbResp2.ErrorCode != kerr.UnknownMemberID.Code {
		t.Fatalf("expected UNKNOWN_MEMBER_ID after session timeout, got %v", kerr.ErrorForCode(hbResp2.ErrorCode))
	}

	// Consumer 2: should pick up released records (fencing releases them).
	cl2 := newShareConsumer(t, c, "share-sessexp", group)

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

	c := newCluster(t, NumBrokers(1), SeedTopics(1, "share-standalone-ack"))
	group := "share-test-standalone-ack"

	const total = 10
	produceShareN(t, c, "share-standalone-ack", group, total)

	// Join share group.
	cl := newPlainClient(t, c)
	memberID, topicID := joinShareGroupRaw(t, cl, group, "share-standalone-ack")

	// ShareFetch to acquire records.
	_, acquired := rawShareFetch(t, cl, group, memberID, topicID, 0)
	if acquired != total {
		t.Fatalf("expected %d acquired records, got %d", total, acquired)
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
	cl2 := newShareConsumer(t, c, "share-standalone-ack", group)

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
	if _, err := cl2.CommitAcks(cCtx); err != nil {
		t.Fatal(err)
	}
	cCancel()
	cl2.Close()

	cl3 := newShareConsumer(t, c, "share-standalone-ack", group)
	verifyZeroRecords(t, cl3, 500*time.Millisecond)
}

// TestShareGroupMultiPartition verifies share group behavior across multiple
// partitions, ensuring records from all partitions are acquired and acked.
func TestShareGroupMultiPartition(t *testing.T) {
	t.Parallel()

	const nPartitions = 5
	c := newCluster(t, NumBrokers(3), SeedTopics(nPartitions, "share-multipart"))
	group := "share-test-multipart"

	admin := newPlainClient(t, c,
		kgo.DefaultProduceTopic("share-multipart"),
		kgo.RecordPartitioner(kgo.RoundRobinPartitioner()),
	)

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
	cl := newShareConsumer(t, c, "share-multipart", group)

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

// TestShareGroupCloseReleasesRecords verifies that closing a share session
// (ShareFetch epoch -1) without acking releases acquired records for
// redelivery. Uses raw protocol for consumer 1 to bypass the kgo client's
// auto-accept-on-close behavior and test pure server-side release.
func TestShareGroupCloseReleasesRecords(t *testing.T) {
	t.Parallel()

	c := newCluster(t, NumBrokers(1), SeedTopics(1, "share-closerel"))
	group := "share-test-closerel"

	const total = 10
	produceShareN(t, c, "share-closerel", group, total)

	// Consumer 1 (raw): join, acquire records, then close session
	// WITHOUT acking. The server should release them.
	cl1 := newPlainClient(t, c)
	memberID, topicID := joinShareGroupRaw(t, cl1, group, "share-closerel")

	// ShareFetch epoch 0: acquire all records. Session epoch -> 1.
	_, acquired := rawShareFetch(t, cl1, group, memberID, topicID, 0)
	if acquired < total {
		t.Fatalf("consumer 1: expected to acquire %d, got %d", total, acquired)
	}

	// Close session (epoch -1) without sending any acks.
	closeReq := kmsg.NewPtrShareFetchRequest()
	closeReq.GroupID = &group
	closeReq.MemberID = &memberID
	closeReq.ShareSessionEpoch = -1
	closeTopic := kmsg.NewShareFetchRequestTopic()
	closeTopic.TopicID = topicID
	closeReq.Topics = append(closeReq.Topics, closeTopic)
	_, err := closeReq.RequestWith(context.Background(), cl1)
	if err != nil {
		t.Fatalf("session close: %v", err)
	}

	// Consumer 2: should see all records redelivered.
	cl2 := newShareConsumer(t, c, "share-closerel", group)

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
		NumBrokers(1),
		SeedTopics(1, "share-renew"),
		BrokerConfigs(map[string]string{
			"group.share.record.lock.duration.ms": "150",
			"share.record.lock.sweep.interval.ms": "50",
		}),
	)
	group := "share-test-renew"

	const total = 10
	produceShareN(t, c, "share-renew", group, total)

	cl := newPlainClient(t, c)

	// Join share group.
	memberID, topicID := joinShareGroupRaw(t, cl, group, "share-renew")

	// ShareFetch epoch 0: acquire all records. Session epoch -> 1.
	_, acquired := rawShareFetch(t, cl, group, memberID, topicID, 0)
	if acquired < total {
		t.Fatalf("expected to acquire %d records, got %d", total, acquired)
	}

	// Wait 90ms -- 60% of the 150ms lock duration.
	time.Sleep(90 * time.Millisecond)

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

	// Wait 90ms more. Total elapsed since acquire: ~180ms.
	// Without renew, locks on 0-4 would have expired at ~150ms.
	// With renew at ~90ms, their new expiry is ~90+150 = ~240ms.
	time.Sleep(90 * time.Millisecond)

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
	cl2 := newShareConsumer(t, c, "share-renew", group)
	verifyZeroRecords(t, cl2, 500*time.Millisecond)
}

// TestShareGroupConcurrentFetchAndAck verifies that multiple consumers can
// concurrently fetch and ack records without data loss or duplication in
// the final accepted set.
func TestShareGroupConcurrentFetchAndAck(t *testing.T) {
	t.Parallel()

	c := newCluster(t, SeedTopics(1, "share-concurrent"))
	group := "share-test-concurrent"

	const total = 100
	produceShareN(t, c, "share-concurrent", group, total)

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
			if _, err := cl.CommitAcks(cCtx); err != nil {
				t.Errorf("commit acks: %v", err)
			}
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

	c := newCluster(t, SeedTopics(1, "share-pollbuf"))
	group := "share-test-pollbuf"

	const total = 50
	produceShareN(t, c, "share-pollbuf", group, total)

	cl := newShareConsumer(t, c, "share-pollbuf", group)

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

// TestShareGroupAsyncEarlyReturn verifies that when multiple sources
// exist and one returns records quickly, the poll returns immediately
// without waiting for the other sources' MaxWait to expire. Records
// from the slower sources are collected on the next poll.
func TestShareGroupAsyncEarlyReturn(t *testing.T) {
	t.Parallel()

	// 3 brokers, 3 partitions -- one partition per broker, so
	// each broker is a separate source.
	c := newCluster(t,
		NumBrokers(3),
		SeedTopics(3, "share-async-early"),
	)
	group := "share-test-async-early"

	admin := newPlainClient(t, c,
		kgo.DefaultProduceTopic("share-async-early"),
		kgo.RecordPartitioner(kgo.RoundRobinPartitioner()),
	)
	setShareAutoOffsetReset(t, admin, group)

	// Produce 30 records spread across all 3 partitions via
	// round-robin so each broker has data.
	for i := range 30 {
		r := kgo.StringRecord(strconv.Itoa(i))
		if err := admin.ProduceSync(context.Background(), r).FirstErr(); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}

	// Use a long MaxWait (2s) so the early return is clearly
	// visible in timing: if we waited for all sources, the poll
	// would take 2s. With early return, the first source to have
	// records returns immediately.
	cl := newPlainClient(t, c,
		kgo.ConsumeTopics("share-async-early"),
		kgo.ShareGroup(group),
		kgo.FetchMaxWait(2*time.Second),
	)
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var (
		got       int
		fastPolls int
	)
	for got < 30 && ctx.Err() == nil {
		start := time.Now()
		fetches := cl.PollFetches(ctx)
		elapsed := time.Since(start)
		recs := fetches.Records()
		got += len(recs)
		for _, r := range recs {
			r.Ack(kgo.AckAccept)
		}
		// A poll that returns records in under 1s (well below
		// the 2s MaxWait) demonstrates early return.
		if len(recs) > 0 && elapsed < time.Second {
			fastPolls++
		}
	}
	if got < 30 {
		t.Fatalf("expected 30 records, got %d", got)
	}
	if fastPolls == 0 {
		t.Fatal("no fast polls observed -- early return may not be working")
	}
	t.Logf("consumed %d records, %d fast polls (< 1s with 2s MaxWait)", got, fastPolls)
}

// TestShareGroupAsyncNoSpin verifies that when all sources have
// goroutines still running from a previous poll round, the poll
// blocks on the results channel rather than spin-returning empty.
// Measured by counting empty polls: a tight spin would produce
// thousands, while correct blocking produces very few.
func TestShareGroupAsyncNoSpin(t *testing.T) {
	t.Parallel()

	c := newCluster(t, SeedTopics(1, "share-nospin"))
	group := "share-test-nospin"

	produceShareN(t, c, "share-nospin", group, 10)

	cl := newShareConsumer(t, c, "share-nospin", group,
		kgo.FetchMaxWait(500*time.Millisecond),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	var (
		got        int
		emptyPolls int
	)
	for got < 10 && ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		recs := fetches.Records()
		if len(recs) == 0 {
			emptyPolls++
		} else {
			got += len(recs)
			for _, r := range recs {
				r.Ack(kgo.AckAccept)
			}
		}
	}
	if got < 10 {
		t.Fatalf("expected 10 records, got %d", got)
	}
	// With 500ms MaxWait and 5s timeout, blocking behavior gives
	// at most ~10 empty polls. A spin loop would give thousands.
	if emptyPolls > 50 {
		t.Fatalf("too many empty polls (%d) -- possible spin loop", emptyPolls)
	}
	t.Logf("consumed %d records, %d empty polls", got, emptyPolls)
}

// TestShareGroupAsyncShutdownNoHang verifies that closing a share
// consumer does not hang even when fetch goroutines are in-flight.
// This is the deadlock regression test: if goroutines block on a
// full fetchResults channel during shutdown, fetchWg.Wait hangs.
func TestShareGroupAsyncShutdownNoHang(t *testing.T) {
	t.Parallel()

	// 3 brokers to maximize concurrent goroutines.
	c := newCluster(t,
		NumBrokers(3),
		SeedTopics(3, "share-shutdown"),
	)
	group := "share-test-shutdown"

	produceShareN(t, c, "share-shutdown", group, 50)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics("share-shutdown"),
		kgo.ShareGroup(group),
		kgo.FetchMaxWait(5*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Poll once to establish sessions and start goroutines.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	cl.PollFetches(ctx)
	cancel()

	// Start a poll with a long timeout. Goroutines are now
	// in-flight waiting for broker MaxWait.
	go func() {
		longCtx, longCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer longCancel()
		cl.PollFetches(longCtx)
	}()
	time.Sleep(100 * time.Millisecond)

	// Close should complete within a few seconds, not hang.
	done := make(chan struct{})
	go func() {
		cl.Close()
		close(done)
	}()

	select {
	case <-done:
		// Success: Close returned promptly.
	case <-time.After(10 * time.Second):
		t.Fatal("cl.Close() hung for 10s -- probable deadlock in fetchWg.Wait")
	}
}

// TestShareGroupAsyncMultiSourceRecordIntegrity verifies that with
// multiple brokers, ALL partitions' records are eventually consumed
// even with the async early-return model. This catches regressions
// where early return starves slower sources.
func TestShareGroupAsyncMultiSourceRecordIntegrity(t *testing.T) {
	t.Parallel()

	// 3 brokers, 3 partitions, 1 per broker.
	c := newCluster(t,
		NumBrokers(3),
		SeedTopics(3, "share-integrity"),
	)
	group := "share-test-integrity"

	admin := newPlainClient(t, c,
		kgo.DefaultProduceTopic("share-integrity"),
		kgo.RecordPartitioner(kgo.RoundRobinPartitioner()),
	)

	setShareAutoOffsetReset(t, admin, group)

	const total = 90
	for i := range total {
		admin.Produce(
			context.Background(),
			kgo.StringRecord(strconv.Itoa(i)),
			func(_ *kgo.Record, err error) {
				if err != nil {
					t.Errorf("produce %d: %v", i, err)
				}
			},
		)
	}
	if err := admin.Flush(context.Background()); err != nil {
		t.Fatal(err)
	}

	cl := newShareConsumer(t, c, "share-integrity", group)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	partitions := make(map[int32]int)
	var got int
	for got < total && ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		for _, r := range fetches.Records() {
			got++
			partitions[r.Partition]++
			r.Ack(kgo.AckAccept)
		}
	}
	if got != total {
		t.Fatalf("expected %d records, got %d (by partition: %v)", total, got, partitions)
	}
	// With round-robin across 3 partitions, each should have ~30.
	if len(partitions) < 3 {
		t.Errorf("expected records from 3 partitions, got %d: %v", len(partitions), partitions)
	}
	t.Logf("records by partition: %v", partitions)
}
