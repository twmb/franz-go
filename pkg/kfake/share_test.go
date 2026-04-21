package kfake

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
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
	if err := cl1.FlushAcks(commitCtx); err != nil {
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
		if err := cl1.FlushAcks(commitCtx); err != nil {
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
// reaching the max delivery attempt count. Java broker semantics: any member
// can re-acquire records it just released (there is no member-ownership gate
// in acquire, see SharePartition.java:924 / :1942). With kgo's background
// prefetch, a single consumer that keeps polling will naturally re-acquire
// and re-release its own records until they hit the delivery count limit.
// We exploit that here: drain a single consumer until no more records
// arrive, and verify that each record was delivered exactly maxDelivery
// times before archival.
func TestShareGroupMaxDeliveryCount(t *testing.T) {
	t.Parallel()

	const maxDelivery = 2
	c := newCluster(t,
		SeedTopics(1, "share-maxdlv"),
		BrokerConfigs(map[string]string{
			"group.share.delivery.count.limit": strconv.Itoa(maxDelivery),
		}),
	)
	group := "share-test-maxdlv"

	const total = 5
	produceShareN(t, c, "share-maxdlv", group, total)

	cl := newShareConsumer(t, c, "share-maxdlv", group)

	// Drain until idle: each record can be delivered up to maxDelivery
	// times before archival. We stop when a poll returns nothing for
	// longer than a quiet period.
	perOffsetCount := make(map[int64]int32)
	deadline := time.Now().Add(10 * time.Second)
	quietDeadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		fetches := cl.PollFetches(ctx)
		cancel()
		recs := fetches.Records()
		if len(recs) == 0 {
			if time.Now().After(quietDeadline) {
				break
			}
			continue
		}
		quietDeadline = time.Now().Add(time.Second)
		for _, r := range recs {
			perOffsetCount[r.Offset]++
			if r.DeliveryCount() > maxDelivery {
				t.Errorf("offset %d delivery count %d exceeds max %d",
					r.Offset, r.DeliveryCount(), maxDelivery)
			}
			r.Ack(kgo.AckRelease)
		}
	}

	if len(perOffsetCount) != total {
		t.Errorf("expected %d unique offsets, got %d", total, len(perOffsetCount))
	}
	for off, n := range perOffsetCount {
		if n != maxDelivery {
			t.Errorf("offset %d delivered %d times, want %d", off, n, maxDelivery)
		}
	}

	cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := cl.FlushAcks(cCtx); err != nil {
		t.Fatal(err)
	}
	cCancel()
	cl.Close()

	// A fresh consumer should see nothing: all records are archived.
	cl2 := newShareConsumer(t, c, "share-maxdlv", group)
	verifyZeroRecords(t, cl2, 500*time.Millisecond)
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
	trigCtx, trigCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	cl1.PollFetches(trigCtx)
	trigCancel()

	// CommitAcks sends any pending acks -- including auto-accepted ones
	// that were routed to sources during finalizePreviousPoll.
	// But CommitAcks only sends records finalized by finalizeMarkedRecords,
	// NOT those already in source pending acks. The pending acks from
	// auto-accept will be piggybacked on the next ShareFetch or sent
	// on close. Force another poll + commit cycle to send them.
	cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := cl1.FlushAcks(cCtx); err != nil {
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
	ab1 := kmsg.NewShareAcknowledgeRequestTopicPartitionAcknowledgementBatch()
	ab1.FirstOffset = 0
	ab1.LastOffset = int64(total/2 - 1)
	ab1.AcknowledgeTypes = []int8{1} // Accept
	ap.AcknowledgementBatches = append(ap.AcknowledgementBatches, ab1)
	// Release offsets 5-9.
	ab2 := kmsg.NewShareAcknowledgeRequestTopicPartitionAcknowledgementBatch()
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
	if err := cl2.FlushAcks(cCtx); err != nil {
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
			"group.share.record.lock.duration.ms": "500",
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
	rb := kmsg.NewShareFetchRequestTopicPartitionAcknowledgementBatch()
	rb.FirstOffset = 0
	rb.LastOffset = 4
	rb.AcknowledgeTypes = []int8{4} // Renew
	renewPart.AcknowledgementBatches = append(renewPart.AcknowledgementBatches, rb)
	ab := kmsg.NewShareFetchRequestTopicPartitionAcknowledgementBatch()
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
	acceptBatch := kmsg.NewShareFetchRequestTopicPartitionAcknowledgementBatch()
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
	// A shared context is cancelled once all records are consumed
	// so idle consumers wake up promptly.
	var totalAccepted atomic.Int64
	sharedCtx, sharedCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer sharedCancel()

	consume := func(name string) {
		cl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.ConsumeTopics("share-concurrent"),
			kgo.ShareGroup(group),
		)
		if err != nil {
			t.Errorf("%s: %v", name, err)
			return
		}
		defer cl.Close()

		for {
			fetches := cl.PollFetches(sharedCtx)
			records := fetches.Records()
			if len(records) == 0 && sharedCtx.Err() != nil {
				break
			}
			for _, r := range records {
				r.Ack(kgo.AckAccept)
			}
			totalAccepted.Add(int64(len(records)))
			cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
			if err := cl.FlushAcks(cCtx); err != nil {
				t.Errorf("commit acks: %v", err)
			}
			cCancel()

			if totalAccepted.Load() >= total {
				sharedCancel()
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

	cl := newShareConsumer(t, c, "share-nospin", group)

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

// TestShareGroupMaxConcurrentFetches verifies that share consumers work
// correctly under each fetch-concurrency mode:
//   - MaxConcurrentFetches 0: strict pull (fetch only on poll)
//   - MaxConcurrentFetches 1: single buffered fetch
//   - MaxConcurrentFetches 2: two concurrent fetches
//   - ShareMaxRecordsStrict: convenience option that implicitly sets
//     MaxConcurrentFetches=0 and caps records-per-poll at ShareMaxRecords
//
// The "strict" subtest additionally asserts that no PollFetches returns
// more than ShareMaxRecords records -- the contract the option exists
// to provide.
func TestShareGroupMaxConcurrentFetches(t *testing.T) {
	t.Parallel()
	const maxPerStrictPoll int32 = 50
	cases := []struct {
		name       string
		opts       []kgo.Opt
		maxPerPoll int32 // >0 asserts each poll returns <= this
	}{
		{"0", []kgo.Opt{kgo.MaxConcurrentFetches(0)}, 0},
		{"1", []kgo.Opt{kgo.MaxConcurrentFetches(1)}, 0},
		{"2", []kgo.Opt{kgo.MaxConcurrentFetches(2)}, 0},
		{"strict", []kgo.Opt{kgo.ShareMaxRecords(maxPerStrictPoll), kgo.ShareMaxRecordsStrict()}, maxPerStrictPoll},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testShareGroupFetchLimits(t, tc.name, tc.opts, tc.maxPerPoll)
		})
	}
}

func testShareGroupFetchLimits(t *testing.T, name string, opts []kgo.Opt, maxPerPoll int32) {
	topic := "share-fetchlim-" + name
	group := "share-test-fetchlim-" + name
	const total = 1000

	c := newCluster(t, NumBrokers(2), SeedTopics(1, topic))
	produceShareN(t, c, topic, group, total)

	cl := newShareConsumer(t, c, topic, group, opts...)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	var got int
	var maxObservedPoll int32
	for got < total && ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		recs := fetches.Records()
		if n := int32(len(recs)); n > maxObservedPoll {
			maxObservedPoll = n
		}
		if maxPerPoll > 0 && int32(len(recs)) > maxPerPoll {
			t.Errorf("poll returned %d records, expected at most %d", len(recs), maxPerPoll)
		}
		got += len(recs)
		for _, r := range recs {
			r.Ack(kgo.AckAccept)
		}
	}
	if got != total {
		t.Fatalf("expected %d records, got %d (max-per-poll observed=%d)", total, got, maxObservedPoll)
	}
	if maxPerPoll > 0 && maxObservedPoll == 0 {
		t.Fatalf("no records observed in any poll")
	}
	flushCtx, flushCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := cl.FlushAcks(flushCtx); err != nil {
		t.Fatalf("FlushAcks: %v", err)
	}
	flushCancel()
}

// TestShareGroupAckRenew verifies that AckRenew extends the broker's
// acquisition lock past its normal timeout. The lock duration is set
// to 200ms; the test holds a record for ~600ms (3x the lock window)
// while renewing every 80ms, then accepts. A second consumer must not
// see the record redelivered -- without the renews, the broker would
// release the record after the 200ms lock expires and the sweep fires.
func TestShareGroupAckRenew(t *testing.T) {
	t.Parallel()

	c := newCluster(t,
		SeedTopics(1, "share-renew-ext"),
		BrokerConfigs(map[string]string{
			// Larger lock + self-paced renews (not ticker-paced) so
			// slow FlushAcks round-trips under parallel -race load
			// cannot cause two consecutive renews to straddle a lock
			// expiry.
			"group.share.record.lock.duration.ms": "500",
			"share.record.lock.sweep.interval.ms": "100",
		}),
	)
	group := "share-test-renew-ext"
	produceShareN(t, c, "share-renew-ext", group, 1)

	cl1 := newShareConsumer(t, c, "share-renew-ext", group)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Acquire the record.
	var acquired *kgo.Record
	for acquired == nil && ctx.Err() == nil {
		fetches := cl1.PollFetches(ctx)
		if recs := fetches.Records(); len(recs) > 0 {
			acquired = recs[0]
		}
	}
	if acquired == nil {
		t.Fatal("never acquired the record")
	}

	// Self-paced renew: after each FlushAcks returns, sleep 100ms
	// (well below the 500ms lock) then renew again. Runs for ~900ms
	// total so the held record crosses nearly two lock windows.
	// Sleeping after the round-trip (rather than ticker-paced) makes
	// the gap between renews bounded by sleep + FlushAcks latency,
	// not by ticker drift when FlushAcks stalls.
	renewCount := 0
	deadline := time.Now().Add(900 * time.Millisecond)
	for time.Now().Before(deadline) {
		acquired.Ack(kgo.AckRenew)
		renewCount++
		flushCtx, flushCancel := context.WithTimeout(ctx, 1*time.Second)
		if err := cl1.FlushAcks(flushCtx); err != nil {
			flushCancel()
			t.Fatalf("FlushAcks renew: %v", err)
		}
		flushCancel()
		if !time.Now().Before(deadline) {
			break
		}
		select {
		case <-time.After(100 * time.Millisecond):
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		}
	}
	// At least one renew must have fired: without it, the test isn't
	// exercising the renew path and the post-test no-redelivery check
	// is meaningless. The exact count is timing-dependent under -race
	// + parallel test load (each FlushAcks round-trip can take
	// hundreds of ms), so we don't assert a larger lower bound. The
	// true correctness check is the "no redelivery on cl2" below.
	if renewCount < 1 {
		t.Fatalf("expected >=1 renew, got %d", renewCount)
	}

	acquired.Ack(kgo.AckAccept)
	flushCtx, flushCancel := context.WithTimeout(ctx, 1*time.Second)
	if err := cl1.FlushAcks(flushCtx); err != nil {
		flushCancel()
		t.Fatalf("FlushAcks accept: %v", err)
	}
	flushCancel()
	cl1.Close()

	// Consumer 2 must not see the record redelivered.
	cl2 := newShareConsumer(t, c, "share-renew-ext", group)
	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer verifyCancel()
	var redelivered int
	for verifyCtx.Err() == nil {
		fetches := cl2.PollFetches(verifyCtx)
		redelivered += len(fetches.Records())
		for _, r := range fetches.Records() {
			r.Ack(kgo.AckAccept)
		}
	}
	if redelivered > 0 {
		t.Errorf("renewed-then-accepted record was redelivered %d time(s); renew likely failed", redelivered)
	}
}

// TestShareGroupRenewFlush verifies that r.Ack(AckRenew) followed by
// FlushAcks completes without timing out. The renew extends the lock;
// the next poll's auto-accept finalizes the record.
func TestShareGroupRenewFlush(t *testing.T) {
	t.Parallel()

	const total = 10
	c := newCluster(t, SeedTopics(1, "share-renew-flush"))
	group := "share-test-renew-flush"
	produceShareN(t, c, "share-renew-flush", group, total)

	cl := newShareConsumer(t, c, "share-renew-flush", group)

	// Poll records, renew all, flush.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var got int
	for got < total && ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		recs := fetches.Records()
		got += len(recs)
		for _, r := range recs {
			r.Ack(kgo.AckRenew)
		}
	}
	if got < total {
		t.Fatalf("expected %d records, got %d", total, got)
	}

	flushCtx, flushCancel := context.WithTimeout(context.Background(), 5*time.Second)
	if err := cl.FlushAcks(flushCtx); err != nil {
		t.Fatalf("FlushAcks after renew: %v", err)
	}
	flushCancel()

	// Poll again to trigger auto-accept, then flush.
	pollCtx, pollCancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	cl.PollFetches(pollCtx)
	pollCancel()

	flushCtx2, flushCancel2 := context.WithTimeout(context.Background(), 5*time.Second)
	if err := cl.FlushAcks(flushCtx2); err != nil {
		t.Fatalf("FlushAcks after auto-accept: %v", err)
	}
	flushCancel2()
}

// TestShareGroupClientRejoinAfterFence verifies the kgo share consumer
// transparently recovers from a broker-side fence. The session timeout
// (500ms) is shorter than the kgo client's 1s heartbeat-interval floor
// (consumer_share.go), so the broker fences the member between
// heartbeats; the next heartbeat returns UNKNOWN_MEMBER_ID and the
// client must reset to epoch 0 and rejoin without losing records.
//
// Per-record processing is slow enough that the run straddles multiple
// fence cycles. The test asserts (a) every record was eventually
// consumed (rejoin must work; a broken rejoin would strand records)
// and (b) the group's epoch bumped past the initial join, proving the
// fence/rejoin path was actually exercised rather than silently skipped.
func TestShareGroupClientRejoinAfterFence(t *testing.T) {
	t.Parallel()

	const topic = "share-rejoin"
	const group = "share-test-rejoin"
	// 100 records at 10ms each = ~1s of consumption, which spans one
	// fence+rejoin cycle (session-timeout 500ms < 1s heartbeat floor
	// means the fence fires ~500ms after each heartbeat; the next
	// heartbeat at ~1s detects the fence and the client rejoins).
	const total = 100

	c := newCluster(t,
		SeedTopics(1, topic),
		BrokerConfigs(map[string]string{
			// Broker suggests heartbeat every 100ms. The kgo share
			// client floors the heartbeat interval at 1s
			// (consumer_share.go), so it heartbeats every 1s in
			// practice. Session timeout 500ms is below that floor,
			// so the broker fences the member between heartbeats;
			// the next heartbeat returns UNKNOWN_MEMBER_ID and the
			// client must reset epoch and rejoin.
			"group.share.heartbeat.interval.ms": "100",
			"group.share.session.timeout.ms":    "500",
		}),
	)
	produceShareN(t, c, topic, group, total)

	admin := newPlainClient(t, c)
	defer admin.Close()

	cl := newShareConsumer(t, c, topic, group)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// produceShareN sets Value (not Key), so identify records by Value.
	seen := make(map[string]bool, total)
	var deliveries int
	for len(seen) < total && ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		for _, r := range fetches.Records() {
			deliveries++
			seen[string(r.Value)] = true
			// Slow per-record processing so the run straddles at
			// least one fence/rejoin cycle.
			time.Sleep(10 * time.Millisecond)
			r.Ack(kgo.AckAccept)
		}
	}
	if len(seen) < total {
		t.Fatalf("expected %d unique records, got %d (deliveries=%d)", total, len(seen), deliveries)
	}

	// Verify a fence actually happened: the member's epoch (tracked
	// by the broker and returned via ShareGroupDescribe) bumps on
	// each join. The first join sets epoch=1; every subsequent
	// rejoin after a fence bumps it again. An epoch > 1 at end of
	// test proves the fence/rejoin path was exercised.
	req := kmsg.NewPtrShareGroupDescribeRequest()
	req.GroupIDs = []string{group}
	resp, err := req.RequestWith(context.Background(), admin)
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	if len(resp.Groups) != 1 || len(resp.Groups[0].Members) == 0 {
		t.Fatalf("expected 1 group with >=1 member, got %+v", resp.Groups)
	}
	memberEpoch := resp.Groups[0].Members[0].MemberEpoch
	if memberEpoch <= 1 {
		t.Fatalf("expected member epoch > 1 (proof of rejoin), got %d; fence/rejoin cycle never fired or test ran too fast to trigger one",
			memberEpoch)
	}
	t.Logf("deliveries=%d, unique=%d, final member epoch=%d (rejoin cycles observed)",
		deliveries, len(seen), memberEpoch)
}

// TestShareGroupRebalanceOccurs verifies that the broker actually
// rebalances when membership changes AND that records flow correctly
// across each rebalance. Asserts both:
//
//  1. ShareGroupDescribe's GroupEpoch bumps when consumer 2 joins and
//     again when it leaves -- without this, a bug that silently broke
//     rebalance computation could pass "looks like it's working" tests.
//  2. Every produced record is consumed across the whole test, even
//     though rebalances fire mid-flow. A rebalance that drops acquired
//     records, double-counts acks, or strands a cursor on the old
//     leader would manifest as missing or extra records here.
//
// Consumer 1 processes slowly (per-record sleep) so consumer 2 has
// work to do when it joins, and so both consumers are actively
// consuming when the rebalance fires. A multi-partition topic is
// required so the broker can redistribute partitions between members.
func TestShareGroupRebalanceOccurs(t *testing.T) {
	t.Parallel()

	const topic = "share-rebal"
	const group = "share-test-rebal"
	const total = 150
	const partitions = 4
	// perRecordDelay stretches consumption so the rebalance fires
	// while records are still in flight. Each consumer processes at
	// this rate; with 150 records and two consumers for a portion of
	// the run, total wall-clock is ~1s under -race.
	const perRecordDelay = 2 * time.Millisecond

	c := newCluster(t, SeedTopics(int32(partitions), topic))
	produceShareN(t, c, topic, group, total)

	admin := newPlainClient(t, c)
	defer admin.Close()

	// describeEpoch returns (epoch, members). If the group has not
	// been created yet (GROUP_ID_NOT_FOUND -- happens briefly before
	// any consumer has heartbeated), it returns (0, 0) rather than
	// fatal-ing so the polling helper below can wait it out.
	describeEpoch := func() (epoch int32, members int) {
		req := kmsg.NewPtrShareGroupDescribeRequest()
		req.GroupIDs = []string{group}
		resp, err := req.RequestWith(context.Background(), admin)
		if err != nil {
			t.Fatalf("describe: %v", err)
		}
		if len(resp.Groups) != 1 {
			t.Fatalf("expected 1 group, got %d", len(resp.Groups))
		}
		g := resp.Groups[0]
		if errors.Is(kerr.ErrorForCode(g.ErrorCode), kerr.GroupIDNotFound) {
			return 0, 0
		}
		if err := kerr.ErrorForCode(g.ErrorCode); err != nil {
			t.Fatalf("describe error: %v", err)
		}
		return g.GroupEpoch, len(g.Members)
	}

	waitForMembers := func(want int) int32 {
		t.Helper()
		deadline := time.Now().Add(15 * time.Second)
		var lastE int32
		var lastM int
		for time.Now().Before(deadline) {
			lastE, lastM = describeEpoch()
			if lastM == want {
				return lastE
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("timeout waiting for %d members (last: epoch=%d, members=%d)", want, lastE, lastM)
		return 0
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	// seen is shared between both consumers; a record acquired twice
	// (e.g. released on one member's leave) appears in `deliveries`
	// but not in `seen`.
	var seenMu sync.Mutex
	seen := make(map[string]bool, total)
	var deliveries atomic.Int64

	// runConsumer polls + acks with a per-record delay so rebalances
	// happen while records are in flight. Runs until ctx expires OR
	// the shared `seen` covers all `total` keys.
	runConsumer := func(name string, cl *kgo.Client, perRecordDelay time.Duration) {
		for {
			seenMu.Lock()
			done := len(seen) >= total
			seenMu.Unlock()
			if done || ctx.Err() != nil {
				return
			}
			fetches := cl.PollFetches(ctx)
			for _, r := range fetches.Records() {
				deliveries.Add(1)
				seenMu.Lock()
				seen[string(r.Value)] = true
				seenMu.Unlock()
				time.Sleep(perRecordDelay)
				r.Ack(kgo.AckAccept)
			}
			_ = name
		}
	}

	// Consumer 1 starts alone.
	cl1 := newShareConsumer(t, c, topic, group)
	defer cl1.Close()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); runConsumer("cl1", cl1, perRecordDelay) }()
	e1 := waitForMembers(1)

	// Give consumer 1 time to consume some records so the rebalance
	// fires mid-flow rather than before any records are processed.
	for deliveries.Load() < int64(total/5) && ctx.Err() == nil {
		time.Sleep(20 * time.Millisecond)
	}
	if ctx.Err() != nil {
		t.Fatal("consumer 1 never consumed any records")
	}

	// Consumer 2 joins while consumer 1 is actively processing.
	cl2 := newShareConsumer(t, c, topic, group)
	wg.Add(1)
	go func() { defer wg.Done(); runConsumer("cl2", cl2, perRecordDelay) }()
	e2 := waitForMembers(2)
	if e2 <= e1 {
		t.Fatalf("expected epoch bump after consumer 2 joined (e1=%d, e2=%d)", e1, e2)
	}

	// Let the two-member phase run long enough that each consumer
	// processes a non-trivial share of the remaining records before
	// consumer 2 leaves. The rebalance on leave will release records
	// consumer 2 had acquired back to consumer 1.
	targetAfterJoin := deliveries.Load() + int64(total/5)
	for deliveries.Load() < targetAfterJoin && ctx.Err() == nil {
		time.Sleep(20 * time.Millisecond)
	}

	// Consumer 2 leaves mid-flow -- the rebalance that follows must
	// redistribute partitions so consumer 1 can drain the rest.
	cl2.Close()
	e3 := waitForMembers(1)
	if e3 <= e2 {
		t.Fatalf("expected epoch bump after consumer 2 left (e2=%d, e3=%d)", e2, e3)
	}

	// Consumer 1 drains the remaining records. Wait for runConsumer
	// goroutines to exit so we can read `seen` without racing.
	wg.Wait()
	seenMu.Lock()
	uniqueSeen := len(seen)
	seenMu.Unlock()
	if uniqueSeen != total {
		t.Fatalf("expected %d unique records across the rebalance, got %d (deliveries=%d, final epoch=%d)",
			total, uniqueSeen, deliveries.Load(), e3)
	}
	t.Logf("epochs: join=%d, +c2=%d, -c2=%d; unique=%d, deliveries=%d",
		e1, e2, e3, uniqueSeen, deliveries.Load())
}

// TestShareGroupInvalidShareSessionReset verifies the client transparently
// recovers when the broker reports InvalidShareSessionEpoch at the top
// level of a ShareFetch response -- the signal that the broker's session
// state has diverged from the client's (e.g., after a broker-side
// session eviction). The client must call resetShareSession and rebuild
// the session at epoch 0 without losing records.
//
// ControlKey hooks are one-shot by default (dropped after returning
// true), so the post-reset fetches would bypass the hook entirely if
// we let that happen. KeepControl keeps the hook alive for every
// ShareFetch so we can observe the full epoch sequence across the
// injection and recovery.
func TestShareGroupInvalidShareSessionReset(t *testing.T) {
	t.Parallel()

	const topic = "share-invalidsess"
	const group = "share-test-invalidsess"
	const total = 50

	c := newCluster(t, SeedTopics(1, topic))
	produceShareN(t, c, topic, group, total)

	var injected atomic.Bool
	var postResetEpoch0Seen atomic.Bool
	var allEpochs []int32
	var epochsMu sync.Mutex
	c.ControlKey(int16(kmsg.ShareFetch), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		// Keep the hook for every ShareFetch -- otherwise it drops as
		// soon as we return true to inject the error, and the
		// post-reset epoch=0 fetch goes unobserved.
		c.KeepControl()
		req := kreq.(*kmsg.ShareFetchRequest)
		epochsMu.Lock()
		allEpochs = append(allEpochs, req.ShareSessionEpoch)
		epochsMu.Unlock()
		if injected.Load() && req.ShareSessionEpoch == 0 {
			postResetEpoch0Seen.Store(true)
			return nil, nil, false
		}
		if req.ShareSessionEpoch > 0 && injected.CompareAndSwap(false, true) {
			resp := req.ResponseKind().(*kmsg.ShareFetchResponse)
			resp.ErrorCode = kerr.InvalidShareSessionEpoch.Code
			return resp, nil, true
		}
		return nil, nil, false
	})

	cl := newShareConsumer(t, c, topic, group)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Poll until every record has been consumed AND the post-reset
	// epoch=0 fetch has been observed. The client's source loop issues
	// empty fetches between polls, which is what drives the post-error
	// reset cycle the hook is watching for.
	seen := make(map[string]bool, total)
	deadline := time.Now().Add(5 * time.Second)
	for ctx.Err() == nil && (len(seen) < total || !postResetEpoch0Seen.Load()) {
		if time.Now().After(deadline) {
			break
		}
		pollCtx, pollCancel := context.WithTimeout(ctx, 300*time.Millisecond)
		fetches := cl.PollFetches(pollCtx)
		pollCancel()
		for _, r := range fetches.Records() {
			seen[string(r.Value)] = true
			r.Ack(kgo.AckAccept)
		}
	}
	if len(seen) < total {
		t.Fatalf("expected %d unique records, got %d", total, len(seen))
	}
	if !injected.Load() {
		t.Fatal("never injected InvalidShareSessionEpoch -- no post-initial fetch occurred")
	}
	if !postResetEpoch0Seen.Load() {
		epochsMu.Lock()
		seq := append([]int32(nil), allEpochs...)
		epochsMu.Unlock()
		t.Fatalf("client never sent ShareSessionEpoch=0 after the reset -- observed epoch sequence: %v", seq)
	}
}

// TestShareGroupSubscriptionPurge exercises two intertwined paths:
//
//  1. Subscription change mid-consume: when PurgeTopicsFromConsuming
//     removes a topic, the next ShareGroupHeartbeat must carry the
//     updated SubscribedTopicNames and the broker must rebalance the
//     group so the removed topic is no longer assigned.
//  2. Purge state cleanup: records from the purged topic stop arriving
//     and any pending acks queued for the purged topic are reported
//     via the ShareAckCallback as errShareConsumerLeft (purgeTopics'
//     drainAndClose path). No stale cursor or ack state should
//     survive the purge.
//
// Combining these into one test catches regressions in either path
// in a single run and also verifies they compose correctly (purge
// should drive the subscription change, not just locally drop state).
func TestShareGroupSubscriptionPurge(t *testing.T) {
	t.Parallel()

	const topicA = "share-purge-a"
	const topicB = "share-purge-b"
	const group = "share-test-purge"
	const perTopic = 25

	c := newCluster(t,
		SeedTopics(1, topicA),
		SeedTopics(1, topicB),
		BrokerConfigs(map[string]string{
			// kfake's default heartbeat interval is 5s; the kgo share
			// client floors at 1s. Without this override the test
			// waits up to 5s for the next heartbeat to carry the
			// updated SubscribedTopicNames after the purge.
			"group.share.heartbeat.interval.ms": "100",
		}),
	)

	// Produce records to both topics before the consumer joins so that
	// share.auto.offset.reset=earliest picks them up.
	prodCl := newPlainClient(t, c)
	defer prodCl.Close()
	setShareAutoOffsetReset(t, prodCl, group)
	for i := range perTopic {
		v := []byte(strconv.Itoa(i))
		prodCl.Produce(context.Background(), &kgo.Record{Topic: topicA, Value: v}, nil)
		prodCl.Produce(context.Background(), &kgo.Record{Topic: topicB, Value: v}, nil)
	}
	if err := prodCl.Flush(context.Background()); err != nil {
		t.Fatalf("produce flush: %v", err)
	}

	// Collect ShareAckCallback invocations so we can verify
	// errShareConsumerLeft fires for A's queued acks at purge time.
	var cbMu sync.Mutex
	var cbResults []kgo.ShareAckResult
	ackCb := func(_ *kgo.Client, res kgo.ShareAckResults) {
		cbMu.Lock()
		cbResults = append(cbResults, res...)
		cbMu.Unlock()
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics(topicA, topicB),
		kgo.ShareGroup(group),
		kgo.FetchMaxWait(200*time.Millisecond),
		kgo.ShareAckCallback(ackCb),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Phase 1: poll until we grab at least one topicA record, holding
	// up to 3 A records un-acked from the poll that first produced
	// them; ack every non-held record normally. We stop polling the
	// moment we have held records: finalizePreviousPoll on a
	// subsequent poll would auto-accept records whose status is 0,
	// sending the "held" acks to the broker and defeating the
	// closed-cursor ack test below. Polls before we see an A record
	// are safe because all their records get acked (status=Accept),
	// so finalizePreviousPoll on those records is a no-op.
	aBeforePurge := make(map[string]bool)
	bBeforePurge := make(map[string]bool)
	var heldA []*kgo.Record
	deadline := time.Now().Add(10 * time.Second)
	for len(heldA) == 0 && time.Now().Before(deadline) && ctx.Err() == nil {
		pollCtx, pollCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		fetches := cl.PollFetches(pollCtx)
		pollCancel()
		for _, r := range fetches.Records() {
			if r.Topic == topicA {
				aBeforePurge[string(r.Value)] = true
				if len(heldA) < 3 {
					heldA = append(heldA, r)
				} else {
					r.Ack(kgo.AckAccept)
				}
			} else {
				bBeforePurge[string(r.Value)] = true
				r.Ack(kgo.AckAccept)
			}
		}
	}
	if len(heldA) == 0 {
		t.Fatalf("never polled any topicA records within deadline (aBeforePurge=%d, bBeforePurge=%d)", len(aBeforePurge), len(bBeforePurge))
	}

	// Phase 2: purge topicA. This should (a) close the topicA cursors
	// so subsequent r.Ack calls fail with errShareConsumerLeft via
	// the callback, and (b) trigger a heartbeat with the reduced
	// SubscribedTopicNames.
	cl.PurgeTopicsFromConsuming(topicA)

	// Immediately ack the held A records. With the cursor closed,
	// appendAck enqueues an errShareConsumerLeft ShareAckResult per
	// record via the callback ring.
	for _, r := range heldA {
		r.Ack(kgo.AckAccept)
	}

	// Phase 3: verify broker sees the updated subscription. Poll
	// ShareGroupDescribe until the member's SubscribedTopicNames lists
	// only topicB.
	admin := newPlainClient(t, c)
	defer admin.Close()
	deadline = time.Now().Add(10 * time.Second)
	var lastSubscribed []string
	for time.Now().Before(deadline) {
		req := kmsg.NewPtrShareGroupDescribeRequest()
		req.GroupIDs = []string{group}
		resp, err := req.RequestWith(context.Background(), admin)
		if err != nil {
			t.Fatalf("describe: %v", err)
		}
		if len(resp.Groups) == 1 && len(resp.Groups[0].Members) == 1 {
			lastSubscribed = resp.Groups[0].Members[0].SubscribedTopicNames
			if len(lastSubscribed) == 1 && lastSubscribed[0] == topicB {
				break
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !(len(lastSubscribed) == 1 && lastSubscribed[0] == topicB) {
		t.Fatalf("broker never saw subscription change: last observed SubscribedTopicNames=%v", lastSubscribed)
	}

	// Phase 4: continue consuming. Verify only B records arrive and
	// that all B records are eventually seen.
	produceExtraStart := perTopic
	for i := range perTopic {
		v := []byte(strconv.Itoa(produceExtraStart + i))
		prodCl.Produce(context.Background(), &kgo.Record{Topic: topicA, Value: v}, nil)
		prodCl.Produce(context.Background(), &kgo.Record{Topic: topicB, Value: v}, nil)
	}
	if err := prodCl.Flush(context.Background()); err != nil {
		t.Fatalf("produce flush 2: %v", err)
	}

	bAllSeen := make(map[string]bool)
	for k := range bBeforePurge {
		bAllSeen[k] = true
	}
	var postPurgeAFromFetch int
	deadline = time.Now().Add(10 * time.Second)
	for len(bAllSeen) < perTopic*2 && time.Now().Before(deadline) && ctx.Err() == nil {
		pollCtx, pollCancel := context.WithTimeout(ctx, 500*time.Millisecond)
		fetches := cl.PollFetches(pollCtx)
		pollCancel()
		for _, r := range fetches.Records() {
			if r.Topic == topicA {
				postPurgeAFromFetch++
			} else {
				bAllSeen[string(r.Value)] = true
				r.Ack(kgo.AckAccept)
			}
		}
	}
	if postPurgeAFromFetch != 0 {
		t.Errorf("received %d topicA records after purge; purge did not stop topicA delivery", postPurgeAFromFetch)
	}
	if len(bAllSeen) < perTopic*2 {
		t.Fatalf("expected %d topicB records across the run, got %d", perTopic*2, len(bAllSeen))
	}

	// Phase 5: verify the callback reported errShareConsumerLeft for
	// A partitions. The sentinel is unexported on purpose (the
	// condition is non-actionable, kgo does not want users writing
	// errors.Is retry patterns against it), so we match on the
	// stable leading substring of its message.
	cbMu.Lock()
	defer cbMu.Unlock()
	var leftErrsForA int
	for _, r := range cbResults {
		if r.Topic == topicA && r.Err != nil && strings.Contains(r.Err.Error(), "share consumer has left") {
			leftErrsForA++
		}
	}
	if leftErrsForA == 0 {
		t.Errorf("ShareAckCallback never fired with errShareConsumerLeft for topicA after purge; results=%+v", cbResults)
	}
}

// TestShareGroupInvalidShareSessionResetOnAck mirrors
// TestShareGroupInvalidShareSessionReset for the ShareAcknowledge
// path: the client sends a standalone ShareAcknowledge (triggered by
// FlushAcks or piggybacked on ShareFetch), the broker returns a
// top-level InvalidShareSessionEpoch, and the client must
// resetShareSession, surface the failed acks via ShareAckCallback,
// and continue consuming. Without this, a session-reset fired by the
// ack path (shareAck at consumer_share.go:1742) is untested -- same
// risk class as the fetch path but a different code site.
func TestShareGroupInvalidShareSessionResetOnAck(t *testing.T) {
	t.Parallel()

	const topic = "share-invalidack"
	const group = "share-test-invalidack"
	const total = 30

	c := newCluster(t,
		SeedTopics(1, topic),
		BrokerConfigs(map[string]string{
			"group.share.heartbeat.interval.ms": "100",
		}),
	)
	produceShareN(t, c, topic, group, total)

	// Intercept the first ack-carrying request once and return a
	// top-level InvalidShareSessionEpoch. The client may send acks
	// either as a standalone ShareAcknowledge (FlushAcks with no
	// pending fetch to piggyback on) OR piggybacked on ShareFetch;
	// handle both cases. KeepControl lets us continue observing
	// post-reset traffic so a regression that stops issuing fetches
	// after the reset would surface here.
	var injected atomic.Bool
	var postResetFetchEpoch0 atomic.Bool
	injectTopLevel := func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		switch req := kreq.(type) {
		case *kmsg.ShareAcknowledgeRequest:
			// Only intercept a request that actually carries acks.
			hasAcks := false
			for _, rt := range req.Topics {
				for _, rp := range rt.Partitions {
					if len(rp.AcknowledgementBatches) > 0 {
						hasAcks = true
						break
					}
				}
			}
			if !hasAcks || !injected.CompareAndSwap(false, true) {
				return nil, nil, false
			}
			resp := req.ResponseKind().(*kmsg.ShareAcknowledgeResponse)
			resp.ErrorCode = kerr.InvalidShareSessionEpoch.Code
			return resp, nil, true
		case *kmsg.ShareFetchRequest:
			if injected.Load() && req.ShareSessionEpoch == 0 {
				postResetFetchEpoch0.Store(true)
				return nil, nil, false
			}
			// Inject only on a ShareFetch that carries piggybacked acks.
			hasAcks := false
			for _, rt := range req.Topics {
				for _, rp := range rt.Partitions {
					if len(rp.AcknowledgementBatches) > 0 {
						hasAcks = true
						break
					}
				}
			}
			if !hasAcks || !injected.CompareAndSwap(false, true) {
				return nil, nil, false
			}
			resp := req.ResponseKind().(*kmsg.ShareFetchResponse)
			resp.ErrorCode = kerr.InvalidShareSessionEpoch.Code
			return resp, nil, true
		}
		return nil, nil, false
	}
	c.ControlKey(int16(kmsg.ShareAcknowledge), injectTopLevel)
	c.ControlKey(int16(kmsg.ShareFetch), injectTopLevel)

	// Capture ShareAckCallback invocations so we can verify the
	// failed acks surfaced to the user with InvalidShareSessionEpoch.
	var cbMu sync.Mutex
	var cbResults []kgo.ShareAckResult
	ackCb := func(_ *kgo.Client, res kgo.ShareAckResults) {
		cbMu.Lock()
		cbResults = append(cbResults, res...)
		cbMu.Unlock()
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics(topic),
		kgo.ShareGroup(group),
		kgo.FetchMaxWait(200*time.Millisecond),
		kgo.ShareAckCallback(ackCb),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Consume every record. Records whose acks hit the injected error
	// get released on the broker side (the broker never recorded the
	// ack because the session was "invalid"); after the client resets
	// and re-fetches with epoch=0, those records are re-delivered.
	seen := make(map[string]int, total) // key -> delivery count
	deadline := time.Now().Add(5 * time.Second)
	for ctx.Err() == nil && (len(seen) < total || !injected.Load() || !postResetFetchEpoch0.Load()) {
		if time.Now().After(deadline) {
			break
		}
		pollCtx, pollCancel := context.WithTimeout(ctx, 300*time.Millisecond)
		fetches := cl.PollFetches(pollCtx)
		pollCancel()
		for _, r := range fetches.Records() {
			seen[string(r.Value)]++
			r.Ack(kgo.AckAccept)
		}
	}
	if len(seen) < total {
		t.Fatalf("expected %d unique records, got %d (injected=%v, postResetFetchEpoch0=%v)",
			total, len(seen), injected.Load(), postResetFetchEpoch0.Load())
	}
	if !injected.Load() {
		t.Fatal("never injected top-level ack error -- no ack-carrying request reached the broker")
	}
	if !postResetFetchEpoch0.Load() {
		t.Fatal("client never issued a post-reset ShareSessionEpoch=0 fetch")
	}

	// Callback must have reported at least one partition result with
	// InvalidShareSessionEpoch -- the failed batch that triggered the
	// reset.
	cbMu.Lock()
	defer cbMu.Unlock()
	var gotInvalidSess int
	for _, r := range cbResults {
		if errors.Is(r.Err, kerr.InvalidShareSessionEpoch) {
			gotInvalidSess++
		}
	}
	if gotInvalidSess == 0 {
		t.Errorf("ShareAckCallback never surfaced InvalidShareSessionEpoch; results=%+v", cbResults)
	}
}

// TestShareGroupAckCallbackSuccessPath verifies the ShareAckCallback
// happy path: acked records produce a callback invocation with nil
// Err for every partition the acks covered. Previously only the
// failure branches of the callback (errShareConsumerLeft,
// non-retryable ack errors) had any coverage; a regression that
// dropped the callback on success would have shipped invisibly.
func TestShareGroupAckCallbackSuccessPath(t *testing.T) {
	t.Parallel()

	const topic = "share-cbsuccess"
	const group = "share-test-cbsuccess"
	const partitions = 3
	const total = 60 // spread across partitions to assert multi-partition callback coverage

	c := newCluster(t, SeedTopics(partitions, topic))

	admin := newPlainClient(t, c)
	defer admin.Close()
	setShareAutoOffsetReset(t, admin, group)

	for i := range total {
		admin.Produce(context.Background(), &kgo.Record{
			Topic: topic,
			// Spread across partitions by including i in the key; the
			// default hash partitioner will distribute.
			Key:   []byte(strconv.Itoa(i)),
			Value: []byte(strconv.Itoa(i)),
		}, nil)
	}
	if err := admin.Flush(context.Background()); err != nil {
		t.Fatalf("produce flush: %v", err)
	}

	var cbMu sync.Mutex
	var cbResults []kgo.ShareAckResult
	ackCb := func(_ *kgo.Client, res kgo.ShareAckResults) {
		cbMu.Lock()
		cbResults = append(cbResults, res...)
		cbMu.Unlock()
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics(topic),
		kgo.ShareGroup(group),
		kgo.FetchMaxWait(200*time.Millisecond),
		kgo.ShareAckCallback(ackCb),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	touchedParts := make(map[int32]bool)
	seen := make(map[string]bool)
	for len(seen) < total && ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		for _, r := range fetches.Records() {
			seen[string(r.Value)] = true
			touchedParts[r.Partition] = true
			r.Ack(kgo.AckAccept)
		}
	}
	if len(seen) < total {
		t.Fatalf("expected %d records, got %d", total, len(seen))
	}

	// Force any remaining queued acks to flush so the callback
	// invocations are deterministic before we read cbResults.
	flushCtx, flushCancel := context.WithTimeout(ctx, 5*time.Second)
	if err := cl.FlushAcks(flushCtx); err != nil {
		t.Fatalf("FlushAcks: %v", err)
	}
	flushCancel()

	cbMu.Lock()
	defer cbMu.Unlock()
	if len(cbResults) == 0 {
		t.Fatal("ShareAckCallback never fired on success path")
	}
	// Every callback result must be for our topic with a nil error;
	// any non-nil error here would indicate a silent ack failure.
	partsWithNilErr := make(map[int32]bool)
	for _, r := range cbResults {
		if r.Topic != topic {
			t.Errorf("callback reported unexpected topic %q", r.Topic)
			continue
		}
		if r.Err != nil {
			t.Errorf("callback reported error on happy path for partition %d: %v", r.Partition, r.Err)
			continue
		}
		partsWithNilErr[r.Partition] = true
	}
	// Every partition we actually saw records from must have been
	// reported by the callback at least once.
	for p := range touchedParts {
		if !partsWithNilErr[p] {
			t.Errorf("partition %d had records consumed but no success callback result", p)
		}
	}
}

// TestShareGroupNonRetryableAckError verifies that a non-retryable
// per-partition ack error (UnknownTopicID in this case) reaches the
// user via ShareAckCallback and that the acks are NOT requeued --
// isShareAckRetryable excludes NotLeader/FencedLeader/UnknownTopic*
// specifically because requeuing would loop forever against an error
// the broker will never recover from. The retryable path has its own
// test (TestShareGroupAckRequeue); this is its required counterpart.
func TestShareGroupNonRetryableAckError(t *testing.T) {
	t.Parallel()

	const topic = "share-nonretryack"
	const group = "share-test-nonretryack"
	// Single record keeps the ack flow deterministic: one record
	// produces one ack, which reaches the broker in exactly one
	// request. This lets us unambiguously assert that no ack-carrying
	// request follows the intercepted one -- any such request means
	// the client retried a non-retryable error.
	const total = 1

	c := newCluster(t,
		SeedTopics(1, topic),
		BrokerConfigs(map[string]string{
			"group.share.heartbeat.interval.ms": "100",
		}),
	)
	produceShareN(t, c, topic, group, total)

	// Intercept the first ack-carrying request once (either standalone
	// ShareAcknowledge or piggybacked ShareFetch) and return a
	// per-partition non-retryable error. KeepControl lets us observe
	// that the client does NOT re-send those same offsets on later
	// requests (if it did, that'd indicate an accidental requeue).
	var injected atomic.Bool
	var ackReqAfterInject atomic.Int32
	inject := func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		switch req := kreq.(type) {
		case *kmsg.ShareAcknowledgeRequest:
			hasAcks := false
			for _, rt := range req.Topics {
				for _, rp := range rt.Partitions {
					if len(rp.AcknowledgementBatches) > 0 {
						hasAcks = true
						break
					}
				}
			}
			if !hasAcks {
				return nil, nil, false
			}
			if !injected.CompareAndSwap(false, true) {
				ackReqAfterInject.Add(1)
				return nil, nil, false
			}
			resp := req.ResponseKind().(*kmsg.ShareAcknowledgeResponse)
			for _, rt := range req.Topics {
				respTopic := kmsg.NewShareAcknowledgeResponseTopic()
				respTopic.TopicID = rt.TopicID
				for _, rp := range rt.Partitions {
					respPart := kmsg.NewShareAcknowledgeResponseTopicPartition()
					respPart.Partition = rp.Partition
					respPart.ErrorCode = kerr.UnknownTopicID.Code
					respTopic.Partitions = append(respTopic.Partitions, respPart)
				}
				resp.Topics = append(resp.Topics, respTopic)
			}
			return resp, nil, true
		case *kmsg.ShareFetchRequest:
			hasAcks := false
			for _, rt := range req.Topics {
				for _, rp := range rt.Partitions {
					if len(rp.AcknowledgementBatches) > 0 {
						hasAcks = true
						break
					}
				}
			}
			if !hasAcks {
				return nil, nil, false
			}
			if !injected.CompareAndSwap(false, true) {
				ackReqAfterInject.Add(1)
				return nil, nil, false
			}
			resp := req.ResponseKind().(*kmsg.ShareFetchResponse)
			for _, rt := range req.Topics {
				respTopic := kmsg.NewShareFetchResponseTopic()
				respTopic.TopicID = rt.TopicID
				for _, rp := range rt.Partitions {
					respPart := kmsg.NewShareFetchResponseTopicPartition()
					respPart.Partition = rp.Partition
					respPart.AcknowledgeErrorCode = kerr.UnknownTopicID.Code
					respTopic.Partitions = append(respTopic.Partitions, respPart)
				}
				resp.Topics = append(resp.Topics, respTopic)
			}
			return resp, nil, true
		}
		return nil, nil, false
	}
	c.ControlKey(int16(kmsg.ShareAcknowledge), inject)
	c.ControlKey(int16(kmsg.ShareFetch), inject)

	var cbMu sync.Mutex
	var cbResults []kgo.ShareAckResult
	ackCb := func(_ *kgo.Client, res kgo.ShareAckResults) {
		cbMu.Lock()
		cbResults = append(cbResults, res...)
		cbMu.Unlock()
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics(topic),
		kgo.ShareGroup(group),
		kgo.FetchMaxWait(200*time.Millisecond),
		kgo.ShareAckCallback(ackCb),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	seen := make(map[string]bool, total)
	for len(seen) < total && ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		for _, r := range fetches.Records() {
			seen[string(r.Value)] = true
			r.Ack(kgo.AckAccept)
		}
	}
	// Force any queued acks out through the broker (FlushAcks will
	// return whether the acks succeeded or failed -- the callback
	// carries the per-partition outcomes).
	flushCtx, flushCancel := context.WithTimeout(ctx, 5*time.Second)
	if err := cl.FlushAcks(flushCtx); err != nil {
		t.Fatalf("FlushAcks: %v", err)
	}
	flushCancel()

	if !injected.Load() {
		t.Fatal("never injected non-retryable ack error")
	}

	cbMu.Lock()
	defer cbMu.Unlock()
	var gotUnknownTopicID int
	for _, r := range cbResults {
		if errors.Is(r.Err, kerr.UnknownTopicID) {
			gotUnknownTopicID++
		}
	}
	if gotUnknownTopicID == 0 {
		t.Errorf("ShareAckCallback never surfaced UnknownTopicID; results=%+v", cbResults)
	}
	// isShareAckRetryable excludes UnknownTopicID specifically so the
	// client does NOT requeue -- a requeue would loop forever against
	// an error the broker will never recover from. Zero post-inject
	// ack-carrying requests proves the acks were not requeued; any
	// value > 0 means the client is treating this error as retryable.
	if n := ackReqAfterInject.Load(); n != 0 {
		t.Errorf("expected 0 post-inject ack-carrying requests, got %d -- client appears to have requeued a non-retryable ack error", n)
	}
}

// TestShareGroupAckDedupRenewThenTerminal is a regression test for PR
// review finding #1: calling r.Ack(AckRenew) and then r.Ack(AckAccept)
// on the same record before the broker confirms the renew leaves two
// entries in cursor.pendingAcks for the same offset. buildAckRanges
// must dedupe these into ONE wire-format AcknowledgementBatch;
// otherwise the request carries two adjacent [X,X] batches for the
// same offset and the real broker rejects with INVALID_RECORD_STATE.
//
// Without the dedup loop in buildAckRanges, this test would observe
// two batches and fail.
func TestShareGroupAckDedupRenewThenTerminal(t *testing.T) {
	t.Parallel()

	const topic = "share-dedup"
	const group = "share-test-dedup"

	c := newCluster(t,
		SeedTopics(1, topic),
		BrokerConfigs(map[string]string{
			"group.share.heartbeat.interval.ms": "100",
		}),
	)
	produceShareN(t, c, topic, group, 1)

	// Capture the batch count on the first ack-carrying request.
	// Acks can land either on a standalone ShareAcknowledge (hasRenew
	// path triggers a pre-flight) or piggybacked on a ShareFetch,
	// depending on whether the second Ack's CAS flips status away
	// from AckRenew before the source loop builds the request. We
	// observe both and assert on whichever carries the acks.
	var batchCount atomic.Int32
	record := func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		var total int32
		switch req := kreq.(type) {
		case *kmsg.ShareAcknowledgeRequest:
			for _, rt := range req.Topics {
				for _, rp := range rt.Partitions {
					total += int32(len(rp.AcknowledgementBatches))
				}
			}
		case *kmsg.ShareFetchRequest:
			for _, rt := range req.Topics {
				for _, rp := range rt.Partitions {
					total += int32(len(rp.AcknowledgementBatches))
				}
			}
		}
		if total > 0 {
			// Record the FIRST ack-carrying request only, so a later
			// request retrying or piggybacking doesn't overwrite our
			// observation.
			batchCount.CompareAndSwap(0, total)
		}
		return nil, nil, false
	}
	c.ControlKey(int16(kmsg.ShareAcknowledge), record)
	c.ControlKey(int16(kmsg.ShareFetch), record)

	cl := newShareConsumer(t, c, topic, group)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	var acked bool
	for !acked && ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		for _, r := range fetches.Records() {
			// Tight back-to-back: r.Ack has no Go scheduling point
			// between these two calls (appendAck uses a non-blocking
			// channel send). Both entries land in cursor.pendingAcks
			// before the source loop's ack timer fires.
			r.Ack(kgo.AckRenew)
			r.Ack(kgo.AckAccept)
			acked = true
		}
	}
	if !acked {
		t.Fatal("never consumed the single record")
	}

	// FlushAcks forces the source loop to drain cursor.pendingAcks
	// now (rather than waiting for the 1s ack timer). At drain time,
	// both entries are present and buildAckRanges must dedupe.
	flushCtx, flushCancel := context.WithTimeout(ctx, 5*time.Second)
	if err := cl.FlushAcks(flushCtx); err != nil {
		t.Fatalf("FlushAcks: %v", err)
	}
	flushCancel()

	got := batchCount.Load()
	if got == 0 {
		t.Fatal("no ack-carrying request was observed")
	}
	if got != 1 {
		t.Fatalf("expected exactly 1 AcknowledgementBatch (dedup of renew+terminal entries for the same offset), got %d -- buildAckRanges dedup regressed", got)
	}
}

// TestShareGroupLeaderMoveInFlightAcks is the TestShareGroupCurrentLeaderMove
// complement: records are consumed from the original leader and acked
// AFTER the move (not before). This exercises the PR review finding
// #2 signal path -- whether the acks complete via the old source
// (drained before the migration) or via the new source (drained
// after, potentially reported as stale), they must eventually flow
// through the callback so FlushAcks unblocks. A broken
// addShareCursor pendingAcks check would leave the acks rotting on
// the migrated cursor and FlushAcks would time out.
func TestShareGroupLeaderMoveInFlightAcks(t *testing.T) {
	t.Parallel()

	const topic = "share-inflight-move"
	const group = "share-test-inflight-move"
	const total = 5

	c := newCluster(t,
		NumBrokers(2),
		SeedTopics(1, topic),
		BrokerConfigs(map[string]string{
			"group.share.heartbeat.interval.ms": "100",
		}),
	)

	origLeader := c.LeaderFor(topic, 0)
	newLeader := int32(1)
	if origLeader == 1 {
		newLeader = 0
	}

	prodCl := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
	defer prodCl.Close()
	setShareAutoOffsetReset(t, prodCl, group)
	for i := range total {
		if err := prodCl.ProduceSync(context.Background(), &kgo.Record{Value: []byte(strconv.Itoa(i))}).FirstErr(); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}

	var cbMu sync.Mutex
	var cbResults []kgo.ShareAckResult
	ackCb := func(_ *kgo.Client, res kgo.ShareAckResults) {
		cbMu.Lock()
		cbResults = append(cbResults, res...)
		cbMu.Unlock()
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics(topic),
		kgo.ShareGroup(group),
		kgo.FetchMaxWait(200*time.Millisecond),
		kgo.ShareAckCallback(ackCb),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Phase 1: poll every record and hold them WITHOUT acking.
	var held []*kgo.Record
	for len(held) < total && ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		held = append(held, fetches.Records()...)
	}
	if len(held) < total {
		t.Fatalf("phase 1: expected %d records, got %d", total, len(held))
	}

	// Phase 2: move the partition BEFORE acking. The client's next
	// ShareFetch will get NOT_LEADER from the old leader and migrate
	// the cursor via applyMoves -> addShareCursor.
	if err := c.MoveTopicPartition(topic, 0, newLeader); err != nil {
		t.Fatalf("move: %v", err)
	}

	// Phase 3: ack the held records AFTER the move. Depending on
	// timing, these land on the cursor either before the migration
	// (drained via old source, acks reach broker normally) or after
	// (drained via new source, filterStaleEntries drops them with a
	// stale-ack error via the callback). Either path must complete
	// for FlushAcks to return -- which is exactly the addShareCursor
	// pending-signal invariant.
	for _, r := range held {
		r.Ack(kgo.AckAccept)
	}

	// Phase 4: FlushAcks must return within its context. A broken
	// addShareCursor pending-signal would leave the migrated cursor's
	// acks undrained forever, making FlushAcks block.
	flushCtx, flushCancel := context.WithTimeout(ctx, 5*time.Second)
	if err := cl.FlushAcks(flushCtx); err != nil {
		t.Fatalf("FlushAcks after in-flight leader move hung or errored: %v (this is the addShareCursor pending-signal invariant -- a broken signal path strands the acks on the migrated cursor)", err)
	}
	flushCancel()

	// Phase 5: the callback must have reported per-partition results
	// for partition 0 (whether Err=nil via old-source drain or
	// Err!=nil via stale-ack drain on the new source). Zero results
	// means the acks never reached any drain, which is the
	// stranded-on-migration failure mode.
	cbMu.Lock()
	defer cbMu.Unlock()
	var p0 int
	for _, r := range cbResults {
		if r.Topic == topic && r.Partition == 0 {
			p0++
		}
	}
	if p0 == 0 {
		t.Errorf("callback never fired for partition 0 after in-flight ack+move; acks may have stranded on the migrated cursor. Results: %+v", cbResults)
	}
}
