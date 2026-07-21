package kfake

import (
	"context"
	"errors"
	"hash/crc32"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

func TestIssue885(t *testing.T) {
	t.Parallel()
	const (
		testTopic        = "foo"
		producedMessages = 5
		followerLogStart = 3
	)

	c, err := NewCluster(
		NumBrokers(2),
		SleepOutOfOrder(),
		SeedTopics(1, testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Flow:
	//
	// * We always say one broker is the leader -- every Metadata response
	//   can be the same; we do not need to hijack Metadata
	//
	// * We produce 5 separate batches just to have some data
	//
	// * We hijack fetch: if to the leader, we say the other broker is the
	//   follower.
	//
	// * We hijack fetch 2: if to follower, we say "offset out of range".
	//
	// END SETUP STAGE.
	//
	// TEST
	//
	// * We return one batch at a time from the leader.
	// * We expect the leader to receive 3 requests.
	// * On the fourth, we redirect back to the follower.
	// * Batch four and five are served from the follower.
	// * We are done.
	// * Any deviation is failure.
	//
	// We control the flow through the stages; any bug results in not continuing
	// forward (i.e. looping through the stages and never finishing).

	// Inline anonymous function so that we can defer and cleanup within scope.
	func() {
		cl, err := kgo.NewClient(
			kgo.DefaultProduceTopic(testTopic),
			kgo.SeedBrokers(c.ListenAddrs()...),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()

		for i := 0; i < producedMessages; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			err := cl.ProduceSync(ctx, kgo.StringRecord(strconv.Itoa(i))).FirstErr()
			cancel()
			if err != nil {
				t.Fatal(err)
			}
		}
	}()

	var followerOOOR bool

	ti := c.TopicInfo(testTopic)
	pi := c.PartitionInfo(testTopic, 0)
	follower := (pi.Leader + 1) % 2
	c.SetFollowers(testTopic, 0, []int32{follower})

	c.ControlKey(int16(kmsg.Fetch), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()

		req := kreq.(*kmsg.FetchRequest)
		if req.Version < 11 {
			t.Fatal("unable to run test with fetch requests < v11")
		}
		if len(req.Topics) != 1 || len(req.Topics[0].Partitions) != 1 {
			t.Fatalf("unexpected malformed req topics or partitions: %v", req)
		}

		// If we *do* return a batch, we want to ensure we return only
		// one batch. We modify the incoming request to ensure at most
		// one batch is returned.
		req.MaxBytes = 1

		resp := req.ResponseKind().(*kmsg.FetchResponse)
		rt := kmsg.NewFetchResponseTopic()
		rt.Topic = testTopic
		rt.TopicID = ti.TopicID
		rp := kmsg.NewFetchResponseTopicPartition()

		resp.Topics = append(resp.Topics, rt)
		rtp := &resp.Topics[0]

		rtp.Partitions = append(rtp.Partitions, rp)
		rpp := &rtp.Partitions[0]

		rpp.Partition = 0
		rpp.ErrorCode = 0
		rpp.HighWatermark = pi.HighWatermark
		rpp.LastStableOffset = pi.LastStableOffset
		rpp.LogStartOffset = 0

		if c.CurrentNode() == pi.Leader {
			if !followerOOOR || req.Topics[0].Partitions[0].FetchOffset >= followerLogStart {
				rpp.PreferredReadReplica = (pi.Leader + 1) % 2
				return resp, nil, true
			}
			return nil, nil, false
		}

		if req.Topics[0].Partitions[0].FetchOffset < followerLogStart {
			rpp.ErrorCode = kerr.OffsetOutOfRange.Code
			rpp.LogStartOffset = 2
			followerOOOR = true
			return resp, nil, true
		}

		return nil, nil, false
	})

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics(testTopic),
		kgo.Rack("foo"),
		kgo.DisableFetchSessions(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	for consumed := 0; consumed != producedMessages; {
		fs := cl.PollFetches(ctx)
		if errs := fs.Errors(); errs != nil {
			t.Errorf("consume error: %v", errs)
			break
		}
		consumed += fs.NumRecords()
	}
}

func TestIssue905(t *testing.T) {
	t.Parallel()
	const (
		testTopic        = "foo"
		producedMessages = 5
	)

	c, err := NewCluster(
		NumBrokers(2),
		SleepOutOfOrder(),
		SeedTopics(1, testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Flow:
	//
	// * Leader always returns the follower
	//
	// * We produce 5 separate batches just to have some data
	//
	// END SETUP STAGE.
	//
	// TEST
	//
	// * We set recheck period to 1s
	// * We fetch -- leader should be hit once, then follower
	// * We sleep >1s before polling again. Follower should be hit a second time (buffering a fetch), then redirect to leader, then follower.
	//
	// If we do not redirect back to follower within 2s, consider failure.

	// Inline anonymous function so that we can defer and cleanup within scope.
	func() {
		cl, err := kgo.NewClient(
			kgo.DefaultProduceTopic(testTopic),
			kgo.SeedBrokers(c.ListenAddrs()...),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()

		for i := 0; i < producedMessages; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			err := cl.ProduceSync(ctx, kgo.StringRecord(strconv.Itoa(i))).FirstErr()
			cancel()
			if err != nil {
				t.Fatal(err)
			}
		}
	}()

	ti := c.TopicInfo(testTopic)
	pi := c.PartitionInfo(testTopic, 0)
	follower := (pi.Leader + 1) % 2
	c.SetFollowers(testTopic, 0, []int32{follower})

	var leaderReqs, followerReqs atomic.Int32
	allowFollower := make(chan struct{}, 1)
	followerHandled := make(chan struct{}, 5)
	c.ControlKey(int16(kmsg.Fetch), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()

		req := kreq.(*kmsg.FetchRequest)
		if req.Version < 11 {
			t.Fatal("unable to run test with fetch requests < v11")
		}
		if len(req.Topics) != 1 || len(req.Topics[0].Partitions) != 1 {
			t.Fatalf("unexpected malformed req topics or partitions: %v", req)
		}

		req.MaxBytes = 1 // Always ensure only one batch is returned

		// Every leader request we redirect back to the follower.
		if c.CurrentNode() == pi.Leader {
			leaderReqs.Add(1)

			resp := req.ResponseKind().(*kmsg.FetchResponse)
			rt := kmsg.NewFetchResponseTopic()
			rt.Topic = testTopic
			rt.TopicID = ti.TopicID
			rp := kmsg.NewFetchResponseTopicPartition()

			resp.Topics = append(resp.Topics, rt)
			rtp := &resp.Topics[0]

			rtp.Partitions = append(rtp.Partitions, rp)
			rpp := &rtp.Partitions[0]

			rpp.Partition = 0
			rpp.ErrorCode = 0
			rpp.HighWatermark = pi.HighWatermark
			rpp.LastStableOffset = pi.LastStableOffset
			rpp.LogStartOffset = 0

			rpp.PreferredReadReplica = (pi.Leader + 1) % 2
			return resp, nil, true
		}

		<-allowFollower
		followerReqs.Add(1)
		followerHandled <- struct{}{}

		return nil, nil, false
	})

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics(testTopic),
		kgo.Rack("foo"),
		kgo.DisableFetchSessions(),
		kgo.RecheckPreferredReplicaInterval(100*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	chkfs := func(fs kgo.Fetches, expOffset int64) {
		if fs.NumRecords() != 1 {
			t.Errorf("got %d records != exp 1", fs.NumRecords())
		} else {
			r := fs.Records()[0]
			if r.Offset != expOffset {
				t.Errorf("got offset %d != exp %d", r.Offset, expOffset)
			}
		}
		if len(fs.Errors()) != 0 {
			t.Errorf("got fetch errors: %v", fs.Errors())
		}
	}

	// First poll. Standard; triggers us to fetch from the follower.
	// We guard if the broker can reply to ensure our followerReqs
	// check does not race against a client internally buffering
	// another fetch quickly.
	{
		allowFollower <- struct{}{}
		fs := cl.PollFetches(ctx)
		<-followerHandled // drain signal from the first follower fetch
		chkfs(fs, 0)
		if lr := leaderReqs.Load(); lr != 1 {
			t.Errorf("stage 1 leader reqs %d != exp 1", lr)
		}
		if fr := followerReqs.Load(); fr != 1 {
			t.Errorf("stage 1 follower reqs reqs %d != exp 1", fr)
		}
		allowFollower <- struct{}{} // allow a background buffered fetch
	}

	// Wait for the background fetch to complete. After the follower
	// handler fires, the source is either processing the response or
	// blocked on its internal semaphore (waiting for PollFetches to
	// drain the buffer). Either way, no new createReq has run, so
	// leaderReqs is stable -- check it now.
	<-followerHandled
	if lr := leaderReqs.Load(); lr != 1 {
		t.Errorf("stage 2 leader reqs %d != exp 1", lr)
	}

	// Sleep past the recheck interval WHILE the source is blocked on
	// its semaphore. The cursor's moveAt (set in stage 1) becomes
	// stale (>100ms old). When PollFetches below drains the buffer and
	// unblocks the semaphore, the source's next createReq will see
	// the stale moveAt and trigger a recheck.
	time.Sleep(150 * time.Millisecond)

	{
		fs := cl.PollFetches(ctx)
		chkfs(fs, 1)
		if fr := followerReqs.Load(); fr != 2 {
			t.Errorf("stage 2 follower reqs reqs %d != exp 2", fr)
		}
	}

	// PollFetches unblocked the source. Its createReq sees stale
	// moveAt and triggers a recheck: cursor goes to leader
	// (leaderReqs=2), redirected back to follower, follower fetch
	// blocks on allowFollower. Send a token and poll again.
	{
		allowFollower <- struct{}{}
		fs := cl.PollFetches(ctx)
		chkfs(fs, 2)
		if lr := leaderReqs.Load(); lr != 2 {
			t.Errorf("stage 3 leader reqs %d != exp 2", lr)
		}
		if fr := followerReqs.Load(); fr != 3 {
			t.Errorf("stage 3 follower reqs reqs %d != exp 3", fr)
		}
		close(allowFollower) // allow all reqs; the next check is our last
	}

	// We should stay with the follower, no more leader.
	{
		fs := cl.PollFetches(ctx)
		chkfs(fs, 3)
		if lr := leaderReqs.Load(); lr != 2 {
			t.Errorf("stage 4 leader reqs %d != exp 2", lr)
		}
		if fr := followerReqs.Load(); fr != 4 {
			t.Errorf("stage 4 follower reqs reqs %d != exp 4", fr)
		}
	}
	{
		fs := cl.PollFetches(ctx)
		chkfs(fs, 4)
		if lr := leaderReqs.Load(); lr != 2 {
			t.Errorf("stage 5 leader reqs %d != exp 2", lr)
		}
		if fr := followerReqs.Load(); fr != 5 {
			t.Errorf("stage 5 follower reqs reqs %d != exp 5", fr)
		}
	}

	// Success.
}

func TestIssue906(t *testing.T) {
	t.Parallel()
	const testTopic = "foo"

	c, err := NewCluster(
		NumBrokers(1),
		SleepOutOfOrder(),
		SeedTopics(1, testTopic),
		AllowAutoTopicCreation(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Seed "foo" with two records.
	func() {
		cl, err := kgo.NewClient(
			kgo.DefaultProduceTopic(testTopic),
			kgo.SeedBrokers(c.ListenAddrs()...),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()

		for i := 0; i < 2; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			err := cl.ProduceSync(ctx, kgo.StringRecord(strconv.Itoa(i))).FirstErr()
			cancel()
			if err != nil {
				t.Fatal(err)
			}
		}
	}()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics("^foo.*"),
		kgo.ConsumeRegex(),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		t.Fatal(err)
		return
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	var produced int
	for produced != 2 {
		fetches := client.PollRecords(ctx, 10)

		var records []*kgo.Record
		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			records = append(records, &kgo.Record{
				Topic: "bar",
				Key:   record.Key,
				Value: record.Value,
			})
		}

		if err := client.ProduceSync(ctx, records...).FirstErr(); err != nil {
			t.Errorf("unable to produce: %v", err)
		}
		produced += len(records)
	}
}

func TestIssueTimestampInclusivity(t *testing.T) {
	t.Parallel()
	const (
		testTopic        = "bar"
		producedBatches  = 5
		followerLogStart = 3
	)

	c, err := NewCluster(
		NumBrokers(2),
		SeedTopics(1, testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Inline anonymous function so that we can defer and cleanup within scope.
	func() {
		cl, err := kgo.NewClient(
			kgo.DefaultProduceTopic(testTopic),
			kgo.SeedBrokers(c.ListenAddrs()...),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()

		for i := 0; i < producedBatches; i++ {
			_, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			offset := i * 4
			r1 := kgo.StringRecord(strconv.Itoa(offset))
			r1.Timestamp = time.UnixMilli(10_000 + int64(offset))
			offset++
			r2 := kgo.StringRecord(strconv.Itoa(offset))
			r2.Timestamp = time.UnixMilli(10_000 + int64(offset))
			offset++
			r3 := kgo.StringRecord(strconv.Itoa(offset))
			r3.Timestamp = time.UnixMilli(10_000 + int64(offset))
			err := cl.ProduceSync(context.Background(), r1, r2, r3).FirstErr()
			cancel()
			if err != nil {
				t.Fatal(err)
			}
		}
	}()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	tests := []struct {
		SearchTimestamp   int64
		ExpectedOffset    int64
		ExpectedTimestamp int64
	}{
		{
			SearchTimestamp:   99,
			ExpectedOffset:    0,
			ExpectedTimestamp: 10_000,
		},
		{
			SearchTimestamp:   10_000,
			ExpectedOffset:    0,
			ExpectedTimestamp: 10_000,
		},
		{
			SearchTimestamp:   10_001,
			ExpectedOffset:    1,
			ExpectedTimestamp: 10_001,
		},
		{
			SearchTimestamp:   10_003,
			ExpectedOffset:    3,
			ExpectedTimestamp: 10_004,
		},
		{
			SearchTimestamp:   10_004,
			ExpectedOffset:    3,
			ExpectedTimestamp: 10_004,
		},
		{
			SearchTimestamp:   10_015,
			ExpectedOffset:    12,
			ExpectedTimestamp: 10_016,
		},
		{
			SearchTimestamp:   10_018,
			ExpectedOffset:    14,
			ExpectedTimestamp: 10_018,
		},
		{
			SearchTimestamp:   11_000,
			ExpectedOffset:    15,
			ExpectedTimestamp: -1,
		},
	}
	for _, test := range tests {
		offsets, err := adm.ListOffsetsAfterMilli(ctx, test.SearchTimestamp)
		if err != nil {
			t.Fatal(err)
		}
		offset, ok := offsets.Lookup(testTopic, 0)
		if !ok {
			t.Fatal("missing partition")
		}
		if offset.Offset != test.ExpectedOffset || offset.Timestamp != test.ExpectedTimestamp {
			t.Fatalf(
				"searching for %d got: %+v, want offset %d, timestamp %d",
				test.SearchTimestamp,
				offset,
				test.ExpectedOffset,
				test.ExpectedTimestamp,
			)
		}
	}
}

func TestIssue1142(t *testing.T) {
	t.Parallel()
	const testTopic = "foo"

	c, err := NewCluster(
		NumBrokers(1),
		SleepOutOfOrder(),
		SeedTopics(1, testTopic),
		AllowAutoTopicCreation(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	client, err := kadm.NewOptClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		t.Fatal(err)
		return
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	for i := 0; i < 2; i++ {
		resp, err := client.Metadata(ctx)
		if err != nil {
			t.Fatal(err)
			return
		}
		if resp.Cluster != "kfake" {
			t.Fatalf("expected cluster kfake, got %s", resp.Cluster)
		}
		if resp.Controller != 0 {
			t.Fatalf("expected controller 0, got %d", resp.Controller)
		}
		if len(resp.Brokers) != 1 {
			t.Fatalf("expected 1 broker, got %d", len(resp.Brokers))
		}
		if len(resp.Topics.Names()) != 1 {
			t.Fatalf("expected 1 topic, got %d", len(resp.Topics.Names()))
		}
	}
}

// TestIssue1167 reproduces a race condition in preferred replica fetching
// where cursor.source is accessed concurrently without synchronization.
//
// The race occurs between concurrent move() calls:
//   - Goroutine A: move() writes cursor.source (source.go:271)
//   - Goroutine B: move()'s deferred allowUsable() reads cursor.source (source.go:210)
//
// Multiple goroutines can call move() concurrently on the same cursor when
// fetch responses return PreferredReadReplica, causing cursors to migrate
// between sources.
func TestIssue1167(t *testing.T) {
	t.Parallel()
	const (
		testTopic        = "test-topic"
		producedMessages = 100
	)

	// Create kfake cluster with 2 brokers to enable replica migration
	c, err := NewCluster(
		NumBrokers(2),
		SeedTopics(1, testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Produce messages to the topic
	func() {
		cl, err := kgo.NewClient(
			kgo.DefaultProduceTopic(testTopic),
			kgo.SeedBrokers(c.ListenAddrs()...),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()

		for i := 0; i < producedMessages; i++ {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			err := cl.ProduceSync(ctx, kgo.StringRecord(strconv.Itoa(i))).FirstErr()
			cancel()
			if err != nil {
				t.Fatal(err)
			}
		}
	}()

	// Set up follower for the partition
	ti := c.TopicInfo(testTopic)
	pi := c.PartitionInfo(testTopic, 0)
	follower := (pi.Leader + 1) % 2
	c.SetFollowers(testTopic, 0, []int32{follower})

	// Control fetch responses to ALWAYS return preferred replicas
	// This will trigger cursor migrations on EVERY fetch to maximize race likelihood
	c.ControlKey(int16(kmsg.Fetch), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()

		req := kreq.(*kmsg.FetchRequest)
		if req.Version < 11 {
			t.Fatal("unable to run test with fetch requests < v11")
		}

		resp := req.ResponseKind().(*kmsg.FetchResponse)
		rt := kmsg.NewFetchResponseTopic()
		rt.Topic = testTopic
		rt.TopicID = ti.TopicID
		rp := kmsg.NewFetchResponseTopicPartition()

		resp.Topics = append(resp.Topics, rt)
		rtp := &resp.Topics[0]
		rtp.Partitions = append(rtp.Partitions, rp)
		rpp := &rtp.Partitions[0]

		rpp.Partition = 0
		rpp.ErrorCode = 0
		rpp.HighWatermark = pi.HighWatermark
		rpp.LastStableOffset = pi.LastStableOffset
		rpp.LogStartOffset = 0

		// Always return the other broker as preferred replica
		// This forces continuous back-and-forth migrations
		if c.CurrentNode() == pi.Leader {
			rpp.PreferredReadReplica = follower
		} else {
			rpp.PreferredReadReplica = pi.Leader
		}

		return resp, nil, true
	})

	var wg sync.WaitGroup
	defer wg.Wait()
	start := time.Now()
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			cl, err := kgo.NewClient(
				kgo.SeedBrokers(c.ListenAddrs()...),
				kgo.ConsumeTopics(testTopic),
				kgo.Rack("test-rack"),                 // Enable rack-aware consuming
				kgo.DisableFetchSessions(),            // Force more fetch requests
				kgo.FetchMaxWait(10*time.Millisecond), // Minimum allowed
			)
			if err != nil {
				t.Errorf("consumer: failed to create client: %v", err)
				return
			}
			defer cl.Close()

			for time.Since(start) < 250*time.Millisecond {
				_ = cl.PollFetches(nil)
			}
		}()
	}
}

func TestTransactionCommit(t *testing.T) {
	t.Parallel()
	const testTopic = "txn-test"

	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(1, testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test basic transaction commit flow
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(testTopic),
		kgo.TransactionalID("test-txn"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	// Produce messages in a transaction
	if err := cl.BeginTransaction(); err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	for i := 0; i < 3; i++ {
		if err := cl.ProduceSync(ctx, kgo.StringRecord("msg-"+strconv.Itoa(i))).FirstErr(); err != nil {
			t.Fatalf("failed to produce: %v", err)
		}
	}

	// Commit the transaction
	if err := cl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatalf("failed to commit transaction: %v", err)
	}

	// Verify read_committed consumer sees the messages
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics(testTopic),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	var consumed int
	for consumed < 3 {
		fs := consumer.PollFetches(ctx)
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("fetch errors: %v", errs)
		}
		consumed += fs.NumRecords()
	}

	if consumed != 3 {
		t.Errorf("expected 3 committed messages, got %d", consumed)
	}
}

// TestTransactSessionEndNeverJoined ensures GroupTransactSession.End returns
// when the group has never joined. The session consumes a topic that does not
// exist, so the group manage loop never starts and nothing runs a heartbeat
// loop. A produce-only End(TryCommit) then commits no offsets; End used to
// force a heartbeat regardless, blocking forever on a channel with no
// receiver and no possible revoke/lost escape.
func TestTransactSessionEndNeverJoined(t *testing.T) {
	t.Parallel()
	const produceTopic = "txn-end-never-joined"

	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(1, produceTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	sess, err := kgo.NewGroupTransactSession(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(produceTopic),
		kgo.TransactionalID("txn-end-never-joined"),
		kgo.ConsumerGroup("g-txn-end-never-joined"),
		kgo.ConsumeTopics("topic-that-never-exists"),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer sess.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := sess.Begin(); err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}
	if err := sess.ProduceSync(ctx, kgo.StringRecord("v")).FirstErr(); err != nil {
		t.Fatalf("failed to produce: %v", err)
	}

	type endRes struct {
		committed bool
		err       error
	}
	done := make(chan endRes, 1)
	go func() {
		committed, err := sess.End(ctx, kgo.TryCommit)
		done <- endRes{committed, err}
	}()
	select {
	case res := <-done:
		if res.err != nil {
			t.Fatalf("End errored: %v", res.err)
		}
		if !res.committed {
			t.Error("expected the produce-only transaction to commit")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("End did not return; the pre-EndTxn heartbeat force hung with no group joined")
	}
}

func TestTransactionAbort(t *testing.T) {
	t.Parallel()
	const testTopic = "txn-abort-test"

	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(1, testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Test transaction abort flow
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(testTopic),
		kgo.TransactionalID("test-txn-abort"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	// Produce messages in a transaction
	if err := cl.BeginTransaction(); err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	for i := 0; i < 3; i++ {
		if err := cl.ProduceSync(ctx, kgo.StringRecord("aborted-"+strconv.Itoa(i))).FirstErr(); err != nil {
			t.Fatalf("failed to produce: %v", err)
		}
	}

	// Abort the transaction
	if err := cl.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatalf("failed to abort transaction: %v", err)
	}

	// Produce non-transactional messages after the abort
	nonTxnProducer, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer nonTxnProducer.Close()

	for i := 0; i < 2; i++ {
		if err := nonTxnProducer.ProduceSync(ctx, kgo.StringRecord("committed-"+strconv.Itoa(i))).FirstErr(); err != nil {
			t.Fatalf("failed to produce non-txn: %v", err)
		}
	}

	// Verify LSO advanced past the aborted transaction.
	pi := c.PartitionInfo(testTopic, 0)
	if pi.LastStableOffset != pi.HighWatermark {
		t.Errorf("LSO should equal HWM after abort, got LSO=%d HWM=%d", pi.LastStableOffset, pi.HighWatermark)
	}

	// Verify read_committed consumer sees only the non-aborted messages
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics(testTopic),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	var consumed int
	var records []string
	for consumed < 2 {
		fs := consumer.PollFetches(ctx)
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("fetch errors: %v", errs)
		}
		fs.EachRecord(func(r *kgo.Record) {
			records = append(records, string(r.Value))
		})
		consumed += fs.NumRecords()
	}

	// Verify we only got the committed messages, not the aborted ones
	if consumed != 2 {
		t.Errorf("expected 2 committed messages, got %d", consumed)
	}
	for _, rec := range records {
		if len(rec) >= 7 && rec[:7] == "aborted" {
			t.Errorf("read_committed consumer saw aborted message: %s", rec)
		}
	}
}

func TestTransactionReadUncommitted(t *testing.T) {
	t.Parallel()
	const testTopic = "txn-uncommitted-test"

	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(1, testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Start a read_uncommitted consumer first
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics(testTopic),
		kgo.FetchIsolationLevel(kgo.ReadUncommitted()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	// Produce messages in a transaction but don't commit yet
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(testTopic),
		kgo.TransactionalID("test-txn-uncommitted"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	if err := producer.BeginTransaction(); err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	for i := 0; i < 3; i++ {
		if err := producer.ProduceSync(ctx, kgo.StringRecord("msg-"+strconv.Itoa(i))).FirstErr(); err != nil {
			t.Fatalf("failed to produce: %v", err)
		}
	}

	// Read_uncommitted consumer should see the uncommitted messages immediately
	var consumed int
	for consumed < 3 {
		fs := consumer.PollFetches(ctx)
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("fetch errors: %v", errs)
		}
		consumed += fs.NumRecords()
	}

	if consumed != 3 {
		t.Errorf("expected read_uncommitted to see 3 messages, got %d", consumed)
	}

	// Clean up - abort the transaction
	if err := producer.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatalf("failed to abort transaction: %v", err)
	}
}

func TestTransactionOffsetCommit(t *testing.T) {
	t.Parallel()
	const (
		inputTopic  = "txn-input"
		outputTopic = "txn-output"
		groupID     = "txn-group"
	)

	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(1, inputTopic, outputTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Produce some input messages first
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(inputTopic),
	)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < 5; i++ {
		if err := producer.ProduceSync(ctx, kgo.StringRecord("input-"+strconv.Itoa(i))).FirstErr(); err != nil {
			t.Fatalf("failed to produce input: %v", err)
		}
	}
	producer.Close()

	// Test transactional consume-transform-produce pattern
	txnCtx := context.WithValue(context.Background(), "opt_in_kafka_next_gen_balancer_beta", true)
	txnClient, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.WithContext(txnCtx),
		kgo.ConsumerGroup(groupID),
		kgo.ConsumeTopics(inputTopic),
		kgo.TransactionalID("test-txn-offsets"),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer txnClient.Close()

	// Begin transaction
	if err := txnClient.BeginTransaction(); err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	// Consume and transform
	fs := txnClient.PollFetches(ctx)
	if errs := fs.Errors(); len(errs) > 0 {
		t.Fatalf("fetch errors: %v", errs)
	}

	consumed := fs.NumRecords()
	if consumed == 0 {
		t.Fatal("expected to consume some records")
	}

	// Produce transformed records
	iter := fs.RecordIter()
	for !iter.Done() {
		rec := iter.Next()
		outRec := &kgo.Record{
			Topic: outputTopic,
			Value: append([]byte("transformed-"), rec.Value...),
		}
		if err := txnClient.ProduceSync(ctx, outRec).FirstErr(); err != nil {
			t.Fatalf("failed to produce output: %v", err)
		}
	}

	// Commit offsets and transaction together
	if err := txnClient.CommitRecords(ctx, fs.Records()...); err != nil {
		t.Fatalf("failed to commit offsets: %v", err)
	}

	if err := txnClient.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatalf("failed to commit transaction: %v", err)
	}

	// Verify the offsets were committed
	adm := kadm.NewClient(txnClient)
	offsets, err := adm.FetchOffsets(ctx, groupID)
	if err != nil {
		t.Fatalf("failed to fetch offsets: %v", err)
	}

	offset, ok := offsets.Lookup(inputTopic, 0)
	if !ok {
		t.Fatal("offset not found for input topic")
	}

	if offset.At != int64(consumed) {
		t.Errorf("expected committed offset %d, got %d", consumed, offset.At)
	}

	// Verify output messages are readable
	outConsumer, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics(outputTopic),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer outConsumer.Close()

	outFs := outConsumer.PollFetches(ctx)
	if outFs.NumRecords() != consumed {
		t.Errorf("expected %d output records, got %d", consumed, outFs.NumRecords())
	}
}

// TestReadCommittedMinBytes verifies that readCommitted consumers respect MinBytes
// when transactions commit. Specifically:
// 1. Uncommitted transactional data should NOT satisfy MinBytes
// 2. When a transaction commits, the committed bytes should now count toward MinBytes
// 3. If committed bytes satisfy MinBytes, the watcher should fire
//
// This test uses ControlKey to intercept fetch requests and verify timing behavior.
func TestReadCommittedMinBytes(t *testing.T) {
	t.Parallel()
	const testTopic = "minbytes-test"

	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(1, testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(testTopic),
		kgo.TransactionalID("test-txn-minbytes"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	// Committed data should satisfy MinBytes immediately.
	if err := producer.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := producer.ProduceSync(ctx, &kgo.Record{Value: []byte("committed-data")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := producer.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}

	pi := c.PartitionInfo(testTopic, 0)
	if pi.LastStableOffset != pi.HighWatermark {
		t.Fatalf("LSO should equal HWM after commit: LSO=%d, HWM=%d", pi.LastStableOffset, pi.HighWatermark)
	}

	consumer1, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics(testTopic),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.FetchMinBytes(1),
		kgo.FetchMaxWait(5*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now()
	fetches := consumer1.PollFetches(ctx)
	elapsed := time.Since(start)
	consumer1.Close()

	if errs := fetches.Errors(); len(errs) > 0 {
		t.Fatalf("fetch errors: %v", errs)
	}
	if fetches.NumRecords() != 1 {
		t.Fatalf("expected 1 committed record, got %d", fetches.NumRecords())
	}
	if elapsed > time.Second {
		t.Fatalf("readCommitted fetch of committed data took %v, expected immediate", elapsed)
	}

	// Uncommitted data should NOT satisfy MinBytes for readCommitted.
	// The consumer should block until the context expires.
	hwmBeforeTxn := c.PartitionInfo(testTopic, 0).HighWatermark

	if err := producer.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := producer.ProduceSync(ctx, &kgo.Record{Value: []byte("uncommitted-data")}).FirstErr(); err != nil {
		t.Fatal(err)
	}

	pi = c.PartitionInfo(testTopic, 0)
	if pi.LastStableOffset != hwmBeforeTxn {
		t.Fatalf("LSO should not advance for uncommitted txn: LSO=%d, expected=%d", pi.LastStableOffset, hwmBeforeTxn)
	}

	shortCtx, shortCancel := context.WithTimeout(ctx, 250*time.Millisecond)
	consumer2, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.FetchMinBytes(1),
		kgo.FetchMaxWait(5*time.Second),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			testTopic: {0: kgo.NewOffset().At(hwmBeforeTxn)},
		}),
	)
	if err != nil {
		shortCancel()
		t.Fatal(err)
	}

	start = time.Now()
	fetches = consumer2.PollFetches(shortCtx)
	elapsed = time.Since(start)
	shortCancel()
	consumer2.Close()

	if elapsed < 150*time.Millisecond {
		t.Fatalf("readCommitted returned too quickly for uncommitted data: %v", elapsed)
	}
	if fetches.NumRecords() > 0 {
		t.Fatalf("readCommitted should not see uncommitted data, got %d records", fetches.NumRecords())
	}

	// After committing, a waiting readCommitted consumer should wake up
	// and return the newly committed data.

	// Use a ControlKey observer to know when the consumer's fetch request
	// arrives at the server, then commit. This avoids a flaky sleep.
	fetchArrived := make(chan struct{}, 1)
	c.ControlKey(int16(kmsg.Fetch), func(kmsg.Request) (kmsg.Response, error, bool) {
		select {
		case fetchArrived <- struct{}{}:
		default:
		}
		c.KeepControl()
		return nil, nil, false
	})

	consumer3, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.FetchMinBytes(1),
		kgo.FetchMaxWait(5*time.Second),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			testTopic: {0: kgo.NewOffset().At(hwmBeforeTxn)},
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	fetchDone := make(chan struct{})
	var fetchResult kgo.Fetches
	var fetchElapsed time.Duration
	go func() {
		start := time.Now()
		fetchResult = consumer3.PollFetches(ctx)
		fetchElapsed = time.Since(start)
		close(fetchDone)
	}()

	// Wait for the consumer's fetch to arrive at the server, then commit.
	select {
	case <-fetchArrived:
	case <-ctx.Done():
		t.Fatal("timeout waiting for fetch request to arrive")
	}

	if err := producer.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}

	select {
	case <-fetchDone:
	case <-ctx.Done():
		t.Fatal("timeout waiting for fetch after commit")
	}
	consumer3.Close()

	if fetchElapsed > 2*time.Second {
		t.Fatalf("readCommitted fetch took %v after commit, expected prompt wakeup", fetchElapsed)
	}
	if fetchResult.NumRecords() != 1 {
		t.Fatalf("expected 1 record after commit, got %d", fetchResult.NumRecords())
	}

	// Baseline: read_uncommitted should see uncommitted data immediately.
	if err := producer.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := producer.ProduceSync(ctx, &kgo.Record{Value: []byte("more-uncommitted")}).FirstErr(); err != nil {
		t.Fatal(err)
	}

	newLSO := c.PartitionInfo(testTopic, 0).LastStableOffset

	consumer4, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.FetchIsolationLevel(kgo.ReadUncommitted()),
		kgo.FetchMinBytes(1),
		kgo.FetchMaxWait(5*time.Second),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			testTopic: {0: kgo.NewOffset().At(newLSO)},
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	start = time.Now()
	fetches = consumer4.PollFetches(ctx)
	elapsed = time.Since(start)
	consumer4.Close()

	if elapsed > time.Second {
		t.Fatalf("read_uncommitted took %v, expected immediate", elapsed)
	}
	if fetches.NumRecords() == 0 {
		t.Fatal("read_uncommitted should see uncommitted data")
	}

	if err := producer.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatalf("cleanup abort: %v", err)
	}
}

// TestGroupRebalanceOnNonLeaderMetadataChange tests that a rebalance is
// triggered when a non-leader member rejoins with changed metadata.
//
// This test directly sends JoinGroup requests to verify the server behavior.
func TestGroupRebalanceOnNonLeaderMetadataChange(t *testing.T) {
	t.Parallel()
	const testTopic = "rebalance-test"

	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(2, testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	groupID := "test-group-" + strconv.FormatInt(time.Now().UnixNano(), 36)

	cl1, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl1.Close()
	cl2, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl2.Close()

	joinGroup := func(cl *kgo.Client, memberID string, metadata []byte) (*kmsg.JoinGroupResponse, error) {
		req := kmsg.NewJoinGroupRequest()
		req.Group = groupID
		req.MemberID = memberID
		req.ProtocolType = "consumer"
		req.SessionTimeoutMillis = 30000
		req.RebalanceTimeoutMillis = 60000
		proto := kmsg.NewJoinGroupRequestProtocol()
		proto.Name = "cooperative-sticky"
		proto.Metadata = metadata
		req.Protocols = append(req.Protocols, proto)
		resp, err := req.RequestWith(ctx, cl)
		return resp, err
	}

	syncGroup := func(cl *kgo.Client, memberID string, gen int32, isLeader bool, assignment []byte) (*kmsg.SyncGroupResponse, error) {
		req := kmsg.NewSyncGroupRequest()
		req.Group = groupID
		req.MemberID = memberID
		req.Generation = gen
		if isLeader {
			assign := kmsg.NewSyncGroupRequestGroupAssignment()
			assign.MemberID = memberID
			assign.MemberAssignment = assignment
			req.GroupAssignment = append(req.GroupAssignment, assign)
		}
		return req.RequestWith(ctx, cl)
	}

	metadataV1 := []byte{0, 1, 2, 3}
	metadataV2 := []byte{0, 1, 2, 4} // changed metadata
	assignment := []byte{0, 0, 0, 0}

	// m1 joins, gets MemberIDRequired, rejoins with ID
	resp1, err := joinGroup(cl1, "", metadataV1)
	if err != nil {
		t.Fatal(err)
	}
	if resp1.ErrorCode != kerr.MemberIDRequired.Code {
		t.Fatalf("expected MemberIDRequired, got %v", kerr.ErrorForCode(resp1.ErrorCode))
	}
	m1ID := resp1.MemberID

	resp1, err = joinGroup(cl1, m1ID, metadataV1)
	if err != nil {
		t.Fatal(err)
	}
	if resp1.ErrorCode != 0 {
		t.Fatalf("m1 join error: %v", kerr.ErrorForCode(resp1.ErrorCode))
	}
	if resp1.LeaderID != m1ID {
		t.Fatalf("expected m1 to be leader, got %s", resp1.LeaderID)
	}
	gen1 := resp1.Generation

	// m1 syncs as leader
	syncResp, err := syncGroup(cl1, m1ID, gen1, true, assignment)
	if err != nil {
		t.Fatal(err)
	}
	if syncResp.ErrorCode != 0 {
		t.Fatalf("m1 sync error: %v", kerr.ErrorForCode(syncResp.ErrorCode))
	}

	// m2 joins (initial), gets MemberIDRequired
	resp2, err := joinGroup(cl2, "", metadataV1)
	if err != nil {
		t.Fatal(err)
	}
	if resp2.ErrorCode != kerr.MemberIDRequired.Code {
		t.Fatalf("expected MemberIDRequired for m2, got %v", kerr.ErrorForCode(resp2.ErrorCode))
	}
	m2ID := resp2.MemberID

	// m2 rejoins with ID, triggering rebalance. We must ensure m2's
	// JoinGroup arrives before m1's: if m1 (leader) arrives first, it
	// completes a rebalance with only itself, then m2's join starts a
	// new rebalance waiting for m1, who is already done - deadlock.
	m2Received := make(chan struct{}, 1)
	c.ControlKey(int16(kmsg.JoinGroup), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if kreq.(*kmsg.JoinGroupRequest).MemberID == m2ID {
			select {
			case m2Received <- struct{}{}:
			default:
			}
		}
		return nil, nil, false
	})

	var wg sync.WaitGroup
	var resp2Rebalance *kmsg.JoinGroupResponse
	var err2 error
	wg.Add(1)
	go func() {
		defer wg.Done()
		resp2Rebalance, err2 = joinGroup(cl2, m2ID, metadataV1)
	}()
	<-m2Received

	resp1Rebalance, err1 := joinGroup(cl1, m1ID, metadataV1)
	wg.Wait()

	if err1 != nil {
		t.Fatalf("m1 rejoin error: %v", err1)
	}
	if err2 != nil {
		t.Fatalf("m2 join error: %v", err2)
	}
	resp1 = resp1Rebalance
	resp2 = resp2Rebalance

	gen2 := resp1.Generation

	// Sync both members to reach Stable state.
	_, _ = syncGroup(cl1, m1ID, gen2, resp1.LeaderID == m1ID, assignment)
	if resp2 != nil {
		_, _ = syncGroup(cl2, m2ID, gen2, resp2.LeaderID == m2ID, assignment)
	}

	// THE CRITICAL TEST: m2 (non-leader) rejoins with CHANGED metadata
	// while group is Stable. With the bug, m2 would get the same generation
	// back (no rebalance). With the fix, a new rebalance is triggered.
	m2JoinReceived := make(chan struct{}, 1)
	c.ControlKey(int16(kmsg.JoinGroup), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if kreq.(*kmsg.JoinGroupRequest).MemberID == m2ID {
			select {
			case m2JoinReceived <- struct{}{}:
			default:
			}
		}
		return nil, nil, false
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		resp2, err = joinGroup(cl2, m2ID, metadataV2)
	}()

	// Wait for m2's JoinGroup to arrive at the server.
	<-m2JoinReceived

	select {
	case <-done:
		// m2's JoinGroup returned immediately - no rebalance was triggered (bug).
		if resp2 != nil && resp2.Generation == gen2 && resp2.ErrorCode == 0 {
			t.Errorf("m2 rejoined with changed metadata but got same generation %d (no rebalance triggered)", gen2)
		}
	default:
		// m2's JoinGroup is blocking - rebalance was triggered (correct).
		// Complete the rebalance by having m1 rejoin.
		if _, err := joinGroup(cl1, m1ID, metadataV1); err != nil {
			t.Fatalf("m1 rejoin error: %v", err)
		}
		<-done
		if resp2 == nil {
			t.Fatal("m2 response is nil after rebalance")
		}
		if resp2.Generation <= gen2 {
			t.Errorf("expected new generation > %d, got %d", gen2, resp2.Generation)
		}
	}
}

// TestKIP447RequireStable verifies that OffsetFetch with RequireStable=true
// returns UNSTABLE_OFFSET_COMMIT when there are pending transactional offset commits.
func TestKIP447RequireStable(t *testing.T) {
	t.Parallel()
	const (
		testTopic = "kip447-test"
		groupID   = "kip447-group"
		txnID     = "kip447-txn"
	)

	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(1, testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Commit an initial offset to create the group
	adminClient, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	adm := kadm.NewClient(adminClient)
	offsets := kadm.Offsets{}
	offsets.Add(kadm.Offset{Topic: testTopic, Partition: 0, At: 0})
	if err = adm.CommitAllOffsets(ctx, groupID, offsets); err != nil {
		t.Fatalf("CommitAllOffsets failed: %v", err)
	}
	adminClient.Close()

	rawClient, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer rawClient.Close()

	// InitProducerID
	initReq := kmsg.NewInitProducerIDRequest()
	txnIDStr := txnID
	initReq.TransactionalID = &txnIDStr
	initReq.TransactionTimeoutMillis = 60000
	initResp, err := initReq.RequestWith(ctx, rawClient)
	if err != nil {
		t.Fatalf("InitProducerID failed: %v", err)
	}
	if initResp.ErrorCode != 0 {
		t.Fatalf("InitProducerID error: %v", kerr.ErrorForCode(initResp.ErrorCode))
	}
	pid := initResp.ProducerID
	epoch := initResp.ProducerEpoch

	// AddOffsetsToTxn
	addOffsetsReq := kmsg.NewAddOffsetsToTxnRequest()
	addOffsetsReq.TransactionalID = txnID
	addOffsetsReq.ProducerID = pid
	addOffsetsReq.ProducerEpoch = epoch
	addOffsetsReq.Group = groupID
	addOffsetsResp, err := addOffsetsReq.RequestWith(ctx, rawClient)
	if err != nil {
		t.Fatalf("AddOffsetsToTxn failed: %v", err)
	}
	if addOffsetsResp.ErrorCode != 0 {
		t.Fatalf("AddOffsetsToTxn error: %v", kerr.ErrorForCode(addOffsetsResp.ErrorCode))
	}

	// TxnOffsetCommit to stage offset commits (not yet committed)
	txnCommitReq := kmsg.NewTxnOffsetCommitRequest()
	txnCommitReq.TransactionalID = txnID
	txnCommitReq.Group = groupID
	txnCommitReq.ProducerID = pid
	txnCommitReq.ProducerEpoch = epoch
	txnCommitReq.Generation = -1
	topic := kmsg.NewTxnOffsetCommitRequestTopic()
	topic.Topic = testTopic
	part := kmsg.NewTxnOffsetCommitRequestTopicPartition()
	part.Partition = 0
	part.Offset = 5
	topic.Partitions = append(topic.Partitions, part)
	txnCommitReq.Topics = append(txnCommitReq.Topics, topic)
	txnCommitResp, err := txnCommitReq.RequestWith(ctx, rawClient)
	if err != nil {
		t.Fatalf("TxnOffsetCommit failed: %v", err)
	}
	if len(txnCommitResp.Topics) > 0 && len(txnCommitResp.Topics[0].Partitions) > 0 {
		if ec := txnCommitResp.Topics[0].Partitions[0].ErrorCode; ec != 0 {
			t.Fatalf("TxnOffsetCommit partition error: %v", kerr.ErrorForCode(ec))
		}
	}

	// Use v8+ format with Groups field to work with auto-negotiated versions.
	// Set TopicID for v10+ where Topic is not serialized on the wire.
	ti := c.TopicInfo(testTopic)
	fetchReq := kmsg.NewOffsetFetchRequest()
	fetchReq.RequireStable = true
	rg := kmsg.NewOffsetFetchRequestGroup()
	rg.Group = groupID
	rgt := kmsg.NewOffsetFetchRequestGroupTopic()
	rgt.Topic = testTopic
	rgt.TopicID = ti.TopicID
	rgt.Partitions = []int32{0}
	rg.Topics = append(rg.Topics, rgt)
	fetchReq.Groups = append(fetchReq.Groups, rg)

	// Test 1: OffsetFetch with RequireStable=true should return UNSTABLE_OFFSET_COMMIT
	// per-partition (not per-group -- Kafka sets it on each partition).
	// Use Broker.Request to bypass the sharder, which retries retryable errors.
	kresp, err := rawClient.Broker(0).Request(ctx, &fetchReq)
	if err != nil {
		t.Fatalf("OffsetFetch RequireStable=true failed: %v", err)
	}
	fetchResp := kresp.(*kmsg.OffsetFetchResponse)
	if len(fetchResp.Groups) == 0 {
		t.Fatal("no groups in RequireStable=true response")
	}
	if len(fetchResp.Groups[0].Topics) == 0 || len(fetchResp.Groups[0].Topics[0].Partitions) == 0 {
		t.Fatal("no partitions in RequireStable=true response")
	}
	if ec := fetchResp.Groups[0].Topics[0].Partitions[0].ErrorCode; ec != kerr.UnstableOffsetCommit.Code {
		t.Errorf("Test 1: expected per-partition UNSTABLE_OFFSET_COMMIT (88), got error code %d", ec)
	}

	// Test 2: OffsetFetch with RequireStable=false should succeed (even with pending txn)
	fetchReq.RequireStable = false
	kresp, err = rawClient.Broker(0).Request(ctx, &fetchReq)
	if err != nil {
		t.Fatalf("OffsetFetch RequireStable=false failed: %v", err)
	}
	fetchResp = kresp.(*kmsg.OffsetFetchResponse)
	if len(fetchResp.Groups) == 0 {
		t.Fatal("no groups in RequireStable=false response")
	}
	if fetchResp.Groups[0].ErrorCode != 0 {
		t.Errorf("Test 2: expected no group error, got error code %d", fetchResp.Groups[0].ErrorCode)
	}
	if len(fetchResp.Groups[0].Topics) == 0 || len(fetchResp.Groups[0].Topics[0].Partitions) == 0 {
		t.Fatal("no partitions in RequireStable=false response")
	}
	if ec := fetchResp.Groups[0].Topics[0].Partitions[0].ErrorCode; ec != 0 {
		t.Errorf("Test 2: expected no partition error, got error code %d", ec)
	}

	// Commit the transaction.
	endReq := kmsg.NewEndTxnRequest()
	endReq.TransactionalID = txnID
	endReq.ProducerID = pid
	endReq.ProducerEpoch = epoch
	endReq.Commit = true
	endResp, err := endReq.RequestWith(ctx, rawClient)
	if err != nil {
		t.Fatalf("EndTxn failed: %v", err)
	}
	if endResp.ErrorCode != 0 {
		t.Fatalf("EndTxn error: %v", kerr.ErrorForCode(endResp.ErrorCode))
	}
	// Test 3: OffsetFetch with RequireStable=true should now succeed
	fetchReq.RequireStable = true
	kresp, err = rawClient.Broker(0).Request(ctx, &fetchReq)
	if err != nil {
		t.Fatalf("OffsetFetch post-commit failed: %v", err)
	}
	fetchResp = kresp.(*kmsg.OffsetFetchResponse)
	if len(fetchResp.Groups) == 0 {
		t.Fatal("no groups in post-commit response")
	}
	if fetchResp.Groups[0].ErrorCode != 0 {
		t.Errorf("Test 3: expected no error after commit, got error code %d", fetchResp.Groups[0].ErrorCode)
	}
	if len(fetchResp.Groups[0].Topics) == 0 || len(fetchResp.Groups[0].Topics[0].Partitions) == 0 {
		t.Fatal("no partitions in post-commit response")
	}
	if offset := fetchResp.Groups[0].Topics[0].Partitions[0].Offset; offset != 5 {
		t.Errorf("expected committed offset 5, got %d", offset)
	}
}

// TestFirstMetadataPartitionErrors verifies that the client re-fetches metadata
// when the first metadata load returns per-partition errors (LEADER_NOT_AVAILABLE)
// on all partitions of a topic.
//
// This reproduces an issue where, on first metadata load, per-partition errors
// were not checked in the new-partitions code path (mergeTopicPartitions), so
// retryWhy was never populated and metadata was never re-fetched. This caused
// producers to stall indefinitely.
func TestFirstMetadataPartitionErrors(t *testing.T) {
	const testTopic = "foo"

	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(1, testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ti := c.TopicInfo(testTopic)

	// Intercept metadata: the first request for our topic returns
	// LEADER_NOT_AVAILABLE on all partitions. The control is consumed
	// after handling one request, so subsequent requests are handled
	// normally by the cluster.
	c.ControlKey(int16(kmsg.Metadata), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		req := kreq.(*kmsg.MetadataRequest)

		if len(req.Topics) == 0 {
			c.KeepControl()
			return nil, nil, false
		}

		// First metadata request with topics: return
		// LEADER_NOT_AVAILABLE on all partitions.
		resp := req.ResponseKind().(*kmsg.MetadataResponse)

		host, portStr, _ := net.SplitHostPort(c.ListenAddrs()[0])
		port, _ := strconv.Atoi(portStr)
		sb := kmsg.NewMetadataResponseBroker()
		sb.NodeID = 0
		sb.Host = host
		sb.Port = int32(port)
		resp.Brokers = append(resp.Brokers, sb)
		resp.ControllerID = 0

		st := kmsg.NewMetadataResponseTopic()
		st.Topic = kmsg.StringPtr(testTopic)
		st.TopicID = ti.TopicID
		sp := kmsg.NewMetadataResponseTopicPartition()
		sp.Partition = 0
		sp.ErrorCode = kerr.LeaderNotAvailable.Code
		sp.Leader = -1
		sp.LeaderEpoch = 0
		sp.Replicas = []int32{0}
		sp.ISR = []int32{0}
		st.Partitions = append(st.Partitions, sp)
		resp.Topics = append(resp.Topics, st)

		return resp, nil, true
	})

	cl, err := kgo.NewClient(
		kgo.DefaultProduceTopic(testTopic),
		kgo.SeedBrokers(c.ListenAddrs()...),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	// The first produce triggers metadata for the topic. Since the first
	// metadata returns LEADER_NOT_AVAILABLE, there are no writable
	// partitions yet.
	//
	// Without the fix, metadata is never re-fetched (retryWhy is not
	// populated for new partitions with errors), so producing is stuck
	// until the default metadata max age (5m).
	//
	// With the fix, retryWhy is populated and metadata is internally
	// retried within ~250ms, getting valid partition data.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var lastErr error
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		lastErr = cl.ProduceSync(ctx, kgo.StringRecord("test")).FirstErr()
		if lastErr == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("produce never succeeded (metadata was likely not re-fetched after first load error): %v", lastErr)
	}
}

// TestIssue1331 verifies that the leaderEpoch=-1 "no leader" sentinel is never
// accepted as a partition's leader epoch, even when the leaderless window spans
// more than maxEpochRewinds (5) metadata refreshes.
//
// When a partition briefly loses its leader (e.g. every replica restarts in a
// full cluster bounce), the broker advertises leaderEpoch=-1. franz-go used to
// treat -1 < oldEpoch as a leader-epoch rewind and count it toward the
// maxEpochRewinds=5 valve; after 5 leaderless refreshes it would "keep the
// metadata to allow forward progress" and accept -1 as the partition's epoch.
// That clobbers the cursor's real epoch: a cursor at epoch -1 opts out of
// KIP-320 fencing, so migrateCursorTo skips OffsetForLeaderEpoch validation (a
// real truncation goes undetected) and the consumer fetches with
// currentLeaderEpoch -1 (never fenced) and stalls. The client must instead keep
// its last known real epoch and wait for a real leader to reappear.
func TestIssue1331(t *testing.T) {
	t.Parallel()
	const (
		testTopic = "foo"
		nrecs     = 5
	)

	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(1, testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ti := c.TopicInfo(testTopic)
	pi := c.PartitionInfo(testTopic, 0)
	host, portStr, _ := net.SplitHostPort(c.ListenAddrs()[0])
	port, _ := strconv.Atoi(portStr)

	// Produce a few records so the partition has a real, non-negative leader
	// epoch for the consumer to consume and validate against.
	func() {
		cl, err := kgo.NewClient(
			kgo.DefaultProduceTopic(testTopic),
			kgo.SeedBrokers(c.ListenAddrs()...),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()
		for i := 0; i < nrecs; i++ {
			if err := cl.ProduceSync(context.Background(), kgo.StringRecord(strconv.Itoa(i))).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
	}()

	// Once armed, every metadata refresh for our topic reports the partition
	// as leaderless (leaderEpoch=-1) while keeping the leader reachable. We
	// count the refreshes so we can wait until the leaderless window has
	// clearly spanned past maxEpochRewinds (5).
	var (
		armed atomic.Bool
		fires atomic.Int64
	)
	c.ControlKey(int16(kmsg.Metadata), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if !armed.Load() {
			return nil, nil, false
		}
		req := kreq.(*kmsg.MetadataRequest)
		wants := len(req.Topics) == 0
		for _, rt := range req.Topics {
			if (rt.Topic != nil && *rt.Topic == testTopic) || rt.TopicID == ti.TopicID {
				wants = true
			}
		}
		if !wants {
			return nil, nil, false
		}
		fires.Add(1)

		resp := req.ResponseKind().(*kmsg.MetadataResponse)
		sb := kmsg.NewMetadataResponseBroker()
		sb.NodeID = pi.Leader
		sb.Host = host
		sb.Port = int32(port)
		resp.Brokers = append(resp.Brokers, sb)
		resp.ControllerID = pi.Leader

		st := kmsg.NewMetadataResponseTopic()
		st.Topic = kmsg.StringPtr(testTopic)
		st.TopicID = ti.TopicID
		sp := kmsg.NewMetadataResponseTopicPartition()
		sp.Partition = 0
		sp.Leader = pi.Leader
		sp.LeaderEpoch = -1 // the "no leader" sentinel
		sp.Replicas = []int32{pi.Leader}
		sp.ISR = []int32{pi.Leader}
		st.Partitions = append(st.Partitions, sp)
		resp.Topics = append(resp.Topics, st)
		return resp, nil, true
	})

	// Refresh metadata frequently so the leaderless window spans many
	// refreshes quickly.
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics(testTopic),
		kgo.MetadataMaxAge(100*time.Millisecond),
		kgo.MetadataMinAge(20*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	// Consume the seeded records so the cursor has a real consumed epoch.
	for consumed := 0; consumed < nrecs; {
		fs := cl.PollFetches(ctx)
		if errs := fs.Errors(); errs != nil {
			t.Fatalf("consume error: %v", errs)
		}
		consumed += fs.NumRecords()
	}

	// Baseline: the client must have picked up a real (non-negative) leader
	// epoch. A -1 here means kfake / the client are not using leader epochs,
	// which would invalidate the test premise.
	if _, epoch, err := cl.PartitionLeader(testTopic, 0); err != nil || epoch < 0 {
		t.Fatalf("baseline PartitionLeader epoch = %d, err = %v; want a real non-negative epoch", epoch, err)
	}

	// Arm the leaderless window and wait until it spans well past
	// maxEpochRewinds (5) refreshes.
	armed.Store(true)
	deadline := time.Now().Add(15 * time.Second)
	for fires.Load() < 8 {
		if time.Now().After(deadline) {
			t.Fatalf("metadata only refreshed %d times under the -1 window, want >= 8", fires.Load())
		}
		time.Sleep(20 * time.Millisecond)
	}

	// The fix: -1 is never accepted, so the client keeps its real epoch.
	// Before the fix, the epoch is clobbered to -1 once the valve trips on
	// the 6th leaderless refresh.
	if _, epoch, err := cl.PartitionLeader(testTopic, 0); err != nil || epoch < 0 {
		t.Fatalf("during the leaderless window PartitionLeader epoch = %d, err = %v; want the real epoch kept (>= 0), not the -1 no-leader sentinel", epoch, err)
	}

	// Disarm and verify the consumer still makes forward progress: newly
	// produced records are consumed once a real leader epoch is reported again.
	armed.Store(false)
	prod, err := kgo.NewClient(
		kgo.DefaultProduceTopic(testTopic),
		kgo.SeedBrokers(c.ListenAddrs()...),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()
	for i := 0; i < nrecs; i++ {
		if err := prod.ProduceSync(ctx, kgo.StringRecord("after-"+strconv.Itoa(i))).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	for consumed := 0; consumed < nrecs; {
		fs := cl.PollFetches(ctx)
		if errs := fs.Errors(); errs != nil {
			t.Fatalf("post-window consume error: %v", errs)
		}
		consumed += fs.NumRecords()
	}
}

func TestRequestCachedMetadata(t *testing.T) {
	t.Parallel()
	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(2, "topic1", "topic2", "internal_topic"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.MetadataMinAge(5*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx := context.Background()
	adm := kadm.NewClient(cl)

	// Mark internal_topic as internal via kfake-specific config.
	v := "true"
	if _, err := adm.AlterTopicConfigsState(ctx, []kadm.AlterConfig{
		{Name: "kfake.is_internal", Value: &v},
	}, "internal_topic"); err != nil {
		t.Fatal(err)
	}

	// Count metadata requests. This ControlKey returns handled=false,
	// so it observes without intercepting - any later ControlKey for
	// Metadata (e.g., in StaleCacheResilience) can still fire.
	var metadataRequests atomic.Int32
	c.ControlKey(int16(kmsg.Metadata), func(kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		metadataRequests.Add(1)
		return nil, nil, false
	})

	metaReqForTopic := func(name string) *kmsg.MetadataRequest {
		req := kmsg.NewPtrMetadataRequest()
		rt := kmsg.NewMetadataRequestTopic()
		rt.Topic = kmsg.StringPtr(name)
		req.Topics = append(req.Topics, rt)
		return req
	}
	metaReqForTopicID := func(id [16]byte) *kmsg.MetadataRequest {
		req := kmsg.NewPtrMetadataRequest()
		rt := kmsg.NewMetadataRequestTopic()
		rt.TopicID = id
		req.Topics = append(req.Topics, rt)
		return req
	}
	assertCached := func(t *testing.T, req *kmsg.MetadataRequest) *kmsg.MetadataResponse {
		t.Helper()
		before := metadataRequests.Load()
		resp, err := cl.RequestCachedMetadata(ctx, req, 0)
		if err != nil {
			t.Fatalf("cached fetch: %v", err)
		}
		if metadataRequests.Load() != before {
			t.Fatalf("expected cached, got %d total (was %d)", metadataRequests.Load(), before)
		}
		return resp
	}

	t.Run("AllTopicsCaching", func(t *testing.T) {
		defer metadataRequests.Store(0)

		req := kmsg.NewPtrMetadataRequest()
		// nil Topics = all topics
		resp, err := cl.RequestCachedMetadata(ctx, req, 0)
		if err != nil {
			t.Fatalf("first all-topics fetch: %v", err)
		}

		if len(resp.Topics) != 3 {
			t.Fatalf("expected 3 topics, got %d", len(resp.Topics))
		}

		gotTopics := make(map[string]bool)
		for _, topic := range resp.Topics {
			if topic.Topic == nil {
				t.Fatal("topic name is nil")
			}
			gotTopics[*topic.Topic] = true
			var zeroID [16]byte
			if topic.TopicID == zeroID {
				t.Errorf("topic %s has zero TopicID", *topic.Topic)
			}
			if len(topic.Partitions) != 2 {
				t.Errorf("topic %s: expected 2 partitions, got %d", *topic.Topic, len(topic.Partitions))
			}
			// Verify IsInternal is set from kfake.is_internal config.
			if *topic.Topic == "internal_topic" && !topic.IsInternal {
				t.Error("internal_topic should be marked internal")
			}
			if (*topic.Topic == "topic1" || *topic.Topic == "topic2") && topic.IsInternal {
				t.Errorf("topic %s should not be internal", *topic.Topic)
			}
		}
		if !gotTopics["topic1"] || !gotTopics["topic2"] || !gotTopics["internal_topic"] {
			t.Fatalf("expected topic1, topic2, internal_topic, got %v", gotTopics)
		}

		if len(resp.Brokers) == 0 {
			t.Fatal("expected at least one broker")
		}

		if metadataRequests.Load() == 0 {
			t.Fatal("expected at least one metadata request")
		}

		// Second call within cache window should use cache.
		resp2 := assertCached(t, req)
		if len(resp2.Topics) != 3 {
			t.Fatalf("cached: expected 3 topics, got %d", len(resp2.Topics))
		}
	})

	t.Run("NoTopicsCaching", func(t *testing.T) {
		defer metadataRequests.Store(0)

		req := kmsg.NewPtrMetadataRequest()
		req.Topics = []kmsg.MetadataRequestTopic{} // empty = no topics

		// By this point the client already has broker info from previous
		// tests, so no-topics should not issue a metadata request at all.
		resp, err := cl.RequestCachedMetadata(ctx, req, 0)
		if err != nil {
			t.Fatalf("no-topics fetch: %v", err)
		}
		if len(resp.Brokers) == 0 {
			t.Fatal("expected brokers")
		}
		if resp.ControllerID < 0 {
			t.Fatal("expected valid controller ID")
		}

		// Second call should also not issue a request.
		resp2 := assertCached(t, req)
		if len(resp2.Brokers) == 0 {
			t.Fatal("cached: expected brokers")
		}
	})

	t.Run("TopicLookupCaching", func(t *testing.T) {
		ti := c.TopicInfo("topic1")
		if ti == nil {
			t.Fatal("topic1 not found in kfake")
		}
		for _, tt := range []struct {
			name string
			req  *kmsg.MetadataRequest
		}{
			{"ByName", metaReqForTopic("topic1")},
			{"ByTopicID", metaReqForTopicID(ti.TopicID)},
		} {
			t.Run(tt.name, func(t *testing.T) {
				defer metadataRequests.Store(0)
				resp, err := cl.RequestCachedMetadata(ctx, tt.req, 0)
				if err != nil {
					t.Fatal(err)
				}
				if len(resp.Topics) != 1 || resp.Topics[0].Topic == nil || *resp.Topics[0].Topic != "topic1" {
					t.Fatalf("expected 1 topic 'topic1', got %v", resp.Topics)
				}
				resp2 := assertCached(t, tt.req)
				if len(resp2.Topics) != 1 || *resp2.Topics[0].Topic != "topic1" {
					t.Fatal("cached: wrong topic")
				}
			})
		}
	})

	t.Run("TopicIDLookupUncached", func(t *testing.T) {
		defer metadataRequests.Store(0)

		// Create a new topic after the client's main metadata loop
		// has already run, so neither metaCache nor id2t know about it.
		adm := kadm.NewClient(cl)
		_, err := adm.CreateTopic(ctx, 1, 1, nil, "newTopic")
		if err != nil {
			t.Fatal(err)
		}

		ti := c.TopicInfo("newTopic")
		if ti == nil {
			t.Fatal("newTopic not found in kfake")
		}

		// Request newTopic by TopicID only  - not in any cache.
		idReq := metaReqForTopicID(ti.TopicID)

		resp, err := cl.RequestCachedMetadata(ctx, idReq, 0)
		if err != nil {
			t.Fatalf("uncached TopicID lookup: %v", err)
		}
		if len(resp.Topics) != 1 {
			t.Fatalf("expected 1 topic, got %d", len(resp.Topics))
		}
		if resp.Topics[0].Topic == nil || *resp.Topics[0].Topic != "newTopic" {
			t.Fatalf("expected newTopic, got %v", resp.Topics[0].Topic)
		}

		// Should have fetched from broker (at least 1 metadata request
		// for the ID resolution + potentially 1 for the name-based fetch).
		if metadataRequests.Load() < 1 {
			t.Fatal("expected at least one metadata request for uncached TopicID")
		}
	})

	// StaleCacheResilience verifies that a failed metadata re-fetch after
	// cache expiry does not destroy the previously cached data. The flow:
	// 1. Cache topic1 metadata with a short TTL.
	// 2. Wait for the cache entry to expire.
	// 3. Intercept the next metadata request to return an error.
	// 4. Attempt a fetch  - the error is returned, but the stale cache
	//    entry is preserved (we no longer delete on cache miss).
	// 5. Restore normal metadata handling and fetch again  - succeeds.
	t.Run("StaleCacheResilience", func(t *testing.T) {
		defer metadataRequests.Store(0)

		req := metaReqForTopic("topic1")

		// Step 1: populate cache.
		_, err := cl.RequestCachedMetadata(ctx, req, 100*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}

		// Step 2: wait for expiry.
		time.Sleep(150 * time.Millisecond)

		// Step 3: intercept next metadata to return an error response.
		// This ControlKey is consumed after one use (no KeepControl).
		c.ControlKey(int16(kmsg.Metadata), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
			metaReq := kreq.(*kmsg.MetadataRequest)
			resp := metaReq.ResponseKind().(*kmsg.MetadataResponse)
			rt := kmsg.NewMetadataResponseTopic()
			rt.Topic = kmsg.StringPtr("topic1")
			rt.ErrorCode = kerr.UnknownServerError.Code
			resp.Topics = append(resp.Topics, rt)
			return resp, nil, true
		})

		// Step 4: fetch - may get error data, but stale entry preserved.
		_, _ = cl.RequestCachedMetadata(ctx, req, 100*time.Millisecond)

		// Step 5: interceptor consumed, re-fetch goes to normal handler.

		time.Sleep(150 * time.Millisecond)
		resp, err := cl.RequestCachedMetadata(ctx, req, 100*time.Millisecond)
		if err != nil {
			t.Fatalf("fetch after failure: %v", err)
		}
		if len(resp.Topics) != 1 || *resp.Topics[0].Topic != "topic1" {
			t.Fatalf("expected topic1 after recovery, got %v", resp.Topics)
		}
	})

	// UnknownTopic verifies that requesting a non-existent topic by name
	// surfaces the broker's UnknownTopicOrPartition error in the response.
	t.Run("UnknownTopic", func(t *testing.T) {
		defer metadataRequests.Store(0)

		req := metaReqForTopic("no_such_topic")

		resp, err := cl.RequestCachedMetadata(ctx, req, 100*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Topics) != 1 {
			t.Fatalf("expected 1 topic, got %d", len(resp.Topics))
		}
		if resp.Topics[0].ErrorCode != kerr.UnknownTopicOrPartition.Code {
			t.Fatalf("expected UnknownTopicOrPartition, got error code %d", resp.Topics[0].ErrorCode)
		}
	})

	// ShardEviction verifies that a sharded request error (e.g.
	// NotLeaderForPartition from ListOffsets) evicts the topic from the
	// metadata cache, causing the next RequestCachedMetadata call to
	// re-fetch from the broker rather than serving stale data.
	t.Run("ShardEviction", func(t *testing.T) {
		defer metadataRequests.Store(0)

		// Step 1: populate cache for topic1.
		req := metaReqForTopic("topic1")
		if _, err := cl.RequestCachedMetadata(ctx, req, 5*time.Second); err != nil {
			t.Fatal(err)
		}
		countAfterCache := metadataRequests.Load()

		// Step 2: intercept ListOffsets to return NotLeaderForPartition,
		// which triggers maybeDeleteCachedMeta for the topic.
		c.ControlKey(int16(kmsg.ListOffsets), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
			lor := kreq.(*kmsg.ListOffsetsRequest)
			resp := lor.ResponseKind().(*kmsg.ListOffsetsResponse)
			for _, rt := range lor.Topics {
				st := kmsg.NewListOffsetsResponseTopic()
				st.Topic = rt.Topic
				for _, rp := range rt.Partitions {
					sp := kmsg.NewListOffsetsResponseTopicPartition()
					sp.Partition = rp.Partition
					sp.ErrorCode = kerr.NotLeaderForPartition.Code
					st.Partitions = append(st.Partitions, sp)
				}
				resp.Topics = append(resp.Topics, st)
			}
			return resp, nil, true
		})
		adm.ListStartOffsets(ctx, "topic1")

		// Step 3: the cache entry for topic1 should be evicted.
		// A new RequestCachedMetadata must re-fetch from the broker.
		resp, err := cl.RequestCachedMetadata(ctx, req, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if metadataRequests.Load() <= countAfterCache {
			t.Fatal("expected a new metadata request after shard eviction")
		}
		if len(resp.Topics) != 1 || *resp.Topics[0].Topic != "topic1" {
			t.Fatalf("expected topic1, got %v", resp.Topics)
		}
	})
}

func TestKadmCachedMetadata(t *testing.T) {
	t.Parallel()
	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(2, "t1", "t2", "t_internal"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.MetadataMinAge(5*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx := context.Background()
	adm := kadm.NewClient(cl)

	// Mark t_internal as internal via kfake-specific config.
	v := "true"
	if _, err := adm.AlterTopicConfigsState(ctx, []kadm.AlterConfig{
		{Name: "kfake.is_internal", Value: &v},
	}, "t_internal"); err != nil {
		t.Fatal(err)
	}

	var metadataRequests atomic.Int32
	c.ControlKey(int16(kmsg.Metadata), func(_ kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		metadataRequests.Add(1)
		return nil, nil, false
	})

	t.Run("MetadataAllTopics", func(t *testing.T) {
		defer metadataRequests.Store(0)

		m, err := adm.Metadata(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(m.Topics) != 3 {
			t.Fatalf("expected 3 topics, got %d: %v", len(m.Topics), m.Topics.Names())
		}
		for _, td := range m.Topics {
			var zeroID kadm.TopicID
			if td.ID == zeroID {
				t.Errorf("topic %s has zero ID", td.Topic)
			}
		}
	})

	t.Run("ListTopicsFiltersInternal", func(t *testing.T) {
		defer metadataRequests.Store(0)

		topics, err := adm.ListTopics(ctx)
		if err != nil {
			t.Fatal(err)
		}
		// t1 and t2 are not internal, should be present.
		if !topics.Has("t1") || !topics.Has("t2") {
			t.Fatalf("expected t1 and t2, got %v", topics.Names())
		}
		// t_internal should be filtered out by ListTopics.
		if topics.Has("t_internal") {
			t.Fatal("t_internal should be filtered by ListTopics")
		}
	})

	t.Run("BrokerMetadata", func(t *testing.T) {
		defer metadataRequests.Store(0)

		m, err := adm.BrokerMetadata(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if len(m.Brokers) == 0 {
			t.Fatal("expected brokers")
		}
	})

	t.Run("CachingReducesRequests", func(t *testing.T) {
		defer metadataRequests.Store(0)

		// First call.
		_, err := adm.Metadata(ctx)
		if err != nil {
			t.Fatal(err)
		}
		countAfterFirst := metadataRequests.Load()

		// Second call should be cached.
		_, err = adm.Metadata(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if metadataRequests.Load() != countAfterFirst {
			t.Fatalf("expected cached, got %d total (was %d)", metadataRequests.Load(), countAfterFirst)
		}
	})
}

// TestIssue1217 verifies that batches are "poisoned" when a produce response
// returns REQUEST_TIMED_OUT or NOT_ENOUGH_REPLICAS_AFTER_APPEND. When
// poisoned, the batch retries past the normal RecordRetries limit and cannot
// be canceled via context cancellation. This prevents data loss when the
// broker state is uncertain.
func TestIssue1217(t *testing.T) {
	t.Parallel()
	const testTopic = "foo"

	c, err := NewCluster(
		NumBrokers(1),
		SleepOutOfOrder(),
		SeedTopics(1, testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	produceErr := func(kreq kmsg.Request, errCode int16) (kmsg.Response, error, bool) {
		req := kreq.(*kmsg.ProduceRequest)
		resp := req.ResponseKind().(*kmsg.ProduceResponse)
		st := kmsg.NewProduceResponseTopic()
		st.Topic = req.Topics[0].Topic
		st.TopicID = req.Topics[0].TopicID
		sp := kmsg.NewProduceResponseTopicPartition()
		sp.Partition = req.Topics[0].Partitions[0].Partition
		sp.ErrorCode = errCode
		st.Partitions = append(st.Partitions, sp)
		resp.Topics = append(resp.Topics, st)
		return resp, nil, true
	}

	newClient := func(opts ...kgo.Opt) *kgo.Client {
		t.Helper()
		cl, err := kgo.NewClient(append([]kgo.Opt{
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.DefaultProduceTopic(testTopic),
			kgo.RetryBackoffFn(func(int) time.Duration { return 10 * time.Millisecond }),
			kgo.MetadataMinAge(10 * time.Millisecond),
		}, opts...)...)
		if err != nil {
			t.Fatal(err)
		}
		return cl
	}

	// With RecordRetries(1), the producer normally gives up after 1 retry.
	// After a poison error, retries bypass the limit. Flow:
	//   attempt 1: return poison error
	//   attempt 2: return NOT_LEADER_FOR_PARTITION (retriable)
	//   attempt 3: let kfake handle normally (success)
	for _, poisonErr := range []struct {
		name string
		code int16
	}{
		{"request_timed_out", kerr.RequestTimedOut.Code},
		{"not_enough_replicas_after_append", kerr.NotEnoughReplicasAfterAppend.Code},
	} {
		t.Run(poisonErr.name, func(t *testing.T) {
			var attempt atomic.Int32
			c.ControlKey(int16(kmsg.Produce), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
				c.KeepControl()
				switch attempt.Add(1) {
				case 1:
					return produceErr(kreq, poisonErr.code)
				case 2:
					return produceErr(kreq, kerr.NotLeaderForPartition.Code)
				default:
					return nil, nil, false
				}
			})

			cl := newClient(kgo.RecordRetries(1))
			defer cl.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			if err := cl.ProduceSync(ctx, kgo.StringRecord("poison-test")).FirstErr(); err != nil {
				t.Fatalf("produce should have succeeded after retries, got: %v", err)
			}
			if a := attempt.Load(); a < 3 {
				t.Fatalf("expected at least 3 produce attempts, got %d", a)
			}
		})
	}

	// After REQUEST_TIMED_OUT, repeated metadata load errors (triggering
	// bumpRepeatedLoadErr) should NOT fail the poisoned batch, even past
	// the RecordRetries limit.
	t.Run("load_errors_do_not_fail_poisoned_batch", func(t *testing.T) {
		ti := c.TopicInfo(testTopic)
		host, portStr, _ := net.SplitHostPort(c.ListenAddrs()[0])
		port, _ := strconv.Atoi(portStr)

		var produceAttempt atomic.Int32
		c.ControlKey(int16(kmsg.Produce), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
			c.KeepControl()
			if produceAttempt.Add(1) == 1 {
				return produceErr(kreq, kerr.RequestTimedOut.Code)
			}
			return nil, nil, false
		})

		// After the poison, return LEADER_NOT_AVAILABLE on metadata
		// to trigger bumpRepeatedLoadErr. We keep returning the error
		// more times than RecordRetries(1) to verify the limit is
		// bypassed.
		var metadataErrors atomic.Int32
		c.ControlKey(int16(kmsg.Metadata), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
			c.KeepControl()
			req := kreq.(*kmsg.MetadataRequest)
			if produceAttempt.Load() < 1 || len(req.Topics) == 0 {
				return nil, nil, false
			}
			if metadataErrors.Add(1) > 3 {
				return nil, nil, false
			}
			resp := req.ResponseKind().(*kmsg.MetadataResponse)
			sb := kmsg.NewMetadataResponseBroker()
			sb.NodeID = 0
			sb.Host = host
			sb.Port = int32(port)
			resp.Brokers = append(resp.Brokers, sb)
			resp.ControllerID = 0
			st := kmsg.NewMetadataResponseTopic()
			st.Topic = kmsg.StringPtr(testTopic)
			st.TopicID = ti.TopicID
			sp := kmsg.NewMetadataResponseTopicPartition()
			sp.Partition = 0
			sp.ErrorCode = kerr.LeaderNotAvailable.Code
			sp.Leader = -1
			sp.Replicas = []int32{0}
			sp.ISR = []int32{0}
			st.Partitions = append(st.Partitions, sp)
			resp.Topics = append(resp.Topics, st)
			return resp, nil, true
		})

		cl := newClient(kgo.RecordRetries(1))
		defer cl.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		if err := cl.ProduceSync(ctx, kgo.StringRecord("load-err-test")).FirstErr(); err != nil {
			t.Fatalf("produce should have survived load errors, got: %v", err)
		}
		if m := metadataErrors.Load(); m < 3 {
			t.Fatalf("expected at least 3 metadata errors, got %d", m)
		}
	})

	// After REQUEST_TIMED_OUT, canceling the record context should NOT
	// cancel the batch. The context is canceled after the first response
	// so that tryAddBatch sees the canceled context when building the
	// next produce request. Without the fix, tryAddBatch calls
	// maybeFailErr which detects the canceled context and fails the batch.
	t.Run("context_cancel_does_not_abort_poisoned_batch", func(t *testing.T) {
		var attempt atomic.Int32
		attemptOneDone := make(chan struct{})
		c.ControlKey(int16(kmsg.Produce), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
			c.KeepControl()
			if attempt.Add(1) == 1 {
				defer close(attemptOneDone)
				return produceErr(kreq, kerr.RequestTimedOut.Code)
			}
			return nil, nil, false
		})

		cl := newClient()
		defer cl.Close()

		recordCtx, recordCancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- cl.ProduceSync(recordCtx, kgo.StringRecord("cancel-test")).FirstErr()
		}()

		// Wait for the broker to process attempt 1 (REQUEST_TIMED_OUT),
		// then cancel the context. The client will process the response
		// (setting canFailFromLoadErrs=true) and then build the retry
		// request via tryAddBatch, which checks the context.
		<-attemptOneDone
		recordCancel()

		select {
		case err := <-done:
			if err != nil {
				t.Fatalf("produce should have succeeded despite context cancel, got: %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Fatal("timed out waiting for produce to complete")
		}
		if a := attempt.Load(); a < 2 {
			t.Fatalf("expected at least 2 produce attempts, got %d", a)
		}
	})
}

// TestIssue1245 verifies that idle connections are not reused after
// connIdleTimeout has elapsed. PR #1245 adds a check in loadConnection
// so that a connection idle for longer than connIdleTimeout is killed
// and a new connection is established, rather than reusing the stale one.
//
// The test is designed to exercise the loadConnection idle check rather
// than the background reap loop. With ConnIdleTimeout of 200ms, the reap
// loop ticks at t=200ms, 400ms, ... from client creation. The first
// produce fires immediately (~t=10ms), so the produce connection's
// lastRead/lastWrite is ~t=10ms. At the t=200ms reap tick the connection
// has been idle for only ~190ms which is NOT > 200ms, so it is not
// reaped. The second produce fires at ~t=310ms (idleTimeout + 100ms
// after the first produce returns), well before the t=400ms reap tick.
// At this point the produce connection has been idle for ~300ms > 200ms,
// so loadConnection's isIdleTimeout check catches it and creates a new
// connection.
func TestIssue1245(t *testing.T) {
	t.Parallel()
	const testTopic = "foo"

	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(1, testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Track the number of new broker connections.
	connects := new(atomic.Int32)

	idleTimeout := 200 * time.Millisecond

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(testTopic),
		kgo.ConnIdleTimeout(idleTimeout),
		kgo.ProducerLinger(0),
		// Set metadata min and max age to 1hr so that no background
		// metadata refreshes create or reuse connections during the test.
		kgo.MetadataMinAge(time.Hour),
		kgo.MetadataMaxAge(time.Hour),
		kgo.WithHooks(&connCountHook{connects}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// First produce: fires immediately after client creation (~t=10ms),
	// establishing a produce connection whose lastRead/lastWrite is set
	// to approximately now.
	if err := cl.ProduceSync(ctx, kgo.StringRecord("msg-1")).FirstErr(); err != nil {
		t.Fatalf("first produce failed: %v", err)
	}

	connectsAfterFirst := connects.Load()
	if connectsAfterFirst == 0 {
		t.Fatal("expected at least 1 connection after first produce")
	}

	// Sleep idleTimeout + 100ms. This is timed so that:
	// - The reap loop at t=200ms sees the produce connection idle for
	//   ~190ms (NOT > 200ms), so it does NOT reap it.
	// - The second produce fires at ~t=310ms, well before the t=400ms
	//   reap tick.
	// - At ~t=310ms the produce connection has been idle for ~300ms >
	//   200ms, so loadConnection's isIdleTimeout check catches it and
	//   forces a new connection to be created.
	time.Sleep(idleTimeout + 100*time.Millisecond)

	// Second produce: loadConnection detects the idle produce connection,
	// kills it, and creates a new one.
	if err := cl.ProduceSync(ctx, kgo.StringRecord("msg-2")).FirstErr(); err != nil {
		t.Fatalf("second produce failed: %v", err)
	}

	// OnBrokerConnect is only called for new connections, so any increase
	// in the counter means a new connection was established.
	if connects.Load() <= connectsAfterFirst {
		t.Fatal("second produce reused the same connection; expected a new connection to be created")
	}
}

// TestIssue1248 verifies that closing a client whose parent context is
// already canceled does not race. LeaveGroupContext can return early via
// ctx.Done() while its goroutine is still running stopSession ->
// allSinksAndSources(reset). Without the fix, killSessionOnClose
// goroutines concurrently call session.kill(), racing on fetchSession
// fields. Run with -race to detect.
func TestIssue1248(t *testing.T) {
	t.Parallel()
	c, err := NewCluster(NumBrokers(1), SeedTopics(1, "t1248"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Produce records for the consumer.
	func() {
		cl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.DefaultProduceTopic("t1248"),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()
		for i := 0; i < 10; i++ {
			if err := cl.ProduceSync(context.Background(), kgo.StringRecord("v")).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
	}()

	// Run multiple iterations to increase the chance of the race
	// detector catching the concurrent reset()/kill() access.
	for i := 0; i < 20; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		cl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.WithContext(ctx),
			kgo.ConsumerGroup("g1248-"+strconv.Itoa(i)),
			kgo.ConsumeTopics("t1248"),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.FetchMaxWait(250*time.Millisecond),
		)
		if err != nil {
			t.Fatal(err)
		}

		// Poll until we get records, establishing fetch sessions.
		pollCtx, pollCancel := context.WithTimeout(context.Background(), 5*time.Second)
		for {
			fs := cl.PollFetches(pollCtx)
			if pollCtx.Err() != nil {
				pollCancel()
				t.Fatal("timed out waiting for records")
			}
			if fs.NumRecords() > 0 {
				break
			}
		}
		pollCancel()

		// Cancel the client context, then Close. LeaveGroupContext
		// returns immediately via ctx.Done() while its goroutine is
		// still running stopSession -> reset(). Concurrently,
		// killSessionOnClose calls session.kill().
		cancel()
		cl.Close()
	}
}

// TestDeleteRecordsThenProduce verifies that producing after deleting all
// records does not panic. trimLeft must properly clean up segment state
// so that a subsequent pushBatch works correctly.
func TestDeleteRecordsThenProduce(t *testing.T) {
	t.Parallel()
	const topic = "delete-then-produce"

	c, err := NewCluster(NumBrokers(1), SeedTopics(1, topic))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(topic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Step 1: Produce some records to create batches.
	for i := 0; i < 5; i++ {
		if err := cl.ProduceSync(ctx, kgo.StringRecord("v")).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	// Step 2: Delete all records (sets logStartOffset to HWM, trimLeft
	// removes all batches and segment files).
	adm := kadm.NewClient(cl)
	deleteOffsets := kadm.Offsets{}
	deleteOffsets.Add(kadm.Offset{Topic: topic, Partition: 0, At: -1}) // -1 = high watermark
	_, err = adm.DeleteRecords(ctx, deleteOffsets)
	if err != nil {
		t.Fatalf("delete records: %v", err)
	}

	// Step 3: Produce again. Before the fix, pushBatch would panic with
	// "index out of range" accessing stale segment state.
	for i := 0; i < 5; i++ {
		if err := cl.ProduceSync(ctx, kgo.StringRecord("v")).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
}

// connCountHook implements HookBrokerConnect to count new connections.
type connCountHook struct {
	connects *atomic.Int32
}

func (h *connCountHook) OnBrokerConnect(_ kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	if err == nil {
		h.connects.Add(1)
	}
}

// TestIssue1281 verifies that kfake accepts an idempotent produce for an
// unknown (PID, epoch) pair, implicitly creating producer state on demand.
// Apache Kafka and Redpanda do this; prior to the fix kfake returned
// INVALID_TXN_STATE.
func TestIssue1281(t *testing.T) {
	t.Parallel()
	topic := "t-issue-1281"

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	v := kversion.Stable()
	v.SetMaxKeyVersion(0, 9) // Produce v9: uses topic names, not topic IDs
	cl := newPlainClient(t, c, kgo.MaxVersions(v))
	ctx := context.Background()

	makeBatch := func(pid int64, epoch int16, seq int32, numRecs int32) []byte {
		var recs []byte
		for i := range numRecs {
			rec := kmsg.Record{
				Key:            []byte("k"),
				Value:          []byte("v"),
				TimestampDelta: i,
			}
			rec.Length = int32(len(rec.AppendTo(nil)) - 1)
			recs = append(recs, rec.AppendTo(nil)...)
		}
		now := time.Now().UnixMilli()
		batch := kmsg.RecordBatch{
			PartitionLeaderEpoch: -1,
			Magic:                2,
			Attributes:           0,
			LastOffsetDelta:      numRecs - 1,
			FirstTimestamp:       now,
			MaxTimestamp:         now,
			ProducerID:           pid,
			ProducerEpoch:        epoch,
			FirstSequence:        seq,
			NumRecords:           numRecs,
			Records:              recs,
		}
		raw := batch.AppendTo(nil)
		batch.Length = int32(len(raw) - 12)
		raw = batch.AppendTo(nil)
		batch.CRC = int32(crc32.Checksum(raw[21:], crc32.MakeTable(crc32.Castagnoli)))
		return batch.AppendTo(nil)
	}

	produce := func(batchBytes []byte) int16 {
		req := kmsg.NewProduceRequest()
		req.Version = 3
		req.Acks = -1
		req.TimeoutMillis = 5000
		rt := kmsg.NewProduceRequestTopic()
		rt.Topic = topic
		rp := kmsg.NewProduceRequestTopicPartition()
		rp.Partition = 0
		rp.Records = batchBytes
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)
		kresp, err := cl.Broker(0).Request(ctx, &req)
		if err != nil {
			t.Fatalf("produce: %v", err)
		}
		resp := kresp.(*kmsg.ProduceResponse)
		return resp.Topics[0].Partitions[0].ErrorCode
	}

	// Unknown PID 1000 epoch 2, non-zero firstSeq=10: should be accepted.
	errCode := produce(makeBatch(1000, 2, 10, 3))
	if errCode != 0 {
		t.Fatalf("expected success for unknown PID first produce, got: %v", kerr.ErrorForCode(errCode))
	}

	// Wrong sequence: PID is now known with nextSeq=13, send seq=99.
	errCode = produce(makeBatch(1000, 2, 99, 1))
	if errCode != kerr.OutOfOrderSequenceNumber.Code {
		t.Fatalf("expected OOOSN for wrong seq, got: %v", kerr.ErrorForCode(errCode))
	}

	// Correct sequence: nextSeq=13, send seq=13.
	errCode = produce(makeBatch(1000, 2, 13, 2))
	if errCode != 0 {
		t.Fatalf("expected success for correct seq, got: %v", kerr.ErrorForCode(errCode))
	}
}

// TestIssue1296 verifies that ClientSoftwareName/Version is sent on every new
// broker connection, not only the first. Without the fix, the per-broker
// version cache let kgo skip ApiVersions on subsequent connections, so any
// non-first connection (e.g. fetch/produce/group when a different typed
// connection already opened to the same broker) registered as "unknown"
// software on the broker side. This breaks KIP-714 client-metric matching.
func TestIssue1296(t *testing.T) {
	t.Parallel()

	const (
		topic       = "t1296"
		softwareNm  = "issue1296-client"
		softwareVer = "v9.9.9"
	)

	c, err := NewCluster(NumBrokers(1), SeedTopics(1, topic))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	type seen struct {
		name    string
		version string
	}
	var (
		mu   sync.Mutex
		reqs []seen
	)
	c.ControlKey(int16(kmsg.ApiVersions), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		req := kreq.(*kmsg.ApiVersionsRequest)
		mu.Lock()
		reqs = append(reqs, seen{req.ClientSoftwareName, req.ClientSoftwareVersion})
		mu.Unlock()
		return nil, nil, false // observe only; let the cluster respond normally
	})

	// Count broker connections so we can assert that ApiVersions was sent
	// on every connection (not just the first to each broker).
	connects := new(atomic.Int32)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.SoftwareNameAndVersion(softwareNm, softwareVer),
		kgo.ConsumerGroup("g1296"),
		kgo.ConsumeTopics(topic),
		kgo.DefaultProduceTopic(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.WithHooks(&connCountHook{connects}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	// Produce a record to force a produce connection to the (sole) broker.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cl.ProduceSync(ctx, kgo.StringRecord("v")).FirstErr(); err != nil {
		t.Fatal(err)
	}

	// Poll fetches to force a fetch connection to the same broker, and to
	// drive group-coordinator traffic (FindCoordinator on cxnNormal,
	// JoinGroup/SyncGroup/Heartbeat on cxnGroup).
	pollCtx, pollCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer pollCancel()
	for {
		fs := cl.PollFetches(pollCtx)
		if pollCtx.Err() != nil {
			t.Fatal("timed out waiting for records")
		}
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("poll errors: %v", errs)
		}
		if fs.NumRecords() > 0 {
			break
		}
	}

	mu.Lock()
	defer mu.Unlock()

	// Every connection setup sends ApiVersions, and each must carry our
	// configured software name and version. Reusing the same broker via a
	// different typed connection (cxnNormal/cxnProduce/cxnFetch/cxnGroup)
	// must not skip the request -- otherwise the broker registers that
	// connection as software=unknown and KIP-714 metric matching breaks.
	gotConnects := int(connects.Load())
	if len(reqs) != gotConnects {
		t.Fatalf("ApiVersions sent %d times across %d broker connections; expected one per connection. Reqs: %+v",
			len(reqs), gotConnects, reqs)
	}
	if len(reqs) < 2 {
		t.Fatalf("expected at least 2 ApiVersions requests across multiple connections, got %d", len(reqs))
	}
	for i, r := range reqs {
		if r.name != softwareNm || r.version != softwareVer {
			t.Errorf("ApiVersions #%d: got SoftwareName=%q SoftwareVersion=%q; want %q/%q",
				i, r.name, r.version, softwareNm, softwareVer)
		}
	}
}

// TestDescribeTopicPartitionsCursor verifies KIP-966 pagination:
// ResponsePartitionLimit caps the number of partitions per response and
// NextCursor is set to the next un-emitted (topic, partition) pair. A
// client walking the cursor should see every partition exactly once.
// Also verifies that mid-topic cursors and bad cursor-topic combinations
// behave correctly.
func TestDescribeTopicPartitionsCursor(t *testing.T) {
	t.Parallel()

	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(3, "a"),
		SeedTopics(5, "b"),
		SeedTopics(2, "c"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Walk all topics with a small limit; accumulate (topic,partition)
	// pairs and verify we see every partition exactly once in sorted
	// order.
	type tp struct {
		t string
		p int32
	}
	var got []tp
	var cursor *kmsg.DescribeTopicPartitionsRequestCursor
	for range 20 {
		req := kmsg.NewPtrDescribeTopicPartitionsRequest()
		req.ResponsePartitionLimit = 2
		req.Cursor = cursor

		kresp, err := cl.Broker(0).Request(ctx, req)
		if err != nil {
			t.Fatalf("request: %v", err)
		}
		resp := kresp.(*kmsg.DescribeTopicPartitionsResponse)
		for _, rt := range resp.Topics {
			if rt.ErrorCode != 0 {
				t.Fatalf("topic %q err %v", *rt.Topic, kerr.ErrorForCode(rt.ErrorCode))
			}
			for _, rp := range rt.Partitions {
				got = append(got, tp{*rt.Topic, rp.Partition})
			}
		}
		if resp.NextCursor == nil {
			break
		}
		nc := kmsg.NewDescribeTopicPartitionsRequestCursor()
		nc.Topic = resp.NextCursor.Topic
		nc.Partition = resp.NextCursor.Partition
		cursor = &nc
	}

	want := []tp{
		{"a", 0}, {"a", 1}, {"a", 2},
		{"b", 0}, {"b", 1}, {"b", 2}, {"b", 3}, {"b", 4},
		{"c", 0}, {"c", 1},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d partitions, want %d; got=%v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("partition %d: got %v want %v", i, got[i], want[i])
		}
	}

	// Cursor mid-topic: start at (b, 3) listing only b - expect b3, b4.
	req := kmsg.NewPtrDescribeTopicPartitionsRequest()
	req.ResponsePartitionLimit = 10
	rt := kmsg.NewDescribeTopicPartitionsRequestTopic()
	rt.Topic = "b"
	req.Topics = append(req.Topics, rt)
	cur := kmsg.NewDescribeTopicPartitionsRequestCursor()
	cur.Topic = "b"
	cur.Partition = 3
	req.Cursor = &cur

	kresp, err := cl.Broker(0).Request(ctx, req)
	if err != nil {
		t.Fatalf("mid-cursor request: %v", err)
	}
	resp := kresp.(*kmsg.DescribeTopicPartitionsResponse)
	if len(resp.Topics) != 1 || *resp.Topics[0].Topic != "b" {
		t.Fatalf("expected 1 topic b, got %v", resp.Topics)
	}
	parts := resp.Topics[0].Partitions
	if len(parts) != 2 || parts[0].Partition != 3 || parts[1].Partition != 4 {
		t.Fatalf("expected b[3,4], got %v", parts)
	}

	// Cursor topic not in explicit topic list: expect an error. The
	// broker returns InvalidRequest before the response is built, which
	// surfaces as a BrokerErrorResponse.
	req = kmsg.NewPtrDescribeTopicPartitionsRequest()
	rt = kmsg.NewDescribeTopicPartitionsRequestTopic()
	rt.Topic = "a"
	req.Topics = append(req.Topics, rt)
	cur = kmsg.NewDescribeTopicPartitionsRequestCursor()
	cur.Topic = "b"
	cur.Partition = 0
	req.Cursor = &cur

	if _, err := cl.Broker(0).Request(ctx, req); err == nil {
		t.Fatal("expected error for cursor topic not in request list, got nil")
	}
}

// TestIssue1330 verifies the share-state coordinator (FindCoordinator type 2)
// rejects keys that are not groupId:topicId:partition triplets with
// INVALID_REQUEST, matching the real broker. kfake previously accepted any key,
// so the #1330 mis-routing passed every kfake test yet failed a real broker.
func TestIssue1330(t *testing.T) {
	t.Parallel()
	c := newCluster(t)
	cl := newPlainClient(t, c)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const (
		bareKey  = "hello-share"                          // a bare group id, as the #1330 bug sent
		validKey = "hello-share:nT3i4OXyR0mr0my1RZx9pQ:0" // groupId:topicId:partition
	)

	req := kmsg.NewPtrFindCoordinatorRequest()
	req.CoordinatorType = 2 // share
	req.CoordinatorKeys = []string{bareKey, validKey}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("FindCoordinator: %v", err)
	}
	if len(resp.Coordinators) != 2 {
		t.Fatalf("expected 2 coordinators, got %d", len(resp.Coordinators))
	}

	got := make(map[string]kmsg.FindCoordinatorResponseCoordinator, 2)
	for _, co := range resp.Coordinators {
		got[co.Key] = co
	}

	// The bare group id is not a valid SharePartitionKey -> INVALID_REQUEST.
	if co, ok := got[bareKey]; !ok {
		t.Errorf("missing coordinator entry for %q", bareKey)
	} else if co.ErrorCode != kerr.InvalidRequest.Code {
		t.Errorf("bare group id: got error code %d (%v), want INVALID_REQUEST",
			co.ErrorCode, kerr.ErrorForCode(co.ErrorCode))
	}

	// A well-formed SharePartitionKey resolves to a real broker.
	if co, ok := got[validKey]; !ok {
		t.Errorf("missing coordinator entry for %q", validKey)
	} else {
		if err := kerr.ErrorForCode(co.ErrorCode); err != nil {
			t.Errorf("valid SharePartitionKey: unexpected error %v", err)
		}
		if co.NodeID < 0 {
			t.Errorf("valid SharePartitionKey: got NodeID %d, want a real broker node", co.NodeID)
		}
	}
}

// TestIssue1328 reproduces a data race in the metadata cache.
//
// Sequence of events:
//   - The metadata loop calls fetchMetadata, which caches the broker
//     response by storing a reference to topic.Partitions in metaCache.
//   - fetchMetadata returns; fetchTopicMetadata then sort.Slice's the
//     response's Partitions in place to validate strictly-increasing
//     partition IDs.
//   - Concurrently, a caller (kadm.Metadata => RequestCachedMetadata)
//     reads the cached topic. resolveTopicMeta copies the cached entry
//     under metaCache.mu, but the copy still shares the underlying
//     Partitions slice. The caller then iterates that slice without the
//     lock, racing the in-flight sort.
//
// The fix is to clone Partitions when storing into the cache so that
// later mutations of the broker response do not touch the cached slice.
func TestIssue1328(t *testing.T) {
	t.Parallel()
	const (
		topic         = "test-topic"
		numPartitions = 10
	)

	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(numPartitions, topic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ti := c.TopicInfo(topic)
	host, portStr, _ := net.SplitHostPort(c.ListenAddrs()[0])
	port, _ := strconv.Atoi(portStr)

	// Build partitions in reverse order so sort.Slice in fetchTopicMetadata
	// actually swaps elements -- already-sorted input sorts without writes,
	// and without a write there is no race to detect.
	c.ControlKey(int16(kmsg.Metadata), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()

		resp := kreq.(*kmsg.MetadataRequest).ResponseKind().(*kmsg.MetadataResponse)
		sb := kmsg.NewMetadataResponseBroker()
		sb.NodeID = 0
		sb.Host = host
		sb.Port = int32(port)
		resp.Brokers = append(resp.Brokers, sb)
		resp.ControllerID = 0

		st := kmsg.NewMetadataResponseTopic()
		st.Topic = kmsg.StringPtr(topic)
		st.TopicID = ti.TopicID
		for p := int32(numPartitions - 1); p >= 0; p-- {
			sp := kmsg.NewMetadataResponseTopicPartition()
			sp.Partition = p
			sp.Leader = -1
			sp.Replicas = []int32{0}
			sp.ISR = []int32{0}
			st.Partitions = append(st.Partitions, sp)
		}
		resp.Topics = append(resp.Topics, st)
		return resp, nil, true
	})

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.MetadataMinAge(10*time.Millisecond),
		kgo.MetadataMaxAge(50*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	adm := kadm.NewClient(cl)

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	var wg sync.WaitGroup
	for range 3 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for ctx.Err() == nil {
				adm.Metadata(ctx, topic) //nolint:errcheck // best-effort racer
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			cl.ForceMetadataRefresh()
			time.Sleep(10 * time.Millisecond)
		}
	}()
	wg.Wait()
}

// TestProduceUnknownFailLimitDeletedTopic verifies that a producer whose
// topic is deleted (and never recreated) fails its buffered records with
// UNKNOWN_TOPIC_ID once the unknown-topic fail limit (UnknownTopicRetries)
// is reached, rather than retrying forever.
//
// Produce v13+ addresses topics by ID (KIP-516). A deleted topic answers
// every produce with UNKNOWN_TOPIC_ID. That error is retriable and the
// default produce limits are unbounded (RecordRetries is effectively
// infinite and RecordDeliveryTimeout is disabled), so the unknown-topic fail
// limit is the only bound: UNKNOWN_TOPIC_ID must count toward it. It used to
// reset the count instead, leaving records buffered and retrying silently
// forever. (A topic deleted and RECREATED instead heals via the recreation
// swap, TestRecreationProduceHeal.)
func TestProduceUnknownFailLimitDeletedTopic(t *testing.T) {
	t.Parallel()
	const testTopic = "foo"

	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(1, testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// MetadataMaxAge is long so that no periodic refresh can observe the
	// sub-millisecond delete/create window below and fail the topic's
	// load with UNKNOWN_TOPIC_OR_PARTITION; we want the produce failures
	// alone (always UNKNOWN_TOPIC_ID) to drive the test.
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(testTopic),
		kgo.UnknownTopicRetries(2),
		kgo.RetryBackoffFn(func(int) time.Duration { return time.Millisecond }),
		kgo.MetadataMinAge(10*time.Millisecond),
		kgo.MetadataMaxAge(time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Produce once successfully so the client locks in the original
	// incarnation's topic ID on its per-partition produce buffer.
	if err := cl.ProduceSync(ctx, kgo.StringRecord("before")).FirstErr(); err != nil {
		t.Fatal(err)
	}

	// Delete the topic: the client keeps producing with the dead ID, and
	// no new incarnation ever exists to heal onto.
	adm := kadm.NewClient(cl)
	if _, err := adm.DeleteTopics(ctx, testTopic); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		done <- cl.ProduceSync(context.Background(), kgo.StringRecord("after")).FirstErr()
	}()
	select {
	case err := <-done:
		// The bound can trip from the produce response (UNKNOWN_TOPIC_ID,
		// the dead ID) or from the metadata merge's missing-topic path
		// (UNKNOWN_TOPIC_OR_PARTITION); both are the loud unknown-topic
		// failure the limit exists to force.
		if !errors.Is(err, kerr.UnknownTopicID) && !errors.Is(err, kerr.UnknownTopicOrPartition) {
			t.Fatalf("got %v, want an unknown-topic error", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("produce to a deleted topic did not fail within 10s; unknown-topic errors are defeating the fail limit")
	}
}

// TestProduceUnknownFailLimitNotResetByOtherErrors verifies that the
// unknown-topic fail limit cannot be defeated by other errors interleaving
// with UNKNOWN_TOPIC_OR_PARTITION. The limit used to reset on any
// non-unknown error, so a broker alternating UNKNOWN_TOPIC_OR_PARTITION
// with any other retriable error kept the count below the limit forever and
// the records never failed. Now only a successful produce resets the count;
// other errors leave it unchanged.
func TestProduceUnknownFailLimitNotResetByOtherErrors(t *testing.T) {
	t.Parallel()
	const testTopic = "foo"

	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(1, testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Fail every produce request, alternating UNKNOWN_TOPIC_OR_PARTITION
	// with the retriable NOT_ENOUGH_REPLICAS. Only the unknown errors may
	// count toward the fail limit; the others must not reset the count.
	var produceCalls atomic.Int32
	c.ControlKey(int16(kmsg.Produce), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		req := kreq.(*kmsg.ProduceRequest)
		errCode := kerr.UnknownTopicOrPartition.Code
		if produceCalls.Add(1)%2 == 0 {
			errCode = kerr.NotEnoughReplicas.Code
		}
		resp := req.ResponseKind().(*kmsg.ProduceResponse)
		for _, rt := range req.Topics {
			st := kmsg.NewProduceResponseTopic()
			st.Topic = rt.Topic
			st.TopicID = rt.TopicID
			for _, rp := range rt.Partitions {
				sp := kmsg.NewProduceResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = errCode
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(testTopic),
		kgo.UnknownTopicRetries(2),
		kgo.RetryBackoffFn(func(int) time.Duration { return time.Millisecond }),
		kgo.MetadataMinAge(10*time.Millisecond),
		kgo.MetadataMaxAge(time.Minute),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	done := make(chan error, 1)
	go func() {
		done <- cl.ProduceSync(context.Background(), kgo.StringRecord("x")).FirstErr()
	}()
	select {
	case err := <-done:
		if !errors.Is(err, kerr.UnknownTopicOrPartition) {
			t.Fatalf("got %v, want UnknownTopicOrPartition", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("produce did not fail within 10s; interleaved errors are resetting the unknown-topic fail limit")
	}
}

// TestEndTxnUnconfirmedErrorNoSilentJoin: an EndTxn(commit) that fails with
// an unconfirmed outcome (here UNKNOWN_SERVER_ERROR intercepting the request
// before the broker processes it) must not leave the client able to silently
// join the still-ongoing broker transaction. Pre-fix, EndTransaction consumed
// the client txn state without failing the producer ID: the documented
// TryAbort retry no-op'd at the !inTxn guard, the next transaction produced
// at the same pid/epoch into the ongoing transaction, and its commit
// committed BOTH transactions' records. Post-fix, the producer ID is flagged
// for reload; the next BeginTransaction re-inits (KIP-360), which bumps the
// epoch and fence-aborts the ongoing transaction, so a read_committed
// consumer sees only the second transaction's record.
func TestEndTxnUnconfirmedErrorNoSilentJoin(t *testing.T) {
	t.Parallel()
	const testTopic = "txn-unconfirmed-endtxn"

	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(1, testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// One-shot control: fail the FIRST EndTxn with UNKNOWN_SERVER_ERROR
	// without kfake ever processing it, leaving the broker-side
	// transaction ongoing.
	c.ControlKey(int16(kmsg.EndTxn), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		req := kreq.(*kmsg.EndTxnRequest)
		resp := req.ResponseKind().(*kmsg.EndTxnResponse)
		resp.ErrorCode = kerr.UnknownServerError.Code
		return resp, nil, true
	})

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(testTopic),
		kgo.TransactionalID("txn-unconfirmed"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	if err := cl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := cl.ProduceSync(ctx, kgo.StringRecord("txn1")).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := cl.EndTransaction(ctx, kgo.TryCommit); err == nil {
		t.Fatal("expected EndTransaction to fail from the controlled UNKNOWN_SERVER_ERROR")
	}
	// The documented recovery: retry as an abort. This is a client-side
	// no-op (state was consumed), but the producer ID is now flagged for
	// reload.
	if err := cl.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatalf("abort retry errored: %v", err)
	}

	if err := cl.BeginTransaction(); err != nil {
		t.Fatalf("begin after unconfirmed end errored: %v", err)
	}
	if err := cl.ProduceSync(ctx, kgo.StringRecord("txn2")).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := cl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatalf("second commit errored: %v", err)
	}

	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics(testTopic),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	// The topic has one partition: if txn1's record were committed it
	// would arrive before txn2's. Seeing txn2 first proves txn1 aborted.
	for {
		fs := consumer.PollFetches(ctx)
		if errs := fs.Errors(); len(errs) > 0 {
			t.Fatalf("fetch errors: %v", errs)
		}
		var done bool
		fs.EachRecord(func(r *kgo.Record) {
			switch string(r.Value) {
			case "txn1":
				t.Error("read_committed consumer saw txn1's record; the unconfirmed transaction was silently committed by txn2")
			case "txn2":
				done = true
			}
		})
		if done || t.Failed() {
			break
		}
	}
}

// reentrantDisconnectHook re-enters the client from OnBrokerDisconnect.
// Hooks are not documented as forbidden from doing this; pre-fix, brokers
// were stopped while holding the brokersMu write lock, so the re-entry
// (which read-locks brokersMu) deadlocked Close and every request path.
type reentrantDisconnectHook struct {
	cl    atomic.Pointer[kgo.Client]
	fired atomic.Int32
}

func (h *reentrantDisconnectHook) OnBrokerDisconnect(kgo.BrokerMetadata, net.Conn) {
	if cl := h.cl.Load(); cl != nil {
		cl.DiscoveredBrokers()
		h.fired.Add(1)
	}
}

func TestOnBrokerDisconnectReentrantHook(t *testing.T) {
	t.Parallel()
	const testTopic = "hook-reentry"

	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(1, testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	hook := new(reentrantDisconnectHook)
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(testTopic),
		kgo.WithHooks(hook),
	)
	if err != nil {
		t.Fatal(err)
	}
	hook.cl.Store(cl)

	// Establish a live connection so Close has something to disconnect.
	if err := cl.ProduceSync(ctx, kgo.StringRecord("x")).FirstErr(); err != nil {
		t.Fatal(err)
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		cl.Close()
	}()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("Close deadlocked: OnBrokerDisconnect hook re-entering the client blocked on brokersMu")
	}
	if hook.fired.Load() == 0 {
		t.Fatal("hook never fired; test proved nothing")
	}
}

// TestMetadataZeroPartitionsNoFakeSuccess: a buggy broker or proxy can reply
// to metadata for an unknown topic with ErrorCode 0 and an empty partition
// array. Pre-fix, storePartitionsUpdate promised every buffered record for
// that topic with a NIL error: user promises fired as successes (offset
// 0..n, producer id 0) for records that were never sent anywhere. Post-fix,
// the client treats the reply as a retryable unknown-topic failure, bounded
// by UnknownTopicRetries.
func TestMetadataZeroPartitionsNoFakeSuccess(t *testing.T) {
	t.Parallel()
	const testTopic = "zero-partitions"

	c, err := NewCluster(NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	addr := c.ListenAddrs()[0]
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatal(err)
	}

	// Every metadata response reports requested topics as existing with
	// no error and zero partitions.
	c.ControlKey(int16(kmsg.Metadata), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		req := kreq.(*kmsg.MetadataRequest)
		resp := req.ResponseKind().(*kmsg.MetadataResponse)
		b := kmsg.NewMetadataResponseBroker()
		b.NodeID = 0
		b.Host = host
		b.Port = int32(port)
		resp.Brokers = append(resp.Brokers, b)
		resp.ControllerID = 0
		for _, rt := range req.Topics {
			mt := kmsg.NewMetadataResponseTopic()
			mt.Topic = rt.Topic
			mt.ErrorCode = 0
			resp.Topics = append(resp.Topics, mt)
		}
		return resp, nil, true
	})

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(testTopic),
		kgo.UnknownTopicRetries(1),
		kgo.MetadataMinAge(10*time.Millisecond),
		kgo.MetadataMaxAge(50*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	promised := make(chan error, 1)
	cl.Produce(context.Background(), kgo.StringRecord("v"), func(_ *kgo.Record, err error) {
		promised <- err
	})

	select {
	case err := <-promised:
		if err == nil {
			t.Fatal("record promised with nil error: fabricated success for a record that was never produced")
		}
		if !errors.Is(err, kerr.UnknownTopicOrPartition) {
			t.Fatalf("got %v, want UnknownTopicOrPartition", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("promise did not fire within 10s")
	}
}

// TestOffsetFetchTopicIDOldWire: an OffsetFetch built with only TopicIDs,
// issued by a client that neither produces nor consumes the topic, against
// a wire version below v10 (no TopicID field on the wire). The sharder must
// resolve the ID to a name via the metadata cache it just populated;
// pre-fix it read the unrelated cl.id2t map (only written for
// produced/consumed topics), sent an empty topic name, and the broker could
// not match the topic.
func TestOffsetFetchTopicIDOldWire(t *testing.T) {
	t.Parallel()
	const (
		testTopic = "offset-fetch-by-id"
		group     = "offset-fetch-by-id-group"
	)

	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(1, testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	setup, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer setup.Close()

	// Learn the topic's ID.
	mreq := kmsg.NewPtrMetadataRequest()
	mt := kmsg.NewMetadataRequestTopic()
	mt.Topic = kmsg.StringPtr(testTopic)
	mreq.Topics = append(mreq.Topics, mt)
	mresp, err := mreq.RequestWith(ctx, setup)
	if err != nil {
		t.Fatal(err)
	}
	if len(mresp.Topics) != 1 || mresp.Topics[0].ErrorCode != 0 {
		t.Fatalf("bad metadata response: %v", mresp)
	}
	topicID := mresp.Topics[0].TopicID

	// Commit an offset for the group (simple, generation-less commit).
	oreq := kmsg.NewPtrOffsetCommitRequest()
	oreq.Group = group
	ot := kmsg.NewOffsetCommitRequestTopic()
	ot.Topic = testTopic
	ot.TopicID = topicID // v10+ matches by TopicID
	op := kmsg.NewOffsetCommitRequestTopicPartition()
	op.Partition = 0
	op.Offset = 3
	ot.Partitions = append(ot.Partitions, op)
	oreq.Topics = append(oreq.Topics, ot)
	oresp, err := oreq.RequestWith(ctx, setup)
	if err != nil {
		t.Fatal(err)
	}
	if ec := oresp.Topics[0].Partitions[0].ErrorCode; ec != 0 {
		t.Fatalf("offset commit failed with code %d", ec)
	}

	// A fresh client (empty id2t: it produces and consumes nothing),
	// pinned below OffsetFetch v10 so the topic NAME is what matters on
	// the wire.
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.MaxVersions(kversion.V3_5_0()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	freq := kmsg.NewPtrOffsetFetchRequest()
	fg := kmsg.NewOffsetFetchRequestGroup()
	fg.Group = group
	ft := kmsg.NewOffsetFetchRequestGroupTopic()
	ft.TopicID = topicID
	ft.Partitions = []int32{0}
	fg.Topics = append(fg.Topics, ft)
	freq.Groups = append(freq.Groups, fg)

	fresp, err := freq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	var found bool
	for _, g := range fresp.Groups {
		for _, rt := range g.Topics {
			for _, rp := range rt.Partitions {
				if rt.Topic == testTopic && rp.Partition == 0 && rp.Offset == 3 {
					found = true
				}
			}
		}
	}
	if !found {
		t.Fatalf("committed offset not returned; the request went out without a resolved topic name: %v", fresp)
	}
}

// TestShareGroupIDNotFoundRejoin: if share group state vanishes under a live
// member (coordinator state loss), every heartbeat at the member's current
// epoch returns GROUP_ID_NOT_FOUND, and the broker only recreates a share
// group on a member-epoch 0 heartbeat. The client must reset to epoch 0 and
// rejoin; pre-fix it retried the same epoch forever and never healed.
func TestShareGroupIDNotFoundRejoin(t *testing.T) {
	t.Parallel()
	const (
		testTopic = "share-gid-not-found"
		group     = "share-gid-not-found-group"
	)

	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(1, testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	// Share groups default share.auto.offset.reset to latest; set earliest
	// so the consumer sees the record produced before it joined.
	acreq := kmsg.NewPtrIncrementalAlterConfigsRequest()
	acres := kmsg.NewIncrementalAlterConfigsRequestResource()
	acres.ResourceType = kmsg.ConfigResourceTypeGroupConfig
	acres.ResourceName = group
	accfg := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	accfg.Name = "share.auto.offset.reset"
	accfg.Op = 0
	accfg.Value = kmsg.StringPtr("earliest")
	acres.Configs = append(acres.Configs, accfg)
	acreq.Resources = append(acreq.Resources, acres)
	acresp, err := acreq.RequestWith(ctx, producer)
	if err != nil {
		t.Fatal(err)
	}
	if ec := acresp.Resources[0].ErrorCode; ec != 0 {
		t.Fatalf("alter group config failed with code %d", ec)
	}

	if err := producer.ProduceSync(ctx, kgo.StringRecord("r1")).FirstErr(); err != nil {
		t.Fatal(err)
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics(testTopic),
		kgo.ShareGroup(group),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	// Wait until the member is established (it received a record).
	for {
		fs := cl.PollFetches(ctx)
		if fs.NumRecords() > 0 {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("share consumer never received the first record")
		}
	}

	// Simulate group state loss: every heartbeat at a live epoch gets
	// GROUP_ID_NOT_FOUND until an epoch-0 (re)join arrives; the join
	// passes through to kfake (which still has the group and re-admits
	// the member) and ends the fencing, modeling the broker recreating
	// the group.
	rejoined := make(chan struct{})
	var healed atomic.Bool
	c.ControlKey(int16(kmsg.ShareGroupHeartbeat), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if healed.Load() {
			return nil, nil, false
		}
		req := kreq.(*kmsg.ShareGroupHeartbeatRequest)
		if req.MemberEpoch > 0 {
			resp := req.ResponseKind().(*kmsg.ShareGroupHeartbeatResponse)
			resp.ErrorCode = kerr.GroupIDNotFound.Code
			return resp, nil, true
		}
		if req.MemberEpoch == 0 && healed.CompareAndSwap(false, true) {
			close(rejoined)
		}
		return nil, nil, false
	})

	select {
	case <-rejoined:
	case <-time.After(10 * time.Second):
		t.Fatal("share consumer never reset to epoch 0 after GROUP_ID_NOT_FOUND; it cannot heal")
	}

	// Let the pre-reset in-flight ShareFetch drain before producing:
	// that fetch was sent on the OLD (reset) session and can park
	// broker-side for up to FetchMaxWait; if r2 lands in that window,
	// the broker delivers it on the stale fetch, the client's
	// mid-flight-reset guard discards the data (by design: its acks
	// could not ride the new session), and r2 sits acquisition-locked
	// for the full lock duration -- an unrelated, deliberate behavior
	// that would mask what this test asserts. Two FetchMaxWaits bounds
	// the drain deterministically.
	time.Sleep(500 * time.Millisecond)

	// The member rejoined; prove the group is functional again.
	if err := producer.ProduceSync(ctx, kgo.StringRecord("r2")).FirstErr(); err != nil {
		t.Fatal(err)
	}
	for {
		fs := cl.PollFetches(ctx)
		var got bool
		fs.EachRecord(func(r *kgo.Record) {
			if string(r.Value) == "r2" {
				got = true
			}
		})
		if got {
			break
		}
		if ctx.Err() != nil {
			t.Fatal("share consumer did not resume consuming after rejoining")
		}
	}
}

// reentrantFetchHook re-enters the client from OnFetchRecordUnbuffered.
// Pre-fix, the hook fired while the poll path held c.mu (and the discard
// path held c.mu+sessionChangeMu), so a hook calling anything that needs
// c.mu deadlocked the poll permanently and wedged rebalances and Close
// behind it. Post-fix, polled-record hooks fire after the locks release.
type reentrantFetchHook struct {
	cl    atomic.Pointer[kgo.Client]
	fired atomic.Int32
}

func (h *reentrantFetchHook) OnFetchRecordUnbuffered(r *kgo.Record, polled bool) {
	if !polled {
		return
	}
	if cl := h.cl.Load(); cl != nil {
		cl.AddConsumeTopics("fetch-hook-reentry-extra") // needs c.mu
		h.fired.Add(1)
	}
}

func TestFetchUnbufferedHookReentrancy(t *testing.T) {
	t.Parallel()
	const testTopic = "fetch-hook-reentry"

	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(1, testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	producer, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	if err := producer.ProduceSync(ctx, kgo.StringRecord("v")).FirstErr(); err != nil {
		t.Fatal(err)
	}

	hook := new(reentrantFetchHook)
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics(testTopic),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.WithHooks(hook),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	hook.cl.Store(cl)

	polled := make(chan struct{})
	go func() {
		defer close(polled)
		for {
			fs := cl.PollFetches(ctx)
			if fs.NumRecords() > 0 {
				return
			}
			if ctx.Err() != nil {
				return
			}
		}
	}()
	select {
	case <-polled:
	case <-time.After(10 * time.Second):
		t.Fatal("poll deadlocked: OnFetchRecordUnbuffered re-entering the client blocked on c.mu")
	}
	if hook.fired.Load() == 0 {
		t.Fatal("hook never fired; test proved nothing")
	}
}

// TestOnDataLossCallbackReentrancy: ProducerOnDataLossDetected used to fire
// while the partition's recBuf.mu was held; producing to the reported
// partition from the callback -- the natural reaction -- deadlocked that
// partition and the sink's response processing forever. Post-fix the
// callback dispatches on its own goroutine.
func TestOnDataLossCallbackReentrancy(t *testing.T) {
	t.Parallel()
	const testTopic = "on-data-loss-reentry"

	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(1, testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// One-shot: the first produce gets OUT_OF_ORDER_SEQUENCE_NUMBER,
	// which for a non-transactional producer (stopOnDataLoss unset)
	// fires the data-loss callback and reloads the producer id.
	c.ControlKey(0, func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		req := kreq.(*kmsg.ProduceRequest)
		resp := req.ResponseKind().(*kmsg.ProduceResponse)
		for _, rt := range req.Topics {
			respt := kmsg.NewProduceResponseTopic()
			respt.Topic = rt.Topic
			respt.TopicID = rt.TopicID
			for _, rp := range rt.Partitions {
				respp := kmsg.NewProduceResponseTopicPartition()
				respp.Partition = rp.Partition
				respp.ErrorCode = kerr.OutOfOrderSequenceNumber.Code
				respt.Partitions = append(respt.Partitions, respp)
			}
			resp.Topics = append(resp.Topics, respt)
		}
		return resp, nil, true
	})

	var cl *kgo.Client
	hookDone := make(chan error, 1)
	var hookOnce sync.Once
	cl, err = kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(testTopic),
		kgo.ProducerOnDataLossDetected(func(topic string, partition int32) {
			hookOnce.Do(func() {
				// Produce to the very partition the callback reports.
				hookDone <- cl.ProduceSync(ctx, kgo.StringRecord("from-hook")).FirstErr()
			})
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	if err := cl.ProduceSync(ctx, kgo.StringRecord("v")).FirstErr(); err != nil {
		t.Fatalf("produce after data-loss retry errored: %v", err)
	}
	select {
	case err := <-hookDone:
		if err != nil {
			t.Fatalf("produce from data-loss callback errored: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("data-loss callback deadlocked: producing to the reported partition blocked on recBuf.mu")
	}
}

// TestKadmACLDefaultPatternRoundTrip: kadm ACL builders that never call
// ResourcePatternType used to send pattern UNKNOWN(0) in describe/delete
// filters -- rejected by real brokers at request parse (kfake now models
// that) -- and creating without a pattern failed validation despite the
// docs calling literal the default. Builders that never call Operations
// generated zero filters, silently doing nothing. Now: create defaults to
// literal, filters default to any-pattern and any-operation.
func TestKadmACLDefaultPatternRoundTrip(t *testing.T) {
	t.Parallel()
	const testTopic = "kadm-acl-defaults"

	c, err := NewCluster(NumBrokers(1), EnableACLs(), Superuser("PLAIN", "admin", "pass"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.SASL(plain.Auth{User: "admin", Pass: "pass"}.AsMechanism()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	adm := kadm.NewClient(cl)

	// Create with pattern unset: defaults to literal per the docs.
	cb := kadm.NewACLs().Topics(testTopic).Allow("User:alice").AllowHosts().Operations(kadm.OpRead)
	created, err := adm.CreateACLs(ctx, cb)
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	for _, cr := range created {
		if cr.Err != nil {
			t.Fatalf("create result errored: %v", cr.Err)
		}
	}

	// Describe with pattern AND operations unset: matches any.
	db := kadm.NewACLs().Topics(testTopic).Allow().AllowHosts().Deny().DenyHosts()
	described, err := adm.DescribeACLs(ctx, db)
	if err != nil {
		t.Fatalf("describe: %v", err)
	}
	var found bool
	for _, dr := range described {
		if dr.Err != nil {
			t.Fatalf("describe result errored: %v", dr.Err)
		}
		for _, d := range dr.Described {
			if d.Principal == "User:alice" && d.Name == testTopic {
				found = true
			}
		}
	}
	if !found {
		t.Fatalf("created ACL not described: %v", described)
	}

	// Delete with the same fully-default filter.
	deleted, err := adm.DeleteACLs(ctx, db)
	if err != nil {
		t.Fatalf("delete: %v", err)
	}
	var ndeleted int
	for _, dr := range deleted {
		if dr.Err != nil {
			t.Fatalf("delete result errored: %v", dr.Err)
		}
		for _, d := range dr.Deleted {
			if d.Err == nil {
				ndeleted++
			}
		}
	}
	if ndeleted != 1 {
		t.Fatalf("expected exactly 1 deleted ACL, got %d", ndeleted)
	}
}

// A real broker validates the ApiVersions v3+ client software name/version
// and answers INVALID_REQUEST on a mismatch; kfake accepting anything made
// franz-go's tests blind to clients sending invalid values.
func TestApiVersionsSoftwareNameValidation(t *testing.T) {
	t.Parallel()

	c, err := NewCluster(NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	req := kmsg.NewPtrApiVersionsRequest()
	req.ClientSoftwareName = "bad name" // space: invalid
	req.ClientSoftwareVersion = "1.0.0"
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp.ErrorCode != kerr.InvalidRequest.Code {
		t.Errorf("got error code %d, want INVALID_REQUEST", resp.ErrorCode)
	}

	req.ClientSoftwareName = "good-name"
	resp, err = req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp.ErrorCode != 0 {
		t.Errorf("got error code %d for a valid name, want 0", resp.ErrorCode)
	}
}

// An EndTxn whose outcome is unconfirmed (transport error, retries exhausted,
// or UNKNOWN_SERVER_ERROR) restores the client's transaction state so the
// documented retry with TryAbort heals immediately: the retry reloads the
// producer ID, whose epoch bump fence-aborts anything still ongoing broker
// side, and sends no second EndTxn. Previously the retry was a silent no-op
// and the broker transaction lingered until the next transaction began (or
// the transaction timeout fired), stalling read_committed consumers.
func TestEndTxnUnconfirmedAbortRetry(t *testing.T) {
	t.Parallel()
	const topic = "txn-unconfirmed-abort"

	c, err := NewCluster(NumBrokers(1), SeedTopics(1, topic))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var endTxns, initPIDs atomic.Int32
	c.ControlKey(int16(kmsg.EndTxn), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if endTxns.Add(1) == 1 {
			resp := kreq.ResponseKind().(*kmsg.EndTxnResponse)
			resp.ErrorCode = kerr.UnknownServerError.Code
			return resp, nil, true
		}
		return nil, nil, false
	})
	c.ControlKey(int16(kmsg.InitProducerID), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		initPIDs.Add(1)
		return nil, nil, false
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.TransactionalID("txn-unconfirmed-abort"),
		kgo.DefaultProduceTopic(topic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	if err := cl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := cl.ProduceSync(ctx, kgo.StringRecord("first")).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := cl.EndTransaction(ctx, kgo.TryCommit); err == nil {
		t.Fatal("expected an error from the hijacked EndTxn commit")
	}

	preInits := initPIDs.Load()
	if err := cl.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatalf("abort retry after unconfirmed commit errored: %v", err)
	}
	if got := endTxns.Load(); got != 1 {
		t.Errorf("abort retry sent an EndTxn (saw %d total); the producer id reload is the abort", got)
	}
	if got := initPIDs.Load(); got != preInits+1 {
		t.Errorf("abort retry did not reload the producer id (%d inits before, %d after)", preInits, got)
	}

	// The next transaction runs cleanly at the bumped epoch.
	if err := cl.BeginTransaction(); err != nil {
		t.Fatalf("begin after healed abort: %v", err)
	}
	if err := cl.ProduceSync(ctx, kgo.StringRecord("second")).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := cl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatalf("commit after healed abort: %v", err)
	}

	// read_committed sees only the second transaction: the first was
	// fence-aborted by the reload.
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics(topic),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	var vals []string
	for len(vals) == 0 {
		fs := consumer.PollFetches(ctx)
		if err := fs.Err0(); err != nil {
			t.Fatalf("consume: %v", err)
		}
		fs.EachRecord(func(r *kgo.Record) { vals = append(vals, string(r.Value)) })
	}
	if len(vals) != 1 || vals[0] != "second" {
		t.Errorf("read_committed consumed %v, want only [second]", vals)
	}
}

// Retrying a commit whose outcome is unconfirmed is refused: the prior
// outcome is unknowable, and the retry's producer id reload may have just
// fence-aborted it, so returning success would lie. The caller must retry
// with TryAbort, per EndTransaction's docs.
func TestEndTxnUnconfirmedCommitRetryRefused(t *testing.T) {
	t.Parallel()
	const topic = "txn-unconfirmed-commit"

	c, err := NewCluster(NumBrokers(1), SeedTopics(1, topic))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var endTxns atomic.Int32
	c.ControlKey(int16(kmsg.EndTxn), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if endTxns.Add(1) == 1 {
			resp := kreq.ResponseKind().(*kmsg.EndTxnResponse)
			resp.ErrorCode = kerr.UnknownServerError.Code
			return resp, nil, true
		}
		return nil, nil, false
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.TransactionalID("txn-unconfirmed-commit"),
		kgo.DefaultProduceTopic(topic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	if err := cl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := cl.ProduceSync(ctx, kgo.StringRecord("v")).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := cl.EndTransaction(ctx, kgo.TryCommit); err == nil {
		t.Fatal("expected an error from the hijacked EndTxn commit")
	}
	err = cl.EndTransaction(ctx, kgo.TryCommit)
	if err == nil || !strings.Contains(err.Error(), "unconfirmed") {
		t.Fatalf("commit retry: got %v, want an unconfirmed-refusal error", err)
	}

	// The refusal's reload already aborted everything; a follow-up abort
	// is a truthful no-op, and the next transaction runs cleanly.
	if err := cl.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatalf("abort after refused commit retry: %v", err)
	}
	if err := cl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := cl.ProduceSync(ctx, kgo.StringRecord("v2")).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := cl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatalf("commit after heal: %v", err)
	}
}
