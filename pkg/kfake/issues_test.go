package kfake

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
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
		chkfs(fs, 0)
		if lr := leaderReqs.Load(); lr != 1 {
			t.Errorf("stage 1 leader reqs %d != exp 1", lr)
		}
		if fr := followerReqs.Load(); fr != 1 {
			t.Errorf("stage 1 follower reqs reqs %d != exp 1", fr)
		}
		allowFollower <- struct{}{} // allow a background buffered fetch
	}

	// Sleep past the recheck interval so the client rechecks the preferred replica.
	time.Sleep(150 * time.Millisecond)
	for followerReqs.Load() != 2 {
		time.Sleep(50 * time.Millisecond)
	}
	{
		fs := cl.PollFetches(ctx)
		chkfs(fs, 1)
		if lr := leaderReqs.Load(); lr != 1 {
			t.Errorf("stage 2 leader reqs %d != exp 1", lr)
		}
		if fr := followerReqs.Load(); fr != 2 {
			t.Errorf("stage 2 follower reqs reqs %d != exp 2", fr)
		}
	}

	// Poll again. This should go back to the leader, then the follower
	// again.
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
			offset += 1
			r2 := kgo.StringRecord(strconv.Itoa(offset))
			r2.Timestamp = time.UnixMilli(10_000 + int64(offset))
			offset += 1
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
	txnClient, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
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

	// Create a transactional producer
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(testTopic),
		kgo.TransactionalID("test-txn-minbytes"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	// Produce and commit a transaction, then verify committed data
	// satisfies MinBytes immediately.
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
		t.Fatalf("expected 1 record, got %d", fetches.NumRecords())
	}
	if elapsed > time.Second {
		t.Fatalf("committed fetch took too long: %v (expected immediate)", elapsed)
	}

	// Uncommitted data should NOT satisfy MinBytes.
	hwmBeforeTxn := c.PartitionInfo(testTopic, 0).HighWatermark

	if err := producer.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := producer.ProduceSync(ctx, &kgo.Record{Value: []byte("uncommitted-data")}).FirstErr(); err != nil {
		t.Fatal(err)
	}

	pi = c.PartitionInfo(testTopic, 0)
	if pi.LastStableOffset != hwmBeforeTxn {
		t.Errorf("LSO should not advance for uncommitted txn: LSO=%d, expected=%d", pi.LastStableOffset, hwmBeforeTxn)
	}

	shortCtx, shortCancel := context.WithTimeout(ctx, 100*time.Millisecond)
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

	if elapsed < 50*time.Millisecond {
		t.Fatalf("uncommitted fetch returned too quickly: %v (expected ~100ms wait)", elapsed)
	}
	if fetches.NumRecords() > 0 {
		t.Fatalf("readCommitted should not see uncommitted data, got %d records", fetches.NumRecords())
	}

	// After commit, a waiting readCommitted consumer should get data.
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

	// Poll in background
	fetchDone := make(chan struct{})
	var fetchResult kgo.Fetches
	var fetchElapsed time.Duration
	go func() {
		start := time.Now()
		fetchResult = consumer3.PollFetches(ctx)
		fetchElapsed = time.Since(start)
		close(fetchDone)
	}()

	time.Sleep(50 * time.Millisecond)

	if err := producer.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}

	// Wait for the fetch to complete
	select {
	case <-fetchDone:
	case <-ctx.Done():
		t.Fatal("timeout waiting for fetch after commit")
	}
	consumer3.Close()

	if fetchElapsed > 2*time.Second {
		t.Fatalf("fetch took too long after commit: %v (expected <2s)", fetchElapsed)
	}
	if fetchResult.NumRecords() != 1 {
		t.Fatalf("expected 1 record after commit, got %d", fetchResult.NumRecords())
	}

	// Verify read_uncommitted sees data immediately (baseline).
	if err := producer.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := producer.ProduceSync(ctx, &kgo.Record{Value: []byte("more-uncommitted")}).FirstErr(); err != nil {
		t.Fatal(err)
	}

	newHWM := c.PartitionInfo(testTopic, 0).HighWatermark
	newLSO := c.PartitionInfo(testTopic, 0).LastStableOffset

	// read_uncommitted consumer should see the data immediately
	consumer4, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.FetchIsolationLevel(kgo.ReadUncommitted()),
		kgo.FetchMinBytes(1),
		kgo.FetchMaxWait(5*time.Second),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			testTopic: {0: kgo.NewOffset().At(newLSO)}, // start at LSO, which is before HWM
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
		t.Fatalf("read_uncommitted took too long: %v", elapsed)
	}
	if fetches.NumRecords() == 0 {
		t.Fatalf("read_uncommitted should see uncommitted data, HWM=%d LSO=%d", newHWM, newLSO)
	}

	_ = producer.EndTransaction(ctx, kgo.TryAbort)
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

	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	joinGroup := func(ctx context.Context, memberID string, metadata []byte) (*kmsg.JoinGroupResponse, error) {
		req := kmsg.NewJoinGroupRequest()
		req.Group = groupID
		req.MemberID = memberID
		req.ProtocolType = "consumer"
		req.SessionTimeoutMillis = 10000
		req.RebalanceTimeoutMillis = 10000
		proto := kmsg.NewJoinGroupRequestProtocol()
		proto.Name = "cooperative-sticky"
		proto.Metadata = metadata
		req.Protocols = append(req.Protocols, proto)
		return req.RequestWith(ctx, cl)
	}

	syncGroup := func(memberID string, gen int32, isLeader bool, assignment []byte) (*kmsg.SyncGroupResponse, error) {
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
	metadataV2 := []byte{0, 1, 2, 4}
	assignment := []byte{0, 0, 0, 0}

	// m1 joins, gets MemberIDRequired, rejoins with ID to become leader.
	resp1, err := joinGroup(ctx, "", metadataV1)
	if err != nil {
		t.Fatal(err)
	}
	if resp1.ErrorCode != kerr.MemberIDRequired.Code {
		t.Fatalf("expected MemberIDRequired, got %v", kerr.ErrorForCode(resp1.ErrorCode))
	}
	m1ID := resp1.MemberID

	resp1, err = joinGroup(ctx, m1ID, metadataV1)
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

	syncResp, err := syncGroup(m1ID, gen1, true, assignment)
	if err != nil {
		t.Fatal(err)
	}
	if syncResp.ErrorCode != 0 {
		t.Fatalf("m1 sync error: %v", kerr.ErrorForCode(syncResp.ErrorCode))
	}

	// m2 joins (gets MemberIDRequired), then rejoins with ID.
	// m2's rejoin triggers a rebalance; send it first so it blocks,
	// then m1 rejoins to complete the rebalance.
	resp2, err := joinGroup(ctx, "", metadataV1)
	if err != nil {
		t.Fatal(err)
	}
	if resp2.ErrorCode != kerr.MemberIDRequired.Code {
		t.Fatalf("expected MemberIDRequired for m2, got %v", kerr.ErrorForCode(resp2.ErrorCode))
	}
	m2ID := resp2.MemberID

	var wg sync.WaitGroup
	var resp1Rebalance, resp2Rebalance *kmsg.JoinGroupResponse
	var err1, err2 error

	// Send m2 first - it triggers the rebalance and blocks waiting for m1.
	wg.Add(1)
	go func() {
		defer wg.Done()
		resp2Rebalance, err2 = joinGroup(ctx, m2ID, metadataV1)
	}()
	// Brief sleep to ensure m2's request arrives first.
	time.Sleep(10 * time.Millisecond)
	wg.Add(1)
	go func() {
		defer wg.Done()
		resp1Rebalance, err1 = joinGroup(ctx, m1ID, metadataV1)
	}()
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

	// Sync both members to reach Stable.
	_, _ = syncGroup(m1ID, gen2, resp1.LeaderID == m1ID, assignment)
	_, _ = syncGroup(m2ID, gen2, resp2.LeaderID == m2ID, assignment)

	// THE CRITICAL TEST: m2 (non-leader) rejoins with CHANGED metadata
	// while group is Stable. This should trigger a new rebalance.
	// Use a short timeout - if the JoinGroup blocks, rebalance was triggered.
	shortCtx, shortCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	resp2, err = joinGroup(shortCtx, m2ID, metadataV2)
	shortCancel()

	if errors.Is(err, context.DeadlineExceeded) {
		// Correct: rebalance was triggered, JoinGroup is waiting for m1.
		// Complete the rebalance.
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = joinGroup(ctx, m1ID, metadataV1)
		}()
		var resp2Final *kmsg.JoinGroupResponse
		go func() {
			defer wg.Done()
			resp2Final, _ = joinGroup(ctx, m2ID, metadataV2)
		}()
		wg.Wait()

		if resp2Final == nil || resp2Final.Generation <= gen2 {
			t.Fatalf("rebalance did not advance generation past %d", gen2)
		}
	} else if err != nil {
		t.Fatalf("unexpected error: %v", err)
	} else if resp2.ErrorCode != 0 {
		t.Fatalf("m2 rejoin unexpected error: %v", kerr.ErrorForCode(resp2.ErrorCode))
	} else if resp2.Generation > gen2 {
		// Rebalance triggered and completed within the short timeout - also correct.
	} else {
		t.Fatalf("m2 rejoined with changed metadata but got same generation %d (no rebalance triggered)", gen2)
	}
}

func TestTransactionAbortedMetadata(t *testing.T) {
	t.Parallel()
	const testTopic = "txn-aborted-metadata-test"

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

	// Produce and abort a transaction
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(testTopic),
		kgo.TransactionalID("test-txn-metadata"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()

	if err := producer.BeginTransaction(); err != nil {
		t.Fatalf("failed to begin transaction: %v", err)
	}

	for i := 0; i < 3; i++ {
		if err := producer.ProduceSync(ctx, kgo.StringRecord("aborted-"+strconv.Itoa(i))).FirstErr(); err != nil {
			t.Fatalf("failed to produce: %v", err)
		}
	}

	if err := producer.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatalf("failed to abort transaction: %v", err)
	}

	// Produce committed messages after the abort to verify filtering
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

	// Verify partition info shows LSO advanced after abort
	pi := c.PartitionInfo(testTopic, 0)
	if pi.LastStableOffset != pi.HighWatermark {
		t.Errorf("LSO should equal HWM after abort, got LSO=%d HWM=%d", pi.LastStableOffset, pi.HighWatermark)
	}

	// Verify read_committed consumer sees only committed messages, not aborted ones
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

	// Verify we only got committed messages
	if consumed != 2 {
		t.Errorf("expected 2 committed messages, got %d", consumed)
	}
	for _, rec := range records {
		if len(rec) >= 7 && rec[:7] == "aborted" {
			t.Errorf("read_committed consumer saw aborted message: %s", rec)
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Create a client and use kadm to commit an offset, which creates the group
	adminClient, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	adm := kadm.NewClient(adminClient)

	// Commit an initial offset to create the group
	offsets := kadm.Offsets{}
	offsets.Add(kadm.Offset{Topic: testTopic, Partition: 0, At: 0})
	err = adm.CommitAllOffsets(ctx, groupID, offsets)
	if err != nil {
		t.Fatalf("CommitAllOffsets failed: %v", err)
	}
	adminClient.Close()

	// Create a raw client for transaction operations. RequestRetries(0)
	// prevents kgo from retrying UNSTABLE_OFFSET_COMMIT (a retriable error)
	// which would block until timeout instead of returning the response.
	rawClient, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.RequestRetries(0),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rawClient.Close()

	// Step 1: InitProducerID to get a producer ID for our transaction
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

	// Step 2: AddOffsetsToTxn to register the group with the transaction
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

	txnCommitReq := kmsg.NewTxnOffsetCommitRequest()
	txnCommitReq.TransactionalID = txnID
	txnCommitReq.Group = groupID
	txnCommitReq.ProducerID = pid
	txnCommitReq.ProducerEpoch = epoch
	txnCommitReq.Generation = -1 // Simple consumer, not in active group session
	txnCommitReq.MemberID = ""
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

	// OffsetFetch with RequireStable=true should return UNSTABLE_OFFSET_COMMIT.
	fetchReq := kmsg.NewOffsetFetchRequest()
	fetchReq.RequireStable = true
	rg := kmsg.NewOffsetFetchRequestGroup()
	rg.Group = groupID
	rgt := kmsg.NewOffsetFetchRequestGroupTopic()
	rgt.Topic = testTopic
	rgt.Partitions = []int32{0}
	rg.Topics = append(rg.Topics, rgt)
	fetchReq.Groups = append(fetchReq.Groups, rg)

	fetchResp, err := fetchReq.RequestWith(ctx, rawClient)
	if err != nil {
		t.Fatalf("OffsetFetch RequireStable=true failed: %v", err)
	}
	if len(fetchResp.Groups) == 0 {
		t.Fatal("no groups in RequireStable=true response")
	}
	if fetchResp.Groups[0].ErrorCode != kerr.UnstableOffsetCommit.Code {
		t.Fatalf("expected UNSTABLE_OFFSET_COMMIT, got %v", kerr.ErrorForCode(fetchResp.Groups[0].ErrorCode))
	}

	// OffsetFetch with RequireStable=false should succeed even with pending txn.
	fetchReq.RequireStable = false
	fetchResp, err = fetchReq.RequestWith(ctx, rawClient)
	if err != nil {
		t.Fatalf("OffsetFetch RequireStable=false failed: %v", err)
	}
	if len(fetchResp.Groups) == 0 {
		t.Fatal("no groups in RequireStable=false response")
	}
	if fetchResp.Groups[0].ErrorCode != 0 {
		t.Fatalf("expected no error with RequireStable=false, got %v", kerr.ErrorForCode(fetchResp.Groups[0].ErrorCode))
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

	// After commit, OffsetFetch with RequireStable=true should succeed.
	fetchReq.RequireStable = true
	fetchResp, err = fetchReq.RequestWith(ctx, rawClient)
	if err != nil {
		t.Fatalf("OffsetFetch post-commit failed: %v", err)
	}
	if len(fetchResp.Groups) == 0 {
		t.Fatal("no groups in post-commit response")
	}
	if fetchResp.Groups[0].ErrorCode != 0 {
		t.Fatalf("expected no error after commit, got %v", kerr.ErrorForCode(fetchResp.Groups[0].ErrorCode))
	}
	if len(fetchResp.Groups[0].Topics) == 0 || len(fetchResp.Groups[0].Topics[0].Partitions) == 0 {
		t.Fatal("no partitions in post-commit response")
	}
	if offset := fetchResp.Groups[0].Topics[0].Partitions[0].Offset; offset != 5 {
		t.Fatalf("expected committed offset 5, got %d", offset)
	}
}
