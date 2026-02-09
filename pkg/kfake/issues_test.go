package kfake

import (
	"context"
	"net"
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

	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	joinGroup := func(memberID string, metadata []byte) (*kmsg.JoinGroupResponse, error) {
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
	metadataV2 := []byte{0, 1, 2, 4} // changed metadata
	assignment := []byte{0, 0, 0, 0}

	// m1 joins, gets MemberIDRequired, rejoins with ID
	resp1, err := joinGroup("", metadataV1)
	if err != nil {
		t.Fatal(err)
	}
	if resp1.ErrorCode != kerr.MemberIDRequired.Code {
		t.Fatalf("expected MemberIDRequired, got %v", kerr.ErrorForCode(resp1.ErrorCode))
	}
	m1ID := resp1.MemberID

	resp1, err = joinGroup(m1ID, metadataV1)
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
	syncResp, err := syncGroup(m1ID, gen1, true, assignment)
	if err != nil {
		t.Fatal(err)
	}
	if syncResp.ErrorCode != 0 {
		t.Fatalf("m1 sync error: %v", kerr.ErrorForCode(syncResp.ErrorCode))
	}

	// m2 joins (initial), gets MemberIDRequired
	resp2, err := joinGroup("", metadataV1)
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
		resp2Rebalance, err2 = joinGroup(m2ID, metadataV1)
	}()
	<-m2Received

	resp1Rebalance, err1 := joinGroup(m1ID, metadataV1)
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
	_, _ = syncGroup(m1ID, gen2, resp1.LeaderID == m1ID, assignment)
	if resp2 != nil {
		_, _ = syncGroup(m2ID, gen2, resp2.LeaderID == m2ID, assignment)
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
		resp2, err = joinGroup(m2ID, metadataV2)
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
		if _, err := joinGroup(m1ID, metadataV1); err != nil {
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

	// Use v8+ format with Groups field to work with auto-negotiated versions
	fetchReq := kmsg.NewOffsetFetchRequest()
	fetchReq.RequireStable = true
	rg := kmsg.NewOffsetFetchRequestGroup()
	rg.Group = groupID
	rgt := kmsg.NewOffsetFetchRequestGroupTopic()
	rgt.Topic = testTopic
	rgt.Partitions = []int32{0}
	rg.Topics = append(rg.Topics, rgt)
	fetchReq.Groups = append(fetchReq.Groups, rg)

	// Test 1: OffsetFetch with RequireStable=true should return UNSTABLE_OFFSET_COMMIT.
	// Use Broker.Request to bypass the sharder, which retries retriable errors.
	kresp, err := rawClient.Broker(0).Request(ctx, &fetchReq)
	if err != nil {
		t.Fatalf("OffsetFetch RequireStable=true failed: %v", err)
	}
	fetchResp := kresp.(*kmsg.OffsetFetchResponse)
	if len(fetchResp.Groups) == 0 {
		t.Fatal("no groups in RequireStable=true response")
	}
	if fetchResp.Groups[0].ErrorCode != kerr.UnstableOffsetCommit.Code {
		t.Errorf("Test 1: expected UNSTABLE_OFFSET_COMMIT (88), got error code %d", fetchResp.Groups[0].ErrorCode)
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
		t.Errorf("Test 2: expected no error, got error code %d", fetchResp.Groups[0].ErrorCode)
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
	c.ControlKey(int16(kmsg.Metadata), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
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
