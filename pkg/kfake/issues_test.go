package kfake

import (
	"context"
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
		kgo.RecheckPreferredReplicaInterval(time.Second),
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

	// Sleep 1s. We are now polling a buffered fetch from the follower.
	time.Sleep(time.Second)
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
			err := cl.ProduceSync(context.TODO(), r1, r2, r3).FirstErr()
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
	for i := 0; i < 5; i++ {
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

			for time.Since(start) < 3*time.Second {
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

	// TEST 1: Committed data should satisfy MinBytes immediately
	t.Log("Test 1: Committed data satisfies MinBytes immediately")

	// Produce and commit a transaction
	if err := producer.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := producer.ProduceSync(ctx, &kgo.Record{Value: []byte("committed-data")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if err := producer.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}

	// Verify LSO advanced
	pi := c.PartitionInfo(testTopic, 0)
	if pi.LastStableOffset != pi.HighWatermark {
		t.Fatalf("LSO should equal HWM after commit: LSO=%d, HWM=%d", pi.LastStableOffset, pi.HighWatermark)
	}
	t.Logf("After commit: HWM=%d, LSO=%d", pi.HighWatermark, pi.LastStableOffset)

	// Create a readCommitted consumer - should get data immediately
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
		t.Errorf("Test 1: fetch took too long: %v (expected immediate)", elapsed)
	}
	t.Logf("Test 1 passed: got committed data in %v", elapsed)

	// TEST 2: Uncommitted data should NOT satisfy MinBytes
	t.Log("Test 2: Uncommitted data should not satisfy MinBytes")

	// Record current HWM before new transaction
	hwmBeforeTxn := c.PartitionInfo(testTopic, 0).HighWatermark

	// Start a new transaction but don't commit
	if err := producer.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := producer.ProduceSync(ctx, &kgo.Record{Value: []byte("uncommitted-data")}).FirstErr(); err != nil {
		t.Fatal(err)
	}

	// LSO should not have advanced
	pi = c.PartitionInfo(testTopic, 0)
	if pi.LastStableOffset != hwmBeforeTxn {
		t.Errorf("LSO should not advance for uncommitted txn: LSO=%d, expected=%d", pi.LastStableOffset, hwmBeforeTxn)
	}
	t.Logf("After uncommitted produce: HWM=%d, LSO=%d", pi.HighWatermark, pi.LastStableOffset)

	// Create a readCommitted consumer starting after committed data
	// Use a short context timeout to verify it waits
	shortCtx, shortCancel := context.WithTimeout(ctx, 500*time.Millisecond)
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

	// Should have waited until context timeout since no committed data
	if elapsed < 400*time.Millisecond {
		t.Errorf("Test 2: fetch returned too quickly: %v (expected ~500ms wait)", elapsed)
	}
	if fetches.NumRecords() > 0 {
		t.Errorf("Test 2: readCommitted should not see uncommitted data, got %d records", fetches.NumRecords())
	}
	t.Logf("Test 2 passed: waited %v for uncommitted data (expected ~500ms)", elapsed)

	// TEST 3: After commit, the waiting consumer should get data
	// This is the key test for the MinBytes fix
	t.Log("Test 3: After commit, readCommitted consumer should get data")

	// Start a consumer that will wait
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

	// Give the consumer time to establish the watch
	time.Sleep(200 * time.Millisecond)

	// Now commit the transaction
	commitStart := time.Now()
	if err := producer.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}
	t.Logf("Transaction committed at %v", time.Since(commitStart))

	// Wait for the fetch to complete
	select {
	case <-fetchDone:
	case <-ctx.Done():
		t.Fatal("timeout waiting for fetch after commit")
	}
	consumer3.Close()

	// The fetch should have returned shortly after commit, not waiting for full MaxWait
	if fetchElapsed > 2*time.Second {
		t.Errorf("Test 3: fetch took too long after commit: %v (expected <2s)", fetchElapsed)
	}
	if fetchResult.NumRecords() != 1 {
		t.Errorf("Test 3: expected 1 record after commit, got %d", fetchResult.NumRecords())
	}
	t.Logf("Test 3 passed: got data %v after starting fetch (commit woke the watcher)", fetchElapsed)

	// TEST 4: Verify read_uncommitted sees data immediately (baseline)
	t.Log("Test 4: read_uncommitted should see uncommitted data immediately")

	// Start another uncommitted transaction
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
		t.Errorf("Test 4: read_uncommitted took too long: %v", elapsed)
	}
	if fetches.NumRecords() == 0 {
		t.Errorf("Test 4: read_uncommitted should see uncommitted data, HWM=%d LSO=%d", newHWM, newLSO)
	}
	t.Logf("Test 4 passed: read_uncommitted got data in %v", elapsed)

	// Cleanup
	if err := producer.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Logf("cleanup abort: %v", err)
	}
}

// TestGroupRebalanceOnNonLeaderMetadataChange tests that a rebalance is triggered
// when a non-leader member rejoins with changed metadata.
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

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	groupID := "test-group-" + strconv.FormatInt(time.Now().UnixNano(), 36)

	// Create a client for sending raw requests
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	// Helper to send JoinGroup and get response
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

	// Helper to send SyncGroup
	syncGroup := func(memberID string, gen int32, isLeader bool, assignment []byte) (*kmsg.SyncGroupResponse, error) {
		req := kmsg.NewSyncGroupRequest()
		req.Group = groupID
		req.MemberID = memberID
		req.Generation = gen
		if isLeader {
			// Leader sends assignments for all members
			assign := kmsg.NewSyncGroupRequestGroupAssignment()
			assign.MemberID = memberID
			assign.MemberAssignment = assignment
			req.GroupAssignment = append(req.GroupAssignment, assign)
		}
		resp, err := req.RequestWith(ctx, cl)
		return resp, err
	}

	metadataV1 := []byte{0, 1, 2, 3} // Initial metadata
	metadataV2 := []byte{0, 1, 2, 4} // Changed metadata (simulates partition revocation)
	assignment := []byte{0, 0, 0, 0} // Dummy assignment

	// Step 1: m1 joins, gets MemberIDRequired, rejoins with ID
	t.Log("Step 1: m1 initial join")
	resp1, err := joinGroup("", metadataV1)
	if err != nil {
		t.Fatal(err)
	}
	if resp1.ErrorCode != kerr.MemberIDRequired.Code {
		t.Fatalf("expected MemberIDRequired, got %v", kerr.ErrorForCode(resp1.ErrorCode))
	}
	m1ID := resp1.MemberID
	t.Logf("m1 ID: %s", m1ID)

	// Step 2: m1 rejoins with ID, becomes leader
	t.Log("Step 2: m1 rejoins with ID")
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
	t.Logf("m1 is leader, generation %d", gen1)

	// Step 3: m1 syncs as leader
	t.Log("Step 3: m1 syncs")
	syncResp, err := syncGroup(m1ID, gen1, true, assignment)
	if err != nil {
		t.Fatal(err)
	}
	if syncResp.ErrorCode != 0 {
		t.Fatalf("m1 sync error: %v", kerr.ErrorForCode(syncResp.ErrorCode))
	}

	// Step 4: m2 joins (initial)
	t.Log("Step 4: m2 initial join")
	resp2, err := joinGroup("", metadataV1)
	if err != nil {
		t.Fatal(err)
	}
	if resp2.ErrorCode != kerr.MemberIDRequired.Code {
		t.Fatalf("expected MemberIDRequired for m2, got %v", kerr.ErrorForCode(resp2.ErrorCode))
	}
	m2ID := resp2.MemberID
	t.Logf("m2 ID: %s", m2ID)

	// Step 5: m2 rejoins with ID, triggering rebalance
	// Both m1 and m2 must join concurrently for rebalance to complete
	t.Log("Step 5: m1 and m2 join concurrently (m2 triggers rebalance)")
	var wg sync.WaitGroup
	var resp1Rebalance, resp2Rebalance *kmsg.JoinGroupResponse
	var err1, err2 error
	wg.Add(2)
	go func() {
		defer wg.Done()
		resp1Rebalance, err1 = joinGroup(m1ID, metadataV1)
	}()
	go func() {
		defer wg.Done()
		resp2Rebalance, err2 = joinGroup(m2ID, metadataV1)
	}()
	wg.Wait()

	if err1 != nil {
		t.Logf("m1 rejoin error: %v", err1)
	}
	if err2 != nil {
		t.Logf("m2 join error: %v", err2)
	}
	if resp1Rebalance == nil {
		t.Fatal("m1 rejoin returned nil response")
	}
	resp1 = resp1Rebalance
	resp2 = resp2Rebalance

	gen2 := resp1.Generation
	t.Logf("After rebalance: generation %d, m1 leader=%v", gen2, resp1.LeaderID == m1ID)

	// Sync both members
	_, _ = syncGroup(m1ID, gen2, resp1.LeaderID == m1ID, assignment)
	if resp2 != nil {
		_, _ = syncGroup(m2ID, gen2, resp2.LeaderID == m2ID, assignment)
	}

	// Now group should be Stable with m1 as leader, m2 as follower
	// Generation should be 2

	// Step 7: THE CRITICAL TEST
	// m2 (non-leader) rejoins with CHANGED metadata while group is Stable
	// With the bug: m2 would get the same generation back (no rebalance)
	// With the fix: a new rebalance should be triggered, requiring both to rejoin
	t.Log("Step 7: m2 (non-leader) rejoins with CHANGED metadata")

	// Use a short timeout context to detect if rebalance was triggered
	// If the bug exists, m2's JoinGroup returns immediately with same generation
	// If fixed, m2's JoinGroup triggers rebalance and waits for m1
	shortCtx, shortCancel := context.WithTimeout(ctx, time.Second)

	joinGroupShort := func(memberID string, metadata []byte) (*kmsg.JoinGroupResponse, error) {
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
		return req.RequestWith(shortCtx, cl)
	}

	resp2, err = joinGroupShort(m2ID, metadataV2)
	shortCancel()

	if err != nil && err.Error() == "context deadline exceeded" {
		// CORRECT behavior: rebalance was triggered, JoinGroup is waiting for m1
		t.Log("CORRECT: m2's changed metadata triggered rebalance (JoinGroup is waiting)")

		// Complete the rebalance by having both rejoin
		var resp2Final *kmsg.JoinGroupResponse
		wg.Add(2)
		go func() {
			defer wg.Done()
			_, _ = joinGroup(m1ID, metadataV1)
		}()
		go func() {
			defer wg.Done()
			resp2Final, _ = joinGroup(m2ID, metadataV2)
		}()
		wg.Wait()

		if resp2Final != nil && resp2Final.Generation > gen2 {
			t.Logf("CORRECT: Rebalance completed, new generation %d", resp2Final.Generation)
		}
	} else if err != nil {
		t.Fatalf("unexpected error: %v", err)
	} else if resp2 != nil && resp2.Generation == gen2 && resp2.ErrorCode == 0 {
		// BUG: server returned same generation without triggering rebalance
		t.Errorf("BUG: m2 rejoined with changed metadata but got same generation %d (no rebalance triggered)", gen2)
	} else if resp2 != nil {
		t.Logf("Response: gen=%d, err=%v", resp2.Generation, kerr.ErrorForCode(resp2.ErrorCode))
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
