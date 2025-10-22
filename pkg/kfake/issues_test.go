package kfake

import (
	"context"
	"strconv"
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

	c.ControlKey(1, func(kreq kmsg.Request) (kmsg.Response, error, bool) {
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
	c.ControlKey(1, func(kreq kmsg.Request) (kmsg.Response, error, bool) {
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

	chkfs := func(fs kgo.Fetches) {
		if fs.NumRecords() != 1 {
			t.Errorf("got %d records != exp 1", fs.NumRecords())
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
		chkfs(fs)
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
		chkfs(fs)
		if lr := leaderReqs.Load(); lr != 1 {
			t.Errorf("stage 2 leader reqs %d != exp 1", lr)
		}
		if fr := followerReqs.Load(); fr != 2 {
			t.Errorf("stage 2 follower reqs reqs %d != exp 2", fr)
		}
		allowFollower <- struct{}{} // again allow a background buffered fetch; this one will notice we need to redir back to follower
	}

	// Poll again. This is buffered, and the NEXT one should go back
	// to the leader again.
	{
		fs := cl.PollFetches(ctx)
		chkfs(fs)
		if lr := leaderReqs.Load(); lr != 1 {
			t.Errorf("stage 3 leader reqs %d != exp 1", lr)
		}
		if fr := followerReqs.Load(); fr != 3 {
			t.Errorf("stage 3 follower reqs reqs %d != exp 3", fr)
		}
		close(allowFollower) // allow all reqs; the next check is our last
	}

	// We now expect leader finally before the follower again.
	{
		fs := cl.PollFetches(ctx)
		chkfs(fs)
		if lr := leaderReqs.Load(); lr != 2 {
			t.Errorf("stage 4 leader reqs %d != exp 2", lr)
		}
		if fr := followerReqs.Load(); fr != 4 {
			t.Errorf("stage 4 follower reqs reqs %d != exp 4", fr)
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
