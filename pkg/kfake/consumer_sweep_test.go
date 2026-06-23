package kfake

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Regression tests from the consumer.go + consumer_direct.go audit sweep
// (round 8). Each TestAudit* below fails before its corresponding kgo fix.

// fenceNextFetch installs a control that answers exactly one fetch request
// with FENCED_LEADER_EPOCH for partition 0 of the topic. The client reacts by
// marking the cursor unusable and queueing an OffsetForLeaderEpoch validation
// at the cursor's current offset - the validation load's completion is the
// only thing that re-enables the cursor.
func fenceNextFetch(c *Cluster, topic string) {
	ti := c.TopicInfo(topic)
	pi := c.PartitionInfo(topic, 0)
	var fenced atomic.Bool
	c.ControlKey(int16(kmsg.Fetch), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if fenced.Swap(true) {
			return nil, nil, false // already fenced once; serve real data
		}
		req := kreq.(*kmsg.FetchRequest)
		resp := req.ResponseKind().(*kmsg.FetchResponse)
		rt := kmsg.NewFetchResponseTopic()
		rt.Topic = topic
		rt.TopicID = ti.TopicID
		rp := kmsg.NewFetchResponseTopicPartition()
		rp.Partition = 0
		rp.ErrorCode = kerr.FencedLeaderEpoch.Code
		rp.HighWatermark = pi.HighWatermark
		rp.LastStableOffset = pi.LastStableOffset
		rp.LogStartOffset = 0
		rt.Partitions = append(rt.Partitions, rp)
		resp.Topics = append(resp.Topics, rt)
		return resp, nil, true
	})
}

// failNextOFLE installs a control that answers exactly one
// OffsetForLeaderEpoch request with a retriable NOT_LEADER_FOR_PARTITION
// error, closing entered as it does. The retriable failure makes the client
// keep the validation load pending (it schedules a reload), opening a window
// in which the load exists but has not completed - without ever blocking the
// cluster's request loop.
func failNextOFLE(c *Cluster) (entered chan struct{}) {
	entered = make(chan struct{})
	var once atomic.Bool
	c.ControlKey(int16(kmsg.OffsetForLeaderEpoch), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if once.Swap(true) {
			return nil, nil, false // later validations are served for real
		}
		req := kreq.(*kmsg.OffsetForLeaderEpochRequest)
		resp := req.ResponseKind().(*kmsg.OffsetForLeaderEpochResponse)
		for _, rt := range req.Topics {
			st := kmsg.NewOffsetForLeaderEpochResponseTopic()
			st.Topic = rt.Topic
			for _, rp := range rt.Partitions {
				sp := kmsg.NewOffsetForLeaderEpochResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = kerr.NotLeaderForPartition.Code
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		close(entered)
		return resp, nil, true
	})
	return entered
}

// TestAuditSetOffsetsNotClobberedByPendingLoad verifies that SetOffsets on a
// previously-consumed partition wins over a pending offset load for that
// partition. Pre-fix, assignSetMatching set the cursor's offset but KEPT the
// pending load (an epoch validation at the old offset here); the load
// completed in the new session and overwrote the seek with its stale result,
// so the consumer silently resumed at the pre-seek position.
func TestAuditSetOffsetsNotClobberedByPendingLoad(t *testing.T) {
	t.Parallel()

	const (
		topic = "setoffsets-clobber"
		msgs  = 10
		seek  = int64(1)
	)

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	produceN(t, c, topic, msgs)

	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableFetchSessions(),
		kgo.FetchMaxWait(100*time.Millisecond),
	)

	// Consume everything so the cursor sits at the log end with a
	// non-negative last consumed epoch.
	collectRecords(t, cl, msgs, 8*time.Second)

	// Fence the next fetch so the client queues an epoch validation at
	// offset 10, and fail that validation retriably so it stays pending.
	ofleEntered := failNextOFLE(c)
	fenceNextFetch(c, topic)

	select {
	case <-ofleEntered:
	case <-time.After(8 * time.Second):
		t.Fatal("timed out waiting for the epoch validation load to be issued")
	}

	// The validation load for offset 10 is now pending (its retriable
	// failure scheduled a reload). Seek to offset 1: the seek must win.
	cl.SetOffsets(map[string]map[int32]kgo.EpochOffset{
		topic: {0: {Epoch: -1, Offset: seek}},
	})

	got := collectRecords(t, cl, 1, 8*time.Second)
	if got[0].Offset != seek {
		t.Fatalf("BUG REPRODUCED: first record after SetOffsets(%d) has offset %d; the pending epoch validation overwrote the seek", seek, got[0].Offset)
	}
}

// TestAuditStopSessionPromptWhilePendingReload verifies that stopping a
// consumer session (here via SetOffsets; equally any revoke, leave, or
// close) returns promptly while an offset load is in a retriable-error
// reload cycle. Pre-fix, the dying session busy-looped: with metadata
// fresher than MetadataMinAge there is no metadata wait, so the reload was
// re-issued on the dead context, failed instantly, and re-entered itself -
// burning CPU and holding the session stop (and the consumer lock) for the
// full MetadataMinAge.
func TestAuditStopSessionPromptWhilePendingReload(t *testing.T) {
	t.Parallel()

	const (
		topic = "stopsession-spin"
		msgs  = 5
	)

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	produceN(t, c, topic, msgs)

	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableFetchSessions(),
		kgo.FetchMaxWait(100*time.Millisecond),
		kgo.MetadataMinAge(10*time.Second), // widen the pre-fix spin so the assert is unambiguous
	)
	collectRecords(t, cl, msgs, 8*time.Second)

	ofleEntered := failNextOFLE(c)
	fenceNextFetch(c, topic)
	select {
	case <-ofleEntered:
	case <-time.After(8 * time.Second):
		t.Fatal("timed out waiting for the epoch validation load to be issued")
	}

	// The epoch validation is now reload-looping. Stopping the session
	// must park the pending load, not spin until MetadataMinAge.
	start := time.Now()
	cl.SetOffsets(map[string]map[int32]kgo.EpochOffset{
		topic: {0: {Epoch: -1, Offset: 0}},
	})
	if took := time.Since(start); took > 5*time.Second {
		t.Fatalf("BUG REPRODUCED: SetOffsets (a session stop) took %v; the dying session busy-looped its pending reload until MetadataMinAge", took)
	}
}

// TestAuditPendingReloadSurvivesPartitionRevoke verifies that an offset load
// caught mid-reload is carried into the next session when the session is stopped
// by a revoke that RETAINS the loading partition - the cooperative-rebalance
// shape (assignInvalidateMatching), driven deterministically here via
// RemoveConsumePartitions on a direct consumer with explicit partitions.
//
// The dying-session guard in listOrEpoch returns early once the session context
// is dead, but only AFTER the loads were parked in the session's waiting set;
// the reload goroutine still re-parks its loads on cancellation too. stopSession
// returns that waiting set to the next session, which re-issues the load. Were
// the guard instead to DROP the reload (return without re-parking it, as a more
// targeted "do not requeue on a dead session" fix would), the retained
// partition would have no cursor and no pending load in the new session - and
// because these are explicit ConsumePartitions, nothing re-lists it, so it would
// never consume again. The kept topic's records arriving proves the reload
// survived the revoke; if it were dropped, this test hangs to its deadline.
func TestAuditPendingReloadSurvivesPartitionRevoke(t *testing.T) {
	t.Parallel()

	const (
		kept    = "reload-survive-kept"
		dropped = "reload-survive-dropped"
		msgs    = 5
	)

	c := newCluster(t, NumBrokers(1), SeedTopics(1, kept, dropped))
	produceN(t, c, kept, msgs)

	var failKept atomic.Bool
	failKept.Store(true)
	var signaled atomic.Bool
	keptFailing := make(chan struct{})

	// Fail the kept topic's offset listing with a retriable error while
	// failKept is set, keeping it in a reload loop. Everything else (the
	// dropped topic, and the kept topic once we allow it) lists to offset 0,
	// the log start of these freshly seeded topics.
	c.ControlKey(int16(kmsg.ListOffsets), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		req := kreq.(*kmsg.ListOffsetsRequest)
		resp := req.ResponseKind().(*kmsg.ListOffsetsResponse)
		for _, rt := range req.Topics {
			st := kmsg.NewListOffsetsResponseTopic()
			st.Topic = rt.Topic
			for _, rp := range rt.Partitions {
				sp := kmsg.NewListOffsetsResponseTopicPartition()
				sp.Partition = rp.Partition
				if rt.Topic == kept && failKept.Load() {
					sp.ErrorCode = kerr.NotLeaderForPartition.Code
					if !signaled.Swap(true) {
						close(keptFailing)
					}
				} else {
					sp.Offset = 0
				}
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})

	cl := newPlainClient(t, c,
		kgo.FetchMaxWait(100*time.Millisecond),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			kept:    {0: kgo.NewOffset().AtStart()},
			dropped: {0: kgo.NewOffset().AtStart()},
		}),
	)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	recs := make(chan *kgo.Record, msgs)
	go func() {
		for ctx.Err() == nil {
			fs := cl.PollFetches(ctx)
			fs.EachRecord(func(r *kgo.Record) {
				select {
				case recs <- r:
				case <-ctx.Done():
				}
			})
		}
	}()

	// Wait for the kept partition to enter its retriable reload loop. Firing
	// the revoke now lands while the reload is in flight or in its backoff.
	select {
	case <-keptFailing:
	case <-time.After(10 * time.Second):
		t.Fatal("kept partition never entered the offset reload loop")
	}

	// Stop the session by revoking the OTHER partition. kept is retained, so
	// its in-flight reload must be carried into the new session.
	cl.RemoveConsumePartitions(map[string][]int32{dropped: {0}})

	// Let kept's listing succeed. It only resumes if the reload survived the
	// stop; if it was dropped, kept has no cursor and never lists again.
	failKept.Store(false)

	got := 0
	deadline := time.After(15 * time.Second)
	for got < msgs {
		select {
		case <-recs:
			got++
		case <-deadline:
			t.Fatalf("BUG: kept consumed %d/%d records after another partition was revoked; its pending offset load was dropped by the session stop", got, msgs)
		}
	}
}

// TestAuditTxnAbortRewindNotClobberedByPendingLoad verifies the
// GroupTransactSession abort path: End(TryAbort) rewinds consumption to the
// committed offsets via setOffsets so the aborted records are re-consumed.
// Pre-fix, a pending epoch validation (queued here by a fenced fetch; in
// production by any leader move, which is exactly when aborts cluster)
// survived the rewind and reset the cursor back to the pre-abort position:
// the aborted records were never re-consumed and the next transaction
// committed past them - an exactly-once violation.
func TestAuditTxnAbortRewindNotClobberedByPendingLoad(t *testing.T) {
	t.Parallel()

	const (
		topic     = "txn-rewind-clobber"
		group     = "txn-rewind-g"
		msgs      = 10
		committed = 2
	)

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	produceN(t, c, topic, msgs)

	s, err := kgo.NewGroupTransactSession(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.TransactionalID("txn-rewind-id"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableFetchSessions(),
		kgo.FetchMaxWait(100*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	pollN := func(n int) []*kgo.Record {
		t.Helper()
		var records []*kgo.Record
		for ctx.Err() == nil && len(records) < n {
			fetches := s.PollRecords(ctx, n-len(records))
			if err := fetches.Err0(); err != nil {
				t.Fatalf("poll error: %v", err)
			}
			records = append(records, fetches.Records()...)
		}
		if len(records) < n {
			t.Fatalf("polled %d/%d records", len(records), n)
		}
		return records
	}

	// Transaction 1: consume two records and commit, so the committed
	// offset is 2.
	if err := s.Begin(); err != nil {
		t.Fatal(err)
	}
	pollN(committed)
	if ok, err := s.End(ctx, kgo.TryCommit); !ok || err != nil {
		t.Fatalf("commit failed: ok=%v err=%v", ok, err)
	}

	// Transaction 2: consume the rest, then abort while an epoch
	// validation for the cursor's position is pending.
	if err := s.Begin(); err != nil {
		t.Fatal(err)
	}
	pollN(msgs - committed)

	ofleEntered := failNextOFLE(c)
	fenceNextFetch(c, topic)
	select {
	case <-ofleEntered:
	case <-time.After(8 * time.Second):
		t.Fatal("timed out waiting for the epoch validation load to be issued")
	}

	if _, err := s.End(ctx, kgo.TryAbort); err != nil {
		t.Fatalf("abort failed: %v", err)
	}

	// Transaction 3: the abort rewound us to offset 2; the previously
	// aborted records must be re-consumed, starting at offset 2.
	if err := s.Begin(); err != nil {
		t.Fatal(err)
	}
	re := pollN(msgs - committed)
	if _, err := s.End(ctx, kgo.TryAbort); err != nil {
		t.Fatalf("cleanup abort failed: %v", err)
	}
	if re[0].Offset != committed {
		t.Fatalf("BUG REPRODUCED: first record after abort rewind has offset %d, want %d; the pending epoch validation undid the rewind", re[0].Offset, committed)
	}
}

// TestAuditShareAddConsumeTopics verifies AddConsumeTopics and
// GetConsumeTopics work for share consumers. Pre-fix, both functions'
// early-return guards excluded exactly the share consumer (the only consumer
// kind where c.g and c.d are both nil), so AddConsumeTopics was a silent
// no-op and GetConsumeTopics returned nil - the share arms in their bodies
// were dead code.
func TestAuditShareAddConsumeTopics(t *testing.T) {
	t.Parallel()

	const (
		t1    = "share-add-t1"
		t2    = "share-add-t2"
		group = "share-add-g"
		msgs  = 3
	)

	c := newCluster(t,
		NumBrokers(1),
		SeedTopics(1, t1, t2),
		BrokerConfigs(map[string]string{
			"group.share.heartbeat.interval.ms": "100",
		}),
	)
	produceShareN(t, c, t1, group, msgs)

	cl := newShareConsumer(t, c, t1, group)
	collectRecords(t, cl, msgs, 8*time.Second)

	cl.AddConsumeTopics(t2)

	topics := cl.GetConsumeTopics()
	var sawT2 bool
	for _, topic := range topics {
		sawT2 = sawT2 || topic == t2
	}
	if !sawT2 {
		t.Fatalf("BUG REPRODUCED: GetConsumeTopics after AddConsumeTopics(%q) = %v; AddConsumeTopics/GetConsumeTopics are no-ops for share consumers", t2, topics)
	}

	produceShareN(t, c, t2, group, msgs)
	records := collectRecords(t, cl, msgs, 8*time.Second)
	for _, r := range records {
		if r.Topic != t2 {
			t.Fatalf("expected only %q records after consuming %q fully, got one from %q", t2, t1, r.Topic)
		}
	}
}

// TestAuditNegativeListOffsetsRejected verifies that a ListOffsets success
// response carrying a negative offset - which no legitimate listing produces
// - is rejected as a per-partition load error. Pre-fix, the client clamped
// the offset to 0 and silently re-consumed the partition from the start.
func TestAuditNegativeListOffsetsRejected(t *testing.T) {
	t.Parallel()

	const (
		topic = "negative-list-offsets"
		msgs  = 5
	)

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	produceN(t, c, topic, msgs)

	var once atomic.Bool
	c.ControlKey(int16(kmsg.ListOffsets), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if once.Swap(true) {
			return nil, nil, false // serve later listings for real
		}
		req := kreq.(*kmsg.ListOffsetsRequest)
		resp := req.ResponseKind().(*kmsg.ListOffsetsResponse)
		for _, rt := range req.Topics {
			st := kmsg.NewListOffsetsResponseTopic()
			st.Topic = rt.Topic
			for _, rp := range rt.Partitions {
				sp := kmsg.NewListOffsetsResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = 0
				sp.Timestamp = -1
				sp.Offset = -42 // protocol violation: negative offset, no error
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})

	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtEnd()),
		kgo.FetchMaxWait(100*time.Millisecond),
	)

	// Poll for a while: we must see the negative-offset error surfaced and
	// no records (consuming at the end). Pre-fix, the clamp to offset 0
	// re-consumes the whole partition instead.
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var sawNegativeErr bool
	for ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		if records := fetches.Records(); len(records) > 0 {
			t.Fatalf("BUG REPRODUCED: consumed record at offset %d; a negative ListOffsets offset was clamped to 0 and re-consumed the partition", records[0].Offset)
		}
		fetches.EachError(func(_ string, _ int32, err error) {
			if strings.Contains(err.Error(), "negative offset") {
				sawNegativeErr = true
			}
		})
	}
	if !sawNegativeErr {
		t.Fatal("expected the negative ListOffsets offset to surface as a poll error")
	}
}
