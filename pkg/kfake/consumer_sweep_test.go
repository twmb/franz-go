package kfake

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Regression tests for consumer.go + consumer_direct.go. Each TestAudit*
// below fails before its corresponding kgo fix.

// fenceNextFetch fails exactly one fetch of partition 0 of the topic with
// FENCED_LEADER_EPOCH. The client marks the cursor unusable and queues an
// OffsetForLeaderEpoch validation. Only that validation re-enables the
// cursor.
func fenceNextFetch(c *Cluster, topic string) {
	c.Fault(Fault{
		Keys:       []kmsg.Key{kmsg.Fetch},
		Topic:      topic,
		Partitions: []int32{0},
		Err:        kerr.FencedLeaderEpoch,
		Count:      1,
	})
}

// failNextOFLE fails exactly one OffsetForLeaderEpoch request with a
// retryable NOT_LEADER_FOR_PARTITION. The client schedules a reload and
// keeps the validation load pending, so the load exists but has not
// completed.
func failNextOFLE(c *Cluster) *FaultHandle {
	return c.Fault(Fault{
		Keys: []kmsg.Key{kmsg.OffsetForLeaderEpoch},
		Err:  kerr.NotLeaderForPartition,
	})
}

// TestAuditSetOffsetsNotClobberedByPendingLoad verifies that SetOffsets wins
// over an offset load pending for the partition, with and without a cursor
// in use. Pre-fix, assignSetMatching kept the load and set nothing when the
// partition had no cursor in use yet; the load completed in the new session
// and the consumer silently started at the load's offset.
// The test also verifies that UncommittedOffsets reports the set offset, so
// the next commit commits the seek, and that a negative offset is skipped.
func TestAuditSetOffsetsNotClobberedByPendingLoad(t *testing.T) {
	t.Parallel()

	const (
		topic = "setoffsets-clobber"
		group = "setoffsets-clobber-g"
		msgs  = 10
		seek  = int64(1)
	)

	newConsumer := func(t *testing.T, c *Cluster) *kgo.Client {
		return newPlainClient(t, c,
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup(group),
			kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
			kgo.DisableAutoCommit(),
			kgo.DisableFetchSessions(),
			kgo.FetchMaxWait(100*time.Millisecond),
		)
	}

	for _, tt := range []struct {
		name   string
		direct bool // no group: UncommittedOffsets has nothing to report
		// load is the request key of partition 0's pending offset load.
		load kmsg.Key
		// pend returns a consumer with partition 0's offset load pending.
		pend func(*testing.T, *Cluster) *kgo.Client
	}{
		{"direct, first load", true, kmsg.ListOffsets, func(t *testing.T, c *Cluster) *kgo.Client {
			// A direct consumer's first load lists the start offset.
			return newPlainClient(t, c,
				kgo.ConsumeTopics(topic),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
				kgo.DisableFetchSessions(),
				kgo.FetchMaxWait(100*time.Millisecond),
			)
		}},

		{"reload after consuming", false, kmsg.OffsetForLeaderEpoch, func(t *testing.T, c *Cluster) *kgo.Client {
			// Consume everything so the cursor sits at the log end with
			// a non-negative last consumed epoch, then fence the next
			// fetch so the client validates offset 10.
			cl := newConsumer(t, c)
			consumeN(t, cl, msgs, 8*time.Second)
			fenceNextFetch(c, topic)
			return cl
		}},

		{"first load", false, kmsg.OffsetForLeaderEpoch, func(t *testing.T, c *Cluster) *kgo.Client {
			// Commit offset 3 at epoch 0 so that the consumer's first
			// load validates it. The partition has no cursor in use
			// until that load finishes.
			commitOffset(t, c, group, topic, 3)
			return newConsumer(t, c)
		}},

		{"no commit", false, kmsg.ListOffsets, func(t *testing.T, c *Cluster) *kgo.Client {
			// No commit and a non-absolute reset: the first load lists
			// the start offset, and the group has no uncommit for the
			// partition until a record is polled from the partition.
			return newConsumer(t, c)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
			produceN(t, c, topic, msgs)

			// Fail every load retryably until the seek is in, so the
			// load is still pending when SetOffsets runs.
			load := c.Fault(Fault{
				Keys:  []kmsg.Key{tt.load},
				Err:   kerr.NotLeaderForPartition,
				Count: -1,
			})
			cl := tt.pend(t, c)
			waitHits(t, load, 1, "the offset load was never issued")

			cl.SetOffsets(map[string]map[int32]kgo.EpochOffset{
				topic: {0: {Epoch: -1, Offset: seek}},
			})
			load.Remove()
			// A negative offset is not an offset: skipped.
			cl.SetOffsets(map[string]map[int32]kgo.EpochOffset{
				topic: {0: {Epoch: -1, Offset: -1}},
			})
			if got := cl.UncommittedOffsets()[topic][0].Offset; !tt.direct && got != seek {
				t.Fatalf("BUG REPRODUCED: uncommitted offset after SetOffsets(%d) is %d; the next commit would not commit the seek", seek, got)
			}

			got := consumeN(t, cl, 1, 8*time.Second)
			if got[0].Offset != seek {
				t.Fatalf("BUG REPRODUCED: first record after SetOffsets(%d) has offset %d; the pending epoch validation overwrote the seek", seek, got[0].Offset)
			}
		})
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
	consumeN(t, cl, msgs, 8*time.Second)

	ofle := failNextOFLE(c)
	fenceNextFetch(c, topic)
	waitHits(t, ofle, 1, "the epoch validation load was never issued")

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

	// Fail the kept topic's offset listing with a retryable error. It stays
	// in a reload loop until we remove the fault.
	failKept := c.Fault(Fault{
		Keys:  []kmsg.Key{kmsg.ListOffsets},
		Topic: kept,
		Err:   kerr.NotLeaderForPartition,
		Count: -1,
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

	// Wait for the kept partition to enter its retryable reload loop. Firing
	// the revoke now lands while the reload is in flight or in its backoff.
	waitHits(t, failKept, 1, "kept partition never entered the offset reload loop")

	// Stop the session by revoking the OTHER partition. kept is retained, so
	// its in-flight reload must be carried into the new session.
	cl.RemoveConsumePartitions(map[string][]int32{dropped: {0}})

	// Let kept's listing succeed. It only resumes if the reload survived the
	// stop; if it was dropped, kept has no cursor and never lists again.
	failKept.Remove()

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

// TestSetOffsetsWhileCommitInFlight verifies that a SetOffsets below a commit
// that is in flight is committed by the next commit. The commit response
// moves head forward to the committed offset; pre-fix, the response also
// moved the seek's head forward, and the next commit saw nothing to commit.
// AutoCommitMarks keeps head where the response left head (default
// autocommit moves head to dirty on the next poll), so the test uses
// AutoCommitMarks. The test holds a commit at the broker; the SetOffsets
// table test above is about pending offset loads and did not fit.
func TestSetOffsetsWhileCommitInFlight(t *testing.T) {
	t.Parallel()

	const (
		topic = "setoffsets-inflight"
		group = "setoffsets-inflight-g"
		msgs  = 10
		seek  = int64(5)
	)

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	produceN(t, c, topic, msgs)

	// Record every commit's offset, and hold the commit of msgs at the
	// broker until released.
	var (
		mu       sync.Mutex
		commits  []int64
		held     = make(chan struct{})
		release  = make(chan struct{})
		holdOnce sync.Once
	)
	c.ControlKey(int16(kmsg.OffsetCommit), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		req := kreq.(*kmsg.OffsetCommitRequest)
		if req.Group != group || len(req.Topics) != 1 || len(req.Topics[0].Partitions) != 1 {
			return nil, nil, false
		}
		offset := req.Topics[0].Partitions[0].Offset
		mu.Lock()
		commits = append(commits, offset)
		mu.Unlock()
		if offset == msgs {
			holdOnce.Do(func() { close(held) })
			c.SleepControl(func() { <-release })
		}
		return nil, nil, false
	})

	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.AutoCommitMarks(),
		kgo.AutoCommitInterval(time.Hour), // only the manual commits below
		kgo.FetchMaxWait(100*time.Millisecond),
	)
	consumeN(t, cl, msgs, 8*time.Second)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	done := make(chan error, 1)
	cl.CommitOffsets(ctx, map[string]map[int32]kgo.EpochOffset{topic: {0: {Epoch: -1, Offset: msgs}}}, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, err error) {
		done <- err
	})
	select {
	case <-held:
	case <-ctx.Done():
		t.Fatal("the commit never reached the broker")
	}

	cl.SetOffsets(map[string]map[int32]kgo.EpochOffset{topic: {0: {Epoch: -1, Offset: seek}}})
	close(release)
	if err := <-done; err != nil {
		t.Fatalf("commit failed: %v", err)
	}

	if got := cl.MarkedOffsets()[topic][0].Offset; got != seek {
		t.Fatalf("BUG REPRODUCED: marked offset after the commit response is %d, want the seek %d", got, seek)
	}
	if err := cl.CommitMarkedOffsets(ctx); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if last := commits[len(commits)-1]; last != seek {
		t.Fatalf("BUG REPRODUCED: commits the broker received are %v; the seek to %d was not committed", commits, seek)
	}
}

// TestSetOffsetsReenablesFatalCursorWithPendingLoad verifies that
// SetOffsets re-enables a cursor stopped by a batch over
// MaxDecompressBatchBytes when the cursor also has a pending offset load,
// here an epoch validation from a leader move. Pre-fix, assignPartitions
// dropped the load and skipped clearing the fatal flag, so allowUsable did
// nothing and the partition was never fetched again. The pending-load table
// test above has no stopped cursor, and a stopped cursor fetches nothing, so
// a fence cannot queue its load; neither fit.
func TestSetOffsetsReenablesFatalCursorWithPendingLoad(t *testing.T) {
	t.Parallel()

	const (
		topic = "setoffsets-fatal-load"
		small = 3 // records before the batch over the limit
	)

	c := newCluster(t, NumBrokers(2), SeedTopics(1, topic))
	producer := newPlainClient(t, c,
		kgo.DefaultProduceTopic(topic),
		kgo.ProducerBatchCompression(kgo.GzipCompression()),
	)
	for range small {
		produceSync(t, producer, kgo.StringRecord("v"))
	}

	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.MaxDecompressBatchBytes(1<<10),
		kgo.MetadataMinAge(50*time.Millisecond), // the leader move below is seen by a refresh
		kgo.MetadataMaxAge(100*time.Millisecond),
		kgo.DisableFetchSessions(),
		kgo.FetchMaxWait(100*time.Millisecond),
	)
	consumeN(t, cl, small, 8*time.Second) // the cursor now has an epoch to validate

	// A record over the limit stops the cursor; a poll returns the error.
	produceSync(t, producer, &kgo.Record{Value: make([]byte, 4<<10)})
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var tooLarge *kgo.ErrDecompressTooLarge
	for ctx.Err() == nil && tooLarge == nil {
		for _, e := range cl.PollFetches(ctx).Errors() {
			var ok bool
			if tooLarge, ok = errors.AsType[*kgo.ErrDecompressTooLarge](e.Err); !ok {
				t.Fatalf("poll error: %v", e.Err)
			}
		}
	}
	if tooLarge == nil {
		t.Fatal("the batch over MaxDecompressBatchBytes never surfaced")
	}
	produceSync(t, producer, kgo.StringRecord("after"))

	// Move the leader: the metadata refresh queues an epoch validation
	// for the stopped cursor, and the fault keeps the validation pending.
	ofle := c.Fault(Fault{
		Keys:  []kmsg.Key{kmsg.OffsetForLeaderEpoch},
		Err:   kerr.NotLeaderForPartition,
		Count: -1,
	})
	if err := c.MoveTopicPartition(topic, 0, 1-c.LeaderFor(topic, 0)); err != nil {
		t.Fatal(err)
	}
	waitHits(t, ofle, 1, "the epoch validation was never issued")

	cl.SetOffsets(map[string]map[int32]kgo.EpochOffset{topic: {0: {Epoch: -1, Offset: tooLarge.NextOffset}}})
	ofle.Remove()
	got := consumeN(t, cl, 1, 8*time.Second)
	if got[0].Offset != tooLarge.NextOffset {
		t.Fatalf("BUG REPRODUCED: first record after SetOffsets(%d) has offset %d", tooLarge.NextOffset, got[0].Offset)
	}
}

// commitOffset commits offset at leader epoch 0 for partition 0 of topic in
// the group with a raw OffsetCommit, as a prior member would have.
func commitOffset(t *testing.T, c *Cluster, group, topic string, offset int64) {
	t.Helper()
	commit := kmsg.NewPtrOffsetCommitRequest()
	commit.Group = group
	ct := kmsg.NewOffsetCommitRequestTopic()
	ct.Topic, ct.TopicID = topic, c.TopicInfo(topic).TopicID
	cp := kmsg.NewOffsetCommitRequestTopicPartition()
	cp.Offset, cp.LeaderEpoch = offset, 0
	ct.Partitions = append(ct.Partitions, cp)
	commit.Topics = append(commit.Topics, ct)
	resp, err := commit.RequestWith(context.Background(), newPlainClient(t, c))
	if err != nil {
		t.Fatal(err)
	}
	if code := resp.Topics[0].Partitions[0].ErrorCode; code != 0 {
		t.Fatalf("commit answered %d", code)
	}
}

// TestUncommitOrigins checks every way a group partition's uncommitted
// entry comes to exist against every reader of the entry. The origins: no
// entry, a fetched commit, the first poll, a SetOffsets before the first
// poll, and a mark before the first poll. The readers: UncommittedOffsets,
// MarkedOffsets, CommittedOffsets, then the group's offset at the broker
// after CommitMarkedOffsets and after CommitUncommittedOffsets. Pre-fix,
// CommittedOffsets reported an absolute ConsumeResetOffset (AfterMilli's
// millisecond included) before the first poll and 0 after the first poll,
// and a mark before the first poll left dirty and committed at 0. The
// pending-load table test above polls every partition, and the abort table
// test below polls before the rewind, so neither fit.
func TestUncommitOrigins(t *testing.T) {
	t.Parallel()

	const (
		topic = "uncommit-origins"
		group = "uncommit-origins-g"
		msgs  = 10
		none  = int64(-1) // the reader omits the partition, or the group has no commit
	)

	start := kgo.NewOffset().AtStart()
	for _, tt := range []struct {
		name  string
		reset kgo.Offset
		// load is the request key of the partition's first offset load.
		// The test fails the load retryably so that the partition is
		// never polled; 0 polls every record instead.
		load   kmsg.Key
		before func(*testing.T, *Cluster)    // before the consumer starts
		after  func(*testing.T, *kgo.Client) // once the offset fetch is done

		uncommitted, marked, committed int64 // before any commit
		afterMarked, afterUncommitted  int64 // the broker's offset after each commit, in that order
	}{
		{"no entry", kgo.NewOffset().AfterMilli(time.Now().UnixMilli()), kmsg.ListOffsets, nil, nil,
			none, none, none, none, none},

		{"fetched commit", start, kmsg.OffsetForLeaderEpoch, func(t *testing.T, c *Cluster) {
			commitOffset(t, c, group, topic, 3)
		}, nil, none, none, 3, 3, 3},

		{"first poll", start, 0, nil, func(t *testing.T, cl *kgo.Client) {
			consumeN(t, cl, msgs, 8*time.Second)
		}, msgs, none, none, none, msgs},

		{"first poll, then a mark", start, 0, nil, func(t *testing.T, cl *kgo.Client) {
			consumeN(t, cl, msgs, 8*time.Second)
			cl.MarkCommitOffsets(map[string]map[int32]kgo.EpochOffset{topic: {0: {Epoch: -1, Offset: 4}}})
		}, msgs, 4, none, 4, msgs},

		{"SetOffsets before the first poll", start, kmsg.ListOffsets, nil, func(t *testing.T, cl *kgo.Client) {
			cl.SetOffsets(map[string]map[int32]kgo.EpochOffset{topic: {0: {Epoch: -1, Offset: 5}}})
		}, 5, 5, none, 5, 5},

		{"a mark before the first poll", start, kmsg.ListOffsets, nil, func(t *testing.T, cl *kgo.Client) {
			cl.MarkCommitOffsets(map[string]map[int32]kgo.EpochOffset{topic: {0: {Epoch: -1, Offset: 7}}})
		}, 7, 7, none, 7, 7},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
			produceN(t, c, topic, msgs)
			if tt.before != nil {
				tt.before(t, c)
			}
			var load *FaultHandle
			if tt.load != 0 {
				load = c.Fault(Fault{Keys: []kmsg.Key{tt.load}, Err: kerr.NotLeaderForPartition, Count: -1})
			}
			cl := newPlainClient(t, c,
				kgo.ConsumeTopics(topic),
				kgo.ConsumerGroup(group),
				kgo.ConsumeResetOffset(tt.reset),
				kgo.AutoCommitMarks(),
				kgo.AutoCommitInterval(time.Hour), // only the manual commits below
				kgo.DisableFetchSessions(),
				kgo.FetchMaxWait(100*time.Millisecond),
			)
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			if load != nil {
				// A poll joins the group. Once the load is issued, the
				// group's offset fetch is done, and the poll is stopped
				// so that nothing drains records a SetOffsets fetches.
				pollCtx, pollCancel := context.WithCancel(ctx)
				pollDone := make(chan struct{})
				go func() {
					defer close(pollDone)
					cl.PollFetches(pollCtx)
				}()
				waitHits(t, load, 1, "the offset load was never issued")
				pollCancel()
				<-pollDone
			}
			if tt.after != nil {
				tt.after(t, cl)
			}

			// value returns the partition's offset in a reader's map, or none.
			value := func(m map[string]map[int32]kgo.EpochOffset) int64 {
				if o, ok := m[topic][0]; ok {
					return o.Offset
				}
				return none
			}
			if got := value(cl.UncommittedOffsets()); got != tt.uncommitted {
				t.Errorf("UncommittedOffsets reports %d, want %d", got, tt.uncommitted)
			}
			if got := value(cl.MarkedOffsets()); got != tt.marked {
				t.Errorf("MarkedOffsets reports %d, want %d", got, tt.marked)
			}
			if got := value(cl.CommittedOffsets()); got != tt.committed {
				t.Errorf("CommittedOffsets reports %d, want %d", got, tt.committed)
			}
			if tt.committed == none && cl.CommittedOffsets() != nil {
				t.Errorf("CommittedOffsets with no commit is non-nil")
			}

			adm := kadm.NewClient(cl)
			broker := func() int64 {
				t.Helper()
				os, err := adm.FetchOffsets(ctx, group)
				if err != nil {
					t.Fatal(err)
				}
				if o, ok := os.Lookup(topic, 0); ok && o.At >= 0 {
					return o.At
				}
				return none
			}
			if err := cl.CommitMarkedOffsets(ctx); err != nil {
				t.Fatal(err)
			}
			if got := broker(); got != tt.afterMarked {
				t.Errorf("BUG REPRODUCED: the group's offset after CommitMarkedOffsets is %d, want %d", got, tt.afterMarked)
			}
			if err := cl.CommitUncommittedOffsets(ctx); err != nil {
				t.Fatal(err)
			}
			if got := broker(); got != tt.afterUncommitted {
				t.Errorf("BUG REPRODUCED: the group's offset after CommitUncommittedOffsets is %d, want %d", got, tt.afterUncommitted)
			}
			if got := value(cl.CommittedOffsets()); got != tt.afterUncommitted {
				t.Errorf("CommittedOffsets after the commits reports %d, want %d", got, tt.afterUncommitted)
			}
		})
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
//
// The "no commit" row verifies that a partition the group has no commit for
// rewinds to where the transaction started reading, and that CommittedOffsets
// omits the partition before the abort. Pre-fix, the first poll recorded
// offset 0 as committed, and the abort rewound to 0.
func TestAuditTxnAbortRewindNotClobberedByPendingLoad(t *testing.T) {
	t.Parallel()

	const (
		topic = "txn-rewind-clobber"
		group = "txn-rewind-g"
		msgs  = 10
	)

	for _, tt := range []struct {
		name      string
		reset     kgo.Offset
		precommit bool  // consume and commit the records before start first
		start     int64 // where the aborted transaction starts reading; the abort rewinds to it
	}{
		{"after a commit", kgo.NewOffset().AtStart(), true, 2},
		{"no commit", kgo.NewOffset().AtEnd().Relative(-5), false, 5},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
			produceN(t, c, topic, msgs)

			s, err := kgo.NewGroupTransactSession(
				kgo.SeedBrokers(c.ListenAddrs()...),
				kgo.ConsumeTopics(topic),
				kgo.ConsumerGroup(group),
				kgo.TransactionalID("txn-rewind-id"),
				kgo.ConsumeResetOffset(tt.reset),
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

			if tt.precommit {
				if err := s.Begin(); err != nil {
					t.Fatal(err)
				}
				pollN(int(tt.start))
				if ok, err := s.End(ctx, kgo.TryCommit); !ok || err != nil {
					t.Fatalf("commit failed: ok=%v err=%v", ok, err)
				}
			}

			// Consume the rest, then abort while an epoch validation
			// for the cursor's position is pending.
			if err := s.Begin(); err != nil {
				t.Fatal(err)
			}
			pollN(msgs - int(tt.start))
			got, ok := s.Client().CommittedOffsets()[topic][0]
			if ok != tt.precommit || ok && got.Offset != tt.start {
				t.Fatalf("committed offset before the abort is %v (present %v), want %d (present %v)", got, ok, tt.start, tt.precommit)
			}

			ofle := failNextOFLE(c)
			fenceNextFetch(c, topic)
			waitHits(t, ofle, 1, "the epoch validation load was never issued")

			if _, err := s.End(ctx, kgo.TryAbort); err != nil {
				t.Fatalf("abort failed: %v", err)
			}

			// The abort rewound us to start; the aborted records must be
			// re-consumed from there.
			if err := s.Begin(); err != nil {
				t.Fatal(err)
			}
			re := pollN(msgs - int(tt.start))
			if _, err := s.End(ctx, kgo.TryAbort); err != nil {
				t.Fatalf("cleanup abort failed: %v", err)
			}
			if re[0].Offset != tt.start {
				t.Fatalf("BUG REPRODUCED: first record after abort rewind has offset %d, want %d", re[0].Offset, tt.start)
			}
		})
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
	consumeN(t, cl, msgs, 8*time.Second)

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
	records := consumeN(t, cl, msgs, 8*time.Second)
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
