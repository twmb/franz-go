package kfake

import (
	"context"
	"strings"
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
		// load is the request key of partition 0's pending offset load,
		// or OffsetFetch for the group's own offset fetch, which the test
		// holds at the broker rather than failing.
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
			// the start offset, and the group tracks nothing for the
			// partition until the seek.
			return newConsumer(t, c)
		}},

		{"during the offset fetch", false, kmsg.OffsetFetch, func(t *testing.T, c *Cluster) *kgo.Client {
			// The group's offset fetch is pending: the partition has
			// no cursor and no load, and the seek is kept for the
			// fetch to start the partition at.
			return newConsumer(t, c)
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
			produceN(t, c, topic, msgs)

			// Fail every load retryably until the seek is in, so the
			// load is still pending when SetOffsets runs. The group's
			// offset fetch is held at the broker instead.
			var (
				cl      *kgo.Client
				release func()
			)
			if tt.load == kmsg.OffsetFetch {
				var held <-chan struct{}
				held, release = hold(c, kmsg.OffsetFetch, 0)
				cl = tt.pend(t, c)
				select {
				case <-held:
				case <-time.After(15 * time.Second):
					t.Fatal("the offset fetch was never issued")
				}
			} else {
				load := c.Fault(Fault{
					Keys:  []kmsg.Key{tt.load},
					Err:   kerr.NotLeaderForPartition,
					Count: -1,
				})
				cl = tt.pend(t, c)
				waitHits(t, load, 1, "the offset load was never issued")
				release = load.Remove
			}

			cl.SetOffsets(map[string]map[int32]kgo.EpochOffset{
				topic: {0: {Epoch: -1, Offset: seek}},
			})
			release()
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

// commitOffset commits offset at leader epoch 0 for partition 0 of topic in
// the group through kadm, as a prior member would have.
func commitOffset(t *testing.T, c *Cluster, group, topic string, offset int64) {
	t.Helper()
	var os kadm.Offsets
	os.AddOffset(topic, 0, offset, 0)
	if err := kadm.NewClient(newPlainClient(t, c)).CommitAllOffsets(context.Background(), group, os); err != nil {
		t.Fatal(err)
	}
}

// TestAuditUncommitOrigins checks every way the group starts tracking a
// partition's offsets against every reader of those offsets. The origins:
// nothing tracked, a fetched commit, a fetched commit AdjustFetchOffsetsFn
// adjusted, the first poll, a SetOffsets before the first poll, and a mark
// before the first poll. The readers: UncommittedOffsets, MarkedOffsets,
// CommittedOffsets, then the group's offset at the broker after
// CommitMarkedOffsets and after CommitUncommittedOffsets. Pre-fix,
// CommittedOffsets reported an absolute ConsumeResetOffset (AfterMilli's
// millisecond included) before the first poll and 0 after the first poll,
// a mark before the first poll recorded the partition with dirty and committed
// at 0, and an exact adjusted offset was recorded as the group's commit.
// The pending-load table test above polls every partition, and the abort
// table test below polls before the rewind, so neither fit.
func TestAuditUncommitOrigins(t *testing.T) {
	t.Parallel()

	const (
		topic = "uncommit-origins"
		group = "uncommit-origins-g"
		msgs  = 10
		none  = int64(-1) // the reader omits the partition, or the group has no commit
	)

	start := kgo.NewOffset().AtStart()
	// adjust returns an AdjustFetchOffsetsFn applying fn to every fetched offset.
	adjust := func(fn func(kgo.Offset) kgo.Offset) func(context.Context, map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
		return func(_ context.Context, fetched map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
			for _, ps := range fetched {
				for p, o := range ps {
					ps[p] = fn(o)
				}
			}
			return fetched, nil
		}
	}
	for _, tt := range []struct {
		name  string
		reset kgo.Offset
		// load is the request key the partition first needs records
		// through. The test fails the request retryably so that the
		// partition is never polled; 0 polls every record instead.
		load     kmsg.Key
		before   func(*testing.T, *Cluster) // before the consumer starts
		assigned func(*kgo.Client)          // in OnPartitionsAssigned, before the offset fetch
		adjust   func(context.Context, map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error)
		after    func(*testing.T, *kgo.Client) // once the offset fetch is done

		uncommitted, marked, committed int64 // before any commit
		afterMarked, afterUncommitted  int64 // the broker's offset after each commit, in that order
	}{
		{"nothing tracked", kgo.NewOffset().AfterMilli(time.Now().UnixMilli()), kmsg.ListOffsets, nil, nil, nil, nil,
			none, none, none, none, none},

		{"fetched commit", start, kmsg.OffsetForLeaderEpoch, func(t *testing.T, c *Cluster) {
			commitOffset(t, c, group, topic, 3)
		}, nil, nil, nil, none, none, 3, 3, 3},

		{"fetched commit, adjusted relative", start, 0, func(t *testing.T, c *Cluster) {
			commitOffset(t, c, group, topic, 3)
		}, nil, adjust(func(o kgo.Offset) kgo.Offset { return o.Relative(-2) }), func(t *testing.T, cl *kgo.Client) {
			consumeN(t, cl, msgs-1, 8*time.Second) // from 1
		}, msgs, none, none, 3, msgs},

		{"a partition the fn added", start, kmsg.OffsetForLeaderEpoch, func(t *testing.T, c *Cluster) {
			commitOffset(t, c, group, topic, 3)
		}, nil, func(_ context.Context, fetched map[string]map[int32]kgo.Offset) (map[string]map[int32]kgo.Offset, error) {
			fetched[topic][1] = kgo.NewOffset().At(7)
			return fetched, nil
		}, func(t *testing.T, cl *kgo.Client) {
			if o, ok := cl.UncommittedOffsets()[topic][1]; ok {
				t.Errorf("BUG REPRODUCED: a partition the fn added reads as a seek to %d", o.Offset)
			}
		}, none, none, 3, 3, 3},

		{"fetched commit, adjusted to the start", start, 0, func(t *testing.T, c *Cluster) {
			commitOffset(t, c, group, topic, 3)
		}, nil, adjust(func(o kgo.Offset) kgo.Offset { return o.AtStart() }), func(t *testing.T, cl *kgo.Client) {
			consumeN(t, cl, msgs, 8*time.Second)
		}, msgs, none, none, 3, msgs},

		{"fetched commit, adjusted AfterMilli", start, 0, func(t *testing.T, c *Cluster) {
			commitOffset(t, c, group, topic, 3)
		}, nil, adjust(func(o kgo.Offset) kgo.Offset { return o.AfterMilli(3) }), func(t *testing.T, cl *kgo.Client) {
			consumeN(t, cl, msgs, 8*time.Second)
		}, msgs, none, none, 3, msgs},

		{"fetched commit, adjusted epoch", start, kmsg.Fetch, func(t *testing.T, c *Cluster) {
			commitOffset(t, c, group, topic, 3)
		}, nil, adjust(func(o kgo.Offset) kgo.Offset { return o.WithEpoch(-1) }), nil,
			none, none, 3, 3, 3},

		{"fetched commit, adjusted exact", start, 0, func(t *testing.T, c *Cluster) {
			commitOffset(t, c, group, topic, 3)
		}, nil, adjust(func(o kgo.Offset) kgo.Offset { return o.At(5) }), func(t *testing.T, cl *kgo.Client) {
			consumeN(t, cl, msgs-5, 8*time.Second) // from 5
		}, msgs, none, none, 3, msgs},

		{"first poll", start, 0, nil, nil, nil, func(t *testing.T, cl *kgo.Client) {
			consumeN(t, cl, msgs, 8*time.Second)
		}, msgs, none, none, none, msgs},

		{"first poll, then a mark", start, 0, nil, nil, nil, func(t *testing.T, cl *kgo.Client) {
			consumeN(t, cl, msgs, 8*time.Second)
			cl.MarkCommitOffsets(map[string]map[int32]kgo.EpochOffset{topic: {0: {Epoch: -1, Offset: 4}}})
		}, msgs, 4, none, 4, msgs},

		{"SetOffsets before the first poll", start, kmsg.ListOffsets, nil, nil, nil, func(t *testing.T, cl *kgo.Client) {
			cl.SetOffsets(map[string]map[int32]kgo.EpochOffset{topic: {0: {Epoch: -1, Offset: 5}}})
		}, 5, 5, none, 5, 5},

		{"SetOffsets in OnPartitionsAssigned", start, 0, nil, func(cl *kgo.Client) {
			cl.SetOffsets(map[string]map[int32]kgo.EpochOffset{topic: {0: {Epoch: -1, Offset: 7}}})
		}, nil, func(t *testing.T, cl *kgo.Client) {
			consumeN(t, cl, msgs-7, 8*time.Second) // from 7, the seek, not the start
		}, msgs, 7, none, 7, msgs},

		{"SetOffsets in OnPartitionsAssigned, over a commit", start, 0, func(t *testing.T, c *Cluster) {
			commitOffset(t, c, group, topic, 3)
		}, func(cl *kgo.Client) {
			cl.SetOffsets(map[string]map[int32]kgo.EpochOffset{topic: {0: {Epoch: -1, Offset: 7}}})
		}, nil, func(t *testing.T, cl *kgo.Client) {
			consumeN(t, cl, msgs-7, 8*time.Second) // from 7, the seek, not the commit
		}, msgs, 7, 3, 7, msgs},

		{"a mark before the first poll", start, kmsg.ListOffsets, nil, nil, nil, func(t *testing.T, cl *kgo.Client) {
			// The partition is not being consumed yet: the mark is dropped.
			cl.MarkCommitOffsets(map[string]map[int32]kgo.EpochOffset{topic: {0: {Epoch: -1, Offset: 7}}})
		}, none, none, none, none, none},

		{"a mark, then SetOffsets, before the first poll", start, kmsg.ListOffsets, nil, nil, nil, func(t *testing.T, cl *kgo.Client) {
			seek := map[string]map[int32]kgo.EpochOffset{topic: {0: {Epoch: -1, Offset: 7}}}
			cl.MarkCommitOffsets(seek)
			cl.SetOffsets(seek)
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
			opts := []kgo.Opt{
				kgo.ConsumeTopics(topic),
				kgo.ConsumerGroup(group),
				kgo.ConsumeResetOffset(tt.reset),
				kgo.AdjustFetchOffsetsFn(tt.adjust),
				kgo.AutoCommitMarks(),
				kgo.AutoCommitInterval(time.Hour), // only the manual commits below
				kgo.DisableFetchSessions(),
				kgo.FetchMaxWait(100 * time.Millisecond),
			}
			if tt.assigned != nil {
				opts = append(opts, kgo.OnPartitionsAssigned(func(_ context.Context, cl *kgo.Client, _ map[string][]int32) { tt.assigned(cl) }))
			}
			cl := newPlainClient(t, c, opts...)
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

			// broker returns the group's offset for the partition at the broker, or none.
			broker := func() int64 {
				if gc, ok := groupCommits(c, group)[topic][0]; ok {
					return gc.Offset
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
// offset 0 as committed, and the abort rewound to 0. The "commit changed by
// the fn" row verifies that End records what the TxnOffsetCommit carried
// after a PreTxnCommitFnContext fn changed it, so the abort rewinds to the
// offset the broker holds.
func TestAuditTxnAbortRewindNotClobberedByPendingLoad(t *testing.T) {
	t.Parallel()

	const (
		topic = "txn-rewind-clobber"
		group = "txn-rewind-g"
		msgs  = 10
	)

	const none = int64(-1)
	for _, tt := range []struct {
		name      string
		reset     kgo.Offset
		precommit int   // records consumed and committed in a first transaction; 0 for none
		fnOffset  int64 // when non-zero, the first transaction's fn rewrites its commit to this offset
		committed int64 // CommittedOffsets before the abort, or none
		from      int64 // where the aborted transaction starts reading
		rewind    int64 // where the abort rewinds to
	}{
		{"after a commit", kgo.NewOffset().AtStart(), 2, 0, 2, 2, 2},
		{"no commit", kgo.NewOffset().AtEnd().Relative(-5), 0, 0, none, 5, 5},
		{"commit changed by the fn", kgo.NewOffset().AtStart(), 2, 1, 1, 2, 1},
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

			if tt.precommit > 0 {
				if err := s.Begin(); err != nil {
					t.Fatal(err)
				}
				pollN(tt.precommit)
				endCtx := ctx
				if tt.fnOffset != 0 {
					endCtx = kgo.PreTxnCommitFnContext(ctx, func(req *kmsg.TxnOffsetCommitRequest) error {
						for i := range req.Topics {
							for j := range req.Topics[i].Partitions {
								req.Topics[i].Partitions[j].Offset = tt.fnOffset
							}
						}
						return nil
					})
				}
				if ok, err := s.End(endCtx, kgo.TryCommit); !ok || err != nil {
					t.Fatalf("commit failed: ok=%v err=%v", ok, err)
				}
			}

			// Consume the rest, then abort while an epoch validation
			// for the cursor's position is pending.
			if err := s.Begin(); err != nil {
				t.Fatal(err)
			}
			pollN(msgs - int(tt.from))
			got, ok := s.Client().CommittedOffsets()[topic][0]
			if want := tt.committed != none; ok != want || ok && got.Offset != tt.committed {
				t.Fatalf("committed offset before the abort is %v (present %v), want %d (present %v)", got, ok, tt.committed, want)
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
			re := pollN(msgs - int(tt.rewind))
			if _, err := s.End(ctx, kgo.TryAbort); err != nil {
				t.Fatalf("cleanup abort failed: %v", err)
			}
			if re[0].Offset != tt.rewind {
				t.Fatalf("BUG REPRODUCED: first record after abort rewind has offset %d, want %d", re[0].Offset, tt.rewind)
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
