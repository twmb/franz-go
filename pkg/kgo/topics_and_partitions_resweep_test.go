package kgo

import (
	"context"
	"net"
	"testing"
	"time"
)

// TestAuditShareMetadataMigrationWaitsForLeave is the regression test for
// the metadata-merge share-cursor migration vs leave race.
//
// migrateShareCursorTo relocates a share cursor between sources when a metadata
// refresh observes a leader change for a share partition (mergeTopicPartitions,
// the partitionKindShare case), and for a CurrentLeader hint, from
// applyMovesBlocking through kip951move.doMove. The share consumer's leave
// waits for every share worker to exit (sc.workers == 0) and then drains each
// source's cursors (closeShareSession) over a snapshot of the source list.
// applyMovesBlocking registers its migration as a worker so
// leave's barrier waits for it; migrateShareCursorTo did NOT -- it ran on the
// metadata loop with no incWorker -- so a metadata-driven migration racing
// leave/Close could strand a cursor's pending acks on a source that
// closeShareSession already drained (or one created after the snapshot):
// sc.pendingAcks never returns to 0 (FlushAcks hangs) and the held records
// release only via the broker's acquisition-lock timeout. This is the
// metadata-merge counterpart of TestAuditShareLeaveWaitsForInflightMigration
// (share_resweep_test.go), which covers the applyMovesBlocking path.
//
// The end-to-end strand is a shutdown race not deterministically reproducible,
// so this asserts the fix's mechanism: it parks the migration after its
// incWorker by holding sinksAndSourcesMu (the first lock the swap takes) and
// verifies the migration shows up in sc.workers -- exactly what leave's barrier
// waits on. Pre-fix the migration takes sinksAndSourcesMu as its first action
// with no incWorker, so it never touches sc.workers and the registration poll
// times out.
//
// After the migration, the test covers a source that the client forgets:
// the prune kills the old source, whose fetch loop must exit in every fetch
// concurrency mode, and leave takes the buffered fetch of a source that
// sinksAndSources does not have.
func TestAuditShareMetadataMigrationWaitsForLeave(t *testing.T) {
	t.Parallel()
	// The fetch manager lets a source fetch only during a poll in the last
	// two modes, which is where a killed source's loop waits.
	for name, opts := range map[string][]Opt{
		"default":                 nil,
		"MaxConcurrentFetches(0)": {MaxConcurrentFetches(0)},
		"ShareMaxRecordsStrict":   {ShareMaxRecords(10), ShareMaxRecordsStrict()},
	} {
		t.Run(name, func(t *testing.T) { testShareMetadataMigration(t, opts...) })
	}
}

type unbufferedHook chan bool

func (h unbufferedHook) OnFetchRecordUnbuffered(_ *Record, polled bool) { h <- polled }

func testShareMetadataMigration(t *testing.T, opts ...Opt) {
	t.Parallel()

	// A guaranteed-refused seed keeps the metadata loop alive (cycling fast
	// dial failures) without a real broker, and no assignment ever resolves,
	// so the manage and source loops never start: sc.workers has a stable
	// baseline and the only worker that can appear is the migration itself.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddr := ln.Addr().String()
	ln.Close()

	unbuffered := make(unbufferedHook, 1)
	cl, err := NewClient(append([]Opt{
		SeedBrokers(deadAddr),
		ShareGroup("audit-share-meta-migrate"),
		ConsumeTopics("audit-share-meta-migrate-topic"),
		WithHooks(unbuffered),
	}, opts...)...)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	sc := cl.consumer.s
	if sc == nil {
		t.Fatal("expected a share consumer")
	}

	// A cursor assigned to source 0; the migration target is leader 1. Append
	// directly (not addShareCursor) so we do not spawn a fetch loop that would
	// race the migration's bookkeeping.
	s0 := cl.newSource(0)
	c := &shareCursor{topic: "t", topicID: [16]byte{1, 2, 3}, partition: 0}
	c.source.Store(s0)
	s0.share.mu.Lock()
	c.cursorsIdx = 0
	s0.share.cursors = []*shareCursor{c}
	s0.share.mu.Unlock()

	cl.sinksAndSourcesMu.Lock()
	newSrc := cl.newSource(1)
	cl.sinksAndSources[0] = sinkAndSource{sink: cl.newSink(0), source: s0}
	cl.sinksAndSources[1] = sinkAndSource{sink: cl.newSink(1), source: newSrc}
	cl.sinksAndSourcesMu.Unlock()

	tp := &topicPartition{shareCursor: c}
	newtp := &topicPartition{topicPartitionData: topicPartitionData{leader: 1, leaderEpoch: 5}}

	sc.mu.Lock()
	baseline := sc.workers
	sc.mu.Unlock()

	// Park the migration: hold sinksAndSourcesMu, the first lock the swap
	// takes. Post-fix the goroutine incWorkers, then blocks here, so sc.workers
	// stays elevated. Pre-fix it blocks here before any incWorker.
	cl.sinksAndSourcesMu.Lock()
	done := make(chan struct{})
	go func() {
		tp.migrateShareCursorTo(cl, newtp)
		close(done)
	}()

	registered := false
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		sc.mu.Lock()
		n := sc.workers
		sc.mu.Unlock()
		if n > baseline {
			registered = true
			break
		}
		time.Sleep(time.Millisecond)
	}
	cl.sinksAndSourcesMu.Unlock() // let the swap complete

	if !registered {
		t.Fatal("in-flight metadata-merge share-cursor migration did not register as a share-consumer worker; " +
			"leave's barrier would not wait for it, stranding pending acks on a leave/Close race")
	}

	<-done

	// The migration completed: the cursor moved to the new leader's source and
	// the new partition carries it. (The migration's own worker is released by
	// its deferred decWorker; addShareCursor on the new source may then start a
	// fetch loop of its own, so we do not assert an exact post-migration worker
	// count -- Close drains all of it.)
	if got := c.source.Load(); got != newSrc {
		t.Errorf("cursor was not migrated to the new leader's source")
	}
	if newtp.shareCursor != c {
		t.Errorf("new.shareCursor was not set after migration")
	}

	// The old source now has no cursor, and no metadata knows broker 0,
	// so a prune forgets the source. The source can have a share session,
	// and a fetch loop waiting on a buffered fetch that no poll has taken:
	// the prune kills the source, so the loop exits and can build no more
	// requests.
	//
	// Removing the cursor started the source's loop. With a fetch limit
	// of 0, the fetch manager lets a source fetch only during a poll, so
	// that loop is waiting for its turn, and the kill must end it there.
	// Otherwise the loop finds nothing and exits, and we give the source
	// a buffered fetch and a loop waiting on it.
	strict := len(opts) > 0
	exited := func() bool {
		for deadline := time.Now().Add(2 * time.Second); time.Now().Before(deadline); time.Sleep(time.Millisecond) {
			sc.mu.Lock()
			n := sc.workers
			sc.mu.Unlock()
			// With a fetch limit of 0, the new source's loop is
			// waiting for its turn as well, so the worker count
			// stays up; the old source's loop state is the test.
			if (strict || n == baseline) && s0.fetchState.state.Load() == stateUnstarted {
				return true
			}
		}
		return false
	}
	if !strict && !exited() {
		t.Fatal("the old source's fetch loop did not exit after its cursor moved")
	}
	s0.share.mu.Lock()
	s0.share.sessionEpoch = 1
	s0.share.sessionParts[tidp{c.topicID, c.partition}] = struct{}{}
	s0.share.mu.Unlock()
	parked := make(chan bool, 1)
	if !strict {
		fetch := Fetch{Topics: []FetchTopic{{Topic: "t", Partitions: []FetchPartition{{Records: []*Record{{}}}}}}}
		s0.hookBuffered(&fetch)
		s0.share.buffered = shareBufferedFetch{fetch: fetch, doneFetch: parked}
		s0.sem = make(chan struct{})
		sc.c.addSourceReadyForDraining(s0)
		s0.maybeShareConsume()
	}

	cl.pruneSinksAndSources()
	cl.sinksAndSourcesMu.Lock()
	_, exists := cl.sinksAndSources[0]
	_, kept := cl.sinksAndSources[1]
	cl.sinksAndSourcesMu.Unlock()
	if exists {
		t.Error("the old source was not pruned")
	}
	if !kept {
		t.Error("the new source, which has the cursor, was pruned")
	}
	if !strict {
		select {
		case <-parked:
		default:
			t.Error("the prune did not take the old source's buffered fetch")
		}
		select {
		case polled := <-unbuffered:
			if polled {
				t.Error("the unbuffered hook ran with polled true for a dropped record")
			}
		case <-time.After(2 * time.Second):
			t.Error("the unbuffered hook did not run for the dropped record before any poll")
		}
	}
	if !exited() {
		t.Errorf("the old source's fetch loop did not exit after the prune (old loop state %d, new loop state %d)", s0.fetchState.state.Load(), newSrc.fetchState.state.Load())
	}
	// A request would have failed and reset the session, so the epoch
	// shows that the loop sent none.
	s0.share.mu.Lock()
	epoch := s0.share.sessionEpoch
	s0.share.mu.Unlock()
	if epoch != 1 {
		t.Errorf("the pruned source's session epoch is %d, want 1: its loop built a request", epoch)
	}
	if req, _, _, _, _, _, _, _ := s0.createShareReq(false); req != nil {
		t.Errorf("the pruned source built a request at session epoch %d", req.ShareSessionEpoch)
	}
	// The broker returns under the same ID: the pruned source must not
	// find it.
	cl.brokersMu.Lock()
	cl.brokers = append(cl.brokers, cl.newBroker(0, "127.0.0.1", 1, nil))
	cl.brokersMu.Unlock()
	if _, err := s0.shareBroker(nil); err == nil {
		t.Error("the pruned source found the returned broker to send to")
	}

	// Leaving takes every buffered fetch in sourcesReadyForDraining,
	// which finishes each fetch for the fetch manager, whether or not
	// sinksAndSources has the fetch's source.
	gone := cl.newSource(2)
	doneFetch := make(chan bool, 1)
	gone.share.buffered = shareBufferedFetch{doneFetch: doneFetch}
	gone.sem = make(chan struct{})
	sc.c.addSourceReadyForDraining(gone)
	sc.leave(context.Background())
	select {
	case <-doneFetch:
	default:
		t.Error("leave did not take the buffered fetch of a source that sinksAndSources does not have")
	}
}
