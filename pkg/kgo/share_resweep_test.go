package kgo

import (
	"net"
	"testing"
	"time"
)

// TestAuditShareLeaveWaitsForInflightMigration is the consumer_share.go
// ledger re-sweep (patterns 16-48) regression test for the unsupervised
// applyMoves goroutine.
//
// A CurrentLeader-hint cursor migration (applyMoves) runs on the metadata
// loop as a detached goroutine. Before the fix, leave's worker barrier
// (for sc.workers > 0) did not account for it: a migration racing
// LeaveGroup/Close could relocate a cursor -- or create a brand-new source --
// AFTER the per-source closeShareSession drained, stranding that cursor's
// pending acks. sc.pendingAcks then never returns to 0 (FlushAcks hangs) and
// the held records release only via the broker's acquisition-lock timeout.
// The fix registers the migration via sc.incWorker/decWorker -- the share
// consumer's OWN worker count (share groups do not use consumer sessions) --
// so leave's barrier waits for an in-flight migration before draining.
//
// The end-to-end strand is a 3-way shutdown race (the migration must
// interleave its remove/add between the two closeShareSession drains) and is
// not deterministically reproducible without internal hooks, so this asserts
// the fix's mechanism directly: it parks the metadata loop, fires an
// in-flight applyMoves, and verifies the migration shows up in sc.workers
// (which is exactly what leave's barrier waits on). Pre-fix the goroutine
// never touches sc.workers and the registration poll times out.
func TestAuditShareLeaveWaitsForInflightMigration(t *testing.T) {
	t.Parallel()

	// A guaranteed-refused seed: the metadata loop stays alive (cycling fast
	// dial failures) without a real broker, and no assignment ever resolves,
	// so manage and the source loops never start -- sc.workers has a stable 0
	// baseline and the only worker that can appear is the migration itself.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddr := ln.Addr().String()
	ln.Close()

	cl, err := NewClient(
		SeedBrokers(deadAddr),
		ShareGroup("audit-share-resweep"),
		ConsumeTopics("audit-share-resweep-topic"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	sc := cl.consumer.s
	if sc == nil {
		t.Fatal("expected a share consumer")
	}

	// Park the metadata loop so the migration's blockingMetadataFn send
	// queues behind us, keeping the migration goroutine alive (and, with the
	// fix, registered as a worker) for the assertion window below.
	occupied := make(chan struct{})
	release := make(chan struct{})
	go cl.blockingMetadataFn(func() {
		close(occupied)
		<-release
	})
	select {
	case <-occupied:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("metadata loop never parked in our blockingMetadataFn")
	}

	sc.mu.Lock()
	baseline := sc.workers
	sc.mu.Unlock()

	// Fire an in-flight migration. The move target is irrelevant -- the fn
	// resolves no cursor (empty id2t) -- we only need applyMoves to spawn its
	// goroutine. It does carry a cursor: applyMoves marks each move's cursor
	// unusable before spawning (the collapse), and applyMovesBlocking
	// re-signals cursor.source on the way out, so both must be non-nil.
	mc := &shareCursor{topic: "t", topicID: [16]byte{9}, partition: 0}
	mc.source.Store(cl.newSource(1))
	sc.applyMoves([]shareMove{{topicID: mc.topicID, partition: 0, leaderID: 1, cursor: mc}}, nil)

	// With the fix the goroutine incWorkers, then blocks on its
	// blockingMetadataFn send (queued behind us), so sc.workers stays
	// elevated. Without the fix it never registers and this times out.
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
	close(release) // unblock the metadata loop so it drains our fn + the migration

	if !registered {
		t.Fatal("in-flight applyMoves migration did not register as a share-consumer worker; " +
			"leave's barrier would not wait for it, stranding pending acks on a leave/Close race")
	}

	// The migration must eventually unregister (decWorker) once the metadata
	// loop drains it, so leave/Close completes rather than hanging.
	deadline = time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		sc.mu.Lock()
		n := sc.workers
		sc.mu.Unlock()
		if n == baseline {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("migration worker never unregistered after the metadata loop drained it")
}

// TestAuditShareMoveCollapseMarksCursorUnusable is the consumer_share.go
// ledger re-sweep regression test for the leader-move ping-pong.
//
// The classic consumer collapses a leader move: use() marks the cursor
// unusable at request-build and the strip path never re-enables it, so the
// old source does not re-fetch the partition (getting NOT_LEADER and
// re-queuing the move) until the migration replaces the cursor. The share
// cursor had no such state, so a move racing the fetch loop re-fetched the
// stale partition repeatedly until the async applyMoves landed. This adds
// the share analog: shareCursor.moving, set by applyMoves BEFORE it spawns
// the migration (so the very next createShareReq on the fetch goroutine
// already skips it), and cleared by applyMovesBlocking once the move has run
// (with a maybeShareConsume re-signal so the now-usable cursor is fetched).
//
// This drives the real createShareReq/applyMoves/applyMovesBlocking and
// asserts the full lifecycle: a non-moving assigned cursor is in the fetch
// request, applyMoves marks it moving (synchronously, before spawning) so
// createShareReq then skips it, and once the migration runs the cursor is
// re-enabled and back in the request.
func TestAuditShareMoveCollapseMarksCursorUnusable(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	deadAddr := ln.Addr().String()
	ln.Close()

	cl, err := NewClient(
		SeedBrokers(deadAddr),
		ShareGroup("audit-share-collapse"),
		ConsumeTopics("audit-share-collapse-topic"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	sc := cl.consumer.s
	if sc == nil {
		t.Fatal("expected a share consumer")
	}

	// A standalone source holding one assigned cursor. Append directly
	// (not addShareCursor) so we do not spawn a fetch loop that would race
	// our direct createShareReq calls.
	s0 := cl.newSource(0)
	c := &shareCursor{topic: "t", topicID: [16]byte{1, 2, 3}, partition: 0}
	c.assigned.Store(true)
	c.source.Store(s0)
	s0.share.mu.Lock()
	c.cursorsIdx = 0
	s0.share.cursors = []*shareCursor{c}
	s0.share.mu.Unlock()

	hasPartition := func() bool {
		req, _, _, _, _, _, _, _ := s0.createShareReq(false)
		if req == nil {
			return false
		}
		for _, rt := range req.Topics {
			if rt.TopicID != c.topicID {
				continue
			}
			for _, rp := range rt.Partitions {
				if rp.Partition == c.partition {
					return true
				}
			}
		}
		return false
	}

	// Baseline: an assigned, non-moving cursor is in the fetch request.
	if !hasPartition() {
		t.Fatal("baseline: assigned cursor should be in the share fetch request")
	}

	// Park the metadata loop so applyMovesBlocking's clear cannot run yet,
	// letting us observe the moving=true window deterministically.
	occupied := make(chan struct{})
	release := make(chan struct{})
	go cl.blockingMetadataFn(func() {
		close(occupied)
		<-release
	})
	select {
	case <-occupied:
	case <-time.After(10 * time.Second):
		close(release)
		t.Fatal("metadata loop never parked in our blockingMetadataFn")
	}

	// Queue a move. applyMoves marks the cursor moving synchronously before
	// spawning, so the next createShareReq skips it. The topic id is not in
	// id2t, so the migration itself is a no-op -- but the re-enable still
	// runs, which is what we verify after release.
	sc.applyMoves([]shareMove{{topicID: c.topicID, partition: c.partition, leaderID: 1, cursor: c}}, nil)

	if !c.moving.Load() {
		close(release)
		t.Fatal("applyMoves must mark the cursor moving before spawning the migration (else the next fetch races it)")
	}
	if hasPartition() {
		close(release)
		t.Fatal("a moving cursor must be skipped in the share fetch request (collapse), not re-fetched")
	}

	// Release the metadata loop; applyMovesBlocking runs and re-enables the
	// cursor.
	close(release)
	deadline := time.Now().Add(2 * time.Second)
	for c.moving.Load() {
		if time.Now().After(deadline) {
			t.Fatal("applyMovesBlocking never cleared moving; the cursor would be stranded unusable")
		}
		time.Sleep(time.Millisecond)
	}
	if !hasPartition() {
		t.Fatal("after the migration ran, the re-enabled cursor should be back in the fetch request")
	}
}
