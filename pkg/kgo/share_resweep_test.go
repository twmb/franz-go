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

	// Fire an in-flight migration. The move targets are irrelevant -- the fn
	// resolves no cursor (empty id2t) -- we only need applyMoves to spawn its
	// goroutine.
	sc.applyMoves([]shareMove{{partition: 0, leaderID: 1}}, nil)

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
