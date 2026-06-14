package kgo

import (
	"net"
	"testing"
	"time"
)

// TestAuditShareMetadataMigrationWaitsForLeave is the topics_and_partitions.go
// ledger re-sweep regression test for the metadata-merge share-cursor
// migration vs leave race.
//
// migrateShareCursorTo relocates a share cursor between sources when a metadata
// refresh observes a leader change for a share partition (mergeTopicPartitions,
// the partitionKindShare case). It does the same removeShareCursor /
// addShareCursor as the CurrentLeader-hint sibling applyMovesBlocking. The
// share consumer's leave waits for every share worker to exit (sc.workers == 0)
// and then drains each source's cursors (closeShareSession) over a snapshot of
// the source list. applyMovesBlocking registers its migration as a worker so
// leave's barrier waits for it; migrateShareCursorTo did NOT -- it ran on the
// metadata loop with no incWorker -- so a metadata-driven migration racing
// leave/Close could strand a cursor's pending acks on a source that
// closeShareSession already drained (or one created after the snapshot):
// sc.pendingAcks never returns to 0 (FlushAcks hangs) and the held records
// release only via the broker's acquisition-lock timeout. This is the
// metadata-merge sibling of TestAuditShareLeaveWaitsForInflightMigration
// (share_resweep_test.go), which covers the applyMovesBlocking path.
//
// The end-to-end strand is a shutdown race not deterministically reproducible,
// so this asserts the fix's mechanism: it parks the migration after its
// incWorker by holding sinksAndSourcesMu (the first lock the swap takes) and
// verifies the migration shows up in sc.workers -- exactly what leave's barrier
// waits on. Pre-fix the migration takes sinksAndSourcesMu as its first action
// with no incWorker, so it never touches sc.workers and the registration poll
// times out.
func TestAuditShareMetadataMigrationWaitsForLeave(t *testing.T) {
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

	cl, err := NewClient(
		SeedBrokers(deadAddr),
		ShareGroup("audit-share-meta-migrate"),
		ConsumeTopics("audit-share-meta-migrate-topic"),
	)
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
}
