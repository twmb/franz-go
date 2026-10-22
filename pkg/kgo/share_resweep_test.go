package kgo

import (
	"errors"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestAuditShareLeaveWaitsForInflightMigration is the regression test for
// the unsupervised applyMoves goroutine.
//
// A CurrentLeader-hint cursor migration (applyMoves) runs on the metadata
// loop as a detached goroutine. Before the fix, leave's worker barrier
// (for sc.workers > 0) did not account for it: a migration racing
// LeaveGroup/Close could relocate a cursor -- or create a brand-new source --
// after the per-source closeShareSession drained, stranding that cursor's
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

// TestAuditShareMoveCollapseMarksCursorUnusable is the regression test for
// the leader-move ping-pong.
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

// No topics are subscribed and the cursor stays unassigned: this exercises
// routing on the real metadata loop without starting group traffic.
func newShareRoutingClient(t testing.TB) (*Client, *topicPartitions, *metadataTopic) {
	t.Helper()
	cl, err := NewClient(SeedBrokers("127.0.0.1:1"), ShareGroup("share-routing"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	id := [16]byte{1}
	var tp topicPartitions
	var mt *metadataTopic
	cl.blockingMetadataFn(func() {
		s := cl.newSource(2)
		cl.sinksAndSourcesMu.Lock()
		cl.sinksAndSources[2] = sinkAndSource{sink: cl.newSink(2), source: s}
		cl.sinksAndSourcesMu.Unlock()
		k := kip951move{brokers: []BrokerMetadata{{NodeID: 2, Host: "127.0.0.1", Port: 1}, {NodeID: 3, Host: "127.0.0.1", Port: 1}}}
		k.ensureBrokers(cl)
		mt = &metadataTopic{topic: "t", id: id, partitions: []metadataPartition{{topic: "t", topicID: id, leader: 2, leaderEpoch: 10, sns: sinkAndSource{source: s}}}}
		tp.v.Store(mt.newPartitions(cl, partitionKindShare))
		c := tp.load().partitions[0].shareCursor
		c.cursorsIdx = 0
		s.share.cursors = []*shareCursor{c}
		cl.consumer.s.tps.storeData(topicsPartitionsData{"t": &tp})
		cl.id2t.Store(map[[16]byte]string{id: "t"})
	})
	return cl, &tp, mt
}

func TestShareFetchDefaultLeaderErrors(t *testing.T) {
	t.Parallel()
	for _, partErr := range []*kerr.Error{kerr.KafkaStorageError, kerr.TopicAuthorizationFailed, kerr.UnknownTopicID, kerr.InvalidRequest} {
		t.Run(partErr.Message, func(t *testing.T) {
			t.Parallel()
			_, tp, _ := newShareRoutingClient(t)
			c := tp.load().partitions[0].shareCursor
			s := c.source.Load()
			req := kmsg.NewPtrShareFetchRequest()
			req.SetVersion(1)
			resp := kmsg.NewPtrShareFetchResponse()
			resp.SetVersion(1)
			resp.AcquisitionLockTimeoutMillis = 30000
			rp := kmsg.NewShareFetchResponseTopicPartition()
			rp.ErrorCode = partErr.Code // CurrentLeader deliberately remains 0/0.
			resp.Topics = []kmsg.ShareFetchResponseTopic{{TopicID: c.topicID, Partitions: []kmsg.ShareFetchResponseTopicPartition{rp}}}
			decoded := kmsg.NewPtrShareFetchResponse()
			decoded.SetVersion(1)
			if err := decoded.ReadFrom(resp.AppendTo(nil)); err != nil {
				t.Fatal(err)
			}
			res := s.handleShareReqResp(req, decoded, []*shareCursor{c}, nil, nil)
			if len(res.moves) != 0 {
				t.Fatalf("ordinary error scheduled leader moves: %+v", res.moves)
			}
			if !kerr.IsRetriable(partErr) {
				if len(res.fetch.Topics) != 1 || len(res.fetch.Topics[0].Partitions) != 1 || !errors.Is(res.fetch.Topics[0].Partitions[0].Err, partErr) {
					t.Fatalf("original error lost: %+v", res.fetch)
				}
			} else if !res.allErrsStripped {
				t.Fatal("retryable error did not take the backoff path")
			}
			if c.source.Load().nodeID != 2 {
				t.Fatal("cursor left its leader")
			}
		})
	}
}

func TestShareLeaderHintRouting(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name          string
		target, epoch int32
		endpoints     []BrokerMetadata
		staleCursor   bool
		wantMove      bool
	}{
		{name: "known target", target: 3, epoch: 11, wantMove: true},
		{name: "same leader newer epoch", target: 2, epoch: 11, wantMove: true},
		{name: "old epoch", target: 3, epoch: 9},
		{name: "old epoch with endpoint", target: 3, epoch: 9, endpoints: []BrokerMetadata{{NodeID: 3, Host: "127.0.0.1", Port: 2}}},
		{name: "old unknown target with endpoint", target: 0, epoch: 9, endpoints: []BrokerMetadata{{NodeID: 0, Host: "127.0.0.1", Port: 1}}},
		{name: "equal epoch conflicting leader", target: 3, epoch: 10},
		{name: "duplicate", target: 2, epoch: 10},
		{name: "duplicate with endpoint", target: 2, epoch: 10, endpoints: []BrokerMetadata{{NodeID: 2, Host: "127.0.0.1", Port: 2}}},
		{name: "conflicting epoch with endpoint", target: 3, epoch: 10, endpoints: []BrokerMetadata{{NodeID: 3, Host: "127.0.0.1", Port: 2}}},
		{name: "unknown target", target: 0, epoch: 11},
		{name: "invalid endpoint", target: 0, epoch: 11, endpoints: []BrokerMetadata{{NodeID: 0, Port: 9092}}},
		{name: "legal broker zero", target: 0, epoch: 11, endpoints: []BrokerMetadata{{NodeID: 0, Host: "127.0.0.1", Port: 1}}, wantMove: true},
		{name: "new broker", target: 4, epoch: 11, endpoints: []BrokerMetadata{{NodeID: 4, Host: "127.0.0.1", Port: 1}}, wantMove: true},
		{name: "purged cursor", target: 3, epoch: 11, staleCursor: true},
		{name: "purged cursor with endpoint", target: 3, epoch: 11, staleCursor: true, endpoints: []BrokerMetadata{{NodeID: 3, Host: "127.0.0.1", Port: 2}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			cl, tp, mt := newShareRoutingClient(t)
			cl.brokersMu.RLock()
			beforeBrokers := make(map[int32]*broker, len(cl.brokers))
			for _, br := range cl.brokers {
				beforeBrokers[br.meta.NodeID] = br
			}
			cl.brokersMu.RUnlock()
			original := tp.load()
			cursor := original.partitions[0].shareCursor
			moveCursor := cursor
			if tc.staleCursor {
				moveCursor = &shareCursor{topic: "t", topicID: mt.id}
				moveCursor.source.Store(cursor.source.Load())
			}
			moveCursor.moving.Store(true)
			cl.consumer.s.applyMovesBlocking([]shareMove{{topicID: mt.id, partition: 0, leaderID: tc.target, leaderEpoch: tc.epoch, cursor: moveCursor}}, tc.endpoints)
			got := tp.load().partitions[0]
			wantLeader, wantEpoch := int32(2), int32(10)
			if tc.wantMove {
				wantLeader, wantEpoch = tc.target, tc.epoch
			}
			if got.leader != wantLeader || got.leaderEpoch != wantEpoch || got.shareCursor.source.Load().nodeID != wantLeader {
				t.Fatalf("metadata=%d/%d source=%d, want %d/%d", got.leader, got.leaderEpoch, got.shareCursor.source.Load().nodeID, wantLeader, wantEpoch)
			}
			if moveCursor.moving.Load() {
				t.Fatal("rejected or applied move left cursor disabled")
			}
			if original.partitions[0].leader != 2 || original.writablePartitions[0].leaderEpoch != 10 {
				t.Fatal("move mutated an existing metadata snapshot")
			}
			if got != tp.load().writablePartitions[0] {
				t.Fatal("writable partition snapshot diverged")
			}
			if !tc.wantMove {
				cl.brokersMu.RLock()
				unchanged := len(cl.brokers) == len(beforeBrokers)
				for _, br := range cl.brokers {
					unchanged = unchanged && beforeBrokers[br.meta.NodeID] == br
				}
				cl.brokersMu.RUnlock()
				if !unchanged {
					t.Fatal("rejected hint registered or replaced a broker")
				}
			}
			if !tc.wantMove && tc.target == 0 {
				cl.sinksAndSourcesMu.Lock()
				_, exists := cl.sinksAndSources[0]
				cl.sinksAndSourcesMu.Unlock()
				if exists {
					t.Fatal("unknown target acquired a source")
				}
			}
			// A delayed response cannot roll back a newer accepted hint.
			if tc.wantMove {
				cl.consumer.s.applyMovesBlocking([]shareMove{{topicID: mt.id, partition: 0, leaderID: 2, leaderEpoch: 10, cursor: cursor}}, nil)
				cl.blockingMetadataFn(func() {
					var why multiUpdateWhy
					cl.mergeTopicPartitions("t", tp, mt, partitionKindShare, nil, &why)
				})
				if got := tp.load().partitions[0]; got.leader != wantLeader || got.leaderEpoch != wantEpoch || got.shareCursor.source.Load().nodeID != wantLeader {
					t.Fatal("stale response or metadata rolled routing backwards")
				}
			}
		})
	}
}

func TestShareIdenticalMetadataRepairsSource(t *testing.T) {
	t.Parallel()
	cl, tp, mt := newShareRoutingClient(t)
	cl.blockingMetadataFn(func() {
		cursor := tp.load().partitions[0].shareCursor
		oldSource := cursor.source.Load()
		bad := cl.newSource(0) // Reproduce the state left by the old hint path.
		oldSource.removeShareCursor(cursor)
		cursor.source.Store(bad)
		bad.addShareCursor(cursor)
		var why multiUpdateWhy
		cl.mergeTopicPartitions("t", tp, mt, partitionKindShare, nil, &why)
		got := tp.load().partitions[0]
		if got.shareCursor != cursor || cursor.source.Load() != oldSource {
			t.Error("identical metadata failed to repair the existing cursor")
		}
		if got.leader != 2 || got.leaderEpoch != 10 {
			t.Error("repair changed authoritative metadata")
		}
	})
}

// A batch can update several partitions of one topic, with later hints
// superseding earlier ones. Only the final targets may install endpoints.
func TestShareLeaderHintBatch(t *testing.T) {
	t.Parallel()
	cl, tp, mt := newShareRoutingClient(t)
	cl.blockingMetadataFn(func() {
		src := tp.load().partitions[0].shareCursor.source.Load()
		next := *tp.load()
		next.partitions = append([]*topicPartition(nil), next.partitions...)
		for i := int32(1); i < 3; i++ {
			p := metadataPartition{topic: "t", topicID: mt.id, partition: i, leader: 2, leaderEpoch: 10, sns: sinkAndSource{source: src}}.newPartition(cl, partitionKindShare)
			p.shareCursor.cursorsIdx = len(src.share.cursors)
			src.share.cursors = append(src.share.cursors, p.shareCursor)
			next.partitions = append(next.partitions, p)
		}
		next.writablePartitions = []*topicPartition{next.partitions[0], next.partitions[2]}
		tp.v.Store(&next)
	})
	original := tp.load()
	broker2, _ := cl.brokerOrErr(nil, 2, errUnknownBroker)
	broker3, _ := cl.brokerOrErr(nil, 3, errUnknownBroker)
	c0, c2 := original.partitions[0].shareCursor, original.partitions[2].shareCursor
	c0.moving.Store(true)
	c2.moving.Store(true)
	cl.consumer.s.applyMovesBlocking([]shareMove{
		{topicID: mt.id, partition: 2, leaderID: 3, leaderEpoch: 11, cursor: c2},
		{topicID: mt.id, partition: 0, leaderID: 0, leaderEpoch: 11, cursor: c0},
		{topicID: mt.id, partition: 2, leaderID: 0, leaderEpoch: 12, cursor: c2},
		{topicID: mt.id, partition: 2, leaderID: 3, leaderEpoch: 11, cursor: c2},
	}, []BrokerMetadata{
		{NodeID: 0, Host: "127.0.0.1", Port: 1},
		{NodeID: 2, Host: "127.0.0.1", Port: 2},
		{NodeID: 3, Host: "127.0.0.1", Port: 2},
	})
	got := tp.load()
	for i, want := range []topicPartitionData{{leader: 0, leaderEpoch: 11}, {leader: 2, leaderEpoch: 10}, {leader: 0, leaderEpoch: 12}} {
		p := got.partitions[i]
		if p.topicPartitionData != want || p.shareCursor.source.Load().nodeID != want.leader {
			t.Fatalf("partition %d: metadata=%+v source=%d, want %+v", i, p.topicPartitionData, p.shareCursor.source.Load().nodeID, want)
		}
		if original.partitions[i].leader != 2 || original.partitions[i].leaderEpoch != 10 {
			t.Fatal("batch modified the old snapshot")
		}
		if p.shareCursor.moving.Load() {
			t.Fatal("batch left a cursor disabled")
		}
	}
	if got.partitions[1] != original.partitions[1] {
		t.Fatal("batch replaced an untouched partition")
	}
	if len(got.writablePartitions) != 2 || got.writablePartitions[0] != got.partitions[0] || got.writablePartitions[1] != got.partitions[2] {
		t.Fatal("batch lost writable partition membership")
	}
	for _, before := range []*broker{broker2, broker3} {
		after, _ := cl.brokerOrErr(nil, before.meta.NodeID, errUnknownBroker)
		if after != before {
			t.Fatalf("batch replaced unrelated or superseded broker %d", before.meta.NodeID)
		}
	}
	cl.sinksAndSourcesMu.Lock()
	_, created := cl.sinksAndSources[3]
	cl.sinksAndSourcesMu.Unlock()
	if created {
		t.Fatal("superseded target acquired a source")
	}
}

func BenchmarkShareLeaderHintBatch(b *testing.B) {
	for _, n := range []int{100, 1000, 4000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			cl, tp, mt := newShareRoutingClient(b)
			moves := make([]shareMove, n)
			cl.blockingMetadataFn(func() {
				src := tp.load().partitions[0].shareCursor.source.Load()
				// Keep fetch loops parked so measurements isolate metadata migration,
				// rather than scheduling unassigned cursors on background source loops.
				src.fetchState.state.Store(stateWorking)
				dst := cl.newSource(3)
				dst.fetchState.state.Store(stateWorking)
				cl.sinksAndSourcesMu.Lock()
				cl.sinksAndSources[3] = sinkAndSource{sink: cl.newSink(3), source: dst}
				cl.sinksAndSourcesMu.Unlock()
				next := *tp.load()
				next.partitions = make([]*topicPartition, n)
				next.writablePartitions = make([]*topicPartition, n)
				src.share.cursors = make([]*shareCursor, n)
				for i := range n {
					c := &shareCursor{topic: "t", topicID: mt.id, partition: int32(i), cursorsIdx: i}
					c.source.Store(src)
					src.share.cursors[i] = c
					p := &topicPartition{topicPartitionData: topicPartitionData{leader: 2, leaderEpoch: 10}, shareCursor: c}
					next.partitions[i], next.writablePartitions[i] = p, p
					moves[i] = shareMove{topicID: mt.id, partition: int32(i), leaderID: 3, cursor: c}
				}
				tp.v.Store(&next)
			})
			epoch := int32(10)
			b.ReportAllocs()
			for b.Loop() {
				epoch++
				for i := range moves {
					moves[i].leaderEpoch = epoch
					moves[i].cursor.moving.Store(true)
				}
				cl.consumer.s.applyMovesBlocking(moves, nil)
			}
		})
	}
}
