package kgo

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// Regression tests from the producer.go audit sweep. These are internal
// (package kgo) tests: they orchestrate interleavings via unexported state
// and never need a live broker - every produced record fails before any
// network use, or the test asserts purely structural invariants.

// batchPromisesLen returns the current number of queued promise elements.
func batchPromisesLen(cl *Client) int {
	r := &cl.producer.batchPromises
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.l
}

// The batchPromises ring's maxLen backpressure (#1194) exists because
// records can fail faster than promises drain: every accepted record costs
// memory until its promise runs, and the promise is the only completion
// channel, so produce-entry failure pushes - blocking Produce and TryProduce
// alike - must park at the bound rather than grow the ring without bound.
// This test pins all three properties of the design:
//
//  1. The bound holds: a TryProduce failure spin-loop plateaus the ring at
//     exactly maxLen (the spinner parks); it must not push past it.
//  2. Internal pushes never park: purgeTopics/failBufferedRecords push under
//     topicsMu+unknownTopicsMu, storePartitionsUpdate under unknownTopicsMu,
//     and recBuf failure paths under recBuf.mu. The promise worker is the
//     ring's only drainer and re-enters those locks through user promises,
//     so a parked lock-holder deadlocks the client. Internal pushes are
//     bounded by the max-buffered admission instead, so forcing them does
//     not unbound the ring.
//  3. The documented produce-from-promise pattern (spawn a goroutine, as
//     AbortingFirstErrPromise does) converges even at the bound: the
//     goroutine's push parks while the worker - NOT parked itself - drains.
func TestAuditFullPromiseRingBoundAndConvergence(t *testing.T) {
	t.Parallel()

	cl, err := NewClient(
		SeedBrokers("127.0.0.1:1"), // never successfully dialed; all records fail pre-buffer
		MaxBufferedRecords(8192),   // ring maxLen floor; keeps the fill cheap
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx := context.Background()
	maxLen := cl.producer.batchPromises.maxLen

	// Element 1: parks the promise worker on a gate. The element stays in
	// the ring until the worker finishes it, so the ring never empties
	// and no second worker can spawn. Its promise produces via the
	// documented goroutine pattern.
	gate := make(chan struct{})
	viaGoroutine := make(chan struct{})
	cl.TryProduce(ctx, &Record{}, func(*Record, error) {
		<-gate
		go cl.TryProduce(ctx, &Record{}, func(*Record, error) { close(viaGoroutine) })
	})

	// A failure spin-loop: pushes one promise element per record. With the
	// worker gated, the ring fills to maxLen and the spinner parks.
	const extra = 64
	fillerDone := make(chan struct{})
	go func() {
		defer close(fillerDone)
		for i := 0; i < maxLen+extra; i++ {
			cl.TryProduce(ctx, &Record{}, nil)
		}
	}()
	deadline := time.Now().Add(10 * time.Second)
	for batchPromisesLen(cl) < maxLen {
		if time.Now().After(deadline) {
			t.Fatal("ring never filled to maxLen")
		}
		time.Sleep(time.Millisecond)
	}
	// Give the spinner time to (wrongly) push past the bound, then assert
	// the plateau: this is the #1194 OOM protection.
	time.Sleep(50 * time.Millisecond)
	if n := batchPromisesLen(cl); n > maxLen {
		t.Fatalf("ring grew to %d past maxLen %d: TryProduce failure pushes are not bounded and a failure spin-loop OOMs the client", n, maxLen)
	}

	// Probe the internal promiseBatch entry (what purge/fail paths and
	// storePartitionsUpdate use, under client locks): it must not park
	// even at the bound.
	internalDone := make(chan struct{})
	go func() {
		defer close(internalDone)
		cl.producer.promiseBatch(batchPromise{
			recs:      []promisedRec{{ctx, noPromise, &Record{}}},
			beforeBuf: true,
			err:       errors.New("probe"),
		})
	}()
	select {
	case <-internalDone:
	case <-time.After(3 * time.Second):
		t.Fatal("internal promiseBatch parked on a full ring; lock-holding pushers would deadlock against the promise worker")
	}

	// Release the worker: the goroutine'd in-promise produce, the parked
	// spinner, and the ring itself must all converge.
	close(gate)
	select {
	case <-viaGoroutine:
	case <-time.After(15 * time.Second):
		t.Fatal("goroutine'd produce-from-promise never completed after the worker resumed")
	}
	select {
	case <-fillerDone:
	case <-time.After(15 * time.Second):
		t.Fatal("parked TryProduce spinner never resumed as the worker drained")
	}
	deadline = time.Now().Add(15 * time.Second)
	for batchPromisesLen(cl) > 0 {
		if time.Now().After(deadline) {
			t.Fatal("promise ring never drained")
		}
		time.Sleep(time.Millisecond)
	}
}

// finishPromises accumulated its cond broadcast and fired it only when the
// worker exited, i.e. when the ring was observed empty - but ead18d3c's
// stated batching granularity was "one broadcast at the end of a batch". As
// long as new promise elements kept arriving, a Flush whose condition had
// long become true (bufferedRecords hit 0) was never woken, and blocked
// Produce calls starved the same way.
//
// The chain below makes the starvation causal rather than timing-dependent:
// each chain promise pushes the next element from within the worker, so the
// worker never observes an empty ring and never exits while the chain runs.
// The fixed property - "a broadcast fires per batch, mid-drain, not only at
// ring exit" - is asserted at its source: the onBatchPromiseBroadcast test
// hook fires on the producer's per-batch p.c.Broadcast(), reporting whether
// further elements are still queued. With the self-feeding chain keeping the
// worker in its drain loop, a broadcast that fires while more is still queued
// is exactly the mid-drain broadcast the fix introduced. Pre-fix the only
// broadcast happens at ring exit (moreQueued == false) and the chain runs to
// completion before any broadcast at all; observing a moreQueued broadcast is
// therefore both necessary and sufficient, and fully deterministic - no woken
// goroutine has to win a scheduler race. As a corroborating end-to-end check,
// a real Flush goroutine blocked on the same condition must then return.
func TestAuditFlushNotStarvedByPromiseChain(t *testing.T) {
	t.Parallel()

	cl, err := NewClient(SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx := context.Background()
	p := &cl.producer

	// midDrainBroadcast is closed (once) the first time the per-batch
	// broadcast fires while more promise elements are still queued - the
	// mid-drain broadcast that the fix guarantees. stopChain is closed
	// alongside it so the self-feeding chain stops growing the ring.
	var (
		midDrainBroadcast = make(chan struct{})
		stopChain         = make(chan struct{})
		broadcastOnce     sync.Once
	)
	p.onBatchPromiseBroadcast = func(moreQueued bool) {
		if moreQueued {
			broadcastOnce.Do(func() {
				close(midDrainBroadcast)
				close(stopChain)
			})
		}
	}

	// One manually-accounted buffered record; Flush waits on it. The
	// record carries no key/value so bufferedBytes stays balanced.
	p.mu.Lock()
	p.bufferedRecords = 1
	p.mu.Unlock()

	flushDone := make(chan struct{})
	go func() {
		defer close(flushDone)
		cl.Flush(ctx)
	}()
	// Flush bumps flushing before it waits; the cond protocol makes the
	// wake-up safe regardless, but we want the broadcast condition
	// (flushing > 0) to be set before the counted record finishes.
	deadline := time.Now().Add(10 * time.Second)
	for p.flushing.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("flush never started")
		}
		time.Sleep(time.Millisecond)
	}

	// The self-feeding chain keeps the worker in its drain loop: each link
	// pushes the next element before returning, so the ring is never empty
	// and the worker never exits while the chain runs. The chain stops once
	// stopChain is closed (by the hook on a mid-drain broadcast) or at a
	// generous cap, so a regression cannot spin forever. chainStopped is
	// closed when the chain actually returns, so the test can read chainLinks
	// and capHit without racing the worker goroutine that mutates them. Those
	// two vars are only ever touched on the worker goroutine while the chain
	// runs (the drain loop is single-threaded), so no lock is needed there.
	const chainCap = 20000
	var (
		chainLinks   int
		capHit       bool
		chainStopped = make(chan struct{})
		stopOnce     sync.Once
		chain        func(*Record, error)
	)
	chain = func(*Record, error) {
		select {
		case <-stopChain:
			stopOnce.Do(func() { close(chainStopped) })
			return
		default:
		}
		chainLinks++
		if chainLinks >= chainCap {
			capHit = true
			stopOnce.Do(func() { close(chainStopped) })
			return
		}
		cl.TryProduce(ctx, &Record{}, chain) // no topic: fails pre-buffer, pushes the next link
	}

	// The counted record: its finish drops bufferedRecords to 0 (making
	// Flush's condition true) and its promise seeds the chain, so the
	// ring is already non-empty when the worker finishes this element.
	p.promiseBatch(batchPromise{
		recs: []promisedRec{{ctx, func(*Record, error) {
			cl.TryProduce(ctx, &Record{}, chain)
		}, &Record{}}},
	})

	// Deterministic assertion: a broadcast must fire mid-drain. This does
	// not depend on any goroutine being scheduled - it is observed directly
	// on the producer's broadcast path. On regression the hook never closes
	// midDrainBroadcast and the chain runs to its cap, closing chainStopped.
	select {
	case <-midDrainBroadcast:
	case <-chainStopped:
	case <-time.After(30 * time.Second):
		t.Fatal("flush starved: no per-batch broadcast fired mid-drain and the chain never stopped")
	}

	// Wait for the chain to fully stop before reading chainLinks/capHit, so
	// the reads do not race the worker goroutine.
	select {
	case <-chainStopped:
	case <-time.After(30 * time.Second):
		t.Fatal("chain never stopped after mid-drain broadcast")
	}
	if capHit {
		t.Fatalf("flush starved: %d promise elements drained without a mid-drain broadcast", chainLinks)
	}

	// Corroborating end-to-end check: the broadcast we observed must wake
	// the real Flush blocked on the same condition.
	select {
	case <-flushDone:
	case <-time.After(30 * time.Second):
		t.Fatal("mid-drain broadcast fired but Flush never returned")
	}
}

// purgeTopics deleted unknown-topic waiters and stored the cleaned topics
// map AFTER releasing unknownTopicsMu (the store via a late defer): a
// produce racing inside that window saw the topic still present in
// producer.topics, took the exists-path, and re-created an unknownTopics
// waiter that the purge then orphaned by removing the topic from the map.
// The metadata request set is built from producer.topics and response
// handling iterates only requested topics, so nothing could ever notify the
// orphan: the record's promise never fired, BufferedProduceRecords stayed
// stuck, and Flush hung forever (no record timeout by default). The heal
// invariant is the one storePartitionsUpdate already documents: state that
// gates waiter creation must change while unknownTopicsMu is held.
//
// The test freezes the purge inside its window deterministically: the purge
// fails the purged topics' recBufs via sink.removeRecBuf, which takes
// sink.recBufsMu - held by the test. Pre-fix the freeze point is after the
// waiter sweep but before the topics store; post-fix the store happens under
// unknownTopicsMu before the freeze point, and the produce's re-check makes
// it re-register the topic properly.
func TestAuditPurgeVsProduceUnknownTopicNoOrphan(t *testing.T) {
	t.Parallel()

	cl, err := NewClient(SeedBrokers("127.0.0.1:1")) // metadata never loads; failures only tick retry counters
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx := context.Background()
	p := &cl.producer

	// Craft a known topic K with one partition on a sink, mirroring what a
	// metadata load builds: the purge's recBuf removal is our freeze
	// vehicle.
	s := cl.newSink(1)
	r := &recBuf{
		cl:                  cl,
		topic:               "K",
		partition:           0,
		maxRecordBatchBytes: 1 << 20,
		recBufsIdx:          -1,
		lastAckedOffset:     -1,
		sink:                s,
	}
	s.addRecBuf(r)
	p.topics.storeTopics([]string{"K"})
	tp := &topicPartition{records: r}
	p.topics.load()["K"].v.Store(&topicPartitionsData{
		topic:              "K",
		partitions:         []*topicPartition{tp},
		writablePartitions: []*topicPartition{tp},
	})

	// First produce to U: stores U in producer.topics and registers the
	// unknown-topic waiter that the purge will sweep.
	pU1 := make(chan error, 1)
	cl.Produce(ctx, &Record{Topic: "U", Value: []byte("v")}, func(_ *Record, err error) { pU1 <- err })

	// Freeze point armed: the purge will block removing K's recBuf.
	s.recBufsMu.Lock()
	purgeDone := make(chan struct{})
	go func() {
		defer close(purgeDone)
		cl.PurgeTopicsFromClient("K", "U")
	}()

	// The waiter sweep promises U's first record errPurged; once observed,
	// the purge is at or before the frozen recBuf removal, strictly before
	// it finishes.
	select {
	case err := <-pU1:
		if !errors.Is(err, errPurged) {
			t.Fatalf("first U record failed with %v, want errPurged", err)
		}
	case <-time.After(10 * time.Second):
		s.recBufsMu.Unlock()
		t.Fatal("purge never swept the unknown-topic waiter")
	}

	// Produce to U inside the purge window. Pre-fix this lands in the
	// exists-path (topics map not yet stored) and re-creates the waiter
	// the purge orphans; post-fix it blocks on topicsMu and re-registers
	// the topic after the purge.
	pU2 := make(chan error, 1)
	produceReturned := make(chan struct{})
	go func() {
		defer close(produceReturned)
		cl.Produce(ctx, &Record{Topic: "U", Value: []byte("v2")}, func(_ *Record, err error) { pU2 <- err })
	}()
	time.Sleep(100 * time.Millisecond) // let the produce reach the window (pre-fix) or park on topicsMu (post-fix)

	s.recBufsMu.Unlock()
	<-purgeDone
	select {
	case <-produceReturned:
	case <-time.After(10 * time.Second):
		t.Fatal("produce never returned after the purge completed")
	}

	// The invariant: an unknownTopics waiter must imply its topic is in
	// producer.topics, else no metadata update will ever request the topic
	// and the waiter is silently permanent.
	p.unknownTopicsMu.Lock()
	_, unknownExists := p.unknownTopics["U"]
	p.unknownTopicsMu.Unlock()
	if !unknownExists {
		t.Fatal("expected an unknown-topic waiter for U (record is buffered awaiting metadata)")
	}
	if p.topics.load()["U"] == nil {
		t.Fatal("unknown-topic waiter for U exists but U is not in producer.topics: the waiter is orphaned and the record hangs forever")
	}

	// Close must fail the still-buffered record (the waiter resolves via
	// client shutdown); guard against a promise leak either way.
	cl.Close()
	select {
	case <-pU2:
	case <-time.After(10 * time.Second):
		t.Fatal("second U record's promise never fired, even through client close")
	}
}

// Failed records previously inherited the batchPromise zero values: a failed
// record's Offset was its index within the failed batch and its
// ProducerID/ProducerEpoch were 0 - plausible-looking garbage. Failures must
// carry the -1 unknown-sentinels (the success path already does for acks=0's
// unknown base offset).
func TestAuditFailedRecordSentinels(t *testing.T) {
	t.Parallel()

	cl, err := NewClient(SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	done := make(chan struct{})
	cl.TryProduce(context.Background(), &Record{}, func(r *Record, err error) {
		defer close(done)
		if err == nil {
			t.Error("expected topic-less record to fail")
		}
		if r.Offset != -1 || r.ProducerID != -1 || r.ProducerEpoch != -1 || r.LeaderEpoch != -1 {
			t.Errorf("failed record carries offset=%d pid=%d pepoch=%d lepoch=%d, want -1 sentinels",
				r.Offset, r.ProducerID, r.ProducerEpoch, r.LeaderEpoch)
		}
	})
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("promise never fired")
	}
}

// On the produce block path, a record that hits the max-buffered limit waits
// in a goroutine; when its context is canceled, drainBuffered decrements
// p.blocked. Unlike the success path - which compensates with bufferedRecords++
// so the bufferedRecords+blocked sum Flush waits on is unchanged - the cancel
// path just drops the sum, and pre-fix the only broadcast on that path (the one
// that wakes the block goroutine) fired BEFORE the decrement. A Flush that
// re-checked its predicate in between observed the stale pre-decrement sum and
// went back to waiting; once the real sum reached zero it was never woken, so
// Flush(context.Background()) hung forever.
//
// Whether Flush re-acquires the lock before the block goroutine decrements is a
// scheduler race, so this drives the scenario many times. Pre-fix at least one
// iteration loses the race and Flush hangs; post-fix every iteration completes
// because drainBuffered broadcasts after the decrement.
func TestAuditFlushWokenByBlockedProduceCancel(t *testing.T) {
	t.Parallel()

	cl, err := NewClient(SeedBrokers("127.0.0.1:1"), MaxBufferedRecords(1))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	p := &cl.producer

	waitUntil := func(cond func() bool) bool {
		deadline := time.Now().Add(10 * time.Second)
		for !cond() {
			if time.Now().After(deadline) {
				return false
			}
			time.Sleep(50 * time.Microsecond)
		}
		return true
	}

	for i := 0; i < 200; i++ {
		// Pretend one record is already buffered so the next Produce blocks
		// at the max-buffered limit. The record carries no key/value so the
		// byte accounting stays balanced.
		p.mu.Lock()
		p.bufferedRecords = 1
		p.mu.Unlock()

		ctx, cancel := context.WithCancel(context.Background())
		produceDone := make(chan struct{})
		go func() {
			defer close(produceDone)
			cl.Produce(ctx, &Record{Topic: "t"}, func(*Record, error) {})
		}()
		if !waitUntil(func() bool { return p.blocked.Load() == 1 }) {
			t.Fatalf("iteration %d: produce never blocked", i)
		}

		flushDone := make(chan struct{})
		go func() {
			defer close(flushDone)
			cl.Flush(context.Background())
		}()
		if !waitUntil(func() bool { return p.flushing.Load() > 0 }) {
			t.Fatalf("iteration %d: flush never started", i)
		}
		time.Sleep(time.Millisecond) // let Flush reach its cond Wait

		// The "buffered" record completes, but with no broadcast (in
		// production Flush already consumed that broadcast and re-waited):
		// now only blocked holds the sum above zero.
		p.mu.Lock()
		p.bufferedRecords = 0
		p.mu.Unlock()

		// Cancel the blocked produce: drainBuffered decrements blocked to 0.
		cancel()

		select {
		case <-flushDone:
		case <-time.After(2 * time.Second):
			t.Fatalf("iteration %d: Flush hung after a blocked produce was canceled - lost wakeup on the drainBuffered cancel path", i)
		}
		<-produceDone
	}
}
