package kgo

import (
	"context"
	"errors"
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

// The batchPromises ring gained maxLen backpressure (#1194): push parks when
// the ring is full. The promise worker is the ring's only drainer (dropPeek
// is the only Signal site), so any push made FROM the worker goroutine parks
// forever once the ring is full. The documented way to produce from inside a
// promise is TryProduce - and a TryProduce whose record fails before
// buffering (no topic, not in txn, over limits) pushes its failure promise
// right back onto the ring from the worker goroutine. Pre-fix this wedged
// the whole producer: every promise, Flush, and blocked Produce.
//
// The same blocking push was also reachable while holding client locks
// (purgeTopics and failBufferedRecords under topicsMu+unknownTopicsMu,
// storePartitionsUpdate under unknownTopicsMu, recBuf failure paths under
// recBuf.mu); the worker re-enters those locks through user promises, so a
// parked lock-holder deadlocks the same way. The internal promiseBatch entry
// must therefore never park either, which the probe below exercises.
func TestAuditTryProduceFromPromiseNoDeadlock(t *testing.T) {
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
	// and no second worker can spawn.
	gate := make(chan struct{})
	reentered := make(chan struct{})
	cl.TryProduce(ctx, &Record{}, func(*Record, error) {
		<-gate
		// The documented in-promise produce pattern; the record has no
		// topic so it fails pre-buffer and pushes onto the ring from
		// the worker goroutine.
		cl.TryProduce(ctx, &Record{}, func(*Record, error) { close(reentered) })
	})

	// Fill the ring to maxLen. Pre-fix, TryProduce itself parks at the
	// limit (violating its fail-fast contract), so fill from a goroutine
	// and wait for the ring to report full.
	fillerDone := make(chan struct{})
	go func() {
		defer close(fillerDone)
		for i := 0; i < maxLen+64; i++ {
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

	// Probe the internal promiseBatch entry (what purge/fail paths and
	// storePartitionsUpdate use, under client locks): it must not park.
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

	// Release the worker; its in-promise TryProduce must complete.
	close(gate)
	select {
	case <-reentered:
	case <-time.After(3 * time.Second):
		t.Fatal("promise worker deadlocked pushing its own TryProduce failure onto the full ring")
	}

	// Post-fix the filler never parks; wait for it and for the ring to
	// drain so Close is clean.
	select {
	case <-fillerDone:
	case <-time.After(10 * time.Second):
		t.Fatal("TryProduce filler parked; TryProduce must not block on the promise ring")
	}
	deadline = time.Now().Add(10 * time.Second)
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
// worker never observes an empty ring and never exits while the chain runs,
// and the chain only stops once Flush returns (or at a generous cap).
// Pre-fix, Flush cannot return before the chain ends (no broadcast is ever
// fired mid-drain and nothing else broadcasts), so the chain provably hits
// its cap. Post-fix, the broadcast after the first (counted) element wakes
// Flush within a few chain links.
func TestAuditFlushNotStarvedByPromiseChain(t *testing.T) {
	t.Parallel()

	cl, err := NewClient(SeedBrokers("127.0.0.1:1"))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx := context.Background()
	p := &cl.producer

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

	const chainCap = 20000
	var (
		chainLinks int
		capHit     bool
		chainDone  = make(chan struct{})
		chain      func(*Record, error)
	)
	chain = func(*Record, error) {
		select {
		case <-flushDone:
			close(chainDone) // flush returned while the chain was alive: success
			return
		default:
		}
		chainLinks++
		if chainLinks >= chainCap {
			capHit = true
			close(chainDone)
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

	select {
	case <-chainDone:
	case <-time.After(30 * time.Second):
		t.Fatal("promise chain neither observed flush completion nor hit its cap")
	}
	if capHit {
		t.Fatalf("flush starved: %d promise elements drained without a broadcast while flush's condition was satisfied", chainLinks)
	}
	<-flushDone
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
