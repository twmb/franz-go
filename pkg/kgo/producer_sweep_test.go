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
