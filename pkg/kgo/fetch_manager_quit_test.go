package kgo

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// TestFetchManagerQuitDrainsPendingFetches models the exact dance every
// source.loopFetch goroutine performs against the fetchManager, and asserts
// that all sources can exit once the session context is canceled.
//
// Since v1.21.0, manageFetchConcurrency gates fetch grants on pollActive
// when MaxConcurrentFetches == 0. Zero is not the default (that is -1,
// unbounded; before v1.21.0, zero WAS both the default and unbounded): it is
// poll-gated mode, entered by explicitly opting in with
// MaxConcurrentFetches(0), or forced for share groups by
// ShareMaxRecordsStrict. When no poll is in progress,
// registered sources accumulate in wantFetch ungranted. If the session is
// then stopped (metadata update -> migrateCursorTo -> stopSession cancels the
// session ctx), manageFetchConcurrency observes wantQuit && activeFetches ==
// 0 and returns immediately -- while wantFetch is still non-empty and the
// registered sources are about to send on cancelFetchCh. cancelFetchCh is
// buffered (4): the first four cancels are absorbed, every additional source
// blocks forever on the bare `session.cancelFetchCh <- canFetch` send
// (source.go, ctx.Done branch of the fetch handoff). Each such source holds a
// session worker (incWorker/decWorker), so consumer.stopSession waits on
// workersCond forever and PollRecords never returns: the client is a zombie.
//
// This is the runtime counterpart of the shutdown-path hang: it needs no
// Close() call, only a session stop while >4 sources are registered and no
// poll is active.
func TestFetchManagerQuitDrainsPendingFetches(t *testing.T) {
	t.Parallel()

	const sources = 8 // > cancelFetchCh buffer of 4

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var pollActive atomic.Bool // false: no poll in progress
	pollWake := make(chan struct{}, 1)

	// 0 == poll-gated mode: explicit MaxConcurrentFetches(0), or forced by
	// ShareMaxRecordsStrict; not the default, which is -1 (unbounded).
	fm := newFetchManager(ctx, cancel, &pollActive, pollWake, 0)

	var registered sync.WaitGroup
	var finished sync.WaitGroup
	registered.Add(sources)
	finished.Add(sources)

	for i := 0; i < sources; i++ {
		go func() {
			defer finished.Done()

			// Mirrors source.loopFetch: register the desire to
			// fetch, then either receive the grant or cancel.
			canFetch := make(chan chan bool, 1)
			select {
			case <-ctx.Done():
				// Unreachable: cancel() fires only after
				// registered.Wait() returns. If this ever
				// fires, the premise below that every source
				// is sitting in wantFetch is silently broken
				// (we would count this source as registered
				// without it registering), so flag it.
				t.Error("ctx canceled before all sources registered; the all-sources-in-wantFetch premise is broken")
				registered.Done()
				return
			case fm.desireFetch() <- canFetch:
				registered.Done()
			}

			select {
			case <-ctx.Done():
				fm.cancelFetchCh <- canFetch // bare send, as in source.go
			case doneFetch := <-canFetch:
				// Unreachable in this test (no poll is ever
				// active and allowed fetches is 0, so the
				// manager never grants); kept to document the
				// full loopFetch shape, mirroring source.go
				// exactly: a grant that lost the race with
				// cancelation returns the token with false.
				if ctx.Err() != nil {
					doneFetch <- false
					return
				}
				doneFetch <- true
			}
		}()
	}

	registered.Wait() // all sources are in wantFetch (unbuffered desireFetchCh)
	cancel()          // session stop: stopSession cancels the session ctx

	allDone := make(chan struct{})
	go func() {
		finished.Wait()
		close(allDone)
	}()

	select {
	case <-allDone:
	case <-time.After(5 * time.Second):
		t.Fatal("fetch sources still blocked 5s after session cancel: " +
			"manageFetchConcurrency exited with pending wantFetch, " +
			"orphaning cancelFetchCh; sources hold session workers forever " +
			"and stopSession can never finish")
	}
}
