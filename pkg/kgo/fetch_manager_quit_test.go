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
// (with the default MaxConcurrentFetches == 0). When no poll is in progress,
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

	fm := newFetchManager(ctx, cancel, &pollActive, pollWake, 0) // 0 == default MaxConcurrentFetches

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
				registered.Done()
				return
			case fm.desireFetch() <- canFetch:
				registered.Done()
			}

			select {
			case <-ctx.Done():
				fm.cancelFetchCh <- canFetch // bare send, as in source.go
			case doneFetch := <-canFetch:
				select {
				case <-ctx.Done():
				default:
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
