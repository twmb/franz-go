package kgo

import (
	"sync"
	"testing"
)

// Regression test for issue #1353: a data race on coordinatorLoad.node
// between doLoadCoordinators (the loader) and deleteStaleCoordinatorsByNode.
//
// doLoadCoordinators inserts a coordinatorLoad into cl.coordinators with an
// open loadWait, releases coordinatorsMu, then -- lock-free -- writes c.node
// from the FindCoordinator response and publishes that write by closing
// loadWait. deleteStaleCoordinatorsByNode runs under coordinatorsMu, but the
// mutex does not order it against the lock-free node write: before the fix it
// read v.node in the range filter, before observing close(loadWait), which is
// a genuine data race.
//
// This test recreates exactly that interleaving: a loader goroutine writes
// c.node then closes loadWait, racing a deleteStaleCoordinatorsByNode call.
// Under `go test -race` the pre-fix code reports a race; the fixed code, which
// only reads v.node inside the `case <-v.loadWait` arm, does not.
func TestDeleteStaleCoordinatorsByNodeNoRace(t *testing.T) {
	t.Parallel()

	const iterations = 300
	for i := 0; i < iterations; i++ {
		cl := &Client{coordinators: make(map[coordinatorKey]*coordinatorLoad)}

		loadWait := make(chan struct{})
		c := &coordinatorLoad{loadWait: loadWait}
		cl.coordinators[coordinatorKey{"group", coordinatorTypeGroup}] = c

		start := make(chan struct{})
		var wg sync.WaitGroup
		wg.Add(2)

		// Loader: write node lock-free, then publish via close(loadWait),
		// exactly as doLoadCoordinators does.
		go func() {
			defer wg.Done()
			<-start
			c.node = 1
			close(loadWait)
		}()

		// Concurrent stale-deletion as a broker disconnect would trigger.
		go func() {
			defer wg.Done()
			<-start
			cl.deleteStaleCoordinatorsByNode(1)
		}()

		close(start) // release both goroutines together to maximize overlap
		wg.Wait()
	}
}
