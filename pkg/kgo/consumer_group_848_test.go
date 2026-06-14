package kgo

import "testing"

// signalSubscriptionChange must route an 848 subscription change to a forced
// heartbeat, NEVER to rejoinCh. Feeding rejoinCh in 848 mode bounces the
// heartbeat session and runs the session-end revoke's nowAssigned
// read-modify-write concurrently with live heartbeats, losing a completing
// heartbeat's nowAssigned store (the hazard documented at
// signalSubscriptionChange and ForceRebalance). PurgeFetchTopics fed rejoinCh
// unconditionally - the one subscription-change feeder that forgot the 848
// guard its siblings (findNewAssignments, ForceRebalance) carry - until the
// dispatch was centralized here.
func TestSignalSubscriptionChange848(t *testing.T) {
	t.Parallel()
	newG := func(is848 bool) *groupConsumer {
		return &groupConsumer{
			is848:    is848,
			rejoinCh: make(chan string, 1),
			// Buffered (production is unbuffered) so the best-effort,
			// non-blocking send is observable without a receiver.
			heartbeatForceCh: make(chan func(error), 1),
		}
	}

	// 848: forces a heartbeat and must not feed rejoinCh.
	g := newG(true)
	g.signalSubscriptionChange("change")
	select {
	case f := <-g.heartbeatForceCh:
		if f == nil {
			t.Fatal("848: heartbeatForceCh received a nil func")
		}
	default:
		t.Fatal("848: expected a forced heartbeat, got nothing")
	}
	select {
	case why := <-g.rejoinCh:
		t.Fatalf("848: rejoinCh must never be fed (got %q); it bounces the session and races the nowAssigned store", why)
	default:
	}

	// Classic: feeds rejoinCh and forces no heartbeat.
	g = newG(false)
	g.signalSubscriptionChange("change")
	select {
	case why := <-g.rejoinCh:
		if why != "change" {
			t.Fatalf("classic: rejoinCh why = %q, want %q", why, "change")
		}
	default:
		t.Fatal("classic: expected a rejoin, got nothing")
	}
	select {
	case <-g.heartbeatForceCh:
		t.Fatal("classic: heartbeatForceCh must not be fed")
	default:
	}
}

// shouldNotify848Restart must never divide by zero. RequestRetries(0) is a
// valid public config (no floor), and it disables in-session heartbeat
// retries so the 848 transient-restart arm runs on the first transient error;
// before this guard that arm computed restarts % cfg.retries, panicking
// "integer divide by zero" on the manage848 goroutine and crashing the
// client. retries=0 must still fire the (only) unreachable-group notification,
// on every restart, not drop it.
func TestShouldNotify848Restart(t *testing.T) {
	t.Parallel()

	// retries=0 must not panic; it notifies on every restart. The recover
	// turns the pre-fix panic into a clean test failure rather than a
	// process crash (the real bug panics on a background goroutine).
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("shouldNotify848Restart(_, 0) panicked: %v", r)
			}
		}()
		for restarts := int64(1); restarts <= 3; restarts++ {
			if !shouldNotify848Restart(restarts, 0) {
				t.Errorf("retries=0 restart=%d: want notify on every restart", restarts)
			}
		}
	}()

	// retries=2 notifies on every 2nd restart, never on the 1st (preserved
	// behavior for the configured-retries case).
	for _, tc := range []struct {
		restarts int64
		want     bool
	}{
		{1, false},
		{2, true},
		{3, false},
		{4, true},
	} {
		if got := shouldNotify848Restart(tc.restarts, 2); got != tc.want {
			t.Errorf("shouldNotify848Restart(%d, 2) = %v, want %v", tc.restarts, got, tc.want)
		}
	}
}
