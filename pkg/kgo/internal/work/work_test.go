package work

import "testing"

func TestSequential(t *testing.T) {
	// promote sequence
	var l Loop
	if !l.MaybeBegin() {
		t.Error("not beginning on first iteration")
	}
	if l.state != stateWorking {
		t.Errorf("invalid internal state %d != stateWorking", l.state)
	}
	if l.MaybeBegin() {
		t.Error("signaled to begin even though state is working")
	}
	if l.state != stateContinueWorking {
		t.Errorf("invalid internal state %d != stateContinueWorking", l.state)
	}
	if l.MaybeBegin() {
		t.Error("signaled to begin even though state is continue working")
	}
	if l.state != stateContinueWorking {
		t.Errorf("invalid internal state %d != stateContinueWorking", l.state)
	}

	// demote sequence
	if l.MaybeFinish(true) {
		t.Error("signaled to end even though state was continue working")
	}
	if l.state != stateWorking {
		t.Errorf("invalid internal state %d != stateWorking", l.state)
	}
	if l.MaybeFinish(true) {
		t.Error("signaled to end even though we wanted to continue on state working")
	}
	if l.state != stateWorking {
		t.Errorf("invalid internal state %d != stateWorking", l.state)
	}
	if !l.MaybeFinish(false) {
		t.Error("signaled to continue even though we wanted to end on state working")
	}
	if l.state != stateUnstarted {
		t.Errorf("invalid internal state %d != stateUnstarted", l.state)
	}
}

// TestConcurrent runs all loop functions concurrently; this test checks nothing
// but does ensure that there are no races.
func TestConcurrent(t *testing.T) {
	var l Loop
	for i := 0; i < 1000; i++ {
		if i%2 == 0 {
			go l.MaybeBegin()
		} else if i%3 == 0 {
			go l.MaybeFinish(true)
		} else {
			go l.MaybeFinish(false)
		}
	}
}

func BenchmarkMaybeBegin(b *testing.B) {
	var l Loop
	for i := 0; i < b.N; i++ {
		l.state = stateUnstarted
		l.MaybeBegin()
	}
}

func BenchmarkMaybeFinishWorking(b *testing.B) {
	var l Loop
	for i := 0; i < b.N; i++ {
		l.state = stateWorking
		l.MaybeFinish(false)
	}
}

func BenchmarkMaybeFinishContinueWorking(b *testing.B) {
	var l Loop
	for i := 0; i < b.N; i++ {
		l.state = stateContinueWorking
		l.MaybeFinish(false)
	}
}
