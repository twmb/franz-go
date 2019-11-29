package work

import "sync/atomic"

const (
	stateUnstarted = iota
	stateWorking
	stateContinueWorking
)

type Loop struct{ state uint32 }

// MaybeBegin returns whether a work loop should begin.
func (l *Loop) MaybeBegin() bool {
	var state uint32
	var done bool
	for !done {
		switch state = atomic.LoadUint32(&l.state); state {
		case stateUnstarted:
			done = atomic.CompareAndSwapUint32(&l.state, state, stateWorking)
			state = stateWorking
		case stateWorking:
			done = atomic.CompareAndSwapUint32(&l.state, state, stateContinueWorking)
			state = stateContinueWorking
		case stateContinueWorking:
			done = true
		}
	}

	return state == stateWorking
}

// MaybeFinish demotes Loop's internal state and returns whether work should
// actually stop. This function should be called before looping to continue
// work.
//
// If willGoAgain is true, this will avoid demoting from working to not
// working. Again would be true if the loop knows it should continue working;
// calling this function is necessary even in this case to update Loop's
// internal state.
//
// This function is a no-op if the loop is already finished, but generally,
// since the loop itself calls MaybeFinish after it has been started, this
// should never be called if the loop is unstarted.
func (l *Loop) MaybeFinish(willGoAgain bool) bool {
	switch state := atomic.LoadUint32(&l.state); state {
	// Working:
	// If again, we know we should continue; keep our state.
	// If not again, we try to downgrade state and stop.
	// If we cannot, then something slipped in to say keep going.
	case stateWorking:
		if !willGoAgain {
			willGoAgain = !atomic.CompareAndSwapUint32(&l.state, state, stateUnstarted)
		}
	// Continue: demote ourself and run again no matter what.
	case stateContinueWorking:
		atomic.StoreUint32(&l.state, stateWorking)
		willGoAgain = true
	}

	return !willGoAgain
}
