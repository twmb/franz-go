package kgo

import "sync/atomic"

const (
	stateUnstarted = iota
	stateWorking
	stateContinueWorking
)

// maybeBeginWork changes workState to some form of working and returns whether
// the caller needs to begin work.
func maybeBeginWork(workState *uint32) bool {
	var state uint32
	var done bool
	for !done {
		switch state = atomic.LoadUint32(workState); state {
		case stateUnstarted:
			done = atomic.CompareAndSwapUint32(workState, state, stateWorking)
			state = stateWorking
		case stateWorking:
			done = atomic.CompareAndSwapUint32(workState, state, stateContinueWorking)
			state = stateContinueWorking
		case stateContinueWorking:
			done = true
		}
	}

	return state == stateWorking
}

// maybeTryFinishWork demotes workState and returns whether work should
// continue.
//
// If again is true, this will avoid demoting from working to not working.
func maybeTryFinishWork(workState *uint32, again bool) bool {
	switch state := atomic.LoadUint32(workState); state {
	// Working:
	// If again, we know we should continue; keep our state.
	// If not again, we try to downgrade state and stop.
	// If we cannot, then something slipped in to say keep going.
	case stateWorking:
		if !again {
			again = !atomic.CompareAndSwapUint32(workState, state, stateUnstarted)
		}
	// Continue: demote ourself and run again no matter what.
	case stateContinueWorking:
		atomic.StoreUint32(workState, stateWorking)
		again = true
	}

	return again
}
