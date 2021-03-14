package kgo

import "sync/atomic"

// a helper type for some places
type atomicBool uint32

func (b *atomicBool) set(v bool) {
	if v {
		atomic.StoreUint32((*uint32)(b), 1)
	} else {
		atomic.StoreUint32((*uint32)(b), 0)
	}
}

func (b *atomicBool) get() bool {
	v := atomic.LoadUint32((*uint32)(b))
	if v == 1 {
		return true
	}
	return false
}

const (
	stateUnstarted = iota
	stateWorking
	stateContinueWorking
)

type workLoop struct{ state uint32 }

// maybeBegin returns whether a work loop should begin.
func (l *workLoop) maybeBegin() bool {
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

// maybeFinish demotes loop's internal state and returns whether work should
// keep going. This function should be called before looping to continue
// work.
//
// If again is true, this will avoid demoting from working to not
// working. Again would be true if the loop knows it should continue working;
// calling this function is necessary even in this case to update loop's
// internal state.
//
// This function is a no-op if the loop is already finished, but generally,
// since the loop itself calls MaybeFinish after it has been started, this
// should never be called if the loop is unstarted.
func (l *workLoop) maybeFinish(again bool) bool {
	switch state := atomic.LoadUint32(&l.state); state {
	// Working:
	// If again, we know we should continue; keep our state.
	// If not again, we try to downgrade state and stop.
	// If we cannot, then something slipped in to say keep going.
	case stateWorking:
		if !again {
			again = !atomic.CompareAndSwapUint32(&l.state, state, stateUnstarted)
		}
	// Continue: demote ourself and run again no matter what.
	case stateContinueWorking:
		atomic.StoreUint32(&l.state, stateWorking)
		again = true
	}

	return again
}

func (l *workLoop) hardFinish() {
	atomic.StoreUint32(&l.state, stateUnstarted)
}
