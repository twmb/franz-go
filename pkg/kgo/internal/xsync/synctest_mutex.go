//go:build synctests

package xsync

import "sync"

// In this file we reimplement sync.Mutex and sync.RWMutex and we rely exclusively on channels to implement the mutex.
// The reason we do this is that synctest cannot track the state of mutexes, so it considers that a goroutine is not 'durably blocked' when it is waiting on a mutex.
// The consequence of this is that it is impossible to test code that uses mutexes with synctest. For example, this test hangs forever with synctest
//
//	func TestA(t *testing.T) {
//		synctest.Test(t, func(t *testing.T) {
//			var l sync.Mutex
//			for range 10 {
//				go func() {
//					l.Lock()
//					defer l.Unlock()
//					time.Sleep(1 * time.Second)
//				}()
//			}
//		})
//	}
//
// because time is never moved forward because the synctest runtime considers the goroutines are not durably blocked.
// Our intent here is to use these mutexes in the code that we run with synctest.
type Mutex struct {
	// Internally this uses a mutex too, but it's OK since we never sleep while holding the mutex, we simply run
	// the very very non-blocking operation of initializing the channel.
	once sync.Once
	ch   chan struct{}
}

func (m *Mutex) init() {
	m.once.Do(func() {
		m.ch = make(chan struct{}, 1)
		m.ch <- struct{}{}
	})
}

func (m *Mutex) Lock() {
	m.init()
	<-m.ch
}

func (m *Mutex) TryLock() bool {
	m.init()
	select {
	case <-m.ch:
		return true
	default:
		return false
	}
}

func (m *Mutex) Unlock() {
	m.init()
	select {
	case m.ch <- struct{}{}:
	default:
		panic("sync: unlock of unlocked mutex")
	}
}

type RWMutex struct {
	once         sync.Once
	w            Mutex
	mu           Mutex
	readerCount  int
	writerWait   bool
	readerSignal chan struct{}
	writerSignal chan struct{}
}

func (rw *RWMutex) init() {
	rw.once.Do(func() {
		rw.readerSignal = make(chan struct{})
		rw.writerSignal = make(chan struct{}, 1)
		rw.mu.init()
		rw.w.init()
	})
}

func (rw *RWMutex) RLock() {
	rw.init()
	for {
		rw.mu.Lock()
		if !rw.writerWait {
			rw.readerCount++
			rw.mu.Unlock()
			return
		}
		sig := rw.readerSignal
		rw.mu.Unlock()
		<-sig
	}
}

func (rw *RWMutex) TryRLock() bool {
	rw.init()
	if !rw.mu.TryLock() {
		return false
	}
	if rw.writerWait {
		rw.mu.Unlock()
		return false
	}
	rw.readerCount++
	rw.mu.Unlock()
	return true
}

func (rw *RWMutex) RUnlock() {
	rw.init()
	rw.mu.Lock()
	rw.readerCount--
	if rw.readerCount < 0 {
		rw.mu.Unlock()
		panic("sync: RUnlock of unlocked RWMutex")
	}
	if rw.readerCount == 0 && rw.writerWait {
		select {
		case rw.writerSignal <- struct{}{}:
		default:
		}
	}
	rw.mu.Unlock()
}

func (rw *RWMutex) Lock() {
	rw.init()
	rw.w.Lock()
	rw.mu.Lock()
	rw.writerWait = true
	for rw.readerCount > 0 {
		rw.mu.Unlock()
		<-rw.writerSignal
		rw.mu.Lock()
	}
	rw.mu.Unlock()
}

func (rw *RWMutex) TryLock() bool {
	rw.init()
	if !rw.w.TryLock() {
		return false
	}
	if !rw.mu.TryLock() {
		rw.w.Unlock()
		return false
	}
	if rw.readerCount > 0 {
		rw.mu.Unlock()
		rw.w.Unlock()
		return false
	}
	rw.writerWait = true
	rw.mu.Unlock()
	return true
}

func (rw *RWMutex) Unlock() {
	rw.init()
	rw.mu.Lock()
	if !rw.writerWait {
		rw.mu.Unlock()
		panic("sync: Unlock of unlocked RWMutex")
	}
	rw.writerWait = false
	close(rw.readerSignal)
	rw.readerSignal = make(chan struct{})
	rw.mu.Unlock()
	rw.w.Unlock()
}

func (rw *RWMutex) RLocker() sync.Locker {
	return (*rlocker)(rw)
}

type rlocker RWMutex

func (r *rlocker) Lock()   { (*RWMutex)(r).RLock() }
func (r *rlocker) Unlock() { (*RWMutex)(r).RUnlock() }
