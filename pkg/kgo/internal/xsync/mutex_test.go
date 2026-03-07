package xsync

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
)

// This test is copied from the standard library's sync package.
func HammerMutex(m *Mutex, loops int, cdone chan bool) {
	for i := 0; i < loops; i++ {
		if i%3 == 0 {
			if m.TryLock() {
				m.Unlock()
			}
			continue
		}
		m.Lock()
		//nolint:staticcheck
		m.Unlock()
	}
	cdone <- true
}

// This test is copied from the standard library's sync package.
func TestMutex(t *testing.T) {
	m := new(Mutex)

	m.Lock()
	if m.TryLock() {
		t.Fatalf("TryLock succeeded with mutex locked")
	}
	m.Unlock()
	if !m.TryLock() {
		t.Fatalf("TryLock failed with mutex unlocked")
	}
	m.Unlock()

	c := make(chan bool)
	for i := 0; i < 10; i++ {
		go HammerMutex(m, 1000, c)
	}
	for i := 0; i < 10; i++ {
		<-c
	}
}

func parallelReader(m *RWMutex, clocked, cunlock, cdone chan bool) {
	m.RLock()
	clocked <- true
	<-cunlock
	m.RUnlock()
	cdone <- true
}

func doTestParallelReaders(numReaders, gomaxprocs int) {
	runtime.GOMAXPROCS(gomaxprocs)
	var m RWMutex
	clocked := make(chan bool)
	cunlock := make(chan bool)
	cdone := make(chan bool)
	for i := 0; i < numReaders; i++ {
		go parallelReader(&m, clocked, cunlock, cdone)
	}
	// Wait for all parallel RLock()s to succeed.
	for i := 0; i < numReaders; i++ {
		<-clocked
	}
	for i := 0; i < numReaders; i++ {
		cunlock <- true
	}
	// Wait for the goroutines to finish.
	for i := 0; i < numReaders; i++ {
		<-cdone
	}
}

// This test is copied from the standard library's sync package.
func TestParallelReaders(t *testing.T) {
	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(-1))
	doTestParallelReaders(1, 4)
	doTestParallelReaders(3, 4)
	doTestParallelReaders(4, 2)
}

func reader(rwm *RWMutex, numIterations int, activity *int32, cdone chan bool) {
	for i := 0; i < numIterations; i++ {
		rwm.RLock()
		n := atomic.AddInt32(activity, 1)
		if n < 1 || n >= 10000 {
			rwm.RUnlock()
			panic(fmt.Sprintf("wlock(%d)\n", n))
		}
		for i := 0; i < 100; i++ {
		}
		atomic.AddInt32(activity, -1)
		rwm.RUnlock()
	}
	cdone <- true
}

func writer(rwm *RWMutex, numIterations int, activity *int32, cdone chan bool) {
	for i := 0; i < numIterations; i++ {
		rwm.Lock()
		n := atomic.AddInt32(activity, 10000)
		if n != 10000 {
			rwm.Unlock()
			panic(fmt.Sprintf("wlock(%d)\n", n))
		}
		for i := 0; i < 100; i++ {
		}
		atomic.AddInt32(activity, -10000)
		rwm.Unlock()
	}
	cdone <- true
}

func HammerRWMutex(gomaxprocs, numReaders, numIterations int) {
	runtime.GOMAXPROCS(gomaxprocs)
	// Number of active readers + 10000 * number of active writers.
	var activity int32
	var rwm RWMutex
	cdone := make(chan bool)
	go writer(&rwm, numIterations, &activity, cdone)
	var i int
	for i = 0; i < numReaders/2; i++ {
		go reader(&rwm, numIterations, &activity, cdone)
	}
	go writer(&rwm, numIterations, &activity, cdone)
	for ; i < numReaders; i++ {
		go reader(&rwm, numIterations, &activity, cdone)
	}
	// Wait for the 2 writers and all readers to finish.
	for i := 0; i < 2+numReaders; i++ {
		<-cdone
	}
}

// This test is copied from the standard library's sync package.
func TestRWMutex(t *testing.T) {
	var m RWMutex

	m.Lock()
	if m.TryLock() {
		t.Fatalf("TryLock succeeded with mutex locked")
	}
	if m.TryRLock() {
		t.Fatalf("TryRLock succeeded with mutex locked")
	}
	m.Unlock()

	if !m.TryLock() {
		t.Fatalf("TryLock failed with mutex unlocked")
	}
	m.Unlock()

	if !m.TryRLock() {
		t.Fatalf("TryRLock failed with mutex unlocked")
	}
	if !m.TryRLock() {
		t.Fatalf("TryRLock failed with mutex rlocked")
	}
	if m.TryLock() {
		t.Fatalf("TryLock succeeded with mutex rlocked")
	}
	m.RUnlock()
	m.RUnlock()

	defer runtime.GOMAXPROCS(runtime.GOMAXPROCS(-1))
	n := 1000
	if testing.Short() {
		n = 5
	}
	HammerRWMutex(1, 1, n)
	HammerRWMutex(1, 3, n)
	HammerRWMutex(1, 10, n)
	HammerRWMutex(4, 1, n)
	HammerRWMutex(4, 3, n)
	HammerRWMutex(4, 10, n)
	HammerRWMutex(10, 1, n)
	HammerRWMutex(10, 3, n)
	HammerRWMutex(10, 10, n)
	HammerRWMutex(10, 5, n)
}

// This test is copied from the standard library's sync package.
func TestRLocker(t *testing.T) {
	var wl RWMutex
	var rl sync.Locker
	wlocked := make(chan bool, 1)
	rlocked := make(chan bool, 1)
	rl = wl.RLocker()
	n := 10
	go func() {
		for i := 0; i < n; i++ {
			rl.Lock()
			rl.Lock()
			rlocked <- true
			wl.Lock()
			wlocked <- true
		}
	}()
	for i := 0; i < n; i++ {
		<-rlocked
		rl.Unlock()
		select {
		case <-wlocked:
			t.Fatal("RLocker() didn't read-lock it")
		default:
		}
		rl.Unlock()
		<-wlocked
		select {
		case <-rlocked:
			t.Fatal("RLocker() didn't respect the write lock")
		default:
		}
		wl.Unlock()
	}
}
