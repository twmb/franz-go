package protobuf

import (
	"container/list"
	"sync"
)

const maxCacheEntries = 1024

// lru is a tiny cap-bounded cache, safe for concurrent use.
//
// get locks exclusively rather than sharing a read lock, because a hit
// reorders the recency list and so mutates.
//
// This is duplicated in pkg/sr/serde; the two packages are separate modules
// and this is not worth exporting.
type lru[K comparable, V any] struct {
	mu  sync.Mutex
	cap int
	ll  *list.List
	m   map[K]*list.Element
}

type lruItem[K comparable, V any] struct {
	key K
	val V
}

func newLRU[K comparable, V any](cap int) *lru[K, V] {
	return &lru[K, V]{
		cap: cap,
		ll:  list.New(),
		m:   make(map[K]*list.Element),
	}
}

func (l *lru[K, V]) get(k K) (V, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	el, ok := l.m[k]
	if !ok {
		var zero V
		return zero, false
	}
	l.ll.MoveToFront(el)
	return el.Value.(lruItem[K, V]).val, true
}

func (l *lru[K, V]) add(k K, v V) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if el, ok := l.m[k]; ok {
		el.Value = lruItem[K, V]{key: k, val: v}
		l.ll.MoveToFront(el)
		return
	}
	el := l.ll.PushFront(lruItem[K, V]{key: k, val: v})
	l.m[k] = el
	for l.ll.Len() > l.cap {
		back := l.ll.Back()
		l.ll.Remove(back)
		delete(l.m, back.Value.(lruItem[K, V]).key)
	}
}

// flight collapses concurrent calls for the same key into one, so a cold cache
// under load issues one registry request rather than one per caller.
//
// This is duplicated in pkg/sr/serde; see lru.
type flight[K comparable, V any] struct {
	mu sync.Mutex
	m  map[K]*flightCall[V]
}

type flightCall[V any] struct {
	wg  sync.WaitGroup
	val V
	err error
}

func (f *flight[K, V]) do(k K, fn func() (V, error)) (V, error) {
	f.mu.Lock()
	if f.m == nil {
		f.m = make(map[K]*flightCall[V])
	}
	if c, ok := f.m[k]; ok {
		f.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err
	}
	c := new(flightCall[V])
	c.wg.Add(1)
	f.m[k] = c
	f.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	f.mu.Lock()
	delete(f.m, k)
	f.mu.Unlock()
	return c.val, c.err
}
