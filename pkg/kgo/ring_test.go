package kgo

import (
	"testing"
	"time"
)

func TestRing(t *testing.T) {
	t.Run("push multiple elements and then drop them", func(t *testing.T) {
		r := &ring[int]{}

		assertRingPush(t, r, 1, true, false)
		assertRingPush(t, r, 2, false, false)
		assertRingPush(t, r, 3, false, false)

		assertRingDropPeek(t, r, 2, true, false)
		assertRingDropPeek(t, r, 3, true, false)
		assertRingDropPeek(t, r, 0, false, false)
	})

	t.Run("push and drop elements iteratively", func(t *testing.T) {
		r := &ring[int]{}

		// Push an initial element.
		assertRingPush(t, r, 1, true, false)

		// Push an element and them drop the previous one, multiple times.
		for i := 2; i < 10; i++ {
			assertRingPush(t, r, i, false, false)
			assertRingDropPeek(t, r, i, true, false)
		}

		// Finally, drop the last element.
		assertRingDropPeek(t, r, 0, false, false)
	})

	t.Run("push elements above the initial capacity and verify growth", func(t *testing.T) {
		r := &ring[int]{}

		for i := 1; i <= 10; i++ {
			assertRingPush(t, r, i, i == 1, false)
		}

		// We expect the buffer has grown beyond minRingCap.
		if cap(r.elems) <= minRingCap {
			t.Errorf("expected capacity > %d, got %d", minRingCap, cap(r.elems))
		}

		for i := 1; i <= 9; i++ {
			assertRingDropPeek(t, r, i+1, true, false)
		}
		assertRingDropPeek(t, r, 0, false, false)

		// At this point the buffer should have shrunk back to minRingCap.
		if cap(r.elems) != minRingCap {
			t.Errorf("expected capacity %d after drain, got %d", minRingCap, cap(r.elems))
		}
	})

	t.Run("interrupt a non-full ring", func(t *testing.T) {
		r := &ring[int]{}

		assertRingPush(t, r, 1, true, false)
		assertRingPush(t, r, 2, false, false)
		assertRingPush(t, r, 3, false, false)

		r.die()

		assertRingPush(t, r, 4, false, true)
		assertRingDropPeek(t, r, 2, true, true)
	})

	t.Run("continuously keeping items above min capacity should not grow indefinitely", func(t *testing.T) {
		r := &ring[int]{}

		// Push an initial number of elements above minRingCap.
		for i := 1; i <= 10; i++ {
			assertRingPush(t, r, i, i == 1, false)
		}

		// Now keep pushing and dropping elements continuously.
		for i := 11; i <= 1000; i++ {
			assertRingPush(t, r, i, false, false)
			assertRingDropPeek(t, r, i-9, true, false)
		}

		// Capacity should stay bounded since we maintain ~10 elements.
		if cap(r.elems) > 32 {
			t.Errorf("unexpected high capacity, got: %d", cap(r.elems))
		}
	})

	t.Run("temporarily high number of items should shrink back after drain", func(t *testing.T) {
		r := &ring[int]{}

		// Push a large number of elements.
		for i := 1; i <= 1000; i++ {
			assertRingPush(t, r, i, i == 1, false)
		}

		if cap(r.elems) < 1000 {
			t.Errorf("unexpected low capacity, got: %d, expected >= 1000", cap(r.elems))
		}

		// Drop all but a few.
		for i := 1; i <= 996; i++ {
			r.dropPeek()
		}

		// Should have shrunk since we're at 4 elements (<=minRingCap/2).
		if cap(r.elems) != minRingCap {
			t.Errorf("expected capacity %d after partial drain, got %d", minRingCap, cap(r.elems))
		}

		// Verify remaining elements are correct.
		assertRingDropPeek(t, r, 998, true, false)
		assertRingDropPeek(t, r, 999, true, false)
		assertRingDropPeek(t, r, 1000, true, false)
		assertRingDropPeek(t, r, 0, false, false)
	})
}

func TestRingMaxLen(t *testing.T) {
	var r ring[int]
	r.initMaxLen(3) // small limit for testing

	// Fill buffer up to limit.
	for i := range 3 {
		first, dead := r.push(i)
		if dead {
			t.Fatal("unexpected dead")
		}
		if first != (i == 0) {
			t.Fatalf("first mismatch at %d", i)
		}
	}

	// Next push should block - verify with goroutine + timeout.
	blocked := make(chan struct{})
	done := make(chan struct{})
	go func() {
		close(blocked)
		r.push(99)
		close(done)
	}()

	<-blocked
	select {
	case <-done:
		t.Fatal("push should have blocked")
	case <-time.After(100 * time.Millisecond):
		// Expected - push is blocked
	}

	// Drain one element - should unblock the pusher.
	r.dropPeek()

	select {
	case <-done:
		// Expected - push completed
	case <-time.After(time.Second):
		t.Fatal("push should have unblocked after dropPeek")
	}
}

func TestRingMaxLenDie(t *testing.T) {
	var r ring[int]
	r.initMaxLen(1)

	// Fill to limit.
	r.push(0)

	// Next push should block.
	done := make(chan bool)
	go func() {
		_, dead := r.push(99)
		done <- dead
	}()

	time.Sleep(100 * time.Millisecond)
	r.die()

	select {
	case dead := <-done:
		if !dead {
			t.Fatal("expected dead=true after die()")
		}
	case <-time.After(time.Second):
		t.Fatal("push should have unblocked after die()")
	}
}

func assertRingPush(t *testing.T, r *ring[int], elem int, expectedFirst, expectedDead bool) {
	t.Helper()

	first, dead := r.push(elem)
	if first != expectedFirst {
		t.Errorf("unexpected first: got %t, want %t", first, expectedFirst)
	}
	if dead != expectedDead {
		t.Errorf("unexpected dead: got %t, want %t", dead, expectedDead)
	}
}

func assertRingDropPeek(t *testing.T, r *ring[int], expectedNext int, expectedMore, expectedDead bool) {
	t.Helper()

	next, more, dead := r.dropPeek()
	if next != expectedNext {
		t.Errorf("unexpected next element: got %d, want %d", next, expectedNext)
	}
	if more != expectedMore {
		t.Errorf("unexpected more: got %t, want %t", more, expectedMore)
	}
	if dead != expectedDead {
		t.Errorf("unexpected dead: got %t, want %t", dead, expectedDead)
	}
}

func BenchmarkRingPushPopSimple(b *testing.B) {
	var r ring[int]
	// Pre-warm
	r.push(0)
	r.dropPeek()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.push(i)
		r.dropPeek()
	}
}

func BenchmarkRingPushPopBatch100(b *testing.B) {
	var r ring[int]
	for i := 0; i < b.N; i++ {
		for j := 0; j < 100; j++ {
			r.push(j)
		}
		for j := 0; j < 100; j++ {
			r.dropPeek()
		}
	}
}

func BenchmarkRingOverflowSteady(b *testing.B) {
	var r ring[int]
	// Fill beyond initial buffer size
	for i := 0; i < 50; i++ {
		r.push(i)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		r.push(i)
		r.dropPeek()
	}
}

func BenchmarkRingGrowDrain1000(b *testing.B) {
	for i := 0; i < b.N; i++ {
		var r ring[int]
		for j := 0; j < 1000; j++ {
			r.push(j)
		}
		for j := 0; j < 1000; j++ {
			r.dropPeek()
		}
	}
}
