package kgo

import (
	"testing"
	"time"
)

func TestFixedRing(t *testing.T) {
	t.Run("push multiple elements and then drop them", func(t *testing.T) {
		r := &fixedRing[int]{}

		assertFixedRingPush(t, r, 1, true, false)
		assertFixedRingPush(t, r, 2, false, false)
		assertFixedRingPush(t, r, 3, false, false)

		assertFixedRingDropPeek(t, r, 2, true, false)
		assertFixedRingDropPeek(t, r, 3, true, false)
		assertFixedRingDropPeek(t, r, 0, false, false)
	})

	t.Run("push and drop elements iteratively", func(t *testing.T) {
		r := &fixedRing[int]{}

		// Push an initial element.
		assertFixedRingPush(t, r, 1, true, false)

		// Push an element and them drop the previous one, multiple times.
		for i := 2; i < 10; i++ {
			assertFixedRingPush(t, r, i, false, false)
			assertFixedRingDropPeek(t, r, i, true, false)
		}

		// Finally, drop the last element.
		assertFixedRingDropPeek(t, r, 0, false, false)
	})

	t.Run("push does not block if the ring is not full", func(t *testing.T) {
		r := &fixedRing[int]{}

		// Push 8 elements.
		for i := 1; i <= 8; i++ {
			assertFixedRingPush(t, r, i, i == 1, false)
		}

		// Then drop them.
		for i := 1; i < 8; i++ {
			assertFixedRingDropPeek(t, r, i+1, true, false)
		}
		assertFixedRingDropPeek(t, r, 0, false, false)
	})

	t.Run("push blocks if the ring is full", func(t *testing.T) {
		r := &fixedRing[int]{}

		// Push 8 elements.
		for i := 1; i <= 8; i++ {
			assertFixedRingPush(t, r, i, i == 1, false)
		}

		// Push 2 more elements.
		delayedPushDone := make(chan struct{}, 2)

		go func() {
			defer func() {
				delayedPushDone <- struct{}{}
			}()

			assertFixedRingPush(t, r, 9, false, false)
		}()

		go func() {
			defer func() {
				delayedPushDone <- struct{}{}
			}()

			assertFixedRingPush(t, r, 10, false, false)
		}()

		// Drop the 1st element. This is expected to unblock one of the two goroutines.
		assertFixedRingDropPeek(t, r, 2, true, false)

		select {
		case <-delayedPushDone:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for 1st delayed push")
		}

		// The 2nd goroutine should still be blocked.
		select {
		case <-time.After(100 * time.Millisecond):
		case <-delayedPushDone:
			t.Fatal("unexpected 2nd delayed push() has already returned")
		}

		// Drop the 2nd element. This is expected to unblock the other goroutine.
		assertFixedRingDropPeek(t, r, 3, true, false)

		select {
		case <-delayedPushDone:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for 2nd delayed push")
		}

		// Drop the next 5 elements.
		for expectedNext := 4; expectedNext <= 8; expectedNext++ {
			assertFixedRingDropPeek(t, r, expectedNext, true, false)
		}

		// We don't know which of the 2 goroutines have been able to add the element first,
		// so we're happy with any of the two.
		firstDelayedNext, more, _ := r.dropPeek()
		if firstDelayedNext != 9 && firstDelayedNext != 10 {
			t.Errorf("unexpected next element: got %d, want 9 or 10", firstDelayedNext)
		}
		if !more {
			t.Error("expected more elements")
		}

		secondDelayedNext, more, _ := r.dropPeek()
		if (secondDelayedNext != 9 && secondDelayedNext != 10) || (firstDelayedNext == secondDelayedNext) {
			t.Errorf("unexpected next element: got %d, want 9 or 10, but not %d", secondDelayedNext, firstDelayedNext)
		}
		if !more {
			t.Error("expected more elements")
		}

		// Drop the last element.
		assertFixedRingPush(t, r, 0, false, false)
	})

	t.Run("interrupt a non-full ring", func(t *testing.T) {
		r := &fixedRing[int]{}

		assertFixedRingPush(t, r, 1, true, false)
		assertFixedRingPush(t, r, 2, false, false)
		assertFixedRingPush(t, r, 3, false, false)

		r.die()

		assertFixedRingPush(t, r, 4, false, true)
		assertFixedRingDropPeek(t, r, 2, true, true)
	})

	t.Run("interrupt a full ring", func(t *testing.T) {
		r := &fixedRing[int]{}

		// Push 8 elements.
		for i := 1; i <= 8; i++ {
			assertFixedRingPush(t, r, i, i == 1, false)
		}

		// Push 1 more element.
		delayedPushDone := make(chan struct{})

		go func() {
			defer func() {
				delayedPushDone <- struct{}{}
			}()

			assertFixedRingPush(t, r, 9, false, true)
		}()

		// Interrupt the ring. We expect the waiting goroutine to be released.
		r.die()

		select {
		case <-delayedPushDone:
		case <-time.After(time.Second):
			t.Fatal("timed out waiting for delayed push goroutine to be released")
		}

		assertFixedRingDropPeek(t, r, 2, true, true)
	})
}

func TestUnlimitedRing(t *testing.T) {
	t.Run("push multiple elements and then drop them", func(t *testing.T) {
		r := &unlimitedRing[int]{}

		assertUnlimitedRingPush(t, r, 1, true, false)
		assertUnlimitedRingPush(t, r, 2, false, false)
		assertUnlimitedRingPush(t, r, 3, false, false)

		assertUnlimitedRingDropPeek(t, r, 2, true, false)
		assertUnlimitedRingDropPeek(t, r, 3, true, false)
		assertUnlimitedRingDropPeek(t, r, 0, false, false)
	})

	t.Run("push and drop elements iteratively", func(t *testing.T) {
		r := &unlimitedRing[int]{}

		// Push an initial element.
		assertUnlimitedRingPush(t, r, 1, true, false)

		// Push an element and them drop the previous one, multiple times.
		for i := 2; i < 10; i++ {
			assertUnlimitedRingPush(t, r, i, false, false)
			assertUnlimitedRingDropPeek(t, r, i, true, false)

			if len(r.overflow) > 0 {
				t.Error("unexpected overflow usage")
			}
		}

		// Finally, drop the last element.
		assertUnlimitedRingDropPeek(t, r, 0, false, false)
	})

	t.Run("push elements above the ring capacity and get them stored in the overflow", func(t *testing.T) {
		r := &unlimitedRing[int]{}

		for i := 1; i <= 10; i++ {
			assertUnlimitedRingPush(t, r, i, i == 1, false)
		}

		// We expect the overflow has been used.
		if len(r.overflow) == 0 {
			t.Error("unexpected empty overflow")
		}

		for i := 1; i <= 9; i++ {
			assertUnlimitedRingDropPeek(t, r, i+1, true, false)
		}
		assertUnlimitedRingDropPeek(t, r, 0, false, false)

		// At this point the overflow should have been cleared.
		if len(r.overflow) > 0 {
			t.Error("unexpected overflow usage")
		}
	})

	t.Run("interrupt a non-full ring", func(t *testing.T) {
		r := &unlimitedRing[int]{}

		assertUnlimitedRingPush(t, r, 1, true, false)
		assertUnlimitedRingPush(t, r, 2, false, false)
		assertUnlimitedRingPush(t, r, 3, false, false)

		r.die()

		assertUnlimitedRingPush(t, r, 4, false, true)
		assertUnlimitedRingDropPeek(t, r, 2, true, true)
	})

	t.Run("continuously keeping the items in the ring above the fixed size limit should not grow the overflow slice indefinitely", func(t *testing.T) {
		r := &unlimitedRing[int]{}

		// Push an initial number of elements above the fixed size length.
		for i := 1; i <= 10; i++ {
			assertUnlimitedRingPush(t, r, i, i == 1, false)
		}

		// Now keep pushing and dropping elements continuously.
		for i := 11; i <= 1000; i++ {
			assertUnlimitedRingPush(t, r, i, false, false)
			assertUnlimitedRingDropPeek(t, r, i-9, true, false)
		}

		if cap(r.overflow) > 20 {
			t.Errorf("unexpected high overflow slice capacity, got: %d", cap(r.overflow))
		}
	})

	t.Run("having a temporarily high number of items in the ring should not keep the overflow slice capacity high indefinitely", func(t *testing.T) {
		r := &unlimitedRing[int]{}

		// Push a large number of elements.
		for i := 1; i <= 1000; i++ {
			assertUnlimitedRingPush(t, r, i, i == 1, false)
		}

		if cap(r.overflow) < 1000 {
			t.Errorf("unexpected low overflow slice capacity, got: %d, expected >= 1000", cap(r.overflow))
		}

		// Drop most of them, but keep it above the fixed size limit.
		for i := 1; i <= 990; i++ {
			assertUnlimitedRingDropPeek(t, r, i+1, true, false)
		}

		// Push few more items and then drop all the remaining ones.
		for i := 1001; i <= 1010; i++ {
			assertUnlimitedRingPush(t, r, i, false, false)
		}

		for i := 991; i < 1010; i++ {
			assertUnlimitedRingDropPeek(t, r, i+1, true, false)
		}
		assertUnlimitedRingDropPeek(t, r, 0, false, false)

		if cap(r.overflow) > 100 {
			t.Errorf("unexpected high overflow slice capacity, got: %d", cap(r.overflow))
		}
	})
}

func assertFixedRingPush(t *testing.T, r *fixedRing[int], elem int, expectedFirst, expectedDead bool) {
	t.Helper()

	first, dead := r.push(elem)
	if first != expectedFirst {
		t.Errorf("unexpected first: got %t, want %t", first, expectedFirst)
	}
	if dead != expectedDead {
		t.Errorf("unexpected dead: got %t, want %t", dead, expectedDead)
	}
}

func assertFixedRingDropPeek(t *testing.T, r *fixedRing[int], expectedNext int, expectedMore, expectedDead bool) {
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

func assertUnlimitedRingPush(t *testing.T, r *unlimitedRing[int], elem int, expectedFirst, expectedDead bool) {
	t.Helper()

	first, dead := r.push(elem)
	if first != expectedFirst {
		t.Errorf("unexpected first: got %t, want %t", first, expectedFirst)
	}
	if dead != expectedDead {
		t.Errorf("unexpected dead: got %t, want %t", dead, expectedDead)
	}
}

func assertUnlimitedRingDropPeek(t *testing.T, r *unlimitedRing[int], expectedNext int, expectedMore, expectedDead bool) {
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
