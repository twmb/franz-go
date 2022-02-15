package kversion

import (
	"math"
	"testing"
)

func TestSetMaxKeyVersion(t *testing.T) {
	var vs Versions
	for i := int16(0); i < math.MaxInt16; i++ {
		vs.SetMaxKeyVersion(i, i)
	}
	for i, v := range vs.k2v {
		if int16(i) != v {
			t.Errorf("set incorrect: at %d got %d != exp %d", i, v, i)
		}
	}
}

func TestVersionGuess(t *testing.T) {
	// Cases where last can be empty.
	{
		v := V0_8_0()
		if got, exp := v.VersionGuess(), "v0.8.0"; got != exp {
			t.Errorf("got %s != exp %s without modifications", got, exp)
		}
		v.SetMaxKeyVersion(0, -1)
		if got, exp := v.VersionGuess(), "not even v0.8.0"; got != exp {
			t.Errorf("got %s != exp %s unsetting produce", got, exp)
		}
		v.SetMaxKeyVersion(0, 100)
		if got, exp := v.VersionGuess(), "unknown custom version at least v0.8.0"; got != exp {
			t.Errorf("got %s != exp %s maxing produce", got, exp)
		}
		v.SetMaxKeyVersion(1, -1)
		if got, exp := v.VersionGuess(), "unknown custom version"; got != exp {
			t.Errorf("got %s != exp %s maxing produce and unsetting fetch", got, exp)
		}
	}

	// In between and into the next version.
	{
		v := V0_9_0()
		if got, exp := v.VersionGuess(), "v0.9.0"; got != exp {
			t.Errorf("got %s != exp %s without modifications", got, exp)
		}
		v.SetMaxKeyVersion(17, 0)
		if got, exp := v.VersionGuess(), "between v0.9.0 and v0.10.0"; got != exp {
			t.Errorf("got %s != exp %s setting sasl handshake to 0", got, exp)
		}
		v.SetMaxKeyVersion(0, 2)
		v.SetMaxKeyVersion(1, 2)
		v.SetMaxKeyVersion(3, 1)
		v.SetMaxKeyVersion(6, 2)
		v.SetMaxKeyVersion(18, 0)
		if got, exp := v.VersionGuess(), "v0.10.0"; got != exp {
			t.Errorf("got %s != exp %s setting api versions to 0", got, exp)
		}
	}

	// This hits the case where versions are -1.
	{
		v := V2_7_0()
		v.SetMaxKeyVersion(int16(len(v.k2v)+1), -1)
		if got, exp := v.VersionGuess(), "v2.7"; got != exp {
			t.Errorf("got %s != exp %s without modifications", got, exp)
		}
	}

	{
		v := Tip()
		v.SetMaxKeyVersion(0, 999)
		if got, exp := v.VersionGuess(), "at least v3.1"; got != exp {
			t.Errorf("got %s != exp %s without modifications", got, exp)
		}
	}

	{ // Here, we ensure we skip 4, 5, 6, and 7 by default.
		v := V2_7_0()
		v.SetMaxKeyVersion(4, -1)
		v.SetMaxKeyVersion(5, -1)
		v.SetMaxKeyVersion(6, -1)
		v.SetMaxKeyVersion(7, -1)

		if got, exp := v.VersionGuess(), "v2.7"; got != exp {
			t.Errorf("got %s != exp %s for v2.7 with 4,5,6,7 unset", got, exp)
		}

		if got, exp := v.VersionGuess(SkipKeys()), "unknown custom version"; got != exp {
			t.Errorf("got %s != exp %s for v2.7 with 4,5,6,7 unset without skipping them", got, exp)
		}
	}
}

func TestEqual(t *testing.T) {
	l := V2_7_0()
	l.SetMaxKeyVersion(int16(len(l.k2v)+1), -1)

	r := V2_7_0()

	if !l.Equal(r) {
		t.Errorf("unexpectedly not equal")
	}

	l.SetMaxKeyVersion(0, -1)
	if l.Equal(r) {
		t.Errorf("unexpectedly equal after unsetting produce in left")
	}

	r.SetMaxKeyVersion(0, -1)
	if !l.Equal(r) {
		t.Errorf("unexpectedly not equal after unsetting produce in both")
	}

	l = V0_8_0()
	r = V0_8_1()
	if l.Equal(r) {
		t.Errorf("unexpectedly equal v0.8.0 to v0.8.1")
	}

	r.SetMaxKeyVersion(8, -1)
	r.SetMaxKeyVersion(9, -1)
	if !l.Equal(r) {
		t.Errorf("unexpectedly not equal after backing v0.8.1 down to v0.8.0")
	}
	if !r.Equal(l) {
		t.Errorf("unexpectedly not equal after backing v0.8.1 down to v0.8.0, opposite direction")
	}
}
