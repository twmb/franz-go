package kgo

import (
	"math"
	"testing"
	"time"
)

func TestLookbackClampsNegative(t *testing.T) {
	t.Parallel()
	if o := NewOffset().Lookback(-time.Hour); o.lookback != 0 {
		t.Errorf("Lookback(-time.Hour) kept %s, want 0", o.lookback)
	}
}

func TestLookbackMilli(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name  string
		milli int64
		d     time.Duration
		want  int64
	}{
		{"no lookback", 1000000, 0, 1000000},
		{"subtracts", 1000000, time.Second, 999000},
		{"onto zero", 1000, time.Second, 1},
		{"into the reserved range", 500, time.Second, 1},
		{"a record below the epoch", -5000, time.Second, 1},
		{"a subtraction that wraps", math.MinInt64 + 5, 24 * time.Hour, 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := lookbackMilli(test.milli, test.d); got != test.want {
				t.Errorf("lookbackMilli(%d, %s) = %d, want %d", test.milli, test.d, got, test.want)
			}
		})
	}
}
