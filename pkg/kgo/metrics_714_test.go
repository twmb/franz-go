package kgo

import (
	"bytes"
	"testing"
	"time"
)

// A rate is events-per-second over the aggregation window. The prior code
// divided the period's event count by tot-lastTot, which equals the count
// itself, so every rate collapsed to a constant ~1.0.
func TestMetricRateRollNumsRate(t *testing.T) {
	var r metricRate
	for range 10 {
		r.observe()
	}
	rate, tot, lastTot := r.rollNums(2 * time.Second) // 10 events / 2s = 5/s
	if rate != 5.0 {
		t.Errorf("rate: got %v != exp 5.0", rate)
	}
	if tot != 10 || lastTot != 0 {
		t.Errorf("tot/lastTot: got %d/%d != exp 10/0", tot, lastTot)
	}

	for range 6 {
		r.observe()
	}
	rate, tot, lastTot = r.rollNums(3 * time.Second) // 6 events / 3s = 2/s
	if rate != 2.0 {
		t.Errorf("rate2: got %v != exp 2.0", rate)
	}
	if tot != 16 || lastTot != 10 {
		t.Errorf("tot2/lastTot2: got %d/%d != exp 16/10", tot, lastTot)
	}
}

// An avg is the mean observation (sum/count). The prior code divided the sum
// of observations by the aggregation window duration in millis, which is not
// the number of observations.
func TestMetricTimeRollNumsAvg(t *testing.T) {
	var m metricTime
	for _, v := range []int64{10, 20, 30} {
		m.observe(v)
	}
	avg, max := m.rollNums() // (10+20+30)/3 = 20
	if avg != 20.0 {
		t.Errorf("avg: got %v != exp 20.0", avg)
	}
	if max != 30 {
		t.Errorf("max: got %d != exp 30", max)
	}

	// After a roll with no observations, both are zero (and zero gauges
	// are elided when serializing, so no NaN is ever produced).
	avg, max = m.rollNums()
	if avg != 0 || max != 0 {
		t.Errorf("post-roll: got %v/%d != exp 0/0", avg, max)
	}
}

// An attribute value of an unsupported type must be skipped entirely. The
// prior code wrote the field tag before checking the type and then continued
// the loop on an unsupported type, leaving a dangling tag with no
// length+payload that corrupts the entire serialized protobuf.
func TestAppendOtelAttributesSkipsUnsupported(t *testing.T) {
	// The lone attribute is unsupported, so nothing should be written.
	if got := appendOtelAttributesTo(nil, 7, map[string]any{"bad": struct{}{}}); len(got) != 0 {
		t.Errorf("all-unsupported attrs: got %d bytes (%x), want 0", len(got), got)
	}

	// A supported attribute alongside an unsupported one must serialize
	// identically to the supported attribute alone. Map iteration order is
	// irrelevant because the unsupported attribute is fully skipped.
	good := appendOtelAttributesTo(nil, 7, map[string]any{"k": "v"})
	if len(good) == 0 {
		t.Fatal("supported attr produced no bytes")
	}
	mixed := appendOtelAttributesTo(nil, 7, map[string]any{"k": "v", "bad": []string{"x"}})
	if !bytes.Equal(good, mixed) {
		t.Errorf("supported attr corrupted by sibling unsupported attr:\n good  %x\n mixed %x", good, mixed)
	}
}
