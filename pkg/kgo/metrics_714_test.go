package kgo

import (
	"bytes"
	"encoding/binary"
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

// A broker that advertises a non-positive PushIntervalMillis with an empty
// RequestedMetrics list would, without validation, drive the no-requested-
// metrics re-get arm to time on time.Duration(<=0)*time.Millisecond - a timer
// that fires immediately - re-issuing GetTelemetrySubscriptions at round-trip
// pace forever (the push loop already floors at max(..., time.Second), but the
// re-get arm did not). validatePushIntervalMillis substitutes Kafka's default,
// matching the Java client.
func TestValidatePushIntervalMillis(t *testing.T) {
	for _, advertised := range []int32{0, -1, -1000, -1 << 31} {
		if got := validatePushIntervalMillis(advertised); got != defaultPushIntervalMillis {
			t.Errorf("advertised=%d: got %d, want default %d", advertised, got, defaultPushIntervalMillis)
		}
		// The substituted interval must produce a non-immediate timer in the
		// re-get arm.
		if wait := time.Duration(validatePushIntervalMillis(advertised)) * time.Millisecond; wait <= 0 {
			t.Errorf("advertised=%d: substituted wait %v is non-positive (would hot-loop)", advertised, wait)
		}
	}
	for _, advertised := range []int32{1, 1000, 30000, 1 << 30} {
		if got := validatePushIntervalMillis(advertised); got != advertised {
			t.Errorf("advertised=%d: got %d, want unchanged", advertised, got)
		}
	}
}

// protoFields parses a flat protobuf message into (fieldNumber, wireType,
// payload) tuples. For length-delimited fields the payload is the inner bytes;
// for varint fields it is the raw varint bytes; other wire types are returned
// as their fixed-width bytes. It is intentionally minimal: enough to walk the
// OTLP nesting in tests without pulling in a protobuf dependency.
func protoFields(t *testing.T, b []byte) []struct {
	num     int
	wire    int
	payload []byte
} {
	t.Helper()
	var out []struct {
		num     int
		wire    int
		payload []byte
	}
	for len(b) > 0 {
		tag, n := binary.Uvarint(b)
		if n <= 0 {
			t.Fatalf("bad tag varint at %x", b)
		}
		b = b[n:]
		num, wire := int(tag>>3), int(tag&0x7)
		var payload []byte
		switch wire {
		case 0: // varint
			_, n := binary.Uvarint(b)
			if n <= 0 {
				t.Fatalf("bad varint payload at %x", b)
			}
			payload, b = b[:n], b[n:]
		case 1: // 64-bit
			payload, b = b[:8], b[8:]
		case 2: // length-delimited
			l, n := binary.Uvarint(b)
			if n <= 0 {
				t.Fatalf("bad length varint at %x", b)
			}
			b = b[n:]
			payload, b = b[:l], b[l:]
		case 5: // 32-bit
			payload, b = b[:4], b[4:]
		default:
			t.Fatalf("unsupported wire type %d", wire)
		}
		out = append(out, struct {
			num     int
			wire    int
			payload []byte
		}{num, wire, payload})
	}
	return out
}

// field returns the payload of the first field with the given number/wire, or
// nil if not present.
func protoField(t *testing.T, b []byte, num, wire int) []byte {
	t.Helper()
	for _, f := range protoFields(t, b) {
		if f.num == num && f.wire == wire {
			return f.payload
		}
	}
	return nil
}

// A cumulative or delta sum emitted for a ".total" counter (e.g.
// connection.creation.total) is a monotonically increasing counter; OTLP's
// Sum.is_monotonic (field 3, bool) declares that semantic so downstream
// collectors treat it as a counter rather than an up-down gauge. appendSum
// built the otelSum but never set isMonotonic, so the field was never
// serialized even though otelSum.appendTo was ready to emit it and the struct
// comment promised "We always set isMonotonic to true." Java sets
// setIsMonotonic(monotonic) for these counters.
func TestAppendSumIsMonotonic(t *testing.T) {
	for _, delta := range []bool{false, true} {
		cl := &Client{}
		m := &cl.metrics
		m.init(cl)
		m.cl = cl

		// One connection-creation observation yields a non-zero
		// pConnCreation total, so appendSum emits a ".total" sum.
		m.pConnCreation.observe()

		serialized, _, n := m.appendTo(nil, delta, 1<<30, func(string) bool { return true }, nil)
		if n == 0 {
			t.Fatalf("delta=%v: no metrics serialized", delta)
		}

		// Walk: MetricsData(1) -> ResourceMetrics(1) -> ScopeMetrics(2) ->
		// Metric(2, repeated). Find the Metric whose name ends in ".total"
		// and assert its Sum(7) carries is_monotonic(3)==true.
		rm := protoField(t, serialized, 1, 2)
		if rm == nil {
			t.Fatalf("delta=%v: no resourceMetrics", delta)
		}
		sm := protoField(t, rm, 2, 2)
		if sm == nil {
			t.Fatalf("delta=%v: no scopeMetrics", delta)
		}
		var sawTotalSum bool
		for _, f := range protoFields(t, sm) {
			if f.num != 2 || f.wire != 2 { // Metric (repeated)
				continue
			}
			metric := f.payload
			name := string(protoField(t, metric, 1, 2))
			sum := protoField(t, metric, 7, 2) // Sum
			if sum == nil {
				continue
			}
			sawTotalSum = true
			mono := protoField(t, sum, 3, 0) // is_monotonic varint
			if len(mono) == 0 {
				t.Errorf("delta=%v: sum %q missing is_monotonic", delta, name)
				continue
			}
			v, _ := binary.Uvarint(mono)
			if v != 1 {
				t.Errorf("delta=%v: sum %q is_monotonic=%d, want 1", delta, name, v)
			}
		}
		if !sawTotalSum {
			t.Fatalf("delta=%v: no sum metric found in serialized output", delta)
		}
	}
}
