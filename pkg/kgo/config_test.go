package kgo

import (
	"strings"
	"testing"
	"time"
)

// A heartbeat interval larger than the session timeout must be rejected at
// construction: the member would heartbeat too slowly and be evicted from the
// group every session. The prior check multiplied the session/rebalance
// timeout (already a time.Duration in nanoseconds) by time.Millisecond,
// inflating the allowed bound by ~1e6 so the check never fired; it also
// compared against the rebalance timeout while the error message - and Kafka's
// actual constraint - reference the session timeout.
func TestConfigHeartbeatExceedsSessionTimeout(t *testing.T) {
	_, _, err := validateCfg(
		ConsumerGroup("g"),
		SessionTimeout(10*time.Second),
		HeartbeatInterval(20*time.Second),
	)
	if err == nil {
		t.Fatal("expected error for heartbeat interval larger than session timeout, got nil")
	}
	if !strings.Contains(err.Error(), "heartbeat interval") {
		t.Errorf("unexpected error %q, want a heartbeat-interval error", err)
	}

	// A heartbeat well within the session timeout validates.
	if _, _, err := validateCfg(
		ConsumerGroup("g"),
		SessionTimeout(10*time.Second),
		HeartbeatInterval(3*time.Second),
	); err != nil {
		t.Fatalf("unexpected error for valid heartbeat/session timeouts: %v", err)
	}
}
