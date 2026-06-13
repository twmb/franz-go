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

// Empty topic names and negative partitions are never valid; today they spin
// on UNKNOWN_TOPIC metadata (or an eternal load) with no surfaced error, so we
// reject them at construction. An empty string in regex mode is a valid
// match-all pattern and is left alone.
func TestConfigRejectsEmptyTopicNames(t *testing.T) {
	bad := []struct {
		name string
		opts []Opt
	}{
		{"empty ConsumeTopics name", []Opt{ConsumeTopics("ok", "")}},
		{"empty ConsumePartitions topic", []Opt{ConsumePartitions(map[string]map[int32]Offset{"": {0: NewOffset().AtStart()}})}},
		{"negative ConsumePartitions partition", []Opt{ConsumePartitions(map[string]map[int32]Offset{"t": {-1: NewOffset().AtStart()}})}},
	}
	for _, b := range bad {
		if _, _, err := validateCfg(b.opts...); err == nil {
			t.Errorf("%s: expected an error, got nil", b.name)
		}
	}

	good := []struct {
		name string
		opts []Opt
	}{
		{"valid ConsumeTopics", []Opt{ConsumeTopics("a", "b")}},
		{"valid ConsumePartitions", []Opt{ConsumePartitions(map[string]map[int32]Offset{"t": {0: NewOffset().AtStart()}})}},
		{"empty regex is match-all", []Opt{ConsumeRegex(), ConsumeTopics("")}},
	}
	for _, g := range good {
		if _, _, err := validateCfg(g.opts...); err != nil {
			t.Errorf("%s: unexpected error: %v", g.name, err)
		}
	}
}

// TransactionalID and InstanceID take a value; an explicit "" is a mistake and
// is distinguishable from unset (both are *string internally).
func TestConfigRejectsEmptyIDs(t *testing.T) {
	if _, _, err := validateCfg(TransactionalID("")); err == nil {
		t.Error("expected an error for empty TransactionalID, got nil")
	}
	if _, _, err := validateCfg(ConsumerGroup("g"), InstanceID("")); err == nil {
		t.Error("expected an error for empty InstanceID, got nil")
	}
	if _, _, err := validateCfg(TransactionalID("tx")); err != nil {
		t.Errorf("unexpected error for valid TransactionalID: %v", err)
	}
	if _, _, err := validateCfg(ConsumerGroup("g"), InstanceID("inst")); err != nil {
		t.Errorf("unexpected error for valid InstanceID: %v", err)
	}
}
