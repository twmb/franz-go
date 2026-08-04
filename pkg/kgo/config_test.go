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

// SessionTimeout, RebalanceTimeout, ProduceRequestTimeout, and
// TransactionTimeout are all serialized to int32-millisecond wire fields. A
// Duration whose millisecond value exceeds an int32 silently overflows that
// field when cast: a 30 day timeout wraps to a negative wire value, and a ~50
// day one wraps to a small positive value the broker quietly accepts as a
// completely different timeout. These must be rejected at construction rather
// than corrupted on the wire.
func TestConfigRejectsInt32MillisOverflow(t *testing.T) {
	// 30 days in milliseconds (2,592,000,000) exceeds math.MaxInt32
	// (2,147,483,647); int32(...) wraps it to a negative value.
	overflow := 30 * 24 * time.Hour
	// 50 days wraps to a small positive int32, the silently-wrong case.
	overflowPositive := 50 * 24 * time.Hour

	bad := []struct {
		name string
		opts []Opt
	}{
		{"session timeout overflow", []Opt{ConsumerGroup("g"), SessionTimeout(overflow), RebalanceTimeout(time.Minute), HeartbeatInterval(time.Second)}},
		{"session timeout overflow positive", []Opt{ConsumerGroup("g"), SessionTimeout(overflowPositive), RebalanceTimeout(time.Minute), HeartbeatInterval(time.Second)}},
		{"rebalance timeout overflow", []Opt{ConsumerGroup("g"), RebalanceTimeout(overflow)}},
		{"produce timeout overflow", []Opt{ProduceRequestTimeout(overflow)}},
		{"transaction timeout overflow", []Opt{TransactionalID("tx"), TransactionTimeout(overflow)}},
		{"transaction timeout overflow positive", []Opt{TransactionalID("tx"), TransactionTimeout(overflowPositive)}},
	}
	for _, b := range bad {
		if _, _, err := validateCfg(b.opts...); err == nil {
			t.Errorf("%s: expected an error, got nil", b.name)
		}
	}

	// Values that fit comfortably in the int32-millisecond wire field still
	// validate.
	good := []struct {
		name string
		opts []Opt
	}{
		{"large but fitting session timeout", []Opt{ConsumerGroup("g"), SessionTimeout(20 * 24 * time.Hour), RebalanceTimeout(20 * 24 * time.Hour), HeartbeatInterval(time.Hour)}},
		{"large but fitting produce timeout", []Opt{ProduceRequestTimeout(20 * 24 * time.Hour)}},
		{"large but fitting transaction timeout", []Opt{TransactionalID("tx"), TransactionTimeout(20 * 24 * time.Hour)}},
	}
	for _, g := range good {
		if _, _, err := validateCfg(g.opts...); err != nil {
			t.Errorf("%s: unexpected error: %v", g.name, err)
		}
	}
}

// A rack says where the client is, which is what brokers need to serve a
// closer replica. Chasing rack locality when assigning partitions is a
// separate decision with its own cost, so it takes its own opt in -- unlike
// Kafka's client, which turns it on whenever a rack is set.
func TestConfigBalanceRacksIsOptIn(t *testing.T) {
	cfg, _, err := validateCfg(ConsumerGroup("g"), Rack("az-1"))
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.rack != "az-1" {
		t.Errorf("rack is %q, want az-1", cfg.rack)
	}
	if cfg.balanceRacks {
		t.Error("a rack alone opted the group into rack aware balancing")
	}

	cfg, _, err = validateCfg(ConsumerGroup("g"), Rack("az-1"), BalanceRacks())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !cfg.balanceRacks {
		t.Error("BalanceRacks did not opt the group into rack aware balancing")
	}
}
