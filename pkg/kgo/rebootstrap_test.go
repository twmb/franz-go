package kgo

import (
	"testing"
	"time"
)

// TestRebootstrapTriggerOption verifies the RebootstrapTrigger option is wired
// into the client config.
func TestRebootstrapTriggerOption(t *testing.T) {
	t.Parallel()

	cl, err := NewClient(
		SeedBrokers("127.0.0.1:9092"),
		RebootstrapTrigger(42*time.Second),
	)
	if err != nil {
		t.Fatalf("unable to create client: %v", err)
	}
	defer cl.Close()

	if got := cl.cfg.rebootstrapTrigger; got != 42*time.Second {
		t.Errorf("rebootstrapTrigger = %v, want %v", got, 42*time.Second)
	}
}

// TestRebootstrapUsesOnRebootstrapRequiredSeeds verifies that, when a callback
// is configured, rebootstrap re-seeds the client with the addresses it returns.
func TestRebootstrapUsesOnRebootstrapRequiredSeeds(t *testing.T) {
	t.Parallel()

	cl, err := NewClient(
		SeedBrokers("127.0.0.1:9092"),
		OnRebootstrapRequired(func() ([]string, error) {
			return []string{"127.0.0.10:9092", "127.0.0.11:9092"}, nil
		}),
	)
	if err != nil {
		t.Fatalf("unable to create client: %v", err)
	}
	defer cl.Close()

	if err := cl.rebootstrap(); err != nil {
		t.Fatalf("rebootstrap: %v", err)
	}

	got := seedHosts(cl)
	want := []string{"127.0.0.10", "127.0.0.11"}
	if len(got) != len(want) {
		t.Fatalf("after rebootstrap got %d seeds (%v), want %d (%v)", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("seed %d host = %q, want %q", i, got[i], want[i])
		}
	}
}

// TestRebootstrapReusesConfiguredSeeds verifies that, with no callback,
// rebootstrap re-resolves the originally configured seed brokers.
func TestRebootstrapReusesConfiguredSeeds(t *testing.T) {
	t.Parallel()

	cl, err := NewClient(SeedBrokers("127.0.0.1:9092", "127.0.0.2:9092"))
	if err != nil {
		t.Fatalf("unable to create client: %v", err)
	}
	defer cl.Close()

	if err := cl.rebootstrap(); err != nil {
		t.Fatalf("rebootstrap: %v", err)
	}

	got := seedHosts(cl)
	want := []string{"127.0.0.1", "127.0.0.2"}
	if len(got) != len(want) {
		t.Fatalf("after rebootstrap got %d seeds (%v), want %d (%v)", len(got), got, len(want), want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("seed %d host = %q, want %q", i, got[i], want[i])
		}
	}
}

func seedHosts(cl *Client) []string {
	seeds := cl.loadSeeds()
	hosts := make([]string, len(seeds))
	for i, s := range seeds {
		hosts[i] = s.meta.Host
	}
	return hosts
}
