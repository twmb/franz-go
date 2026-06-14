package kfake_test

import (
	"time"

	"testing"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// 848 re-sweep (consumer_group_848.go re-attack with patterns 16-49).
//
// PurgeTopicsFromConsuming must reconcile a next-gen group through the
// heartbeat path, never by feeding rejoinCh. A rejoinCh bounce in 848 mode
// runs the session-end revoke's nowAssigned read-modify-write concurrently
// with live heartbeats - the one interleaving where a completing heartbeat's
// nowAssigned store is lost - which is exactly why ForceRebalance redirects
// to a forced heartbeat. PurgeTopicsFromConsuming was the lone
// subscription-change feeder that still fed rejoinCh unconditionally (the
// guard its siblings findNewAssignments and ForceRebalance carry); the
// dispatch is now centralized in signalSubscriptionChange. The unit test
// TestSignalSubscriptionChange848 in pkg/kgo is the mechanism repro; this is
// the end-to-end guard that purge keeps the group consuming in 848 mode.
func TestAudit848PurgeReconcilesViaHeartbeat(t *testing.T) {
	t.Parallel()
	const (
		keep  = "a848-purge-keep"
		drop  = "a848-purge-drop"
		group = "a848-purge-g"
	)
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, keep, drop))
	producer := newClient848(t, c)
	produceNStrings(t, producer, keep, 3)
	produceNStrings(t, producer, drop, 3)

	cl := newClient848(t, c,
		kgo.ConsumeTopics(keep, drop),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	consumeN(t, cl, 6, 10*time.Second) // drain both topics' initial records

	cl.PurgeTopicsFromConsuming(drop)

	// The kept topic must keep flowing after the purge reconciles; the
	// dropped topic must not reappear.
	produceNStrings(t, producer, keep, 3)
	for _, r := range consumeN(t, cl, 3, 10*time.Second) {
		if r.Topic != keep {
			t.Fatalf("consumed from %q after PurgeTopicsFromConsuming(%q); expected only %q", r.Topic, drop, keep)
		}
	}
}
