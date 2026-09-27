package kfake

import (
	"slices"
	"sync/atomic"
	"time"

	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

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
//
// The purged topic's offsets must also be committed before a heartbeat
// stops reporting it as owned: once the broker sees it released, a member
// that still subscribes to it can start from older offsets.
func TestAudit848PurgeReconcilesViaHeartbeat(t *testing.T) {
	t.Parallel()
	const (
		keep  = "a848-purge-keep"
		drop  = "a848-purge-drop"
		group = "a848-purge-g"
	)
	c := newCluster(t, NumBrokers(1), SeedTopics(1, keep, drop))
	producer := newClient848(t, c)
	produceNStrings(t, producer, keep, 3)
	produceNStrings(t, producer, drop, 3)
	dropID := c.TopicInfo(drop).TopicID

	cl := newClient848(t, c,
		kgo.ConsumeTopics(keep, drop),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.GreedyAutoCommit(), // the revoke commits everything polled
		kgo.AutoCommitInterval(time.Hour),
	)
	consumeN(t, cl, 6, 10*time.Second) // drain both topics' initial records

	// The first heartbeat that no longer reports the dropped topic as owned
	// releases it; by then its offsets must have been committed.
	commits := c.Fault(Fault{
		Keys:    []kmsg.Key{kmsg.OffsetCommit},
		Topic:   drop,
		Observe: true,
		Count:   -1,
	})
	var released, commitFirst atomic.Bool
	release := c.Fault(Fault{
		Keys:    []kmsg.Key{kmsg.ConsumerGroupHeartbeat},
		Observe: true,
		Count:   -1,
		When: func(kreq kmsg.Request) bool {
			req := kreq.(*kmsg.ConsumerGroupHeartbeatRequest)
			if req.MemberEpoch <= 0 || req.Topics == nil || slices.ContainsFunc(req.Topics, func(t kmsg.ConsumerGroupHeartbeatRequestTopic) bool {
				return t.TopicID == dropID
			}) {
				return false
			}
			if released.CompareAndSwap(false, true) {
				commitFirst.Store(commits.Hits() > 0)
			}
			return true
		},
	})
	cl.PurgeTopicsFromConsuming(drop)

	// The kept topic must keep flowing after the purge reconciles; the
	// dropped topic must not reappear.
	produceNStrings(t, producer, keep, 3)
	for _, r := range consumeN(t, cl, 3, 10*time.Second) {
		if r.Topic != keep {
			t.Fatalf("consumed from %q after PurgeTopicsFromConsuming(%q); expected only %q", r.Topic, drop, keep)
		}
	}

	waitHits(t, release, 1, "purged topic never released")
	if !commitFirst.Load() {
		t.Fatal("purged topic released before its offsets were committed")
	}
}
