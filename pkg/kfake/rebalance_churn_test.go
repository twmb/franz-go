package kfake_test

// Audit round 2 repros (rebalance-churn.md): broker dies / leader moves
// mid-rebalance. Written fail-pre-fix during the audit; with the fixes in,
// all five pass and serve as regressions:
//
// F1: an OffsetFetch failure after internal retries must tear the 848
//     session down (so the restart re-fetches via g.fetching), not be
//     retried in place by the heartbeat loop's transient arm - that
//     silently stalls the assignment with healthy heartbeats.
// F2: StaleMemberEpoch bubbling out of fetchOffsets must rejoin with the
//     SAME member id (epoch-0 rejoin is the protocol's lost-response
//     recovery); a fresh id strands the old server-side member - and its
//     partitions - until the session timeout.
// F3: the 848/share leave heartbeat (MemberEpoch < 0) must be retried like
//     classic LeaveGroup; a single transient NOT_COORDINATOR must not ghost
//     the member until the session timeout.

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// injectOffsetFetchErr persistently intercepts OffsetFetch and, while
// injecting() is true, answers with the given group-level error code.
// Returns a counter of injected responses.
func injectOffsetFetchErr(c *kfake.Cluster, injecting *atomic.Bool, code int16) *atomic.Int64 {
	var n atomic.Int64
	c.ControlKey(int16(kmsg.OffsetFetch), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if !injecting.Load() {
			return nil, nil, false
		}
		n.Add(1)
		freq := kreq.(*kmsg.OffsetFetchRequest)
		resp := freq.ResponseKind().(*kmsg.OffsetFetchResponse)
		resp.ErrorCode = code // v2-v7 top level
		for _, g := range freq.Groups {
			rg := kmsg.NewOffsetFetchResponseGroup()
			rg.Group = g.Group
			rg.ErrorCode = code // v8+ group level
			resp.Groups = append(resp.Groups, rg)
		}
		return resp, nil, true
	})
	return &n
}

// startConsuming triggers group management with a single short poll. The
// caller has offset-fetch failure injection active, so no records can
// possibly arrive (cursors exist only after a successful OffsetFetch) and
// this cannot steal records from later polls.
func startConsuming(t *testing.T, cl *kgo.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	cl.PollFetches(ctx)
}

// pollRecords polls until n records arrive or the timeout expires, ignoring
// (but logging) fetch errors. Returns the number of records seen.
func pollRecords(t *testing.T, cl *kgo.Client, n int, timeout time.Duration) int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var got int
	for got < n && ctx.Err() == nil {
		fs := cl.PollFetches(ctx)
		fs.EachError(func(topic string, p int32, err error) {
			if ctx.Err() == nil {
				t.Logf("fetch error (continuing): %s/%d: %v", topic, p, err)
			}
		})
		got += fs.NumRecords()
	}
	return got
}

// waitInjections waits until the injection counter either stops increasing
// across a 2.5s window (the client gave up and nothing re-attempts; the
// window must exceed the client's max jittered retry backoff of ~1.2s so a
// mid-cycle backoff gap cannot masquerade as giving up) or exceeds maxBound
// (the client keeps re-attempting across session restarts, which is the
// recovering behavior). Returns the count at that point.
func waitInjections(t *testing.T, n *atomic.Int64, atLeast, maxBound int64, timeout time.Duration) int64 {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last, stableFor int64
	for time.Now().Before(deadline) {
		time.Sleep(250 * time.Millisecond)
		cur := n.Load()
		if cur >= maxBound {
			return cur
		}
		if cur >= atLeast && cur == last {
			stableFor += 250
			if stableFor >= 2500 {
				return cur
			}
		} else {
			stableFor = 0
		}
		last = cur
	}
	t.Fatalf("offset fetch injections never settled: got %d, want >= %d and stable, or >= %d", n.Load(), atLeast, maxBound)
	return 0
}

// F1: a transient coordinator error on OffsetFetch (here NOT_COORDINATOR,
// equally COORDINATOR_LOAD_IN_PROGRESS or a connection death) that outlives
// the client's internal retryTimeout must not strand the assignment: once
// the cluster heals, the member must consume without any external trigger.
func TestAudit848OffsetFetchErrorStallsAssignment(t *testing.T) {
	t.Parallel()
	const topic = "audit-848-stall"
	c := newCluster(t, kfake.SeedTopics(1, topic))

	producer := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, 3)

	var injecting atomic.Bool
	injecting.Store(true)
	n := injectOffsetFetchErr(c, &injecting, kerr.NotCoordinator.Code)

	consumer := newGroupConsumer(t, c, topic, "audit-848-stall-g",
		kgo.RetryTimeout(2*time.Second),
		kgo.RequestRetries(3),
	)
	startConsuming(t, consumer)

	// Wait for at least one full internal OffsetFetch retry cycle to give
	// up, then clear the failure: from here on, every request the client
	// makes succeeds.
	waitInjections(t, n, 2, 10, 15*time.Second)
	injecting.Store(false)

	// Correct behavior: with the coordinator healthy again, the member
	// eventually fetches offsets for its assignment and consumes, without
	// any external trigger (the classic protocol does: see the sibling
	// test).
	if got := pollRecords(t, consumer, 3, 6*time.Second); got >= 3 {
		return // fixed
	}

	// Demonstrate the mechanism: a session bounce re-runs fetchOffsets via
	// g.fetching and everything flows. The member was alive and heartbeating
	// the whole time - the stall was silent.
	consumer.ForceRebalance()
	if got := pollRecords(t, consumer, 3, 8*time.Second); got >= 3 {
		t.Fatalf("BUG REPRODUCED: 848 member sat assigned-but-stalled with healthy heartbeats after a transient OffsetFetch failure; records arrived only after ForceRebalance bounced the session")
	}
	t.Fatalf("records never arrived even after a forced rebalance; unexpected secondary failure")
}

// Classic control for F1: the same OffsetFetch failure tears the classic
// session down (onLost), the manage loop backs off and rejoins, and the
// rejoin re-fetches offsets: consumption recovers on its own.
func TestAuditClassicOffsetFetchErrorRecovers(t *testing.T) {
	t.Parallel()
	const topic = "audit-classic-recover"
	c := newCluster(t, kfake.SeedTopics(1, topic))

	producer := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, 3)

	var injecting atomic.Bool
	injecting.Store(true)
	n := injectOffsetFetchErr(c, &injecting, kerr.NotCoordinator.Code)

	consumer := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup("audit-classic-recover-g"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.RetryTimeout(2*time.Second),
		kgo.RequestRetries(3),
	)
	startConsuming(t, consumer)

	waitInjections(t, n, 2, 10, 15*time.Second)
	injecting.Store(false)

	if got := pollRecords(t, consumer, 3, 20*time.Second); got < 3 {
		t.Fatalf("classic consumer did not recover after transient OffsetFetch failure: got %d/3 records", got)
	}
}

// F2: STALE_MEMBER_EPOCH bubbling out of fetchOffsets (after its 10
// forced-heartbeat retries) resets the member. The reset must keep the
// member id - the server still has this member, and an epoch-0 same-id
// rejoin re-admits it in place - so the group must end up with exactly one
// member. A fresh id strands the old incarnation, parking its partitions
// until the session timeout.
func TestAudit848StaleEpochRejoinStrandsOldMember(t *testing.T) {
	t.Parallel()
	const topic = "audit-848-stale"
	const group = "audit-848-stale-g"
	c := newCluster(t, kfake.SeedTopics(2, topic))

	producer := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, 4)

	var injecting atomic.Bool
	injecting.Store(true)
	n := injectOffsetFetchErr(c, &injecting, kerr.StaleMemberEpoch.Code)

	consumer := newGroupConsumer(t, c, topic, group)
	startConsuming(t, consumer)

	// fetchOffsets retries STALE 10 times (each forcing a heartbeat)
	// before giving up, so the 11th injected response unconditionally
	// triggers the manage loop's reset-and-rejoin; heal the cluster then.
	// We cannot wait for the rejoined incarnation's own fetch attempt:
	// with the fresh-id bug, the ghost incarnation holds every partition,
	// so the rejoined member receives an EMPTY assignment and never
	// fetches offsets at all.
	deadline := time.Now().Add(20 * time.Second)
	for n.Load() < 11 && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}
	if n.Load() < 11 {
		t.Fatalf("STALE_MEMBER_EPOCH reset path not reached: %d injections", n.Load())
	}
	injecting.Store(false)

	// Let the rejoined incarnation settle, then count members. Correct
	// behavior: exactly one member. Buggy behavior: the pre-reset
	// incarnation ghosts in the group until the session timeout.
	adm := kadm.NewClient(newPlainClient(t, c))
	var dg kadm.DescribedConsumerGroup
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		described, err := adm.DescribeConsumerGroups(context.Background(), group)
		if err == nil {
			dg = described[group]
			cur, _ := consumer.GroupMetadata()
			var hasCurrent bool
			for _, m := range dg.Members {
				if m.MemberID == cur {
					hasCurrent = true
				}
			}
			if hasCurrent && dg.State == "Stable" {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if len(dg.Members) != 1 {
		var ids []string
		for _, m := range dg.Members {
			ids = append(ids, m.MemberID)
		}
		t.Fatalf("BUG REPRODUCED: group has %d members %v for a single client; abandoned incarnation(s) park their partitions until the session timeout", len(dg.Members), ids)
	}

	// End to end: the surviving member owns everything and consumes.
	if got := pollRecords(t, consumer, 4, 10*time.Second); got < 4 {
		t.Fatalf("rejoined member did not consume its assignment: got %d/4 records", got)
	}
}

// F3: the 848 leave (ConsumerGroupHeartbeat with MemberEpoch=-1) must
// survive a transient NOT_COORDINATOR by retrying, like classic LeaveGroup.
func TestAudit848LeaveGroupNotRetried(t *testing.T) {
	t.Parallel()
	const topic = "audit-848-leave"
	c := newCluster(t, kfake.SeedTopics(1, topic))

	producer := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, 1)

	consumer := newGroupConsumer(t, c, topic, "audit-848-leave-g")
	consumeN(t, consumer, 1, 10*time.Second) // fully joined

	var injected atomic.Int64
	c.ControlKey(int16(kmsg.ConsumerGroupHeartbeat), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		hreq := kreq.(*kmsg.ConsumerGroupHeartbeatRequest)
		if hreq.MemberEpoch >= 0 || injected.Load() > 0 {
			return nil, nil, false
		}
		injected.Add(1)
		resp := hreq.ResponseKind().(*kmsg.ConsumerGroupHeartbeatResponse)
		resp.ErrorCode = kerr.NotCoordinator.Code
		return resp, nil, true
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := consumer.LeaveGroupContext(ctx); err != nil {
		t.Fatalf("BUG REPRODUCED: one transient NOT_COORDINATOR lost the 848 leave entirely (no retry): %v; member ghosts until the session timeout", err)
	}
	if injected.Load() == 0 {
		t.Fatal("test setup issue: leave heartbeat was never intercepted")
	}
}

// F3 corollary: because the leave is retried, a retry can find the member
// already gone (a prior attempt succeeded but its response was lost, or the
// session expired first). UNKNOWN_MEMBER_ID on a leave is the goal state of
// leaving and must surface as success, not as an error.
func TestAudit848LeaveUnknownMemberIsSuccess(t *testing.T) {
	t.Parallel()
	const topic = "audit-848-leave-unknown"
	c := newCluster(t, kfake.SeedTopics(1, topic))

	producer := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, 1)

	consumer := newGroupConsumer(t, c, topic, "audit-848-leave-unknown-g")
	consumeN(t, consumer, 1, 10*time.Second) // fully joined

	c.ControlKey(int16(kmsg.ConsumerGroupHeartbeat), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		hreq := kreq.(*kmsg.ConsumerGroupHeartbeatRequest)
		if hreq.MemberEpoch >= 0 {
			return nil, nil, false
		}
		resp := hreq.ResponseKind().(*kmsg.ConsumerGroupHeartbeatResponse)
		resp.ErrorCode = kerr.UnknownMemberID.Code
		return resp, nil, true
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := consumer.LeaveGroupContext(ctx); err != nil {
		t.Fatalf("a leave finding the member already gone must report success, got: %v", err)
	}
}

// Classic control for F3: LeaveGroup rides the coordinator request wrapper,
// which deletes the stale cached coordinator and retries.
func TestAuditClassicLeaveGroupRetried(t *testing.T) {
	t.Parallel()
	const topic = "audit-classic-leave"
	c := newCluster(t, kfake.SeedTopics(1, topic))

	producer := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, 1)

	consumer := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup("audit-classic-leave-g"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	consumeN(t, consumer, 1, 10*time.Second)

	var attempts atomic.Int64
	c.ControlKey(int16(kmsg.LeaveGroup), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if attempts.Add(1) > 1 {
			return nil, nil, false
		}
		resp := kreq.(*kmsg.LeaveGroupRequest).ResponseKind().(*kmsg.LeaveGroupResponse)
		resp.ErrorCode = kerr.NotCoordinator.Code
		return resp, nil, true
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := consumer.LeaveGroupContext(ctx); err != nil {
		t.Fatalf("classic leave did not survive one transient NOT_COORDINATOR: %v", err)
	}
	if attempts.Load() < 2 {
		t.Fatalf("classic leave was not retried: %d attempts", attempts.Load())
	}
}
