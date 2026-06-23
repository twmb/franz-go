package kfake_test

// Audit round 6 repros (848-sweep.md): the KIP-848 client state machine.
// Written fail-pre-fix during the audit; with the fixes in, all pass and
// serve as regressions:
//
// B1: stale unresolvedAssigned (topic IDs the client could not map to
//     names) must be cleared when (re)joining: a join's owned-partitions
//     field must be an empty list, and a real broker rejects every join
//     carrying the leaked state with INVALID_REQUEST - permanently
//     wedging the consumer.
// B2: consecutive transient heartbeat-session restarts must accumulate so
//     the "heartbeat persistently failing" ErrGroupSession notification
//     can fire; resetting the counter on the restart itself makes an
//     unreachable coordinator a silent infinite stall.
// B3: a MaxVersions cap below ConsumerGroupHeartbeat v1 must fall back to
//     the classic protocol; v0 drops SubscribedTopicRegex from the wire,
//     so a regex consumer would join with no subscription and silently
//     consume nothing.
// B5: the regex+excludes fallback subscription must skip internal topics,
//     matching classic regex consuming.
// D1: a negative MemberEpoch in a success response must be ignored, not
//     stored - storing it turns the next heartbeat into an unintended
//     leave (-1).
// D2: assignment partitions must be deduplicated: a duplicated partition
//     re-"adds" an owned partition, re-fetching the committed offset and
//     rewinding a live cursor into duplicate consumption.
// B4: ForceRebalance on a next-gen group forces a heartbeat rather than
//     bouncing the session (guard test: consumption is undisturbed).

import (
	"context"
	"errors"
	"net"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

// metadataHiding persistently intercepts Metadata and answers every request
// with a hand-built response that contains broker 0 and ONLY the given
// topics: any other topic (requested or not) is simply absent, so the
// client can never resolve its ID.
func metadataHiding(c *kfake.Cluster, visible ...string) {
	type vt struct {
		topic string
		id    [16]byte
		parts int32
	}
	var vts []vt
	for _, topic := range visible {
		ti := c.TopicInfo(topic)
		var parts int32
		for c.PartitionInfo(topic, parts) != nil {
			parts++
		}
		vts = append(vts, vt{topic, ti.TopicID, parts})
	}
	host, portStr, _ := net.SplitHostPort(c.ListenAddrs()[0])
	port, _ := strconv.Atoi(portStr)
	c.ControlKey(int16(kmsg.Metadata), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		req := kreq.(*kmsg.MetadataRequest)
		resp := req.ResponseKind().(*kmsg.MetadataResponse)
		b := kmsg.NewMetadataResponseBroker()
		b.NodeID = 0
		b.Host = host
		b.Port = int32(port)
		resp.Brokers = append(resp.Brokers, b)
		resp.ControllerID = 0
		for _, v := range vts {
			st := kmsg.NewMetadataResponseTopic()
			st.Topic = kmsg.StringPtr(v.topic)
			st.TopicID = v.id
			for p := int32(0); p < v.parts; p++ {
				pi := c.PartitionInfo(v.topic, p)
				sp := kmsg.NewMetadataResponseTopicPartition()
				sp.Partition = p
				sp.Leader = pi.Leader
				sp.LeaderEpoch = pi.Epoch
				sp.Replicas = []int32{pi.Leader}
				sp.ISR = []int32{pi.Leader}
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})
}

// B1: an assigned topic ID the client cannot resolve via metadata lives in
// g848.unresolvedAssigned and is folded into the heartbeat's owned Topics.
// When the member is then fenced and rejoins, the join MUST NOT carry that
// stale state: a real broker rejects any (re)join whose owned-partitions
// list is non-empty with INVALID_REQUEST, and since only a successful
// assignment-carrying response clears unresolvedAssigned, every retry of
// the join fails identically - the consumer is dead until process restart.
// Pre-fix this test dies with ErrGroupSession(INVALID_REQUEST) from the
// join loop; post-fix the rejoin succeeds and consumption resumes.
func TestAudit848StaleUnresolvedJoin(t *testing.T) {
	t.Parallel()
	const (
		t1    = "a848-unres-1"
		t2    = "a848-unres-2"
		group = "a848-unres-g"
	)
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, t1, t2))

	producer := newClient848(t, c)
	produceNStrings(t, producer, t1, 3)
	produceNStrings(t, producer, t2, 3)

	// From here on the client can only ever see t1: t2's ID is
	// unresolvable, so the server's assignment of t2 parks in
	// unresolvedAssigned forever.
	metadataHiding(c, t1)

	cl := newClient848(t, c,
		kgo.ConsumeTopics(t1, t2),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.RetryBackoffFn(func(int) time.Duration { return 25 * time.Millisecond }),
	)

	// t1 flows; t2 is assigned but unresolved.
	consumeN(t, cl, 3, 10*time.Second)

	// Fence the member once on a regular heartbeat. The client keeps its
	// member ID and rejoins at epoch 0 - with stale unresolved t2 in its
	// owned Topics pre-fix. The recorder (FIFO before the injector)
	// counts epoch-0 requests so we only assert consumption after the
	// rejoin was actually attempted; without the wait, the pre-fence
	// cursor could deliver the fresh records before the fence lands.
	var joinAttempts atomic.Int64
	c.ControlKey(int16(kmsg.ConsumerGroupHeartbeat), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if kreq.(*kmsg.ConsumerGroupHeartbeatRequest).MemberEpoch == 0 {
			joinAttempts.Add(1)
		}
		return nil, nil, false
	})
	c.ControlKey(int16(kmsg.ConsumerGroupHeartbeat), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		req := kreq.(*kmsg.ConsumerGroupHeartbeatRequest)
		if req.MemberEpoch <= 0 {
			c.KeepControl()
			return nil, nil, false
		}
		resp := req.ResponseKind().(*kmsg.ConsumerGroupHeartbeatResponse)
		resp.ErrorCode = kerr.FencedMemberEpoch.Code
		return resp, nil, true
	})
	waitDeadline := time.Now().Add(5 * time.Second)
	for joinAttempts.Load() == 0 && time.Now().Before(waitDeadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if joinAttempts.Load() == 0 {
		t.Fatal("member was never fenced into rejoining")
	}

	// The rejoin must succeed and t1 must keep consuming. Pre-fix,
	// consumeN fatals on the injected ErrGroupSession(INVALID_REQUEST).
	produceNStrings(t, producer, t1, 3)
	consumeN(t, cl, 3, 10*time.Second)
}

// B2: with the coordinator answering every heartbeat NOT_COORDINATOR, the
// heartbeat session restarts transparently. After cfg.retries consecutive
// restarts with no successful heartbeat, the client must surface an
// ErrGroupSession("...consecutive attempts...") fake fetch so the user
// learns the group is dead. Pre-fix the restart counter was zeroed on the
// same iteration that incremented it, so the notification never fired.
func TestAudit848TransientRestartNotification(t *testing.T) {
	t.Parallel()
	const (
		topic = "a848-restart-t"
		group = "a848-restart-g"
	)
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	producer := newClient848(t, c)
	produceNStrings(t, producer, topic, 3)

	cl := newClient848(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.RequestRetries(2),
		kgo.RetryBackoffFn(func(int) time.Duration { return 10 * time.Millisecond }),
	)
	consumeN(t, cl, 3, 10*time.Second)

	var stopInject atomic.Bool
	defer stopInject.Store(true) // let the leave during cleanup succeed
	c.ControlKey(int16(kmsg.ConsumerGroupHeartbeat), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if stopInject.Load() {
			return nil, nil, false
		}
		req := kreq.(*kmsg.ConsumerGroupHeartbeatRequest)
		resp := req.ResponseKind().(*kmsg.ConsumerGroupHeartbeatResponse)
		resp.ErrorCode = kerr.NotCoordinator.Code
		return resp, nil, true
	})

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		fs := cl.PollFetches(ctx)
		cancel()
		for _, fe := range fs.Errors() {
			var gs *kgo.ErrGroupSession
			if errors.As(fe.Err, &gs) && strings.Contains(fe.Err.Error(), "consecutive attempts") {
				return // notification surfaced
			}
		}
	}
	t.Fatal("heartbeat failed continuously but no ErrGroupSession 'consecutive attempts' notification ever surfaced")
}

// B3: capping MaxVersions below ConsumerGroupHeartbeat v1 must disable the
// next-gen path entirely. v0 has no SubscribedTopicRegex field, so a regex
// consumer's join would carry no subscription at all: the member joins,
// is assigned nothing, and silently consumes nothing. Post-fix the client
// falls back to the classic protocol and consumes.
func TestAudit848MaxVersionsV0FallsBackToClassic(t *testing.T) {
	t.Parallel()
	const (
		topic = "a848-v0-t"
		group = "a848-v0-g"
	)
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	producer := newClient848(t, c)
	produceNStrings(t, producer, topic, 3)

	maxv := kversion.V4_0_0()
	maxv.SetMaxKeyVersion(int16(kmsg.ConsumerGroupHeartbeat), 0)
	cl := newClient848(t, c,
		kgo.MaxVersions(maxv),
		kgo.ConsumeRegex(),
		kgo.ConsumeTopics("a848-v0-t.*"),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	consumeN(t, cl, 3, 10*time.Second)
}

// B5: with regex consuming plus excludes, the client cannot use the
// server-side regex (it has no exclude counterpart) and instead subscribes
// to the resolved topic names. That emulation must skip internal topics
// like classic regex consuming does; pre-fix the internal topic is
// explicitly subscribed by name and its records are consumed.
func TestAudit848RegexExcludesSkipsInternalTopics(t *testing.T) {
	t.Parallel()
	const (
		regular  = "a848x-reg"
		internal = "a848x-int"
		group    = "a848x-g"
	)
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, regular))

	producer := newClient848(t, c)

	// Create the internal topic (kfake marks topics internal via the
	// kfake.is_internal topic config).
	creq := kmsg.NewPtrCreateTopicsRequest()
	ct := kmsg.NewCreateTopicsRequestTopic()
	ct.Topic = internal
	ct.NumPartitions = 1
	ct.ReplicationFactor = 1
	ccfg := kmsg.NewCreateTopicsRequestTopicConfig()
	ccfg.Name = "kfake.is_internal"
	ccfg.Value = kmsg.StringPtr("true")
	ct.Configs = append(ct.Configs, ccfg)
	creq.Topics = append(creq.Topics, ct)
	cresp, err := creq.RequestWith(context.Background(), producer)
	if err != nil || cresp.Topics[0].ErrorCode != 0 {
		t.Fatalf("create internal topic: err=%v code=%d", err, cresp.Topics[0].ErrorCode)
	}

	produceNStrings(t, producer, regular, 3)
	produceNStrings(t, producer, internal, 3)

	var subscribedInternal atomic.Bool
	c.ControlKey(int16(kmsg.ConsumerGroupHeartbeat), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		req := kreq.(*kmsg.ConsumerGroupHeartbeatRequest)
		if slices.Contains(req.SubscribedTopicNames, internal) {
			subscribedInternal.Store(true)
		}
		return nil, nil, false
	})

	cl := newClient848(t, c,
		kgo.ConsumeRegex(),
		kgo.ConsumeTopics("a848x-.*"),
		kgo.ConsumeExcludeTopics("a848x-never-matches.*"),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)

	recs := consumeN(t, cl, 3, 10*time.Second)
	for _, r := range recs {
		if r.Topic != regular {
			t.Fatalf("consumed record from %q; regex+excludes consumers must not consume internal topics", r.Topic)
		}
	}
	if subscribedInternal.Load() {
		t.Fatal("heartbeat SubscribedTopicNames contained the internal topic; the regex+excludes fallback must skip internal topics like classic regex consuming")
	}
}

// D1: a buggy/hostile broker returning a success response with
// MemberEpoch=-1 must be ignored. Pre-fix the client stored the epoch and
// its next heartbeat - carrying MemberEpoch=-1 - was an unintended leave.
// The crafted response also carries HeartbeatIntervalMillis=0, which must
// not produce a hot heartbeat loop (the client falls back to its
// configured interval).
func TestAudit848NegativeEpochIgnored(t *testing.T) {
	t.Parallel()
	const (
		topic = "a848-neg-t"
		group = "a848-neg-g"
	)
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	producer := newClient848(t, c)
	produceNStrings(t, producer, topic, 3)

	// Recorder first (FIFO): flags any request that carries a negative
	// epoch, i.e. a leave this test never asks for.
	var sentLeave atomic.Bool
	c.ControlKey(int16(kmsg.ConsumerGroupHeartbeat), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		req := kreq.(*kmsg.ConsumerGroupHeartbeatRequest)
		if req.MemberEpoch < 0 {
			sentLeave.Store(true)
		}
		return nil, nil, false
	})

	cl := newClient848(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	consumeN(t, cl, 3, 10*time.Second)

	// One crafted "success" with a negative epoch on a regular heartbeat.
	c.ControlKey(int16(kmsg.ConsumerGroupHeartbeat), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		req := kreq.(*kmsg.ConsumerGroupHeartbeatRequest)
		if req.MemberEpoch <= 0 {
			c.KeepControl()
			return nil, nil, false
		}
		resp := req.ResponseKind().(*kmsg.ConsumerGroupHeartbeatResponse)
		resp.MemberID = &req.MemberID
		resp.MemberEpoch = -1
		resp.HeartbeatIntervalMillis = 0
		return resp, nil, true
	})

	// Several heartbeat intervals pass (the cluster heartbeats at 100ms);
	// the client must never echo the poisoned epoch back as a leave, and
	// consumption must continue undisturbed.
	time.Sleep(1500 * time.Millisecond)
	if sentLeave.Load() {
		t.Fatal("client stored a negative MemberEpoch from a success response and sent an unintended leave heartbeat")
	}
	produceNStrings(t, producer, topic, 3)
	consumeN(t, cl, 3, 10*time.Second)
}

// D2: an assignment listing the same partition twice must be deduplicated.
// Pre-fix the duplicate made the assignment look new, re-fetched the
// committed offset, and rewound the live cursor: records since the last
// commit were consumed twice.
func TestAudit848DuplicatePartitionAssignmentNoRewind(t *testing.T) {
	t.Parallel()
	const (
		topic = "a848-dup-t"
		group = "a848-dup-g"
	)
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	producer := newClient848(t, c)
	produceNStrings(t, producer, topic, 50)

	cl := newClient848(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.DisableAutoCommit(),
	)

	seen := make(map[int64]int)
	track := func(recs []*kgo.Record) {
		for _, r := range recs {
			seen[r.Offset]++
		}
	}

	// Commit at offset 50, then consume 50 more uncommitted records: a
	// rewind to the committed offset re-delivers 50-99 visibly.
	track(consumeN(t, cl, 50, 10*time.Second))
	if err := cl.CommitUncommittedOffsets(context.Background()); err != nil {
		t.Fatalf("commit: %v", err)
	}
	produceNStrings(t, producer, topic, 50)
	track(consumeN(t, cl, 50, 10*time.Second))

	// Any OffsetFetch from here on can only be the re-fetch caused by the
	// duplicate-partition reassignment bounce: autocommit is off and the
	// group is otherwise stable.
	var refetches atomic.Int64
	c.ControlKey(int16(kmsg.OffsetFetch), func(kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		refetches.Add(1)
		return nil, nil, false
	})

	ti := c.TopicInfo(topic)
	c.ControlKey(int16(kmsg.ConsumerGroupHeartbeat), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		req := kreq.(*kmsg.ConsumerGroupHeartbeatRequest)
		if req.MemberEpoch <= 0 {
			c.KeepControl()
			return nil, nil, false
		}
		resp := req.ResponseKind().(*kmsg.ConsumerGroupHeartbeatResponse)
		resp.MemberID = &req.MemberID
		resp.MemberEpoch = req.MemberEpoch
		resp.HeartbeatIntervalMillis = 100
		assn := new(kmsg.ConsumerGroupHeartbeatResponseAssignment)
		at := kmsg.NewConsumerGroupHeartbeatResponseAssignmentTopic()
		at.TopicID = ti.TopicID
		at.Partitions = []int32{0, 0}
		assn.Topics = append(assn.Topics, at)
		resp.Assignment = assn
		return resp, nil, true
	})

	// Pre-fix, the duplicate makes the assignment look new: the session
	// bounces, partition 0 is diffed as "added", its committed offset is
	// re-fetched, and the live cursor is setOffset back to the committed
	// offset (whether the re-delivery is visible depends on racing the
	// in-flight long-poll, so the deterministic assertions are the
	// re-fetch itself plus any visible duplicates).
	produceNStrings(t, producer, topic, 10)
	track(consumeN(t, cl, 10, 10*time.Second))
	time.Sleep(1 * time.Second) // several heartbeat intervals: the (pre-fix) bounce settles
	for off, n := range seen {
		if n > 1 {
			t.Fatalf("offset %d consumed %d times: duplicated partition in assignment rewound the live cursor", off, n)
		}
	}
	if n := refetches.Load(); n > 0 {
		t.Fatalf("duplicated partition in the assignment caused %d offset re-fetches (live-cursor rewind to the committed offset)", n)
	}
}

// B4 guard: ForceRebalance on a next-gen group is a forced heartbeat, not
// a session bounce; consumption continues undisturbed.
func TestAudit848ForceRebalanceNoDisruption(t *testing.T) {
	t.Parallel()
	const (
		topic = "a848-force-t"
		group = "a848-force-g"
	)
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	producer := newClient848(t, c)
	produceNStrings(t, producer, topic, 3)

	cl := newClient848(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	consumeN(t, cl, 3, 10*time.Second)
	cl.ForceRebalance()
	produceNStrings(t, producer, topic, 3)
	consumeN(t, cl, 3, 10*time.Second)
}
