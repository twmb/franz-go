package kfake

// Regression tests for classic consumer_group.go + topics_and_partitions.go.
// Each fails before its corresponding kgo fix:
//
// B1: blockAuto must count outstanding manual commits, not flag them: with
//     two overlapping async CommitOffsets, the first completion re-enabled
//     autocommit while the second was still in flight, and an autocommit
//     whose head snapshot predated the in-flight commit was then issued
//     after it (commits are strictly ordered, snapshots are not re-taken),
//     silently rewinding the broker's committed offset.
//
// B3: an OffsetFetch response omitting a requested partition must not be
//     silently accepted: the partition is never assigned a cursor and a
//     successful fetchOffsets clears g.fetching, so nothing inside a live
//     session ever re-fetches it - cooperative and 848 sessions (whose
//     unchanged assignment diffs to an empty "added") silently never
//     consume the partition. The response is now validated, retried a
//     bounded number of times, and then surfaced as a loud per-partition
//     error fetch.
//
// The round's fourth fix (kip951move.doMove panicking on a purged+re-added
// topic) is reproduced in pkg/kgo/topics_and_partitions_test.go, where the
// staged-move state can be constructed directly.

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestAuditOverlappingCommits reproduces B1. Marks-mode autocommit gives the
// test full control of the head offset. Two manual async commits (100, then
// 200) overlap via gated kfake responses; after the first completes, the head
// is marked to 150 while the second is still in flight. Pre-fix, blockAuto was
// already false, so an autocommit tick snapshotted head=150 and - commits
// being strictly ordered behind the in-flight 200 - rewound the broker's
// committed offset to 150 after the 200 landed. (A later autocommit tick
// re-commits head=200, so the rewind lasts only until that tick: the
// exposure is a rebalance, fatal group error, or crash before the tick. The
// test therefore asserts on the sequence of commits the broker received,
// which must be monotonic.) Commits waiting behind the in-flight 200 must
// also return when their own contexts end (#1546).
func TestAuditOverlappingCommits(t *testing.T) {
	t.Parallel()
	const (
		topic = "audit-overlap-commit"
		group = "audit-overlap-commit-g"
		msgs  = 200
	)

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	producer := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, msgs)

	consumer := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.AutoCommitMarks(),
		kgo.AutoCommitInterval(100*time.Millisecond),
	)
	consumeN(t, consumer, msgs, 10*time.Second)

	// Gate the two manual commits by their offsets and record the offset
	// of every commit in arrival order. The client serializes commits on
	// one connection and never pipelines them (each waits for the prior),
	// so arrival order is application order.
	gate100 := make(chan struct{})
	gate200 := make(chan struct{})
	var seqMu sync.Mutex
	var commitSeq []int64
	c.ControlKey(int16(kmsg.OffsetCommit), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		req := kreq.(*kmsg.OffsetCommitRequest)
		if req.Group != group || len(req.Topics) != 1 || len(req.Topics[0].Partitions) != 1 {
			return nil, nil, false
		}
		offset := req.Topics[0].Partitions[0].Offset
		seqMu.Lock()
		commitSeq = append(commitSeq, offset)
		seqMu.Unlock()
		var gate chan struct{}
		switch offset {
		case 100:
			gate = gate100
		case 200:
			gate = gate200
		default:
			return nil, nil, false
		}
		c.SleepControl(func() { <-gate })
		return nil, nil, false
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	off := func(o int64) map[string]map[int32]kgo.EpochOffset {
		return map[string]map[int32]kgo.EpochOffset{topic: {0: {Epoch: -1, Offset: o}}}
	}
	done100 := make(chan error, 1)
	done200 := make(chan error, 1)
	consumer.CommitOffsets(ctx, off(100), func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, err error) {
		done100 <- err
	})
	consumer.CommitOffsets(ctx, off(200), func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, err error) {
		done200 <- err
	})

	// Let the first commit complete; the second is now on the wire, held.
	close(gate100)
	select {
	case err := <-done100:
		if err != nil {
			t.Fatalf("commit(100) failed: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for commit(100)")
	}

	// head: 150. Autocommit ticks now run while commit(200) is in flight;
	// pre-fix they were no longer blocked and snapshotted head=150.
	consumer.MarkCommitOffsets(off(150))
	time.Sleep(400 * time.Millisecond)

	// A commit waiting behind commit(200) returns when its own context
	// ends and never sends: the sequence check below catches a 175 that
	// arrives after the 200.
	shortCommit := func(what string, commit func(context.Context) error) {
		errc := make(chan error, 1)
		go func() {
			short, shortCancel := context.WithTimeout(ctx, 100*time.Millisecond)
			defer shortCancel()
			errc <- commit(short)
		}()
		select {
		case err := <-errc:
			if !errors.Is(err, context.DeadlineExceeded) {
				t.Fatalf("%s behind commit(200): got %v, want %v", what, err, context.DeadlineExceeded)
			}
		case <-time.After(5 * time.Second):
			close(gate200)
			t.Fatalf("%s waited past its context for commit(200)", what)
		}
	}
	// The async commit goes first: it shares commit(200)'s read lock and
	// waits only on the commit order. The sync commit waits on the lock.
	shortCommit("CommitOffsets", func(ctx context.Context) error {
		errc := make(chan error, 1)
		consumer.CommitOffsets(ctx, off(175), func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, err error) {
			errc <- err
		})
		return <-errc
	})
	shortCommit("CommitRecords", func(ctx context.Context) error {
		return consumer.CommitRecords(ctx, &kgo.Record{Topic: topic, Offset: 174, LeaderEpoch: -1})
	})

	close(gate200)
	select {
	case err := <-done200:
		if err != nil {
			t.Fatalf("commit(200) failed: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for commit(200)")
	}

	// Give any stale-snapshot autocommit (the bug) time to land, then
	// verify the broker never saw a rewind: the sequence of committed
	// offsets must be non-decreasing. Pre-fix the sequence contains a 150
	// AFTER the 200 (followed by a healing autocommit of head=200, which
	// is why the final offset alone cannot catch the bug).
	time.Sleep(500 * time.Millisecond)

	seqMu.Lock()
	seq := append([]int64(nil), commitSeq...)
	seqMu.Unlock()
	var max int64
	for i, o := range seq {
		if o < max {
			t.Fatalf("BUG REPRODUCED: broker received commit sequence %v: offset %d at index %d rewinds the committed offset %d; an autocommit with a stale head snapshot was issued after a higher in-flight manual commit", seq, o, i, max)
		}
		max = o
	}
}

// TestAuditCommit848StaleRetry: a manual commit of both owned partitions is
// answered STALE_MEMBER_EPOCH until the member moves to a new epoch. If a
// partition left the assignment after the commit was built, the commit must
// not be retried under the new epoch: the partition could have moved away and
// back, and its offset would rewind the other member's commit (#1544). If
// nothing left, the retry under the new epoch succeeds.
func TestAuditCommit848StaleRetry(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		bump func(context.Context, *testing.T, *Cluster, *kgo.Client, string, string)
		want int16 // every partition's error code
	}{
		{"revoke", func(_ context.Context, t *testing.T, c *Cluster, _ *kgo.Client, topic, group string) {
			newGroupConsumer(t, c, topic, group) // takes a partition from m1
		}, kerr.StaleMemberEpoch.Code},
		{"no-revoke", func(ctx context.Context, t *testing.T, _ *Cluster, cl *kgo.Client, topic, _ string) {
			if _, err := kadm.NewClient(cl).CreatePartitions(ctx, 1, topic); err != nil {
				t.Fatal(err) // m1 gains a partition and loses none
			}
		}, 0},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			topic := "audit-848-stale-commit-" + test.name
			group := topic + "-g"

			c := newCluster(t, NumBrokers(1), SeedTopics(2, topic))

			producer := newPlainClient(t, c, kgo.RecordPartitioner(kgo.ManualPartitioner()))
			for p := int32(0); p < 2; p++ {
				for i := range 2 {
					produceSync(t, producer, &kgo.Record{
						Topic:     topic,
						Partition: p,
						Value:     []byte("v-" + strconv.Itoa(i)),
					})
				}
			}

			m1 := newGroupConsumer(t, c, topic, group, kgo.DisableAutoCommit())
			consumeN(t, m1, 4, 10*time.Second)

			_, origGen := m1.GroupMetadata()
			if origGen < 0 {
				t.Fatal("m1 has no member epoch after consuming")
			}

			// Answer any commit at the original epoch with
			// STALE_MEMBER_EPOCH; kfake would accept the first
			// attempt. A retry at the new epoch passes through to
			// kfake, which accepts it.
			commitStaled := make(chan struct{})
			var closedOnce atomic.Bool
			c.ControlKey(int16(kmsg.OffsetCommit), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
				c.KeepControl()
				req := kreq.(*kmsg.OffsetCommitRequest)
				if req.Group != group || req.Generation != origGen {
					return nil, nil, false
				}
				resp := req.ResponseKind().(*kmsg.OffsetCommitResponse)
				for _, rt := range req.Topics {
					st := kmsg.NewOffsetCommitResponseTopic()
					st.Topic = rt.Topic
					st.TopicID = rt.TopicID
					for _, rp := range rt.Partitions {
						sp := kmsg.NewOffsetCommitResponseTopicPartition()
						sp.Partition = rp.Partition
						sp.ErrorCode = kerr.StaleMemberEpoch.Code
						st.Partitions = append(st.Partitions, sp)
					}
					resp.Topics = append(resp.Topics, st)
				}
				if closedOnce.CompareAndSwap(false, true) {
					close(commitStaled)
				}
				return resp, nil, true
			})

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			offsets := map[string]map[int32]kgo.EpochOffset{topic: {
				0: {Epoch: -1, Offset: 1},
				1: {Epoch: -1, Offset: 1},
			}}
			type result struct {
				resp *kmsg.OffsetCommitResponse
				err  error
			}
			commitDone := make(chan result, 1)
			m1.CommitOffsets(ctx, offsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
				commitDone <- result{resp, err}
			})

			select {
			case <-commitStaled:
			case <-ctx.Done():
				t.Fatal("timed out waiting for the first commit attempt")
			}

			// The 848 heartbeat loop moves m1 to the new epoch
			// without needing a poll.
			test.bump(ctx, t, c, producer, topic, group)

			var res result
			select {
			case res = <-commitDone:
			case <-ctx.Done():
				t.Fatal("timed out waiting for the commit to complete")
			}
			if res.err != nil {
				t.Fatalf("commit failed: %v", res.err)
			}
			for _, rt := range res.resp.Topics {
				for _, rp := range rt.Partitions {
					if rp.ErrorCode != test.want {
						t.Errorf("partition %d: got %v, want %v", rp.Partition, kerr.ErrorForCode(rp.ErrorCode), kerr.ErrorForCode(test.want))
					}
				}
			}
		})
	}
}

// TestAuditOffsetFetchOmittedPartition reproduces B3. kfake is made to omit
// partition 1 from every OffsetFetch response for the group. Pre-fix the
// consumer silently consumed only partition 0 - no error, no retry, and
// nothing inside the live session would ever re-fetch the omitted
// partition. Post-fix the response is validated; after bounded retries the
// omission surfaces as a loud error fetch for the partition.
func TestAuditOffsetFetchOmittedPartition(t *testing.T) {
	t.Parallel()
	const (
		topic = "audit-offsetfetch-omit"
		group = "audit-offsetfetch-omit-g"
		msgs  = 5 // per partition
	)

	c := newCluster(t, NumBrokers(1), SeedTopics(2, topic))

	producer := newPlainClient(t, c, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	for p := int32(0); p < 2; p++ {
		for i := range msgs {
			produceSync(t, producer, &kgo.Record{
				Topic:     topic,
				Partition: p,
				Value:     []byte("v-" + strconv.Itoa(i)),
			})
		}
	}

	// Answer the group's OffsetFetch by echoing every requested partition
	// (no committed offsets) EXCEPT partition 1, persistently.
	var omittedReqs atomic.Int64
	c.ControlKey(int16(kmsg.OffsetFetch), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		req := kreq.(*kmsg.OffsetFetchRequest)
		if len(req.Groups) != 1 || req.Groups[0].Group != group {
			return nil, nil, false
		}
		resp := req.ResponseKind().(*kmsg.OffsetFetchResponse)
		rg := kmsg.NewOffsetFetchResponseGroup()
		rg.Group = group
		for _, rt := range req.Groups[0].Topics {
			st := kmsg.NewOffsetFetchResponseGroupTopic()
			st.Topic = rt.Topic
			st.TopicID = rt.TopicID
			for _, p := range rt.Partitions {
				if p == 1 {
					continue // the omission under test
				}
				sp := kmsg.NewOffsetFetchResponseGroupTopicPartition()
				sp.Partition = p
				sp.Offset = -1
				sp.LeaderEpoch = -1
				st.Partitions = append(st.Partitions, sp)
			}
			rg.Topics = append(rg.Topics, st)
		}
		resp.Groups = append(resp.Groups, rg)
		omittedReqs.Add(1)
		return resp, nil, true
	})

	consumer := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)

	// Post-fix: p0's records arrive, and after bounded retries (~3s) a
	// loud error fetch surfaces for the omitted p1. Pre-fix: p0's records
	// arrive and NOTHING ever surfaces for p1 - the timeout below fires.
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	var p0Records int
	var sawOmissionErr bool
	for ctx.Err() == nil && (!sawOmissionErr || p0Records < msgs) {
		fetches := consumer.PollFetches(ctx)
		fetches.EachRecord(func(r *kgo.Record) {
			switch r.Partition {
			case 0:
				p0Records++
			case 1:
				t.Fatal("partition 1 consumed despite its offset being omitted from every OffsetFetch response; test control is broken")
			}
		})
		fetches.EachError(func(et string, ep int32, err error) {
			if et == topic && ep == 1 {
				sawOmissionErr = true
			}
		})
	}
	if p0Records < msgs {
		t.Errorf("consumed %d/%d records from the non-omitted partition", p0Records, msgs)
	}
	if !sawOmissionErr {
		t.Errorf("BUG REPRODUCED: OffsetFetch omitted partition 1 in %d responses and the consumer surfaced no error: the partition would silently never consume for the rest of the session", omittedReqs.Load())
	}
	if n := omittedReqs.Load(); n < 2 {
		t.Errorf("expected the omitted response to be retried, saw %d OffsetFetch requests", n)
	}
}

// TestAuditOffsetFetchDuplicateInjectAcrossRetries extends B3 above.
// fetchOffsets surfaces a partition's non-retryable error via addFakeReady-
// ForDraining, which is NOT idempotent. The `injected` dedup set was declared
// after the start label, so it reset on every goto-start retry: a partition
// holding a non-retryable error was re-injected once per retry pass, emitting
// a duplicate error fetch each time.
//
// A conformant Kafka broker never triggers this: it never omits a requested
// partition (so the omitted retry never runs) and it orders authorized topics
// ahead of error topics, so TOPIC_AUTHORIZATION_FAILED always sorts after any
// UNSTABLE_OFFSET_COMMIT partition and the retryable goto-start short-circuits
// before the loop reaches it. This repro therefore drives the bug with a
// non-conformant broker: partition 0 returns the non-retryable
// TOPIC_AUTHORIZATION_FAILED and partition 1 is omitted, driving the bounded
// omitted-retry (3 passes + a terminal pass).
//
// Pre-fix: partition 0's error is surfaced 4 times (once per pass). Post-fix:
// exactly once, because `injected` now persists across the goto-start retries.
// The number of OffsetFetch requests is unchanged by the fix (the retry cadence
// is identical); only the per-partition injection is now deduped.
func TestAuditOffsetFetchDuplicateInjectAcrossRetries(t *testing.T) {
	t.Parallel()
	const (
		topic = "audit-offsetfetch-dupinject"
		group = "audit-offsetfetch-dupinject-g"
	)

	c := newCluster(t, NumBrokers(1), SeedTopics(2, topic))

	var reqs atomic.Int64
	c.ControlKey(int16(kmsg.OffsetFetch), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		req := kreq.(*kmsg.OffsetFetchRequest)
		if len(req.Groups) != 1 || req.Groups[0].Group != group {
			return nil, nil, false
		}
		reqs.Add(1)
		resp := req.ResponseKind().(*kmsg.OffsetFetchResponse)
		rg := kmsg.NewOffsetFetchResponseGroup()
		rg.Group = group
		for _, rt := range req.Groups[0].Topics {
			st := kmsg.NewOffsetFetchResponseGroupTopic()
			st.Topic = rt.Topic
			st.TopicID = rt.TopicID
			for _, p := range rt.Partitions {
				if p == 1 {
					continue // omitted: drives the bounded omitted-retry
				}
				// partition 0: a non-retryable per-partition error.
				sp := kmsg.NewOffsetFetchResponseGroupTopicPartition()
				sp.Partition = p
				sp.ErrorCode = kerr.TopicAuthorizationFailed.Code
				sp.Offset = -1
				sp.LeaderEpoch = -1
				st.Partitions = append(st.Partitions, sp)
			}
			rg.Topics = append(rg.Topics, st)
		}
		resp.Groups = append(resp.Groups, rg)
		return resp, nil, true
	})

	consumer := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)

	// Drain error fetches. Partition 1's omission error is injected on the
	// final pass, AFTER every partition-0 injection (partition 0 in the
	// response loop, partition 1 in the omitted-fill). The fake-fetch queue
	// is drained wholesale and in FIFO order, so by the time partition 1's
	// error is observed, every partition-0 error already has been; we can
	// stop and assert as soon as partition 1 surfaces.
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var p0Errs, p1Errs int
	for ctx.Err() == nil && p1Errs == 0 {
		fetches := consumer.PollFetches(ctx)
		fetches.EachError(func(et string, ep int32, _ error) {
			if et != topic {
				return
			}
			switch ep {
			case 0:
				p0Errs++
			case 1:
				p1Errs++
			}
		})
		fetches.EachRecord(func(r *kgo.Record) {
			t.Fatalf("unexpected record for %s p%d; both partitions should be dropped", r.Topic, r.Partition)
		})
	}

	if p1Errs == 0 {
		t.Fatalf("test control never drove fetchOffsets to completion: partition 1 omission error never surfaced (saw %d OffsetFetch requests)", reqs.Load())
	}
	if p0Errs != 1 {
		t.Errorf("BUG REPRODUCED: partition 0's non-retryable OffsetFetch error was surfaced %d times across %d OffsetFetch passes; it must be surfaced exactly once (addFakeReadyForDraining is not idempotent and injected reset on every goto-start)", p0Errs, reqs.Load())
	}
}
