package kgo

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestShareGroupETL tests share group consumption in a chained ETL pipeline.
// Each level has two topics (one with 3 partitions, one with 1 partition)
// consumed by the same share group, exercising multi-topic assignment:
//
//   - produce N records split across topic1a (3p) and topic1b (1p)
//   - share group 1 (multiple consumers) consumes from both, with selective
//     release, reject, short-lived consumers, a pure-release stress consumer,
//     and mid-test rebalancing, producing to topic2a and topic2b
//   - share group 2 (multiple consumers) consumes from topic2a and topic2b,
//     with auto/explicit/bulk accept, mixedMark, pure-release stress,
//     short-lived bail consumers, and two mid-test rebalances, producing to
//     topic3a and topic3b
//   - validate completeness, produced == total accepts, at-least-once
//     semantics, delivery count invariants, body integrity through the chain,
//     and that rejected keys do not appear downstream
func TestShareGroupETL(t *testing.T) {
	t.Parallel()
	adm() // ensure allowShare is initialized
	if !allowShare {
		t.Skip("broker does not support share groups (requires ShareFetch v2 and ShareAcknowledge v2, Kafka 4.2+)")
	}

	totalRecords := testRecordLimit

	const (
		releaseEvery = 10
		// Keys in [0, rejectBelow) are rejected at level 1 during the
		// initial solo-reject phase. They are archived by the broker
		// and must not appear at level 2 or 3.
		rejectBelow = 100
	)

	body := []byte(randsha()) // unique body to verify integrity through chain

	// Each level consumes from two topics: a 3-partition and a 1-partition.
	topic1a, topic1aCleanup := tmpTopicPartitions(t, 3)
	topic1b, topic1bCleanup := tmpTopicPartitions(t, 1)
	topic2a, topic2aCleanup := tmpTopicPartitions(t, 3)
	topic2b, topic2bCleanup := tmpTopicPartitions(t, 1)
	topic3a, topic3aCleanup := tmpTopicPartitions(t, 3)
	topic3b, topic3bCleanup := tmpTopicPartitions(t, 1)
	t.Cleanup(func() {
		topic1aCleanup()
		topic1bCleanup()
		topic2aCleanup()
		topic2bCleanup()
		topic3aCleanup()
		topic3bCleanup()
	})

	group1, group1Cleanup := tmpShareGroup(t)
	group2, group2Cleanup := tmpShareGroup(t)
	t.Cleanup(group1Cleanup)
	t.Cleanup(group2Cleanup)

	// SPSO at log start so we see records produced before consumers join,
	// and shorten the broker's acquisition-lock timeout from the 30s
	// default so stuck acquisitions roll over in seconds (important for
	// the release strategies exercised by this test).
	for _, g := range []string{group1, group2} {
		setShareGroupConfigs(t, adm(), g,
			"share.auto.offset.reset", "earliest",
			"share.record.lock.duration.ms", "15000",
		)
	}

	//////////////
	// ETL CORE //
	//////////////

	type strategy int
	const (
		autoAccept       strategy = iota // just poll, never call Ack
		explicitAccept                   // r.Ack(AckAccept) per record
		bulkAccept                       // cl.MarkAcks(AckAccept) for all
		selectiveRelease                 // release every Kth, accept the rest
		pureRelease                      // release everything, never accept
		rejectLowKeys                    // reject keys < rejectBelow, accept the rest
		mixedMark                        // r.Ack(AckRelease) for some, then cl.MarkAcks(AckAccept) for rest
	)

	// shareLevel tracks per-level correctness state.
	type shareLevel struct {
		mu          sync.Mutex
		accepted    map[int]int      // key -> number of times accepted
		rejected    map[int]struct{} // keys that were rejected (level 1 only)
		redelivered map[int]struct{} // keys seen with delivery count > 1
		maxDC       int32            // max delivery count seen
		produced    atomic.Int64     // records produced downstream
		consumed    atomic.Int64     // total polls including redeliveries
		badDC       atomic.Int64     // records with delivery count < 1
		badBody     atomic.Int64     // records with wrong body
	}

	type topicPair struct{ a, b string }

	// runConsumer reads from consumeTopics, applies an ack strategy, and
	// produces each accepted record to the correct output topic.
	// maxPolls limits how many non-empty poll rounds the consumer
	// processes before returning: -1 = unlimited; a positive N bails
	// after N non-empty polls. Empty polls (2s timeout with no records,
	// e.g. during a rebalance) are intentionally not counted so a
	// bailAfter-N consumer reliably processes N non-empty batches.
	// pollRecords, if > 0, uses PollRecords(ctx, n) instead of
	// PollFetches to exercise the maxPollRecords path.
	runConsumer := func(
		ctx context.Context,
		t *testing.T,
		name string,
		group string,
		consumeTopics [2]string,
		produceTopics topicPair,
		mode strategy,
		maxPolls int,
		pollRecords int,
		lvl *shareLevel,
	) {
		opts := []Opt{
			ConsumeTopics(consumeTopics[0], consumeTopics[1]),
			ShareGroup(group),
			MaxBufferedRecords(10000),
			MaxBufferedBytes(50000),
			WithLogger(testLogger()),
		}
		if testChaos {
			opts = append(opts, Dialer(chaosDialer{}.DialContext))
		}
		cl, err := newTestClient(opts...)
		if err != nil {
			t.Errorf("%s: unable to create client: %v", name, err)
			return
		}
		defer cl.Close()
		defer func() {
			// Bounded fresh ctx (not derived from the outer test ctx,
			// which may already be cancelled here) so teardown cannot
			// hang past deadline on a broker that stops responding.
			flushCtx, flushCancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer flushCancel()
			if err := cl.Flush(flushCtx); err != nil {
				t.Errorf("%s: flush: %v", name, err)
			}
		}()

		npolls := 0
		for {
			pollCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
			var fetches Fetches
			if pollRecords > 0 {
				fetches = cl.PollRecords(pollCtx, pollRecords)
			} else {
				fetches = cl.PollFetches(pollCtx)
			}
			cancel()

			// ErrFirstReadEOF can occur during broker restarts: the
			// client reconnects and sends ApiVersions, but the old
			// server closes the connection before responding. This is
			// transient and the client will reconnect successfully.
			var firstReadEOF *ErrFirstReadEOF
			for _, fetchErr := range fetches.Errors() {
				if fetchErr.Err == context.DeadlineExceeded || fetchErr.Err == context.Canceled {
					continue
				}
				if errors.As(fetchErr.Err, &firstReadEOF) {
					continue
				}
				t.Errorf("%s: fetch error: %v", name, fetchErr)
			}

			gotRecords := false
			needFlush := false
			for r := range fetches.RecordsAll() {
				gotRecords = true
				lvl.consumed.Add(1)
				keyNum, err := strconv.Atoi(string(r.Key))
				if err != nil {
					t.Errorf("%s: bad key %q: %v", name, r.Key, err)
					continue
				}

				dc := r.DeliveryCount()
				if dc < 1 {
					lvl.badDC.Add(1)
				}
				if !bytes.Equal(r.Value, body) {
					lvl.badBody.Add(1)
				}

				lvl.mu.Lock()
				if dc > lvl.maxDC {
					lvl.maxDC = dc
				}
				if dc > 1 {
					lvl.redelivered[keyNum] = struct{}{}
				}
				lvl.mu.Unlock()

				switch mode {
				case autoAccept:
					// auto-ACCEPT on next poll
				case explicitAccept:
					r.Ack(AckAccept)
					needFlush = true
				case bulkAccept:
					// handled after the loop via MarkAcks
				case selectiveRelease:
					// dc <= 1 means first delivery: the record has never been
					// handed to any consumer before, so no prior accept is
					// possible and no "already accepted" guard is needed.
					if keyNum%releaseEvery == 0 && dc <= 1 {
						r.Ack(AckRelease)
						needFlush = true
						continue
					}
					r.Ack(AckAccept)
					needFlush = true
				case pureRelease:
					r.Ack(AckRelease)
					needFlush = true
					continue
				case mixedMark:
					// Release some individually; the rest get bulk-accepted
					// after the loop via MarkAcks. This tests that
					// MarkAcks does not override per-record Ack calls.
					if keyNum%5 == 0 && dc <= 1 {
						r.Ack(AckRelease)
						continue
					}
				case rejectLowKeys:
					if keyNum < rejectBelow && dc <= 1 {
						r.Ack(AckReject)
						needFlush = true
						lvl.mu.Lock()
						lvl.rejected[keyNum] = struct{}{}
						lvl.mu.Unlock()
						continue
					}
					r.Ack(AckAccept)
					needFlush = true
				}

				outTopic := produceTopics.a
				if keyNum%2 != 0 {
					outTopic = produceTopics.b
				}
				cl.Produce(context.Background(), &Record{
					Topic: outTopic,
					Key:   r.Key,
					Value: r.Value,
				}, func(_ *Record, err error) {
					if err != nil {
						t.Errorf("%s: etl produce: %v", name, err)
					} else {
						lvl.produced.Add(1)
					}
				})

				lvl.mu.Lock()
				lvl.accepted[keyNum]++
				lvl.mu.Unlock()
			}

			if (mode == bulkAccept || mode == mixedMark) && gotRecords {
				cl.MarkAcks(AckAccept)
				needFlush = true
			}
			if needFlush {
				// Derive from the outer ctx so test-wide cancellation
				// aborts the flush promptly rather than letting every
				// consumer burn its own 10s budget after timeout.
				flushCtx, flushCancel := context.WithTimeout(ctx, 10*time.Second)
				err := cl.FlushAcks(flushCtx)
				flushCancel()
				// Only fail on a real flush problem. If the outer ctx
				// died (consumer was deliberately canceled by the test),
				// the inner WithTimeout propagates Canceled immediately
				// and that propagation is not a kgo bug.
				if err != nil && ctx.Err() == nil {
					t.Errorf("%s: flush acks: %v", name, err)
				}
			}

			if gotRecords && maxPolls > 0 {
				npolls++
				if npolls >= maxPolls {
					return
				}
			}

			if ctx.Err() != nil {
				return
			}
		}
	}

	// validateLevel checks completeness and correctness for a share level.
	// Called after wg.Wait(), so every consumer goroutine has exited and
	// no concurrent access to lvl is possible; the per-level mutexes are
	// not re-acquired here.
	validateLevel := func(name string, lvl *shareLevel, expectedUnique int, prevLvl *shareLevel) {
		allKeys := make([]int, 0, len(lvl.accepted))
		for k := range lvl.accepted {
			allKeys = append(allKeys, k)
		}
		if len(allKeys) != expectedUnique {
			t.Errorf("%s: got %d unique keys != exp %d", name, len(allKeys), expectedUnique)
		}

		// Verify completeness: every non-rejected key should be present.
		sort.Ints(allKeys)
		rejLvl := prevLvl
		if rejLvl == nil {
			rejLvl = lvl // level 1 checks its own rejected map
		}
		for i := range totalRecords {
			if _, rej := rejLvl.rejected[i]; rej && rejLvl.accepted[i] == 0 {
				continue
			}
			j := sort.SearchInts(allKeys, i)
			if j >= len(allKeys) || allKeys[j] != i {
				t.Errorf("%s: key %d missing", name, i)
			}
		}
		if prevLvl != nil && prevLvl != lvl {
			// Rejected keys that were never accepted upstream must not appear.
			for k := range prevLvl.rejected {
				if prevLvl.accepted[k] == 0 && lvl.accepted[k] > 0 {
					t.Errorf("%s: rejected key %d appeared downstream (%d times)", name, k, lvl.accepted[k])
				}
			}
		}

		var dups int
		var totalAccepts int64
		for _, count := range lvl.accepted {
			totalAccepts += int64(count)
			if count > 1 {
				dups++
			}
		}

		p := lvl.produced.Load()
		if p != totalAccepts {
			t.Errorf("%s: produced %d but total accepts %d -- mismatch", name, p, totalAccepts)
		}
		if p < int64(expectedUnique) {
			t.Errorf("%s: produced %d < %d expected unique keys", name, p, expectedUnique)
		}
		if lvl.consumed.Load() < p {
			t.Errorf("%s: consumed %d < produced %d -- impossible", name, lvl.consumed.Load(), p)
		}
		if lvl.badDC.Load() > 0 {
			t.Errorf("%s: %d records had DeliveryCount < 1", name, lvl.badDC.Load())
		}
		if lvl.badBody.Load() > 0 {
			t.Errorf("%s: %d records had wrong body", name, lvl.badBody.Load())
		}

		t.Logf("%s: %d unique keys, %d total accepts, %d produced, %d duplicates, %d redelivered, max dc %d, consumed %d",
			name, len(allKeys), totalAccepts, p, dups, len(lvl.redelivered), lvl.maxDC, lvl.consumed.Load())
	}

	// Without -race, a 500k-record 2-hop share ETL finishes in 20-60s
	// even when running alongside TestGroupETL / TestTxnEtl. 5m is
	// generous slack.
	//
	// Under -race the whole hot path (atomics, maps, channels) is
	// instrumented and can slow full-suite execution ~10x on a single
	// broker. Solo race = ~90s; full-suite race has been seen at 800s+.
	// We use 19m there so this test doesn't fail in the 1-in-20 long-tail
	// iteration. Outer `go test -timeout` must be >= 20m when running
	// with -race.
	testCtxTimeout := 5 * time.Minute
	if testIsRace {
		testCtxTimeout = 19 * time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), testCtxTimeout)
	defer cancel()

	var wg sync.WaitGroup

	l1in := [2]string{topic1a, topic1b}
	l1out := topicPair{topic2a, topic2b}
	l2in := [2]string{topic2a, topic2b}
	l2out := topicPair{topic3a, topic3b}

	////////////////////
	// PRODUCER START //
	////////////////////

	// Produce concurrently with consumers so the watcher long-poll
	// path is exercised: consumers may see empty polls before records
	// land and must block in the broker's ShareFetch long-poll wait
	// until data arrives. The reject phase below starts immediately;
	// its "rejected >= rejectBelow/2" gate paces it against the
	// producer without a synchronous wait.
	prodCl, _ := newTestClient(
		MaxBufferedRecords(10000),
		MaxBufferedBytes(50000),
		UnknownTopicRetries(-1), // under -race, metadata propagation can lag past the default retry budget
		WithLogger(testLogger()),
	)
	defer prodCl.Close()

	// producerDone is awaited at the end so the goroutine cannot outlive
	// the test body; otherwise its callbacks could fire t.Errorf after
	// the test has returned. Produce uses the test ctx so cancellation
	// aborts in-flight sends, and callbacks silently skip the expected
	// context.Canceled errors that result.
	var producerDone sync.WaitGroup
	producerDone.Add(1)
	go func() {
		defer producerDone.Done()
		for i := range totalRecords {
			if ctx.Err() != nil {
				return
			}
			topic := topic1a
			if i%2 != 0 {
				topic = topic1b
			}
			prodCl.Produce(ctx, &Record{
				Topic: topic,
				Key:   []byte(strconv.Itoa(i)),
				Value: body,
			}, func(_ *Record, err error) {
				if err != nil && !errors.Is(err, context.Canceled) {
					t.Errorf("produce %d: %v", i, err)
				}
			})
		}
		// Bounded flush so a broker hang during teardown cannot leak
		// this goroutine past the test deadline.
		flushCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		if err := prodCl.Flush(flushCtx); err != nil && !errors.Is(err, context.Canceled) {
			t.Errorf("flush: %v", err)
		}
	}()

	/////////////
	// LEVEL 1 //
	/////////////

	lvl1 := &shareLevel{
		accepted:    make(map[int]int),
		rejected:    make(map[int]struct{}),
		redelivered: make(map[int]struct{}),
	}

	// Phase 1a: run a solo reject consumer. Keys < rejectBelow get
	// AckReject'd (archived by the broker). Running solo ensures no
	// other consumer races to accept these keys first. The producer is
	// feeding records concurrently; the gate below paces on rejected
	// count, not on total records produced.
	l1rejectCtx, l1rejectCancel := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		runConsumer(l1rejectCtx, t, "l1-c0-reject", group1, l1in, l1out, rejectLowKeys, -1, 10, lvl1)
	}()
	for ctx.Err() == nil {
		lvl1.mu.Lock()
		n := len(lvl1.rejected)
		lvl1.mu.Unlock()
		if n >= rejectBelow/2 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	l1rejectCancel()
	wg.Wait()

	t.Logf("phase 1a done: reject consumer consumed %d, rejected %d keys",
		lvl1.consumed.Load(), len(lvl1.rejected))

	// Phase 1b: start the selective-release consumer alone so it gets a
	// chance to release records before others accept them.
	l1c1ctx, l1c1cancel := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		runConsumer(l1c1ctx, t, "l1-c1-release", group1, l1in, l1out, selectiveRelease, -1, 0, lvl1)
	}()

	for lvl1.consumed.Load() < int64(totalRecords/5) && ctx.Err() == nil {
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("level 1 phase 2: adding consumers after %d consumed", lvl1.consumed.Load())

	// Phase 2: add bulk consumers and stress consumers.
	l1c2ctx, l1c2cancel := context.WithCancel(ctx)
	wg.Add(7)
	go func() {
		defer wg.Done()
		runConsumer(l1c2ctx, t, "l1-c2-auto", group1, l1in, l1out, autoAccept, -1, 25, lvl1)
	}()
	go func() {
		defer wg.Done()
		runConsumer(ctx, t, "l1-c3-explicit", group1, l1in, l1out, explicitAccept, -1, 0, lvl1)
	}()
	go func() {
		defer wg.Done()
		runConsumer(ctx, t, "l1-c3b-mixed", group1, l1in, l1out, mixedMark, -1, 15, lvl1)
	}()
	l1c1cancel() // kill the releaser, triggering rebalance
	go func() {
		defer wg.Done()
		runConsumer(ctx, t, "l1-c4-release", group1, l1in, l1out, selectiveRelease, -1, 0, lvl1)
	}()
	go func() {
		defer wg.Done()
		runConsumer(ctx, t, "l1-c5-purerel", group1, l1in, l1out, pureRelease, 3, 5, lvl1)
	}()
	go func() {
		defer wg.Done()
		runConsumer(ctx, t, "l1-c6-bailAfter1", group1, l1in, l1out, autoAccept, 1, 0, lvl1)
	}()
	go func() {
		defer wg.Done()
		runConsumer(ctx, t, "l1-c7-bailAfter2", group1, l1in, l1out, explicitAccept, 2, 3, lvl1)
	}()

	// Phase 3: more rebalancing.
	for lvl1.consumed.Load() < int64(totalRecords/2) && ctx.Err() == nil {
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("level 1 phase 3: killing l1-c2 after %d consumed", lvl1.consumed.Load())
	l1c2cancel()

	wg.Add(1)
	go func() {
		defer wg.Done()
		runConsumer(ctx, t, "l1-c9-explicit", group1, l1in, l1out, explicitAccept, -1, 0, lvl1)
	}()

	/////////////
	// LEVEL 2 //
	/////////////

	lvl2 := &shareLevel{
		accepted:    make(map[int]int),
		rejected:    make(map[int]struct{}),
		redelivered: make(map[int]struct{}),
	}

	// Phase 1: spin up the long-running strategies plus short-lived bail
	// consumers. Two cancellable contexts so phases 2 and 3 can each kill
	// one mid-test to force rebalances.
	l2c1ctx, l2c1cancel := context.WithCancel(ctx)
	l2c3ctx, l2c3cancel := context.WithCancel(ctx)
	wg.Add(7)
	go func() {
		defer wg.Done()
		runConsumer(l2c1ctx, t, "l2-c1-auto", group2, l2in, l2out, autoAccept, -1, 20, lvl2)
	}()
	go func() {
		defer wg.Done()
		runConsumer(ctx, t, "l2-c2-explicit", group2, l2in, l2out, explicitAccept, -1, 0, lvl2)
	}()
	go func() {
		defer wg.Done()
		runConsumer(l2c3ctx, t, "l2-c3-bulk", group2, l2in, l2out, bulkAccept, -1, 30, lvl2)
	}()
	go func() {
		defer wg.Done()
		runConsumer(ctx, t, "l2-c4-mixed", group2, l2in, l2out, mixedMark, -1, 15, lvl2)
	}()
	go func() {
		defer wg.Done()
		runConsumer(ctx, t, "l2-c5-bailAfter1", group2, l2in, l2out, autoAccept, 1, 0, lvl2)
	}()
	go func() {
		defer wg.Done()
		runConsumer(ctx, t, "l2-c6-bailAfter2", group2, l2in, l2out, explicitAccept, 2, 3, lvl2)
	}()
	go func() {
		defer wg.Done()
		runConsumer(ctx, t, "l2-c7-purerel", group2, l2in, l2out, pureRelease, 3, 5, lvl2)
	}()

	// Phase 2: kill the auto consumer and add a replacement to force a
	// rebalance partway through.
	for lvl2.consumed.Load() < int64(totalRecords/3) && ctx.Err() == nil {
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("level 2 rebalance 1: killing l2-c1 after %d consumed", lvl2.consumed.Load())
	l2c1cancel()

	wg.Add(1)
	go func() {
		defer wg.Done()
		runConsumer(ctx, t, "l2-c8-auto", group2, l2in, l2out, autoAccept, -1, 0, lvl2)
	}()

	// Phase 3: kill the bulk consumer and add a replacement to force a
	// second rebalance while ack state is heavily in flight.
	for lvl2.consumed.Load() < int64(totalRecords*2/3) && ctx.Err() == nil {
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("level 2 rebalance 2: killing l2-c3 after %d consumed", lvl2.consumed.Load())
	l2c3cancel()

	wg.Add(1)
	go func() {
		defer wg.Done()
		runConsumer(ctx, t, "l2-c9-bulk", group2, l2in, l2out, bulkAccept, -1, 0, lvl2)
	}()

	//////////////////////
	// FINAL VALIDATION //
	//////////////////////

	l1ExpectedUnique := func() int {
		lvl1.mu.Lock()
		defer lvl1.mu.Unlock()
		rejected := 0
		for k := range lvl1.rejected {
			if lvl1.accepted[k] == 0 {
				rejected++
			}
		}
		return totalRecords - rejected
	}

	// Wait for level 2 to accept all keys that made it through level 1.
	for {
		expected := l1ExpectedUnique()
		lvl2.mu.Lock()
		n := len(lvl2.accepted)
		lvl2.mu.Unlock()
		if n >= expected {
			break
		}
		if ctx.Err() != nil {
			lvl1.mu.Lock()
			n1 := len(lvl1.accepted)
			// Collect up to 20 missing keys (at level 2) to aid
			// debugging: on chaos failures, knowing which keys
			// never propagated is far more useful than the raw
			// accept count.
			var missing []int
			lvl2.mu.Lock()
			for i := range totalRecords {
				if _, rej := lvl1.rejected[i]; rej && lvl1.accepted[i] == 0 {
					continue
				}
				if lvl2.accepted[i] == 0 {
					missing = append(missing, i)
					if len(missing) >= 20 {
						break
					}
				}
			}
			lvl2.mu.Unlock()
			lvl1.mu.Unlock()
			t.Fatalf("timed out: level 1 accepted %d, level 2 accepted %d, expected %d; first missing at level 2: %v",
				n1, n, expected, missing)
		}
		time.Sleep(500 * time.Millisecond)
	}

	cancel()
	wg.Wait()
	// Drain the producer so any in-flight Produce callbacks fire before
	// the test returns -- callbacks hitting t.Errorf after test exit
	// trigger the Go test framework's post-completion warnings.
	producerDone.Wait()

	expUnique := l1ExpectedUnique()

	// Count purely rejected for level 1 logging.
	lvl1.mu.Lock()
	purelyRejected := 0
	for k := range lvl1.rejected {
		if lvl1.accepted[k] == 0 {
			purelyRejected++
		}
	}
	nRedel1 := len(lvl1.redelivered)
	lvl1.mu.Unlock()

	validateLevel("level 1", lvl1, expUnique, nil)
	validateLevel("level 2", lvl2, expUnique, lvl1)

	t.Logf("level 1: %d purely rejected, %d redelivered", purelyRejected, nRedel1)

	// selectiveRelease and pureRelease both force redeliveries at level 1,
	// so with any meaningful record count we should see many. Zero means
	// the release paths never ran -- likely a regression in ack plumbing
	// (AckRelease treated as AckAccept, release never sent to broker,
	// etc.).
	if nRedel1 == 0 {
		t.Error("level 1: expected at least some redelivered records, got 0")
	}
}

// TestShareGroupAckOnClose verifies that explicitly acknowledged records are
// properly sent via standalone ShareAcknowledge on Close, without requiring
// another PollFetches cycle to piggyback the acks.
func TestShareGroupAckOnClose(t *testing.T) {
	t.Parallel()
	adm() // ensure allowShare is initialized
	if !allowShare {
		t.Skip("broker does not support share groups (requires ShareFetch v2 and ShareAcknowledge v2, Kafka 4.2+)")
	}

	const totalRecords = 50

	topic, topicCleanup := tmpTopicPartitions(t, 1)
	defer topicCleanup()
	group, groupCleanup := tmpShareGroup(t)
	defer groupCleanup()

	admin, _ := newTestClient(DefaultProduceTopic(topic), UnknownTopicRetries(-1))
	defer admin.Close()
	setShareAutoOffsetReset(t, admin, group)

	for i := range totalRecords {
		admin.Produce(context.Background(), StringRecord(strconv.Itoa(i)), func(_ *Record, err error) {
			if err != nil {
				t.Errorf("produce %d: %v", i, err)
			}
		})
	}
	if err := admin.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	// Consumer 1: poll records, explicitly accept them, then close
	// immediately -- no second poll. This forces the close path to send
	// a standalone ShareAcknowledge.
	cl1, err := newTestClient(
		ConsumeTopics(topic),
		ShareGroup(group),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var got int
	for got < totalRecords {
		fetches := cl1.PollFetches(ctx)
		var firstReadEOF *ErrFirstReadEOF
		for _, e := range fetches.Errors() {
			if errors.As(e.Err, &firstReadEOF) {
				continue
			}
			t.Errorf("fetch error: %v", e)
		}
		records := fetches.Records()
		if len(records) > 0 {
			for _, r := range records {
				r.Ack(AckAccept)
			}
			got += len(records)
		}
		if ctx.Err() != nil {
			break
		}
	}
	if got < totalRecords {
		t.Fatalf("only got %d/%d records", got, totalRecords)
	}

	// Close without another poll -- the close path must send the acks.
	cl1.Close()

	// Consumer 2: verify the records were properly acked and are not
	// redelivered. A poll that returns no records confirms the acks
	// were received by the broker.
	cl2, err := newTestClient(
		ConsumeTopics(topic),
		ShareGroup(group),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl2.Close()

	verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer verifyCancel()

	var redelivered int
	for {
		fetches := cl2.PollFetches(verifyCtx)
		records := fetches.Records()
		redelivered += len(records)
		for _, r := range records {
			r.Ack(AckAccept)
		}
		if verifyCtx.Err() != nil {
			break
		}
	}
	if redelivered > 0 {
		// Some redelivery is possible if the broker hadn't fully
		// processed the acks before consumer 2 started, but it
		// should be a small number, not the full set.
		t.Logf("redelivered %d records (acceptable if small)", redelivered)
		if redelivered >= totalRecords {
			t.Errorf("all %d records redelivered; acks on close likely failed", totalRecords)
		}
	}
}

// setShareGroupConfigs applies one or more share-group configs in a
// single IncrementalAlterConfigs call on the GROUP resource, then
// polls DescribeConfigs until the broker's DynamicConfigPublisher has
// actually applied the change.
//
// IncrementalAlterConfigs returns success as soon as the change is
// committed to the metadata log, but the broker-side publisher that
// feeds ShareGroupConfigProvider runs asynchronously behind other
// metadata work and can lag several seconds under load. A share
// consumer that joins in that gap initializes its SharePartition
// with the DEFAULT share.auto.offset.reset (latest) -- so records
// produced before the gap closes become invisible to the group,
// breaking any test that assumes earliest.
func setShareGroupConfigs(t *testing.T, cl *Client, group string, kvs ...string) {
	t.Helper()
	if len(kvs)%2 != 0 {
		t.Fatalf("setShareGroupConfigs: odd number of args")
	}
	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	res := kmsg.NewIncrementalAlterConfigsRequestResource()
	res.ResourceType = kmsg.ConfigResourceTypeGroupConfig
	res.ResourceName = group
	for i := 0; i < len(kvs); i += 2 {
		cfg := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
		cfg.Name = kvs[i]
		cfg.Op = 0
		cfg.Value = kmsg.StringPtr(kvs[i+1])
		res.Configs = append(res.Configs, cfg)
	}
	req.Resources = append(req.Resources, res)
	resp, err := req.RequestWith(context.Background(), cl)
	if err != nil {
		t.Fatalf("IncrementalAlterConfigs: %v", err)
	}
	for _, r := range resp.Resources {
		if err := kerr.ErrorForCode(r.ErrorCode); err != nil {
			t.Fatalf("IncrementalAlterConfigs resource error: %v", err)
		}
	}

	want := make(map[string]string, len(kvs)/2)
	for i := 0; i < len(kvs); i += 2 {
		want[kvs[i]] = kvs[i+1]
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		dreq := kmsg.NewPtrDescribeConfigsRequest()
		dres := kmsg.NewDescribeConfigsRequestResource()
		dres.ResourceType = kmsg.ConfigResourceTypeGroupConfig
		dres.ResourceName = group
		for k := range want {
			dres.ConfigNames = append(dres.ConfigNames, k)
		}
		dreq.Resources = append(dreq.Resources, dres)
		dresp, err := dreq.RequestWith(context.Background(), cl)
		if err != nil {
			t.Fatalf("DescribeConfigs: %v", err)
		}
		applied := true
		for _, r := range dresp.Resources {
			if e := kerr.ErrorForCode(r.ErrorCode); e != nil {
				t.Fatalf("DescribeConfigs resource error: %v", e)
			}
			got := make(map[string]string, len(r.Configs))
			for _, c := range r.Configs {
				if c.Value != nil {
					got[c.Name] = *c.Value
				}
			}
			for k, v := range want {
				if got[k] != v {
					applied = false
				}
			}
		}
		if applied {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("share group %q configs %v not applied after 30s", group, want)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// setShareAutoOffsetReset sets share.auto.offset.reset=earliest on a share
// group so the SPSO initializes to log start.
func setShareAutoOffsetReset(t *testing.T, cl *Client, group string) {
	t.Helper()
	setShareGroupConfigs(t, cl, group, "share.auto.offset.reset", "earliest")
}
