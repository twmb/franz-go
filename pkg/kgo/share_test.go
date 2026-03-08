package kgo

import (
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
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
//     with auto/explicit accept and mid-test rebalancing, producing to
//     topic3a and topic3b
//   - validate completeness, produced == total accepts, at-least-once
//     semantics, delivery count invariants, body integrity through the chain,
//     and that rejected keys do not appear downstream
func TestShareGroupETL(t *testing.T) {
	t.Parallel()

	totalRecords := testRecordLimit * 2 / 5

	const (
		releaseEvery = 10
		// Keys in [0, rejectBelow) are rejected at level 1 during the
		// initial solo-reject phase. They are archived by the broker
		// and must not appear at level 2.
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

	group1 := randsha()
	group2 := randsha()

	setShareAutoOffsetReset(t, adm(), group1)
	setShareAutoOffsetReset(t, adm(), group2)

	////////////////////
	// PRODUCER START //
	////////////////////

	prodCl, _ := newTestClient(
		MaxBufferedRecords(10000),
		MaxBufferedBytes(50000),
		WithLogger(BasicLogger(os.Stderr, testLogLevel, nil)),
	)
	defer prodCl.Close()

	// Split records: even keys go to topic*a, odd keys to topic*b.
	for i := range totalRecords {
		topic := topic1a
		if i%2 != 0 {
			topic = topic1b
		}
		prodCl.Produce(context.Background(), &Record{
			Topic: topic,
			Key:   []byte(strconv.Itoa(i)),
			Value: body,
		}, func(_ *Record, err error) {
			if err != nil {
				t.Errorf("produce %d: %v", i, err)
			}
		})
	}
	if err := prodCl.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}

	//////////////
	// ETL CORE //
	//////////////

	type strategy int
	const (
		autoAccept       strategy = iota // just poll, never call Acknowledge
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
	// processes before returning (-1 = unlimited, 0 = bail immediately).
	// pollRecords, if > 0, uses PollRecords(ctx, n) instead of
	// PollFetches to exercise the maxPollRecords path.
	runConsumer := func(
		ctx context.Context,
		t *testing.T,
		name string,
		group string,
		consumeTopics [2]string,
		produceTopics topicPair,
		strat strategy,
		maxPolls int,
		pollRecords int,
		lvl *shareLevel,
	) {
		opts := []Opt{
			ConsumeTopics(consumeTopics[0], consumeTopics[1]),
			ShareGroup(group),
			FetchMaxWait(200 * time.Millisecond),
			MaxBufferedRecords(10000),
			MaxBufferedBytes(50000),
			WithLogger(BasicLogger(os.Stderr, testLogLevel, nil)),
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
			if err := cl.Flush(context.Background()); err != nil {
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
			needCommit := false
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
				if string(r.Value) != string(body) {
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

				switch strat {
				case autoAccept:
					// auto-ACCEPT on next poll
				case explicitAccept:
					r.Ack(AckAccept)
					needCommit = true
				case bulkAccept:
					// handled after the loop via MarkAcks
				case selectiveRelease:
					lvl.mu.Lock()
					alreadyAccepted := lvl.accepted[keyNum] > 0
					lvl.mu.Unlock()
					if !alreadyAccepted && keyNum%releaseEvery == 0 && dc <= 1 {
						r.Ack(AckRelease)
						needCommit = true
						continue
					}
					r.Ack(AckAccept)
					needCommit = true
				case pureRelease:
					r.Ack(AckRelease)
					needCommit = true
					continue
				case mixedMark:
					// Release some individually; the rest get bulk-accepted
					// after the loop via MarkAcks. This tests that MarkAcks
					// does not override per-record Ack calls.
					if keyNum%5 == 0 && dc <= 1 {
						r.Ack(AckRelease)
						continue
					}
				case rejectLowKeys:
					if keyNum < rejectBelow && dc <= 1 {
						r.Ack(AckReject)
						needCommit = true
						lvl.mu.Lock()
						lvl.rejected[keyNum] = struct{}{}
						lvl.mu.Unlock()
						continue
					}
					r.Ack(AckAccept)
					needCommit = true
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

			if (strat == bulkAccept || strat == mixedMark) && gotRecords {
				cl.MarkAcks(AckAccept)
				needCommit = true
			}
			if needCommit {
				commitCtx, commitCancel := context.WithTimeout(context.Background(), 10*time.Second)
				if _, err := cl.CommitAcks(commitCtx); err != nil {
					t.Errorf("%s: commit acks: %v", name, err)
				}
				commitCancel()
			}

			if gotRecords && maxPolls >= 0 {
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

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	var wg sync.WaitGroup

	l1in := [2]string{topic1a, topic1b}
	l1out := topicPair{topic2a, topic2b}
	l2in := [2]string{topic2a, topic2b}
	l2out := topicPair{topic3a, topic3b}

	/////////////
	// LEVEL 1 //
	/////////////

	lvl1 := &shareLevel{
		accepted:    make(map[int]int),
		rejected:    make(map[int]struct{}),
		redelivered: make(map[int]struct{}),
	}

	// Phase 1a: run a solo reject consumer briefly. Keys < rejectBelow
	// get AckReject'd (archived by the broker). Running solo ensures no
	// other consumer races to accept these keys first.
	l1rejectCtx, l1rejectCancel := context.WithCancel(ctx)
	wg.Add(1)
	go func() {
		defer wg.Done()
		runConsumer(l1rejectCtx, t, "l1-c0-reject", group1, l1in, l1out, rejectLowKeys, -1, 10, lvl1)
	}()
	// Wait until the reject consumer has actually rejected enough low
	// keys. We need to poll long enough that the broker delivers
	// records with keys < rejectBelow across all partitions.
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
	wg.Wait() // wait for reject consumer to fully close and flush acks

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

	// Phase 2: add bulk consumers and a pure-release stress consumer.
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
	// Pure-release consumer: releases everything, stressing redelivery.
	// Runs briefly then dies, returning records to the pool.
	go func() {
		defer wg.Done()
		runConsumer(ctx, t, "l1-c5-purerel", group1, l1in, l1out, pureRelease, 3, 5, lvl1)
	}()
	// Short-lived consumers: bail immediately or after a few polls.
	go func() {
		defer wg.Done()
		runConsumer(ctx, t, "l1-c6-bail0", group1, l1in, l1out, autoAccept, 0, 0, lvl1)
	}()
	go func() {
		defer wg.Done()
		runConsumer(ctx, t, "l1-c7-bail2", group1, l1in, l1out, explicitAccept, 2, 3, lvl1)
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
		runConsumer(ctx, t, "l1-c8-explicit", group1, l1in, l1out, explicitAccept, -1, 0, lvl1)
	}()

	/////////////
	// LEVEL 2 //
	/////////////

	lvl2 := &shareLevel{
		accepted:    make(map[int]int),
		rejected:    make(map[int]struct{}),
		redelivered: make(map[int]struct{}),
	}

	l2c1ctx, l2c1cancel := context.WithCancel(ctx)
	wg.Add(4)
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
		runConsumer(ctx, t, "l2-c3-bulk", group2, l2in, l2out, bulkAccept, -1, 30, lvl2)
	}()
	// Short-lived consumer at level 2 too.
	go func() {
		defer wg.Done()
		runConsumer(ctx, t, "l2-c4-bail1", group2, l2in, l2out, autoAccept, 1, 0, lvl2)
	}()

	for lvl2.consumed.Load() < int64(totalRecords/3) && ctx.Err() == nil {
		time.Sleep(100 * time.Millisecond)
	}
	t.Logf("level 2 rebalance: killing l2-c1 after %d consumed", lvl2.consumed.Load())
	l2c1cancel()

	wg.Add(1)
	go func() {
		defer wg.Done()
		runConsumer(ctx, t, "l2-c5-auto", group2, l2in, l2out, autoAccept, -1, 0, lvl2)
	}()

	//////////////////////
	// FINAL VALIDATION //
	//////////////////////

	// Level 1 rejects `rejectBelow` keys, so downstream has fewer.
	// With at-least-once, some rejected keys might have been accepted
	// by a concurrent consumer before the reject consumer got them, so
	// we count actual rejects.
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
			lvl1.mu.Unlock()
			t.Fatalf("timed out: level 1 accepted %d unique, level 2 accepted %d, expected %d",
				n1, n, expected)
		}
		time.Sleep(500 * time.Millisecond)
	}

	cancel()
	wg.Wait()

	// --- Level 1 validation ---
	lvl1.mu.Lock()

	// Count keys that were only rejected (never accepted by anyone).
	purelyRejected := 0
	for k := range lvl1.rejected {
		if lvl1.accepted[k] == 0 {
			purelyRejected++
		}
	}
	l1ExpUnique := totalRecords - purelyRejected

	allKeys1 := make([]int, 0, len(lvl1.accepted))
	for k := range lvl1.accepted {
		allKeys1 = append(allKeys1, k)
	}
	if len(allKeys1) != l1ExpUnique {
		t.Errorf("level 1: got %d unique keys != exp %d (%d total - %d purely rejected)",
			len(allKeys1), l1ExpUnique, totalRecords, purelyRejected)
	}

	// Verify completeness: every non-rejected key should be present.
	sort.Ints(allKeys1)
	for i := range totalRecords {
		if _, rej := lvl1.rejected[i]; rej && lvl1.accepted[i] == 0 {
			continue // purely rejected, expected absent
		}
		j := sort.SearchInts(allKeys1, i)
		if j >= len(allKeys1) || allKeys1[j] != i {
			t.Errorf("level 1: key %d missing", i)
		}
	}

	var dups1 int
	var totalAccepts1 int64
	for _, count := range lvl1.accepted {
		totalAccepts1 += int64(count)
		if count > 1 {
			dups1++
		}
	}
	maxDC1 := lvl1.maxDC
	nRedel1 := len(lvl1.redelivered)
	lvl1.mu.Unlock()

	p1 := lvl1.produced.Load()
	if p1 != totalAccepts1 {
		t.Errorf("level 1: produced %d but total accepts %d -- mismatch", p1, totalAccepts1)
	}
	if p1 < int64(l1ExpUnique) {
		t.Errorf("level 1: produced %d < %d expected unique keys", p1, l1ExpUnique)
	}
	if lvl1.consumed.Load() < p1 {
		t.Errorf("level 1: consumed %d < produced %d -- impossible", lvl1.consumed.Load(), p1)
	}
	if lvl1.badDC.Load() > 0 {
		t.Errorf("level 1: %d records had DeliveryCount < 1", lvl1.badDC.Load())
	}
	if lvl1.badBody.Load() > 0 {
		t.Errorf("level 1: %d records had wrong body", lvl1.badBody.Load())
	}

	t.Logf("level 1: %d unique keys, %d purely rejected, %d total accepts, %d produced, %d duplicates, %d redelivered, max dc %d, consumed %d",
		len(allKeys1), purelyRejected, totalAccepts1, p1, dups1, nRedel1, maxDC1, lvl1.consumed.Load())

	// With enough records and consumer churn (releases, deaths,
	// rebalancing), redeliveries should occur. With very small record
	// counts or unlimited concurrency, all records may be consumed
	// on the first pass before any releases happen.
	if nRedel1 == 0 && totalRecords >= 10000 {
		t.Error("level 1: expected at least some redelivered records, got 0")
	}

	// --- Level 2 validation ---
	lvl2.mu.Lock()

	allKeys2 := make([]int, 0, len(lvl2.accepted))
	for k := range lvl2.accepted {
		allKeys2 = append(allKeys2, k)
	}

	// Rejected keys that were never accepted at level 1 must not appear
	// at level 2 (the broker archived them).
	lvl1.mu.Lock()
	for k := range lvl1.rejected {
		if lvl1.accepted[k] == 0 {
			if lvl2.accepted[k] > 0 {
				t.Errorf("level 2: rejected key %d appeared downstream (%d times)", k, lvl2.accepted[k])
			}
		}
	}
	lvl1.mu.Unlock()

	var dups2 int
	var totalAccepts2 int64
	for _, count := range lvl2.accepted {
		totalAccepts2 += int64(count)
		if count > 1 {
			dups2++
		}
	}
	maxDC2 := lvl2.maxDC
	lvl2.mu.Unlock()

	p2 := lvl2.produced.Load()
	if p2 != totalAccepts2 {
		t.Errorf("level 2: produced %d but total accepts %d -- mismatch", p2, totalAccepts2)
	}
	if p2 < int64(l1ExpUnique) {
		t.Errorf("level 2: produced %d < %d expected unique keys", p2, l1ExpUnique)
	}
	if lvl2.consumed.Load() < p2 {
		t.Errorf("level 2: consumed %d < produced %d -- impossible", lvl2.consumed.Load(), p2)
	}
	if lvl2.badDC.Load() > 0 {
		t.Errorf("level 2: %d records had DeliveryCount < 1", lvl2.badDC.Load())
	}
	if lvl2.badBody.Load() > 0 {
		t.Errorf("level 2: %d records had wrong body", lvl2.badBody.Load())
	}

	// Verify completeness: every non-rejected key should be present.
	sort.Ints(allKeys2)
	lvl1.mu.Lock()
	for i := range totalRecords {
		if _, rej := lvl1.rejected[i]; rej && lvl1.accepted[i] == 0 {
			continue // purely rejected, expected absent
		}
		j := sort.SearchInts(allKeys2, i)
		if j >= len(allKeys2) || allKeys2[j] != i {
			t.Errorf("level 2: key %d missing", i)
		}
	}
	lvl1.mu.Unlock()

	t.Logf("level 2: %d unique keys, %d total accepts, %d produced, %d duplicates, max dc %d, consumed %d",
		len(allKeys2), totalAccepts2, p2, dups2, maxDC2, lvl2.consumed.Load())

	// Delivery count distribution for level 1 (most interesting due to
	// releases and rejects).
	lvl1.mu.Lock()
	dcDist := make(map[int32]int)
	for _, count := range lvl1.accepted {
		dcDist[int32(count)]++
	}
	lvl1.mu.Unlock()
	t.Logf("level 1 accept-count distribution: %v", func() string {
		keys := make([]int32, 0, len(dcDist))
		for k := range dcDist {
			keys = append(keys, k)
		}
		sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
		s := ""
		for _, k := range keys {
			if s != "" {
				s += ", "
			}
			s += fmt.Sprintf("%dx=%d", k, dcDist[k])
		}
		return s
	}())
}

// TestShareGroupAckOnClose verifies that explicitly acknowledged records are
// properly sent via standalone ShareAcknowledge on Close, without requiring
// another PollFetches cycle to piggyback the acks.
func TestShareGroupAckOnClose(t *testing.T) {
	t.Parallel()

	const totalRecords = 50

	topic, topicCleanup := tmpTopicPartitions(t, 1)
	defer topicCleanup()
	group := randsha()

	admin, _ := newTestClient(DefaultProduceTopic(topic))
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
		FetchMaxWait(200*time.Millisecond),
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
		FetchMaxWait(200*time.Millisecond),
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

func TestMergeAckBatches(t *testing.T) {
	t.Parallel()

	ab := func(first, last int64, types ...int8) shareAckBatch {
		return shareAckBatch{first, last, types}
	}

	tests := []struct {
		name   string
		in     []shareAckBatch
		expect []shareAckBatch
	}{
		{
			name:   "nil",
			in:     nil,
			expect: nil,
		},
		{
			name:   "single",
			in:     []shareAckBatch{ab(0, 0, 1)},
			expect: []shareAckBatch{ab(0, 0, 1)},
		},
		{
			name: "contiguous same type merges",
			in: []shareAckBatch{
				ab(0, 0, 1), ab(1, 1, 1), ab(2, 2, 1),
			},
			expect: []shareAckBatch{ab(0, 2, 1)},
		},
		{
			name: "different types merge into per-offset array",
			in: []shareAckBatch{
				ab(0, 0, 1), ab(1, 1, 2), ab(2, 2, 1),
			},
			expect: []shareAckBatch{ab(0, 2, 1, 2, 1)},
		},
		{
			name: "non-contiguous same type stays separate",
			in: []shareAckBatch{
				ab(0, 0, 1), ab(5, 5, 1),
			},
			expect: []shareAckBatch{
				ab(0, 0, 1), ab(5, 5, 1),
			},
		},
		{
			name: "unsorted input gets sorted and merged",
			in: []shareAckBatch{
				ab(3, 3, 1), ab(1, 1, 1), ab(2, 2, 1), ab(0, 0, 1),
			},
			expect: []shareAckBatch{ab(0, 3, 1)},
		},
		{
			name: "mixed merge and split",
			in: []shareAckBatch{
				ab(0, 2, 1), ab(3, 5, 2), ab(6, 8, 2),
			},
			expect: []shareAckBatch{
				ab(0, 8, 1, 1, 1, 2, 2, 2, 2, 2, 2),
			},
		},
		{
			name: "non-contiguous mixed types",
			in: []shareAckBatch{
				ab(0, 2, 1), ab(10, 11, 2),
			},
			expect: []shareAckBatch{
				ab(0, 2, 1), ab(10, 11, 2),
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := mergeAckBatches(tt.in)
			if len(got) != len(tt.expect) {
				t.Fatalf("len: got %d, want %d\n  got:  %v\n  want: %v", len(got), len(tt.expect), got, tt.expect)
			}
			for i := range got {
				g, e := got[i], tt.expect[i]
				if g.firstOffset != e.firstOffset || g.lastOffset != e.lastOffset || !slices.Equal(g.ackTypes, e.ackTypes) {
					t.Fatalf("batch[%d]: got %v, want %v", i, g, e)
				}
			}
		})
	}
}

func TestShareAckFromCtx(t *testing.T) {
	t.Parallel()

	// Record with nil context.
	r := &Record{}
	if st := shareAckFromCtx(r); st != nil {
		t.Fatal("expected nil for nil context")
	}

	// Record with a context but no share ack state.
	r.Context = context.Background()
	if st := shareAckFromCtx(r); st != nil {
		t.Fatal("expected nil for non-share record")
	}

	// Record with share ack state via slab.
	slab := &shareAckSlab{
		states:   []shareAckState{{deliveryCount: 3, source: &source{}}},
		records0: r,
	}
	r.Context = context.WithValue(context.Background(), shareAckKey, slab)
	st := shareAckFromCtx(r)
	if st == nil {
		t.Fatal("expected non-nil for share record")
	}
	if st.deliveryCount != 3 {
		t.Fatalf("delivery count: got %d, want 3", st.deliveryCount)
	}
}

// setShareAutoOffsetReset sets share.auto.offset.reset=earliest on a share
// group so the SPSO initializes to log start.
func setShareAutoOffsetReset(t *testing.T, cl *Client, group string) {
	t.Helper()
	req := kmsg.NewPtrIncrementalAlterConfigsRequest()
	res := kmsg.NewIncrementalAlterConfigsRequestResource()
	res.ResourceType = kmsg.ConfigResourceTypeGroupConfig
	res.ResourceName = group
	cfg := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	cfg.Name = "share.auto.offset.reset"
	cfg.Op = 0
	cfg.Value = kmsg.StringPtr("earliest")
	res.Configs = append(res.Configs, cfg)
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
}
