package kgo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const recordLimit = 1000000

var testLogLevel = func() LogLevel {
	if strings.ToLower(os.Getenv("KGO_LOG_LEVEL")) == "debug" {
		return LogLevelDebug
	}
	return LogLevelInfo
}()

var randsha = func() func() string {
	var mu sync.Mutex
	last := time.Now().UnixNano()
	return func() string {
		mu.Lock()
		defer mu.Unlock()

		now := time.Now().UnixNano()
		if now == last { // should never be the case
			now++
		}

		last = now

		sum := sha256.Sum256([]byte(strconv.Itoa(int(now))))
		return hex.EncodeToString(sum[:])
	}
}()

var okRe = regexp.MustCompile(`\bOK\b`)

func tmpTopic(tb testing.TB) (string, func()) {
	tb.Helper()
	path, err := exec.LookPath("kcl")
	if err != nil {
		tb.Fatal("unable to find `kcl` in $PATH; cannot create temporary topics for tests")
	}

	topic := randsha()

	cmd := exec.Command(path, "topic", "create", topic, "-p", "20")
	output, err := cmd.CombinedOutput()
	if err != nil {
		tb.Fatalf("unable to run kcl topic create command: %v", err)
	}
	if !okRe.Match(output) {
		tb.Fatalf("topic create failed")
	}

	return topic, func() {
		tb.Helper()
		tb.Logf("deleting topic %s", topic)
		cmd := exec.Command(path, "topic", "delete", topic)
		output, err := cmd.CombinedOutput()
		if err != nil {
			tb.Fatalf("unable to run kcl topic delete command: %v", err)
		}
		if !okRe.Match(output) {
			tb.Fatalf("topic delete failed")
		}
	}
}

func tmpGroup(tb testing.TB) (string, func()) {
	tb.Helper()
	path, err := exec.LookPath("kcl")
	if err != nil {
		tb.Fatal("unable to find `kcl` in $PATH; cannot ensure a created group will be deleted")
	}

	group := randsha()

	return group, func() {
		tb.Helper()
		tb.Logf("deleting group %s", group)
		cmd := exec.Command(path, "group", "delete", group)
		output, err := cmd.CombinedOutput()
		if err != nil {
			tb.Fatalf("unable to run kcl group delete command: %v", err)
		}
		if !okRe.Match(output) {
			tb.Fatalf("group delete failed\n%s", output)
		}
	}
}

// TestProduceConsumeGroup tests:
//
// - producing a lot of messages to a single topic, ensuring that all messages
// are produced in order.
//
// - a consumer group with 5 members, one that leaves immediately after joining
// and fetching
//
// - that we never doubly consume records, and that all records are consumed in
// order
func TestProduceConsumeGroup(t *testing.T) {
	t.Parallel()

	topic, topicCleanup := tmpTopic(t)
	defer topicCleanup()

	errs := make(chan error)
	body := []byte(strings.Repeat(randsha(), 100)) // a large enough body

	////////////////////
	// PRODUCER START //
	////////////////////

	go func() {
		cl, _ := NewClient(WithLogger(BasicLogger(testLogLevel, nil)))
		defer cl.Close()

		var offsetsMu sync.Mutex
		offsets := make(map[int32]int64)

		defer func() {
			if err := cl.Flush(context.Background()); err != nil {
				errs <- fmt.Errorf("unable to flush: %v", err)
			}
		}()
		for i := 0; i < recordLimit; i++ {
			myKey := []byte(strconv.Itoa(i))
			cl.Produce(
				context.Background(),
				&Record{
					Topic: topic,
					Key:   myKey,
					Value: body,
				},
				func(r *Record, err error) {
					if err != nil {
						errs <- fmt.Errorf("unexpected produce err: %v", err)
					}
					if !bytes.Equal(r.Key, myKey) {
						errs <- fmt.Errorf("unexpected out of order key; got %s != exp %v", r.Key, myKey)
					}

					// ensure the offsets for this partition are contiguous
					offsetsMu.Lock()
					current, ok := offsets[r.Partition]
					if ok && r.Offset != current+1 {
						errs <- fmt.Errorf("partition produced offsets out of order, got %d != exp %d", r.Offset, current+1)
					} else if !ok && r.Offset != 0 {
						errs <- fmt.Errorf("expected first produced record to partition to have offset 0, got %d", r.Offset)
					}
					offsets[r.Partition] = r.Offset
					offsetsMu.Unlock()
				},
			)
		}

	}()

	//////////////////////
	// GROUP DEFINITION //
	//////////////////////

	group, groupCleanup := tmpGroup(t)
	defer groupCleanup()

	var (
		wg       sync.WaitGroup // waits for all consumers to quit
		consumed uint64         // used to quit consuming

		groupMu     sync.Mutex
		part2key    = make(map[int32][]int)         // ensures ordering
		partOffsets = make(map[partOffset]struct{}) // ensures we never double consume
	)

	addConsumer := func() {
		wg.Add(1)
		go func() {
			l := BasicLogger(testLogLevel, nil)
			cl, _ := NewClient(WithLogger(l))
			defer wg.Done()
			defer cl.Close()

			cl.AssignGroup(group, GroupTopics(topic))
			defer cl.AssignGroup("") // leave group to allow for group deletion

			for {
				ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
				fetches := cl.PollFetches(ctx)
				cancel()
				if fetchErrs := fetches.Errors(); len(fetchErrs) > 0 {
					errs <- fmt.Errorf("poll got unexpected errs: %v", fetchErrs)
				}

				iter := fetches.RecordIter()
				for !iter.Done() {
					r := iter.Next()
					keyNum, err := strconv.Atoi(string(r.Key))
					if err != nil {
						errs <- err
					}
					if !bytes.Equal(r.Value, body) {
						errs <- fmt.Errorf("body not what was expected")
					}

					groupMu.Lock()
					if _, exists := partOffsets[partOffset{r.Partition, r.Offset}]; exists {
						errs <- fmt.Errorf("saw double offset p%do%d", r.Partition, r.Offset)
					}
					partOffsets[partOffset{r.Partition, r.Offset}] = struct{}{}
					part2key[r.Partition] = append(part2key[r.Partition], keyNum)
					groupMu.Unlock()

					atomic.AddUint64(&consumed, 1)
				}

				if atomic.LoadUint64(&consumed) == recordLimit {
					break
				}
			}
		}()
	}

	/////////////////////
	// CONSUMERS START //
	/////////////////////

	for i := 0; i < 3; i++ { // three consumers start with standard poll&commit behavior
		addConsumer()
	}

	// Have one consumer quit immediately after polling, causing others to
	// rebalance.
	wg.Add(1)
	go func() {
		cl, _ := NewClient(WithLogger(BasicLogger(testLogLevel, nil)))
		defer wg.Done()

		cl.AssignGroup(group,
			GroupTopics(topic),
			DisableAutoCommit(), // we do not want to commit since we do not process records
		)
		defer cl.AssignGroup("") // leave group to allow for group deletion

		// we quit immediately
		fetches := cl.PollFetches(context.Background())
		if fetchErrs := fetches.Errors(); len(fetchErrs) > 0 {
			errs <- fmt.Errorf("poll got unexpected errs: %v", fetchErrs)
		}
	}()

	// Wait 5s (past kafka internal initial 3s wait period) to add two more
	// consumers that will trigger other consumers to lose some partitions.
	time.Sleep(5 * time.Second)
	for i := 0; i < 3; i++ {
		addConsumer()
	}

	doneConsume := make(chan struct{})
	go func() {
		wg.Wait()
		close(doneConsume)
	}()

out:
	for {
		select {
		case err := <-errs:
			t.Fatal(err)
		case <-doneConsume:
			break out
		}
	}

	//////////
	// DONE //
	//////////

	allKeys := make([]int, 0, recordLimit) // did we receive everything we sent?
	for part, keys := range part2key {
		if !sort.IsSorted(sort.IntSlice(keys)) { // did we consume every partition in order?
			t.Errorf("partition %d does not have sorted keys", part)
		}
		allKeys = append(allKeys, keys...)
	}

	if len(allKeys) != recordLimit {
		t.Fatalf("got %d keys != exp %d", len(allKeys), recordLimit)
	}

	sort.Ints(allKeys)
	for i := 0; i < recordLimit; i++ {
		if allKeys[i] != i {
			t.Fatalf("got key %d != exp %d", allKeys[i], i)
		}
	}
}

// This test is similar to the one above (especially in the setup), but uses a
// transactional producer and two levels of consuming. The first level does a
// minor transformation before re-publishing.
func TestProduceConsumeGroupTxn(t *testing.T) {
	t.Parallel()

	topic1, topic1Cleanup := tmpTopic(t)
	defer topic1Cleanup()

	errs := make(chan error)
	body := []byte(strings.Repeat(randsha(), 100)) // a large enough body

	////////////////////
	// PRODUCER START //
	////////////////////

	go func() {
		cl, _ := NewClient(TransactionalID("p" + randsha()))
		defer cl.Close()

		var offsetsMu sync.Mutex
		offsets := make(map[int32]int64)
		partsUsed := make(map[int32]struct{})

		if err := cl.BeginTransaction(); err != nil {
			errs <- fmt.Errorf("unable to begin transaction: %v", err)
		}
		defer func() {
			if err := cl.Flush(context.Background()); err != nil {
				errs <- fmt.Errorf("unable to flush: %v", err)
			}
			if err := cl.EndTransaction(context.Background(), true); err != nil {
				errs <- fmt.Errorf("unable to end transaction: %v", err)
			}
		}()
		for i := 0; i < recordLimit; i++ {
			// We start with a transaction, and every 10k records
			// we commit and begin a new one.
			if i > 0 && i%10000 == 0 {
				if err := cl.Flush(context.Background()); err != nil {
					errs <- fmt.Errorf("unable to flush: %v", err)
				}
				// Control markers ending a transaction take up
				// one record offset, so for all partitions that
				// were used in the txn, we bump their offset.
				for partition := range partsUsed {
					offsets[partition]++
				}
				if err := cl.EndTransaction(context.Background(), true); err != nil {
					errs <- fmt.Errorf("unable to end transaction: %v", err)
				}
				if err := cl.BeginTransaction(); err != nil {
					errs <- fmt.Errorf("unable to begin transaction: %v", err)
				}
			}

			myKey := []byte(strconv.Itoa(i))
			cl.Produce(
				context.Background(),
				&Record{
					Topic: topic1,
					Key:   myKey,
					Value: body,
				},
				func(r *Record, err error) {
					if err != nil {
						errs <- fmt.Errorf("unexpected produce err: %v", err)
					}
					if !bytes.Equal(r.Key, myKey) {
						errs <- fmt.Errorf("unexpected out of order key; got %s != exp %v", r.Key, myKey)
					}

					// ensure the offsets for this partition are contiguous
					offsetsMu.Lock()
					current, ok := offsets[r.Partition]
					if ok && r.Offset != current+1 {
						errs <- fmt.Errorf("partition %d produced offsets out of order, got %d != exp %d", r.Partition, r.Offset, current+1)
					} else if !ok && r.Offset != 0 {
						errs <- fmt.Errorf("expected first produced record to partition to have offset 0, got %d", r.Offset)
					}
					offsets[r.Partition] = r.Offset
					partsUsed[r.Partition] = struct{}{}
					offsetsMu.Unlock()
				},
			)
		}

	}()

	///////////////////////////
	// CONSUMER LEVELS SETUP //
	///////////////////////////

	var (
		/////////////
		// LEVEL 1 //
		/////////////

		group1, groupCleanup1 = tmpGroup(t)
		topic2, topic2Cleanup = tmpTopic(t)

		consumers1 = newTxnConsumer(
			errs,
			topic1,
			topic2,
			group1,
			body,
		)

		/////////////
		// LEVEL 2 //
		/////////////

		group2, groupCleanup2 = tmpGroup(t)
		topic3, topic3Cleanup = tmpTopic(t)

		consumers2 = newTxnConsumer(
			errs,
			topic2,
			topic3,
			group2,
			body,
		)

		/////////////
		// LEVEL 3 //
		/////////////

		group3, groupCleanup3 = tmpGroup(t)
		topic4, topic4Cleanup = tmpTopic(t)

		consumers3 = newTxnConsumer(
			errs,
			topic3,
			topic4,
			group3,
			body,
		)
	)

	defer func() {
		groupCleanup1()
		groupCleanup2()
		groupCleanup3()
		topic2Cleanup()
		topic3Cleanup()
		topic4Cleanup()
	}()

	////////////////////
	// CONSUMER START //
	////////////////////

	for i := 0; i < 3; i++ { // three consumers start with standard poll&commit behavior
		consumers1.gorun(-1)
		consumers2.gorun(-1)
		consumers3.gorun(-1)
	}

	consumers1.gorun(0) // bail immediately
	consumers1.gorun(2) // bail after two txns
	consumers2.gorun(2) // same

	time.Sleep(5 * time.Second)
	for i := 0; i < 3; i++ { // trigger rebalance after 5s with more consumers
		consumers1.gorun(-1)
		consumers2.gorun(-1)
		consumers3.gorun(-1)
	}

	consumers2.gorun(0) // bail immediately
	consumers1.gorun(1) // bail after one txn

	doneConsume := make(chan struct{})
	go func() {
		consumers1.wait()
		consumers2.wait()
		consumers3.wait()
		close(doneConsume)
	}()

	//////////////////////
	// FINAL VALIDATION //
	//////////////////////

out:
	for {
		select {
		case err := <-errs:
			t.Fatal(err)
		case <-doneConsume:
			break out
		}
	}

	for level, part2key := range []map[int32][]int{
		consumers1.part2key,
		consumers2.part2key,
		consumers3.part2key,
	} {
		allKeys := make([]int, 0, recordLimit) // did we receive everything we sent?
		for part, keys := range part2key {
			if !sort.IsSorted(sort.IntSlice(keys)) { // did we consume every partition in order?
				t.Errorf("consumers %d: partition %d does not have sorted keys", level, part)
			}
			allKeys = append(allKeys, keys...)
		}

		if len(allKeys) != recordLimit {
			t.Fatalf("consumers %d: got %d keys != exp %d", level, len(allKeys), recordLimit)
		}

		sort.Ints(allKeys)
		for i := 0; i < recordLimit; i++ {
			if allKeys[i] != i {
				t.Fatalf("consumers %d: got key %d != exp %d", level, allKeys[i], i)
			}
		}
	}
}

type partOffset struct {
	part   int32
	offset int64
}

type txnConsumer struct {
	errCh chan<- error

	consumeFrom string
	produceTo   string

	group string

	expBody []byte // what every record body should be

	consumed uint64 // shared atomically

	wg sync.WaitGroup
	mu sync.Mutex

	// part2key tracks keys we have consumed from which partition
	part2key map[int32][]int

	// part2offssets tracks offsets we have consumed per partition; must be
	// strictly increasing
	part2offsets map[int32][]int64

	// partOffsets tracks partition offsets we have consumed to ensure no
	// duplicates
	partOffsets map[partOffset]struct{}
}

func newTxnConsumer(
	errCh chan error,
	consumeFrom string,
	produceTo string,
	group string,
	expBody []byte,
) *txnConsumer {
	return &txnConsumer{
		errCh: errCh,

		consumeFrom: consumeFrom,
		produceTo:   produceTo,

		group: group,

		expBody: expBody,

		part2key:     make(map[int32][]int),
		part2offsets: make(map[int32][]int64),
		partOffsets:  make(map[partOffset]struct{}),
	}
}

func (c *txnConsumer) gorun(txnsBeforeQuit int) {
	c.wg.Add(1)
	go c.run(txnsBeforeQuit)
}

func (c *txnConsumer) wait() {
	c.wg.Wait()
}

func (c *txnConsumer) run(txnsBeforeQuit int) {
	defer c.wg.Done()
	cl, _ := NewClient(
		TransactionalID(randsha()),
		WithLogger(BasicLogger(testLogLevel, nil)),
		// Control records have their own unique offset, so for testing,
		// we keep the record to ensure we do not doubly consume control
		// records (unless aborting).
		KeepControlRecords(),
		FetchIsolationLevel(ReadCommitted()),
	)
	defer cl.Close()

	txnSess := cl.AssignGroupTransactSession(c.group,
		GroupTopics(c.consumeFrom),
		Balancers(CooperativeStickyBalancer()),
	)
	defer cl.AssignGroup("") // leave group to allow for group deletion

	ntxns := 0 // for if txnsBeforeQuit is non-negative

	for {
		// We poll with a short timeout so that we do not hang waiting
		// at the end if another consumer hit the limit.
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		fetches := cl.PollFetches(ctx)
		cancel()
		if len(fetches) == 0 {
			if consumed := atomic.LoadUint64(&c.consumed); consumed == recordLimit {
				return
			} else if consumed > recordLimit {
				panic("invalid: consumed too much")
			}
			continue
		}

		if fetchErrs := fetches.Errors(); len(fetchErrs) > 0 {
			c.errCh <- fmt.Errorf("poll got unexpected errs: %v", fetchErrs)
		}

		if err := txnSess.Begin(); err != nil {
			c.errCh <- fmt.Errorf("BeginTransaction unexpected err: %v", err)
		}

		// We save everything we consume in fetchRecs and only account
		// for the consumption if our transaction is successful.
		type fetchRec struct {
			offset  int64
			num     int // key num
			control bool
		}
		fetchRecs := make(map[int32][]fetchRec)

		for iter := fetches.RecordIter(); !iter.Done(); {
			r := iter.Next()

			if r.Attrs.IsControl() {
				fetchRecs[r.Partition] = append(fetchRecs[r.Partition], fetchRec{offset: r.Offset, control: true})
				continue
			} else {
				keyNum, err := strconv.Atoi(string(r.Key))
				if err != nil {
					c.errCh <- err
				}
				if !bytes.Equal(r.Value, c.expBody) {
					c.errCh <- fmt.Errorf("body not what was expected")
				}
				fetchRecs[r.Partition] = append(fetchRecs[r.Partition], fetchRec{offset: r.Offset, num: keyNum})
			}

			cl.Produce(
				context.Background(),
				&Record{
					Topic: c.produceTo,
					Key:   r.Key,
					Value: r.Value,
				},
				func(_ *Record, err error) {
					if err != nil {
						c.errCh <- fmt.Errorf("unexpected transactional produce err: %v", err)
					}
				},
			)
		}

		wantCommit := txnsBeforeQuit < 0 || ntxns < txnsBeforeQuit

		committed, err := txnSess.End(context.Background(), TransactionEndTry(wantCommit))
		if err != nil {
			c.errCh <- fmt.Errorf("flush unexpected err: %v", err)
		} else if !committed {
			if !wantCommit {
				return
			}
			continue
		}

		ntxns++

		c.mu.Lock()

		for part, recs := range fetchRecs {
			for _, rec := range recs {
				po := partOffset{part, rec.offset}
				if _, exists := c.partOffsets[po]; exists {
					c.errCh <- fmt.Errorf("saw double offset p%do%d", po.part, po.offset)
				}
				c.partOffsets[po] = struct{}{}

				offsetOrdering := c.part2offsets[part]
				if len(offsetOrdering) > 0 && rec.offset < offsetOrdering[len(offsetOrdering)-1]+1 {
					c.errCh <- fmt.Errorf("part %d last offset %d; this offset %d; expected increasing", part, offsetOrdering[len(offsetOrdering)-1], rec.offset)
				}
				c.part2offsets[part] = append(offsetOrdering, rec.offset)

				if !rec.control {
					c.part2key[part] = append(c.part2key[part], rec.num)
					atomic.AddUint64(&c.consumed, 1)
				}
			}

		}
		c.mu.Unlock()
	}

}
