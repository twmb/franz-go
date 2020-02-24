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
			tb.Fatalf("group delete failed")
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
	topic, topicCleanup := tmpTopic(t)
	defer topicCleanup()

	const limit = 1000000

	errs := make(chan error, 100)
	body := []byte(strings.Repeat(randsha(), 100)) // a large enough body

	// Begin a goroutine to produce all of our records.
	go func() {
		cl, _ := NewClient(WithLogger(BasicLogger(testLogLevel, true)))
		defer cl.Close()

		var offsetsMu sync.Mutex
		offsets := make(map[int32]int64)

		var wg sync.WaitGroup
		defer func() { wg.Wait() }() // wait, otherwise we fail unflushed records at the end
		for i := 0; i < limit; i++ {
			myKey := []byte(strconv.Itoa(i))
			wg.Add(1)
			cl.Produce(
				context.Background(),
				&Record{
					Topic: topic,
					Key:   myKey,
					Value: body,
				},
				func(r *Record, err error) {
					defer wg.Done()
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

	group, groupCleanup := tmpGroup(t)
	defer groupCleanup()

	type partOffset struct {
		part   int32
		offset int64
	}
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
			l := BasicLogger(testLogLevel, true)
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

				if atomic.LoadUint64(&consumed) == limit {
					break
				}
			}
		}()
	}

	for i := 0; i < 3; i++ { // three consumers start with standard poll&commit behavior
		addConsumer()
	}

	// Have one consumer quit immediately after polling, causing others to
	// rebalance.
	wg.Add(1)
	go func() {
		cl, _ := NewClient(WithLogger(BasicLogger(testLogLevel, true)))
		defer wg.Done()

		cl.AssignGroup(group,
			GroupTopics(topic),
			DisableAutocommit(), // we do not want to commit since we do not process records
		)
		defer cl.AssignGroup("") // leave group to allow for group deletion

		// we quit immediately
		fetches := cl.PollFetches(context.Background())
		if fetchErrs := fetches.Errors(); len(fetchErrs) > 0 {
			errs <- fmt.Errorf("poll got unexpected errs: %v", fetchErrs)
		}
	}()

	// Wait 5s (past initial 3s wait period) to add two more consumers that
	// will trigger other consumers to lose some partitions.
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

	allKeys := make([]int, 0, limit) // did we receive everything we sent?
	for part, keys := range part2key {
		if !sort.IsSorted(sort.IntSlice(keys)) { // did we consume every partition in order?
			t.Errorf("partition %d does not have sorted keys", part)
		}
		allKeys = append(allKeys, keys...)
	}

	if len(allKeys) != limit {
		t.Fatalf("got %d keys != exp %d", len(allKeys), limit)
	}

	sort.Ints(allKeys) // likely unnecessary due to our duplicate checking, but just for sanity
	for i := 0; i < limit; i++ {
		if allKeys[i] != i {
			t.Fatalf("got key %d != exp %d", allKeys[i], i)
		}
	}
}
