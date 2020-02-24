package kgo

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os/exec"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/kafka-go/pkg/kmsg"
)

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

func TestProduceConsumeGroup(t *testing.T) {
	topic, topicCleanup := tmpTopic(t)
	defer topicCleanup()

	const limit = 1000000

	errs := make(chan error, 100)
	body := []byte(strings.Repeat(randsha(), 100))

	// Begin a goroutine to produce all of our records.
	go func() {
		cl, err := NewClient(WithLogger(BasicLogger(LogLevelDebug, true)))
		if err != nil {
			errs <- err
		}
		defer cl.Close()
		var finalPartsMu sync.Mutex
		finalParts := make(map[int32]int64)
		var wg sync.WaitGroup
		for i := 0; i < limit; i++ {
			myKey := strconv.Itoa(i)
			wg.Add(1)
			cl.Produce(
				context.Background(),
				&Record{
					Topic: topic,
					Key:   []byte(myKey),
					Value: body,
				},
				func(r *Record, err error) {
					defer wg.Done()
					if err != nil {
						errs <- fmt.Errorf("unexpected produce err: %v", err)
					}
					if got := string(r.Key); got != myKey {
						errs <- fmt.Errorf("unexpected out of order key; got %s != exp %v", got, myKey)
					}

					finalPartsMu.Lock()
					current, ok := finalParts[r.Partition]
					if ok {
						if r.Offset != current+1 {
							errs <- fmt.Errorf("partition produced offsets out of order, got %d != exp %d", r.Offset, current+1)
						}
					} else {
						if r.Offset != 0 {
							errs <- fmt.Errorf("expected first produced record to partition to have offset 0, got %d", r.Offset)
						}
					}
					finalParts[r.Partition] = r.Offset
					finalPartsMu.Unlock()
				},
			)
		}
		// Wait for all our records to be published before closing the
		// client, which otherwise would kill all buffered unflushed
		// records.
		wg.Wait()

		fmt.Println("FINAL OFFSETS FOR PARTITIONS", finalParts) // for debugging
		total := int64(0)
		for _, offsets := range finalParts {
			total += offsets + 1
		}
		fmt.Println("TOTAL", total)
	}()

	group, groupCleanup := tmpGroup(t)
	defer groupCleanup()

	var wgGroup sync.WaitGroup
	var consumed uint64
	var part2KeyMu sync.Mutex
	part2key := make(map[int32][]int)

	type partOffset struct {
		part   int32
		offset int64
	}
	partOffsets := make(map[partOffset]bool)

	addConsumer := func() {
		wgGroup.Add(1)
		go func() {
			l := BasicLogger(LogLevelDebug, true)
			cl, _ := NewClient(WithLogger(l))
			defer wgGroup.Done()
			defer cl.Close()

			// Since we are committing manually after every poll, we must ensure
			// our commits are syncronous.
			var syncCommit sync.Mutex
			commit := func() {
				syncCommit.Lock()
				defer syncCommit.Unlock()
				committed := make(chan struct{})
				cl.CommitOffsets(
					context.Background(),
					cl.Uncommitted(),
					func(_ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, err error) {
						defer close(committed)
						// We should have no errors, not even context canceled errors,
						// since we are ensuring every commit, even the commit in
						// onRevoke, is syncronous and does not cancel any prior.
						if err != nil {
							errs <- fmt.Errorf("unable to commit offsets: %v", err)
						}
					},
				)
				<-committed
			}

			cl.AssignGroup(group,
				GroupTopics(topic),
				DisableAutocommit(),
				OnRevoked(func(ctx context.Context, _ map[string][]int32) {
					l.Log(LogLevelDebug, "in revoke")
					defer l.Log(LogLevelDebug, "left revoke")
					commit()
				}),
			)

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
					part2KeyMu.Lock()
					if partOffsets[partOffset{r.Partition, r.Offset}] {
						l.Log(LogLevelDebug, "ME FAIL")
						panic(fmt.Sprintf("SAW DOUBLE OFFSET %d %d", r.Partition, r.Offset))
					}
					partOffsets[partOffset{r.Partition, r.Offset}] = true
					part2key[r.Partition] = append(part2key[r.Partition], keyNum)
					part2KeyMu.Unlock()

					atomic.AddUint64(&consumed, 1)
				}

				commit()
				fmt.Println("TOTAL READ", atomic.LoadUint64(&consumed))

				if atomic.LoadUint64(&consumed) == limit {
					break
				}
			}
		}()
	}

	// Have three consumers generically poll and commit.
	for i := 0; i < 3; i++ {
		addConsumer()
	}

	// Have one consumer quit immediately after polling, causing others to
	// rebalance.
	wgGroup.Add(1)
	go func() {
		cl, _ := NewClient(WithLogger(BasicLogger(LogLevelDebug, true)))
		defer wgGroup.Done()

		cl.AssignGroup(group,
			GroupTopics(topic),
			DisableAutocommit(),
		)
		defer cl.AssignGroup("") // leave group to allow for group deletion

		// we quit immediately
		fetches := cl.PollFetches(context.Background())
		if fetchErrs := fetches.Errors(); len(fetchErrs) > 0 {
			errs <- fmt.Errorf("poll got unexpected errs: %v", fetchErrs)
		}
	}()

	// Wait 6s (past initial 3s wait period) to add two more consumers that
	// will trigger other consumers to lose some partitions.
	time.Sleep(6 * time.Second)
	for i := 0; i < 3; i++ {
		addConsumer()
	}

	doneConsume := make(chan struct{})
	go func() {
		wgGroup.Wait()
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

	allKeys := make([]int, 0, limit)
	for part, keys := range part2key {
		if !sort.IsSorted(sort.IntSlice(keys)) {
			t.Errorf("partition %d does not have sorted keys", part)
		}
		allKeys = append(allKeys, keys...)
	}

	if len(allKeys) != limit {
		t.Fatalf("got %d keys != exp %d", len(allKeys), limit)
	}

	sort.Ints(allKeys)

	for i := 0; i < limit; i++ {
		if allKeys[i] != i {
			t.Fatalf("got key %d != exp %d", allKeys[i], i)
		}
	}
}
