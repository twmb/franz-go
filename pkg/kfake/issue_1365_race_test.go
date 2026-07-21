package kfake_test

import (
	"context"
	"math/rand"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Regression for #1365: a mid-life cooperative -> eager protocol downgrade
// used to assign offsets over cursors that were still fetching. Member A
// advertises [cooperative-sticky, sticky] and consumes hot; member B joins
// advertising only [sticky], so the group's protocol vote is forced to
// sticky. A's prior cooperative session revoked nothing at its end, and the
// eager session's diffAssigned returns the entire assignment as newly added:
// without the downgrade revoke, fetchOffsets issued list/epoch loads for
// live cursors and the load's completion raced concurrent polls (data race
// on the cursor offset, plus torn cursor state).
//
// Run with -race. Pre-fix this raced in most runs; post-fix it must be
// clean, and all partitions must resume consuming after the downgrade.
func TestIssue1365CooperativeDowngrade(t *testing.T) {
	t.Parallel()
	const (
		topic      = "issue-1365-downgrade"
		group      = "issue-1365-downgrade-g"
		partitions = 6
	)
	c := newCluster(t, kfake.SeedTopics(partitions, topic))

	// Keep epoch/list loads in flight longer: pre-fix, the race window is
	// a load completing while records are buffered and being polled.
	delay := func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		c.SleepControl(func() { time.Sleep(time.Duration(50+rand.Intn(200)) * time.Millisecond) })
		return nil, nil, false
	}
	c.ControlKey(int16(kmsg.OffsetForLeaderEpoch), delay)
	c.ControlKey(int16(kmsg.ListOffsets), delay)

	producer := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
	prodCtx, prodCancel := context.WithCancel(context.Background())
	var prodWg sync.WaitGroup
	prodWg.Add(1)
	go func() {
		defer prodWg.Done()
		for i := 0; prodCtx.Err() == nil; i++ {
			r := kgo.StringRecord("v" + strconv.Itoa(i))
			r.Topic = topic
			r.Key = []byte(strconv.Itoa(i)) // spread across partitions
			producer.Produce(prodCtx, r, nil)
			if i%50 == 49 {
				producer.Flush(prodCtx)
			}
		}
	}()
	defer func() {
		prodCancel()
		prodWg.Wait()
	}()

	baseOpts := []kgo.Opt{
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(50 * time.Millisecond),
		kgo.AutoCommitMarks(),
		kgo.AutoCommitInterval(100 * time.Millisecond),
		kgo.HeartbeatInterval(100 * time.Millisecond),
	}

	// A: prefers cooperative, also supports eager sticky (the standard
	// migration config).
	a := newPlainClient(t, c, append([]kgo.Opt{
		kgo.Balancers(kgo.CooperativeStickyBalancer(), kgo.StickyBalancer()),
	}, baseOpts...)...)

	var mu sync.Mutex
	seen := make(map[int32]int) // partition => records consumed by A
	pollCtx, pollCancel := context.WithCancel(context.Background())
	var pollWg sync.WaitGroup
	pollWg.Add(1)
	go func() {
		defer pollWg.Done()
		for pollCtx.Err() == nil {
			fs := a.PollFetches(pollCtx)
			fs.EachRecord(func(r *kgo.Record) {
				a.MarkCommitRecords(r)
				mu.Lock()
				seen[r.Partition]++
				mu.Unlock()
			})
		}
	}()
	defer func() {
		pollCancel()
		pollWg.Wait()
	}()

	// Let A consume and commit (marks) so partitions have committed
	// offsets with leader epochs: the downgrade re-assign then issues
	// OffsetForLeaderEpoch loads for every partition.
	time.Sleep(750 * time.Millisecond)

	// B joins eager-only: the group protocol downgrades to sticky while A
	// has buffered fetches and is polling.
	b := newPlainClient(t, c, append([]kgo.Opt{
		kgo.Balancers(kgo.StickyBalancer()),
	}, baseOpts...)...)
	bctx, bcancel := context.WithTimeout(context.Background(), 3*time.Second)
	for bctx.Err() == nil {
		fs := b.PollFetches(bctx)
		if fs.NumRecords() > 0 {
			break
		}
	}
	bcancel()

	// Keep both consuming through the downgrade era, then B leaves.
	time.Sleep(500 * time.Millisecond)
	b.Close()

	// Post-downgrade functional check: A owns everything again and every
	// partition resumes consuming (the downgrade revoke must not strand
	// partitions).
	mu.Lock()
	clear(seen)
	mu.Unlock()
	deadline := time.Now().Add(10 * time.Second)
	for {
		mu.Lock()
		n := len(seen)
		mu.Unlock()
		if n == partitions {
			break
		}
		if time.Now().After(deadline) {
			mu.Lock()
			got := make(map[int32]int, len(seen))
			for p, c := range seen {
				got[p] = c
			}
			mu.Unlock()
			t.Fatalf("after downgrade and member leave, only %d/%d partitions resumed consuming: %v", n, partitions, got)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
