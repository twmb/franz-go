package kgo

import (
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

const testRecordLimit = 1000000

var loggerNum int64

func testLogger() Logger {
	num := atomic.AddInt64(&loggerNum, 1)
	pfx := fmt.Sprintf("[%d] ", num)
	return BasicLogger(os.Stderr, testLogLevel, func() string {
		return pfx
	})
}

var testLogLevel = func() LogLevel {
	level := strings.ToLower(os.Getenv("KGO_LOG_LEVEL"))
	switch level {
	case "debug":
		return LogLevelDebug
	case "none":
		return LogLevelNone
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

	cmd := exec.Command(path, "admin", "topic", "create", topic, "-p", "20")
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
		cmd := exec.Command(path, "admin", "topic", "delete", topic)
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
		cmd := exec.Command(path, "admin", "group", "delete", group)
		output, err := cmd.CombinedOutput()
		if err != nil {
			tb.Fatalf("unable to run kcl group delete command: %v", err)
		}
		if !okRe.Match(output) {
			tb.Fatalf("group delete failed\n%s", output)
		}
	}
}

// testConsumer performs ETL inside or outside transactions;
// the ETL functions are defined in group_test.go and txn_test.go
type testConsumer struct {
	errCh chan<- error

	consumeFrom string
	produceTo   string

	group    string
	balancer GroupBalancer

	expBody []byte // what every record body should be

	consumed uint64 // shared atomically

	wg sync.WaitGroup
	mu sync.Mutex

	// part2key tracks keys we have consumed from which partition
	part2key map[int32][]int

	// partOffsets tracks partition offsets we have consumed to ensure no
	// duplicates
	partOffsets map[partOffset]struct{}
}

type partOffset struct {
	part   int32
	offset int64
}

func newTestConsumer(
	errCh chan error,
	consumeFrom string,
	produceTo string,
	group string,
	balancer GroupBalancer,
	expBody []byte,
) *testConsumer {
	return &testConsumer{
		errCh: errCh,

		consumeFrom: consumeFrom,
		produceTo:   produceTo,

		group:    group,
		balancer: balancer,

		expBody: expBody,

		part2key:    make(map[int32][]int),
		partOffsets: make(map[partOffset]struct{}),
	}
}

func (c *testConsumer) wait() {
	c.wg.Wait()
}

func (c *testConsumer) goRun(transactional bool, etlsBeforeQuit int) {
	if transactional {
		c.goTransact(etlsBeforeQuit)
	} else {
		c.goGroupETL(etlsBeforeQuit)
	}
}

func testChainETL(
	t *testing.T,
	topic1 string,
	body []byte,
	errs chan error,
	transactional bool,
	balancer GroupBalancer,
) {
	t.Helper()

	var (
		/////////////
		// LEVEL 1 //
		/////////////

		group1, groupCleanup1 = tmpGroup(t)
		topic2, topic2Cleanup = tmpTopic(t)

		consumers1 = newTestConsumer(
			errs,
			topic1,
			topic2,
			group1,
			balancer,
			body,
		)

		/////////////
		// LEVEL 2 //
		/////////////

		group2, groupCleanup2 = tmpGroup(t)
		topic3, topic3Cleanup = tmpTopic(t)

		consumers2 = newTestConsumer(
			errs,
			topic2,
			topic3,
			group2,
			balancer,
			body,
		)

		/////////////
		// LEVEL 3 //
		/////////////

		group3, groupCleanup3 = tmpGroup(t)
		topic4, topic4Cleanup = tmpTopic(t)

		consumers3 = newTestConsumer(
			errs,
			topic3,
			topic4,
			group3,
			balancer,
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
		consumers1.goRun(transactional, -1)
		consumers2.goRun(transactional, -1)
		consumers3.goRun(transactional, -1)
	}

	consumers1.goRun(transactional, 0) // bail immediately
	consumers1.goRun(transactional, 2) // bail after two txns
	consumers2.goRun(transactional, 2) // same

	time.Sleep(5 * time.Second)
	for i := 0; i < 3; i++ { // trigger rebalance after 5s with more consumers
		consumers1.goRun(transactional, -1)
		consumers2.goRun(transactional, -1)
		consumers3.goRun(transactional, -1)
	}

	consumers2.goRun(transactional, 0) // bail immediately
	consumers1.goRun(transactional, 1) // bail after one txn

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
		allKeys := make([]int, 0, testRecordLimit) // did we receive everything we sent?
		for _, keys := range part2key {
			allKeys = append(allKeys, keys...)
		}

		if len(allKeys) != testRecordLimit {
			t.Fatalf("consumers %d: got %d keys != exp %d", level, len(allKeys), testRecordLimit)
		}

		sort.Ints(allKeys)
		for i := 0; i < testRecordLimit; i++ {
			if allKeys[i] != i {
				t.Fatalf("consumers %d: got key %d != exp %d", level, allKeys[i], i)
			}
		}
	}
}
