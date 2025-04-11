package kgo

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"
)

// TestGroupETL tests:
//
// - producing a lot of messages to a single topic, ensuring that all messages
// are produced in order.
//
// - a chain of consumer groups with members joining and leaving
//
// - that we never doubly consume records, and that all records are consumed in
// order
//
// - all balancers
func TestGroupETL(t *testing.T) {
	t.Parallel()

	topic1, topic1Cleanup := tmpTopic(t)
	t.Cleanup(topic1Cleanup)

	errs := make(chan error)
	body := []byte(randsha()) // a small body so we do not flood RAM

	////////////////////
	// PRODUCER START //
	////////////////////

	go func() {
		cl, _ := newTestClient(
			WithLogger(BasicLogger(os.Stderr, testLogLevel, nil)),
			MaxBufferedRecords(10000),
			MaxBufferedBytes(50000),
			UnknownTopicRetries(-1), // see txn_test comment
		)
		defer cl.Close()

		offsets := make(map[int32]int64)

		defer func() {
			if err := cl.Flush(context.Background()); err != nil {
				errs <- fmt.Errorf("unable to flush: %v", err)
			}
		}()
		for i := 0; i < testRecordLimit; i++ {
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
					current, ok := offsets[r.Partition]
					if ok && r.Offset != current+1 {
						errs <- fmt.Errorf("partition produced offsets out of order, got %d != exp %d", r.Offset, current+1)
					} else if !ok && r.Offset != 0 {
						errs <- fmt.Errorf("expected first produced record to partition to have offset 0, got %d", r.Offset)
					}
					offsets[r.Partition] = r.Offset
				},
			)
		}
	}()

	////////////////////////////
	// CONSUMER CHAINING TEST //
	////////////////////////////

	for _, tc := range []struct {
		name     string
		balancer GroupBalancer
	}{
		{"roundrobin", RoundRobinBalancer()},
		{"range", RangeBalancer()},
		{"sticky", StickyBalancer()},
		{"cooperative-sticky", CooperativeStickyBalancer()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testChainETL(
				t,
				topic1,
				body,
				errs,
				false,
				tc.balancer,
			)
		})
	}
}

func (c *testConsumer) goGroupETL(etlsBeforeQuit int) {
	c.wg.Add(1)
	go c.etl(etlsBeforeQuit)
}

func (c *testConsumer) etl(etlsBeforeQuit int) {
	defer c.wg.Done()

	netls := 0 // for if etlsBeforeQuit is non-negative

	opts := []Opt{
		UnknownTopicRetries(-1), // see txn_test comment
		WithLogger(testLogger()),
		ConsumerGroup(c.group),
		ConsumeTopics(c.consumeFrom),
		Balancers(c.balancer),
		MaxBufferedRecords(10000),
		MaxBufferedBytes(50000),
		ConsumePreferringLagFn(PreferLagAt(1)),
		BlockRebalanceOnPoll(),

		// Even with autocommitting, autocommitting does not commit
		// *the latest* when being revoked. We always want to commit
		// everything we have processed, because our loop below always
		// is successful. If we do not commit on revoke, we would have
		// duplicate processing.
		//
		// If we have etlsBeforeQuit, the behavior we want to trigger
		// is to *not* commit when we leave.
		//
		// Lastly, we do not want to fall back from OnPartitionsLost:
		// OnPartitionsLost should not be called due to us not
		// erroring, but we may as well explicitly disable it.
		OnPartitionsRevoked(func(ctx context.Context, cl *Client, _ map[string][]int32) {
			if etlsBeforeQuit >= 0 && netls >= etlsBeforeQuit {
				return
			}
			if err := cl.CommitUncommittedOffsets(ctx); err != nil {
				c.errCh <- fmt.Errorf("unable to commit in revoked: %v", err)
			}
		}),
		OnPartitionsLost(func(context.Context, *Client, map[string][]int32) {}),
	}

	cl, _ := newTestClient(opts...)
	defer cl.Close()

	defer func() {
		defer cl.LeaveGroup()

		if err := cl.Flush(context.Background()); err != nil {
			c.errCh <- fmt.Errorf("unable to flush: %v", err)
		}
	}()

	defer cl.AllowRebalance()
	for {
		cl.AllowRebalance()
		// If we etl a few times before quitting, then we want to
		// commit at least some of our work (except the last commit,
		// see above). To do so, we commit every time _before_ we poll.
		// Thus, the final poll will remain uncommitted.
		if etlsBeforeQuit > 0 {
			if err := cl.CommitUncommittedOffsets(context.Background()); err != nil {
				c.errCh <- fmt.Errorf("unable to commit: %v", err)
			}
		}

		// We poll with a short timeout so that we do not hang waiting
		// at the end if another consumer hit the limit.
		//
		// If the host is slow, the context could be canceled immediately
		// before we poll.
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		fetches := cl.PollRecords(ctx, 100)
		cancel()
		if err := fetches.Err(); err == context.DeadlineExceeded || err == context.Canceled || err == ErrClientClosed {
			if consumed := int(c.consumed.Load()); consumed == testRecordLimit {
				return
			} else if consumed > testRecordLimit {
				panic(fmt.Sprintf("invalid: consumed too much from %s (group %s)", c.consumeFrom, c.group))
			}
			if fetches.NumRecords() > 0 {
				c.errCh <- fmt.Errorf("poll got err %s but also unexpectedly received %d records", err, fetches.NumRecords())
			}
			continue
		}
		if fetchErrs := fetches.Errors(); len(fetchErrs) > 0 {
			c.errCh <- fmt.Errorf("poll got unexpected errs: %v", fetchErrs)
		}

		if etlsBeforeQuit >= 0 && netls >= etlsBeforeQuit {
			return
		}
		netls++

		for iter := fetches.RecordIter(); !iter.Done(); {
			r := iter.Next()
			keyNum, err := strconv.Atoi(string(r.Key))
			if err != nil {
				c.errCh <- err
			}
			if !bytes.Equal(r.Value, c.expBody) {
				c.errCh <- fmt.Errorf("body not what was expected")
			}

			c.mu.Lock()
			// check dup
			if _, exists := c.partOffsets[partOffset{r.Partition, r.Offset}]; exists {
				c.errCh <- fmt.Errorf("saw double offset t %s p%do%d", r.Topic, r.Partition, r.Offset)
			}
			c.partOffsets[partOffset{r.Partition, r.Offset}] = struct{}{}

			// save key for later for all-keys-consumed validation
			c.part2key[r.Partition] = append(c.part2key[r.Partition], keyNum)
			c.mu.Unlock()

			c.consumed.Add(1)

			cl.Produce(
				context.Background(),
				&Record{
					Topic: c.produceTo,
					Key:   r.Key,
					Value: r.Value,
				},
				func(_ *Record, err error) {
					if err != nil {
						c.errCh <- fmt.Errorf("unexpected etl produce err: %v", err)
					}
				},
			)
			r.Recycle() // should be a no-op since we are not using pooling
		}
	}
}
