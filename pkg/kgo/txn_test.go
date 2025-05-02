package kgo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"testing"
	"time"
)

// This test is identical to TestGroupETL but based around transactions.
func TestTxnEtl(t *testing.T) {
	t.Parallel()

	topic1, topic1Cleanup := tmpTopic(t)
	t.Cleanup(topic1Cleanup)

	errs := make(chan error)
	body := []byte(randsha()) // a small body so we do not flood RAM

	////////////////////
	// PRODUCER START //
	////////////////////

	go func() {
		cl, err := newTestClient(
			WithLogger(BasicLogger(os.Stderr, testLogLevel, func() string {
				return time.Now().UTC().Format("15:04:05.999") + " "
			})),
			TransactionalID("p"+randsha()),
			TransactionTimeout(2*time.Minute),
			MaxBufferedRecords(10000),
			UnknownTopicRetries(-1), // see comment below
		)
		if err != nil {
			panic(err)
		}

		defer cl.Close()

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
		var safeUnsafe bool
		for i := 0; i < testRecordLimit; i++ {
			// We start with a transaction, and every 10k records
			// we commit and begin a new one.
			if i > 0 && i%10000 == 0 {
				how := EndBeginTxnSafe
				if safeUnsafe && allowUnsafe {
					how = EndBeginTxnUnsafe
				}
				safeUnsafe = !safeUnsafe
				if err := cl.EndAndBeginTransaction(context.Background(), how, TryCommit, func(_ context.Context, err error) error {
					if err != nil {
						errs <- fmt.Errorf("unable to end transaction: %v", err)
					}
					return err
				}); err != nil {
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

					// ensure the offsets for this partition are monotonically increasing
					current, ok := offsets[r.Partition]
					if ok && r.Offset <= current {
						errs <- fmt.Errorf("partition %d produced offsets out of order, got %d != exp %d", r.Partition, r.Offset, current+1)
					}
					offsets[r.Partition] = r.Offset
					partsUsed[r.Partition] = struct{}{}
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
				true,
				tc.balancer,
			)
		})
	}
}

func (c *testConsumer) goTransact(txnsBeforeQuit int) {
	c.wg.Add(1)
	go c.transact(txnsBeforeQuit)
}

func (c *testConsumer) transact(txnsBeforeQuit int) {
	defer c.wg.Done()

	txid := randsha()
	opts := []Opt{
		// Kraft sometimes returns success from topic creation, and
		// then returns UnknownTopicXyz for a while in metadata loads.
		// It also returns NotLeaderXyz; we handle both problems.
		UnknownTopicRetries(-1),
		TransactionalID(txid),
		TransactionTimeout(60 * time.Second),
		WithLogger(testLogger()),
		// Control records have their own unique offset, so for testing,
		// we keep the record to ensure we do not doubly consume control
		// records (unless aborting).
		KeepControlRecords(),
		ConsumerGroup(c.group),
		ConsumeTopics(c.consumeFrom),
		FetchIsolationLevel(ReadCommitted()),
		Balancers(c.balancer),
		MaxBufferedRecords(10000),
		WithPools(new(primitivePool)),
	}
	if requireStableFetch {
		opts = append(opts, RequireStableFetchOffsets())
	}
	opts = append(opts, testClientOpts()...)

	txnSess, _ := NewGroupTransactSession(opts...)
	defer txnSess.Close()

	ntxns := 0 // for if txnsBeforeQuit is non-negative

	for {
		// We poll with a short timeout so that we do not hang waiting
		// at the end if another consumer hit the limit.
		ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
		fetches := txnSess.PollFetches(ctx)
		cancel()
		if err := fetches.Err(); err == context.DeadlineExceeded || err == context.Canceled || err == ErrClientClosed {
			if consumed := int(c.consumed.Load()); consumed == testRecordLimit {
				return
			} else if consumed > testRecordLimit {
				panic(fmt.Sprintf("invalid: consumed too much from %s (at %d, group %s, tx %s)", c.consumeFrom, consumed, c.group, txid))
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
			}
			keyNum, err := strconv.Atoi(string(r.Key))
			if err != nil {
				c.errCh <- err
			}
			if !bytes.Equal(r.Value, c.expBody) {
				c.errCh <- fmt.Errorf("body not what was expected")
			}
			fetchRecs[r.Partition] = append(fetchRecs[r.Partition], fetchRec{offset: r.Offset, num: keyNum})

			txnSess.Produce(
				context.Background(),
				&Record{
					Topic: c.produceTo,
					Key:   slices.Clone(r.Key),
					Value: slices.Clone(r.Value),
				},
				func(_ *Record, err error) {
					if err != nil && !errors.Is(err, ErrAborting) {
						c.errCh <- fmt.Errorf("unexpected transactional produce err: %v", err)
					}
				},
			)
			r.Recycle() // we take care above to copy necessary fields
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
					c.errCh <- fmt.Errorf("saw double offset t %s p%do%d", c.consumeFrom, po.part, po.offset)
				}
				c.partOffsets[po] = struct{}{}

				if !rec.control {
					c.part2key[part] = append(c.part2key[part], rec.num)
					c.consumed.Add(1)
				}
			}
		}
		c.mu.Unlock()
	}
}
