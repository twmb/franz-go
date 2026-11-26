package kfake

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestAuditTxnEndTxnUnknownServerErrorNotFalseCommit verifies that an
// UNKNOWN_SERVER_ERROR from EndTxn during a GroupTransactSession commit is NOT
// reported as a successful commit, and that a resent commit that succeeds is.
//
// GroupTransactSession.End calls EndTransaction again with the same
// TransactionEndTry after UNKNOWN_SERVER_ERROR, and EndTransaction(ctx,
// TryCommit) resends the commit. Previously, a retry that returned nil without
// sending anything was reported as a commit, and the consumer offsets advanced
// for a transaction the broker may have aborted: a silent EOS data loss.
//
// Some brokers (e.g. Redpanda in certain versions) return UNKNOWN_SERVER_ERROR
// from EndTxn; conformant Kafka / kfake does not emit it on their own, so the
// test injects it via a fault.
//
// A resent commit answered with INVALID_TXN_STATE is definitive and ends End,
// even when a producer ID reload would get UNKNOWN_SERVER_ERROR.
func TestAuditTxnEndTxnUnknownServerErrorNotFalseCommit(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name          string
		faults        int         // EndTxn requests that get UNKNOWN_SERVER_ERROR; -1 is every one
		then          *kerr.Error // if non-nil, the next EndTxn gets this answer, and an InitProducerID gets UNKNOWN_SERVER_ERROR
		wantCommitted bool
	}{
		{"every-end", -1, nil, false},
		{"one-end", 1, nil, true},
		{"resend-invalid-txn-state", 1, kerr.InvalidTxnState, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			const (
				topic = "audit-endtxn-use"
				group = "audit-endtxn-use-g"
				msgs  = 4
			)

			c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
			produceN(t, c, topic, msgs)

			s, err := kgo.NewGroupTransactSession(
				kgo.SeedBrokers(c.ListenAddrs()...),
				kgo.ConsumeTopics(topic),
				kgo.ConsumerGroup(group),
				kgo.TransactionalID("audit-endtxn-use-id"),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
				kgo.FetchMaxWait(100*time.Millisecond),
			)
			if err != nil {
				t.Fatal(err)
			}
			defer s.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			// Consume all records so the commit has real offsets to advance
			// (and so the TxnOffsetCommit negotiates KIP-447, avoiding End's
			// pre-447 sleep).
			if err := s.Begin(); err != nil {
				t.Fatal(err)
			}
			var got int
			for ctx.Err() == nil && got < msgs {
				fetches := s.PollRecords(ctx, msgs-got)
				if err := fetches.Err0(); err != nil {
					t.Fatalf("poll error: %v", err)
				}
				got += len(fetches.Records())
			}
			if got < msgs {
				t.Fatalf("polled %d/%d records", got, msgs)
			}

			endtxns := c.Fault(Fault{
				Keys:  []kmsg.Key{kmsg.EndTxn},
				Err:   kerr.UnknownServerError,
				Count: test.faults,
			})
			if test.then != nil {
				c.Fault(Fault{Keys: []kmsg.Key{kmsg.EndTxn}, Err: test.then})
				c.Fault(Fault{Keys: []kmsg.Key{kmsg.InitProducerID}, Err: kerr.UnknownServerError})
			}

			committed, err := s.End(ctx, kgo.TryCommit)
			if endtxns.Hits() == 0 {
				t.Fatalf("expected the EndTxn fault to fire at least once, got 0 (err=%v)", err)
			}
			if committed != test.wantCommitted || (err == nil) != test.wantCommitted {
				t.Fatalf("End returned committed=%v err=%v, want committed=%v", committed, err, test.wantCommitted)
			}
		})
	}
}
