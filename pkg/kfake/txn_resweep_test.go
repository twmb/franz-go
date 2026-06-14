package kfake

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Regression test from the txn.go ledger re-sweep (round 23, Phase 2).

// TestAuditTxnEndTxnUnknownServerErrorNotFalseCommit verifies that an
// UNKNOWN_SERVER_ERROR from EndTxn during a GroupTransactSession commit is NOT
// reported as a successful commit.
//
// GroupTransactSession.End retries EndTransaction on a few error codes. The
// OperationNotAttempted and TransactionAbortable arms both downgrade to an
// abort (willTryCommit = false) before retrying, because EndTransaction
// consumes inTxn on its erroring call and a re-call no-ops at its !inTxn
// guard, returning nil. The UNKNOWN_SERVER_ERROR arm omitted that downgrade:
// the no-op retry returned nil while willTryCommit stayed true, so End's
// success tail reported a committed transaction and advanced the consumer
// offsets to postcommit even though the broker's UNKNOWN_SERVER_ERROR left the
// commit unconfirmed (the broker may have aborted): a silent EOS data loss.
//
// Some brokers (e.g. Redpanda in certain versions) return UNKNOWN_SERVER_ERROR
// from EndTxn; conformant Kafka / kfake does not emit it on their own, so the
// test injects it via a control. Pre-fix End(TryCommit) returns committed=true;
// post-fix it returns committed=false and rewinds to the last committed
// offsets (at-least-once).
func TestAuditTxnEndTxnUnknownServerErrorNotFalseCommit(t *testing.T) {
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

	// Consume all records so the commit has real offsets to advance (and so
	// the TxnOffsetCommit negotiates KIP-447, avoiding End's pre-447 sleep).
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

	// Fail every EndTxn with UNKNOWN_SERVER_ERROR. The control is keep-forever
	// so the in-loop retry (pre-fix) sees it too.
	var endtxns atomic.Int32
	c.ControlKey(int16(kmsg.EndTxn), func(req kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		endtxns.Add(1)
		resp := req.ResponseKind().(*kmsg.EndTxnResponse)
		resp.ErrorCode = kerr.UnknownServerError.Code
		return resp, nil, true
	})

	committed, err := s.End(ctx, kgo.TryCommit)
	if endtxns.Load() == 0 {
		t.Fatalf("expected the EndTxn control to fire at least once, got 0 (err=%v)", err)
	}
	if committed {
		t.Fatalf("BUG REPRODUCED: End reported committed=true even though every EndTxn failed with UNKNOWN_SERVER_ERROR; the consumer offsets were advanced for a transaction the broker may have aborted (err=%v)", err)
	}
}
