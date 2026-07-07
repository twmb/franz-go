package kgo

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestIDStableLongEnough(t *testing.T) {
	t.Parallel()
	if idStableLongEnough(time.Time{}) {
		t.Error("a zero agreed-at time must never count as stable")
	}
	if idStableLongEnough(time.Now()) {
		t.Error("a just-agreed ID must not count as stable")
	}
	if !idStableLongEnough(time.Now().Add(-recreationStableIDAge - time.Second)) {
		t.Error("an ID held longer than the stable age must count as stable")
	}
}

// recreateTestTopic deletes and recreates topic against the real test
// broker. Deletion propagates asynchronously, so the create retries while
// the broker still answers TopicAlreadyExists -- and the broker's metadata
// view lags the controller, so this also waits until metadata reports a
// topic ID different from the old incarnation's before returning: a client
// starting during the gap would briefly (and legitimately) consume the
// dying incarnation.
func recreateTestTopic(tb testing.TB, topic string) {
	tb.Helper()

	metaID := func() [16]byte {
		req := kmsg.NewPtrMetadataRequest()
		mt := kmsg.NewMetadataRequestTopic()
		mt.Topic = kmsg.StringPtr(topic)
		req.Topics = append(req.Topics, mt)
		resp, err := req.RequestWith(context.Background(), adm())
		if err != nil || len(resp.Topics) != 1 {
			return [16]byte{}
		}
		return resp.Topics[0].TopicID
	}
	oldID := metaID()

	delReq := kmsg.NewPtrDeleteTopicsRequest()
	delReq.TopicNames = []string{topic}
	dt := kmsg.NewDeleteTopicsRequestTopic()
	dt.Topic = kmsg.StringPtr(topic)
	delReq.Topics = append(delReq.Topics, dt)
	delResp, err := delReq.RequestWith(context.Background(), adm())
	if err == nil {
		err = kerr.ErrorForCode(delResp.Topics[0].ErrorCode)
	}
	if err != nil {
		tb.Fatalf("unable to delete %q for recreation: %v", topic, err)
	}

	req := kmsg.NewPtrCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic = topic
	rt.NumPartitions = 1
	rt.ReplicationFactor = int16(testrf)
	req.Topics = append(req.Topics, rt)
	deadline := time.Now().Add(60 * time.Second)
	for {
		resp, err := req.RequestWith(context.Background(), adm())
		if err == nil {
			err = kerr.ErrorForCode(resp.Topics[0].ErrorCode)
		}
		if err == nil {
			break
		}
		if !errors.Is(err, kerr.TopicAlreadyExists) || time.Now().After(deadline) {
			tb.Fatalf("unable to recreate %q: %v", topic, err)
		}
		time.Sleep(250 * time.Millisecond)
	}

	if oldID == ([16]byte{}) {
		return // broker predates metadata topic IDs; nothing to confirm
	}
	for {
		if id := metaID(); id != oldID && id != ([16]byte{}) || time.Now().After(deadline) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// collectRecreationVals polls until every wanted value arrived, failing on
// any unexpected value: an old-incarnation record or a duplicate delivery.
func collectRecreationVals(ctx context.Context, t *testing.T, cl *Client, want ...string) {
	t.Helper()
	wanted := make(map[string]bool, len(want))
	for _, w := range want {
		wanted[w] = false
	}
	seen := 0
	for seen < len(want) {
		if ctx.Err() != nil {
			t.Fatalf("timed out with %d/%d wanted values", seen, len(want))
		}
		fs := cl.PollFetches(ctx)
		fs.EachRecord(func(r *Record) {
			v := string(r.Value)
			had, ok := wanted[v]
			if !ok || had {
				t.Errorf("unexpected or duplicate value %q (offset %d, leader epoch %d)", v, r.Offset, r.LeaderEpoch)
				return
			}
			wanted[v] = true
			seen++
		})
	}
}

// TestTopicRecreation runs the core recreation contract against a real
// broker: after a delete+recreate under the same name, the producer heals
// with no surfaced error, and the consumer restarts from the new topic's
// beginning, delivering exactly the new incarnation's records.
func TestTopicRecreation(t *testing.T) {
	t.Parallel()

	topic, cleanup := tmpTopicPartitions(t, 1)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	producer, err := newTestClient(DefaultProduceTopic(topic))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	consumer, err := newTestClient(
		ConsumeTopics(topic),
		ConsumeResetOffset(NewOffset().AtStart()),
		FetchMaxWait(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	var a []string
	for i := range 5 {
		a = append(a, fmt.Sprintf("a%d", i))
		if err := producer.ProduceSync(ctx, StringRecord(a[i])).FirstErr(); err != nil {
			t.Fatalf("produce before recreation: %v", err)
		}
	}
	collectRecreationVals(ctx, t, consumer, a...)

	// Fewer records than the consumed position: every detection tier
	// converges (worst case via out-of-range classification).
	recreateTestTopic(t, topic)
	for _, v := range []string{"b0", "b1"} {
		if err := producer.ProduceSync(ctx, StringRecord(v)).FirstErr(); err != nil {
			t.Fatalf("produce across recreation did not heal: %v", err)
		}
	}
	collectRecreationVals(ctx, t, consumer, "b0", "b1")

	// Liveness after the heal, both sides.
	if err := producer.ProduceSync(ctx, StringRecord("c0")).FirstErr(); err != nil {
		t.Fatalf("produce after heal: %v", err)
	}
	collectRecreationVals(ctx, t, consumer, "c0")
}

// TestTopicRecreationTransactions runs the transactional recreation
// contract against a real broker: transactions across a recreation fail
// LOUDLY (an error wrapping TRANSACTION_ABORTABLE, or the commit refusal
// wrapping OPERATION_NOT_ATTEMPTED), aborting recovers, and a bounded
// number of rounds later a transaction commits cleanly into the new
// incarnation, which read-committed consumption then sees exactly.
func TestTopicRecreationTransactions(t *testing.T) {
	t.Parallel()

	topic, cleanup := tmpTopicPartitions(t, 1)
	defer cleanup()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	cl, err := newTestClient(
		DefaultProduceTopic(topic),
		TransactionalID(randsha()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	if err := cl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := cl.ProduceSync(ctx, StringRecord("t0")).FirstErr(); err != nil {
		t.Fatalf("produce in the first transaction: %v", err)
	}
	if err := cl.EndTransaction(ctx, TryCommit); err != nil {
		t.Fatalf("commit before recreation: %v", err)
	}

	recreateTestTopic(t, topic)

	loud := func(err error) bool {
		return errors.Is(err, kerr.TransactionAbortable) || errors.Is(err, kerr.OperationNotAttempted)
	}
	var committed string
	for round := 0; ; round++ {
		if round > 8 || ctx.Err() != nil {
			t.Fatal("no transaction committed within the retry budget after recreation")
		}
		if err := cl.BeginTransaction(); err != nil {
			t.Fatalf("begin round %d: %v", round, err)
		}
		val := fmt.Sprintf("post%d", round)
		perr := cl.ProduceSync(ctx, StringRecord(val)).FirstErr()
		var cerr error
		if perr == nil {
			cerr = cl.EndTransaction(ctx, TryCommit)
		}
		t.Logf("round %d: produce err %v; commit err %v", round, perr, cerr)
		if perr == nil && cerr == nil {
			committed = val
			break
		}
		for _, err := range []error{perr, cerr} {
			if err != nil && !loud(err) {
				t.Fatalf("round %d failed outside the designed loud classes: %v", round, err)
			}
		}
		if err := cl.EndTransaction(ctx, TryAbort); err != nil {
			t.Fatalf("abort after recreation (round %d): %v", round, err)
		}
	}

	// Read committed sees exactly the committed transaction; the first
	// transaction's record died with the old incarnation.
	consumer, err := newTestClient(
		ConsumeTopics(topic),
		ConsumeResetOffset(NewOffset().AtStart()),
		FetchIsolationLevel(ReadCommitted()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	collectRecreationVals(ctx, t, consumer, committed)
}
