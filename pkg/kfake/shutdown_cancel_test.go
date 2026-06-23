package kfake

// Round 18 of the FRANZ_AUDIT.md program: scenario "client shutdown and
// context cancellation, everywhere" (shutdown-cancel-scenario). Tests whose
// fatal message starts with BUG REPRODUCED fail against the pre-fix client and
// flip to passing with the round-18 fixes.

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// A Produce to an already-known topic, issued after Close, was silently
// orphaned: close() cancels cl.ctx and then sweeps every recBuf exactly once
// via failBufferedRecords, but the record buffering path (recBuf.bufferRecord)
// had no closed-client guard, so a record buffered into the (now drainerless)
// recBuf after the sweep was never failed. Its promise never fired, and
// BufferedProduceRecords never returned to zero, so a later Flush would hang.
// This violates the documented ErrClientClosed contract ("for producing,
// records are failed with this error"). The unknown-topic sibling path already
// honored the contract (waitUnknownTopic selects on cl.ctx.Done), so this was
// a guard present on one path-class and missing on its sibling.
func TestAuditProduceAfterCloseFailsKnownTopic(t *testing.T) {
	t.Parallel()
	const topic = "audit-produce-after-close"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))

	// Build the client by hand (not newPlainClient): newPlainClient registers
	// t.Cleanup(cl.Close), and a second Close would run failBufferedRecords
	// again, sweeping (and thus failing) the orphaned record and masking the
	// bug. We Close exactly once.
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}

	// Produce once so the topic's partitions load and a recBuf is created:
	// this is what makes the topic "known" and routes the post-close produce
	// through bufferRecord rather than the unknown-topic wait path.
	if err := cl.ProduceSync(context.Background(), &kgo.Record{Topic: topic, Value: []byte("v1")}).FirstErr(); err != nil {
		t.Fatalf("initial ProduceSync: %v", err)
	}

	cl.Close()

	ch := make(chan error, 1)
	cl.Produce(context.Background(), &kgo.Record{Topic: topic, Value: []byte("v2")}, func(_ *kgo.Record, err error) {
		ch <- err
	})

	select {
	case err := <-ch:
		if !errors.Is(err, kgo.ErrClientClosed) {
			t.Errorf("promise fired with %v, want ErrClientClosed", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("BUG REPRODUCED: promise for a known-topic Produce after Close never fired; the record was orphaned in a drainerless recBuf, contradicting the ErrClientClosed contract")
	}

	// With the record failed, the buffer count returns to zero, so a Flush
	// does not hang (pre-fix the orphan pinned BufferedProduceRecords above
	// zero forever).
	if n := cl.BufferedProduceRecords(); n != 0 {
		t.Errorf("BufferedProduceRecords = %d after the failed promise, want 0", n)
	}
}

// Control: a Produce to an UNKNOWN topic after Close already honored the
// ErrClientClosed contract before the round-18 fix, because the unknown-topic
// wait path (waitUnknownTopic) selects on cl.ctx.Done. This passes both pre-
// and post-fix and pins the sibling whose guard the known-topic path was
// missing.
func TestAuditProduceAfterCloseFailsUnknownTopic(t *testing.T) {
	t.Parallel()
	c := newCluster(t, NumBrokers(1))

	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	cl.Close()

	ch := make(chan error, 1)
	cl.Produce(context.Background(), &kgo.Record{Topic: "audit-never-seen-topic", Value: []byte("v")}, func(_ *kgo.Record, err error) {
		ch <- err
	})

	select {
	case err := <-ch:
		if !errors.Is(err, kgo.ErrClientClosed) {
			t.Errorf("promise fired with %v, want ErrClientClosed", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("promise for an unknown-topic Produce after Close never fired")
	}
}
