package kfake

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

// txnV6Harness seeds a record into consumeTopic, consumes it in a transact
// session, produces one transactional record, and returns after observing
// every TxnOffsetCommit request via the installed control.
func txnV6Harness(t *testing.T, consumeTopic, produceTopic, extraTopic, txnID, group string, endCtxFn func(context.Context) context.Context) (gotVersion int32, sawZeroID bool, adm *kadm.Client, cleanup func()) {
	t.Helper()

	topics := []string{consumeTopic, produceTopic}
	if extraTopic != "" {
		topics = append(topics, extraTopic)
	}
	c, err := NewCluster(NumBrokers(1), SeedTopics(1, topics...))
	if err != nil {
		t.Fatal(err)
	}

	var version atomic.Int32
	var zeroID atomic.Bool
	c.ControlKey(int16(kmsg.TxnOffsetCommit), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		req := kreq.(*kmsg.TxnOffsetCommitRequest)
		version.Store(int32(req.Version))
		for _, rt := range req.Topics {
			if rt.TopicID == ([16]byte{}) {
				zeroID.Store(true)
			}
		}
		return nil, nil, false
	})

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	seed, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		c.Close()
		t.Fatal(err)
	}
	if err := seed.ProduceSync(ctx, &kgo.Record{Topic: consumeTopic, Value: []byte("v")}).FirstErr(); err != nil {
		seed.Close()
		c.Close()
		t.Fatalf("failed to seed: %v", err)
	}

	// kgo's default MaxVersions is kversion.Stable(), which will not allow
	// TxnOffsetCommit v6 until Kafka 4.4 is released and kversion promotes
	// it; raise the cap explicitly to exercise the v6 path.
	maxVersions := kversion.Stable()
	maxVersions.SetMaxKeyVersion(int16(kmsg.TxnOffsetCommit), 6)
	sess, err := kgo.NewGroupTransactSession(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.MaxVersions(maxVersions),
		kgo.DefaultProduceTopic(produceTopic),
		kgo.TransactionalID(txnID),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(consumeTopic),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	if err != nil {
		seed.Close()
		c.Close()
		t.Fatal(err)
	}

	for polled := 0; polled == 0; {
		fs := sess.PollFetches(ctx)
		if err := fs.Err0(); err != nil {
			t.Fatalf("poll error: %v", err)
		}
		polled = fs.NumRecords()
	}

	if err := sess.Begin(); err != nil {
		t.Fatalf("failed to begin: %v", err)
	}
	if err := sess.ProduceSync(ctx, kgo.StringRecord("out")).FirstErr(); err != nil {
		t.Fatalf("failed to produce: %v", err)
	}

	endCtx := ctx
	if endCtxFn != nil {
		endCtx = endCtxFn(ctx)
	}
	committed, err := sess.End(endCtx, kgo.TryCommit)
	if err != nil {
		t.Fatalf("end error: %v", err)
	}
	if !committed {
		t.Fatal("transaction did not commit")
	}

	return version.Load(), zeroID.Load(), kadm.NewClient(seed), func() {
		sess.Close()
		seed.Close()
		c.Close()
	}
}

// TxnOffsetCommit v6 (KIP-1319) identifies topics by ID like OffsetCommit
// v10. End to end: kgo fills TopicIDs and negotiates v6, kfake resolves the
// IDs back to names, and the commit lands under the right topic.
func TestTxnOffsetCommitV6(t *testing.T) {
	t.Parallel()
	const (
		consumeTopic = "txn-v6-consume"
		produceTopic = "txn-v6-produce"
		group        = "g-txn-v6"
	)

	version, sawZeroID, adm, cleanup := txnV6Harness(t, consumeTopic, produceTopic, "", "txn-v6", group, nil)
	defer cleanup()

	if version != 6 {
		t.Errorf("TxnOffsetCommit went out at v%d, want v6", version)
	}
	if sawZeroID {
		t.Error("a topic in the commit had a zero TopicID")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	os, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatalf("fetch offsets: %v", err)
	}
	if o, ok := os.Lookup(consumeTopic, 0); !ok || o.At != 1 {
		t.Errorf("committed offset for %s: got %+v (ok=%v), want offset 1", consumeTopic, o, ok)
	}
}

// If any topic in a txn offset commit has no TopicID -- reachable only via a
// PreTxnCommitFnContext fn adding topics, since metadata always supplies ids
// on brokers new enough to serve v6 -- the whole request must pin to v5 and
// commit by name: a zero TopicID on a v6 wire would commit to no topic,
// inside an otherwise-committed transaction.
func TestTxnOffsetCommitV6PinFallback(t *testing.T) {
	t.Parallel()
	const (
		consumeTopic = "txn-v6pin-consume"
		produceTopic = "txn-v6pin-produce"
		extraTopic   = "txn-v6pin-extra"
		group        = "g-txn-v6pin"
	)

	version, _, adm, cleanup := txnV6Harness(t, consumeTopic, produceTopic, extraTopic, "txn-v6pin", group,
		func(ctx context.Context) context.Context {
			return kgo.PreTxnCommitFnContext(ctx, func(req *kmsg.TxnOffsetCommitRequest) error {
				rt := kmsg.NewTxnOffsetCommitRequestTopic()
				rt.Topic = extraTopic // deliberately no TopicID
				rp := kmsg.NewTxnOffsetCommitRequestTopicPartition()
				rp.Partition = 0
				rp.Offset = 7
				rt.Partitions = append(rt.Partitions, rp)
				req.Topics = append(req.Topics, rt)
				return nil
			})
		})
	defer cleanup()

	if version != 5 {
		t.Errorf("TxnOffsetCommit with an id-less topic went out at v%d, want pinned v5", version)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	os, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatalf("fetch offsets: %v", err)
	}
	if o, ok := os.Lookup(consumeTopic, 0); !ok || o.At != 1 {
		t.Errorf("committed offset for %s: got %+v (ok=%v), want offset 1", consumeTopic, o, ok)
	}
	if o, ok := os.Lookup(extraTopic, 0); !ok || o.At != 7 {
		t.Errorf("committed offset for %s: got %+v (ok=%v), want offset 7", extraTopic, o, ok)
	}
}
