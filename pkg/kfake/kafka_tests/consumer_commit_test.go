// Derived via LLM from Apache Kafka's PlaintextConsumerCommitTest.java (Apache 2.0).
// https://github.com/apache/kafka/blob/trunk/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients/consumer/PlaintextConsumerCommitTest.java

package kafka_tests

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestNoCommittedOffsets verifies that fetching offsets for a group with no
// commits returns no data (or an error indicating the group doesn't exist).
func TestNoCommittedOffsets(t *testing.T) {
	t.Parallel()
	topic := "commit-none"
	group := "commit-none-group"
	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))

	adm := newAdminClient(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	fetched, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		// GROUP_ID_NOT_FOUND is expected - the group doesn't exist.
		return
	}
	_, ok := fetched.Lookup(topic, 0)
	if ok {
		t.Error("expected no committed offsets for empty group")
	}
}

// TestCommitOffsets commits through kadm and reads each commit back off the
// cluster.
func TestCommitOffsets(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		commits []kadm.Offset // committed in order, each read back
	}{
		{"metadata", []kadm.Offset{{Partition: 0, At: 5, Metadata: "my-custom-metadata"}}},
		{"specified-offsets", []kadm.Offset{{Partition: 0, At: 3}, {Partition: 0, At: 7}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			topic := "commit-" + tc.name
			group := topic + "-group"
			c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
			adm := newAdminClient(t, c)
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			for _, want := range tc.commits {
				want.Topic = topic
				offsets := kadm.Offsets{}
				offsets.Add(want)
				if _, err := adm.CommitOffsets(ctx, group, offsets); err != nil {
					t.Fatalf("commit at %d failed: %v", want.At, err)
				}
				o, ok := groupCommits(c, group)[topic][0]
				if !ok {
					t.Fatal("committed offset not found")
				}
				if o.Offset != want.At {
					t.Errorf("expected offset %d, got %d", want.At, o.Offset)
				}
				if o.Metadata != want.Metadata {
					t.Errorf("expected metadata %q, got %q", want.Metadata, o.Metadata)
				}
			}
		})
	}
}

// TestCommitConsumed consumes, commits what it consumed, and reads the
// commit back off the cluster.
func TestCommitConsumed(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name      string
		produce   int
		consume   int
		async     bool // commit from another goroutine
		closeCl   bool // close the consumer before reading the commit back
		wantAtGap bool // the commit may sit ahead of what we consumed
	}{
		{name: "sync-then-close", produce: 5, consume: 5, closeCl: true},
		{name: "async", produce: 5, consume: 5, async: true},
		{name: "partial", produce: 10, consume: 5, wantAtGap: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			topic := "commit-" + tc.name
			group := topic + "-group"
			c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, topic))

			producer := newClient848(t, c, kgo.DefaultProduceTopic(topic))
			for i := range tc.produce {
				produceSync(t, producer, kgo.StringRecord("v-"+strconv.Itoa(i)))
			}

			cl := newPlainClient(t, c,
				kgo.ConsumerGroup(group),
				kgo.ConsumeTopics(topic),
				kgo.DisableAutoCommit(),
			)
			consumeN(t, cl, tc.consume, 10*time.Second)

			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			commit := func() {
				if err := cl.CommitUncommittedOffsets(ctx); err != nil {
					t.Errorf("commit failed: %v", err)
				}
			}
			if tc.async {
				var wg sync.WaitGroup
				wg.Add(1)
				go func() {
					defer wg.Done()
					commit()
				}()
				wg.Wait()
			} else {
				commit()
			}
			if tc.closeCl {
				cl.Close()
			}

			o, ok := groupCommits(c, group)[topic][0]
			if !ok {
				t.Fatal("committed offset not found")
			}
			switch {
			case tc.wantAtGap && o.Offset < int64(tc.consume):
				t.Errorf("expected committed offset >= %d, got %d", tc.consume, o.Offset)
			case !tc.wantAtGap && o.Offset != int64(tc.consume):
				t.Errorf("expected committed offset %d, got %d", tc.consume, o.Offset)
			}
		})
	}
}
