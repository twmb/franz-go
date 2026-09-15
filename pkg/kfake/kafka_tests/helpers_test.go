// Derived via LLM from Apache Kafka's clients-integration-tests (Apache 2.0).
// https://github.com/apache/kafka/tree/trunk/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients

package kafka_tests

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// newCluster creates a new kfake cluster with the given options and registers
// cleanup on test completion.
func newCluster(t *testing.T, opts ...kfake.Opt) *kfake.Cluster {
	t.Helper()
	opts = append([]kfake.Opt{kfake.BrokerConfigs(map[string]string{
		"group.consumer.heartbeat.interval.ms": "100",
	})}, opts...)
	c, err := kfake.NewCluster(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

// newClient848 creates a kgo client with the KIP-848 context opt-in enabled.
func newClient848(t *testing.T, c *kfake.Cluster, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	ctx := context.WithValue(context.Background(), "opt_in_kafka_next_gen_balancer_beta", true)
	opts = append([]kgo.Opt{kgo.SeedBrokers(c.ListenAddrs()...), kgo.WithContext(ctx)}, opts...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// groupCommits returns the group's committed offsets, or nil if the group
// does not exist.
func groupCommits(c *kfake.Cluster, group string) map[string]map[int32]kfake.GroupCommit {
	g := c.GroupInfo(group)
	if g == nil {
		return nil
	}
	return g.Commits
}

// waitStable waits for the group to be Stable with nMembers members. This
// covers classic and 848 groups alike.
func waitStable(t *testing.T, c *kfake.Cluster, group string, nMembers int) *kfake.GroupInfo {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	g, err := c.WaitGroupStable(ctx, group, nMembers)
	if err != nil {
		t.Fatalf("group %s not stable with %d members: %v", group, nMembers, err)
	}
	return g
}

// newAdminClient creates a kadm client connected to the given cluster and
// registers cleanup on test completion.
func newAdminClient(t *testing.T, c *kfake.Cluster) *kadm.Client {
	t.Helper()
	cl := newClient848(t, c)
	return kadm.NewClient(cl)
}

// produceSync produces records synchronously and fails the test on error.
func produceSync(t *testing.T, cl *kgo.Client, records ...*kgo.Record) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatalf("produce failed: %v", err)
	}
}

// produceNStrings produces n string records to the given topic and returns the
// produced records.
func produceNStrings(t *testing.T, cl *kgo.Client, topic string, n int) []*kgo.Record {
	t.Helper()
	var records []*kgo.Record
	for i := range n {
		r := kgo.StringRecord("value-" + strconv.Itoa(i))
		r.Topic = topic
		r.Key = []byte("key-" + strconv.Itoa(i))
		records = append(records, r)
	}
	produceSync(t, cl, records...)
	return records
}

// consumeN consumes exactly n records and returns them. Fails if timeout is
// reached before n records are consumed.
func consumeN(t *testing.T, cl *kgo.Client, n int, timeout time.Duration) []*kgo.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var records []*kgo.Record
	for len(records) < n {
		fs := cl.PollFetches(ctx)
		if errs := fs.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err == context.DeadlineExceeded || e.Err == context.Canceled {
					t.Fatalf("timeout consuming records: got %d/%d", len(records), n)
				}
			}
			t.Fatalf("consume errors: %v", errs)
		}
		fs.EachRecord(func(r *kgo.Record) {
			records = append(records, r)
		})
	}
	return records
}

// newGroupConsumer creates a KIP-848 consumer group client with common
// defaults: ConsumeTopics, ConsumerGroup, AtStart, and FetchMaxWait(250ms).
func newGroupConsumer(t *testing.T, c *kfake.Cluster, topic, group string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	base := []kgo.Opt{
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250 * time.Millisecond),
	}
	return newClient848(t, c, append(base, opts...)...)
}

// poll1FromEachClient polls each client until every one has received at least
// one record, or the timeout expires.
func poll1FromEachClient(t *testing.T, timeout time.Duration, clients ...*kgo.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	remaining := make(map[int]*kgo.Client, len(clients))
	for i, cl := range clients {
		remaining[i] = cl
	}
	for len(remaining) > 0 {
		for i, cl := range remaining {
			fs := cl.PollRecords(ctx, 10)
			if fs.NumRecords() > 0 {
				delete(remaining, i)
			}
		}
		if ctx.Err() != nil {
			t.Fatalf("timeout waiting for all clients to get records: %d/%d remaining", len(remaining), len(clients))
		}
	}
}

// newPlainClient creates a kgo client without 848 opt-in.
func newPlainClient(t *testing.T, c *kfake.Cluster, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	opts = append([]kgo.Opt{kgo.SeedBrokers(c.ListenAddrs()...)}, opts...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}
