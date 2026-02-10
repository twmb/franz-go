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

// waitForStableGroup polls DescribeConsumerGroups until the group is Stable
// with the expected number of members, then returns the described group.
func waitForStableGroup(t *testing.T, adm *kadm.Client, group string, nMembers int, timeout time.Duration) kadm.DescribedConsumerGroup {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		described, err := adm.DescribeConsumerGroups(ctx, group)
		if err != nil {
			t.Fatalf("describe failed: %v", err)
		}
		dg := described[group]
		if dg.State == "Stable" && len(dg.Members) == nMembers {
			return dg
		}
		if ctx.Err() != nil {
			t.Fatalf("timeout waiting for stable group %q with %d members (state=%s, members=%d)", group, nMembers, dg.State, len(dg.Members))
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// totalAssignedPartitions returns the total number of partitions assigned
// across all members of a described consumer group.
func totalAssignedPartitions(dg kadm.DescribedConsumerGroup) int {
	n := 0
	for _, m := range dg.Members {
		for _, parts := range m.Assignment {
			n += len(parts)
		}
	}
	return n
}
