// Derived from Apache Kafka's clients-integration-tests (Apache 2.0).
// https://github.com/apache/kafka/tree/trunk/clients/clients-integration-tests/src/test/java/org/apache/kafka/clients

package kafka_ai_test_rewrites

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

// newClient creates a new kgo client connected to the given cluster and
// registers cleanup on test completion.
func newClient(t *testing.T, c *kfake.Cluster, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	opts = append([]kgo.Opt{kgo.SeedBrokers(c.ListenAddrs()...)}, opts...)
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
	cl := newClient(t, c)
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
