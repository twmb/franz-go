package kfake

import (
	"context"
	"errors"
	"net"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Repeated KIP-951 hints for the current leader must not bypass the produce
// retry backoff and exhaust the retry budget in a tight loop.
func TestIssue1412ProduceRetryBackoff(t *testing.T) {
	t.Parallel()
	for _, idempotent := range []bool{false, true} {
		for _, test := range []struct {
			name       string
			epochDelta int32
			timeout    bool
			immediate  int32
		}{
			{name: "same_epoch"},
			{name: "stale_epoch", epochDelta: -1},
			{name: "new_epoch_then_repeated", epochDelta: 1, immediate: 1},
			{name: "request_timeout", timeout: true},
		} {
			// An idempotent producer intentionally keeps retrying a timed-out
			// append whose outcome is unknown, even beyond RecordRetries.
			if test.timeout && idempotent {
				continue
			}
			t.Run(test.name+"/idempotent="+strconv.FormatBool(idempotent), func(t *testing.T) {
				t.Parallel()
				const topic = "retry-backoff"
				const retries = 5
				const backoff = 50 * time.Millisecond
				c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
				// Start at epoch 1 so the stale hint still has a valid epoch.
				if err := c.MoveTopicPartition(topic, 0, 0); err != nil {
					t.Fatal(err)
				}
				var backoffs atomic.Int32
				var produces atomic.Int32
				opts := []kgo.Opt{
					// Telemetry requests share RetryBackoffFn; count only produce retries.
					kgo.DisableClientMetrics(),
					kgo.RecordRetries(retries),
					kgo.RetryBackoffFn(func(int) time.Duration {
						backoffs.Add(1)
						return backoff
					}),
				}
				if !idempotent {
					opts = append(opts, kgo.DisableIdempotentWrite(), kgo.MaxProduceRequestsInflightPerBroker(1))
				}
				cl := newPlainClient(t, c, opts...)
				ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
				defer cancel()
				if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("warmup")}).FirstErr(); err != nil {
					t.Fatal(err)
				}
				backoffs.Store(0)
				pi := c.PartitionInfo(topic, 0)
				host, portText, err := net.SplitHostPort(c.ListenAddrs()[0])
				if err != nil {
					t.Fatal(err)
				}
				port, err := strconv.Atoi(portText)
				if err != nil {
					t.Fatal(err)
				}
				c.ControlKey(int16(kmsg.Produce), func(req kmsg.Request) (kmsg.Response, error, bool) {
					c.KeepControl()
					n := produces.Add(1)
					if n > retries+1 {
						// Allow recovery so a retry-limit regression cannot hang.
						return nil, nil, false
					}
					if want := n - 1 - test.immediate; backoffs.Load() < want {
						t.Errorf("produce %d: backoff calls = %d, want at least %d", n, backoffs.Load(), want)
					}
					if n == 2 && test.immediate != 0 && backoffs.Load() != 0 {
						t.Error("advancing leader epoch did not retry immediately")
					}
					resp := req.ResponseKind().(*kmsg.ProduceResponse)
					rt := kmsg.NewProduceResponseTopic()
					rt.Topic = topic
					rt.TopicID = req.(*kmsg.ProduceRequest).Topics[0].TopicID
					rp := kmsg.NewProduceResponseTopicPartition()
					rp.Partition = 0
					rp.ErrorCode = kerr.NotLeaderForPartition.Code
					if test.timeout {
						rp.ErrorCode = kerr.RequestTimedOut.Code
					}
					rp.CurrentLeader.LeaderID = pi.Leader
					rp.CurrentLeader.LeaderEpoch = pi.Epoch + test.epochDelta
					rt.Partitions = append(rt.Partitions, rp)
					resp.Topics = append(resp.Topics, rt)
					if !test.timeout {
						resp.Brokers = []kmsg.ProduceResponseBroker{{NodeID: pi.Leader, Host: host, Port: int32(port)}}
					}
					return resp, nil, true
				})
				start := time.Now()
				err = cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("probe")}).FirstErr()
				elapsed := time.Since(start)
				if !errors.Is(err, kgo.ErrRecordRetries) {
					t.Fatalf("expected retry exhaustion, got %v", err)
				}
				if got := produces.Load(); got != retries+1 {
					t.Errorf("produce requests = %d, want %d", got, retries+1)
				}
				if want := time.Duration(retries-test.immediate) * backoff; elapsed < want {
					t.Errorf("retries took %v, want at least %v of backoff", elapsed, want)
				}
			})
		}
	}
}

// A real leader change must still retry on the new broker without backoff.
func TestIssue1412ProduceNewLeader(t *testing.T) {
	t.Parallel()
	const topic = "new-leader"
	c := newCluster(t, NumBrokers(2), SeedTopics(1, topic))
	var backoffs atomic.Int32
	cl := newPlainClient(t, c, kgo.DisableClientMetrics(), kgo.RetryBackoffFn(func(int) time.Duration {
		backoffs.Add(1)
		return time.Second
	}))
	ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
	defer cancel()
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("warmup")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	backoffs.Store(0)
	old := c.PartitionInfo(topic, 0)
	if err := c.MoveTopicPartition(topic, 0, (old.Leader+1)%2); err != nil {
		t.Fatal(err)
	}
	var produces atomic.Int32
	c.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		produces.Add(1)
		return nil, nil, false
	})
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("probe")}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	if got := produces.Load(); got != 2 {
		t.Errorf("produce requests = %d, want failed attempt and successful retry", got)
	}
	if got := backoffs.Load(); got != 0 {
		t.Errorf("new leader backoff calls = %d, want 0", got)
	}
}
