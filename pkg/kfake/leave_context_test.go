package kfake

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func newLeaveContextClient(t *testing.T, protocol string) (*Cluster, *kgo.Client) {
	t.Helper()
	const topic, group = "leave-context", "leave-context-group"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	producer := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, 1)
	opts := []kgo.Opt{kgo.ConsumeTopics(topic)}
	switch protocol {
	case "classic":
		opts = append(opts, kgo.ConsumerGroup(group), kgo.DisableAutoCommit())
	case "consumer":
		opts = append(opts, kgo.ConsumerGroup(group), kgo.ServerSideBalancer(), kgo.Balancers(kgo.StickyBalancer()), kgo.DisableAutoCommit())
	case "share":
		c.SetGroupConfigs(group, map[string]string{"share.auto.offset.reset": "earliest"})
		opts = append(opts, kgo.ShareGroup(group))
	}
	consumer := newPlainClient(t, c, opts...)
	consumeN(t, consumer, 1, 10*time.Second)
	return c, consumer
}

func gateLeaveContext(t *testing.T, c *Cluster, protocol string) (<-chan struct{}, func()) {
	t.Helper()
	gate := make(chan struct{})
	started := make(chan struct{})
	var releaseOnce, startOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(gate) }) }
	t.Cleanup(release)
	key := kmsg.LeaveGroup
	switch protocol {
	case "consumer":
		key = kmsg.ConsumerGroupHeartbeat
	case "share":
		key = kmsg.ShareGroupHeartbeat
	}
	c.ControlKey(int16(key), func(req kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		switch r := req.(type) {
		case *kmsg.ConsumerGroupHeartbeatRequest:
			if r.MemberEpoch >= 0 {
				return nil, nil, false
			}
		case *kmsg.ShareGroupHeartbeatRequest:
			if r.MemberEpoch >= 0 {
				return nil, nil, false
			}
		}
		startOnce.Do(func() { close(started) })
		c.SleepControl(func() { <-gate })
		return nil, nil, false
	})
	return started, release
}

func TestCloseAfterCanceledLeave(t *testing.T) {
	// With one P, the two leave goroutines compete for the same run queue.
	// Repeat the sequence to cover both schedules of the original race.
	previous := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(previous)
	for _, protocol := range []string{"classic", "consumer", "share"} {
		for attempt := range 8 {
			t.Run(fmt.Sprintf("%s/%d", protocol, attempt), func(t *testing.T) {
				c, consumer := newLeaveContextClient(t, protocol)
				started, release := gateLeaveContext(t, c, protocol)
				ctx, cancel := context.WithCancel(context.Background())
				cancel()
				closed := make(chan struct{})
				go func() {
					if err := consumer.LeaveGroupContext(ctx); !errors.Is(err, context.Canceled) {
						t.Errorf("LeaveGroupContext = %v, want context canceled", err)
					}
					consumer.Close()
					close(closed)
				}()
				select {
				case <-closed:
				case <-time.After(time.Second):
					release()
					<-closed
					t.Fatal("Close waited for the broker after a canceled LeaveGroupContext")
				}
				select {
				case <-started:
					t.Error("a later call issued a leave request with a live context")
				default:
				}
			})
		}
	}
}

func TestCloseWaitsForNilContextLeave(t *testing.T) {
	for _, protocol := range []string{"classic", "consumer", "share"} {
		t.Run(protocol, func(t *testing.T) {
			c, consumer := newLeaveContextClient(t, protocol)
			started, release := gateLeaveContext(t, c, protocol)
			if err := consumer.LeaveGroupContext(nil); err != nil {
				t.Fatal(err)
			}
			closed := make(chan struct{})
			go func() {
				consumer.Close()
				close(closed)
			}()
			select {
			case <-started:
			case <-time.After(time.Second):
				release()
				<-closed
				t.Fatal("nil-context leave did not reach the broker")
			}
			canceled, cancel := context.WithCancel(context.Background())
			cancel()
			if err := consumer.LeaveGroupContext(canceled); !errors.Is(err, context.Canceled) {
				t.Errorf("later canceled caller = %v, want context canceled", err)
			}
			select {
			case <-closed:
				t.Error("Close returned before the initial leave completed")
			case <-time.After(30 * time.Millisecond):
			}
			release()
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("Close did not finish after releasing the broker")
			}
		})
	}
}
