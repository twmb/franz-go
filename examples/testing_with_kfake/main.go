// This example demonstrates kfake, a fake in-memory Kafka cluster for
// testing without a real broker. kfake provides control functions that let
// you intercept, observe, modify, or delay any Kafka protocol request.
//
// Three modes show different control function patterns:
//
//   - inject-error: one-shot ControlKey to inject a produce error; kgo retries
//     and succeeds because the control is consumed after one interception
//   - observe: KeepControl to persistently count every request type flowing
//     through the cluster during a produce-and-consume cycle
//   - sleep: SleepControl with SleepOutOfOrder to coordinate between requests,
//     delaying a fetch response until a produce arrives
//
// Unlike other examples in this directory, this one does not require a running
// Kafka cluster - everything runs in-process.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

var mode = flag.String("mode", "inject-error", "demo mode: inject-error, observe, sleep")

func die(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func main() {
	flag.Parse()
	switch *mode {
	case "inject-error":
		demoInjectError()
	case "observe":
		demoObserve()
	case "sleep":
		demoSleep()
	default:
		die("unknown mode %q; use inject-error, observe, or sleep", *mode)
	}
}

// demoInjectError uses a one-shot ControlKey to inject a retriable error on
// the first produce request. Because KeepControl is NOT called, the control
// is removed after handling one request. The client retries and the second
// produce goes through normally.
func demoInjectError() {
	c, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(1, "test-topic"),
	)
	if err != nil {
		die("NewCluster: %v", err)
	}
	defer c.Close()

	// One-shot control: return NOT_LEADER_FOR_PARTITION on the first produce.
	// The control handles exactly one request and is then removed.
	var intercepted atomic.Bool
	c.ControlKey(int16(kmsg.Produce), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		intercepted.Store(true)
		fmt.Println("  [control] intercepted produce, returning NOT_LEADER_FOR_PARTITION")

		req := kreq.(*kmsg.ProduceRequest)
		resp := req.ResponseKind().(*kmsg.ProduceResponse)
		for _, rt := range req.Topics {
			st := kmsg.NewProduceResponseTopic()
			st.Topic = rt.Topic
			for _, rp := range rt.Partitions {
				sp := kmsg.NewProduceResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = kerr.NotLeaderForPartition.Code
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil, true
	})

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic("test-topic"),
	)
	if err != nil {
		die("NewClient: %v", err)
	}
	defer cl.Close()

	fmt.Println("inject-error: producing a record (first attempt will fail)...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	r := &kgo.Record{Value: []byte("hello")}
	if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
		die("produce failed: %v", err)
	}

	fmt.Printf("  produced to partition %d offset %d\n", r.Partition, r.Offset)
	fmt.Printf("  control intercepted first attempt: %v\n", intercepted.Load())
	fmt.Println("  kgo retried automatically and succeeded")
}

// demoObserve uses KeepControl to persistently observe every request without
// intercepting. The control function returns (nil, nil, false) - "not handled"
// - so all requests are processed normally by the cluster.
func demoObserve() {
	c, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(3, "test-topic"),
	)
	if err != nil {
		die("NewCluster: %v", err)
	}
	defer c.Close()

	// Persistent observer using Control (matches all request keys).
	// KeepControl prevents the control from being removed after use.
	var mu sync.Mutex
	counts := make(map[string]int)
	c.Control(func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		mu.Lock()
		counts[kmsg.NameForKey(kreq.Key())]++
		mu.Unlock()
		return nil, nil, false
	})

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic("test-topic"),
		kgo.ConsumeTopics("test-topic"),
	)
	if err != nil {
		die("NewClient: %v", err)
	}

	fmt.Println("observe: producing 10 records...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	for i := range 10 {
		r := &kgo.Record{Value: []byte(fmt.Sprintf("record-%d", i))}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			die("produce: %v", err)
		}
	}

	fmt.Println("observe: consuming records...")
	fetches := cl.PollFetches(ctx)
	fmt.Printf("  consumed %d records\n", fetches.NumRecords())

	cl.Close()

	fmt.Println("\n  request type counts:")
	mu.Lock()
	keys := make([]string, 0, len(counts))
	for k := range counts {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("    %-30s %d\n", k, counts[k])
	}
	mu.Unlock()
}

// demoSleep uses SleepControl with SleepOutOfOrder to coordinate between
// requests on different connections. A fetch control function sleeps until a
// produce request arrives, guaranteeing the fetch returns data rather than
// timing out empty.
//
// franz-go uses separate connections for produce and fetch requests, so
// SleepOutOfOrder allows the produce to be processed while the fetch sleeps.
func demoSleep() {
	c, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(1, "test-topic"),
		// SleepOutOfOrder allows requests on other connections to
		// proceed while a control function is sleeping.
		kfake.SleepOutOfOrder(),
	)
	if err != nil {
		die("NewCluster: %v", err)
	}
	defer c.Close()

	// Signal channel: produce observer notifies when data has been written.
	produced := make(chan struct{}, 1)

	// Observe produces: signal when one arrives. KeepControl keeps this
	// observer active for the entire test.
	c.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		select {
		case produced <- struct{}{}:
		default:
		}
		return nil, nil, false
	})

	// Sleep the first fetch until a produce is observed. SleepControl
	// yields so that the produce request (on a different connection) can
	// be processed while this fetch waits. DropControl removes this
	// control after it fires, so subsequent fetches run normally.
	c.ControlKey(int16(kmsg.Fetch), func(kmsg.Request) (kmsg.Response, error, bool) {
		fmt.Println("  [control] fetch arrived, sleeping until produce completes...")
		c.SleepControl(func() {
			<-produced
		})
		fmt.Println("  [control] produce observed, waking fetch")
		// DropControl removes this control even though we return
		// handled=false. Without it, the control would fire again
		// on the next fetch because unhandled controls are kept.
		c.DropControl()
		return nil, nil, false
	})

	// Start a consumer - its first fetch will sleep in the control.
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics("test-topic"),
	)
	if err != nil {
		die("NewClient consumer: %v", err)
	}
	defer consumer.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Poll in the background - the fetch will sleep until produce arrives.
	type result struct {
		n   int
		err error
	}
	done := make(chan result, 1)
	go func() {
		f := consumer.PollFetches(ctx)
		var fetchErr error
		f.EachError(func(_ string, _ int32, err error) {
			fetchErr = err
		})
		done <- result{f.NumRecords(), fetchErr}
	}()

	// Brief pause to let the consumer connect and issue its fetch.
	time.Sleep(100 * time.Millisecond)

	// Produce a record - this wakes the sleeping fetch control.
	producer, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic("test-topic"),
	)
	if err != nil {
		die("NewClient producer: %v", err)
	}
	defer producer.Close()

	fmt.Println("sleep: producing a record (will wake the sleeping fetch)...")
	r := &kgo.Record{Value: []byte("coordinated")}
	if err := producer.ProduceSync(ctx, r).FirstErr(); err != nil {
		die("produce: %v", err)
	}
	fmt.Printf("  produced to partition %d offset %d\n", r.Partition, r.Offset)

	res := <-done
	if res.err != nil {
		die("consume: %v", res.err)
	}
	fmt.Printf("  consumer fetched %d record(s) after being woken\n", res.n)
}
