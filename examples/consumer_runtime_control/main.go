// This example demonstrates runtime consumer control: dynamically adding and
// removing topics/partitions, and pausing/resuming fetch operations.
//
// These APIs are useful for:
//   - Dynamically discovering and consuming new topics
//   - Implementing backpressure or priority-based consuming
//   - Temporarily halting consumption from specific topics/partitions
//
// Run with -mode=add-topics, -mode=add-partitions, or -mode=pause to see
// each feature in action.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	seedBrokers = flag.String("brokers", "localhost:9092", "comma delimited list of seed brokers")
	mode        = flag.String("mode", "add-topics", "demo mode: add-topics, add-partitions, pause")
)

func die(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, msg, args...)
	os.Exit(1)
}

func main() {
	flag.Parse()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),
		kgo.ConsumeTopics("topic-a"),
	)
	if err != nil {
		die("unable to create client: %v\n", err)
	}
	defer cl.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	switch *mode {
	case "add-topics":
		demoAddTopics(ctx, cl)
	case "add-partitions":
		demoAddPartitions(ctx, cl)
	case "pause":
		demoPause(ctx, cl)
	default:
		die("unknown mode %q\n", *mode)
	}
}

// demoAddTopics shows AddConsumeTopics: after a delay, "topic-b" is added
// to the set of consumed topics at runtime.
func demoAddTopics(ctx context.Context, cl *kgo.Client) {
	go func() {
		time.Sleep(10 * time.Second)
		cl.AddConsumeTopics("topic-b")
		fmt.Printf("added topic-b, now consuming: %v\n", cl.GetConsumeTopics())
	}()
	pollLoop(ctx, cl)
}

// demoAddPartitions shows AddConsumePartitions and RemoveConsumePartitions:
// specific partitions of "topic-c" are added, then one is removed.
func demoAddPartitions(ctx context.Context, cl *kgo.Client) {
	go func() {
		time.Sleep(10 * time.Second)
		fmt.Println("adding topic-c partitions 0 and 1")
		cl.AddConsumePartitions(map[string]map[int32]kgo.Offset{
			"topic-c": {
				0: kgo.NewOffset().AtStart(),
				1: kgo.NewOffset().AtEnd(),
			},
		})
		fmt.Printf("now consuming: %v\n", cl.GetConsumeTopics())

		time.Sleep(10 * time.Second)
		fmt.Println("removing topic-c partition 0")
		cl.RemoveConsumePartitions(map[string][]int32{
			"topic-c": {0},
		})
	}()
	pollLoop(ctx, cl)
}

// demoPause shows PauseFetchTopics and ResumeFetchTopics: topic-a is paused
// after some records, then resumed after a cooldown.
func demoPause(ctx context.Context, cl *kgo.Client) {
	count := 0
	paused := false

	for {
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(t string, p int32, err error) {
			die("fetch err topic %s partition %d: %v\n", t, p, err)
		})
		fetches.EachRecord(func(r *kgo.Record) {
			fmt.Printf("topic=%s partition=%d offset=%d\n", r.Topic, r.Partition, r.Offset)
			count++
		})

		if !paused && count > 50 {
			cl.PauseFetchTopics("topic-a")
			paused = true
			fmt.Printf("paused topic-a after %d records\n", count)

			// Query currently paused topics (no args = query only).
			fmt.Printf("currently paused: %v\n", cl.PauseFetchTopics())

			go func() {
				time.Sleep(5 * time.Second)
				cl.ResumeFetchTopics("topic-a")
				fmt.Println("resumed topic-a")
			}()
		}
	}
}

func pollLoop(ctx context.Context, cl *kgo.Client) {
	for {
		fetches := cl.PollFetches(ctx)
		if fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(t string, p int32, err error) {
			fmt.Printf("fetch err topic %s partition %d: %v\n", t, p, err)
		})
		fetches.EachRecord(func(r *kgo.Record) {
			fmt.Printf("topic=%s partition=%d offset=%d value=%s\n",
				r.Topic, r.Partition, r.Offset, string(r.Value))
		})
	}
}
