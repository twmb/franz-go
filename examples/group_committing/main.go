// This example demonstrates different ways to consume and commit offsets.
// Run with -commit-style to select a strategy:
//
//   - autocommit: the simplest - offsets are committed automatically after
//     each poll. Nothing can be lost because autocommitting commits the
//     *prior* poll's offsets.
//   - records: disable autocommit and explicitly commit specific records.
//   - uncommitted: disable autocommit and commit all uncommitted offsets
//     after each poll (the recommended manual commit pattern).
//   - marks: use AutoCommitMarks + BlockRebalanceOnPoll so only explicitly
//     marked offsets are committed. On revoke, marked offsets are flushed.
//   - kadm: consume from previously committed offsets using direct partition
//     assignment (no consumer group protocol). Offsets are committed via kadm.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	seedBrokers = flag.String("brokers", "localhost:9092", "comma delimited list of seed brokers")
	topic       = flag.String("topic", "", "topic to consume from")
	style       = flag.String("commit-style", "autocommit", "commit style; autocommit|records|uncommitted|marks|kadm")
	group       = flag.String("group", "", "group to consume within")
	logger      = flag.Bool("logger", false, "if true, enable an info level logger")
)

func die(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, msg, args...)
	os.Exit(1)
}

func main() {
	flag.Parse()

	styleNum := 0
	switch {
	case strings.HasPrefix("autocommit", *style):
	case strings.HasPrefix("records", *style):
		styleNum = 1
	case strings.HasPrefix("uncommitted", *style):
		styleNum = 2
	case strings.HasPrefix("marks", *style):
		styleNum = 3
	case strings.HasPrefix("kadm", *style):
		styleNum = 4
	default:
		die("unrecognized style %s", *style)
	}

	seeds := kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...)

	// The kadm style bypasses the consumer group protocol entirely.
	// It fetches committed offsets via kadm, consumes via direct
	// partition assignment, and commits offsets back via kadm.
	if styleNum == 4 {
		consumeKadm(seeds)
		return
	}

	opts := []kgo.Opt{
		seeds,
		kgo.ConsumerGroup(*group),
		kgo.ConsumeTopics(*topic),
	}
	switch styleNum {
	case 1, 2:
		opts = append(opts, kgo.DisableAutoCommit())
	case 3:
		// AutoCommitMarks causes autocommitting to only commit
		// offsets that have been explicitly marked, rather than
		// committing all polled offsets. BlockRebalanceOnPoll
		// ensures no rebalance happens between polling and marking.
		opts = append(opts,
			kgo.AutoCommitMarks(),
			kgo.BlockRebalanceOnPoll(),
			kgo.OnPartitionsRevoked(func(ctx context.Context, cl *kgo.Client, _ map[string][]int32) {
				// Before partitions are revoked, commit all marked offsets.
				if err := cl.CommitMarkedOffsets(ctx); err != nil {
					fmt.Printf("revoke commit failed: %v\n", err)
				}
			}),
		)
	}
	if *logger {
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, nil)))
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		die("unable to create client: %v", err)
	}

	go consume(cl, styleNum)

	sigs := make(chan os.Signal, 2)
	signal.Notify(sigs, os.Interrupt)

	<-sigs
	fmt.Println("received interrupt signal; closing client")
	done := make(chan struct{})
	go func() {
		defer close(done)
		cl.Close()
	}()

	select {
	case <-sigs:
		fmt.Println("received second interrupt signal; quitting without waiting for graceful close")
	case <-done:
	}
}

func consume(cl *kgo.Client, style int) {
	for {
		fetches := cl.PollFetches(context.Background())
		if fetches.IsClientClosed() {
			return
		}
		fetches.EachError(func(t string, p int32, err error) {
			die("fetch err topic %s partition %d: %v", t, p, err)
		})

		switch style {
		case 0:
			var seen int
			fetches.EachRecord(func(*kgo.Record) {
				seen++
			})
			fmt.Printf("processed %d records--autocommitting now allows the **prior** poll to be available for committing, nothing can be lost!\n", seen)

		case 1:
			var rs []*kgo.Record
			fetches.EachRecord(func(r *kgo.Record) {
				rs = append(rs, r)
			})
			if err := cl.CommitRecords(context.Background(), rs...); err != nil {
				fmt.Printf("commit records failed: %v", err)
				continue
			}
			fmt.Printf("committed %d records individually--this demo does this in a naive way by just hanging on to all records, but you could just hang on to the max offset record per topic/partition!\n", len(rs))

		case 2:
			var seen int
			fetches.EachRecord(func(*kgo.Record) {
				seen++
			})
			if err := cl.CommitUncommittedOffsets(context.Background()); err != nil {
				fmt.Printf("commit records failed: %v", err)
				continue
			}
			fmt.Printf("committed %d records successfully--the recommended pattern, as followed in this demo, is to commit all uncommitted offsets after each poll!\n", seen)

		case 3:
			// Build the full mark map across all partitions, then
			// mark everything at once. Only marked offsets are
			// autocommitted.
			marks := make(map[string]map[int32]kgo.EpochOffset)
			fetches.EachPartition(func(p kgo.FetchTopicPartition) {
				if len(p.Records) == 0 {
					return
				}
				last := p.Records[len(p.Records)-1]
				if marks[p.Topic] == nil {
					marks[p.Topic] = make(map[int32]kgo.EpochOffset)
				}
				marks[p.Topic][p.Partition] = kgo.EpochOffset{
					Epoch:  last.LeaderEpoch,
					Offset: last.Offset + 1,
				}
				fmt.Printf("marked %s p%d through offset %d\n", p.Topic, p.Partition, last.Offset)
			})
			cl.MarkCommitOffsets(marks)
			// Allow a blocked rebalance to proceed now that we
			// have marked everything from this poll.
			cl.AllowRebalance()
		}
	}
}

// consumeKadm demonstrates consuming without the consumer group protocol.
// It uses kadm to fetch previously committed offsets, then consumes from
// those offsets using direct partition assignment. After processing, it
// commits offsets back via kadm.
func consumeKadm(seeds kgo.Opt) {
	adm, err := kgo.NewClient(seeds)
	if err != nil {
		die("unable to create admin client: %v", err)
	}
	admCl := kadm.NewClient(adm)

	// FetchOffsetsForTopics returns committed offsets for the group,
	// defaulting to -1 for partitions that have no prior commit.
	os, err := admCl.FetchOffsetsForTopics(context.Background(), *group, *topic)
	if err != nil {
		die("unable to fetch group offsets: %v", err)
	}

	cl, err := kgo.NewClient(seeds, kgo.ConsumePartitions(os.KOffsets()))
	if err != nil {
		die("unable to create client: %v", err)
	}
	defer cl.Close()

	fmt.Println("waiting for one record...")
	fs := cl.PollRecords(context.Background(), 1)

	if err := admCl.CommitAllOffsets(context.Background(), *group, kadm.OffsetsFromFetches(fs)); err != nil {
		die("unable to commit offsets: %v", err)
	}

	r := fs.Records()[0]
	fmt.Printf("committed record on partition %d at offset %d\n", r.Partition, r.Offset)
}
