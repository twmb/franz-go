package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	seedBrokers = flag.String("brokers", "localhost:9092", "comma delimited list of seed brokers")
	topic       = flag.String("topic", "", "topic to consume from")
	group       = flag.String("group", "", "group to consume within")
)

func die(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg, args...)
	os.Exit(1)
}

func main() {
	flag.Parse()

	seeds := kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...)

	var adm *kadm.Client
	{
		cl, err := kgo.NewClient(seeds)
		if err != nil {
			die("unable to create admin client: %v", err)
		}
		adm = kadm.NewClient(cl)
	}

	// With the admin client, we can either FetchOffsets to fetch strictly
	// the prior committed offsets, or FetchOffsetsForTopics to fetch
	// offsets for topics and have -1 offset defaults for topics that are
	// not yet committed. We use FetchOffsetsForTopics here.
	os, err := adm.FetchOffsetsForTopics(context.Background(), *group, *topic)
	if err != nil {
		die("unable to fetch group offsets: %v", err)
	}

	cl, err := kgo.NewClient(seeds, kgo.ConsumePartitions(os.Into().Into()))
	if err != nil {
		die("unable to create client: %v", err)
	}
	defer cl.Close()

	fmt.Println("Waiting for one record...")
	fs := cl.PollRecords(context.Background(), 1)

	// kadm has two offset committing functions: CommitOffsets and
	// CommitAllOffsets. The former allows you to check per-offset error
	// codes, the latter returns the first error of any in the offsets.
	//
	// We use the latter here because we are only committing one offset,
	// and even if we committed more, we do not care about partial failed
	// commits. Use the former if you care to check per-offset commit
	// errors.
	if err := adm.CommitAllOffsets(context.Background(), *group, kadm.OffsetsFromFetches(fs)); err != nil {
		die("unable to commit offsets: %v", err)
	}

	r := fs.Records()[0]
	fmt.Printf("Successfully committed record on partition %d at offset %d!\n", r.Partition, r.Offset)
}
