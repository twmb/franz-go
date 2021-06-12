package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	seedBrokers = flag.String("brokers", "localhost:9092", "comma delimited list of seed brokers")
	topic       = flag.String("topic", "", "topic to consume from")
	style       = flag.String("commit-style", "autocommit", "commit style (which consume & commit is chosen); autocommit|records|uncommitted")
	group       = flag.String("group", "", "group to consume within")
	logger      = flag.Bool("logger", false, "if true, enable an info level logger")
)

func die(msg string, args ...interface{}) {
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
	default:
		die("unrecognized style %s", *style)
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),
		kgo.ConsumerGroup(*group),
		kgo.ConsumeTopics(*topic),
	}
	if styleNum != 0 {
		opts = append(opts, kgo.DisableAutoCommit())
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
			fmt.Printf("processed %d records--because of autocommitting, we could have lost these records if a commit happened and then the client crashed before this log!\n", seen)

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
		}
	}
}
