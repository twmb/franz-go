package main

import (
	"context"
	"flag"
	"fmt"
	_ "net/http/pprof"
	"strings"
	"time"

	"github.com/go-logr/logr"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/klogr"
)

var (
	seedBrokers = flag.String("brokers", "localhost:9092", "comma delimited list of seed brokers")
	topic       = flag.String("topic", "foo", "topic to consume for metric incrementing")
	produce     = flag.Bool("produce", false, "if true, rather than consume, produce to the topic once per second (value \"foo\")")
)

func main() {
	flag.Parse()

	logger := logr.Logger
	l := klogr.New(logger)

	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),
		kgo.WithLogger(l),
		kgo.DefaultProduceTopic(*topic),
	}
	if !*produce {
		opts = append(opts, kgo.ConsumeTopics(*topic))
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		panic(fmt.Sprintf("unable to create client: %v", err))
	}
	defer cl.Close()

	if *produce {
		for range time.Tick(time.Second) {
			if err := cl.ProduceSync(context.Background(), kgo.StringRecord("foo")).FirstErr(); err != nil {
				panic(fmt.Sprintf("unable to produce: %v", err))
			}
		}
	} else {
		for {
			cl.PollFetches(context.Background()) // busy work...
		}
	}
}
