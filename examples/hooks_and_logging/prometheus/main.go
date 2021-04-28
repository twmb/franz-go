package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	debugPort   = flag.Int("debug-port", 9999, "localhost port that metrics can be curled from")
	seedBrokers = flag.String("brokers", "localhost:9999", "comma delimited list of seed brokers")
	topic       = flag.String("topic", "test", "topic to consume for metric incrementing")
)

func main() {
	flag.Parse()

	metrics := NewMetrics("kgo")

	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),
		kgo.WithHooks(metrics),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, func() string {
			return time.Now().Format("[2006-01-02 15:04:05.999] ")
		})),
	)
	if err != nil {
		panic(fmt.Sprintf("unable to create client: %v", err))
	}
	defer client.Close()

	go func() {
		http.Handle("/metrics", metrics.Handler())
		log.Fatal(http.ListenAndServe(fmt.Sprintf("localhost:%d", *debugPort), nil))
	}()

	client.AssignPartitions(kgo.ConsumeTopics(kgo.NewOffset().AtEnd(), *topic))
	for {
		client.PollFetches(context.Background()) // busy work...
	}
}
