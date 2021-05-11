package main

import (
	"context"
	"flag"
	"strings"
)

var (
	seedBrokers = flag.String("brokers", "localhost:9092", "comma delimited list of seed brokers")
	topic       = flag.String("topic", "test", "topic to produce to transactionally / consume from")
	group       = flag.String("group", "my-consumer-group", "group to use for consuming only committed records")
)

func main() {
	flag.Parse()

	brokers := strings.Split(*seedBrokers, ",")
	ctx := context.Background()
	go startConsuming(ctx, brokers, *group, *topic)
	startProducing(ctx, brokers, *topic)
}
