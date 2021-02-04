package main

import (
	"context"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

func startConsuming(ctx context.Context, kafkaBrokers string, topic string) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(kafkaBrokers, ",")...),
		// Only read messages that have been written as part of committed transactions.
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
	)
	if err != nil {
		fmt.Printf("error initializing Kafka consumer: %v\n", err)
		return
	}

	client.AssignGroup("my-consumer-group", kgo.GroupTopics(topic))
	defer client.Close()

consumerLoop:
	for {
		fetches := client.PollFetches(ctx)
		iter := fetches.RecordIter()

		for _, fetchErr := range fetches.Errors() {
			fmt.Printf(
				"error consuming from topic: topic=%s, partition=%d, err=%v",
				fetchErr.Topic, fetchErr.Partition, fetchErr.Err,
			)
			break consumerLoop
		}

		for !iter.Done() {
			record := iter.Next()
			fmt.Printf("consumed record with message: %v", string(record.Value))
		}
	}

	fmt.Println("consumer exited")
}
