package main

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

func startConsuming(ctx context.Context, brokers []string, group, topic string) {
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()), // only read messages that have been written as part of committed transactions
	)
	if err != nil {
		fmt.Printf("error initializing Kafka consumer: %v\n", err)
		return
	}

	client.AssignGroup(group, kgo.GroupTopics(topic))
	defer client.Close()
	// The following commit will not run because the only way to kill this
	// program is to interrupt it, but, usually you should commit the final
	// consumed offsets before quitting, as well as check any commit
	// errors.
	defer func() { client.BlockingCommitOffsets(ctx, client.UncommittedOffsets(), nil) }()

consumerLoop:
	for {
		fetches := client.PollFetches(ctx)
		iter := fetches.RecordIter()

		for _, fetchErr := range fetches.Errors() {
			fmt.Printf("error consuming from topic: topic=%s, partition=%d, err=%v\n",
				fetchErr.Topic, fetchErr.Partition, fetchErr.Err)
			break consumerLoop
		}

		for !iter.Done() {
			record := iter.Next()
			fmt.Printf("consumed record from partition %d with message: %v", record.Partition, string(record.Value))
		}
	}

	fmt.Println("consumer exited")
}
