package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kversion"
)

func die(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, msg, args...)
	os.Exit(1)
}

func main() {
	seeds := []string{"localhost:9092"}

	var adminClient *kadm.Client
	{
		client, err := kgo.NewClient(
			kgo.SeedBrokers(seeds...),

			// Do not try to send requests newer than 2.4.0 to avoid breaking changes in the request struct.
			// Sometimes there are breaking changes for newer versions where more properties are required to set.
			kgo.MaxVersions(kversion.V2_4_0()),
		)
		if err != nil {
			panic(err)
		}
		defer client.Close()

		adminClient = kadm.NewClient(client)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Create topic "franz-go" if it doesn't exist already
	topicName := "franz-go"
	topicDetails, err := adminClient.ListTopics(ctx)
	if err != nil {
		die("failed to list topics: %v", err)
	}

	if topicDetails.Has(topicName) {
		fmt.Printf("topic %v already exists\n", topicName)
		return
	}
	fmt.Printf("Creating topic %v\n", topicName)

	createTopicResponse, err := adminClient.CreateTopic(ctx, -1, -1, nil, topicName)
	if err != nil {
		die("failed to create topic: %v", err)
	}
	fmt.Printf("Successfully created topic %v\n", createTopicResponse.Topic)
}
