package main

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

func main() {
	seeds := []string{"localhost:9092"}
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

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Construct message request and send it to Kafka.
	//
	// Because Go does not have RAII, and the Kafka protocol can add new fields
	// at any time, all struct initializes from the kmsg package should use the
	// NewXyz functions (or NewPtr for requests). These functions call "Default"
	// before returning, which is forward compatible when Kafka adds new fields
	// that require non-zero defaults.

	// For the first request, we will create topic foo with 1 partition and
	// a replication factor of 1.
	{
		req := kmsg.NewPtrCreateTopicsRequest()
		topic := kmsg.NewCreateTopicsRequestTopic()
		topic.Topic = "foo"
		topic.NumPartitions = 1
		topic.ReplicationFactor = 1
		req.Topics = append(req.Topics, topic)

		res, err := req.RequestWith(ctx, client)
		if err != nil {
			// Error during request has happened (e. g. context cancelled)
			panic(err)
		}

		if len(res.Topics) != 1 {
			panic(fmt.Sprintf("expected one topic in response, saw %d", len(res.Topics)))
		}
		t := res.Topics[0]

		if err := kerr.ErrorForCode(t.ErrorCode); err != nil {
			fmt.Fprintf(os.Stderr, "topic creation failure: %v", err)
			return
		}
		fmt.Printf("topic %s created successfully!", t.Topic)
	}

	// Now we will issue a metadata request to see that topic.
	{
		req := kmsg.NewPtrMetadataRequest()
		topic := kmsg.NewMetadataRequestTopic()
		topic.Topic = kmsg.StringPtr("foo")
		req.Topics = append(req.Topics, topic)

		res, err := req.RequestWith(ctx, client)
		if err != nil {
			panic(err)
		}

		// Check response for Kafka error codes and print them.
		// Other requests might have top level error codes, which indicate completed but failed requests.
		for _, topic := range res.Topics {
			err := kerr.ErrorForCode(topic.ErrorCode)
			if err != nil {
				fmt.Printf("topic %v response has errored: %v\n", topic.Topic, err.Error())
			}
		}

		fmt.Printf("received '%v' topics and '%v' brokers", len(res.Topics), len(res.Brokers))
	}
}
