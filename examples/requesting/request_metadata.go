package requesting

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

func requestMetadata() {
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

	// Construct message request and send it to Kafka
	req := kmsg.MetadataRequest{
		Topics: []kmsg.MetadataRequestTopic{},
	}

	res, err := req.RequestWith(ctx, client)
	if err != nil {
		// Error during request has happened (e. g. context cancelled)
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
