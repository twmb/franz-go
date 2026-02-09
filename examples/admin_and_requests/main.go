// This example demonstrates two ways to perform admin operations:
//
//   - kadm: the high-level admin client that wraps common operations
//   - kmsg: issuing raw Kafka protocol requests for full control
//
// The kadm approach is simpler and recommended for most use cases. The kmsg
// approach gives access to every field in every Kafka request/response and
// is useful when kadm does not cover your needs.
//
// Run with -mode=kadm or -mode=kmsg to see each approach.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

var (
	seedBrokers = flag.String("brokers", "localhost:9092", "comma delimited list of seed brokers")
	topicName   = flag.String("topic", "franz-go-example", "topic to create")
	mode        = flag.String("mode", "kadm", "admin mode: kadm, kmsg")
)

func die(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, msg, args...)
	os.Exit(1)
}

func main() {
	flag.Parse()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),
		// MaxVersions pins the max request version. This avoids
		// accidentally using newer request fields that require
		// additional properties to be set.
		kgo.MaxVersions(kversion.V2_4_0()),
	)
	if err != nil {
		die("unable to create client: %v\n", err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	switch *mode {
	case "kadm":
		demoKadm(ctx, cl)
	case "kmsg":
		demoKmsg(ctx, cl)
	default:
		die("unknown mode %q\n", *mode)
	}
}

// demoKadm uses the high-level kadm admin client to create a topic
// and list existing topics.
func demoKadm(ctx context.Context, cl *kgo.Client) {
	adm := kadm.NewClient(cl)

	topics, err := adm.ListTopics(ctx)
	if err != nil {
		die("unable to list topics: %v\n", err)
	}
	if topics.Has(*topicName) {
		fmt.Printf("topic %q already exists\n", *topicName)
		return
	}

	resp, err := adm.CreateTopic(ctx, -1, -1, nil, *topicName)
	if err != nil {
		die("unable to create topic: %v\n", err)
	}
	fmt.Printf("created topic %q\n", resp.Topic)

	// List topics again to confirm.
	topics, err = adm.ListTopics(ctx)
	if err != nil {
		die("unable to list topics: %v\n", err)
	}
	fmt.Printf("cluster has %d topics\n", len(topics))
}

// demoKmsg uses raw kmsg requests for full protocol-level control.
// This creates a topic and then issues a metadata request to inspect it.
func demoKmsg(ctx context.Context, cl *kgo.Client) {
	// Because Go does not have RAII and the Kafka protocol can add new
	// fields at any time, always use NewPtr / NewXyz functions from kmsg.
	// These call Default() which is forward compatible when Kafka adds
	// new fields that require non-zero defaults.

	// Create a topic.
	{
		req := kmsg.NewPtrCreateTopicsRequest()
		t := kmsg.NewCreateTopicsRequestTopic()
		t.Topic = *topicName
		t.NumPartitions = 1
		t.ReplicationFactor = 1
		req.Topics = append(req.Topics, t)

		res, err := req.RequestWith(ctx, cl)
		if err != nil {
			die("create topic request failed: %v\n", err)
		}
		if len(res.Topics) != 1 {
			die("expected 1 topic in response, got %d\n", len(res.Topics))
		}
		if err := kerr.ErrorForCode(res.Topics[0].ErrorCode); err != nil {
			fmt.Fprintf(os.Stderr, "topic creation error: %v\n", err)
			// TopicAlreadyExists is not fatal for this demo.
		} else {
			fmt.Printf("created topic %q via kmsg\n", res.Topics[0].Topic)
		}
	}

	// Issue a metadata request for the topic.
	{
		req := kmsg.NewPtrMetadataRequest()
		t := kmsg.NewMetadataRequestTopic()
		t.Topic = kmsg.StringPtr(*topicName)
		req.Topics = append(req.Topics, t)

		res, err := req.RequestWith(ctx, cl)
		if err != nil {
			die("metadata request failed: %v\n", err)
		}
		for _, topic := range res.Topics {
			if err := kerr.ErrorForCode(topic.ErrorCode); err != nil {
				fmt.Printf("topic %s: error %v\n", *topic.Topic, err)
				continue
			}
			fmt.Printf("topic %s: %d partitions\n", *topic.Topic, len(topic.Partitions))
		}
		fmt.Printf("cluster has %d brokers\n", len(res.Brokers))
	}
}
