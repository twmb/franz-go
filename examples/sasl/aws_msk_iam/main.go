package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"os"
	"sort"
	"strings"
	"text/tabwriter"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/aws"
)

var seedBrokers = flag.String("brokers", "localhost:9092", "comma delimited list of seed brokers")

func die(msg string, args ...interface{}) {
	fmt.Printf(msg+"\n", args...)
	os.Exit(1)
}

func main() {
	flag.Parse()

	sess, err := session.NewSession()
	if err != nil {
		die("unable to initialize aws session: %v", err)
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),

		kgo.SASL(aws.ManagedStreamingIAM(func(ctx context.Context) (aws.Auth, error) {
			val, err := sess.Config.Credentials.GetWithContext(ctx)
			if err != nil {
				return aws.Auth{}, err
			}
			return aws.Auth{
				AccessKey:    val.AccessKeyID,
				SecretKey:    val.SecretAccessKey,
				SessionToken: val.SessionToken,
				UserAgent:    "franz-go/creds_test/v1.0.0",
			}, nil
		})),

		kgo.Dialer((&tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}}).DialContext),
	)
	if err != nil {
		die("unable to create client: %v", err)
	}
	defer cl.Close()

	resp, err := kmsg.NewPtrMetadataRequest().RequestWith(context.Background(), cl)
	if err != nil {
		die("unable to request metadata: %v", err)
	}

	if resp.ClusterID != nil {
		fmt.Printf("\nCLUSTER\n======\n%s\n", *resp.ClusterID)
	}

	fmt.Printf("\nBROKERS\n======\n")
	printBrokers(resp.ControllerID, resp.Brokers)

	fmt.Printf("\nTOPICS\n======\n")
	printTopics(resp.Topics)
	fmt.Println()
}

func beginTabWrite() *tabwriter.Writer {
	return tabwriter.NewWriter(os.Stdout, 6, 4, 2, ' ', 0)
}

func printBrokers(controllerID int32, brokers []kmsg.MetadataResponseBroker) {
	sort.Slice(brokers, func(i, j int) bool {
		return brokers[i].NodeID < brokers[j].NodeID
	})

	tw := beginTabWrite()
	defer tw.Flush()

	fmt.Fprintf(tw, "ID\tHOST\tPORT\tRACK\n")
	for _, broker := range brokers {
		var controllerStar string
		if broker.NodeID == controllerID {
			controllerStar = "*"
		}

		var rack string
		if broker.Rack != nil {
			rack = *broker.Rack
		}

		fmt.Fprintf(tw, "%d%s\t%s\t%d\t%s\n",
			broker.NodeID, controllerStar, broker.Host, broker.Port, rack)
	}
}

func printTopics(topics []kmsg.MetadataResponseTopic) {
	// We request with no topic IDs, so we should not receive nil topics.
	sort.Slice(topics, func(i, j int) bool {
		return *topics[i].Topic < *topics[j].Topic
	})

	tw := beginTabWrite()
	defer tw.Flush()

	fmt.Fprintf(tw, "NAME\tPARTITIONS\tREPLICAS\n")
	for _, topic := range topics {
		parts := len(topic.Partitions)
		replicas := 0
		if parts > 0 {
			replicas = len(topic.Partitions[0].Replicas)
		}
		fmt.Fprintf(tw, "%s\t%d\t%d\n", *topic.Topic, parts, replicas)
	}
}
