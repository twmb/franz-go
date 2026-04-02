package main

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestProduceMultipleRecordsWithVirtualNetwork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stack := kfake.NewVirtualNetwork()

		cluster, err := kfake.NewCluster(
			kfake.NumBrokers(1),
			kfake.Ports(9093),
			kfake.SeedTopics(3, "multi-topic"),
			kfake.ListenFn(stack.Listen),
		)
		if err != nil {
			t.Fatalf("NewCluster: %v", err)
		}
		defer cluster.Close()

		client, err := kgo.NewClient(
			kgo.SeedBrokers(cluster.ListenAddrs()...),
			kgo.DefaultProduceTopic("multi-topic"),
			kgo.ConsumeTopics("multi-topic"),
			kgo.Dialer(stack.DialContext),
		)
		if err != nil {
			t.Fatalf("NewClient: %v", err)
		}
		defer client.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		const numRecords = 10
		for i := range numRecords {
			record := &kgo.Record{Value: []byte{byte(i)}}
			if err := client.ProduceSync(ctx, record).FirstErr(); err != nil {
				t.Fatalf("produce %d failed: %v", i, err)
			}
		}

		var consumed int
		for consumed < numRecords {
			fetches := client.PollFetches(ctx)
			consumed += fetches.NumRecords()
		}
		if consumed != numRecords {
			t.Errorf("expected %d records, got %d", numRecords, consumed)
		}
	})
}

func TestTimeoutWithVirtualNetwork(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		stack := kfake.NewVirtualNetwork()

		cluster, err := kfake.NewCluster(
			kfake.NumBrokers(1),
			kfake.Ports(9094),
			kfake.SeedTopics(1, "timeout-topic"),
			kfake.ListenFn(stack.Listen),
		)
		if err != nil {
			t.Fatalf("NewCluster: %v", err)
		}
		defer cluster.Close()

		consumer, err := kgo.NewClient(
			kgo.SeedBrokers(cluster.ListenAddrs()...),
			kgo.ConsumeTopics("timeout-topic"),
			kgo.Dialer(stack.DialContext),
			kgo.FetchMaxWait(100*time.Millisecond),
		)
		if err != nil {
			t.Fatalf("NewClient: %v", err)
		}
		defer consumer.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer cancel()

		fetches := consumer.PollFetches(ctx)
		if fetches.NumRecords() != 0 {
			t.Errorf("expected 0 records from empty topic, got %d", fetches.NumRecords())
		}
	})
}
