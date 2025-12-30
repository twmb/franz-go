package kfake

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// TestSeedTopicsProduceSync_NoHang verifies that producing to a topic created
// via SeedTopics succeeds without hanging in the client metadata path.
func TestSeedTopicsProduceSync_NoHang(t *testing.T) {
	const topic = "test_seed_topic"

	cluster, err := NewCluster(SeedTopics(1, topic))
	if err != nil {
		t.Fatalf("NewCluster failed: %v", err)
	}
	defer cluster.Close()

	client, err := kgo.NewClient(
		kgo.SeedBrokers(cluster.ListenAddrs()...),
	)
	if err != nil {
		t.Fatalf("NewClient failed: %v", err)
	}
	defer client.Close()

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()
	if err := client.Ping(pingCtx); err != nil {
		t.Fatalf("Ping failed: %v", err)
	}

	produceCtx, produceCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer produceCancel()

	results := client.ProduceSync(produceCtx,
		&kgo.Record{Topic: topic, Value: []byte("value1")},
		&kgo.Record{Topic: topic, Value: []byte("value2")},
	)
	if err := results.FirstErr(); err != nil {
		t.Fatalf("ProduceSync FirstErr: %v", err)
	}
}

