Testing
===

This document describes how to test code that uses franz-go.

The franz-go package does not provide an interface for the `kgo.Client` struct,
and this is not planned. See issue [#58](https://github.com/twmb/franz-go/issues/58)
for a detailed explanation. However, this does not prevent you from testing your code.

Package [`kfake`](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kfake) provides
a fake Kafka cluster for testing purposes. This allows you to create a fake cluster
and control each request, either to verify sent data or to prepare custom responses.

The fake cluster runs entirely in-memory and does not require any external
processes. It implements the Kafka protocol and can handle most common client
operations including producing, consuming, metadata requests, and more.

The `ControlKey` method allows you to intercept and control specific request
types. This is useful for verifying that your code sends the correct data, or
for simulating specific server responses (including errors).

The example below demonstrates testing producing requests and verifying that
correct data is sent to the correct topic.

For more examples, see the [kfake tests](https://github.com/twmb/franz-go/blob/main/pkg/kfake/issues_test.go).

```go
import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestProduceToTopicWithKey(t *testing.T) {
	const (
		testTopic = "test-topic"
		testKey   = "test-key"
		testValue = "test-value"
	)
	fakeCluster, err := kfake.NewCluster(
		kfake.SeedTopics(1, testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer fakeCluster.Close()

	client, err := kgo.NewClient(kgo.SeedBrokers(fakeCluster.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()

	var produceTopicID []byte
	produceCnt := 0
	var innerErr error
	fakeCluster.ControlKey(int16(kmsg.Produce), func(r kmsg.Request) (kmsg.Response, error, bool) {
		req := r.(*kmsg.ProduceRequest)

		b := kmsg.NewRecordBatch()
		err := b.ReadFrom(req.Topics[0].Partitions[0].Records)
		if err != nil {
			innerErr = err
		}
		if b.NumRecords != 1 {
			innerErr = fmt.Errorf("expected 1 record, got %d", b.NumRecords)
		}

		rr := kmsg.NewRecord()
		err = rr.ReadFrom(b.Records)
		if err != nil {
			innerErr = err
		}

		if string(rr.Key) != testKey {
			innerErr = fmt.Errorf("expected key %s, got %s", testKey, string(rr.Key))
		}
		if string(rr.Value) != testValue {
			innerErr = fmt.Errorf("expected value %s, got %s", testValue, string(rr.Value))
		}

		produceTopicID = req.Topics[0].TopicID[:]
		produceCnt++
		return nil, nil, false
	})

	err = client.ProduceSync(context.Background(), &kgo.Record{
		Topic: testTopic,
		Value: []byte(testValue),
		Key:   []byte(testKey),
	}).FirstErr()
	if err != nil {
		t.Fatal(err)
	}

	if innerErr != nil {
		t.Fatal(innerErr)
	}

	if produceCnt != 1 {
		t.Fatalf("expected 1 produce, got %d", produceCnt)
	}
	if !bytes.Equal(produceTopicID, fakeCluster.TopicInfo(testTopic).TopicID[:]) {
		t.Fatalf("expected topic id %s, got %s",
			hex.EncodeToString(fakeCluster.TopicInfo(testTopic).TopicID[:]),
			hex.EncodeToString(produceTopicID))
	}
}
```
