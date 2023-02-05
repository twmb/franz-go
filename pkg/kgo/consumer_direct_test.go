package kgo

import (
	"context"
	"testing"
	"time"
)

func TestIssue325(t *testing.T) {
	topic, cleanup := tmpTopic(t)
	defer cleanup()

	cl, _ := NewClient(
		getSeedBrokers(),
		DefaultProduceTopic(topic),
	)
	if err := cl.ProduceSync(context.Background(), StringRecord("foo")).FirstErr(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cl.AddConsumeTopics(topic)
	recs := cl.PollFetches(ctx).Records()
	if len(recs) != 1 && string(recs[0].Value) != "foo" {
		t.Fatal(recs)
	}
}

func TestIssue337(t *testing.T) {
	topic, cleanup := tmpTopicPartitions(t, 2)
	defer cleanup()

	cl, _ := NewClient(
		getSeedBrokers(),
		DefaultProduceTopic(topic),
		RecordPartitioner(ManualPartitioner()),
		ConsumePartitions(map[string]map[int32]Offset{
			topic: {0: NewOffset().At(0)},
		}),
	)

	if err := cl.ProduceSync(context.Background(),
		&Record{Partition: 0, Value: []byte("foo")},
		&Record{Partition: 1, Value: []byte("bar")},
	).FirstErr(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for {
		fs := cl.PollFetches(ctx)
		if err := fs.Err0(); err == context.DeadlineExceeded {
			break
		}
		recs := fs.Records()
		if len(recs) != 1 {
			t.Fatalf("incorrect number of records, saw: %v", len(recs))
		} else if string(recs[0].Value) != "foo" {
			t.Fatalf("wrong value, got: %s", recs[0].Value)
		}
	}
}
