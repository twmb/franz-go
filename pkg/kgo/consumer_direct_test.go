package kgo

import (
	"context"
	"testing"
	"time"
)

func TestIssue325(t *testing.T) {
	t.Parallel()

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
	t.Parallel()

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

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var recs []*Record
out:
	for {
		fs := cl.PollFetches(ctx)
		switch err := fs.Err0(); err {
		default:
			t.Fatalf("unexpected error: %v", err)
		case context.DeadlineExceeded:
			break out
		case nil:
		}
		recs = append(recs, fs.Records()...)
	}
	if len(recs) != 1 {
		t.Fatalf("incorrect number of records, saw: %v", len(recs))
	}
	if string(recs[0].Value) != "foo" {
		t.Fatalf("wrong value, got: %s", recs[0].Value)
	}
}

func TestDirectPartitionPurge(t *testing.T) {
	t.Parallel()

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
	cl.PurgeTopicsFromClient(topic)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	fs := cl.PollFetches(ctx)
	cancel()
	if err := fs.Err0(); err != context.DeadlineExceeded {
		t.Fatal("unexpected success when expecting context.DeadlineExceeded")
	}

	cl.AddConsumeTopics(topic)
	ctx, cancel = context.WithTimeout(context.Background(), 7*time.Second)
	defer cancel()

	exp := map[string]bool{
		"foo": true,
		"bar": true,
	}
	for {
		fs := cl.PollFetches(ctx)
		if err := fs.Err0(); err == context.DeadlineExceeded {
			break
		}
		fs.EachRecord(func(r *Record) {
			v := string(r.Value)
			if !exp[v] {
				t.Errorf("saw unexpected value %v", v)
			}
			delete(exp, v)
		})
	}
	if len(exp) > 0 {
		t.Errorf("did not see expected values %v", exp)
	}
}
