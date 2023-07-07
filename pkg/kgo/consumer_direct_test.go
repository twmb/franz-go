package kgo

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// Allow adding a topic to consume after the client is initialized with nothing
// to consume.
func TestIssue325(t *testing.T) {
	t.Parallel()

	topic, cleanup := tmpTopic(t)
	defer cleanup()

	cl, _ := NewClient(
		getSeedBrokers(),
		DefaultProduceTopic(topic),
		UnknownTopicRetries(-1),
	)
	defer cl.Close()

	if err := cl.ProduceSync(context.Background(), StringRecord("foo")).FirstErr(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	cl.AddConsumeTopics(topic)
	recs := cl.PollFetches(ctx).Records()
	if len(recs) != 1 || string(recs[0].Value) != "foo" {
		t.Fatal(recs)
	}
}

// Ensure we only consume one partition if we only ask for one partition.
func TestIssue337(t *testing.T) {
	t.Parallel()

	topic, cleanup := tmpTopicPartitions(t, 2)
	defer cleanup()

	cl, _ := NewClient(
		getSeedBrokers(),
		DefaultProduceTopic(topic),
		RecordPartitioner(ManualPartitioner()),
		UnknownTopicRetries(-1),
		ConsumePartitions(map[string]map[int32]Offset{
			topic: {0: NewOffset().At(0)},
		}),
	)
	defer cl.Close()

	if err := cl.ProduceSync(context.Background(),
		&Record{Partition: 0, Value: []byte("foo")},
		&Record{Partition: 1, Value: []byte("bar")},
	).FirstErr(); err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
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
		UnknownTopicRetries(-1),
		ConsumePartitions(map[string]map[int32]Offset{
			topic: {0: NewOffset().At(0)},
		}),
	)
	defer cl.Close()

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

// Ensure a deleted topic while regex consuming is no longer fetched.
func TestIssue434(t *testing.T) {
	t.Parallel()

	var (
		t1, cleanup1 = tmpTopicPartitions(t, 1)
		t2, cleanup2 = tmpTopicPartitions(t, 1)
	)
	defer cleanup1()
	defer cleanup2()

	cl, _ := NewClient(
		getSeedBrokers(),
		UnknownTopicRetries(-1),
		ConsumeTopics(fmt.Sprintf("(%s|%s)", t1, t2)),
		ConsumeRegex(),
		FetchMaxWait(100*time.Millisecond),
		KeepRetryableFetchErrors(),
	)
	defer cl.Close()

	if err := cl.ProduceSync(context.Background(),
		&Record{Topic: t1, Value: []byte("t1")},
		&Record{Topic: t2, Value: []byte("t2")},
	).FirstErr(); err != nil {
		t.Fatal(err)
	}
	cleanup2()

	// This test is a slight heuristic check test. We are keeping retryable
	// errors, so if the purge is successful, then we expect no response
	// and we expect the fetch to just contain context.DeadlineExceeded.
	//
	// We can get the topic in the response for a little bit if our fetch
	// is fast enough, so we ignore any errors (UNKNOWN_TOPIC_ID) at the
	// start. We want to ensure the topic is just outright missing from
	// the response because that will mean it is internally purged.
	start := time.Now()
	var missingTopic int
	for missingTopic < 2 {
		if time.Since(start) > 30*time.Second {
			t.Fatal("still seeing topic after 30s")
		}

		ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
		fs := cl.PollFetches(ctx)
		cancel()
		var foundTopic bool
		fs.EachTopic(func(ft FetchTopic) {
			if ft.Topic == t2 {
				foundTopic = true
			}
		})
		if !foundTopic {
			missingTopic++
		}
	}
}
