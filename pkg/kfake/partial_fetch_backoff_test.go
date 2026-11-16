package kfake

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestPartialFetchErrorBackoff(t *testing.T) {
	const topic = "partial-fetch-error"
	c := newCluster(t, NumBrokers(1), SeedTopics(2, topic))
	producer := newPlainClient(t, c, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for partition := range int32(2) {
		if err := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: partition, Value: []byte("initial")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	consumer := newPlainClient(t, c,
		kgo.ConsumeTopics(topic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(100*time.Millisecond),
		kgo.RetryBackoffFn(func(int) time.Duration { return 100 * time.Millisecond }),
	)
	consumeN(t, consumer, 2, 5*time.Second)
	attempts := c.Fault(Fault{Keys: []kmsg.Key{kmsg.Fetch}, Topic: topic, Partitions: []int32{1}, Observe: true, Count: -1})
	fault := c.Fault(Fault{Keys: []kmsg.Key{kmsg.Fetch}, Topic: topic, Partitions: []int32{1}, Err: kerr.NotLeaderForPartition, Count: -1})
	pollCtx, stopPoll := context.WithCancel(ctx)
	polled := make(chan *kgo.Record, 10)
	done := make(chan struct{})
	go func() {
		defer close(done)
		for pollCtx.Err() == nil {
			for _, record := range consumer.PollFetches(pollCtx).Records() {
				polled <- record
			}
		}
	}()
	t.Cleanup(func() { stopPoll(); <-done })
	if err := attempts.Wait(ctx, 1); err != nil {
		t.Fatal(err)
	}
	before := attempts.Hits()
	time.Sleep(400 * time.Millisecond)
	got := attempts.Hits() - before
	t.Logf("partial-error fetches in 400ms: %d", got)
	if got > 12 {
		t.Errorf("partial error caused %d fetches in 400ms, want backoff-limited retries", got)
	}
	// The healthy partition still produces application records, and the bad
	// partition resumes once its retryable error is removed.
	for partition := range int32(2) {
		if partition == 1 {
			fault.Remove()
		}
		if err := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: partition, Value: []byte("next")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
		select {
		case record := <-polled:
			if record.Partition != partition || record.Offset != 1 {
				t.Errorf("received partition/offset %d/%d, want %d/1", record.Partition, record.Offset, partition)
			}
		case <-ctx.Done():
			t.Fatal("partition did not make progress after the partial fetch error")
		}
	}
}
