package kfake

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestCommitRecordsDeadlineBehindCommit(t *testing.T) {
	for _, prior := range []string{"sync", "async"} {
		t.Run(prior, func(t *testing.T) {
			const topic = "commit-context"
			c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
			producer := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
			produceNStrings(t, producer, topic, 3)
			consumer := newPlainClient(t, c,
				kgo.ConsumerGroup("commit-context"),
				kgo.ConsumeTopics(topic),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
				kgo.DisableAutoCommit(),
			)
			records := consumeN(t, consumer, 3, 10*time.Second)

			gate := make(chan struct{})
			var once sync.Once
			release := func() { once.Do(func() { close(gate) }) }
			defer release()
			seen := make(chan int64, 10)
			c.ControlKey(int16(kmsg.OffsetCommit), func(request kmsg.Request) (kmsg.Response, error, bool) {
				c.KeepControl()
				req := request.(*kmsg.OffsetCommitRequest)
				offset := req.Topics[0].Partitions[0].Offset
				seen <- offset
				if offset == 1 {
					c.SleepControl(func() { <-gate })
				}
				return nil, nil, false
			})
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			firstDone := make(chan error, 1)
			if prior == "sync" {
				go func() { firstDone <- consumer.CommitRecords(ctx, records[0]) }()
			} else {
				offsets := map[string]map[int32]kgo.EpochOffset{topic: {0: {Epoch: records[0].LeaderEpoch, Offset: 1}}}
				consumer.CommitOffsets(ctx, offsets, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, _ *kmsg.OffsetCommitResponse, err error) {
					firstDone <- err
				})
			}
			select {
			case offset := <-seen:
				if offset != 1 {
					t.Fatalf("first offset = %d, want 1", offset)
				}
			case <-ctx.Done():
				t.Fatal("first commit did not reach the broker")
			}

			deadline, cancelDeadline := context.WithTimeout(ctx, 100*time.Millisecond)
			defer cancelDeadline()
			done := make(chan error, 1)
			go func() { done <- consumer.CommitRecords(deadline, records[1]) }()
			select {
			case err := <-done:
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Errorf("queued commit = %v, want context deadline exceeded", err)
				}
			case <-time.After(time.Second):
				release()
				<-done
				t.Fatal("queued commit ignored its deadline")
			}

			thirdDone := make(chan error, 1)
			go func() { thirdDone <- consumer.CommitRecords(ctx, records[2]) }()
			select {
			case offset := <-seen:
				t.Errorf("offset %d reached the broker before the first commit finished", offset)
			case <-time.After(30 * time.Millisecond):
			}
			release()
			for _, result := range []<-chan error{firstDone, thirdDone} {
				select {
				case err := <-result:
					if err != nil {
						t.Errorf("uncanceled commit failed: %v", err)
					}
				case <-ctx.Done():
					t.Fatal("commit did not finish after releasing the broker")
				}
			}
			select {
			case offset := <-seen:
				if offset != 3 {
					t.Errorf("next offset = %d, want 3; canceled offset 2 must not be sent", offset)
				}
			default:
				t.Error("third commit did not reach the broker")
			}
		})
	}
}
