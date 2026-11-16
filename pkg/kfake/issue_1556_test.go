package kfake

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestIssue1556(t *testing.T) {
	t.Parallel()
	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))

	producer := newPlainClient(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for range 2 {
		if err := producer.ProduceSync(ctx, &kgo.Record{Topic: topic, Partition: 0, Value: []byte("v")}).FirstErr(); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}

	c.Fault(Fault{Keys: []kmsg.Key{kmsg.OffsetForLeaderEpoch}, Topic: topic, Err: kerr.UnknownTopicOrPartition, Count: -1})

	cl := newPlainClient(t, c, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
		topic: {0: kgo.NewOffset().At(1).WithEpoch(0)},
	}))
	for {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatal("no error for the partition while its epoch validation kept failing")
		}
		if n := len(fs.Records()); n > 0 {
			t.Fatalf("consumed %d records while the epoch validation failed", n)
		}
		var got bool
		fs.EachError(func(tp string, p int32, err error) {
			if tp == topic && p == 0 && errors.Is(err, kerr.UnknownTopicOrPartition) {
				got = true
			}
		})
		if got {
			return
		}
	}
}
