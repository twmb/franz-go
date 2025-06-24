package main

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	exampleGroup = "example-group"
	exampleTopic = "example"
)

type consumer struct {
	client *kgo.Client
}

type message struct {
	topic     string
	key       []byte
	value     []byte
	timestamp time.Time
	offset    int64
	partition int32
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	var err error
	c := &consumer{}
	var wg sync.WaitGroup

	c.client, err = kgo.NewClient([]kgo.Opt{
		kgo.SeedBrokers([]string{"localhost:9092"}...),
		kgo.ConsumerGroup(exampleGroup),
		kgo.ConsumeTopics(exampleTopic),
		kgo.AutoCommitMarks(),
		kgo.AutoCommitInterval(3 * time.Second),
		kgo.OnPartitionsRevoked(c.revoked),
		kgo.BlockRebalanceOnPoll(),
	}...)
	if err != nil {
		panic(err)
	}
	defer c.client.Close()
	if err = c.client.Ping(ctx); err != nil {
		panic(err)
	}

	wg.Add(1)
	go c.run(ctx, &wg)

	// Waiting for stop signal.
	<-ctx.Done()
	wg.Wait()
}

func (c *consumer) run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			fetches := c.client.PollFetches(ctx)
			if fetches.IsClientClosed() {
				return
			}
			if fetches.Empty() {
				continue
			}
			fetches.EachError(func(_ string, _ int32, err error) {
				panic(err)
			})
			fetches.EachPartition(func(p kgo.FetchTopicPartition) {
				var epoch int32
				var offset int64
				for _, r := range p.Records {
					fmt.Printf("Handled record, p: %d, o: %d, e: %d\n", p.Partition, r.Offset, r.LeaderEpoch)
					epoch = r.LeaderEpoch
					offset = r.Offset + 1
				}
				c.client.MarkCommitOffsets(map[string]map[int32]kgo.EpochOffset{p.Topic: {
					p.Partition: kgo.EpochOffset{Epoch: epoch, Offset: offset},
				}})
			})
			// We block rebalance on poll so that we can mark all records
			// polled as eligible to be committed. We don't want to have a
			// rebalance happen that loses us partitions, and then mark
			// in the client that we actually want to still commit for those
			// partitions.
			c.client.AllowRebalance()
		}
	}
}

func (c *consumer) process(r *kgo.Record) error {
	msg := &message{
		topic:     r.Topic,
		key:       r.Key,
		value:     r.Value,
		timestamp: r.Timestamp,
		offset:    r.Offset,
		partition: r.Partition,
	}
	if rand.IntN(100)%2 != 0 {
		// Simulate error.
		return errors.New("failed to process record")
	}

	// Simulate normal behavior.
	fmt.Printf("%v", msg)
	return nil
}

func (c *consumer) revoked(ctx context.Context, cl *kgo.Client, _ map[string][]int32) {
	// Need to commit all marked offsets before the partitions are revoked.
	if err := cl.CommitMarkedOffsets(ctx); err != nil {
		fmt.Printf("Failed to commit marked offsets: %v\n", err)
	}
	// Note: please look at the goroutine-per-partition examples
	// if you want better concurrency and safe handling of partitions.
}
