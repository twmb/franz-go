package main

import (
	"context"
	"errors"
	"log"
	"math/rand/v2"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
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
		kgo.ConsumerGroup("example-group"),
		kgo.ConsumeTopics("example"),
		kgo.AutoCommitMarks(),
		kgo.AutoCommitInterval(3 * time.Second), //default is 5s
		kgo.OnPartitionsRevoked(c.revoked),
	}...)
	if err != nil {
		log.Fatal("failed to create consumer")
	}
	defer c.client.Close()
	if err = c.client.Ping(ctx); err != nil {
		log.Fatal("failed to ping consumer")
	}

	wg.Add(1)
	go c.run(ctx, &wg)

	//waiting for stop signal
	<-ctx.Done()
	wg.Wait()
	log.Println("ok")
}

func (c *consumer) run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	log.Print("start consumer")
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
			fetches.EachPartition(func(p kgo.FetchTopicPartition) {
				var epoch int32
				var offset int64
				for _, r := range p.Records {
					if err := c.process(r); err != nil {
						// do other job like retry and dlq
						log.Println("failed to process record", err)
					}
					epoch = r.LeaderEpoch
					offset = r.Offset + 1
				}
				c.client.MarkCommitOffsets(map[string]map[int32]kgo.EpochOffset{p.Topic: {
					p.Partition: kgo.EpochOffset{Epoch: epoch, Offset: offset},
				}})
			})
		}
	}
}

func (c *consumer) process(r *kgo.Record) error {
	//consider using sync.Pool to decrease mem allocation for message struct
	msg := &message{
		topic:     r.Topic,
		key:       r.Key,
		value:     r.Value,
		timestamp: r.Timestamp,
		offset:    r.Offset,
		partition: r.Partition,
	}
	if rand.IntN(100)%2 != 0 {
		//simulate error
		return errors.New("failed to process record")
	}

	//simulate normal behavior
	log.Printf("%v", msg)
	return nil
}

func (c *consumer) revoked(ctx context.Context, cl *kgo.Client, _ map[string][]int32) {
	//need to commit all marked offsets before the partitions are revoked
	if err := cl.CommitMarkedOffsets(ctx); err != nil {
		log.Println("failed to commit marked offsets", err)
	}
	//do handling revoked partitions
	//see goroutine_per_partition_consuming examples folder
}
