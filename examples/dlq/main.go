// This example demonstrates a dead letter queue (DLQ) pattern: records that
// fail processing after retries are forwarded to a separate DLQ topic with
// error metadata in headers. A DLQ consumer can later inspect and reprocess
// these failed records.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"math/rand/v2"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	exampleGroup     = "example-group"
	exampleTopic     = "example"
	dlqTopic         = "dlq"
	maxRecRetryCount = 3
)

type kafka struct {
	producer *kgo.Client
	consumer *kgo.Client
}

type message struct {
	Topic       string    `json:"topic"`
	Key         []byte    `json:"key"`
	Value       []byte    `json:"value"`
	Timestamp   time.Time `json:"timestamp"`
	Offset      int64     `json:"offset"`
	Partition   int32     `json:"partition"`
	LeaderEpoch int32     `json:"leader_epoch"`
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	k := &kafka{}
	var wg sync.WaitGroup
	if err := k.connectProducer(ctx); err != nil {
		panic(err)
	}
	if err := k.connectConsumer(ctx); err != nil {
		panic(err)
	}

	// Populate fake data.
	for i := 0; i < 10; i++ {
		k.producer.Produce(ctx, &kgo.Record{
			Topic:     exampleTopic,
			Key:       []byte("key" + strconv.Itoa(i)),
			Value:     []byte("value" + strconv.Itoa(i)),
			Timestamp: time.Now(),
		}, nil)
	}
	wg.Add(1)
	go k.run(ctx, &wg)

	// Waiting for the stop signal.
	<-ctx.Done()
	k.close()
	wg.Wait()
}

func (k *kafka) connectConsumer(ctx context.Context) error {
	var err error
	k.consumer, err = kgo.NewClient([]kgo.Opt{
		kgo.SeedBrokers([]string{"localhost:9092"}...),
		kgo.ConsumerGroup(exampleGroup),
		kgo.ConsumeTopics(exampleTopic),
	}...)
	if err != nil {
		return err
	}
	if err = k.consumer.Ping(ctx); err != nil {
		return err
	}
	return nil
}

func (k *kafka) connectProducer(ctx context.Context) error {
	var err error
	k.producer, err = kgo.NewClient([]kgo.Opt{
		kgo.SeedBrokers([]string{"localhost:9092"}...),
		kgo.AllowAutoTopicCreation(),
	}...)
	if err != nil {
		return err
	}
	if err = k.producer.Ping(ctx); err != nil {
		return err
	}
	return nil
}

func (k *kafka) run(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case <-ctx.Done():
			return
		default:
			fetches := k.consumer.PollFetches(ctx)
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
				// Note: please look at the goroutine-per-partition examples
				// if you want better concurrency.
				for _, rec := range p.Records {
					if err := k.process(rec); err != nil {
						// Before DLQ we probably want to add a retry mechanism
						// to make sure that there's no obvious errors.
						if err = k.retry(rec); err != nil {
							failed, _ := json.Marshal(&message{
								Topic:       rec.Topic,
								Key:         rec.Key,
								Value:       rec.Value,
								Timestamp:   rec.Timestamp,
								Offset:      rec.Offset,
								Partition:   rec.Partition,
								LeaderEpoch: rec.LeaderEpoch,
							})
							// Then DLQ consumer should handle produced records.
							k.producer.Produce(ctx, &kgo.Record{
								Topic:     dlqTopic,
								Key:       rec.Key,
								Value:     failed,
								Timestamp: time.Now(),
								Headers: []kgo.RecordHeader{
									// Some headers as review info.
									{Key: "status", Value: []byte("review")},
									{Key: "error", Value: []byte(err.Error())},
								},
							}, nil)
						}
					}
				}
			})
		}
	}
}

func (k *kafka) process(r *kgo.Record) error {
	// Simulate load.
	time.Sleep(1 * time.Second)
	msg := &message{
		Topic:       r.Topic,
		Key:         r.Key,
		Value:       r.Value,
		Timestamp:   r.Timestamp,
		Offset:      r.Offset,
		Partition:   r.Partition,
		LeaderEpoch: r.LeaderEpoch,
	}
	if rand.IntN(100)%2 != 0 {
		// Simulate error.
		return errors.New("failed to process record")
	}
	// Simulate normal behavior.
	log.Printf("%v", msg)
	return nil
}

func (k *kafka) retry(r *kgo.Record) error {
	var err error
	for i := 1; i <= maxRecRetryCount; i++ {
		if err = k.process(r); err != nil {
			// Simulate backoff.
			time.Sleep(time.Duration(i*2) * time.Second)
		} else {
			return nil
		}
	}
	return err
}

func (k *kafka) close() {
	k.producer.Close()
	k.consumer.Close()
}
