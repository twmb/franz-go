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

type kafka struct {
	producer *kgo.Client
	consumer *kgo.Client
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
	//good practice to run app with support of graceful shutdown
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	k := &kafka{}
	var wg sync.WaitGroup

	//separate conns due to different conf
	if err := k.connectProducer(ctx); err != nil {
		log.Fatal("failed to connect producer", err)
	}
	if err := k.connectConsumer(ctx); err != nil {
		log.Fatal("failed to connect consumer", err)
	}

	//populate fake data
	for i := 0; i < 10; i++ {
		k.producer.Produce(ctx, &kgo.Record{
			Topic:     "example",
			Key:       []byte("key" + strconv.Itoa(i)),
			Value:     []byte("value" + strconv.Itoa(i)),
			Timestamp: time.Now(),
		}, nil)
	}

	wg.Add(1)
	go k.run(ctx, &wg)

	//waiting for stop signal
	<-ctx.Done()
	k.close()
	wg.Wait()
	log.Println("ok")
}

func (k *kafka) connectConsumer(ctx context.Context) error {
	var err error
	k.consumer, err = kgo.NewClient([]kgo.Opt{
		kgo.SeedBrokers([]string{"localhost:9092"}...),
		kgo.ConsumerGroup("example"),
		kgo.ConsumeTopics("example"),
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
		kgo.DefaultProduceTopic("example"),
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
	log.Print("start consumer")
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
			fetches.EachPartition(func(p kgo.FetchTopicPartition) {
				for _, rec := range p.Records {
					if err := k.process(rec); err != nil {
						//if error occurs we spawn a goroutine because
						//other records should not wait for the current one
						go func() {
							//before dlq we probably want to add a retry mechanism
							//to make sure that there's no obvious errors
							if err = k.retry(rec); err != nil {
								k.sendToDlq(ctx, rec, err)
							}
						}()
					}
				}
			})
		}
	}
}

func (k *kafka) process(r *kgo.Record) error {
	//simulate load
	time.Sleep(1 * time.Second)
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

func (k *kafka) retry(r *kgo.Record) error {
	var err error
	for i := 1; i <= 3; i++ {
		if err = k.process(r); err != nil {
			//simulate backoff
			time.Sleep(time.Duration(i*2) * time.Second)
		} else {
			return nil
		}
	}
	return err
}

func (k *kafka) sendToDlq(ctx context.Context, r *kgo.Record, err error) {
	failed, _ := json.Marshal(&message{
		topic:     r.Topic,
		key:       r.Key,
		value:     r.Value,
		timestamp: r.Timestamp,
		offset:    r.Offset,
		partition: r.Partition,
	})
	dlq := &kgo.Record{
		Topic:     "dlq",
		Key:       r.Key,
		Value:     failed,
		Timestamp: time.Now(),
		Headers: []kgo.RecordHeader{
			//some headers as review info
			{Key: "status", Value: []byte("review")},
			{Key: "error", Value: []byte(err.Error())},
		},
	}
	k.producer.Produce(ctx, dlq, func(r *kgo.Record, err error) {
		//now dlq consumer should handle these records
		log.Println("failed to produce dlq record", err)
	})
}

func (k *kafka) close() {
	k.producer.Close()
	k.consumer.Close()
}
