package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

func startProducing(ctx context.Context, kafkaBrokers string, topic string) {
	producerId := strconv.FormatInt(int64(os.Getpid()), 10)
	client, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(kafkaBrokers, ",")...),
		kgo.TransactionalID(producerId),
	)
	if err != nil {
		fmt.Printf("error initializing Kafka producer client: %v", err)
		return
	}

	defer client.Close()

	batch := 0
	for {
		if err := client.BeginTransaction(); err != nil {
			fmt.Printf("error beginning transaction: %v\n", err)
			break
		}

		// Write some messages in the transaction.
		if err := produceRecords(ctx, client, topic, batch); err != nil {
			fmt.Printf("error producing message: %v\n", err)
			rollback(ctx, client)
			continue
		}

		// Flush all of the buffered messages.
		if err := client.Flush(ctx); err != nil {
			if err != context.Canceled {
				fmt.Printf("error flushing messages: %v\n", err)
			}

			rollback(ctx, client)
			continue
		}

		// Attempt to commit the transaction and explicitly abort if it fails.
		if err := client.EndTransaction(ctx, kgo.TryCommit); err != nil {
			fmt.Printf("error committing transaction: %v\n", err)

			if rollbackErr := client.AbortBufferedRecords(ctx); err != nil {
				fmt.Printf("error rolling back transaction after commit failure: %v\n", rollbackErr)
			}
		}

		batch += 1
		time.Sleep(10 * time.Second)
	}

	fmt.Println("producer exited")
}

func produceRecords(ctx context.Context, client *kgo.Client, topic string, batch int) error {
	errChan := make(chan error)

	// Records are produced sequentially in order to demonstrate that a consumer
	// using the ReadCommitted isolation level will not consume any records until
	// the transaction is committed.
	for i := 0; i < 10; i++ {
		message := fmt.Sprintf("batch %d record %d\n", batch, i)
		r := &kgo.Record{
			Value: []byte(message),
			Topic: topic,
		}

		client.Produce(ctx, r, func(r *kgo.Record, e error) {
			if e != nil {
				fmt.Printf("produced message: %s", message)
			}
			errChan <- e
		})

		if err := <-errChan; err != nil {
			return err
		}
	}

	return nil
}

func rollback(ctx context.Context, client *kgo.Client) {
	if err := client.EndTransaction(ctx, kgo.TryAbort); err != nil {
		fmt.Printf("error rolling back transaction: %v\n", err)
		return
	}
	fmt.Println("transaction rolled back")
}
