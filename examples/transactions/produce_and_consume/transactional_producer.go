package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

func startProducing(ctx context.Context, brokers []string, topic string) {
	producerId := strconv.FormatInt(int64(os.Getpid()), 10)
	client, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.TransactionalID(producerId),
		kgo.DefaultProduceTopic(topic),
	)
	if err != nil {
		fmt.Printf("error initializing Kafka producer client: %v\n", err)
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
		if err := produceRecords(ctx, client, batch); err != nil {
			fmt.Printf("error producing message: %v\n", err)
			rollback(ctx, client)
			continue
		}

		// Flush all of the buffered messages.
		//
		// Flush only returns an error if the context was canceled, and
		// it is highly not recommended to cancel the context.
		if err := client.Flush(ctx); err != nil {
			fmt.Printf("flush was killed due to context cancelation\n")
			break // nothing to do here, since error means context was canceled
		}

		// Attempt to commit the transaction and explicitly abort if the
		// commit was not attempted.
		switch err := client.EndTransaction(ctx, kgo.TryCommit); err {
		case nil:
		case kerr.OperationNotAttempted:
			rollback(ctx, client)
		default:
			fmt.Printf("error committing transaction: %v\n", err)
		}

		batch += 1
		time.Sleep(10 * time.Second)
	}

	fmt.Println("producer exited")
}

// Records are produced synchronously in order to demonstrate that a consumer
// using the ReadCommitted isolation level will not consume any records until
// the transaction is committed.
func produceRecords(ctx context.Context, client *kgo.Client, batch int) error {
	for i := 0; i < 10; i++ {
		message := fmt.Sprintf("batch %d record %d\n", batch, i)
		if err := client.ProduceSync(ctx, kgo.StringRecord(message)).FirstErr(); err != nil {
			return err
		}
	}
	return nil
}

func rollback(ctx context.Context, client *kgo.Client) {
	if err := client.AbortBufferedRecords(ctx); err != nil {
		fmt.Printf("error aborting buffered records: %v\n", err) // this only happens if ctx is canceled
		return
	}
	if err := client.EndTransaction(ctx, kgo.TryAbort); err != nil {
		fmt.Printf("error rolling back transaction: %v\n", err)
		return
	}
	fmt.Println("transaction rolled back")
}
