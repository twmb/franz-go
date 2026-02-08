// This example demonstrates manual flushing, where records are buffered and
// only sent to Kafka when Flush is explicitly called. This is useful for
// batch-oriented producers that want precise control over when data is sent.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	seedBrokers = flag.String("brokers", "localhost:9092", "comma delimited list of seed brokers")
	topic       = flag.String("topic", "manual-flush-example", "topic to produce to")
	batchSize   = flag.Int("batch-size", 100, "number of records per flush")
)

func die(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, msg, args...)
	os.Exit(1)
}

func main() {
	flag.Parse()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),
		kgo.DefaultProduceTopic(*topic),
		kgo.AllowAutoTopicCreation(),

		// ManualFlushing disables automatic flushing. Records are
		// buffered until Flush is called. If MaxBufferedRecords is
		// reached before a Flush, Produce returns ErrMaxBuffered.
		kgo.ManualFlushing(),
		kgo.MaxBufferedRecords(*batchSize*2),
	)
	if err != nil {
		die("unable to create client: %v\n", err)
	}
	defer cl.Close()

	ctx := context.Background()

	for batch := 0; ; batch++ {
		// Buffer a batch of records without sending them.
		for i := range *batchSize {
			r := kgo.StringRecord(fmt.Sprintf("batch=%d record=%d", batch, i))
			cl.TryProduce(ctx, r, func(r *kgo.Record, err error) {
				if err != nil {
					fmt.Printf("record delivery error: %v\n", err)
				}
			})
		}
		fmt.Printf("buffered %d records (batch %d), flushing...\n", cl.BufferedProduceRecords(), batch)

		// Flush sends all buffered records and waits for completion.
		if err := cl.Flush(ctx); err != nil {
			die("flush failed: %v\n", err)
		}
		fmt.Printf("batch %d flushed successfully\n", batch)

		time.Sleep(5 * time.Second)
	}
}
