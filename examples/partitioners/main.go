// This example demonstrates the various partitioning strategies available in
// franz-go. Run with -strategy to select a partitioner.
//
// Available strategies:
//
//   - sticky (default): pins to a partition until a new batch, then random
//   - sticky-key: like sticky, but hashes keys for consistent partitioning
//   - round-robin: distributes records evenly across all partitions
//   - least-backup: picks the partition with the fewest buffered records
//   - manual: uses the Partition field set on the Record
//   - uniform-bytes: pins until a byte threshold, then switches (KIP-794)
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	seedBrokers = flag.String("brokers", "localhost:9092", "comma delimited list of seed brokers")
	topic       = flag.String("topic", "partitioner-example", "topic to produce to")
	strategy    = flag.String("strategy", "sticky", "partitioning strategy: sticky, sticky-key, round-robin, least-backup, manual, uniform-bytes")
	numRecords  = flag.Int("records", 20, "number of records to produce")
)

func die(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, msg, args...)
	os.Exit(1)
}

func main() {
	flag.Parse()

	var partitioner kgo.Partitioner
	switch *strategy {
	case "sticky":
		// StickyPartitioner pins to a random partition until a new
		// batch is created, then picks a new random partition. This
		// creates larger batches and higher throughput.
		partitioner = kgo.StickyPartitioner()

	case "sticky-key":
		// StickyKeyPartitioner hashes keys to ensure records with
		// the same key always go to the same partition. Records
		// without keys use sticky random partitioning.
		// Pass nil for the default murmur2 hasher (matching Java).
		partitioner = kgo.StickyKeyPartitioner(nil)

	case "round-robin":
		// RoundRobinPartitioner cycles through partitions in order.
		// This gives the most even distribution but lower throughput
		// due to smaller batches.
		partitioner = kgo.RoundRobinPartitioner()

	case "least-backup":
		// LeastBackupPartitioner picks the partition with the fewest
		// buffered records. This is resilient to broker issues - if
		// one broker is slow, records flow to healthy partitions.
		partitioner = kgo.LeastBackupPartitioner()

	case "manual":
		// ManualPartitioner uses the Partition field already set on
		// the record. Records with invalid partitions fail immediately.
		partitioner = kgo.ManualPartitioner()

	case "uniform-bytes":
		// UniformBytesPartitioner (KIP-794) pins to a partition until
		// a byte threshold is hit, then re-partitions. This creates
		// uniformly-sized batches. With adaptive=true, it picks less
		// backed-up partitions. With keys=true, records with non-nil
		// keys are hash-partitioned.
		partitioner = kgo.UniformBytesPartitioner(
			16384, // 16KB threshold
			true,  // adaptive: prefer less backed-up brokers
			true,  // keys: hash records with keys
			nil,   // nil = default murmur2 hasher
		)

	default:
		die("unknown strategy %q\n", *strategy)
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),
		kgo.DefaultProduceTopic(*topic),
		kgo.AllowAutoTopicCreation(),
		kgo.RecordPartitioner(partitioner),
	)
	if err != nil {
		die("unable to create client: %v\n", err)
	}
	defer cl.Close()

	ctx := context.Background()

	for i := range *numRecords {
		r := &kgo.Record{
			Key:   []byte(fmt.Sprintf("key-%d", i%5)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
		}
		// For manual partitioner, set the partition explicitly.
		if *strategy == "manual" {
			r.Partition = int32(i % 3)
		}

		cl.Produce(ctx, r, func(r *kgo.Record, err error) {
			if err != nil {
				fmt.Printf("produce error: %v\n", err)
				return
			}
			fmt.Printf("produced to partition %d (key=%s)\n", r.Partition, string(r.Key))
		})
	}
	if err := cl.Flush(ctx); err != nil {
		die("flush error: %v\n", err)
	}
}
