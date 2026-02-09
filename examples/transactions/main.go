// This example demonstrates Kafka transactions. Run with -mode to select:
//
//   - produce: standalone transactional producer. Records are produced in
//     batches that are atomically committed or aborted. This mode alternates
//     between committing and aborting to demonstrate both paths.
//   - eos: exactly-once consume-transform-produce pipeline. Records are
//     consumed from one topic, transformed, and produced to another topic
//     with consumer offsets committed atomically in the same transaction.
//     This uses NewGroupTransactSession for the EOS session management.
//
// Both modes require -produce-to. The eos mode additionally requires -eos-to.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	seedBrokers = flag.String("brokers", "localhost:9092", "comma delimited list of seed brokers")
	mode        = flag.String("mode", "produce", "transaction mode: produce, eos")
	produceTo   = flag.String("produce-to", "", "topic to produce to (required)")
	eosTo       = flag.String("eos-to", "", "topic to write EOS output to (required for eos mode)")

	group = flag.String("group", "eos-example-group", "group to use for EOS consuming")

	produceTxnID = flag.String("produce-txn-id", "eos-example-input-producer", "transactional ID for the producer")
	consumeTxnID = flag.String("consume-txn-id", "eos-example-eos-consumer", "transactional ID for the EOS consumer/producer")
)

func die(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, msg, args...)
	os.Exit(1)
}

func main() {
	flag.Parse()

	if *produceTo == "" {
		die("-produce-to is required\n")
	}

	switch *mode {
	case "produce":
		inputProducer()
	case "eos":
		if *eosTo == "" {
			die("-eos-to is required for eos mode\n")
		}
		// The EOS pipeline needs input data, so run the input
		// producer concurrently.
		go inputProducer()
		eosConsumer()
	default:
		die("unknown mode %q\n", *mode)
	}
}

// inputProducer demonstrates standalone transactional producing. Each
// iteration produces a batch of 10 records, then commits or aborts the
// transaction (alternating to show both paths).
func inputProducer() {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),
		kgo.DefaultProduceTopic(*produceTo),
		kgo.TransactionalID(*produceTxnID),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, func() string {
			return "[producer] "
		})),
	)
	if err != nil {
		die("unable to create input producer: %v", err)
	}

	ctx := context.Background()

	for doCommit := true; ; doCommit = !doCommit {
		if err := cl.BeginTransaction(); err != nil {
			die("unable to start transaction: %v", err)
		}

		msg := "commit "
		if !doCommit {
			msg = "abort "
		}

		e := kgo.AbortingFirstErrPromise(cl)
		for i := range 10 {
			cl.Produce(ctx, kgo.StringRecord(msg+strconv.Itoa(i)), e.Promise())
		}
		// Always evaluate e.Err() to avoid short-circuit issues
		// (e.g. doCommit && perr==nil would skip Err() if !doCommit).
		perr := e.Err()
		commit := kgo.TransactionEndTry(doCommit && perr == nil)

		switch err := cl.EndTransaction(ctx, commit); err {
		case nil:
			if doCommit {
				fmt.Println("transaction committed")
			} else {
				fmt.Println("transaction aborted")
			}
		case kerr.OperationNotAttempted:
			if err := cl.EndTransaction(ctx, kgo.TryAbort); err != nil {
				die("abort failed: %v", err)
			}
		default:
			die("end transaction failed: %v", err)
		}

		time.Sleep(10 * time.Second)
	}
}

// eosConsumer demonstrates exactly-once consume-transform-produce using
// NewGroupTransactSession. Records are consumed from -produce-to,
// transformed, and produced to -eos-to with consumer offsets committed
// atomically in the same transaction.
func eosConsumer() {
	sess, err := kgo.NewGroupTransactSession(
		kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),
		kgo.DefaultProduceTopic(*eosTo),
		kgo.TransactionalID(*consumeTxnID),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, func() string {
			return "[eos consumer] "
		})),
		kgo.ConsumerGroup(*group),
		kgo.ConsumeTopics(*produceTo),
		kgo.RequireStableFetchOffsets(),
	)
	if err != nil {
		die("unable to create eos consumer/producer: %v", err)
	}
	defer sess.Close()

	ctx := context.Background()

	for {
		fetches := sess.PollFetches(ctx)

		if fetchErrs := fetches.Errors(); len(fetchErrs) > 0 {
			for _, fetchErr := range fetchErrs {
				fmt.Printf("error consuming from topic: topic=%s, partition=%d, err=%v\n",
					fetchErr.Topic, fetchErr.Partition, fetchErr.Err)
			}
			// The errors may be fatal for the partition (auth
			// problems), but we can still process any records if
			// there are any.
		}

		if err := sess.Begin(); err != nil {
			die("unable to start transaction: %v", err)
		}

		e := kgo.AbortingFirstErrPromise(sess.Client())
		fetches.EachRecord(func(r *kgo.Record) {
			sess.Produce(ctx, kgo.StringRecord("eos "+string(r.Value)), e.Promise())
		})
		committed, err := sess.End(ctx, e.Err() == nil)

		if committed {
			fmt.Println("eos commit successful!")
		} else {
			// A failed End always means an error occurred, because
			// End retries as appropriate.
			die("unable to eos commit: %v", err)
		}
	}
}
