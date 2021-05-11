package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	seedBrokers = flag.String("brokers", "localhost:9092", "comma delimited list of seed brokers")
	produceTo   = flag.String("produce-to", "", "input topic to produce transactionally produce to")
	eosTo       = flag.String("eos-to", "", "consume from produce-to, modify, and write to eos-to")

	group = flag.String("group", "eos-example-group", "group to use for EOS consuming")

	produceTxnID = flag.String("produce-txn-id", "eos-example-input-producer", "transactional ID to use for the input producer")
	consumeTxnID = flag.String("consume-txn-id", "eos-example-eos-consumer", "transactional ID to use for the EOS consumer/producer")
)

func die(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg, args...)
	os.Exit(1)
}

func main() {
	flag.Parse()

	if *produceTo == "" || *eosTo == "" {
		die("missing either -produce-to (%s) or -eos-to (%s)", *produceTo, *eosTo)
	}

	go inputProducer()
	go eosConsumer()
}

func inputProducer() {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),
		kgo.ProduceTopic(*produceTo),
		kgo.TransactionalID(*produceTxnID),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, func() string {
			return "[input producer] "
		})),
	)
	if err != nil {
		die("unable to create input producer: %v", err)
	}

	ctx := context.Background()

	for flip := true; ; flip = !flip {
		if err := cl.BeginTransaction(); err != nil {
			// We are unable to start a transaction if the client
			// is not transactional or if we are already in a
			// transaction. A proper transactional loop will never
			// account either error.
			die("unable to start transaction: %v", err)
		}

		var haveFirstErr int64
		var firstErr error

		for i := 0; i < 10; i++ {
			cl.Produce(ctx, kgo.StringRecord(strconv.Itoa(i)), func(_ *kgo.Record, err error) {
				if err != nil && atomic.SwapInt64(&haveFirstErr, 1) == 0 {
					firstErr = err
				}
			})
		}

		if err := cl.Flush(ctx); err != nil {
			die("Flush only returns error if the context is canceled: %v", err)
		}

		switch err := cl.EndTransaction(ctx, firstErr == nil); err {
		case nil:
		case kerr.OperationNotAttempted:
			if err := cl.EndTransaction(ctx, kgo.TryAbort); err != nil {
				die("abort failed: %v", err)
			}
		default:
			die("commit failed: %v", err)
		}
	}
}

func eosConsumer() {
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),
		kgo.ProduceTopic(*produceTo),
		kgo.TransactionalID(*consumeTxnID),
		kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, func() string {
			return "[eos consumer] "
		})),
	)
	if err != nil {
		die("unable to create eos consumer/producer: %v", err)
	}
	defer cl.Close()

	ctx := context.Background()

	sess := cl.AssignGroupTransactSession(*group,
		kgo.GroupTopics(*produceTo),
		kgo.RequireStableFetchOffsets(),
	)
	defer cl.LeaveGroup()

	for {
		fetches := cl.PollFetches(ctx)

		if fetchErrs := fetches.Errors(); len(fetchErrs) > 0 {
			for _, fetchErr := range fetchErrs {
				fmt.Printf("error consuming from topic: topic=%s, partition=%d, err=%v",
					fetchErr.Topic, fetchErr.Partition, fetchErr.Err)
			}

			// The errors may be fatal for the partition (auth
			// problems), but we can still process any records if
			// there are any.
		}

		if err := sess.Begin(); err != nil {
			// Similar to above, we only encounter errors here if
			// we are not transactional or are already in a
			// transaction. We should not hit this error.
			die("unable to start transaction: %v", err)
		}

		var e kgo.FirstErrPromise
		fetches.EachRecord(func(r *kgo.Record) {
			sess.Produce(ctx, kgo.StringRecord("eos "+string(r.Value)), e.Promise)
		})
		if err := cl.Flush(ctx); err != nil {
			die("Flush only returns error if the context is canceled: %v", err)
		}

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
