// This example demonstrates franz-go's logging and metrics plugin ecosystem.
//
// Logging plugins (choose one):
//   - kslog: integrates with log/slog (recommended for new projects)
//   - kzap: integrates with go.uber.org/zap
//   - kgo.BasicLogger: built-in, no dependencies
//
// Metrics plugins (can be used alongside any logger):
//   - kprom: exports Prometheus metrics via an HTTP endpoint
//
// You can use one logger and one metrics plugin together. This example
// registers both a logger and Prometheus metrics to show how they combine.
// Use -logger to select the logging backend.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"time"

	"go.uber.org/zap"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kprom"
	"github.com/twmb/franz-go/plugin/kslog"
	"github.com/twmb/franz-go/plugin/kzap"
)

var (
	seedBrokers = flag.String("brokers", "localhost:9092", "comma delimited list of seed brokers")
	topic       = flag.String("topic", "foo", "topic to consume or produce to")
	produce     = flag.Bool("produce", false, "if true, produce once per second rather than consume")
	logBackend  = flag.String("logger", "slog", "logging backend: slog, zap, basic")
	metricsPort = flag.Int("metrics-port", 9999, "port to serve Prometheus metrics on (0 to disable)")
)

func die(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, msg, args...)
	os.Exit(1)
}

func main() {
	flag.Parse()

	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),
		kgo.DefaultProduceTopic(*topic),
	}

	// Configure a logging plugin. You only need one logger.
	switch *logBackend {
	case "slog":
		sl := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
		opts = append(opts, kgo.WithLogger(kslog.New(sl)))
	case "zap":
		zl, err := zap.NewDevelopment()
		if err != nil {
			die("unable to create zap logger: %v\n", err)
		}
		opts = append(opts, kgo.WithLogger(kzap.New(zl)))
	case "basic":
		opts = append(opts, kgo.WithLogger(kgo.BasicLogger(os.Stderr, kgo.LogLevelInfo, nil)))
	default:
		die("unknown logger %q\n", *logBackend)
	}

	// Configure Prometheus metrics. Metrics hooks can be used alongside
	// any logger - they are independent. Use port 0 to skip metrics.
	if *metricsPort > 0 {
		metrics := kprom.NewMetrics("kgo")
		opts = append(opts, kgo.WithHooks(metrics))
		go func() {
			http.Handle("/metrics", metrics.Handler())
			log.Fatal(http.ListenAndServe(fmt.Sprintf("localhost:%d", *metricsPort), nil))
		}()
		fmt.Fprintf(os.Stderr, "serving metrics on localhost:%d/metrics\n", *metricsPort)
	}

	if !*produce {
		opts = append(opts, kgo.ConsumeTopics(*topic))
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		die("unable to create client: %v\n", err)
	}
	defer cl.Close()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	if *produce {
		for {
			select {
			case <-ctx.Done():
				return
			case <-time.After(time.Second):
			}
			if err := cl.ProduceSync(ctx, kgo.StringRecord("hello")).FirstErr(); err != nil {
				die("unable to produce: %v\n", err)
			}
		}
	} else {
		for {
			fetches := cl.PollFetches(ctx)
			if fetches.IsClientClosed() {
				return
			}
			fetches.EachError(func(t string, p int32, err error) {
				die("fetch err topic %s partition %d: %v\n", t, p, err)
			})
			var seen int
			fetches.EachRecord(func(*kgo.Record) {
				seen++
			})
			fmt.Printf("consumed %d records\n", seen)
		}
	}
}
