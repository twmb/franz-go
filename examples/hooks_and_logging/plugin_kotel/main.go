package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
	"log"
	"strings"
	"sync"
)

var (
	seedBrokers = flag.String("brokers", "localhost:9092", "comma delimited list of seed brokers")
	topic       = flag.String("topic", "foo", "topic to consume for trace exporting")
)

func initProvider() (*sdktrace.TracerProvider, error) {
	exp, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		return nil, fmt.Errorf("failed to initialize stdouttrace exporter: %w", err)
	}
	bsp := sdktrace.NewBatchSpanProcessor(exp)

	r, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("kafka-service"),
		),
	)

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithSpanProcessor(bsp),
		sdktrace.WithResource(r),
	)
	return tp, nil
}

func do() error {
	// Set up the tracer provider
	tracerProvider, err := initProvider()
	if err != nil {
		return err
	}

	// Pass tracer provider to NewKotel hook
	tracing, err := kotel.NewKotel(tracerProvider)
	if err != nil {
		return err
	}

	ctx := context.Background()

	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),
		kgo.WithHooks(tracing),
		kgo.ConsumeTopics(*topic),
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		panic(err)
	}

	// Produce a message
	var wg sync.WaitGroup
	wg.Add(1)
	// Add context to record context.
	record := &kgo.Record{Ctx: context.Background(), Topic: *topic, Value: []byte("bar")}
	cl.Produce(ctx, record, func(_ *kgo.Record, err error) {
		defer wg.Done()
		if err != nil {
			fmt.Printf("record had a produce error: %v\n", err)
		}

	})
	wg.Wait()

	// Consume message
	for {
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			// All errors are retried internally when fetching, but non-retriable errors are
			// returned from polls so that users can notice and take action.
			panic(fmt.Sprint(errs))
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			fmt.Println(string(record.Value), "from an iterator!")
		}
	}
}

func main() {
	flag.Parse()

	if err := do(); err != nil {
		log.Fatal(err)
	}
}
