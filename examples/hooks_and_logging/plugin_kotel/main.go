package main

import (
	"context"
	"flag"
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel/codes"
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
			semconv.ServiceNameKey.String("test-service"),
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

	// Set up Kafka client
	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),
		kgo.WithHooks(tracing), // pass in tracing hook
		kgo.ConsumeTopics(*topic),
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		panic(err)
	}

	// Add request span
	requestTracer := tracerProvider.Tracer("request-service")
	requestCtx, requestSpan := requestTracer.Start(context.Background(), "request-span")

	// Produce a message
	var wg sync.WaitGroup
	wg.Add(1)
	record := &kgo.Record{Topic: *topic, Value: []byte("bar")}
	cl.Produce(requestCtx, record, func(_ *kgo.Record, err error) {
		defer wg.Done()
		if err != nil {
			fmt.Printf("record had a produce error: %v\n", err)
			requestSpan.SetStatus(codes.Error, err.Error())
			requestSpan.RecordError(err)
		}
	})
	wg.Wait()

	requestSpan.End()

	// Consume message
	ctx := context.Background()

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
			// TODO: Trace consumer processor span via record.Context()
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
