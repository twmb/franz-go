package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kotel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
	"go.opentelemetry.io/otel/trace"
)

var (
	seedBrokers = flag.String("brokers", "localhost:9092", "comma delimited list of seed brokers")
	topic       = flag.String("topic", "topic", "topic to produce and consume for trace/metric exporting")
)

func initTracerProvider() (*sdktrace.TracerProvider, error) {
	// Create a new trace exporter
	traceExporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		return nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}
	// Create a new batch span processor
	bsp := sdktrace.NewBatchSpanProcessor(traceExporter)
	// Create a new resource with default attributes and additional attributes
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("test-service"),
			semconv.ServiceNamespaceKey.String("test-namespace"),
			semconv.ServiceVersionKey.String("0.0.1"),
			semconv.ServiceInstanceIDKey.String(uuid.New().String()),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create tracer resource: %w", err)
	}
	// Create a new tracer provider with the batch span processor, always
	// sample, and the created resource.
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(bsp),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
	)
	return tp, nil
}

func initMeterProvider() (*metric.MeterProvider, error) {
	// Create a new meter exporter
	exp, err := stdoutmetric.New()
	if err != nil {
		return nil, fmt.Errorf("failed to create meter exporter: %w", err)
	}
	// Create a new resource with default attributes and additional attributes
	res, err := resource.Merge(
		resource.Default(),
		resource.NewWithAttributes(
			semconv.SchemaURL,
			semconv.ServiceNameKey.String("test-service"),
			semconv.ServiceNamespaceKey.String("test-namespace"),
			semconv.ServiceVersionKey.String("0.0.1"),
			semconv.ServiceInstanceIDKey.String(uuid.New().String()),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create meter resource: %w", err)
	}
	// Create a new meter provider with the created exporter and the created
	// resource.
	mp := metric.NewMeterProvider(
		metric.WithReader(metric.NewPeriodicReader(exp)),
		metric.WithResource(res),
	)
	return mp, nil
}

func newKotelTracer(tracerProvider *sdktrace.TracerProvider) *kotel.Tracer {
	// Create a new kotel tracer with the provided tracer provider and
	// propagator.
	tracerOpts := []kotel.TracerOpt{
		kotel.TracerProvider(tracerProvider),
		kotel.TracerPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{})),
	}
	return kotel.NewTracer(tracerOpts...)
}

func newKotelMeter(meterProvider *metric.MeterProvider) *kotel.Meter {
	// Create a new kotel meter using a provided meter provider.
	meterOpts := []kotel.MeterOpt{kotel.MeterProvider(meterProvider)}
	return kotel.NewMeter(meterOpts...)
}

func newKotel(tracer *kotel.Tracer, meter *kotel.Meter) *kotel.Kotel {
	kotelOps := []kotel.Opt{
		kotel.WithTracer(tracer),
		kotel.WithMeter(meter),
	}
	return kotel.NewKotel(kotelOps...)
}

func newProducerClient(kotelService *kotel.Kotel) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),
		kgo.WithHooks(kotelService.Hooks()...),
	}
	return kgo.NewClient(opts...)
}

func produceMessage(client *kgo.Client, tracer trace.Tracer) error {
	// Start a new span with options.
	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes([]attribute.KeyValue{attribute.String("some-key", "foo")}...),
	}
	ctx, span := tracer.Start(context.Background(), "request", opts...)
	// End the span when function exits.
	defer span.End()

	// Simulate some work.
	time.Sleep(1 * time.Second)

	var wg sync.WaitGroup
	wg.Add(1)
	record := &kgo.Record{Topic: *topic, Key: []byte("some-key"), Value: []byte("some-value")}
	// Pass in the context from the tracer.Start() call to ensure that the span
	// created is linked to the parent span.
	client.Produce(ctx, record, func(_ *kgo.Record, err error) {
		defer wg.Done()
		if err != nil {
			fmt.Printf("record had a produce error: %v\n", err)
			// Set the status and record error on the span.
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		}
	})
	wg.Wait()
	return nil
}

func newConsumerClient(kotelService *kotel.Kotel) (*kgo.Client, error) {
	// Create options for the new client.
	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),
		// Add hooks from kotel service.
		kgo.WithHooks(kotelService.Hooks()...),
		kgo.ConsumeTopics(*topic),
	}
	return kgo.NewClient(opts...)
}

func consumeMessages(client *kgo.Client, tracer trace.Tracer) error {
	fetches := client.PollFetches(context.Background())
	if errs := fetches.Errors(); len(errs) > 0 {
		return fmt.Errorf("%v", errs)
	}

	iter := fetches.RecordIter()
	for !iter.Done() {
		record := iter.Next()
		processRecord(record, tracer)
	}
	return nil
}

func processRecord(record *kgo.Record, tracer trace.Tracer) {
	// Create options for the new span.
	opts := []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			semconv.MessagingOperationProcess,
			attribute.String("some-key", "bar"),
			attribute.String("some-other-key", "baz"),
		),
	}
	// Start a new span using the provided context and options.
	_, span := tracer.Start(record.Context, record.Topic+" process", opts...)
	// Simulate some work.
	time.Sleep(1 * time.Second)
	// End the span when function exits.
	defer span.End()
	fmt.Printf(
		"processed offset '%s' with key '%s' and value '%s'\n",
		strconv.FormatInt(record.Offset, 10),
		string(record.Key),
		string(record.Value),
	)
}

func do() error {
	// Initialize tracer provider and handle shutdown.
	tracerProvider, err := initTracerProvider()
	if err != nil {
		return fmt.Errorf("failed to initialize tracer provider: %w", err)
	}
	defer func() {
		if err := tracerProvider.Shutdown(context.Background()); err != nil {
			log.Printf("Error shutting down tracer provider: %v", err)
		}
	}()

	// Initialize meter provider and handle shutdown.
	meterProvider, err := initMeterProvider()
	if err != nil {
		return fmt.Errorf("failed to initialize meter provider: %w", err)
	}
	defer func() {
		if err := meterProvider.Shutdown(context.Background()); err != nil {
			log.Printf("Error shutting down meter provider: %v", err)
		}
	}()

	// Create a new kotel tracer and meter.
	kotelTracer := newKotelTracer(tracerProvider)
	kotelMeter := newKotelMeter(meterProvider)

	// Create a new kotel service.
	kotelService := newKotel(kotelTracer, kotelMeter)

	// Initialize producer client and handle close.
	producerClient, err := newProducerClient(kotelService)
	if err != nil {
		return fmt.Errorf("unable to create producer client: %w", err)
	}
	defer producerClient.Close()

	// Create request tracer and produce message.
	requestTracer := tracerProvider.Tracer("request-service")
	if err := produceMessage(producerClient, requestTracer); err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	// Initialize consumer client and handle close.
	consumerClient, err := newConsumerClient(kotelService)
	if err != nil {
		return fmt.Errorf("unable to create consumer client: %w", err)
	}
	defer consumerClient.Close()

	// Create process tracer and consume messages in a loop.
	processTracer := tracerProvider.Tracer("process-service")
	for {
		if err := consumeMessages(consumerClient, processTracer); err != nil {
			return fmt.Errorf("failed to consume messages: %w", err)
		}
	}
}

func main() {
	flag.Parse()

	if err := do(); err != nil {
		log.Fatal(err)
	}
}
