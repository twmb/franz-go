package main

import (
	"context"
	"flag"
	"fmt"
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
	"log"
	"strconv"
	"strings"
	"sync"
)

var (
	seedBrokers = flag.String("brokers", "localhost:9092", "comma delimited list of seed brokers")
	topic       = flag.String("topic", "my-topic", "topic to produce and consume for trace/metric exporting")
)

func initTracerProvider() (*sdktrace.TracerProvider, error) {
	traceExporter, err := stdouttrace.New(stdouttrace.WithPrettyPrint())
	if err != nil {
		return nil, fmt.Errorf("failed to create trace exporter: %w", err)
	}

	bsp := sdktrace.NewBatchSpanProcessor(traceExporter)

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

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(bsp),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
	)
	return tp, nil
}

func initMeterProvider() (*metric.MeterProvider, error) {
	exp, err := stdoutmetric.New()
	if err != nil {
		return nil, fmt.Errorf("failed to create meter exporter: %w", err)
	}

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

	mp := metric.NewMeterProvider(
		metric.WithReader(metric.NewPeriodicReader(exp)),
		metric.WithResource(res),
	)
	return mp, nil
}

func newKotelTracer(tracerProvider *sdktrace.TracerProvider) *kotel.Tracer {
	tracingOpts := []kotel.TracingOption{
		kotel.TracerProvider(tracerProvider),
		kotel.TracerPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{})),
	}
	return kotel.NewTracer(tracingOpts...)
}

func newKotelMeter(meterProvider *metric.MeterProvider) *kotel.Meter {
	metricsOpts := []kotel.MetricsOption{kotel.MeterProvider(meterProvider)}
	return kotel.NewMeter(metricsOpts...)
}

func newKotel(tracer *kotel.Tracer, meter *kotel.Meter) *kotel.Kotel {
	kotelOps := []kotel.Option{
		kotel.WithTracing(tracer),
		kotel.WithMetrics(meter),
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

func produceMessage(client *kgo.Client, tracerProvider *sdktrace.TracerProvider) error {
	requestTracer := tracerProvider.Tracer("request-service")
	requestCtx, requestSpan := requestTracer.Start(context.Background(), "request-span")
	requestSpan.SetAttributes(attribute.String("some-key", "foo"))

	var wg sync.WaitGroup
	wg.Add(1)
	record := &kgo.Record{Topic: *topic, Key: []byte("some-key"), Value: []byte("some-value")}
	client.Produce(requestCtx, record, func(_ *kgo.Record, err error) {
		defer wg.Done()
		if err != nil {
			fmt.Printf("record had a produce error: %v\n", err)
			requestSpan.SetStatus(codes.Error, err.Error())
			requestSpan.RecordError(err)
		}
	})
	wg.Wait()
	requestSpan.End()
	return nil
}

func newConsumerClient(kotelService *kotel.Kotel) (*kgo.Client, error) {
	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),
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
		processRecord(tracer, record)
	}
	return nil
}

func processRecord(tracer trace.Tracer, record *kgo.Record) {
	_, processSpan := tracer.Start(record.Context, "process-span")
	defer processSpan.End()

	fmt.Printf(
		"processed offset '%s' with key '%s' and value '%s'\n",
		strconv.FormatInt(record.Offset, 10),
		string(record.Key),
		string(record.Value),
	)
	processSpan.SetAttributes(
		attribute.String("some-key", "bar"),
		attribute.String("some-other-key", "baz"),
	)
}

func do() error {
	tracerProvider, err := initTracerProvider()
	if err != nil {
		return fmt.Errorf("failed to initialize tracer provider: %w", err)
	}

	meterProvider, err := initMeterProvider()
	if err != nil {
		return fmt.Errorf("failed to initialize meter provider: %w", err)
	}

	kotelTracer := newKotelTracer(tracerProvider)
	kotelMeter := newKotelMeter(meterProvider)
	kotelService := newKotel(kotelTracer, kotelMeter)

	producerClient, err := newProducerClient(kotelService)
	if err != nil {
		return fmt.Errorf("unable to create producer client: %w", err)
	}
	defer producerClient.Close()

	if err := produceMessage(producerClient, tracerProvider); err != nil {
		return fmt.Errorf("failed to produce message: %w", err)
	}

	consumerClient, err := newConsumerClient(kotelService)
	if err != nil {
		return fmt.Errorf("unable to create consumer client: %w", err)
	}
	defer consumerClient.Close()

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
