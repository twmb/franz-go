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
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"time"
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

func do() error {
	// Set up the tracer
	tracerProvider, err := initTracerProvider()
	if err != nil {
		return err
	}

	tracingOpts := []kotel.TracingOption{
		kotel.TracerProvider(tracerProvider),
		kotel.TracerPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{})),
	}
	tracer := kotel.NewTracer(tracingOpts...)

	// Set up the meter
	meterProvider, err := initMeterProvider()
	if err != nil {
		return err
	}

	metricsOpts := []kotel.MetricsOption{kotel.MeterProvider(meterProvider)}
	meter := kotel.NewMeter(metricsOpts...)

	// Pass tracer/meter to NewKotel hook
	kotelOps := []kotel.Option{
		kotel.WithTracing(tracer),
		kotel.WithMetrics(meter),
	}

	kotelService := kotel.NewKotel(kotelOps...)

	// Set up producer Kafka client
	fmt.Println("Start producer...")

	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),
		kgo.WithHooks(kotelService.Hooks()...), // pass in kotel hook
	}
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		panic(fmt.Sprintf("unable to create client: %v", err))
		return err
	}

	// Optionally create a request tracer
	requestTracer := tracerProvider.Tracer("request-service")
	requestCtx, requestSpan := requestTracer.Start(context.Background(), "request-span")
	requestSpan.SetAttributes(attribute.String("foo", "bar"))

	// Produce a message
	var wg sync.WaitGroup
	wg.Add(1)
	record := &kgo.Record{Key: []byte(fmt.Sprint(rand.Intn(9-0+1) + 0)), Topic: *topic, Value: []byte("bar")}
	// Optionally pass a Context from a span
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

	// Set up consumer Kafka client
	fmt.Println("Start consumer...")

	cl2, err := kgo.NewClient(append(opts, kgo.ConsumeTopics(*topic))...)
	if err != nil {
		panic(fmt.Sprintf("unable to create client: %v", err))
		return err
	}

	// Optionally create a processor tracer
	processTracer := tracerProvider.Tracer("process-service")

	// Consume message
	for {
		fetches := cl2.PollFetches(context.Background())
		if errs := fetches.Errors(); len(errs) > 0 {
			panic(fmt.Sprint(errs))
		}

		iter := fetches.RecordIter()
		for !iter.Done() {
			record := iter.Next()
			processRecord(processTracer, record)
			time.Sleep(time.Duration(rand.Intn(150)+100) * time.Millisecond)
		}
	}
}

func processRecord(t trace.Tracer, r *kgo.Record) {
	fmt.Println("Got record offset: " + strconv.FormatInt(r.Offset, 10))
	// Extract span from record Context
	// This continues the trace from the consumer hook
	_, span := t.Start(r.Context, fmt.Sprintf("%s process", r.Topic))
	defer span.End()
	span.SetAttributes(attribute.String("baz", "qux"))
}

func main() {
	flag.Parse()

	if err := do(); err != nil {
		log.Fatal(err)
	}
}
