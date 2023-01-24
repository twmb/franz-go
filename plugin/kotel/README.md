kotel
===

`kotel` is a plug-in package for franz-go that provides OpenTelemetry instrumentation. It offers telemetry options
for [tracing](https://pkg.go.dev/go.opentelemetry.io/otel/trace)
and [metrics](https://pkg.go.dev/go.opentelemetry.io/otel/metric) through
a [`kgo.Hook`](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Hook) interface. `kotel` can be used to enrich
records with ancestor trace data for producers, or extract ancestor trace data from records for consumers. In
addition, `kotel` tracks a variety of metrics related to connections, errors, and bytes transferred.

To get started with `kotel`, you will need to set up a tracer and/or meter provider and configure the desired tracer
and/or meter options. You can then create a `kotel` hook and pass it to the `kgo.WithHooks` options when creating a
new Kafka client.

From there, you can use the `kotel` tracing and metrics features in your `franz-go` code as needed. For more detailed
instructions and examples, see the usage sections below.

Please visit the  [OpenTelemetry documentation](https://opentelemetry.io/docs) for additional information about
OpenTelemetry and how it can be used in your `franz-go` projects.

## Tracing

`kotel`'s tracing module allows you to track the lineage of `kgo.Record` objects through a series of `franz-go` hooks:

1) HookProduceRecordBuffered
2) HookProduceRecordUnbuffered
3) HookFetchRecordBuffered
4) HookFetchRecordUnbuffered

To get started with tracing in `kotel`, you'll need to set up a tracer provider and configure any desired tracer
options. You can then create a `kotel` hook and pass it to `kgo.WithHooks` when creating a new client.

Here's an example of how you might do this:

```go
// Initialize tracer provider
tracerProvider, err := initTracerProvider()

// Create a new kotel tracer
tracerOpts := []kotel.TracerOpt{
	kotel.TracerProvider(tracerProvider),
	kotel.TracerPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{})),
}
tracer := kotel.NewTracer(tracerOpts...)

// Create a new kotel service
kotelOps := []kotel.Opt{
	kotel.WithTracer(tracer),
}
kotelService := kotel.NewKotel(kotelOps...)

// Create a new Kafka client
cl, err := kgo.NewClient(
	// Pass in the kotel hook
	kgo.WithHooks(kotelService.Hooks()...),
	// ...other opts
)
```

To include ancestor trace data in your records, you can use the `ctx` object obtained from `tracer.Start` and pass it
to `cl.Produce` as shown in the example below:

```go
func httpHandler(w http.ResponseWriter, r *http.Request) {
	// Start a new span with options.
	opts := []trace.SpanStartOption{
		trace.WithSpanKind(trace.SpanKindServer),
		trace.WithAttributes([]attribute.KeyValue{attribute.String("some-key", "foo")}...),
	}
	ctx, span := tracer.Start(r.Context(), "request", opts...)
	// End the span when function exits.
	defer span.End()

	var wg sync.WaitGroup
	wg.Add(1)
	record := &kgo.Record{Topic: "topic", Value: []byte("foo")}
	// Pass in the context from the tracer.Start() call to ensure that the span
	// created is linked to the parent span.
	cl.Produce(ctx, record, func(_ *kgo.Record, err error) {
		defer wg.Done()
		if err != nil {
			fmt.Printf("record had a produce error: %v\n", err)
			span.SetStatus(codes.Error, err.Error())
			span.RecordError(err)
		}
	})
	wg.Wait()
}
```

To extract and continue trace data in downstream processing, you can pass the Kafka record context to
the `tracer.Start`, which returns a new context and span as shown in the example below:

```go
tracer := tracerProvider.Tracer("process-service")

for {
	fetches := cl.PollFetches(context.Background())
	if errs := fetches.Errors(); len(errs) > 0 {
		panic(fmt.Sprint(errs))
	}

	iter := fetches.RecordIter()
	for !iter.Done() {
		record := iter.Next()
		// Create options for the new span.
		opts := []trace.SpanStartOption{trace.WithSpanKind(trace.SpanKindConsumer),
			trace.WithAttributes(
				semconv.MessagingOperationProcess,
				attribute.String("some-key", "bar"),
				attribute.String("some-other-key", "baz"),
			),
		}
		// Start a new span using the provided context and options.
		ctx, span := tracer.Start(record.Context, record.Topic+" process", opts...)
		// process record here.
		span.End()
		// optionally pass the context to the next processing step.
	}
}
```

## Metrics

The meter module of `kotel` tracks various metrics related to the processing of records, such as the number of
successful and unsuccessful connections, bytes written and read, and the number of buffered records. These metrics are
all counters and are tracked under the following names:

```
messaging.kafka.connects.count{node_id = "#{node}"}
messaging.kafka.connect_errors.count{node_id = "#{node}"}
messaging.kafka.disconnects.count{node_id = "#{node}"}
messaging.kafka.write_errors.count{node_id = "#{node}"}
messaging.kafka.write_bytes{node_id = "#{node}"}
messaging.kafka.read_errors.count{node_id = "#{node}"}
messaging.kafka.read_bytes.count{node_id = "#{node}"}
messaging.kafka.produce_bytes.count{node_id = "#{node}", topic = "#{topic}"}
messaging.kafka.fetch_bytes.count{node_id = "#{node}", topic = "#{topic}"}
```

To get started with metrics in `kotel`, you'll need to set up a meter provider and configure any desired meter
options. You can then create a `kotel` hook and pass it to `kgo.WithHooks` when creating a new client.

Here's an example of how you might do this:

```go
meterProvider, err := initMeterProvider()

meterOpts := []kotel.MeterOpt{kotel.MeterProvider(meterProvider)}
meter := kotel.NewMeter(meterOpts...)

// Pass tracer and meter to NewKotel hook.
kotelOps := []kotel.Opt{
	kotel.WithMeter(meter),
}
kotelService := kotel.NewKotel(kotelOps...)

cl, err := kgo.NewClient(
	kgo.WithHooks(kotelService.Hooks()...),
	// ...other opts.
)
```
