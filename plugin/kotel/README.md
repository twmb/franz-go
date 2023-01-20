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
new client.

From there, you can use the `kotel` tracing and metrics features in your `franz-go` code as needed. For more detailed
instructions and examples, see the usage sections below.

Please visit the  [OpenTelemetry documentation](https://opentelemetry.io/docs) for additional information about
OpenTelemetry and how it can be used in your `franz-go` projects.

## Tracing

`kotel`'s tracing module allows you to track the lineage of `kgo.Record` objects through a series of `franz-go` hooks:

1) HookProduceRecordBuffered
2) HookProduceRecordUnbuffered
3) HookFetchRecordBuffered

To get started with tracing in `kotel`, you'll need to set up a tracer provider and configure any desired tracer
options. You can then create a `kotel` hook and pass it to `kgo.WithHooks` when creating a new client.

Here's an example of how you might do this:

```go
tracerProvider, err := initTracerProvider()

tracerOpts := []kotel.TracerOpt{
	kotel.TracerProvider(tracerProvider),
	kotel.TracerPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{})),
}
tracer := kotel.NewTracer(tracerOpts...)

kotelOps := []kotel.Opt{
	kotel.WithTracer(tracer)
}

kotelService := kotel.NewKotel(kotelOps...)

cl, err := kgo.NewClient(
	kgo.WithHooks(kotelService.Hooks()...),
    // ...other opts
)
```

`kotel` enables you to enrich records with ancestor trace data, such as trace context from a `http.Request` object that
may already contain a parent span from an instrumentation library. This can be useful for tracking the lineage of your
records and providing additional context for tracing and debugging purposes.

To include ancestor trace data in your records, you can use the `ctx` object obtained from `tracer.Start` and pass it to
`cl.Produce` as shown in the example below:

```go
func httpHandler(w http.ResponseWriter, r *http.Request) {
    ctx, span := tracer.Start(r.Context(), "request-span")
    defer span.End()
    span.SetAttributes(attribute.String("foo", "bar"))

    var wg sync.WaitGroup
    wg.Add(1)
    record := &kgo.Record{Topic: "my-topic", Value: []byte("bar")}
    
    cl.Produce(ctx, record, func(_ *kgo.Record, err error) {
    defer wg.Done()
    if err != nil {
        fmt.Printf("record had a produce error: %v\n", err)
        span.SetStatus(codes.Error, err.Error())
        span.RecordError(err)
    }
    })
    wg.Wait()
    // ...
}
```

`kotel` enables consumers to extract ancestor trace data from kafka records and continue the trace in downstream
processing steps. This can be useful for providing additional context and visibility into the processing of records.

To extract and continue trace data in downstream processing, you can pass the kafka record context to the `tracer.Start`
, which returns a new context and span as shown in the example below:

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
		ctx, span := tracer.Start(r.Context, fmt.Sprintf("%s process", r.Topic))
		span.SetAttributes(attribute.String("baz", "qux"))
		span.End()
		// optionally pass the context to the next processing step
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
messaging.kafka.read_bytes.count{node_id = "#{node}", topic = "#{topic}"}
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

// Pass tracer and meter to NewKotel hook
kotelOps := []kotel.Opt{
	kotel.WithMeter(meter)
}

kotelService := kotel.NewKotel(kotelOps...)

cl, err := kgo.NewClient(
	kgo.WithHooks(kotelService.Hooks()...),
	// ...other opts
)
```
