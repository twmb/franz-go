kotel
===

kotel is a plug-in package that provides OpenTelemetry instrumentation. It supplies telemetry options
for [tracing](https://pkg.go.dev/go.opentelemetry.io/otel/trace) and
[metrics](https://pkg.go.dev/go.opentelemetry.io/otel/metric) through
a [`kgo.Hook`](https://pkg.go.dev/github.com/twmb/franz-go/pkg/kgo#Hook).

Please visit the [OpenTelemetry documentation](https://opentelemetry.io/docs) for additional information about OpenTelemetry

## tracing

The tracing module tracks `kgo.record` linage through the following hooks:

1) OnProduceRecordBuffered
2) HookProduceRecordUnbuffered
3) HookFetchRecordBuffered

To get started:

```go
tracerProvider, err := initTracerProvider()

tracingOpts := []kotel.TracingOption{
	kotel.TracerProvider(tracerProvider),
	kotel.TracerPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{})),
}
tracer := kotel.NewTracer(tracingOpts...)

kotelOps := []kotel.Option{
	kotel.WithTracing(tracer)
}

kotelService := kotel.NewKotel(kotelOps...)

cl, err := kgo.NewClient(
	kgo.WithHooks(kotelService.Hooks()...),
// ...other opts
```

Producers can enrich records with ancestor trace data:

```go
tracer := tracerProvider.Tracer("request-service")

ctx, span := requestTracer.Start(context.Background(), "request-span")
span.SetAttributes(attribute.String("foo", "bar"))

var wg sync.WaitGroup
wg.Add(1)
record := &kgo.Record{
	Topic: "my-topic",
	Value: []byte("bar")
}

cl.Produce(requestCtx, record, func(_ *kgo.Record, err error) {
	defer wg.Done()
	if err != nil {
		fmt.Printf("record had a produce error: %v\n", err)
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
	}
})
wg.Wait()
span.End()
```

Consumers can extract ancestor trace data from records:

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
		_, span := tracer.Start(r.Context, fmt.Sprintf("%s process", r.Topic))
		span.SetAttributes(attribute.String("baz", "qux"))
		span.End()
	}
}
```

## metrics

The metrics module tracks the following metrics under the following names, all
metrics being counters:

```
connects_total{node_id = "#{node}"}
connect_errors_total{node_id = "#{node}"}
write_errors_total{node_id = "#{node}"}
write_bytes_total{node_id = "#{node}"}
read_errors_total{node_id = "#{node}"}
read_bytes_total{node_id = "#{node}"}
produce_bytes_total{node_id = "#{node}", topic = "#{topic}"}
fetch_bytes_total{node_id = "#{node}", topic = "#{topic}"}
buffered_produce_records_total
buffered_fetch_records_total
```

To get started:

```go
meterProvider, err := initMeterProvider()

metricsOpts := []kotel.MetricsOption{kotel.MeterProvider(meterProvider)}
meter := kotel.NewMeter(metricsOpts...)

// Pass tracer and meter to NewKotel hook
kotelOps := []kotel.Option{
	kotel.WithMetrics(meter)
}

kotelService := kotel.NewKotel(kotelOps...)

cl, err := kgo.NewClient(
	kgo.WithHooks(kotelService.Hooks()...),
	// ...other opts
)
```
## performance

TODO