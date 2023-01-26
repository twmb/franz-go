package kotel

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
	"go.opentelemetry.io/otel/trace"
)

var ( // interface checks to ensure we implement the hooks properly.
	_ kgo.HookProduceRecordBuffered   = new(Tracer)
	_ kgo.HookProduceRecordUnbuffered = new(Tracer)
	_ kgo.HookFetchRecordBuffered     = new(Tracer)
	_ kgo.HookFetchRecordUnbuffered   = new(Tracer)
)

type Tracer struct {
	tracerProvider trace.TracerProvider
	propagators    propagation.TextMapPropagator
	tracer         trace.Tracer
	clientID       string
	consumerGroup  string
}

// TracerOpt interface used for setting optional config properties.
type TracerOpt interface{ apply(*Tracer) }

type tracerOptFunc func(*Tracer)

// TracerProvider takes a trace.TracerProvider and applies it to the Tracer.
// If none is specified, the global provider is used.
func TracerProvider(provider trace.TracerProvider) TracerOpt {
	return tracerOptFunc(func(t *Tracer) { t.tracerProvider = provider })
}

func (o tracerOptFunc) apply(t *Tracer) { o(t) }

// NewTracer returns a Tracer, used as option for kotel to instrument franz-go
// with tracing.
func NewTracer(opts ...TracerOpt) *Tracer {
	t := &Tracer{}
	for _, opt := range opts {
		opt.apply(t)
	}
	if t.tracerProvider == nil {
		t.tracerProvider = otel.GetTracerProvider()
	}
	if t.propagators == nil {
		t.propagators = otel.GetTextMapPropagator()
	}
	t.tracer = t.tracerProvider.Tracer(
		instrumentationName,
		trace.WithInstrumentationVersion(semVersion()),
		trace.WithSchemaURL(semconv.SchemaURL),
	)
	return t
}

// TracerPropagator takes a propagation.TextMapPropagator and applies it to the
// Tracer.
//
// If none is specified, the global Propagator is used.
func TracerPropagator(propagator propagation.TextMapPropagator) TracerOpt {
	return tracerOptFunc(func(t *Tracer) { t.propagators = propagator })
}

// ClientID sets the optional client_id attribute value.
func ClientID(id string) TracerOpt {
	return tracerOptFunc(func(t *Tracer) { t.clientID = id })
}

// ConsumerGroup sets the optional group attribute value.
func ConsumerGroup(group string) TracerOpt {
	return tracerOptFunc(func(t *Tracer) { t.consumerGroup = group })
}

// WithProcessSpan starts a new span for the "process" operation on a consumer
// record.
//
// It sets up the span options. The user's application code is responsible for
// ending the span.
func (t *Tracer) WithProcessSpan(r *kgo.Record) (context.Context, trace.Span) {
	// Set up the span options.
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String("kafka"),
		semconv.MessagingDestinationKindTopic,
		semconv.MessagingDestinationKey.String(r.Topic),
		semconv.MessagingOperationProcess,
		semconv.MessagingKafkaPartitionKey.Int64(int64(r.Partition)),
	}
	if r.Key != nil {
		attrs = append(attrs, semconv.MessagingKafkaMessageKeyKey.String(string(r.Key)))
	}
	if t.clientID != "" {
		attrs = append(attrs, semconv.MessagingKafkaClientIDKey.String(t.clientID))
	}
	if t.consumerGroup != "" {
		attrs = append(attrs, semconv.MessagingKafkaConsumerGroupKey.String(t.consumerGroup))
	}
	opts := []trace.SpanStartOption{
		trace.WithAttributes(attrs...),
		trace.WithSpanKind(trace.SpanKindConsumer),
	}

	if r.Context == nil {
		r.Context = context.Background()
	}
	// Start a new span using the provided context and options.
	return t.tracer.Start(r.Context, r.Topic+" process", opts...)
}

// Hooks ----------------------------------------------------------------------

// OnProduceRecordBuffered starts a new span for the "send" operation on a
// buffered record.
//
// It sets span options and injects the span context into record and updates
// the record's context, so it can be ended in the OnProduceRecordUnbuffered
// hook.
func (t *Tracer) OnProduceRecordBuffered(r *kgo.Record) {
	// Set up span options.
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String("kafka"),
		semconv.MessagingDestinationKindTopic,
		semconv.MessagingDestinationKey.String(r.Topic),
	}
	if r.Key != nil {
		attrs = append(attrs, semconv.MessagingKafkaMessageKeyKey.String(string(r.Key)))
	}
	if t.clientID != "" {
		attrs = append(attrs, semconv.MessagingKafkaClientIDKey.String(t.clientID))
	}
	opts := []trace.SpanStartOption{
		trace.WithAttributes(attrs...),
		trace.WithSpanKind(trace.SpanKindProducer),
	}
	// Start the "send" span.
	ctx, _ := t.tracer.Start(r.Context, r.Topic+" send", opts...)
	// Inject the span context into the record.
	t.propagators.Inject(ctx, NewRecordCarrier(r))
	// Update the record context.
	r.Context = ctx
}

// OnProduceRecordUnbuffered continues and ends the "send" span for an
// unbuffered record.
//
// It sets attributes with values unset when producing and records any error
// that occurred during the send operation.
func (t *Tracer) OnProduceRecordUnbuffered(r *kgo.Record, err error) {
	span := trace.SpanFromContext(r.Context)
	defer span.End()
	span.SetAttributes(
		semconv.MessagingKafkaPartitionKey.Int64(int64(r.Partition)),
	)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
	}
}

// OnFetchRecordBuffered starts a new span for the "receive" operation on a
// buffered record.
//
// It sets the span options and extracts the span context from the record,
// updates the record's context to ensure it can be ended in the
// OnFetchRecordUnbuffered hook and can be used in downstream consumer
// processing.
func (t *Tracer) OnFetchRecordBuffered(r *kgo.Record) {
	// Set up the span options.
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String("kafka"),
		semconv.MessagingDestinationKindTopic,
		semconv.MessagingDestinationKey.String(r.Topic),
		semconv.MessagingOperationReceive,
		semconv.MessagingKafkaPartitionKey.Int64(int64(r.Partition)),
	}
	if r.Key != nil {
		attrs = append(attrs, semconv.MessagingKafkaMessageKeyKey.String(string(r.Key)))
	}
	if t.clientID != "" {
		attrs = append(attrs, semconv.MessagingKafkaClientIDKey.String(t.clientID))
	}
	if t.consumerGroup != "" {
		attrs = append(attrs, semconv.MessagingKafkaConsumerGroupKey.String(t.consumerGroup))
	}
	opts := []trace.SpanStartOption{
		trace.WithAttributes(attrs...),
		trace.WithSpanKind(trace.SpanKindConsumer),
	}

	if r.Context == nil {
		r.Context = context.Background()
	}
	// Extract the span context from the record.
	ctx := t.propagators.Extract(r.Context, NewRecordCarrier(r))
	// Start the "receive" span.
	newCtx, _ := t.tracer.Start(ctx, r.Topic+" receive", opts...)
	// Update the record context.
	r.Context = newCtx
}

// OnFetchRecordUnbuffered continues and ends the "receive" span for an
// unbuffered record.
func (t *Tracer) OnFetchRecordUnbuffered(r *kgo.Record, _ bool) {
	span := trace.SpanFromContext(r.Context)
	defer span.End()
}
