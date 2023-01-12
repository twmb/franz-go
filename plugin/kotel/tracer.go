package kotel

import (
	"context"
	"fmt"
	"strconv"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
	"go.opentelemetry.io/otel/trace"
)

var ( // interface checks to ensure we implement the hooks properly
	_ kgo.HookProduceRecordBuffered   = new(Tracer)
	_ kgo.HookProduceRecordUnbuffered = new(Tracer)
	_ kgo.HookFetchRecordBuffered     = new(Tracer)
)

type Tracer struct {
	tracerProvider trace.TracerProvider
	propagators    propagation.TextMapPropagator
	tracer         trace.Tracer
}

// TracerOpt interface used for setting optional config properties.
type TracerOpt interface {
	apply(*Tracer)
}

type tracerOptFunc func(*Tracer)

func (o tracerOptFunc) apply(t *Tracer) {
	o(t)
}

// NewTracer returns a Tracer, used as option for kotel to instrument franz-go with tracing
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
		trace.WithInstrumentationVersion(SemVersion()),
		trace.WithSchemaURL(semconv.SchemaURL),
	)
	return t
}

// TracerProvider takes a trace.TracerProvider and applies it to the Tracer
// If none is specified, the global provider is used.
func TracerProvider(provider trace.TracerProvider) TracerOpt {
	return tracerOptFunc(func(t *Tracer) {
		if t != nil {
			t.tracerProvider = provider
		}
	})
}

// TracerPropagator takes a propagation.TextMapPropagator and applies it to the Tracer
// If none is specified, the global Propagator is used.
func TracerPropagator(propagator propagation.TextMapPropagator) TracerOpt {
	return tracerOptFunc(func(t *Tracer) {
		if t != nil {
			t.propagators = propagator
		}
	})
}

// Hooks ---------------------------------------------------------------------

// OnProduceRecordBuffered Begins the producer span
func (t *Tracer) OnProduceRecordBuffered(r *kgo.Record) {
	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String("kafka"),
		semconv.MessagingDestinationKindTopic,
		semconv.MessagingDestinationKey.String(r.Topic),
		semconv.MessagingKafkaClientIDKey.String(strconv.FormatInt(r.ProducerID, 10)),
		semconv.MessagingOperationKey.String("send"),
	}

	opts := []trace.SpanStartOption{
		trace.WithAttributes(attrs...),
		trace.WithSpanKind(trace.SpanKindProducer),
	}

	spanContext, _ := t.tracer.Start(r.Context, fmt.Sprintf("%s send", r.Topic), opts...)

	t.propagators.Inject(spanContext, NewRecordCarrier(r))

	r.Context = spanContext
}

// OnProduceRecordUnbuffered Completes the producer span
func (t *Tracer) OnProduceRecordUnbuffered(r *kgo.Record, err error) {
	span := trace.SpanFromContext(r.Context)
	defer span.End()

	span.SetAttributes(
		semconv.MessagingMessageIDKey.String(strconv.FormatInt(r.Offset, 10)),
		semconv.MessagingKafkaPartitionKey.Int64(int64(r.Partition)),
	)

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
	}
}

// OnFetchRecordBuffered Starts and completes the consumer spans
func (t *Tracer) OnFetchRecordBuffered(r *kgo.Record) {
	if r.Context == nil {
		r.Context = context.Background()
	}
	ctx := r.Context
	ctx = t.propagators.Extract(ctx, NewRecordCarrier(r))

	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String("kafka"),
		semconv.MessagingDestinationKindTopic,
		semconv.MessagingDestinationKey.String(r.Topic),
		semconv.MessagingOperationReceive,
		semconv.MessagingMessageIDKey.String(strconv.FormatInt(r.Offset, 10)),
		semconv.MessagingKafkaPartitionKey.Int64(int64(r.Partition)),
	}

	var nextSpanContext = ctx
	var nextSpan trace.Span

	// Optionally add the LogAppend span if the record timestamp is set in Kafka
	if r.Attrs.TimestampType() == 1 {
		opts := []trace.SpanStartOption{
			trace.WithTimestamp(r.Timestamp),
			trace.WithAttributes(attrs...),
			trace.WithSpanKind(trace.SpanKindConsumer),
		}

		nextSpanContext, nextSpan = t.tracer.Start(ctx, fmt.Sprintf("%s logappend", r.Topic), opts...)
		nextSpan.End()
	}

	opts := []trace.SpanStartOption{
		trace.WithAttributes(attrs...),
		trace.WithSpanKind(trace.SpanKindConsumer),
	}

	receiveCtx, receiveSpan := t.tracer.Start(nextSpanContext, fmt.Sprintf("%s receive", r.Topic), opts...)
	receiveSpan.End()

	r.Context = receiveCtx
}
