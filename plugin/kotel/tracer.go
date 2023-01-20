package kotel

import (
	"context"
	"fmt"

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
	clientID       string
	consumerGroup  string
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

// ClientID sets the optional client_id attribute value
func ClientID(id string) TracerOpt {
	return tracerOptFunc(func(t *Tracer) {
		if t != nil {
			t.clientID = id
		}
	})
}

// ConsumerGroup sets the optional group attribute value
func ConsumerGroup(group string) TracerOpt {
	return tracerOptFunc(func(t *Tracer) {
		if t != nil {
			t.consumerGroup = group
		}
	})
}

// Hooks ---------------------------------------------------------------------

// OnProduceRecordBuffered starts a new span for the "send" operation on a buffered record
//
// It sets the attributes and injects the span context into the record's context
func (t *Tracer) OnProduceRecordBuffered(r *kgo.Record) {
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

	spanContext, _ := t.tracer.Start(r.Context, fmt.Sprintf("%s send", r.Topic), opts...)
	t.propagators.Inject(spanContext, NewRecordCarrier(r))
	r.Context = spanContext
}

// OnProduceRecordUnbuffered continues and ends the "send" span for an unbuffered record
//
// It sets attributes and records any error that occurred during the send operation
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

// OnFetchRecordBuffered starts and ends a new span for the "receive" operation on a buffered record
//
// It sets attributes and injects the span context into the record's context
func (t *Tracer) OnFetchRecordBuffered(r *kgo.Record) {
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
	ctx := t.propagators.Extract(r.Context, NewRecordCarrier(r))
	newCtx, span := t.tracer.Start(ctx, fmt.Sprintf("%s receive", r.Topic), opts...)
	span.End()

	r.Context = newCtx
}
