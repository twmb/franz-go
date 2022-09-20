package kotel

import (
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
	"go.opentelemetry.io/otel/trace"
	"strconv"
)

const (
	instrumentationName = "github.com/twmb/franz-go/tree/master/plugin/kotel"
)

// Kotel represents the configuration options available for the kotel plugin
type Kotel struct {
	Propagators    propagation.TextMapPropagator
	TracerProvider trace.TracerProvider
	tracer         trace.Tracer
}

// Option interface used for setting optional config properties.
type Option interface {
	apply(*Kotel)
}

type optionFunc func(*Kotel)

func (o optionFunc) apply(c *Kotel) {
	o(c)
}

// NewKotel creates a new Kotel struct and applies opts to it.
func NewKotel(opts ...Option) *Kotel {
	c := &Kotel{
		Propagators: otel.GetTextMapPropagator(),
	}
	for _, opt := range opts {
		opt.apply(c)
	}

	c.tracer = c.TracerProvider.Tracer(instrumentationName)

	return c
}

// WithTracerProvider specifies a tracer provider to use for creating a tracer.
// If none is specified, the global provider is used.
func WithTracerProvider(provider trace.TracerProvider) Option {
	return optionFunc(func(cfg *Kotel) {
		if provider != nil {
			cfg.TracerProvider = provider
		}
	})
}

// WithPropagators configures specific propagators. If this
// option isn't specified, then the global TextMapPropagator is used.
func WithPropagators(ps propagation.TextMapPropagator) Option {
	return optionFunc(func(c *Kotel) {
		if ps != nil {
			c.Propagators = ps
		}
	})
}

// franz-go hooks

func (k *Kotel) OnProduceRecordBuffered(r *kgo.Record) {
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

	spanContext, span := k.tracer.Start(r.Context(), fmt.Sprintf("%s send", r.Topic), opts...)
	span.AddEvent("OnProduceRecordBuffered")

	k.Propagators.Inject(spanContext, NewRecordCarrier(r))

	r.SetContext(spanContext)
}

func (k *Kotel) OnProduceRecordUnbuffered(r *kgo.Record, err error) {
	span := trace.SpanFromContext(r.Context())
	span.AddEvent("OnProduceRecordUnbuffered")

	span.SetAttributes(
		semconv.MessagingMessageIDKey.String(strconv.FormatInt(r.Offset, 10)),
		semconv.MessagingKafkaPartitionKey.Int64(int64(r.Partition)),
	)

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
	}

	span.End()
}

func (k *Kotel) OnFetchRecordBuffered(r *kgo.Record) {
	ctx := r.Context()
	ctx = k.Propagators.Extract(ctx, NewRecordCarrier(r))

	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String("kafka"),
		semconv.MessagingDestinationKindTopic,
		semconv.MessagingDestinationKey.String(r.Topic),
		semconv.MessagingOperationReceive,
		semconv.MessagingMessageIDKey.String(strconv.FormatInt(r.Offset, 10)),
		semconv.MessagingKafkaPartitionKey.Int64(int64(r.Partition)),
	}

	// logAppend is an optional parent span
	var logAppendContext = ctx
	var logAppendSpan trace.Span

	// is timestamp set in kafka? (aka log append)
	if r.Attrs.TimestampType() == 1 {
		opts := []trace.SpanStartOption{
			trace.WithTimestamp(r.Timestamp),
			trace.WithAttributes(attrs...),
			trace.WithSpanKind(trace.SpanKindConsumer),
		}

		logAppendContext, logAppendSpan = k.tracer.Start(ctx, fmt.Sprintf("%s logappend", r.Topic), opts...)
		logAppendSpan.AddEvent("logappend")
		logAppendSpan.End()
	}

	opts := []trace.SpanStartOption{
		trace.WithAttributes(attrs...),
		trace.WithSpanKind(trace.SpanKindConsumer),
	}

	receiveCtx, receiveSpan := k.tracer.Start(logAppendContext, fmt.Sprintf("%s receive", r.Topic), opts...)
	receiveSpan.AddEvent("receive")
	receiveSpan.End()

	r.SetContext(receiveCtx)
}
