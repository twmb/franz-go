package kotel

import (
	"fmt"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
	"go.opentelemetry.io/otel/trace"
	"strconv"
)

const libraryName = "github.com/twmb/franz-go/tree/master/plugin/kotel"

// Kotel Requires a TracerProvider from the client
type Kotel struct {
	TracerProvider *sdktrace.TracerProvider
}

func NewKotel(tracerProvider *sdktrace.TracerProvider) (*Kotel, error) {
	return &Kotel{
		TracerProvider: tracerProvider,
	}, nil
}

func (k *Kotel) OnProduceRecordBuffered(r *kgo.Record) {
	println("OnProduceRecordBuffered reached")
	tracer := k.TracerProvider.Tracer(libraryName)

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

	spanContext, span := tracer.Start(r.Context(), fmt.Sprintf("%s send", r.Topic), opts...)
	span.AddEvent("OnProduceRecordBuffered")

	propagator := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{})
	propagator.Inject(spanContext, NewRecordCarrier(r))

	r.SetContext(spanContext)
}

func (k *Kotel) OnProduceRecordUnbuffered(r *kgo.Record, err error) {
	println("OnProduceRecordUnbuffered reached")
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
	println("OnFetchRecordBuffered reached")
	propagator := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{})
	ctx := r.Context()
	ctx = propagator.Extract(ctx, NewRecordCarrier(r))

	tracer := k.TracerProvider.Tracer(libraryName)

	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String("kafka"),
		semconv.MessagingDestinationKindTopic,
		semconv.MessagingDestinationKey.String(r.Topic),
		semconv.MessagingOperationReceive,
		semconv.MessagingMessageIDKey.String(strconv.FormatInt(r.Offset, 10)),
		semconv.MessagingKafkaPartitionKey.Int64(int64(r.Partition)),
	}

	opts := []trace.SpanStartOption{
		trace.WithTimestamp(r.Timestamp),
		trace.WithAttributes(attrs...),
		trace.WithSpanKind(trace.SpanKindConsumer),
	}

	logappendContext, logAppendSpan := tracer.Start(ctx, "logappend", opts...)
	logAppendSpan.AddEvent("logappend")
	logAppendSpan.End()

	opts = []trace.SpanStartOption{
		trace.WithAttributes(attrs...),
		trace.WithSpanKind(trace.SpanKindConsumer),
	}

	receiveCtx, receiveSpan := tracer.Start(logappendContext, fmt.Sprintf("%s receive", r.Topic), opts...)
	receiveSpan.AddEvent("receive")
	receiveSpan.End()

	r.SetContext(receiveCtx)
}
