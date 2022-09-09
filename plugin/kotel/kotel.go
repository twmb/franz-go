package kotel

import (
	"context"
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

	spanContext, _ := tracer.Start(r.Ctx, fmt.Sprintf("%s send", r.Topic), opts...)

	textMapPropagator := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{})
	textMapPropagator.Inject(spanContext, NewRecordCarrier(r))

	r.Ctx = spanContext
}

func (k *Kotel) OnProduceRecordUnbuffered(r *kgo.Record, err error) {
	span := trace.SpanFromContext(r.Ctx)

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
	textMapPropagator := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{})
	producerSpan := context.Background()
	producerSpan = textMapPropagator.Extract(producerSpan, NewRecordCarrier(r))

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

	logappendContext, span := tracer.Start(producerSpan, "logappend", opts...)
	span.End()

	opts = []trace.SpanStartOption{
		trace.WithAttributes(attrs...),
		trace.WithSpanKind(trace.SpanKindConsumer),
	}

	_, span = tracer.Start(logappendContext, fmt.Sprintf("%s receive", r.Topic), opts...)
	span.End()
}
