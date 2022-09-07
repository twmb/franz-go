package kotel

import (
	"context"
	"fmt"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

type Kotel struct {
	spanMap        map[*kgo.Record]context.Context
	TracerProvider *sdktrace.TracerProvider
}

func NewKotel(tracerProvider *sdktrace.TracerProvider) (*Kotel, error) {
	return &Kotel{
		spanMap:        make(map[*kgo.Record]context.Context),
		TracerProvider: tracerProvider,
	}, nil
}

// Attribues ideas:
// Topic, payload size, consumergroup (consumer only), clientid (who is calling)
// Look at what attributes / labesl Sarama applies.

func (k *Kotel) OnProduce(ctx context.Context, r *kgo.Record) {
	fmt.Println("OnProduce Reached")
	tracer := k.TracerProvider.Tracer("foo")

	attrs := []attribute.KeyValue{
		semconv.MessagingSystemKey.String("kafka"),
		semconv.MessagingDestinationKindTopic,
		semconv.MessagingDestinationKey.String(r.Topic),
	}
	opts := []trace.SpanStartOption{
		trace.WithAttributes(attrs...),
		trace.WithSpanKind(trace.SpanKindProducer),
	}

	spanContext, _ := tracer.Start(ctx, "produce", opts...)

	textMapPropagator := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{})
	textMapPropagator.Inject(spanContext, NewRecordCarrier(r))
	k.spanMap[r] = spanContext
}

func (k *Kotel) OnProduceRecordUnbuffered(r *kgo.Record, err error) {
	fmt.Println("HookProduceRecordUnbuffered Reached")
	span := trace.SpanFromContext(k.spanMap[r])

	span.SetAttributes(
		semconv.MessagingMessageIDKey.String(strconv.FormatInt(r.Offset, 10)),
		semconv.MessagingKafkaPartitionKey.Int64(int64(r.Partition)),
	)

	if err != nil {
		span.SetStatus(codes.Error, err.Error())
		span.RecordError(err)
	}

	span.End()
	defer delete(k.spanMap, r)
}

func (k *Kotel) OnFetchRecordBuffered(r *kgo.Record) {
	fmt.Println("HookOnFetchRecordBuffered Reached")
	fmt.Printf("offset %d\n", r.Offset)

	textMapPropagator := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{})
	producerSpan := context.Background()
	producerSpan = textMapPropagator.Extract(producerSpan, NewRecordCarrier(r))
	tracer := k.TracerProvider.Tracer("foo")

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

	_, span = tracer.Start(logappendContext, "consume", opts...)
	time.Sleep(time.Second)
	span.End()
}
