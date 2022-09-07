package kotel

import (
	"context"
	"fmt"
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
	spanContext, _ := tracer.Start(ctx, "produce", trace.WithSpanKind(trace.SpanKindProducer))

	textMapPropagator := propagation.NewCompositeTextMapPropagator(propagation.TraceContext{})
	textMapPropagator.Inject(spanContext, NewRecordCarrier(r))
	k.spanMap[r] = spanContext
}

func (k *Kotel) OnProduceRecordUnbuffered(r *kgo.Record, err error) {
	fmt.Println("HookProduceRecordUnbuffered Reached")
	span := trace.SpanFromContext(k.spanMap[r])
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

	logappendContext, span := tracer.Start(producerSpan, "logappend", trace.WithTimestamp(r.Timestamp), trace.WithSpanKind(trace.SpanKindConsumer))
	span.End()

	_, span = tracer.Start(logappendContext, "consume", trace.WithSpanKind(trace.SpanKindConsumer))
	time.Sleep(time.Second)
	span.End()
}
