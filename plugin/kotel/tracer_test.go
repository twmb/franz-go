package kotel

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.18.0"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestNewTracer(t *testing.T) {
	prop := propagation.NewCompositeTextMapPropagator()

	tracer := otel.GetTracerProvider().Tracer(
		instrumentationName,
		trace.WithInstrumentationVersion(semVersion()),
		trace.WithSchemaURL(semconv.SchemaURL),
	)

	testCases := []struct {
		name string
		opts []TracerOpt
		want *Tracer
	}{
		{
			name: "Empty (Use globals)",
			opts: []TracerOpt{},
			want: &Tracer{
				tracerProvider: otel.GetTracerProvider(),
				tracer:         tracer,
				propagators:    otel.GetTextMapPropagator(),
			},
		},
		{
			name: "With TracerPropagator",
			opts: []TracerOpt{TracerPropagator(prop)},
			want: &Tracer{
				tracerProvider: otel.GetTracerProvider(),
				tracer:         tracer,
				propagators:    prop,
			},
		},
		{
			name: "Nil TracerPropagator",
			opts: []TracerOpt{TracerPropagator(nil)},
			want: &Tracer{
				tracerProvider: otel.GetTracerProvider(),
				tracer:         tracer,
				propagators:    otel.GetTextMapPropagator(),
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := NewTracer(tc.opts...)
			assert.Equal(t, tc.want, result)
		})
	}
}

func TestTracerKeyAttr(t *testing.T) {
	errKey := errors.New("format key")
	tests := []struct {
		name      string
		key       []byte
		formatter func(*kgo.Record) (string, error)
		want      attribute.KeyValue
		wantOK    bool
	}{
		{name: "nil"},
		{
			name:   "valid",
			key:    []byte("key"),
			want:   semconv.MessagingKafkaMessageKeyKey.String("key"),
			wantOK: true,
		},
		{name: "invalid UTF-8", key: []byte{0xff}},
		{
			name: "formatted",
			key:  []byte{0xff},
			formatter: func(*kgo.Record) (string, error) {
				return "formatted", nil
			},
			want:   semconv.MessagingKafkaMessageKeyKey.String("formatted"),
			wantOK: true,
		},
		{
			name: "formatter error",
			key:  []byte("key"),
			formatter: func(*kgo.Record) (string, error) {
				return "", errKey
			},
		},
		{
			name: "formatter invalid UTF-8",
			key:  []byte("key"),
			formatter: func(*kgo.Record) (string, error) {
				return "\xff", nil
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tracer := &Tracer{keyFormatter: test.formatter}
			got, ok := tracer.keyAttr(&kgo.Record{Key: test.key})
			assert.Equal(t, test.wantOK, ok)
			assert.Equal(t, test.want, got)
		})
	}
}

func TestTracerAttributeBuilders(t *testing.T) {
	tracer := &Tracer{clientID: "client", consumerGroup: "group"}
	record := &kgo.Record{
		Topic:     "orders",
		Partition: 3,
		Offset:    42,
		Key:       []byte("key"),
	}

	t.Run("publish", func(t *testing.T) {
		got := tracer.publishAttrs(record)
		want := []attribute.KeyValue{
			semconv.MessagingSystemKey.String("kafka"),
			semconv.MessagingDestinationKindTopic,
			semconv.MessagingDestinationName("orders"),
			semconv.MessagingOperationPublish,
			semconv.MessagingKafkaMessageKeyKey.String("key"),
			semconv.MessagingKafkaClientIDKey.String("client"),
			semconv.MessagingKafkaMessageTombstoneKey.Bool(true),
		}
		assert.Equal(t, want, got)
		assert.Equal(t, len(got), cap(got))
	})

	t.Run("consumer", func(t *testing.T) {
		got := tracer.consumerAttrs(record, semconv.MessagingOperationReceive)
		want := []attribute.KeyValue{
			semconv.MessagingSystemKey.String("kafka"),
			semconv.MessagingSourceKindTopic,
			semconv.MessagingSourceName("orders"),
			semconv.MessagingOperationReceive,
			semconv.MessagingKafkaSourcePartition(3),
			semconv.MessagingKafkaMessageOffsetKey.Int64(42),
			semconv.MessagingKafkaMessageKeyKey.String("key"),
			semconv.MessagingKafkaClientIDKey.String("client"),
			semconv.MessagingKafkaConsumerGroupKey.String("group"),
			semconv.MessagingKafkaMessageTombstoneKey.Bool(true),
		}
		assert.Equal(t, want, got)
		assert.Equal(t, len(got), cap(got))
	})

	t.Run("preserves formatter evaluation order", func(t *testing.T) {
		calls := 0
		tracer := &Tracer{keyFormatter: func(r *kgo.Record) (string, error) {
			calls++
			r.Topic = "changed"
			return "", errors.New("format key")
		}}
		got := tracer.publishAttrs(&kgo.Record{Topic: "orders", Key: []byte("key"), Value: []byte("value")})
		want := []attribute.KeyValue{
			semconv.MessagingSystemKey.String("kafka"),
			semconv.MessagingDestinationKindTopic,
			semconv.MessagingDestinationName("orders"),
			semconv.MessagingOperationPublish,
		}
		assert.Equal(t, want, got)
		assert.Equal(t, 1, calls)
		assert.Equal(t, len(got), cap(got))
	})
}

func BenchmarkTracerRecordLifecycle(b *testing.B) {
	tracer := NewTracer(
		TracerProvider(tracenoop.NewTracerProvider()),
		TracerPropagator(propagation.TraceContext{}),
		ClientID("benchmark-client"),
		ConsumerGroup("benchmark-group"),
	)
	key := []byte("customer-1234567890")

	b.Run("produce", func(b *testing.B) {
		record := &kgo.Record{Topic: "orders", Key: key, Value: []byte("value")}
		b.ReportAllocs()
		for b.Loop() {
			record.Context = context.Background()
			tracer.OnProduceRecordBuffered(record)
			tracer.OnProduceRecordUnbuffered(record, nil)
		}
	})

	b.Run("fetch", func(b *testing.B) {
		record := &kgo.Record{
			Topic:     "orders",
			Partition: 3,
			Offset:    42,
			Key:       key,
			Value:     []byte("value"),
			Headers: []kgo.RecordHeader{{
				Key:   "traceparent",
				Value: []byte("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"),
			}},
		}
		b.ReportAllocs()
		for b.Loop() {
			record.Context = context.Background()
			tracer.OnFetchRecordBuffered(record)
			tracer.OnFetchRecordUnbuffered(record, true)
		}
	})

	b.Run("process", func(b *testing.B) {
		record := &kgo.Record{Topic: "orders", Partition: 3, Offset: 42, Key: key, Value: []byte("value")}
		b.ReportAllocs()
		for b.Loop() {
			record.Context = context.Background()
			_, span := tracer.WithProcessSpan(record)
			span.End()
		}
	})
}
