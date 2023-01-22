package kotel

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
	"go.opentelemetry.io/otel/trace"
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
