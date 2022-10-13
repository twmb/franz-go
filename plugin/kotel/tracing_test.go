package kotel

import (
	"testing"

	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
	"go.opentelemetry.io/otel/trace"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
)

func TestNewTracer(t *testing.T) {
	prop := propagation.NewCompositeTextMapPropagator()

	tracer := otel.GetTracerProvider().Tracer(
		instrumentationName,
		trace.WithInstrumentationVersion(SemVersion()),
		trace.WithSchemaURL(semconv.SchemaURL),
	)

	testCases := []struct {
		name     string
		opts     []TracingOption
		expected *Tracer
	}{

		{
			name: "Empty (Use globals)",
			opts: []TracingOption{},
			expected: &Tracer{
				tracerProvider: otel.GetTracerProvider(),
				tracer:         tracer,
				propagators:    otel.GetTextMapPropagator(),
			},
		},
		{
			name: "With TracerPropagator",
			opts: []TracingOption{TracerPropagator(prop)},
			expected: &Tracer{
				tracerProvider: otel.GetTracerProvider(),
				tracer:         tracer,
				propagators:    prop,
			},
		},
		{
			name: "Nil TracerPropagator",
			opts: []TracingOption{TracerPropagator(nil)},
			expected: &Tracer{
				tracerProvider: otel.GetTracerProvider(),
				tracer:         tracer,
				propagators:    otel.GetTextMapPropagator(),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := NewTracer(tc.opts...)
			assert.Equal(t, tc.expected, result)
		})
	}
}
