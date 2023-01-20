package kotel

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/metric"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
)

func TestWithMeter(t *testing.T) {
	provider := metric.NewNoopMeterProvider()

	testCases := []struct {
		name string
		opts []MeterOpt
		want *Meter
	}{
		{
			name: "With MeterProvider",
			opts: []MeterOpt{MeterProvider(provider)},
			want: &Meter{
				provider: provider,
				meter: provider.Meter(
					instrumentationName,
					metric.WithInstrumentationVersion(SemVersion()),
					metric.WithSchemaURL(semconv.SchemaURL),
				),
				instruments: NewMeter(MeterProvider(provider)).instruments,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := NewMeter(tc.opts...)
			assert.Equal(t, tc.want, result)
		})
	}
}
