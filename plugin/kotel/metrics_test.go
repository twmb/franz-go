package kotel

import (
	"testing"

	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"

	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/otel/metric"
)

func TestWithMetrics(t *testing.T) {
	provider := metric.NewNoopMeterProvider()

	testCases := []struct {
		name string
		opts []MetricsOption
		want *Meter
	}{
		{
			name: "With MeterProvider",
			opts: []MetricsOption{MeterProvider(provider)},
			want: &Meter{
				provider: provider,
				meter: provider.Meter(
					"github.com/twmb/franz-go/plugin/kotel",
					metric.WithInstrumentationVersion(SemVersion()),
					metric.WithSchemaURL(semconv.SchemaURL),
				),
				metrics: NewMeter(MeterProvider(provider)).metrics,
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
