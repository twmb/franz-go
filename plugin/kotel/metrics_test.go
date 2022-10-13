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
		name     string
		opts     []MetricsOption
		expected *Meter
	}{
		// TODO: Fix test
		//{
		//	name: "Empty (Global provider)",
		//	opts: []MetricsOption{},
		//	expected: &Meter{
		//		provider: meterProvider,
		//		meter:    globalMeter,
		//		metrics:  metrics,
		//	},
		//},
		//{
		//	name: "Nil MeterProvider",
		//	opts: []MetricsOption{MeterProvider(nil)},
		//	expected: &Meter{
		//		provider: meterProvider,
		//		meter:    globalMeter,
		//		metrics:  metrics,
		//	},
		//},
		{
			name: "With MeterProvider",
			opts: []MetricsOption{MeterProvider(metric.NewNoopMeterProvider())},
			expected: &Meter{
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
			assert.Equal(t, tc.expected, result)
		})
	}
}
