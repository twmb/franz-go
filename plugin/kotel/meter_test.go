package kotel

import (
	"context"
	"errors"
	"net"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/noop"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/metric/metricdata/metricdatatest"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
)

func TestWithMeter(t *testing.T) {
	provider := noop.NewMeterProvider()

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
					metric.WithInstrumentationVersion(semVersion()),
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

func TestHook_OnBrokerConnect(t *testing.T) {
	t.Run("success path with mergeConnectsMeter:false", func(t *testing.T) {
		r := sdkmetric.NewManualReader()
		mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(r))
		m := NewMeter(MeterProvider(mp))

		meta := kgo.BrokerMetadata{NodeID: 1}
		m.OnBrokerConnect(meta, time.Second, &net.TCPConn{}, nil)

		rm := metricdata.ResourceMetrics{}
		if err := r.Collect(context.Background(), &rm); err != nil {
			t.Errorf("unexpected error collecting metrics: %s", err)
		}

		want := metricdata.Metrics{
			Name:        "messaging.kafka.connects.count",
			Description: "Total number of connections opened, by broker",
			Unit:        "1",
			Data: metricdata.Sum[int64]{
				Temporality: metricdata.CumulativeTemporality,
				IsMonotonic: true,
				DataPoints: []metricdata.DataPoint[int64]{
					{
						Value: 1,
						Attributes: attribute.NewSet(
							attribute.String("node_id", strconv.Itoa(int(meta.NodeID))),
						),
					},
				},
			},
		}

		if len(rm.ScopeMetrics) != 1 {
			t.Errorf("expecting only 1 metrics in meter but got %d", len(rm.ScopeMetrics))
		}

		metricdatatest.AssertEqual(t, want, rm.ScopeMetrics[0].Metrics[0],
			metricdatatest.IgnoreTimestamp(),
		)
	})
	t.Run("failure path with mergeConnectsMeter:false", func(t *testing.T) {
		r := sdkmetric.NewManualReader()
		mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(r))
		m := NewMeter(MeterProvider(mp))

		meta := kgo.BrokerMetadata{NodeID: 1}
		m.OnBrokerConnect(meta, time.Second, &net.TCPConn{}, errors.New("whatever error"))

		rm := metricdata.ResourceMetrics{}
		if err := r.Collect(context.Background(), &rm); err != nil {
			t.Errorf("unexpected error collecting metrics: %s", err)
		}

		want := metricdata.Metrics{
			Name:        "messaging.kafka.connect_errors.count",
			Description: "Total number of connection errors, by broker",
			Unit:        "1",
			Data: metricdata.Sum[int64]{
				Temporality: metricdata.CumulativeTemporality,
				IsMonotonic: true,
				DataPoints: []metricdata.DataPoint[int64]{
					{
						Value: 1,
						Attributes: attribute.NewSet(
							attribute.String("node_id", strconv.Itoa(int(meta.NodeID))),
						),
					},
				},
			},
		}

		if len(rm.ScopeMetrics) != 1 {
			t.Errorf("expecting only 1 metrics in meter but got %d", len(rm.ScopeMetrics))
		}

		metricdatatest.AssertEqual(t, want, rm.ScopeMetrics[0].Metrics[0],
			metricdatatest.IgnoreTimestamp(),
		)
	})

	t.Run("success path with mergeConnectsMeter:true", func(t *testing.T) {
		r := sdkmetric.NewManualReader()
		mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(r))
		m := NewMeter(MeterProvider(mp), WithMergedConnectsMeter())

		meta := kgo.BrokerMetadata{NodeID: 1}
		m.OnBrokerConnect(meta, time.Second, &net.TCPConn{}, nil)

		rm := metricdata.ResourceMetrics{}
		if err := r.Collect(context.Background(), &rm); err != nil {
			t.Errorf("unexpected error collecting metrics: %s", err)
		}

		want := metricdata.Metrics{
			Name:        "messaging.kafka.connects.count",
			Description: "Total number of connections opened, by broker",
			Unit:        "1",
			Data: metricdata.Sum[int64]{
				Temporality: metricdata.CumulativeTemporality,
				IsMonotonic: true,
				DataPoints: []metricdata.DataPoint[int64]{
					{
						Value: 1,
						Attributes: attribute.NewSet(
							attribute.String("node_id", strconv.Itoa(int(meta.NodeID))),
							attribute.String("outcome", "success"),
						),
					},
				},
			},
		}

		if len(rm.ScopeMetrics) != 1 {
			t.Errorf("expecting only 1 metrics in meter but got %d", len(rm.ScopeMetrics))
		}

		metricdatatest.AssertEqual(t, want, rm.ScopeMetrics[0].Metrics[0],
			metricdatatest.IgnoreTimestamp(),
		)
	})
	t.Run("failure path with mergeConnectsMeter:true", func(t *testing.T) {
		r := sdkmetric.NewManualReader()
		mp := sdkmetric.NewMeterProvider(sdkmetric.WithReader(r))
		m := NewMeter(MeterProvider(mp), WithMergedConnectsMeter())

		meta := kgo.BrokerMetadata{NodeID: 1}
		m.OnBrokerConnect(meta, time.Second, &net.TCPConn{}, errors.New("whatever error"))

		rm := metricdata.ResourceMetrics{}
		if err := r.Collect(context.Background(), &rm); err != nil {
			t.Errorf("unexpected error collecting metrics: %s", err)
		}

		want := metricdata.Metrics{
			Name:        "messaging.kafka.connects.count",
			Description: "Total number of connections opened, by broker",
			Unit:        "1",
			Data: metricdata.Sum[int64]{
				Temporality: metricdata.CumulativeTemporality,
				IsMonotonic: true,
				DataPoints: []metricdata.DataPoint[int64]{
					{
						Value: 1,
						Attributes: attribute.NewSet(
							attribute.String("node_id", strconv.Itoa(int(meta.NodeID))),
							attribute.String("outcome", "failure"),
						),
					},
				},
			},
		}

		if len(rm.ScopeMetrics) != 1 {
			t.Errorf("expecting only 1 metrics in meter but got %d", len(rm.ScopeMetrics))
		}

		metricdatatest.AssertEqual(t, want, rm.ScopeMetrics[0].Metrics[0],
			metricdatatest.IgnoreTimestamp(),
		)
	})

}
