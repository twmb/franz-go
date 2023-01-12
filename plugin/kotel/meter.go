package kotel

import (
	"context"
	"math"
	"net"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/global"
	"go.opentelemetry.io/otel/metric/instrument"
	"go.opentelemetry.io/otel/metric/instrument/syncint64"
	"go.opentelemetry.io/otel/metric/unit"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
)

var ( // interface checks to ensure we implement the hooks properly
	_ kgo.HookBrokerConnect       = new(Meter)
	_ kgo.HookBrokerDisconnect    = new(Meter)
	_ kgo.HookBrokerWrite         = new(Meter)
	_ kgo.HookBrokerRead          = new(Meter)
	_ kgo.HookProduceBatchWritten = new(Meter)
	_ kgo.HookFetchBatchRead      = new(Meter)
)

type Meter struct {
	provider metric.MeterProvider
	meter    metric.Meter
	metrics  Metrics
}

// MeterOpt interface used for setting optional config properties.
type MeterOpt interface {
	apply(*Meter)
}

type meterOptFunc func(*Meter)

func (o meterOptFunc) apply(m *Meter) {
	o(m)
}

// NewMeter returns a Meter, used as option for kotel to instrument franz-go with metrics
func NewMeter(opts ...MeterOpt) *Meter {
	m := &Meter{}
	for _, opt := range opts {
		opt.apply(m)
	}
	if m.provider == nil {
		m.provider = global.MeterProvider()
	}
	m.meter = m.provider.Meter(
		instrumentationName,
		metric.WithInstrumentationVersion(SemVersion()),
		metric.WithSchemaURL(semconv.SchemaURL),
	)
	m.metrics = m.NewMetrics()
	return m
}

// MeterProvider takes a metric.MeterProvider and applies it to the Meter
// If none is specified, the global provider is used.
func MeterProvider(provider metric.MeterProvider) MeterOpt {
	return meterOptFunc(func(m *Meter) {
		if provider != nil {
			m.provider = provider
		}
	})
}

// Metrics -------------------------------------------------------------------

type Metrics struct {
	connects    syncint64.Counter
	connectErrs syncint64.Counter
	disconnects syncint64.Counter

	writeErrs  syncint64.Counter
	writeBytes syncint64.Counter

	readErrs  syncint64.Counter
	readBytes syncint64.Counter

	produceBytes syncint64.Counter
	fetchBytes   syncint64.Counter
}

func (m *Meter) NewMetrics() Metrics {
	// connects and disconnects

	connects, _ := m.meter.SyncInt64().Counter(
		"connects_total",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("Total number of connections opened, by broker"),
	)
	connectsErrors, _ := m.meter.SyncInt64().Counter(
		"connect_errors_total",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("Total number of connection errors, by broker"),
	)
	disconnects, _ := m.meter.SyncInt64().Counter(
		"disconnects_total",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("Total number of connections closed, by broker"),
	)

	// write

	writeErrs, _ := m.meter.SyncInt64().Counter(
		"write_errors_total",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("Total number of write errors, by broker"),
	)
	writeBytes, _ := m.meter.SyncInt64().Counter(
		"write_bytes_total",
		instrument.WithUnit(unit.Bytes),
		instrument.WithDescription("Total number of bytes written, by broker"),
	)

	// read

	readErrs, _ := m.meter.SyncInt64().Counter(
		"read_errors_total",
		instrument.WithUnit(unit.Dimensionless),
		instrument.WithDescription("Total number of read errors, by broker"),
	)
	readBytes, _ := m.meter.SyncInt64().Counter(
		"read_bytes_total",
		instrument.WithUnit(unit.Bytes),
		instrument.WithDescription("Total number of bytes read, by broker"),
	)

	// produce & consume

	produceBytes, _ := m.meter.SyncInt64().Counter(
		"produce_bytes_total",
		instrument.WithUnit(unit.Bytes),
		instrument.WithDescription("Total number of uncompressed bytes produced, by broker and topic"),
	)
	fetchBytes, _ := m.meter.SyncInt64().Counter(
		"fetch_bytes_total",
		instrument.WithUnit(unit.Bytes),
		instrument.WithDescription("Total number of uncompressed bytes fetched, by broker and topic"),
	)

	return Metrics{
		connects:    connects,
		connectErrs: connectsErrors,
		disconnects: disconnects,

		writeErrs:  writeErrs,
		writeBytes: writeBytes,

		readErrs:  readErrs,
		readBytes: readBytes,

		produceBytes: produceBytes,
		fetchBytes:   fetchBytes,
	}
}

// Helpers -------------------------------------------------------------------

func strnode(node int32) string {
	if node < 0 {
		return "seed_" + strconv.Itoa(int(node)-math.MinInt32)
	}
	return strconv.Itoa(int(node))
}

// Hooks ---------------------------------------------------------------------

func (m *Meter) OnBrokerConnect(meta kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	node := strnode(meta.NodeID)
	if err != nil {
		m.metrics.connectErrs.Add(context.Background(), 1, attribute.String("node_id", node))
		return
	}
	m.metrics.connects.Add(context.Background(), 1, attribute.String("node_id", node))
}

func (m *Meter) OnBrokerDisconnect(meta kgo.BrokerMetadata, _ net.Conn) {
	node := strnode(meta.NodeID)
	m.metrics.disconnects.Add(context.Background(), 1, attribute.String("node_id", node))
}

func (m *Meter) OnBrokerWrite(meta kgo.BrokerMetadata, _ int16, bytesWritten int, _, _ time.Duration, err error) {
	node := strnode(meta.NodeID)
	if err != nil {
		m.metrics.writeErrs.Add(context.Background(), 1, attribute.String("node_id", node))
		return
	}
	m.metrics.writeBytes.Add(context.Background(), int64(bytesWritten), attribute.String("node_id", node))
}

func (m *Meter) OnBrokerRead(meta kgo.BrokerMetadata, _ int16, bytesRead int, _, _ time.Duration, err error) {
	node := strnode(meta.NodeID)
	if err != nil {
		m.metrics.readErrs.Add(context.Background(), 1, attribute.String("node_id", node))
		return
	}
	m.metrics.readBytes.Add(context.Background(), int64(bytesRead))
}

func (m *Meter) OnProduceBatchWritten(meta kgo.BrokerMetadata, topic string, _ int32, pbm kgo.ProduceBatchMetrics) {
	node := strnode(meta.NodeID)
	m.metrics.produceBytes.Add(context.Background(), int64(pbm.UncompressedBytes), attribute.String("node_id", node), attribute.String("topic", topic))
}

func (m *Meter) OnFetchBatchRead(meta kgo.BrokerMetadata, topic string, _ int32, fbm kgo.FetchBatchMetrics) {
	node := strnode(meta.NodeID)
	m.metrics.fetchBytes.Add(context.Background(), int64(fbm.UncompressedBytes), attribute.String("node_id", node), attribute.String("topic", topic))
}
