package kotel

import (
	"context"
	"log"
	"math"
	"net"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/metric/global"
	"go.opentelemetry.io/otel/metric/instrument"
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

const (
	dimensionless = "1"
	bytes         = "by"
)

type Meter struct {
	provider    metric.MeterProvider
	meter       metric.Meter
	instruments instruments
}

// MeterOpt interface used for setting optional config properties.
type MeterOpt interface {
	apply(*Meter)
}

type meterOptFunc func(*Meter)

// MeterProvider takes a metric.MeterProvider and applies it to the Meter
// If none is specified, the global provider is used.
func MeterProvider(provider metric.MeterProvider) MeterOpt {
	return meterOptFunc(func(m *Meter) {
		if provider != nil {
			m.provider = provider
		}
	})
}

func (o meterOptFunc) apply(m *Meter) {
	o(m)
}

// NewMeter returns a Meter, used as option for kotel to instrument franz-go
// with instruments.
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
		metric.WithInstrumentationVersion(semVersion()),
		metric.WithSchemaURL(semconv.SchemaURL),
	)
	m.instruments = m.newInstruments()
	return m
}

// instruments ---------------------------------------------------------------

type instruments struct {
	connects    instrument.Int64Counter
	connectErrs instrument.Int64Counter
	disconnects instrument.Int64Counter

	writeErrs  instrument.Int64Counter
	writeBytes instrument.Int64Counter

	readErrs  instrument.Int64Counter
	readBytes instrument.Int64Counter

	produceBytes instrument.Int64Counter
	fetchBytes   instrument.Int64Counter
}

func (m *Meter) newInstruments() instruments {
	// connects and disconnects

	connects, err := m.meter.Int64Counter(
		"messaging.kafka.connects.count",
		instrument.WithUnit(dimensionless),
		instrument.WithDescription("Total number of connections opened, by broker"),
	)
	if err != nil {
		log.Printf("failed to create connects instrument, %v", err)
	}

	connectErrs, err := m.meter.Int64Counter(
		"messaging.kafka.connect_errors.count",
		instrument.WithUnit(dimensionless),
		instrument.WithDescription("Total number of connection errors, by broker"),
	)
	if err != nil {
		log.Printf("failed to create connectErrs instrument, %v", err)
	}

	disconnects, err := m.meter.Int64Counter(
		"messaging.kafka.disconnects.count",
		instrument.WithUnit(dimensionless),
		instrument.WithDescription("Total number of connections closed, by broker"),
	)
	if err != nil {
		log.Printf("failed to create disconnects instrument, %v", err)
	}

	// write

	writeErrs, err := m.meter.Int64Counter(
		"messaging.kafka.write_errors.count",
		instrument.WithUnit(dimensionless),
		instrument.WithDescription("Total number of write errors, by broker"),
	)
	if err != nil {
		log.Printf("failed to create writeErrs instrument, %v", err)
	}

	writeBytes, err := m.meter.Int64Counter(
		"messaging.kafka.write_bytes",
		instrument.WithUnit(bytes),
		instrument.WithDescription("Total number of bytes written, by broker"),
	)
	if err != nil {
		log.Printf("failed to create writeBytes instrument, %v", err)
	}

	// read

	readErrs, err := m.meter.Int64Counter(
		"messaging.kafka.read_errors.count",
		instrument.WithUnit(dimensionless),
		instrument.WithDescription("Total number of read errors, by broker"),
	)
	if err != nil {
		log.Printf("failed to create readErrs instrument, %v", err)
	}

	readBytes, err := m.meter.Int64Counter(
		"messaging.kafka.read_bytes.count",
		instrument.WithUnit(bytes),
		instrument.WithDescription("Total number of bytes read, by broker"),
	)
	if err != nil {
		log.Printf("failed to create readBytes instrument, %v", err)
	}

	// produce & consume

	produceBytes, err := m.meter.Int64Counter(
		"messaging.kafka.produce_bytes.count",
		instrument.WithUnit(bytes),
		instrument.WithDescription("Total number of uncompressed bytes produced, by broker and topic"),
	)
	if err != nil {
		log.Printf("failed to create produceBytes instrument, %v", err)
	}

	fetchBytes, err := m.meter.Int64Counter(
		"messaging.kafka.fetch_bytes.count",
		instrument.WithUnit(bytes),
		instrument.WithDescription("Total number of uncompressed bytes fetched, by broker and topic"),
	)
	if err != nil {
		log.Printf("failed to create fetchBytes instrument, %v", err)
	}

	return instruments{
		connects:    connects,
		connectErrs: connectErrs,
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
		m.instruments.connectErrs.Add(
			context.Background(),
			1,
			attribute.String("node_id", node),
		)
		return
	}
	m.instruments.connects.Add(
		context.Background(),
		1,
		attribute.String("node_id", node),
	)
}

func (m *Meter) OnBrokerDisconnect(meta kgo.BrokerMetadata, _ net.Conn) {
	node := strnode(meta.NodeID)
	m.instruments.disconnects.Add(
		context.Background(),
		1,
		attribute.String("node_id", node),
	)
}

func (m *Meter) OnBrokerWrite(meta kgo.BrokerMetadata, _ int16, bytesWritten int, _, _ time.Duration, err error) {
	node := strnode(meta.NodeID)
	if err != nil {
		m.instruments.writeErrs.Add(
			context.Background(),
			1,
			attribute.String("node_id", node),
		)
		return
	}
	m.instruments.writeBytes.Add(
		context.Background(),
		int64(bytesWritten),
		attribute.String("node_id", node),
	)
}

func (m *Meter) OnBrokerRead(meta kgo.BrokerMetadata, _ int16, bytesRead int, _, _ time.Duration, err error) {
	node := strnode(meta.NodeID)
	if err != nil {
		m.instruments.readErrs.Add(
			context.Background(),
			1,
			attribute.String("node_id", node),
		)
		return
	}
	m.instruments.readBytes.Add(context.Background(), int64(bytesRead))
}

func (m *Meter) OnProduceBatchWritten(meta kgo.BrokerMetadata, topic string, _ int32, pbm kgo.ProduceBatchMetrics) {
	node := strnode(meta.NodeID)
	m.instruments.produceBytes.Add(
		context.Background(),
		int64(pbm.UncompressedBytes),
		attribute.String("node_id", node),
		attribute.String("topic", topic),
	)
}

func (m *Meter) OnFetchBatchRead(meta kgo.BrokerMetadata, topic string, _ int32, fbm kgo.FetchBatchMetrics) {
	node := strnode(meta.NodeID)
	m.instruments.fetchBytes.Add(
		context.Background(),
		int64(fbm.UncompressedBytes),
		attribute.String("node_id", node),
		attribute.String("topic", topic),
	)
}
