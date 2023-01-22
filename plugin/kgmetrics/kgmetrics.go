// Package kgmetrics provides rcrowley/go-metrics drop-in metrics for a kgo
// client.
//
// This package tracks the following metrics under the following names, all
// metrics are meters:
//
//	broker.<id>.connects
//	broker.<id>.connect_errors
//	broker.<id>.disconnects
//	broker.<id>.write_errors
//	broker.<id>.write_bytes
//	broker.<id>.read_errors
//	broker.<id>.read_bytes
//	broker.<id>.topic.<topic>.produce_bytes
//	broker.<id>.topic.<topic>.fetch_bytes
//
// The metrics can be prefixed with the NamePrefix option.
//
// This can be used in a client like so:
//
//	m := kgmetrics.NewMetrics()
//	cl, err := kgo.NewClient(
//	        kgo.WithHooks(m),
//	        // ...other opts
//	)
//
// By default, metrics are installed under the DefaultRegistry, but this can
// be overridden with the Registry option.
//
// Note that seed brokers use broker IDs starting at math.MinInt32.
package kgmetrics

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/rcrowley/go-metrics"

	"github.com/twmb/franz-go/pkg/kgo"
)

var ( // interface checks to ensure we implement the hooks properly
	_ kgo.HookBrokerConnect       = new(Metrics)
	_ kgo.HookBrokerDisconnect    = new(Metrics)
	_ kgo.HookBrokerWrite         = new(Metrics)
	_ kgo.HookBrokerRead          = new(Metrics)
	_ kgo.HookProduceBatchWritten = new(Metrics)
	_ kgo.HookFetchBatchRead      = new(Metrics)
)

// Metrics provides rcrowley/go-metrics.
type Metrics struct {
	reg        metrics.Registry
	namePrefix string

	brokers sync.Map
}

// Registry returns the registry that metrics were added to.
func (m *Metrics) Registry() metrics.Registry {
	return m.reg
}

type broker struct {
	id int32

	connects    metrics.Meter
	connectErrs metrics.Meter
	disconnects metrics.Meter

	writeErrs  metrics.Meter
	writeBytes metrics.Meter

	readErrs  metrics.Meter
	readBytes metrics.Meter

	topics sync.Map
}

type brokerTopic struct {
	produceBytes metrics.Meter
	fetchBytes   metrics.Meter
}

// Opt applies options to further tune how metrics are gathered.
type Opt interface {
	apply(*Metrics)
}

type opt struct{ fn func(*Metrics) }

func (o opt) apply(m *Metrics) { o.fn(m) }

// Registry sets the registry to add metrics to, rather than metrics.DefaultRegistry.
func Registry(reg metrics.Registry) Opt {
	return opt{func(m *Metrics) { m.reg = reg }}
}

// NamePrefix sets configures all register names to be prefixed with this
// value.
func NamePrefix(prefix string) Opt {
	return opt{func(m *Metrics) { m.namePrefix = prefix }}
}

// NewMetrics returns a new Metrics.
func NewMetrics(opts ...Opt) *Metrics {
	m := &Metrics{
		reg: metrics.DefaultRegistry,
	}
	for _, opt := range opts {
		opt.apply(m)
	}
	return m
}

func (m *Metrics) loadBroker(id int32) *broker {
	bi, ok := m.brokers.Load(id)
	if !ok {
		name := func(metric string) string {
			return fmt.Sprintf("%sbroker.%d.%s",
				m.namePrefix,
				id,
				metric,
			)
		}
		b := &broker{
			id: id,

			connects:    metrics.GetOrRegisterMeter(name("connects"), m.reg),
			connectErrs: metrics.GetOrRegisterMeter(name("connect_errors"), m.reg),
			disconnects: metrics.GetOrRegisterMeter(name("disconnects"), m.reg),

			writeErrs:  metrics.GetOrRegisterMeter(name("write_errors"), m.reg),
			writeBytes: metrics.GetOrRegisterMeter(name("write_bytes"), m.reg),

			readErrs:  metrics.GetOrRegisterMeter(name("read_errors"), m.reg),
			readBytes: metrics.GetOrRegisterMeter(name("read_bytes"), m.reg),
		}
		bi, _ = m.brokers.LoadOrStore(id, b)
	}
	return bi.(*broker)
}

func (b *broker) loadTopic(m *Metrics, topic string) *brokerTopic {
	ti, ok := b.topics.Load(topic)
	if !ok {
		name := func(metric string) string {
			return fmt.Sprintf("%sbroker.%d.topic.%s.%s",
				m.namePrefix,
				b.id,
				topic,
				metric,
			)
		}
		t := &brokerTopic{
			produceBytes: metrics.GetOrRegisterMeter(name("produce_bytes"), m.reg),
			fetchBytes:   metrics.GetOrRegisterMeter(name("fetch_bytes"), m.reg),
		}
		ti, _ = b.topics.LoadOrStore(topic, t)
	}
	return ti.(*brokerTopic)
}

func (m *Metrics) OnBrokerConnect(meta kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	b := m.loadBroker(meta.NodeID)
	if err != nil {
		b.connectErrs.Mark(1)
		return
	}
	b.connects.Mark(1)
}

func (m *Metrics) OnBrokerDisconnect(meta kgo.BrokerMetadata, _ net.Conn) {
	b := m.loadBroker(meta.NodeID)
	b.disconnects.Mark(1)
}

func (m *Metrics) OnBrokerWrite(meta kgo.BrokerMetadata, _ int16, bytesWritten int, _, _ time.Duration, err error) {
	b := m.loadBroker(meta.NodeID)
	if err != nil {
		b.writeErrs.Mark(1)
		return
	}
	b.writeBytes.Mark(int64(bytesWritten))
}

func (m *Metrics) OnBrokerRead(meta kgo.BrokerMetadata, _ int16, bytesRead int, _, _ time.Duration, err error) {
	b := m.loadBroker(meta.NodeID)
	if err != nil {
		b.readErrs.Mark(1)
		return
	}
	b.readBytes.Mark(int64(bytesRead))
}

func (m *Metrics) OnProduceBatchWritten(meta kgo.BrokerMetadata, topic string, _ int32, pbm kgo.ProduceBatchMetrics) {
	b := m.loadBroker(meta.NodeID)
	t := b.loadTopic(m, topic)
	t.produceBytes.Mark(int64(pbm.UncompressedBytes))
}

func (m *Metrics) OnFetchBatchRead(meta kgo.BrokerMetadata, topic string, _ int32, fbm kgo.FetchBatchMetrics) {
	b := m.loadBroker(meta.NodeID)
	t := b.loadTopic(m, topic)
	t.fetchBytes.Mark(int64(fbm.UncompressedBytes))
}
