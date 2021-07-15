// Package kprom provides prometheus plug-in metrics for a kgo client.
//
// This package tracks the following metrics under the following names,
// all metrics being counter vecs:
//
//     #{ns}_connects_total{node_id="#{node}"}
//     #{ns}_connect_errors_total{node_id="#{node}"}
//     #{ns}_write_errors_total{node_id="#{node}"}
//     #{ns}_write_bytes_total{node_id="#{node}"}
//     #{ns}_read_errors_total{node_id="#{node}"}
//     #{ns}_read_bytes_total{node_id="#{node}"}
//     #{ns}_produce_bytes_total{node_id="#{node}",topic="#{topic}"}
//     #{ns}_fetch_bytes_total{node_id="#{node}",topic="#{topic}"}
//     #{ns}_buffered_produce_records_total
//     #{ns}_buffered_fetch_records_total
//
// This can be used in a client like so:
//
//     m := kprom.NewMetrics()
//     cl, err := kgo.NewClient(
//             kgo.WithHooks(m),
//             // ...other opts
//     )
//
// By default, metrics are installed under the a new prometheus registry, but
// this can be overridden with the Registry option.
//
// Note that seed brokers use broker IDs prefixed with "seed_", with the number
// corresponding to which seed it is.
package kprom

import (
	"math"
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

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

// Metrics provides prometheus metrics to a given registry.
type Metrics struct {
	cfg cfg

	connects    *prometheus.CounterVec
	connectErrs *prometheus.CounterVec
	disconnects *prometheus.CounterVec

	writeErrs  *prometheus.CounterVec
	writeBytes *prometheus.CounterVec

	readErrs  *prometheus.CounterVec
	readBytes *prometheus.CounterVec

	produceBytes *prometheus.CounterVec
	fetchBytes   *prometheus.CounterVec

	bufferedProduceRecords int64
	bufferedFetchRecords   int64
}

// Registry returns the prometheus registry that metrics were added to.
//
// This is useful if you want the Metrics type to create its own registry for
// you to add additional metrics to.
func (m *Metrics) Registry() prometheus.Registerer {
	return m.cfg.reg
}

// Handler returns an http.Handler providing prometheus metrics.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.cfg.gatherer, m.cfg.handlerOpts)
}

type cfg struct {
	namespace string

	reg      prometheus.Registerer
	gatherer prometheus.Gatherer

	handlerOpts  promhttp.HandlerOpts
	goCollectors bool
}

type RegistererGatherer interface {
	prometheus.Registerer
	prometheus.Gatherer
}

// Opt applies options to further tune how prometheus metrics are gathered or
// which metrics to use.
type Opt interface {
	apply(*cfg)
}

type opt struct{ fn func(*cfg) }

func (o opt) apply(c *cfg) { o.fn(c) }

// Registry sets the registerer and gatherer to add metrics to, rather than a new registry.
// Use this option if you want to configure both Gatherer and Registerer with the same object.
func Registry(rg RegistererGatherer) Opt {
	return opt{func(c *cfg) {
		c.reg = rg
		c.gatherer = rg
	}}
}

// Registry sets the registerer to add metrics to, rather than a new registry.
func Registerer(reg prometheus.Registerer) Opt {
	return opt{func(c *cfg) { c.reg = reg }}
}

// Registry sets the gatherer to add metrics to, rather than a new registry.
func Gatherer(gatherer prometheus.Gatherer) Opt {
	return opt{func(c *cfg) { c.gatherer = gatherer }}
}

// GoCollectors adds the prometheus.NewProcessCollector and
// prometheus.NewGoCollector collectors the the Metric's registry.
func GoCollectors() Opt {
	return opt{func(c *cfg) { c.goCollectors = true }}
}

// HandlerOpts sets handler options to use if you wish you use the
// Metrics.Handler function.
//
// This is only useful if you both (a) do not want to provide your own registry
// and (b) want to override the default handler options.
func HandlerOpts(opts promhttp.HandlerOpts) Opt {
	return opt{func(c *cfg) { c.handlerOpts = opts }}
}

// NewMetrics returns a new Metrics that adds prometheus metrics to the
// registry under the given namespace.
func NewMetrics(namespace string, opts ...Opt) *Metrics {
	var regGatherer RegistererGatherer = prometheus.NewRegistry()
	cfg := cfg{
		namespace: namespace,
		reg:       regGatherer,
		gatherer:  regGatherer,
	}
	for _, opt := range opts {
		opt.apply(&cfg)
	}

	if cfg.goCollectors {
		cfg.reg.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
		cfg.reg.MustRegister(prometheus.NewGoCollector())
	}

	factory := promauto.With(cfg.reg)

	return &Metrics{
		cfg: cfg,

		// connects and disconnects

		connects: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "connects_total",
			Help:      "Total number of connections opened, by broker",
		}, []string{"node_id"}),

		connectErrs: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "connect_errors_total",
			Help:      "Total number of connection errors, by broker",
		}, []string{"node_id"}),

		disconnects: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "disconnects_total",
			Help:      "Total number of connections closed, by broker",
		}, []string{"node_id"}),

		// write

		writeErrs: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "write_errors_total",
			Help:      "Total number of write errors, by broker",
		}, []string{"node_id"}),

		writeBytes: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "write_bytes_total",
			Help:      "Total number of bytes written, by broker",
		}, []string{"node_id"}),

		// read

		readErrs: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "read_errors_total",
			Help:      "Total number of read errors, by broker",
		}, []string{"node_id"}),

		readBytes: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "read_bytes_total",
			Help:      "Total number of bytes read, by broker",
		}, []string{"node_id"}),

		// produce & consume

		produceBytes: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "produce_bytes_total",
			Help:      "Total number of uncompressed bytes produced, by broker and topic",
		}, []string{"node_id", "topic"}),

		fetchBytes: factory.NewCounterVec(prometheus.CounterOpts{
			Namespace: namespace,
			Name:      "fetch_bytes_total",
			Help:      "Total number of uncompressed bytes fetched, by broker and topic",
		}, []string{"node_id", "topic"}),
	}
}

func (m *Metrics) OnNewClient(cl *kgo.Client) {
	factory := promauto.With(m.cfg.reg)

	factory.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: m.cfg.namespace,
		Name:      "buffered_produce_records_total",
		Help:      "Total number of records buffered within the client ready to be produced.",
	}, func() float64 { return float64(cl.BufferedProduceRecords()) })

	factory.NewGaugeFunc(prometheus.GaugeOpts{
		Namespace: m.cfg.namespace,
		Name:      "buffered_fetch_records_total",
		Help:      "Total number of records buffered within the client ready to be consumed.",
	}, func() float64 { return float64(cl.BufferedFetchRecords()) })
}

func strnode(node int32) string {
	if node < 0 {
		return "seed_" + strconv.Itoa(int(node)-math.MinInt32)
	}
	return strconv.Itoa(int(node))
}

func (m *Metrics) OnBrokerConnect(meta kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	node := strnode(meta.NodeID)
	if err != nil {
		m.connectErrs.WithLabelValues(node).Inc()
		return
	}
	m.connects.WithLabelValues(node).Inc()
}

func (m *Metrics) OnBrokerDisconnect(meta kgo.BrokerMetadata, _ net.Conn) {
	node := strnode(meta.NodeID)
	m.disconnects.WithLabelValues(node).Inc()
}

func (m *Metrics) OnBrokerWrite(meta kgo.BrokerMetadata, _ int16, bytesWritten int, _, _ time.Duration, err error) {
	node := strnode(meta.NodeID)
	if err != nil {
		m.writeErrs.WithLabelValues(node).Inc()
		return
	}
	m.writeBytes.WithLabelValues(node).Add(float64(bytesWritten))
}

func (m *Metrics) OnBrokerRead(meta kgo.BrokerMetadata, _ int16, bytesRead int, _, _ time.Duration, err error) {
	node := strnode(meta.NodeID)
	if err != nil {
		m.readErrs.WithLabelValues(node).Inc()
		return
	}
	m.readBytes.WithLabelValues(node).Add(float64(bytesRead))
}

func (m *Metrics) OnProduceBatchWritten(meta kgo.BrokerMetadata, topic string, _ int32, pbm kgo.ProduceBatchMetrics) {
	node := strnode(meta.NodeID)
	m.produceBytes.WithLabelValues(node, topic).Add(float64(pbm.UncompressedBytes))
}

func (m *Metrics) OnFetchBatchRead(meta kgo.BrokerMetadata, topic string, _ int32, fbm kgo.FetchBatchMetrics) {
	node := strnode(meta.NodeID)
	m.fetchBytes.WithLabelValues(node, topic).Add(float64(fbm.UncompressedBytes))
}
