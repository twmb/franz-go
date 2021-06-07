package main

import (
	"net"
	"net/http"
	"strconv"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/twmb/franz-go/pkg/kgo"
)

type Metrics struct {
	reg *prometheus.Registry

	connects    *prometheus.CounterVec
	connectErrs *prometheus.CounterVec
	disconnects *prometheus.CounterVec

	writeErrs    *prometheus.CounterVec
	writeBytes   *prometheus.CounterVec
	writeWaits   *prometheus.HistogramVec
	writeTimings *prometheus.HistogramVec

	readErrs    *prometheus.CounterVec
	readBytes   *prometheus.CounterVec
	readWaits   *prometheus.HistogramVec
	readTimings *prometheus.HistogramVec

	throttles *prometheus.HistogramVec
}

func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{})
}

// To see all hooks that are available, ctrl+f "Hook" in the package
// documentation! The code below implements all hooks available at the time of
// this writing for demonstration.

var ( // interface checks to ensure we implement the hooks properly
	_ kgo.HookBrokerConnect    = new(Metrics)
	_ kgo.HookBrokerDisconnect = new(Metrics)
	_ kgo.HookBrokerWrite      = new(Metrics)
	_ kgo.HookBrokerRead       = new(Metrics)
	_ kgo.HookBrokerThrottle   = new(Metrics)
)

func (m *Metrics) OnBrokerConnect(meta kgo.BrokerMetadata, _ time.Duration, _ net.Conn, err error) {
	node := strconv.Itoa(int(meta.NodeID))
	if err != nil {
		m.connectErrs.WithLabelValues(node).Inc()
		return
	}
	m.connects.WithLabelValues(node).Inc()
}

func (m *Metrics) OnBrokerDisconnect(meta kgo.BrokerMetadata, _ net.Conn) {
	node := strconv.Itoa(int(meta.NodeID))
	m.disconnects.WithLabelValues(node).Inc()
}

func (m *Metrics) OnBrokerWrite(meta kgo.BrokerMetadata, _ int16, bytesWritten int, writeWait, timeToWrite time.Duration, err error) {
	node := strconv.Itoa(int(meta.NodeID))
	if err != nil {
		m.writeErrs.WithLabelValues(node).Inc()
		return
	}
	m.writeBytes.WithLabelValues(node).Add(float64(bytesWritten))
	m.writeWaits.WithLabelValues(node).Observe(writeWait.Seconds())
	m.writeTimings.WithLabelValues(node).Observe(timeToWrite.Seconds())
}

func (m *Metrics) OnBrokerRead(meta kgo.BrokerMetadata, _ int16, bytesRead int, readWait, timeToRead time.Duration, err error) {
	node := strconv.Itoa(int(meta.NodeID))
	if err != nil {
		m.readErrs.WithLabelValues(node).Inc()
		return
	}
	m.readBytes.WithLabelValues(node).Add(float64(bytesRead))
	m.readWaits.WithLabelValues(node).Observe(readWait.Seconds())
	m.readTimings.WithLabelValues(node).Observe(timeToRead.Seconds())
}

func (m *Metrics) OnBrokerThrottle(meta kgo.BrokerMetadata, throttleInterval time.Duration, _ bool) {
	node := strconv.Itoa(int(meta.NodeID))
	m.throttles.WithLabelValues(node).Observe(throttleInterval.Seconds())
}

func NewMetrics(namespace string) (m *Metrics) {
	reg := prometheus.NewRegistry()
	factory := promauto.With(reg)

	reg.MustRegister(prometheus.NewProcessCollector(prometheus.ProcessCollectorOpts{}))
	reg.MustRegister(prometheus.NewGoCollector())

	return &Metrics{
		reg: reg,

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

		writeWaits: factory.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "write_wait_latencies",
			Help:      "Latency of time spent waiting to write to Kafka, in seconds by broker",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 1.5, 20),
		}, []string{"node_id"}),

		writeTimings: factory.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "write_latencies",
			Help:      "Latency of time spent writing to Kafka, in seconds by broker",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 1.5, 20),
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

		readWaits: factory.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "read_wait_latencies",
			Help:      "Latency of time spent waiting to read from Kafka, in seconds by broker",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 1.5, 20),
		}, []string{"node_id"}),

		readTimings: factory.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "read_latencies",
			Help:      "Latency of time spent reading from Kafka, in seconds by broker",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 1.5, 20),
		}, []string{"node_id"}),

		// throttles

		throttles: factory.NewHistogramVec(prometheus.HistogramOpts{
			Namespace: namespace,
			Name:      "throttle_latencies",
			Help:      "Latency of Kafka request throttles, in seconds by broker",
			Buckets:   prometheus.ExponentialBuckets(0.0001, 1.5, 20),
		}, []string{"node_id"}),
	}
}
