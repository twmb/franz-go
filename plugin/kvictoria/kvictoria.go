// Package kvictoria provides metrics using the [VictoriaMetrics/metrics] library for a kgo client.
//
// # Metrics
//
// This package tracks the following metrics:
//
// Buffering:
//
//	{namespace}_{subsystem}_buffered_fetch_bytes_total{client_id="#{client_id}"} (gauge)
//	{namespace}_{subsystem}_buffered_fetch_records_total{client_id="#{client_id}"} (gauge)
//	{namespace}_{subsystem}_buffered_produce_bytes_total{client_id="#{client_id}"} (gauge)
//	{namespace}_{subsystem}_buffered_produce_records_total{client_id="#{client_id}"} (gauge)
//
// Connections:
//
//	{namespace}_{subsystem}_connect_errors_total{node_id="#{node_id}"} (counter)
//	{namespace}_{subsystem}_connects_total{node_id="#{node_id}"} (counter)
//	{namespace}_{subsystem}_connect_seconds{node_id="#{node_id}"} (histogram)
//	{namespace}_{subsystem}_disconnects_total{node_id="#{node_id}"} (counter)
//
// End to end:
//
//	{namespace}_{subsystem}_write_errors_total{node_id="#{node_id}"} (counter)
//	{namespace}_{subsystem}_write_bytes_total{node_id="#{node_id}"} (counter)
//	{namespace}_{subsystem}_write_wait_seconds{node_id="#{node_id}"} (histogram)
//	{namespace}_{subsystem}_write_time_seconds{node_id="#{node_id}"} (histogram)
//	{namespace}_{subsystem}_read_errors_total{node_id="#{node_id}"} (counter)
//	{namespace}_{subsystem}_read_bytes_total{node_id="#{node_id}"} (counter)
//	{namespace}_{subsystem}_read_wait_seconds{node_id="#{node_id}"} (histogram)
//	{namespace}_{subsystem}_read_time_seconds{node_id="#{node_id}"} (histogram)
//	{namespace}_{subsystem}_request_duration_e2e_seconds{node_id="#{node_id}"} (histogram)
//
// Misc:
//
//	{namespace}_{subsystem}_request_throttled_seconds{node_id="#{node_id}"} (histogram)
//	{namespace}_{subsystem}_group_manage_error{node_id="#{node_id}"} (counter)
//
// Batches:
//
//	{namespace}_{subsystem}_produce_uncompressed_bytes_total{node_id="#{node_id}", topic="#{topic}", partition="#{partition}"} (counter)
//	{namespace}_{subsystem}_produce_compressed_bytes_total{node_id="#{node_id}", topic="#{topic}", partition="#{partition}"} (counter)
//	{namespace}_{subsystem}_produce_batches_total{node_id="#{node_id}", topic="#{topic}", partition="#{partition}"} (counter)
//	{namespace}_{subsystem}_produce_records_total{node_id="#{node_id}", topic="#{topic}", partition="#{partition}"} (counter)
//
//	{namespace}_{subsystem}_fetch_uncompressed_bytes_total{node_id="#{node_id}", topic="#{topic}", partition="#{partition}"} (counter)
//	{namespace}_{subsystem}_fetch_compressed_bytes_total{node_id="#{node_id}", topic="#{topic}", partition="#{partition}"} (counter)
//	{namespace}_{subsystem}_fetch_batches_total{node_id="#{node_id}", topic="#{topic}", partition="#{partition}"} (counter)
//	{namespace}_{subsystem}_fetch_records_total{node_id="#{node_id}", topic="#{topic}", partition="#{partition}"} (counter)
//
// # Usage
//
// This can be used in a client like this:
//
//	cl, err := kgo.NewCLient(
//		kgo.WithHooks(kvictoria.NewMetrics("my_namespace")),
//	)
//
// Note that you MUST use a new [Metrics] instance per client otherwise you can get surprising behaviour.
//
// Note that seed brokers use broker IDs prefixed with "seed_", with the number
// corresponding to which seed it is.
package kvictoria

import (
	"errors"
	"fmt"
	"maps"
	"net"
	"slices"
	"strconv"
	"strings"
	"time"

	vm "github.com/VictoriaMetrics/metrics"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	// interface checks to ensure we implement the hooks properly
	_ kgo.HookNewClient           = new(Metrics)
	_ kgo.HookClientClosed        = new(Metrics)
	_ kgo.HookBrokerConnect       = new(Metrics)
	_ kgo.HookBrokerDisconnect    = new(Metrics)
	_ kgo.HookBrokerWrite         = new(Metrics)
	_ kgo.HookBrokerRead          = new(Metrics)
	_ kgo.HookBrokerE2E           = new(Metrics)
	_ kgo.HookBrokerThrottle      = new(Metrics)
	_ kgo.HookGroupManageError    = new(Metrics)
	_ kgo.HookProduceBatchWritten = new(Metrics)
	_ kgo.HookFetchBatchRead      = new(Metrics)
)

// Metrics provides metrics using the [VictoriaMetrics/metrics] library.
//
// [VictoriaMetrics/metrics]: https://github.com/VictoriaMetrics/metrics
type Metrics struct {
	cfg cfg

	clientID string
	set      *vm.Set
}

// NewMetrics returns a new Metrics that tracks metrics under the given namespace.
//
// You can pass options to configure the metrics reporting. See [Opt] for all existing options.
func NewMetrics(namespace string, opts ...Opt) *Metrics {
	return &Metrics{
		cfg: newCfg(namespace, opts...),
	}
}

// OnNewClient implements the [kgo.HookNewClient] interface for metrics gathering.
// This method is meant to be called by the hook system and not by the user.
func (m *Metrics) OnNewClient(client *kgo.Client) {
	clientID := client.OptValue(kgo.ClientID).(string)

	if m.clientID != "" {
		panic(fmt.Errorf("do not reuse a Metrics instance for multiple kgo clients; current client id: %s", m.clientID))
	}

	m.clientID = clientID
	m.set = vm.NewSet()

	labels := map[string]string{"client_id": clientID}

	m.set.GetOrCreateGauge(m.buildName("buffered_fetch_bytes_total", labels), func() float64 {
		return float64(client.BufferedFetchBytes())
	})
	m.set.GetOrCreateGauge(m.buildName("buffered_fetch_records_total", labels), func() float64 {
		return float64(client.BufferedFetchRecords())
	})
	m.set.GetOrCreateGauge(m.buildName("buffered_produce_bytes_total", labels), func() float64 {
		return float64(client.BufferedProduceBytes())
	})
	m.set.GetOrCreateGauge(m.buildName("buffered_produce_records_total", labels), func() float64 {
		return float64(client.BufferedProduceRecords())
	})

	vm.RegisterSet(m.set)
}

// OnClientClosed implements the [kgo.HookClientClosed] interface for metrics gathering.
// This method is meant to be called by the hook system and not by the user.
//
// This will unregister all metrics that are scoped to the client id of the client provided.
func (m *Metrics) OnClientClosed(client *kgo.Client) {
	vm.UnregisterSet(m.set, true)
}

// OnBrokerConnect implements the [kgo.HookBrokerConnect] interface for metrics gathering.
// This method is meant to be called by the hook system and not by the user.
func (m *Metrics) OnBrokerConnect(meta kgo.BrokerMetadata, dialTime time.Duration, _ net.Conn, err error) {
	labels := map[string]string{"node_id": kgo.NodeName(meta.NodeID)}

	if err != nil {
		vm.GetOrCreateCounter(m.buildName("connect_errors_total", labels)).Inc()
		return
	}

	vm.GetOrCreateCounter(m.buildName("connects_total", labels)).Inc()
	vm.GetOrCreateHistogram(m.buildName("connect_seconds", labels)).Update(dialTime.Seconds())
}

// OnBrokerDisconnect implements the [kgo.HookBrokerDisconnect] interface for metrics gathering.
// This method is meant to be called by the hook system and not by the user
func (m *Metrics) OnBrokerDisconnect(meta kgo.BrokerMetadata, _ net.Conn) {
	labels := map[string]string{"node_id": kgo.NodeName(meta.NodeID)}

	vm.GetOrCreateCounter(m.buildName("disconnects_total", labels)).Inc()
}

// OnBrokerWrite is a noop implementation of [kgo.HookBrokerWrite], logic moved to OnBrokerE2E
func (m *Metrics) OnBrokerWrite(meta kgo.BrokerMetadata, _ int16, bytesWritten int, _, _ time.Duration, err error) {
}

// OnBrokerRead is a noop implementation of [kgo.HookBrokerRead], logic moved to OnBrokerE2E
func (m *Metrics) OnBrokerRead(meta kgo.BrokerMetadata, _ int16, bytesRead int, _, _ time.Duration, err error) {
}

// OnBrokerE2E implements the [kgo.HookBrokerE2E] interface for metrics gathering
// This method is meant to be called by the hook system and not by the user.
func (m *Metrics) OnBrokerE2E(meta kgo.BrokerMetadata, _ int16, e2e kgo.BrokerE2E) {
	labels := map[string]string{"node_id": kgo.NodeName(meta.NodeID)}

	if e2e.WriteErr != nil {
		vm.GetOrCreateCounter(m.buildName("write_errors_total", labels)).Inc()
		return
	}

	vm.GetOrCreateCounter(m.buildName("write_bytes_total", labels)).Add(e2e.BytesWritten)
	vm.GetOrCreateHistogram(m.buildName("write_wait_seconds", labels)).Update(e2e.WriteWait.Seconds())
	vm.GetOrCreateHistogram(m.buildName("write_time_seconds", labels)).Update(e2e.TimeToWrite.Seconds())

	if e2e.ReadErr != nil {
		vm.GetOrCreateCounter(m.buildName("read_errors_total", labels)).Inc()
		return
	}

	vm.GetOrCreateCounter(m.buildName("read_bytes_total", labels)).Add(e2e.BytesRead)
	vm.GetOrCreateHistogram(m.buildName("read_wait_seconds", labels)).Update(e2e.ReadWait.Seconds())
	vm.GetOrCreateHistogram(m.buildName("read_time_seconds", labels)).Update(e2e.TimeToRead.Seconds())

	vm.GetOrCreateHistogram(m.buildName("request_duration_e2e_seconds", labels)).Update(e2e.DurationE2E().Seconds())
}

// OnBrokerThrottle implements the [kgo.HookBrokerThrottle] interface for metrics gathering.
// This method is meant to be called by the hook system and not by the user.
func (m *Metrics) OnBrokerThrottle(meta kgo.BrokerMetadata, throttleInterval time.Duration, _ bool) {
	labels := map[string]string{"node_id": kgo.NodeName(meta.NodeID)}

	vm.GetOrCreateHistogram(m.buildName("request_throttled_seconds", labels)).Update(throttleInterval.Seconds())
}

// OnGroupManageError implements the [kgo.HookBrokerThrottle] interface for metrics gathering.
// This method is meant to be called by the hook system and not by the user.
func (m *Metrics) OnGroupManageError(err error) {
	labels := make(map[string]string)

	var kerr *kerr.Error
	if errors.As(err, &kerr) {
		labels["error_message"] = kerr.Message
	} else {
		labels["error_message"] = err.Error()
	}

	vm.GetOrCreateCounter(m.buildName("group_manage_error", labels)).Inc()
}

// OnProduceBatchWritten implements the [kgo.HookProduceBatchWritten] interface for metrics gathering.
// This method is meant to be called by the hook system and not by the user.
func (m *Metrics) OnProduceBatchWritten(meta kgo.BrokerMetadata, topic string, partition int32, metrics kgo.ProduceBatchMetrics) {
	labels := map[string]string{
		"node_id":   kgo.NodeName(meta.NodeID),
		"topic":     topic,
		"partition": strconv.FormatInt(int64(partition), 10),
	}

	vm.GetOrCreateCounter(m.buildName("produce_uncompressed_bytes_total", labels)).Add(metrics.UncompressedBytes)
	vm.GetOrCreateCounter(m.buildName("produce_compressed_bytes_total", labels)).Add(metrics.CompressedBytes)
	vm.GetOrCreateCounter(m.buildName("produce_batches_total", labels)).Inc()
	vm.GetOrCreateCounter(m.buildName("produce_records_total", labels)).Add(metrics.NumRecords)
}

// OnFetchBatchRead implements the [kgo.HookFetchBatchRead] interface for metrics gathering.
// This method is meant to be called by the hook system and not by the user.
func (m *Metrics) OnFetchBatchRead(meta kgo.BrokerMetadata, topic string, partition int32, metrics kgo.FetchBatchMetrics) {
	labels := map[string]string{
		"node_id":   kgo.NodeName(meta.NodeID),
		"topic":     topic,
		"partition": strconv.FormatInt(int64(partition), 10),
	}

	vm.GetOrCreateCounter(m.buildName("fetch_uncompressed_bytes_total", labels)).Add(metrics.UncompressedBytes)
	vm.GetOrCreateCounter(m.buildName("fetch_compressed_bytes_total", labels)).Add(metrics.CompressedBytes)
	vm.GetOrCreateCounter(m.buildName("fetch_batches_total", labels)).Inc()
	vm.GetOrCreateCounter(m.buildName("fetch_records_total", labels)).Add(metrics.NumRecords)
}

// buildName constructs a metric name for the VictoriaMetrics metrics library.
//
// The library expects the user to create a metric for each and every variation of a metric
// by providing the full name, including labels: there is no equivalent to the *Vec variants
// in the official Prometheus client.
//
// This function is a helper to build such a name, taking care of properly adding
// the namespace if present, subsystem if present and labels if present.
func (m *Metrics) buildName(name string, labels map[string]string) string {
	var builder strings.Builder

	if m.cfg.namespace != "" {
		builder.WriteString(m.cfg.namespace + "_")
	}
	if m.cfg.subsystem != "" {
		builder.WriteString(m.cfg.subsystem + "_")
	}
	builder.WriteString(name)

	labelNames := slices.Sorted(maps.Keys(labels))

	if len(labels) > 0 {
		builder.WriteRune('{')
		for i, name := range slices.Sorted(maps.Keys(labels)) {
			value := labels[name]

			builder.WriteString(name)
			builder.WriteRune('=')
			builder.WriteString(strconv.Quote(value))
			if i+1 < len(labelNames) {
				builder.WriteRune(',')
			}
		}
		builder.WriteRune('}')
	}

	return builder.String()
}
