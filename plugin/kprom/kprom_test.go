package kprom

import (
	"net"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestBrokerNodeLabel(t *testing.T) {
	t.Run("without_node_id", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		m := NewMetrics("test",
			Registry(reg),
			BrokerNodeLabel(false),
		)

		cl, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:19092"),
			kgo.WithHooks(m),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()

		meta := kgo.BrokerMetadata{NodeID: 1, Host: "localhost", Port: 9092}
		m.OnBrokerConnect(meta, time.Millisecond, &net.TCPConn{}, nil)
		m.OnBrokerE2E(meta, 0, kgo.BrokerE2E{
			BytesWritten: 100,
			BytesRead:    200,
		})

		mfs, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}

		for _, mf := range mfs {
			for _, metric := range mf.GetMetric() {
				for _, lp := range metric.GetLabel() {
					if lp.GetName() == "node_id" {
						t.Errorf("unexpected node_id label on metric %s", mf.GetName())
					}
				}
			}
		}

		assertMetricValue(t, mfs, "test_connects_total", 1)
		assertMetricValue(t, mfs, "test_write_bytes_total", 100)
		assertMetricValue(t, mfs, "test_read_bytes_total", 200)
	})

	t.Run("with_node_id_default", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		m := NewMetrics("test",
			Registry(reg),
		)

		cl, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:19092"),
			kgo.WithHooks(m),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()

		meta := kgo.BrokerMetadata{NodeID: 1, Host: "localhost", Port: 9092}
		m.OnBrokerConnect(meta, time.Millisecond, &net.TCPConn{}, nil)
		m.OnBrokerE2E(meta, 0, kgo.BrokerE2E{
			BytesWritten: 100,
			BytesRead:    200,
		})

		mfs, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}

		foundNodeID := false
		for _, mf := range mfs {
			for _, metric := range mf.GetMetric() {
				for _, lp := range metric.GetLabel() {
					if lp.GetName() == "node_id" {
						foundNodeID = true
						if lp.GetValue() != "1" {
							t.Errorf("expected node_id=1, got node_id=%s", lp.GetValue())
						}
					}
				}
			}
		}
		if !foundNodeID {
			t.Error("expected node_id label on broker-level metrics")
		}

		assertMetricValue(t, mfs, "test_connects_total", 1)
		assertMetricValue(t, mfs, "test_write_bytes_total", 100)
		assertMetricValue(t, mfs, "test_read_bytes_total", 200)
	})
}

func assertMetricValue(t *testing.T, mfs []*dto.MetricFamily, name string, expected float64) {
	t.Helper()
	for _, mf := range mfs {
		if mf.GetName() == name {
			for _, metric := range mf.GetMetric() {
				if c := metric.GetCounter(); c != nil {
					if c.GetValue() != expected {
						t.Errorf("metric %s: expected %v, got %v", name, expected, c.GetValue())
					}
					return
				}
			}
		}
	}
	t.Errorf("metric %s not found", name)
}
