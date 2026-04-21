package kprom

import (
	"net"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestBrokerLabels(t *testing.T) {
	rack := "us-east-1a"
	meta := kgo.BrokerMetadata{NodeID: 1, Host: "broker1.example.com", Port: 9092, Rack: &rack}

	t.Run("no_labels", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		m := NewMetrics("test",
			Registry(reg),
			BrokerLabels(),
		)

		cl, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:19092"),
			kgo.WithHooks(m),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()

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
					switch lp.GetName() {
					case "node_id", "host", "rack":
						t.Errorf("unexpected label %s on metric %s", lp.GetName(), mf.GetName())
					}
				}
			}
		}

		assertMetricValue(t, mfs, "test_connects_total", 1)
		assertMetricValue(t, mfs, "test_write_bytes_total", 100)
		assertMetricValue(t, mfs, "test_read_bytes_total", 200)
	})

	t.Run("host_only", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		m := NewMetrics("test",
			Registry(reg),
			BrokerLabels(BrokerHost),
		)

		cl, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:19092"),
			kgo.WithHooks(m),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()

		m.OnBrokerConnect(meta, time.Millisecond, &net.TCPConn{}, nil)

		mfs, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}

		assertLabel(t, mfs, "test_connects_total", "host", "broker1.example.com")
		assertNoLabel(t, mfs, "test_connects_total", "node_id")
	})

	t.Run("node_id_and_rack", func(t *testing.T) {
		reg := prometheus.NewRegistry()
		m := NewMetrics("test",
			Registry(reg),
			BrokerLabels(BrokerNodeID, BrokerRack),
		)

		cl, err := kgo.NewClient(
			kgo.SeedBrokers("localhost:19092"),
			kgo.WithHooks(m),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()

		m.OnBrokerConnect(meta, time.Millisecond, &net.TCPConn{}, nil)

		mfs, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}

		assertLabel(t, mfs, "test_connects_total", "node_id", "1")
		assertLabel(t, mfs, "test_connects_total", "rack", "us-east-1a")
	})

	t.Run("default_node_id", func(t *testing.T) {
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

		m.OnBrokerConnect(meta, time.Millisecond, &net.TCPConn{}, nil)
		m.OnBrokerE2E(meta, 0, kgo.BrokerE2E{
			BytesWritten: 100,
			BytesRead:    200,
		})

		mfs, err := reg.Gather()
		if err != nil {
			t.Fatal(err)
		}

		assertLabel(t, mfs, "test_connects_total", "node_id", "1")
		assertNoLabel(t, mfs, "test_connects_total", "host")
		assertNoLabel(t, mfs, "test_connects_total", "rack")
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

func assertLabel(t *testing.T, mfs []*dto.MetricFamily, metricName, labelName, labelValue string) {
	t.Helper()
	for _, mf := range mfs {
		if mf.GetName() == metricName {
			for _, metric := range mf.GetMetric() {
				for _, lp := range metric.GetLabel() {
					if lp.GetName() == labelName {
						if lp.GetValue() != labelValue {
							t.Errorf("metric %s label %s: expected %q, got %q", metricName, labelName, labelValue, lp.GetValue())
						}
						return
					}
				}
			}
			t.Errorf("metric %s: label %s not found", metricName, labelName)
			return
		}
	}
	t.Errorf("metric %s not found", metricName)
}

func assertNoLabel(t *testing.T, mfs []*dto.MetricFamily, metricName, labelName string) {
	t.Helper()
	for _, mf := range mfs {
		if mf.GetName() == metricName {
			for _, metric := range mf.GetMetric() {
				for _, lp := range metric.GetLabel() {
					if lp.GetName() == labelName {
						t.Errorf("metric %s: unexpected label %s=%s", metricName, labelName, lp.GetValue())
						return
					}
				}
			}
			return
		}
	}
}
