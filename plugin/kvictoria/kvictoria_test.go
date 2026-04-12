package kvictoria

import (
	"strings"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestMetricsBuildName(t *testing.T) {
	testCases := []struct {
		namespace string
		subsystem string
		name      string
		labels    map[string]string
		exp       string
	}{
		// No labels
		{
			namespace: "foo",
			subsystem: "bar",
			name:      "records_total",
			labels:    nil,
			exp:       "foo_bar_records_total",
		},
		{
			namespace: "foo",
			subsystem: "",
			name:      "records_total",
			labels:    nil,
			exp:       "foo_records_total",
		},
		{
			namespace: "",
			subsystem: "bar",
			name:      "records_total",
			labels:    nil,
			exp:       "bar_records_total",
		},
		{
			namespace: "",
			subsystem: "",
			name:      "records_total",
			labels:    nil,
			exp:       "records_total",
		},
		// With labels
		{
			namespace: "foo",
			subsystem: "bar",
			name:      "records_total",
			labels:    map[string]string{"client_id": "kgo", "node_id": "1"},
			exp:       `foo_bar_records_total{client_id="kgo",node_id="1"}`,
		},
		{
			namespace: "foo",
			subsystem: "",
			name:      "records_total",
			labels:    map[string]string{"client_id": "kgo", "node_id": "1"},
			exp:       `foo_records_total{client_id="kgo",node_id="1"}`,
		},
		{
			namespace: "",
			subsystem: "bar",
			name:      "records_total",
			labels:    map[string]string{"client_id": "kgo", "node_id": "1"},
			exp:       `bar_records_total{client_id="kgo",node_id="1"}`,
		},
		{
			namespace: "",
			subsystem: "",
			name:      "records_total",
			labels:    map[string]string{"client_id": "kgo", "node_id": "1"},
			exp:       `records_total{client_id="kgo",node_id="1"}`,
		},
	}

	for _, tc := range testCases {
		t.Run("", func(t *testing.T) {
			m := &Metrics{
				cfg: cfg{
					namespace: tc.namespace,
					subsystem: tc.subsystem,
				},
			}

			name := m.buildName(tc.name, tc.labels)
			if tc.exp != name {
				t.Errorf("expected metric name %v, got %v", tc.exp, name)
			}
		})
	}
}

func TestBrokerLabels(t *testing.T) {
	rack := "us-east-1a"
	meta := kgo.BrokerMetadata{NodeID: 1, Host: "broker1.example.com", Port: 9092, Rack: &rack}

	t.Run("no_labels", func(t *testing.T) {
		m := &Metrics{
			cfg:      newCfg("test", BrokerLabels()),
			clientID: "kgo",
		}

		labels := m.brokerLabels(meta)
		for _, key := range []string{"node_id", "host", "rack"} {
			if _, ok := labels[key]; ok {
				t.Errorf("unexpected label %s", key)
			}
		}
		if labels["client_id"] != "kgo" {
			t.Errorf("expected client_id=kgo, got %s", labels["client_id"])
		}

		name := m.buildName("read_errors_total", labels)
		if strings.Contains(name, "node_id") || strings.Contains(name, "host") || strings.Contains(name, "rack") {
			t.Errorf("expected no broker labels in metric name, got %s", name)
		}
	})

	t.Run("host_only", func(t *testing.T) {
		m := &Metrics{
			cfg:      newCfg("test", BrokerLabels(BrokerHost)),
			clientID: "kgo",
		}

		labels := m.brokerLabels(meta)
		if labels["host"] != "broker1.example.com" {
			t.Errorf("expected host=broker1.example.com, got %s", labels["host"])
		}
		if _, ok := labels["node_id"]; ok {
			t.Error("unexpected node_id label")
		}
	})

	t.Run("node_id_and_rack", func(t *testing.T) {
		m := &Metrics{
			cfg:      newCfg("test", BrokerLabels(BrokerNodeID, BrokerRack)),
			clientID: "kgo",
		}

		labels := m.brokerLabels(meta)
		if labels["node_id"] != "1" {
			t.Errorf("expected node_id=1, got %s", labels["node_id"])
		}
		if labels["rack"] != "us-east-1a" {
			t.Errorf("expected rack=us-east-1a, got %s", labels["rack"])
		}
		if _, ok := labels["host"]; ok {
			t.Error("unexpected host label")
		}
	})

	t.Run("default_node_id", func(t *testing.T) {
		m := &Metrics{
			cfg:      newCfg("test"),
			clientID: "kgo",
		}

		labels := m.brokerLabels(meta)
		if labels["node_id"] != "1" {
			t.Errorf("expected node_id=1, got %s", labels["node_id"])
		}
		if _, ok := labels["host"]; ok {
			t.Error("unexpected host label")
		}
		if _, ok := labels["rack"]; ok {
			t.Error("unexpected rack label")
		}

		name := m.buildName("read_errors_total", labels)
		if !strings.Contains(name, `node_id="1"`) {
			t.Errorf("expected node_id in metric name, got %s", name)
		}
	})
}
