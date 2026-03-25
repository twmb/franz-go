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

func TestBrokerNodeLabel(t *testing.T) {
	meta := kgo.BrokerMetadata{NodeID: 1, Host: "localhost", Port: 9092}

	t.Run("without_node_id", func(t *testing.T) {
		m := &Metrics{
			cfg:      newCfg("test", BrokerNodeLabel(false)),
			clientID: "kgo",
		}

		labels := m.brokerLabels(meta)
		if _, ok := labels["node_id"]; ok {
			t.Error("expected no node_id label")
		}
		if labels["client_id"] != "kgo" {
			t.Errorf("expected client_id=kgo, got %s", labels["client_id"])
		}

		name := m.buildName("read_errors_total", labels)
		if strings.Contains(name, "node_id") {
			t.Errorf("expected no node_id in metric name, got %s", name)
		}
	})

	t.Run("with_node_id_default", func(t *testing.T) {
		m := &Metrics{
			cfg:      newCfg("test"),
			clientID: "kgo",
		}

		labels := m.brokerLabels(meta)
		if labels["node_id"] != "1" {
			t.Errorf("expected node_id=1, got %s", labels["node_id"])
		}

		name := m.buildName("read_errors_total", labels)
		if !strings.Contains(name, `node_id="1"`) {
			t.Errorf("expected node_id in metric name, got %s", name)
		}
	})
}
