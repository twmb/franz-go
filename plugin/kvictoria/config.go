package kvictoria

type cfg struct {
	namespace      string
	subsystem      string
	brokerLabelSet map[BrokerLabel]bool
}

func newCfg(namespace string, opts ...Opt) cfg {
	cfg := cfg{
		namespace:      namespace,
		brokerLabelSet: map[BrokerLabel]bool{BrokerNodeID: true},
	}

	for _, opt := range opts {
		opt.apply(&cfg)
	}

	return cfg
}

// Opt is an option to configure Metrics.
type Opt interface {
	apply(*cfg)
}

type opt struct{ fn func(*cfg) }

func (o opt) apply(c *cfg) { o.fn(c) }

// Subsystem sets the subsystem for the metrics, overriding the default empty string.
func Subsystem(ss string) Opt {
	return opt{func(c *cfg) { c.subsystem = ss }}
}

// A BrokerLabel is a label that can be set on broker-level metrics.
type BrokerLabel uint8

const (
	BrokerNodeID BrokerLabel = iota // Include "node_id" label on broker metrics.
	BrokerHost                      // Include "host" label on broker metrics.
	BrokerRack                      // Include "rack" label on broker metrics.
)

// BrokerLabels configures which labels are included on broker-level
// connection, read, write, request, and batch metrics, overriding the
// default of (BrokerNodeID). Calling with no arguments removes all
// broker-level labels, aggregating metrics across all brokers. This is
// useful for reducing metrics cardinality in environments with many or
// frequently changing broker IDs.
func BrokerLabels(labels ...BrokerLabel) Opt {
	return opt{func(c *cfg) {
		c.brokerLabelSet = make(map[BrokerLabel]bool)
		for _, l := range labels {
			c.brokerLabelSet[l] = true
		}
	}}
}
