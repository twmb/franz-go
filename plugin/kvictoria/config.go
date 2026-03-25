package kvictoria

type cfg struct {
	namespace      string
	subsystem      string
	brokerNodeLabel bool
}

func newCfg(namespace string, opts ...Opt) cfg {
	cfg := cfg{
		namespace:      namespace,
		brokerNodeLabel: true,
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

// BrokerNodeLabel controls whether the "node_id" label is included on
// broker-level and batch metrics. By default node_id is included. Passing
// false removes the label, aggregating metrics across all brokers. This is
// useful for reducing metrics cardinality in environments with many or
// frequently changing broker IDs.
func BrokerNodeLabel(include bool) Opt {
	return opt{func(c *cfg) { c.brokerNodeLabel = include }}
}
