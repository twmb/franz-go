package kfake

// Opt is an option to configure a client.
type Opt interface {
	apply(*cfg)
}

type opt struct{ fn func(*cfg) }

func (opt opt) apply(cfg *cfg) { opt.fn(cfg) }

type cfg struct {
	nbrokers        int
	ports           []int
	logger          Logger
	clusterID       string
	allowAutoTopic  bool
	defaultNumParts int
}

// NumBrokers sets the number of brokers to start in the fake cluster.
func NumBrokers(n int) Opt {
	return opt{func(cfg *cfg) { cfg.nbrokers = n }}
}

// Ports sets the ports to listen on, overriding randomly choosing NumBrokers
// amount of ports.
func Ports(ports ...int) Opt {
	return opt{func(cfg *cfg) { cfg.ports = ports }}
}

// WithLogger sets the logger to use.
func WithLogger(logger Logger) Opt {
	return opt{func(cfg *cfg) { cfg.logger = logger }}
}

// ClusterID sets the cluster ID to return in metadata responses.
func ClusterID(clusterID string) Opt {
	return opt{func(cfg *cfg) { cfg.clusterID = clusterID }}
}

// AllowAutoTopicCreation allows metadata requests to create topics if the
// metadata request has its AllowAutoTopicCreation field set to true.
func AllowAutoTopicCreation() Opt {
	return opt{func(cfg *cfg) { cfg.allowAutoTopic = true }}
}

// DefaultNumPartitions sets the number of partitions to create by default for
// auto created topics / CreateTopics with -1 partitions.
func DefaultNumPartitions(n int) Opt {
	return opt{func(cfg *cfg) { cfg.defaultNumParts = n }}
}
