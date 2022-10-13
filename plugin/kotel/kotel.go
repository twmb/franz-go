package kotel

import (
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	instrumentationName = "github.com/twmb/franz-go/plugin/kotel"
)

// Kotel represents the configuration options available for the kotel plugin
type Kotel struct {
	Meter  *Meter
	Tracer *Tracer
}

// Option interface used for setting optional kotel properties.
type Option interface {
	apply(*Kotel)
}

type optionFunc func(*Kotel)

func (o optionFunc) apply(c *Kotel) {
	o(c)
}

// NewKotel creates a new Kotel struct and applies opts to it.
func NewKotel(opts ...Option) *Kotel {
	k := &Kotel{}
	for _, opt := range opts {
		opt.apply(k)
	}
	return k
}

// WithTracing configures Kotel with a Tracer
func WithTracing(t *Tracer) Option {
	return optionFunc(func(k *Kotel) {
		if t != nil {
			k.Tracer = t
		}
	})
}

// WithMetrics configures Kotel with a Meter
func WithMetrics(m *Meter) Option {
	return optionFunc(func(k *Kotel) {
		if m != nil {
			k.Meter = m
		}
	})
}

// Hooks return a list of kgo.hooks compatible with its interface
func (k *Kotel) Hooks() []kgo.Hook {
	var hooks []kgo.Hook
	if k.Tracer != nil {
		hooks = append(hooks, k.Tracer)
	}
	if k.Meter != nil {
		hooks = append(hooks, k.Meter)
	}
	return hooks
}
