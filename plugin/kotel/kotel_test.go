package kotel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewConfig(t *testing.T) {
	meter := NewMeter()
	testCases := []struct {
		name string
		opts []Option
		want *Kotel
	}{
		{
			name: "Empty",
			opts: []Option{},
			want: &Kotel{},
		},
		{
			name: "WithMetrics",
			opts: []Option{WithMetrics(meter)},
			want: &Kotel{
				Meter: meter,
			},
		},
		{
			name: "WithTracing",
			opts: []Option{WithTracing(NewTracer())},
			want: &Kotel{
				Tracer: NewTracer(),
			},
		},
		{
			name: "WithMetrics and WithTracing",
			opts: []Option{WithMetrics(meter), WithTracing(NewTracer())},
			want: &Kotel{
				Meter:  meter,
				Tracer: NewTracer(),
			},
		},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			result := NewKotel(tc.opts...)
			assert.Equal(t, tc.want, result)
		})
	}
}
