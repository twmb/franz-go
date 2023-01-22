package kotel

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewConfig(t *testing.T) {
	meter := NewMeter()
	testCases := []struct {
		name string
		opts []Opt
		want *Kotel
	}{
		{
			name: "Empty",
			opts: []Opt{},
			want: &Kotel{},
		},
		{
			name: "WithMeter",
			opts: []Opt{WithMeter(meter)},
			want: &Kotel{
				meter: meter,
			},
		},
		{
			name: "WithTracer",
			opts: []Opt{WithTracer(NewTracer())},
			want: &Kotel{
				tracer: NewTracer(),
			},
		},
		{
			name: "WithMeter and WithTracer",
			opts: []Opt{WithMeter(meter), WithTracer(NewTracer())},
			want: &Kotel{
				meter:  meter,
				tracer: NewTracer(),
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
