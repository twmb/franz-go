package sticky

import (
	"testing"

	"github.com/twmb/kgo/kmsg"
)

type udBuilder struct {
	version     int
	generation  int
	assignments map[string][]int32
}

func oldUD() *udBuilder {
	return &udBuilder{
		version: 0,
	}
}

func newUD() *udBuilder {
	return &udBuilder{
		version:    1,
		generation: 1,
	}
}

func (b *udBuilder) assign(topic string, partitions ...int32) *udBuilder {
	if b.assignments == nil {
		b.assignments = make(map[string][]int32)
	}
	b.assignments[topic] = partitions
	return b
}

func (b *udBuilder) encode() []byte {
	return udEncode(b.version, b.generation, b.assignments)
}

func udEncode(version, generation int, assignments map[string][]int32) []byte {
	switch version {
	case 0:
		v0 := new(kmsg.StickyMemberMetadataV0)
		for topic, partitions := range assignments {
			v0.CurrentAssignment = append(v0.CurrentAssignment, kmsg.StickyMemberMetadataV0CurrentAssignment{
				topic,
				partitions,
			})
		}
		return v0.AppendTo(nil)
	case 1:
		v1 := &kmsg.StickyMemberMetadataV1{
			Generation: int32(generation),
		}
		for topic, partitions := range assignments {
			v1.CurrentAssignment = append(v1.CurrentAssignment, kmsg.StickyMemberMetadataV1CurrentAssignment{
				topic,
				partitions,
			})
		}
		return v1.AppendTo(nil)
	}
	return nil
}

func testEqualDivvy(t *testing.T, plan Plan) {
	t.Helper()

	min := 1 << 31
	max := 0
	for _, topics := range plan {
		assigned := 0
		for _, partitions := range topics {
			assigned += len(partitions)
		}
		if assigned < min {
			min = assigned
		}
		if assigned > max {
			max = assigned
		}
	}
	if max-min > 1 {
		t.Errorf("plan not equally divvied, min assigned %d; max %d", min, max)
	}
}
