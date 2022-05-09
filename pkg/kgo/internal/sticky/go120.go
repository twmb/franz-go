package sticky

import (
	"math"
	"reflect"
	"unsafe"
)

func (b *balancer) partitionConsumersByGenerationSlice() []memberGeneration {
	var partitionConsumersByGeneration []memberGeneration
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&partitionConsumersByGeneration)) //nolint:gosec // known way to convert string to slice
	hdr.Data = ((*reflect.SliceHeader)(unsafe.Pointer(&b.partOwners))).Data        //nolint:gosec // known way to convert string to slice
	hdr.Len = b.nparts
	hdr.Cap = b.nparts
	return partitionConsumersByGeneration
}

func (b *balancer) partitionConsumersSlice() []partitionConsumer {
	if b.nparts == 0 {
		return nil
	}
	space := b.partOwners[b.nparts : b.nparts*2]
	for i := range space {
		space[i] = math.MaxUint32
	}
	var partitionConsumers []partitionConsumer
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&partitionConsumers)) //nolint:gosec // known way to convert string to slice
	hdr.Data = ((*reflect.SliceHeader)(unsafe.Pointer(&space))).Data   //nolint:gosec // known way to convert string to slice
	hdr.Len = b.nparts
	hdr.Cap = b.nparts
	return partitionConsumers
}

func (b *balancer) topicPotentialsBufSlice() []uint16 {
	l := len(b.topicNums) * len(b.members)
	if l == 0 {
		return nil
	}
	space := b.partOwners[b.nparts*2 : cap(b.partOwners)]
	var topicPotentialsBuf []uint16
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&topicPotentialsBuf)) //nolint:gosec // known way to convert string to slice
	hdr.Data = ((*reflect.SliceHeader)(unsafe.Pointer(&space))).Data   //nolint:gosec // known way to convert string to slice
	hdr.Len = l
	hdr.Cap = l
	return topicPotentialsBuf
}
