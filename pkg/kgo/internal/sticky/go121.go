package sticky

// TODO https://github.com/golang/go/issues/58554
// TODO We likely need to gate this file behind go1.21 rather than 1.20, any
// non-patched 1.20 version will fail to compile.

//
// import (
// 	"math"
// 	"unsafe"
// )
//
// func (b *balancer) partitionConsumersByGenerationSlice() []memberGeneration {
// 	return unsafe.Slice((*memberGeneration)(unsafe.Pointer(unsafe.SliceData(b.partOwners))), b.nparts)
// }
//
// func (b *balancer) partitionConsumersSlice() []partitionConsumer {
// 	if b.nparts == 0 {
// 		return nil
// 	}
// 	space := b.partOwners[b.nparts : b.nparts*2]
// 	for i := range space {
// 		space[i] = math.MaxUint32
// 	}
// 	return unsafe.Slice((*partitionConsumer)(unsafe.Pointer(unsafe.SliceData(space))), b.nparts)
// }
//
// func (b *balancer) topicPotentialsBufSlice() []uint16 {
// 	l := len(b.topicNums) * len(b.members)
// 	if l == 0 {
// 		return nil
// 	}
// 	space := b.partOwners[b.nparts*2 : cap(b.partOwners)]
// 	return unsafe.Slice((*uint16)(unsafe.Pointer(unsafe.SliceData(space))), l)
// }
