//go:build !go1.19
// +build !go1.19

package kgo

import "sync/atomic"

type atomicBool uint32

func (b *atomicBool) Store(v bool) {
	if v {
		atomic.StoreUint32((*uint32)(b), 1)
	} else {
		atomic.StoreUint32((*uint32)(b), 0)
	}
}

func (b *atomicBool) Load() bool { return atomic.LoadUint32((*uint32)(b)) == 1 }

func (b *atomicBool) Swap(v bool) bool {
	var swap uint32
	if v {
		swap = 1
	}
	return atomic.SwapUint32((*uint32)(b), swap) == 1
}
