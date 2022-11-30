//go:build go1.19
// +build go1.19

package kgo

import "sync/atomic"

type atomicBool struct {
	atomic.Bool
}
