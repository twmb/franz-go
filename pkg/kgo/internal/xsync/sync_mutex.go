//go:build !synctests

package xsync

import "sync"

type Mutex = sync.Mutex
type RWMutex = sync.RWMutex
