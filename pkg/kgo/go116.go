//go:build go1.16 && !go1.15
// +build go1.16,!go1.15

package kgo

import (
	"errors"
	"net"
)

func isNetClosedErr(err error) bool {
	return errors.Is(err, net.ErrClosed)
}
