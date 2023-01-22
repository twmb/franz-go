//go:build go1.16
// +build go1.16

package kgo

import (
	"errors"
	"net"
)

func isNetClosedErr(err error) bool {
	return errors.Is(err, net.ErrClosed)
}
