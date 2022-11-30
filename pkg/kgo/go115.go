//go:build !go1.16
// +build !go1.16

package kgo

import "strings"

func isNetClosedErr(err error) bool {
	return strings.Contains(err.Error(), "use of closed network connection")
}
