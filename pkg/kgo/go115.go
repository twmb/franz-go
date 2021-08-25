//go:build go1.15
// +build go1.15

package kgo

import "strings"

func isNetClosedErr(err error) bool {
	return strings.Contains(err.Error(), "use of closed network connection")
}
