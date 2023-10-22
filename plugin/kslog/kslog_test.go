package kslog_test

import (
	"log/slog"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kslog"
)

func ExampleNew() {
	l := kslog.New(slog.Default())

	l.Log(kgo.LogLevelInfo, "test message", "test-key", "test-val")
	// Output:
}
