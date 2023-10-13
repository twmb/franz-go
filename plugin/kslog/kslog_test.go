package kslog

import (
	"log/slog"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
)

func TestNew(_ *testing.T) {
	l := New(slog.Default())

	l.Log(kgo.LogLevelInfo, "test message", "test-key", "test-val")
}
