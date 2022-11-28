// Package klogr provides a plug-in kgo.Logger wrapping go-logr for usage in
// a kgo.Client.
//
// This can be used like so:
//
//	cl, err := kgo.NewClient(
//	        kgo.WithLogger(klogr.New(logger)),
//	        // ...other opts
//	)
package klogr

import (
	"github.com/go-logr/logr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Logger provides the kgo.Logger interface for usage in kgo.WithLogger when
// initializing a client.
type Logger struct {
	logger *logr.Logger
}

// New returns a new logger.
func New(logger *logr.Logger) *Logger {
	return &Logger{logger}
}

// Level sets a static level for the kgo.Logger Level function.
func Level(level kgo.LogLevel) Opt {
	return LevelFn(func() kgo.LogLevel { return level })
}

// Level is for the kgo.Logger interface.
func (l *Logger) Level() kgo.LogLevel {
	return l.levelFn()
}

// Log is for the kgo.Logger interface.
func (l *Logger) Log(level kgo.LogLevel, msg string, keyvals ...interface{}) {
	l.zl.WithLevel(logLevelToZerolog(level)).Fields(keyvals).Msg(msg)
}
