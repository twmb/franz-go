// Package klogr is a wrapper around logr.Logger.
package klogr

import (
	"github.com/go-logr/logr"
	"github.com/twmb/franz-go/pkg/kgo"
)

var _ kgo.Logger = (*Logger)(nil)

// Opt applies options to the logger.
type Opt interface{ apply(*Logger) }

type opt struct{ fn func(*Logger) }

func (o opt) apply(l *Logger) { o.fn(l) }

// LevelToV returns the logr V level that corresponds to the given
// kgo.LogLevel.
//
// By default, the kgo.LogLevel is just doubled -- error is 2, warn is 4, info
// is 6, and debug is 8.
func LevelToV(fn func(kgo.LogLevel) int) Opt {
	return opt{func(l *Logger) { l.levelMap = fn }}
}

// Logger that wraps the logr.Logger.
type Logger struct {
	logr     logr.Logger
	levelMap func(kgo.LogLevel) int
}

// New returns a new Logger.
func New(logr logr.Logger, opts ...Opt) *Logger {
	l := &Logger{
		logr:     logr,
		levelMap: func(level kgo.LogLevel) int { return int(level) * 2 },
	}
	for _, opt := range opts {
		opt.apply(l)
	}
	return l
}

// Level returns the log level to log at.
func (l *Logger) Level() kgo.LogLevel {
	// The kgo library really only checks the level ahead of time for debug
	// logs currently.
	for _, level := range []kgo.LogLevel{
		kgo.LogLevelDebug,
		kgo.LogLevelInfo,
		kgo.LogLevelWarn,
		kgo.LogLevelError,
	} {
		if l.logr.GetSink().Enabled(l.levelMap(level)) {
			return level
		}
	}
	return kgo.LogLevelNone
}

// Log using the underlying logr.Logger. If kgo.LogLevelError is set, keyvals
// will be type checked for an error, and the first one found will be used.
func (l *Logger) Log(level kgo.LogLevel, msg string, keyvals ...interface{}) {
	switch level {
	case kgo.LogLevelNone:
	case kgo.LogLevelError:
		var err error
		for _, kv := range keyvals {
			kverr, ok := kv.(error)
			if ok {
				err = kverr
				break
			}
		}
		l.logr.Error(err, msg, keyvals...)

	default:
		l.logr.V(l.levelMap(level)).Info(msg, keyvals...)
	}
}
