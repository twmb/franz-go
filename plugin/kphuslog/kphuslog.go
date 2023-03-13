// Package kphuslog provides a plug-in kgo.Logger wrapping zerolog.Logger for usage in
// a kgo.Client.
//
// This can be used like so:
//
//	cl, err := kgo.NewClient(
//	        kgo.WithLogger(kphuslog.New(logger)),
//	        // ...other opts
//	)
package kphuslog

import (
	"github.com/phuslu/log"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Logger provides the kgo.Logger interface for usage in kgo.WithLogger when
// initializing a client.
type Logger struct {
	pl *log.Logger
}

// New returns a new logger.
func New(pl *log.Logger) *Logger {
	return &Logger{pl}
}

// Level is for the kgo.Logger interface.
func (l *Logger) Level() kgo.LogLevel {
	level := l.pl.Level
	switch level {
	case log.ErrorLevel, log.PanicLevel, log.FatalLevel:
		return kgo.LogLevelError
	case log.WarnLevel:
		return kgo.LogLevelWarn
	case log.InfoLevel:
		return kgo.LogLevelInfo
	case log.TraceLevel, log.DebugLevel:
		return kgo.LogLevelDebug
	default:
		return kgo.LogLevelNone
	}
}

// Log is for the kgo.Logger interface.
func (l *Logger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	l.pl.WithLevel(logLevelToPhuslog(level)).KeysAndValues(keyvals...).Msg(msg)
}

func logLevelToPhuslog(level kgo.LogLevel) log.Level {
	switch level {
	case kgo.LogLevelError:
		return log.ErrorLevel
	case kgo.LogLevelWarn:
		return log.WarnLevel
	case kgo.LogLevelInfo:
		return log.InfoLevel
	case kgo.LogLevelDebug:
		return log.DebugLevel
	case kgo.LogLevelNone:
		return log.Level(8) // noLevel
	default:
		return log.Level(8) // noLevel
	}
}
