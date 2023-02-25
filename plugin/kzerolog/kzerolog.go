// Package kzerolog provides a plug-in kgo.Logger wrapping zerolog.Logger for usage in
// a kgo.Client.
//
// This can be used like so:
//
//	cl, err := kgo.NewClient(
//	        kgo.WithLogger(kzerolog.New(logger)),
//	        // ...other opts
//	)
package kzerolog

import (
	"github.com/rs/zerolog"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Logger provides the kgo.Logger interface for usage in kgo.WithLogger when
// initializing a client.
type Logger struct {
	zl *zerolog.Logger
}

// New returns a new logger.
func New(zl *zerolog.Logger) *Logger {
	return &Logger{zl}
}

// Level is for the kgo.Logger interface.
func (l *Logger) Level() kgo.LogLevel {
	level := l.zl.GetLevel()
	switch level {
	case zerolog.ErrorLevel, zerolog.PanicLevel, zerolog.FatalLevel:
		return kgo.LogLevelError
	case zerolog.WarnLevel:
		return kgo.LogLevelWarn
	case zerolog.InfoLevel:
		return kgo.LogLevelInfo
	case zerolog.TraceLevel, zerolog.DebugLevel:
		return kgo.LogLevelDebug
	case zerolog.NoLevel, zerolog.Disabled:
		return kgo.LogLevelNone
	default:
		return kgo.LogLevelNone
	}
}

// Log is for the kgo.Logger interface.
func (l *Logger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	l.zl.WithLevel(logLevelToZerolog(level)).Fields(keyvals).Msg(msg)
}

func logLevelToZerolog(level kgo.LogLevel) zerolog.Level {
	switch level {
	case kgo.LogLevelError:
		return zerolog.ErrorLevel
	case kgo.LogLevelWarn:
		return zerolog.WarnLevel
	case kgo.LogLevelInfo:
		return zerolog.InfoLevel
	case kgo.LogLevelDebug:
		return zerolog.DebugLevel
	case kgo.LogLevelNone:
		return zerolog.Disabled
	default:
		// Follows the default of zerolog.New of defaulting to TraceLevel.
		return zerolog.TraceLevel
	}
}
