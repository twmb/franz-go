package klogrus

import (
	"fmt"

	"github.com/sirupsen/logrus"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	_ FieldLogger = (*logrus.Logger)(nil)
	_ kgo.Logger  = (*Logger)(nil)
)

// FieldLogger interface combines logrus.FieldLogger with GetLevel method
// useful to represent a wrapper around *logrus.Logger.
type FieldLogger interface {
	logrus.FieldLogger
	GetLevel() logrus.Level
}

// Logger provides the kgo.Logger interface for usage in kgo.WithLogger when
// initializing a client.
type Logger struct {
	lr FieldLogger
}

// New returns a new Logger using a *logrus.Logger instance.
func New(lr *logrus.Logger) *Logger {
	return &Logger{lr}
}

// NewFieldLogger returns a new Logger using a FieldLogger interface.
// it is isofunctional with New constructor, except it can accept either
// *logrus.Logger or a possible wrapper that implements logrus.FieldLogger
// and includes GetLevel method.
func NewFieldLogger(fl FieldLogger) *Logger {
	return &Logger{fl}
}

// Level is for the kgo.Logger interface.
func (l *Logger) Level() kgo.LogLevel {
	return logrusToKgoLevel(l.lr.GetLevel())
}

// Log is for the kgo.Logger interface.
func (l *Logger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	logrusLevel, levelMatched := kgoToLogrusLevel(level)
	if levelMatched {
		fields := make(logrus.Fields, len(keyvals)/2)
		for i := 0; i < len(keyvals); i += 2 {
			fields[fmt.Sprint(keyvals[i])] = keyvals[i+1]
		}
		l.lr.WithFields(fields).Log(logrusLevel, msg)
	}
}

func kgoToLogrusLevel(level kgo.LogLevel) (logrus.Level, bool) {
	switch level {
	case kgo.LogLevelError:
		return logrus.ErrorLevel, true
	case kgo.LogLevelWarn:
		return logrus.WarnLevel, true
	case kgo.LogLevelInfo:
		return logrus.InfoLevel, true
	case kgo.LogLevelDebug:
		return logrus.DebugLevel, true
	case kgo.LogLevelNone:
		return logrus.TraceLevel, false
	}
	return logrus.TraceLevel, false
}

func logrusToKgoLevel(level logrus.Level) kgo.LogLevel {
	switch level {
	case logrus.PanicLevel, logrus.FatalLevel, logrus.ErrorLevel:
		return kgo.LogLevelError
	case logrus.WarnLevel:
		return kgo.LogLevelWarn
	case logrus.InfoLevel:
		return kgo.LogLevelInfo
	case logrus.DebugLevel, logrus.TraceLevel:
		return kgo.LogLevelDebug
	default:
		return kgo.LogLevelNone
	}
}
