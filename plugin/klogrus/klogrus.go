package klogrus

import (
	"github.com/sirupsen/logrus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Logger provides the kgo.Logger interface for usage in kgo.WithLogger when
// initializing a client.
type Logger struct {
	lr *logrus.Logger
}

// New returns a new Logger.
func New(lr *logrus.Logger) *Logger {
	return &Logger{lr}
}

// Level is for the kgo.Logger interface.
func (l *Logger) Level() kgo.LogLevel {
	return logrusToKgoLevel(l.lr.GetLevel())
}

// Log is for the kgo.Logger interface.
func (l *Logger) Log(level kgo.LogLevel, msg string, keyvals ...any) {
	logrusLevel, levelMatched := kgoToLogrusLevel(level)
	if levelMatched {
		var fields logrus.Fields
		for i := 0; i < len(keyvals); i += 2 {
			fields[keyvals[i].(string)] = keyvals[i+1]
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
