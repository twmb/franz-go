// Package kzap provides a plug-in kgo.Logger wrapping uber's zap for usage in
// a kgo.Client.
//
// This can be used like so:
//
//     cl, err := kgo.NewClient(
//             kgo.WithLogger(kzap.New(zapLogger)),
//             // ...other opts
//     )
//
// By default, the logger chooses the highest level possible that is enabled on
// the zap logger, and then sticks with that level forever. A variable level
// can be chosen by specifying the LevelFn option. See the documentation on
// Level or LevelFn for more info.
package kzap

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Logger provides the kgo.Logger interface for usage in kgo.WithLogger when
// initializing a client.
type Logger struct {
	zl *zap.Logger

	levelFn func() kgo.LogLevel
}

// New returns a new logger that by default forever logs at the highest level
// enabled in the zap logger.
func New(zl *zap.Logger, opts ...Opt) *Logger {
	static := kgo.LogLevelError
	switch {
	case zl.Core().Enabled(zapcore.DebugLevel):
		static = kgo.LogLevelDebug
	case zl.Core().Enabled(zapcore.InfoLevel):
		static = kgo.LogLevelInfo
	case zl.Core().Enabled(zapcore.WarnLevel):
		static = kgo.LogLevelWarn
	}
	l := &Logger{
		zl:      zl,
		levelFn: func() kgo.LogLevel { return static },
	}
	for _, opt := range opts {
		opt.apply(l)
	}
	return l
}

// Opt applies options to the logger.
type Opt interface {
	apply(*Logger)
}

type opt struct{ fn func(*Logger) }

func (o opt) apply(l *Logger) { o.fn(l) }

// LevelFn sets a function that can dynamically change the log level.
//
// This log level is independent of the zap logger level. Zap itself does not
// have a way to pre-check which log level the logger is operating at.  While
// zap can have variable levels, the kgo.Client does a *lot* more work to build
// debug strings. Thus, the client often pre-checks "should I do this?", and
// then either performs an expensive operation or skips it.
//
// Thus, this option provides the initial filter before Log is called, after
// which the zap logger level takes effect.
func LevelFn(fn func() kgo.LogLevel) Opt {
	return opt{func(l *Logger) { l.levelFn = fn }}
}

// Level sets a static level for the kgo.Logger Level function.
//
// This log level is independent of the zap logger level. Zap itself does not
// have a way to pre-check which log level the logger is operating at.  While
// zap can have variable levels, the kgo.Client does a *lot* more work to build
// debug strings. Thus, the client often pre-checks "should I do this?", and
// then either performs an expensive operation or skips it.
//
// Thus, this option provides the initial filter before Log is called, after
// which the zap logger level takes effect.
func Level(level kgo.LogLevel) Opt {
	return LevelFn(func() kgo.LogLevel { return level })
}

// Level is for the kgo.Logger interface.
func (l *Logger) Level() kgo.LogLevel {
	return l.levelFn()
}

// Log is for the kgo.Logger interface.
func (l *Logger) Log(level kgo.LogLevel, msg string, keyvals ...interface{}) {
	fields := make([]zap.Field, 0, len(keyvals)/2)
	for i := 0; i < len(keyvals); i += 2 {
		k, v := keyvals[i], keyvals[i+1]
		fields = append(fields, zap.Any(k.(string), v))
	}
	switch level {
	case kgo.LogLevelDebug:
		l.zl.Debug(msg, fields...)
	case kgo.LogLevelError:
		l.zl.Error(msg, fields...)
	case kgo.LogLevelInfo:
		l.zl.Info(msg, fields...)
	case kgo.LogLevelWarn:
		l.zl.Warn(msg, fields...)
	default:
		// do nothing
	}
}
