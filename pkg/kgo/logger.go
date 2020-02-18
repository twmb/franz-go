package kgo

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
)

// LogLevel designates which level the logger should log at.
type LogLevel int8

const (
	// LogLevelNone disables logging.
	LogLevelNone LogLevel = iota
	// LogLevelError logs all errors. Generally, these should not happen.
	LogLevelError
	// LogLevelWarn logs all warnings, such as request failures.
	LogLevelWarn
	// LogLevelInfo logs informational messages, such as requests. This is
	// usually the default log level.
	LogLevelInfo
	// LogLevelDebug logs verbose information, and is usually not used in
	// production.
	LogLevelDebug
)

func (l LogLevel) String() string {
	switch l {
	case LogLevelError:
		return "ERROR"
	case LogLevelWarn:
		return "WARN"
	case LogLevelInfo:
		return "INFO"
	case LogLevelDebug:
		return "DEBUG"
	}
	return "NONE"
}

// Logger is used to log informational messages.
type Logger interface {
	// Level returns the log level to log at.
	//
	// Implementations can change their log level on the fly, but this
	// function must be safe to call concurrently.
	Level() LogLevel

	// Log logs a message with key, value pair arguments for the given log
	// level.
	//
	// This must be safe to call concurrently.
	Log(level LogLevel, msg string, keyvals ...interface{})
}

// BasicLogger returns a logger that will print to stderr in the following
// format:
//
//     # [LEVEL] message; key: val, key: val
//
// If withNum is true, logs are prefixed with a base36 formatted number that is
// the logger's creation number among all BasicLoggers created from this
// package ever. This can be used to differentiate loggers if using multiple
// clients in the same program. Otherwise, the # prefix is left off.
func BasicLogger(level LogLevel, withNum bool) Logger {
	var num uint32
	if withNum {
		num = atomic.AddUint32(&loggerNum, 1)
	}
	return &basicLogger{num, level, withNum}
}

var loggerNum uint32

type basicLogger struct {
	num     uint32
	level   LogLevel
	withNum bool
}

func (b *basicLogger) Level() LogLevel { return b.level }
func (b *basicLogger) Log(level LogLevel, msg string, keyvals ...interface{}) {
	buf := sliceWriters.Get().(*sliceWriter)
	defer sliceWriters.Put(buf)

	buf.inner = buf.inner[:0]
	if b.withNum {
		buf.inner = strconv.AppendUint(buf.inner, uint64(b.num), 36)
		buf.inner = append(buf.inner, " ["...)
	} else {
		buf.inner = append(buf.inner, '[')
	}
	buf.inner = append(buf.inner, level.String()...)
	buf.inner = append(buf.inner, "] "...)
	buf.inner = append(buf.inner, msg...)

	if len(keyvals) > 0 {
		buf.inner = append(buf.inner, "; "...)
		format := strings.Repeat("%v: %v, ", len(keyvals)/2)
		format = format[:len(format)-2] // trim trailing comma and space
		fmt.Fprintf(buf, format, keyvals...)
	}

	buf.inner = append(buf.inner, '\n')

	os.Stderr.Write(buf.inner)
}

// nopLogger, the default logger, drops everything.
type nopLogger struct{}

func (*nopLogger) Level() LogLevel { return LogLevelNone }
func (*nopLogger) Log(level LogLevel, msg string, keyvals ...interface{}) {
}

// wrappedLogger wraps the config logger for convenience at logging callsites.
type wrappedLogger struct {
	inner Logger
}

func (w *wrappedLogger) Level() LogLevel {
	if w.inner == nil {
		return LogLevelNone
	}
	return w.inner.Level()
}

func (w *wrappedLogger) Log(level LogLevel, msg string, keyvals ...interface{}) {
	if w.Level() < level {
		return
	}
	w.inner.Log(level, msg, keyvals...)
}
