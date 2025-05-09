package sr

import (
	"fmt"
	"io"
)

// LogLevel designates which level the logger should log at.
type LogLevel int8

const (
	// LogLevelNone disables logging.
	LogLevelNone LogLevel = iota
	// LogLevelError logs all errors.
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
	default:
		return "NONE"
	}
}

// Logger can be provided to hook into the SR client's logs.
type Logger interface {
	// Level returns the log level to log at. This function must be safe to
	// call concurrently.
	Level() LogLevel

	// Log logs a message with key, value pair arguments for the given log
	// level. Keys are always strings, while values can be any type.
	//
	// This must be safe to call concurrently.
	Log(level LogLevel, msg string, keyvals ...any)
}

type nopLogger struct{}

func (*nopLogger) Log(LogLevel, string, ...any) {}
func (*nopLogger) Level() LogLevel {
	return LogLevelNone
}

type basicLogger struct {
	dst   io.Writer
	level LogLevel
}

// BasicLogger returns a logger that writes newline delimited messages to dst.
func BasicLogger(dst io.Writer, level LogLevel) Logger {
	return &basicLogger{dst, level}
}

func (b *basicLogger) Level() LogLevel {
	return b.level
}

func (b *basicLogger) Log(level LogLevel, msg string, keyvals ...any) {
	if b.level < level {
		return
	}
	fmt.Fprintf(b.dst, "[%s] "+msg+"\n", append([]any{level}, keyvals...)...)
}
