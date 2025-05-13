package sr

// The log levels that can be used to control the verbosity of the
// SR client. The levels are ordered to mirror the kgo leveling.
const (
	// LogLevelNone corresponds to no logging.
	LogLevelNone int8 = iota
	// LogLevelError opts into logging errors.
	LogLevelError
	// LogLevelWarn opts into logging warnings and errors.
	LogLevelWarn
	// LogLevelInfo opts into informational logging, warnings, and errors.
	LogLevelInfo
	// LogLevelDebug opts into all logs.
	LogLevelDebug
)
