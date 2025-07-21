package srfake

import "github.com/twmb/franz-go/pkg/sr"

// Option customizes the behaviour of a Registry.
type Option func(*Registry)

// WithAuth makes the mock expect exactly `value` in the HTTP Authorization header.
func WithAuth(value string) Option {
	return func(r *Registry) { r.expectedAuth = value }
}

// WithGlobalCompat sets the default compatibility level returned by /config.
func WithGlobalCompat(c sr.CompatibilityLevel) Option {
	return func(r *Registry) { r.globalCompat = c }
}
