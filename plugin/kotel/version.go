package kotel

import "runtime/debug"

// Version is the current release version of the kotel instrumentation.
func Version() string {
	info, ok := debug.ReadBuildInfo()
	if ok {
		for _, dep := range info.Deps {
			if dep.Path == instrumentationName {
				return dep.Version
			}
		}
	}
	return "unknown"
}

// SemVersion is the semantic version to be supplied to tracer/meter creation.
func SemVersion() string {
	return "semver:" + Version()
}
