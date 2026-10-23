package serde

import "time"

// cfg holds schema-id resolution settings shared by format-specific serdes.
// The zero value is not usable; see defaultCfg.
type cfg struct {
	autoRegister     bool
	useSchemaID      *int
	useLatestVersion bool
	latestCacheTTL   time.Duration
	normalize        bool
	part             Part
	strategy         Strategy
	referenceSubject func(fileName string) string
}

// defaultCfg returns settings matching Confluent's default serializer behavior
// for subject naming and auto registration.
func defaultCfg() cfg {
	return cfg{
		autoRegister:   true,
		latestCacheTTL: 5 * time.Minute,
		part:           PartValue,
		strategy:       TopicName,
	}
}

func (c cfg) subjectFor(fileName string) string {
	if c.referenceSubject == nil {
		return fileName
	}
	return c.referenceSubject(fileName)
}

type opt struct{ fn func(*cfg) }

func (o opt) apply(c *cfg) { o.fn(c) }

// Opt configures a Resolver or format-specific serde.
type Opt interface {
	apply(*cfg)
}

// AutoRegister sets whether schemas are registered on encode. The default is
// true; when false, schemas are looked up and encoding fails if the schema is
// not already registered.
func AutoRegister(v bool) Opt {
	return opt{func(c *cfg) { c.autoRegister = v }}
}

// UseSchemaID pins encode to an existing schema id. Schema text is neither
// derived nor sent to the registry, and the id is resolved at most once.
func UseSchemaID(id int) Opt {
	return opt{func(c *cfg) { c.useSchemaID = &id }}
}

// UseLatestVersion uses the latest schema version for a subject rather than
// deriving and registering schema text. The lookup is cached for
// LatestCacheTTL.
func UseLatestVersion(v bool) Opt {
	return opt{func(c *cfg) { c.useLatestVersion = v }}
}

// LatestCacheTTL sets how long a latest-schema lookup is reused.
// Zero disables the cache. The default is 5 minutes.
func LatestCacheTTL(d time.Duration) Opt {
	return opt{func(c *cfg) { c.latestCacheTTL = d }}
}

// Normalize enables registry schema normalization on register and lookup
// requests.
func Normalize(v bool) Opt {
	return opt{func(c *cfg) { c.normalize = v }}
}

// SubjectPart selects whether subjects are derived for record keys or values.
// The default is PartValue.
func SubjectPart(p Part) Opt {
	return opt{func(c *cfg) { c.part = p }}
}

// SubjectStrategy sets how subjects are derived from topics. The default is
// TopicName.
func SubjectStrategy(s Strategy) Opt {
	return opt{func(c *cfg) { c.strategy = s }}
}

// ReferenceSubject sets how a referenced schema file is named in the registry.
// Nil, the default, uses the file name.
func ReferenceSubject(fn func(fileName string) string) Opt {
	return opt{func(c *cfg) { c.referenceSubject = fn }}
}

func applyOpts(c cfg, opts []Opt) cfg {
	for _, o := range opts {
		o.apply(&c)
	}
	return c
}
