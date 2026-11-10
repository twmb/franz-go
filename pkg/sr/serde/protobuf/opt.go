package protobuf

import (
	"time"

	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/twmb/franz-go/pkg/sr/serde"
)

// Part is whether a subject names a record key or value.
type Part = serde.Part

// Strategy selects how a Kafka topic maps to a schema registry subject.
type Strategy = serde.Strategy

const (
	// PartValue derives subjects for record values.
	PartValue = serde.PartValue
	// PartKey derives subjects for record keys.
	PartKey = serde.PartKey

	// TopicName uses "{topic}-value" or "{topic}-key".
	TopicName = serde.TopicName
	// RecordName uses the fully qualified protobuf message name.
	RecordName = serde.RecordName
	// TopicRecordName uses "{topic}-{recordName}".
	TopicRecordName = serde.TopicRecordName
)

// Opt configures a Serializer or Deserializer.
type Opt interface {
	apply(*cfg)
}

type cfg struct {
	serdeOpts          []serde.Opt
	keepCodegenOptions bool
	imports            map[string]string
	types              *protoregistry.Types
	verifyDecode       bool
}

func newCfg(opts []Opt) cfg {
	var c cfg
	for _, o := range opts {
		o.apply(&c)
	}
	return c
}

type opt struct{ fn func(*cfg) }

func (o opt) apply(c *cfg) { o.fn(c) }

func fromSerde(o serde.Opt) Opt {
	return opt{func(c *cfg) { c.serdeOpts = append(c.serdeOpts, o) }}
}

// From adapts a serde.Opt into an Opt, for resolver settings that this package
// does not forward directly.
func From(o serde.Opt) Opt { return fromSerde(o) }

// AutoRegister sets whether schemas are registered on encode. The default is
// true; when false, schemas are looked up and encoding fails if the schema is
// not already registered.
func AutoRegister(v bool) Opt { return fromSerde(serde.AutoRegister(v)) }

// UseSchemaID pins encode to an existing schema id. Schema text is neither
// derived nor sent to the registry. The message is validated against the
// pinned schema once, so a mismatched id fails rather than producing records
// no consumer can decode.
func UseSchemaID(id int) Opt { return fromSerde(serde.UseSchemaID(id)) }

// UseLatestVersion encodes against the latest registered version of the
// subject rather than deriving and registering schema text. As with
// UseSchemaID, the message is validated against the resolved schema.
func UseLatestVersion(v bool) Opt { return fromSerde(serde.UseLatestVersion(v)) }

// LatestCacheTTL sets how long a latest-schema lookup is reused.
// Zero disables the cache. The default is 5 minutes.
func LatestCacheTTL(d time.Duration) Opt { return fromSerde(serde.LatestCacheTTL(d)) }

// Normalize enables registry schema normalization on register and lookup.
func Normalize(v bool) Opt { return fromSerde(serde.Normalize(v)) }

// SubjectPart selects whether subjects are derived for record keys or values.
// The default is PartValue.
func SubjectPart(p Part) Opt { return fromSerde(serde.SubjectPart(p)) }

// SubjectStrategy sets how subjects are derived from topics. The default is
// TopicName.
func SubjectStrategy(s Strategy) Opt { return fromSerde(serde.SubjectStrategy(s)) }

// ReferenceSubject sets how an imported .proto file is named in the registry.
// Nil, the default, uses the file name.
func ReferenceSubject(fn func(fileName string) string) Opt {
	return fromSerde(serde.ReferenceSubject(fn))
}

// KeepCodegenOptions leaves per-language codegen options (go_package,
// java_package, and the rest) in registered schema text. They are stripped by
// default because they describe code generation rather than the wire format,
// which makes the registered schema depend on which language registered it.
// Set this to match confluent-kafka-go, which does not strip them.
func KeepCodegenOptions() Opt {
	return opt{func(c *cfg) { c.keepCodegenOptions = true }}
}

// Imports supplies .proto source text for imports that are neither registered
// schema references nor linked into this binary, keyed by the path used in the
// import statement.
//
// This is an escape hatch. Normally an import resolves from
// protoregistry.GlobalFiles because its generated Go package is linked in.
func Imports(files map[string]string) Opt {
	return opt{func(c *cfg) {
		if c.imports == nil {
			c.imports = make(map[string]string, len(files))
		}
		for k, v := range files {
			c.imports[k] = v
		}
	}}
}

// MessageTypes sets an additional registry that DecodeNew resolves concrete Go
// types from, consulted before protoregistry.GlobalTypes.
func MessageTypes(types *protoregistry.Types) Opt {
	return opt{func(c *cfg) { c.types = types }}
}

// VerifyDecode makes Decode check that the schema id and message indexes in
// the record header actually describe the message being decoded into, instead
// of unmarshaling blindly. This costs one schema resolution per unseen id.
func VerifyDecode() Opt {
	return opt{func(c *cfg) { c.verifyDecode = true }}
}
