// Package serde holds the format-independent half of a schema registry
// serde: subject naming, schema id resolution, reference resolution, the
// Confluent wire framing, and the caching around all of it.
//
// A format - protobuf, Avro, JSON Schema - supplies the rest by implementing
// [Encoder] and [Decoder]. [NewSerializer] and [NewDeserializer] drive those
// implementations and own every registry interaction, so an implementation
// deals only in schema text and payload bytes.
//
// This package has no dependencies beyond [github.com/twmb/franz-go/pkg/sr]
// and the standard library, which is why its interfaces are written in terms
// of any rather than a format's message type. Implementations are expected to
// wrap the drivers in a typed facade; see
// github.com/twmb/franz-go/plugin/srproto for protobuf.
package serde

import (
	"context"

	"github.com/twmb/franz-go/pkg/sr"
)

// ResolvedSchema is a schema from the registry together with every schema it
// references, transitively.
//
// Deps is keyed by the import name a schema uses to refer to another, which
// for protobuf is the imported file's path. It holds the referenced schema
// text, not the referencing schema's own. It is cached and shared across
// callers, so it must not be modified; a Decoder that needs to overlay its
// own files should consult both maps rather than merging them.
type ResolvedSchema struct {
	ID     int
	Schema sr.Schema
	Deps   map[string]string
}

// SchemaLoader resolves a schema id to schema text and every schema it
// references.
//
// A Deserializer passes one to each [Decoder] call. The result is cached per
// schema id and concurrent calls for the same id are collapsed, so an
// implementation need not cache them itself. The registry is not contacted at
// all unless Load is called, so a Decoder that can unmarshal without
// consulting the schema should not call it.
type SchemaLoader interface {
	Load(ctx context.Context, id int) (ResolvedSchema, error)
}

// WirePayload is one value handed to a [Decoder]: the fields read out of the
// Confluent wire header, and the payload that followed them.
//
// Only the header is decoded. Payload is still the raw bytes the format's own
// unmarshaller has to interpret.
type WirePayload struct {
	// ID is the schema id from the wire header.
	ID int

	// Index is the protobuf message-index path from the wire header. It is
	// nil for formats that do not write one.
	Index []int

	// Payload is the value with the wire header stripped.
	Payload []byte
}

// Encoder is the format-specific half of a schema registry serializer.
//
// A Serializer calls RecordName on every encode, Index and SchemaFor only
// when it must resolve a schema id it has not cached, and AppendMarshal once
// the id is known.
type Encoder interface {
	// RecordName returns the registry record name for v: the protobuf fully
	// qualified message name, the Avro full name, and so on. It names the
	// subject under the RecordName and TopicRecordName strategies, and keys
	// the Serializer's schema id cache.
	RecordName(v any) (string, error)

	// Index returns the Confluent message-index path to write into the wire
	// header for v. Formats that do not use one return nil.
	Index(v any) ([]int, error)

	// SchemaFor derives registry schema text for v, with References filled
	// in for every schema it imports.
	//
	// It is called only when the Serializer must register a schema or look
	// one up by content, so expensive derivation can live here: the
	// UseSchemaID and UseLatestVersion paths never call it.
	SchemaFor(ctx context.Context, v any) (sr.Schema, error)

	// AppendMarshal appends the encoded payload for v to dst and returns the
	// extended slice. The Confluent wire header is already in dst.
	AppendMarshal(dst []byte, v any) ([]byte, error)
}

// Verifier is an optional interface an [Encoder] may implement.
//
// A Serializer calls it on the UseSchemaID and UseLatestVersion paths, where
// the schema id came from the registry and SchemaFor was never called, so
// nothing has established that the resolved schema describes v. This matters
// for formats that write a pointer into the schema, such as protobuf message
// indexes, where a mismatch silently mislabels the record rather than failing.
//
// Results are cached per schema id and record name, so an implementation need
// not cache them itself.
type Verifier interface {
	Verify(ctx context.Context, rs ResolvedSchema, v any) error
}

// Decoder is the format-specific half of a schema registry deserializer.
//
// Both methods receive a [SchemaLoader] rather than a resolved schema,
// because resolving one costs a registry request that most decodes do not
// need. A Decoder that can unmarshal from w alone should ignore it.
type Decoder interface {
	// Unmarshal decodes w into v, which the caller supplied and which the
	// Decoder may therefore assume is the intended target.
	Unmarshal(ctx context.Context, w WirePayload, s SchemaLoader, v any) error

	// UnmarshalNew decodes w into a newly allocated value whose type is
	// chosen from the schema the value was written with.
	UnmarshalNew(ctx context.Context, w WirePayload, s SchemaLoader) (any, error)
}
