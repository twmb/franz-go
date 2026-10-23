// Package protobuf encodes and decodes protobuf messages in the Confluent
// schema registry wire format: a magic byte, the schema id, the message index
// path, and then the protobuf payload.
//
// A Serializer derives the schema text for a message from its compiled-in
// descriptor, registers it (along with any imported .proto files, as schema
// references), and caches the resulting id per topic. A Deserializer resolves
// a schema id back to a message descriptor to pick the concrete Go type.
//
// # Imports
//
// Schema text is compiled with the protobuf files linked into this binary, by
// way of protoregistry.GlobalFiles, plus the well-known google/protobuf types.
// A schema that imports something else, such as google/type/latlng.proto or
// confluent/meta.proto, resolves only if the generated Go package for it is
// linked in. On the decode path this is automatic, because the message being
// decoded is registered and its generated file imports whatever its schema
// imports. Otherwise blank-import the package:
//
//	import _ "google.golang.org/genproto/googleapis/type/latlng"
//
// or pass the file's text with the [Imports] option.
//
// # Compatibility with other clients
//
// Per-language codegen options (go_package, java_package, and the rest) are
// stripped from registered schema text by default, because they say nothing
// about the wire format and would otherwise make the registered schema depend
// on which language wrote it. confluent-kafka-go leaves them in, so producing
// the same .proto from both clients with auto registration on creates two
// versions of the subject. [KeepCodegenOptions] turns stripping off to match
// confluent-kafka-go byte for byte.
package protobuf

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/pkg/sr/serde"
)

// Serializer encodes protobuf messages using the Confluent schema registry
// wire format. It is safe for concurrent use.
type Serializer struct {
	resolver *serde.Resolver
	builder  *schemaBuilder
	header   sr.ConfluentHeader
	verifier *schemaVerifier

	// cacheIDs is false when the resolver follows the latest subject version,
	// where the id can change underneath us and must be re-resolved.
	cacheIDs bool

	mu   sync.RWMutex
	dsts map[dstKey]encTarget
}

// dstKey is a comparable struct rather than a concatenated string so that
// looking up an already-resolved id on the encode path does not allocate.
type dstKey struct {
	topic string
	name  protoreflect.FullName
}

type encTarget struct {
	id    int
	index []int
}

// NewSerializer returns a Serializer that uses cl for schema registration and
// lookup.
func NewSerializer(cl *sr.Client, opts ...Opt) (*Serializer, error) {
	if cl == nil {
		return nil, errors.New("schema registry client is required")
	}
	cfg := newCfg(opts)
	r := serde.NewResolver(cl, cfg.serdeOpts...)
	return &Serializer{
		resolver: r,
		builder:  newSchemaBuilder(r, cfg.keepCodegenOptions),
		verifier: newSchemaVerifier(cl, cfg.imports),
		cacheIDs: r.StableIDs(),
		dsts:     make(map[dstKey]encTarget),
	}, nil
}

// Encode serializes msg for topic using schema registry framing.
func (s *Serializer) Encode(ctx context.Context, topic string, msg proto.Message) ([]byte, error) {
	return s.AppendEncode(ctx, topic, msg, nil)
}

// AppendEncode appends the encoded record value to dst and returns the
// extended slice, so callers can reuse a buffer across records.
func (s *Serializer) AppendEncode(ctx context.Context, topic string, msg proto.Message, dst []byte) ([]byte, error) {
	if msg == nil {
		return nil, errors.New("nil protobuf message")
	}
	md := msg.ProtoReflect().Descriptor()

	t, err := s.target(ctx, topic, md)
	if err != nil {
		return nil, err
	}
	dst, err = s.header.AppendEncode(dst, t.id, t.index)
	if err != nil {
		return nil, err
	}
	return proto.MarshalOptions{}.MarshalAppend(dst, msg)
}

// target returns the schema id and message index to frame md with under topic,
// resolving and caching them on first use. Without the cache every encode
// would rebuild the schema text just to compute a registry cache key.
func (s *Serializer) target(ctx context.Context, topic string, md protoreflect.MessageDescriptor) (encTarget, error) {
	key := dstKey{topic: topic, name: md.FullName()}

	if s.cacheIDs {
		s.mu.RLock()
		t, ok := s.dsts[key]
		s.mu.RUnlock()
		if ok {
			return t, nil
		}
	}

	recordName := string(md.FullName())
	index := messageIndexArray(md)

	// derived records whether the resolver asked us for schema text. When it
	// did not, the id came from the registry (pinned or latest version) and
	// was never checked against this message, so the index we are about to
	// write could point at an unrelated message.
	var derived bool
	ss, err := s.resolver.ResolveSubjectSchema(ctx, topic, recordName, func(ctx context.Context) (sr.Schema, error) {
		derived = true
		return s.builder.schemaForMessage(ctx, md)
	})
	if err != nil {
		return encTarget{}, err
	}
	if !derived {
		if err := s.verifier.check(ctx, ss, recordName, index); err != nil {
			return encTarget{}, err
		}
	}

	t := encTarget{id: ss.ID, index: index}
	if s.cacheIDs {
		s.mu.Lock()
		s.dsts[key] = t
		s.mu.Unlock()
	}
	return t, nil
}

// schemaVerifier checks that a schema resolved from the registry really does
// contain the message the caller is about to encode at the index we derived
// locally. Results are cached per schema id.
type schemaVerifier struct {
	client  *sr.Client
	imports map[string]string
	seen    *lru[string, error]
}

func newSchemaVerifier(cl *sr.Client, imports map[string]string) *schemaVerifier {
	return &schemaVerifier{
		client:  cl,
		imports: imports,
		seen:    newLRU[string, error](maxCacheEntries),
	}
}

func (v *schemaVerifier) check(ctx context.Context, ss sr.SubjectSchema, recordName string, index []int) error {
	key := fmt.Sprintf("%d\x00%s\x00%v", ss.ID, recordName, index)

	err, ok := v.seen.get(key)
	if ok {
		return err
	}

	err = v.verify(ctx, ss, recordName, index)

	v.seen.add(key, err)
	return err
}

func (v *schemaVerifier) verify(ctx context.Context, ss sr.SubjectSchema, recordName string, index []int) error {
	if ss.Type != sr.TypeProtobuf {
		return fmt.Errorf("schema id %d has type %s, not protobuf", ss.ID, ss.Type)
	}
	deps := make(map[string]string, len(v.imports))
	for k, s := range v.imports {
		deps[k] = s
	}
	err := resolveReferences(ss.Schema, func(subject string, version int) (sr.Schema, error) {
		got, err := v.client.SchemaByVersion(ctx, subject, version)
		if err != nil {
			return sr.Schema{}, err
		}
		return got.Schema, nil
	}, deps)
	if err != nil {
		return err
	}
	fd, err := parseSchema(ss.Schema, deps)
	if err != nil {
		return fmt.Errorf("unable to parse schema id %d to validate %q against it: %w", ss.ID, recordName, err)
	}
	md, err := messageDescriptorAt(fd, index)
	if err != nil {
		return fmt.Errorf("schema id %d has no message at index %v for %q: %w", ss.ID, index, recordName, err)
	}
	if got := md.GetFullyQualifiedName(); got != recordName {
		return fmt.Errorf("schema id %d has message %q at index %v, but encoding %q; "+
			"the pinned or latest schema does not match this message type", ss.ID, got, index, recordName)
	}
	return nil
}
