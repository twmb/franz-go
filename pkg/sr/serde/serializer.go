package serde

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/twmb/franz-go/pkg/sr"
)

// Serializer frames values in the Confluent schema registry wire format,
// driving an [Encoder] for everything format-specific. It is safe for
// concurrent use.
type Serializer struct {
	resolver *Resolver
	enc      Encoder
	header   sr.ConfluentHeader
	loader   *schemaLoader

	// verifier is enc when it implements [Verifier], otherwise nil.
	verifier Verifier
	// verified caches verification results per schema id and record, so the
	// schema behind a pinned id is parsed once rather than once per record.
	verified *lru[string, error]

	// cacheIDs is false when the resolver follows the latest subject
	// version, where a newly registered version changes the id underneath us
	// and it must be re-resolved.
	cacheIDs bool

	mu   sync.RWMutex
	dsts map[dstKey]encTarget
}

// dstKey is a comparable struct rather than a concatenated string so that
// looking up an already-resolved id on the encode path does not allocate.
type dstKey struct {
	topic      string
	recordName string
}

type encTarget struct {
	id    int
	index []int
}

// NewSerializer returns a Serializer that resolves schema ids with r and
// encodes with enc.
//
// r is passed in rather than built from options because an Encoder that
// registers referenced schemas needs the same Resolver; see
// [Resolver.RegisterDependency].
func NewSerializer(r *Resolver, enc Encoder) (*Serializer, error) {
	if r == nil {
		return nil, errors.New("resolver is required")
	}
	if enc == nil {
		return nil, errors.New("encoder is required")
	}
	s := &Serializer{
		resolver: r,
		enc:      enc,
		loader:   newSchemaLoader(r.client),
		verified: newLRU[string, error](maxCacheEntries),
		cacheIDs: r.StableIDs(),
		dsts:     make(map[dstKey]encTarget),
	}
	s.verifier, _ = enc.(Verifier)
	return s, nil
}

// Encode returns v framed for topic.
func (s *Serializer) Encode(ctx context.Context, topic string, v any) ([]byte, error) {
	return s.AppendEncode(ctx, topic, v, nil)
}

// AppendEncode appends the framed encoding of v to dst and returns the
// extended slice, so callers can reuse a buffer across records.
func (s *Serializer) AppendEncode(ctx context.Context, topic string, v any, dst []byte) ([]byte, error) {
	t, err := s.target(ctx, topic, v)
	if err != nil {
		return nil, err
	}
	dst, err = s.header.AppendEncode(dst, t.id, t.index)
	if err != nil {
		return nil, err
	}
	return s.enc.AppendMarshal(dst, v)
}

// target returns the schema id and message index to frame v with under topic,
// resolving and caching them on first use. Without the cache every encode
// would re-derive schema text just to compute a registry cache key.
func (s *Serializer) target(ctx context.Context, topic string, v any) (encTarget, error) {
	recordName, err := s.enc.RecordName(v)
	if err != nil {
		return encTarget{}, err
	}

	key := dstKey{topic: topic, recordName: recordName}
	if s.cacheIDs {
		s.mu.RLock()
		t, ok := s.dsts[key]
		s.mu.RUnlock()
		if ok {
			return t, nil
		}
	}

	index, err := s.enc.Index(v)
	if err != nil {
		return encTarget{}, err
	}

	// derived records whether the resolver asked us for schema text. When it
	// did not, the id came from the registry (pinned or latest version) and
	// was never checked against v, so the index we are about to write could
	// point at an unrelated message.
	var derived bool
	ss, err := s.resolver.ResolveSubjectSchema(ctx, topic, recordName, func(ctx context.Context) (sr.Schema, error) {
		derived = true
		return s.enc.SchemaFor(ctx, v)
	})
	if err != nil {
		return encTarget{}, err
	}
	if !derived {
		if err := s.verify(ctx, ss, recordName, index, v); err != nil {
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

func (s *Serializer) verify(ctx context.Context, ss sr.SubjectSchema, recordName string, index []int, v any) error {
	if s.verifier == nil {
		return nil
	}
	key := fmt.Sprintf("%d\x00%s\x00%v", ss.ID, recordName, index)
	if err, ok := s.verified.get(key); ok {
		return err
	}
	rs, err := s.loader.withSchema(ctx, ss.ID, ss.Schema)
	if err == nil {
		err = s.verifier.Verify(ctx, rs, v)
	}
	s.verified.add(key, err)
	return err
}
