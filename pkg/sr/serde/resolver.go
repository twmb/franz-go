// Package serde holds schema-id resolution shared by format-specific schema
// registry serdes. It handles subject naming, registration versus lookup,
// pinned and latest schema ids, and the caching around all of it.
//
// For protobuf encoding and decoding built on this package, see
// github.com/twmb/franz-go/pkg/sr/serde/protobuf.
package serde

import (
	"context"
	"errors"
	"strconv"
	"time"

	"github.com/twmb/franz-go/pkg/sr"
)

type latestEntry struct {
	ss sr.SubjectSchema
	at time.Time
}

// Resolver obtains schema ids from a schema registry client with caching and
// configurable register / lookup / latest behavior.
type Resolver struct {
	client *sr.Client
	cfg    cfg
	now    func() time.Time

	schemaCache  *lru[string, sr.SubjectSchema]
	schemaFlight flight[string, sr.SubjectSchema]

	latestCache  *lru[string, latestEntry]
	latestFlight flight[string, sr.SubjectSchema]
}

// NewResolver returns a Resolver for cl using default settings modified by
// opts. By default schemas are registered on first use under "{topic}-value".
func NewResolver(cl *sr.Client, opts ...Opt) *Resolver {
	return &Resolver{
		client:      cl,
		cfg:         applyOpts(defaultCfg(), opts),
		now:         time.Now,
		schemaCache: newLRU[string, sr.SubjectSchema](maxCacheEntries),
		latestCache: newLRU[string, latestEntry](maxCacheEntries),
	}
}

// StableIDs reports whether the schema id resolved for a given topic and
// record name stays the same for the life of the Resolver. It is false when
// the resolver follows the latest subject version, where registering a new
// version changes the id. Callers use this to decide whether they can cache
// resolved ids themselves rather than consulting the Resolver per record.
func (r *Resolver) StableIDs() bool { return !r.cfg.useLatestVersion }

// SubjectForTopic computes the subject name without contacting the registry.
func (r *Resolver) SubjectForTopic(topic, recordName string) (string, error) {
	return Subject(topic, r.cfg.part, r.cfg.strategy, recordName)
}

func (r *Resolver) ctx(ctx context.Context) context.Context {
	if r.cfg.normalize {
		return sr.WithParams(ctx, sr.Normalize)
	}
	return ctx
}

// ResolveSubjectSchema returns the registry metadata for a message under topic.
// recordName is the format-specific record identifier (protobuf fully
// qualified name, Avro name, etc.) and may be empty when the subject strategy
// is TopicName.
//
// deriveSchema produces the schema text to register or look up. It is called
// only when the resolver actually needs schema text, which is not the case
// when a schema id is pinned or the latest subject version is in use, so
// callers can leave expensive schema derivation inside it.
//
// When deriveSchema is not called the returned schema comes from the registry
// and was not checked against the caller's message. Callers that encode a
// format-specific pointer into the message (such as protobuf message indexes)
// should validate the returned schema.
func (r *Resolver) ResolveSubjectSchema(ctx context.Context, topic, recordName string, deriveSchema func(context.Context) (sr.Schema, error)) (sr.SubjectSchema, error) {
	subject, err := Subject(topic, r.cfg.part, r.cfg.strategy, recordName)
	if err != nil {
		return sr.SubjectSchema{}, err
	}
	if r.cfg.useSchemaID != nil {
		return r.pinned(ctx, subject, *r.cfg.useSchemaID)
	}
	if r.cfg.useLatestVersion {
		return r.latest(ctx, subject)
	}
	if deriveSchema == nil {
		return sr.SubjectSchema{}, errors.New("a schema derivation function is required to register or look up a schema")
	}
	schema, err := deriveSchema(ctx)
	if err != nil {
		return sr.SubjectSchema{}, err
	}
	return r.registerOrLookup(ctx, subject, schema)
}

// RegisterDependency registers or looks up a referenced schema. The subject is
// derived from fileName with the ReferenceSubject option; the returned
// SubjectSchema reports the subject that was used.
func (r *Resolver) RegisterDependency(ctx context.Context, fileName string, schema sr.Schema) (sr.SubjectSchema, error) {
	return r.registerOrLookup(ctx, r.cfg.subjectFor(fileName), schema)
}

func (r *Resolver) registerOrLookup(ctx context.Context, subject string, schema sr.Schema) (sr.SubjectSchema, error) {
	key, err := schemaCacheKey(schema)
	if err != nil {
		return sr.SubjectSchema{}, err
	}
	cacheKey := subject + "\x00" + key

	ss, ok := r.schemaCache.get(cacheKey)
	if ok {
		return ss, nil
	}

	return r.schemaFlight.do(cacheKey, func() (sr.SubjectSchema, error) {
		ss, ok := r.schemaCache.get(cacheKey)
		if ok {
			return ss, nil
		}

		ctx := r.ctx(ctx)
		var err error
		if r.cfg.autoRegister {
			ss, err = r.client.CreateSchema(ctx, subject, schema)
		} else {
			ss, err = r.client.LookupSchema(ctx, subject, schema)
		}
		if err != nil {
			return sr.SubjectSchema{}, err
		}
		ss.Subject = subject

		r.schemaCache.add(cacheKey, ss)
		return ss, nil
	})
}

// pinned resolves a UseSchemaID id once per subject and caches the result.
// Without the cache this issues a registry request for every encode.
func (r *Resolver) pinned(ctx context.Context, subject string, id int) (sr.SubjectSchema, error) {
	cacheKey := "\x00id\x00" + subject + "\x00" + strconv.Itoa(id)

	ss, ok := r.schemaCache.get(cacheKey)
	if ok {
		return ss, nil
	}

	return r.schemaFlight.do(cacheKey, func() (sr.SubjectSchema, error) {
		ss, ok := r.schemaCache.get(cacheKey)
		if ok {
			return ss, nil
		}

		s, err := r.client.SchemaByID(ctx, id)
		if err != nil {
			return sr.SubjectSchema{}, err
		}
		ss = sr.SubjectSchema{Subject: subject, ID: id, Schema: s}
		// A pinned id need not be registered under this subject. If it is,
		// report the version it has there; if not, Version stays zero.
		if vs, err := r.client.SchemaVersionsByID(ctx, id); err == nil {
			for _, v := range vs {
				if v.Subject == subject {
					ss.Version = v.Version
					break
				}
			}
		}

		r.schemaCache.add(cacheKey, ss)
		return ss, nil
	})
}

func (r *Resolver) latest(ctx context.Context, subject string) (sr.SubjectSchema, error) {
	if r.cfg.latestCacheTTL > 0 {
		e, ok := r.latestCache.get(subject)
		if ok && r.now().Sub(e.at) < r.cfg.latestCacheTTL {
			return e.ss, nil
		}
	}

	return r.latestFlight.do(subject, func() (sr.SubjectSchema, error) {
		if r.cfg.latestCacheTTL > 0 {
			e, ok := r.latestCache.get(subject)
			if ok && r.now().Sub(e.at) < r.cfg.latestCacheTTL {
				return e.ss, nil
			}
		}

		ss, err := r.client.SchemaByVersion(r.ctx(ctx), subject, -1)
		if err != nil {
			return sr.SubjectSchema{}, err
		}
		if r.cfg.latestCacheTTL > 0 {
			r.latestCache.add(subject, latestEntry{ss: ss, at: r.now()})
		}
		return ss, nil
	})
}
