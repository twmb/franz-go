package serde

import (
	"context"
	"fmt"
	"strconv"

	"github.com/twmb/franz-go/pkg/sr"
)

// resolveReferences loads schema and every schema it references, including
// references of references, into deps keyed by import name. A subject/version
// already being resolved is a cycle. A subject/version already finished is
// skipped so a diamond import is fetched once.
func resolveReferences(schema sr.Schema, fetch func(subject string, version int) (sr.Schema, error), deps map[string]string) error {
	resolved := make(map[string]struct{})
	stack := make(map[string]struct{})
	var walk func(sr.Schema) error
	walk = func(schema sr.Schema) error {
		for _, ref := range schema.References {
			key := ref.Subject + "\x00" + strconv.Itoa(ref.Version)
			if _, ok := stack[key]; ok {
				return fmt.Errorf("circular schema reference %q version %d", ref.Subject, ref.Version)
			}
			if _, ok := resolved[key]; ok {
				continue
			}
			stack[key] = struct{}{}
			next, err := fetch(ref.Subject, ref.Version)
			if err != nil {
				return fmt.Errorf("unable to fetch referenced schema %q (subject %q version %d): %w",
					ref.Name, ref.Subject, ref.Version, err)
			}
			deps[ref.Name] = next.Schema
			if err := walk(next); err != nil {
				return err
			}
			delete(stack, key)
			resolved[key] = struct{}{}
		}
		return nil
	}
	return walk(schema)
}

// schemaLoader turns a schema id into schema text plus every schema it
// references, caching the result and collapsing concurrent misses for one id
// into a single set of registry requests.
type schemaLoader struct {
	client *sr.Client
	cache  *lru[int, ResolvedSchema]
	flight flight[int, ResolvedSchema]
}

func newSchemaLoader(cl *sr.Client) *schemaLoader {
	return &schemaLoader{
		client: cl,
		cache:  newLRU[int, ResolvedSchema](maxCacheEntries),
	}
}

// Load fetches the schema with this id and resolves its references. It
// implements [SchemaLoader].
func (l *schemaLoader) Load(ctx context.Context, id int) (ResolvedSchema, error) {
	if rs, ok := l.cache.get(id); ok {
		return rs, nil
	}
	return l.flight.do(id, func() (ResolvedSchema, error) {
		if rs, ok := l.cache.get(id); ok {
			return rs, nil
		}
		s, err := l.client.SchemaByID(ctx, id)
		if err != nil {
			return ResolvedSchema{}, err
		}
		return l.resolve(ctx, id, s)
	})
}

// withSchema resolves the references of a schema the caller already holds,
// saving the fetch that Load would do. It shares Load's cache, so whichever
// runs first serves the other.
func (l *schemaLoader) withSchema(ctx context.Context, id int, s sr.Schema) (ResolvedSchema, error) {
	if rs, ok := l.cache.get(id); ok {
		return rs, nil
	}
	return l.flight.do(id, func() (ResolvedSchema, error) {
		if rs, ok := l.cache.get(id); ok {
			return rs, nil
		}
		return l.resolve(ctx, id, s)
	})
}

func (l *schemaLoader) resolve(ctx context.Context, id int, s sr.Schema) (ResolvedSchema, error) {
	deps := make(map[string]string)
	err := resolveReferences(s, func(subject string, version int) (sr.Schema, error) {
		ss, err := l.client.SchemaByVersion(ctx, subject, version)
		if err != nil {
			return sr.Schema{}, err
		}
		return ss.Schema, nil
	}, deps)
	if err != nil {
		return ResolvedSchema{}, err
	}
	rs := ResolvedSchema{ID: id, Schema: s, Deps: deps}
	l.cache.add(id, rs)
	return rs, nil
}
