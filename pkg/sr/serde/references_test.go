package serde

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/pkg/sr/srfake"
)

// fetchRecorder stands in for the registry in resolveReferences tests,
// counting how many times each subject/version is asked for.
type fetchRecorder struct {
	schemas map[string]sr.Schema
	calls   map[string]int
	err     error
}

func (f *fetchRecorder) fetch(subject string, version int) (sr.Schema, error) {
	key := fmt.Sprintf("%s/%d", subject, version)
	f.calls[key]++
	if f.err != nil {
		return sr.Schema{}, f.err
	}
	s, ok := f.schemas[key]
	if !ok {
		return sr.Schema{}, fmt.Errorf("no schema seeded at %s", key)
	}
	return s, nil
}

func ref(name, subject string, version int) sr.SchemaReference {
	return sr.SchemaReference{Name: name, Subject: subject, Version: version}
}

func TestResolveReferencesFetchesADiamondOnce(t *testing.T) {
	t.Parallel()

	// root imports left and right; both import leaf.
	f := &fetchRecorder{
		calls: make(map[string]int),
		schemas: map[string]sr.Schema{
			"left/1":  {Schema: "left", References: []sr.SchemaReference{ref("leaf.proto", "leaf", 1)}},
			"right/1": {Schema: "right", References: []sr.SchemaReference{ref("leaf.proto", "leaf", 1)}},
			"leaf/1":  {Schema: "leaf"},
		},
	}
	root := sr.Schema{Schema: "root", References: []sr.SchemaReference{
		ref("left.proto", "left", 1),
		ref("right.proto", "right", 1),
	}}

	deps := make(map[string]string)
	if err := resolveReferences(root, f.fetch, deps); err != nil {
		t.Fatalf("resolveReferences: %v", err)
	}

	for name, want := range map[string]string{
		"left.proto":  "left",
		"right.proto": "right",
		"leaf.proto":  "leaf",
	} {
		if got := deps[name]; got != want {
			t.Errorf("deps[%q] = %q, want %q", name, got, want)
		}
	}
	if len(deps) != 3 {
		t.Errorf("resolved %d deps, want 3: %v", len(deps), deps)
	}
	if got := f.calls["leaf/1"]; got != 1 {
		t.Errorf("fetched leaf %d times, want 1; a diamond import should be fetched once", got)
	}
}

func TestResolveReferencesDetectsACycle(t *testing.T) {
	t.Parallel()

	f := &fetchRecorder{
		calls: make(map[string]int),
		schemas: map[string]sr.Schema{
			"a/1": {Schema: "a", References: []sr.SchemaReference{ref("b.proto", "b", 1)}},
			"b/1": {Schema: "b", References: []sr.SchemaReference{ref("a.proto", "a", 1)}},
		},
	}
	root := sr.Schema{Schema: "root", References: []sr.SchemaReference{ref("a.proto", "a", 1)}}

	err := resolveReferences(root, f.fetch, make(map[string]string))
	if err == nil {
		t.Fatal("resolveReferences succeeded on a cyclic reference graph, want an error")
	}
	if !strings.Contains(err.Error(), "circular") {
		t.Errorf("error %q does not mention a circular reference", err)
	}
}

func TestResolveReferencesNamesTheFailedReference(t *testing.T) {
	t.Parallel()

	sentinel := errors.New("registry is down")
	f := &fetchRecorder{calls: make(map[string]int), err: sentinel}
	root := sr.Schema{Schema: "root", References: []sr.SchemaReference{ref("leaf.proto", "leaf", 7)}}

	err := resolveReferences(root, f.fetch, make(map[string]string))
	if err == nil {
		t.Fatal("resolveReferences succeeded despite a failing fetch, want an error")
	}
	if !errors.Is(err, sentinel) {
		t.Errorf("error %q does not wrap the underlying fetch error", err)
	}
	// Without these the caller only learns that some unnamed reference failed.
	for _, want := range []string{"leaf.proto", `"leaf"`, "7"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not mention %s", err, want)
		}
	}
}

// countingRegistry is a srfake registry that tallies requests by path prefix.
type countingRegistry struct {
	*srfake.Registry
	mu     sync.Mutex
	counts map[string]int
}

func newCountingRegistry(t *testing.T) *countingRegistry {
	t.Helper()
	c := &countingRegistry{counts: make(map[string]int)}
	c.Registry = srfake.New()
	t.Cleanup(c.Close)
	c.Intercept(func(_ http.ResponseWriter, r *http.Request) bool {
		c.mu.Lock()
		c.counts[r.Method+" "+r.URL.Path]++
		c.mu.Unlock()
		return false // keep going; we only want the tally
	})
	return c
}

func (c *countingRegistry) total() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	var n int
	for _, v := range c.counts {
		n += v
	}
	return n
}

func (c *countingRegistry) reset() {
	c.mu.Lock()
	c.counts = make(map[string]int)
	c.mu.Unlock()
}

func (c *countingRegistry) client(t *testing.T) *sr.Client {
	t.Helper()
	cl, err := sr.NewClient(sr.URLs(c.URL()))
	if err != nil {
		t.Fatalf("sr.NewClient: %v", err)
	}
	return cl
}

func avro(name string) sr.Schema {
	return sr.Schema{Type: sr.TypeAvro, Schema: fmt.Sprintf(`{"type":"record","name":%q,"fields":[]}`, name)}
}

func TestSchemaLoaderCachesByID(t *testing.T) {
	t.Parallel()

	reg := newCountingRegistry(t)
	reg.SeedSchema("widget-value", 1, 1, avro("Widget"))
	l := newSchemaLoader(reg.client(t))

	ctx := context.Background()
	first, err := l.Load(ctx, 1)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if first.ID != 1 {
		t.Errorf("ID = %d, want 1", first.ID)
	}
	// Guard the assertion below: if the interceptor never fired, a warm
	// count of zero would mean nothing.
	if n := reg.total(); n == 0 {
		t.Fatal("the cold load made no observed requests; the request tally is not working")
	}
	reg.reset()

	for range 20 {
		if _, err := l.Load(ctx, 1); err != nil {
			t.Fatalf("Load: %v", err)
		}
	}
	if n := reg.total(); n != 0 {
		t.Errorf("%d registry requests across 20 warm loads, want 0: %v", n, reg.counts)
	}
}

func TestSchemaLoaderCollapsesConcurrentMisses(t *testing.T) {
	t.Parallel()

	reg := newCountingRegistry(t)
	reg.SeedSchema("widget-value", 1, 1, avro("Widget"))
	l := newSchemaLoader(reg.client(t))

	const goroutines = 32
	var wg sync.WaitGroup
	var failures atomic.Int64
	start := make(chan struct{})
	for range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if _, err := l.Load(context.Background(), 1); err != nil {
				failures.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()

	if n := failures.Load(); n != 0 {
		t.Fatalf("%d of %d concurrent loads failed", n, goroutines)
	}
	// Single-flight should turn a cold stampede into one fetch. Allow a
	// little slack for a goroutine that arrives just as the flight retires.
	if n := reg.counts["GET /schemas/ids/1"]; n > 2 {
		t.Errorf("%d fetches for one id across %d concurrent misses, want 1", n, goroutines)
	}
}

func TestSchemaLoaderWithSchemaSkipsTheFetch(t *testing.T) {
	t.Parallel()

	reg := newCountingRegistry(t)
	reg.SeedSchema("widget-value", 1, 1, avro("Widget"))
	l := newSchemaLoader(reg.client(t))

	// Confirm the tally observes a fetch at all, so the zero counts below
	// are meaningful. A throwaway loader keeps the one under test cold.
	if _, err := newSchemaLoader(reg.client(t)).Load(context.Background(), 1); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if n := reg.total(); n == 0 {
		t.Fatal("a cold Load made no observed requests; the request tally is not working")
	}
	reg.reset()

	rs, err := l.withSchema(context.Background(), 1, avro("Widget"))
	if err != nil {
		t.Fatalf("withSchema: %v", err)
	}
	if rs.Schema.Schema != avro("Widget").Schema {
		t.Errorf("schema = %q, want the one passed in", rs.Schema.Schema)
	}
	// The caller already had the text, so there is nothing to fetch: this
	// schema has no references.
	if n := reg.total(); n != 0 {
		t.Errorf("%d registry requests, want 0: %v", n, reg.counts)
	}

	// And it populates the same cache Load reads.
	if _, err := l.Load(context.Background(), 1); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if n := reg.total(); n != 0 {
		t.Errorf("%d registry requests after a Load that withSchema should have served, want 0: %v", n, reg.counts)
	}
}
