package serde

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/pkg/sr/srfake"
)

func TestSubject(t *testing.T) {
	tests := []struct {
		topic, record string
		part          Part
		strategy      Strategy
		want          string
		wantErr       bool
	}{
		{"foo", "", PartValue, TopicName, "foo-value", false},
		{"foo", "", PartKey, TopicName, "foo-key", false},
		{"", "com.example.Widget", PartValue, RecordName, "com.example.Widget", false},
		{"", "", PartValue, RecordName, "", true},
		{"orders", "com.example.Widget", PartValue, TopicRecordName, "orders-com.example.Widget", false},
	}
	for _, tt := range tests {
		got, err := Subject(tt.topic, tt.part, tt.strategy, tt.record)
		if tt.wantErr {
			if err == nil {
				t.Fatalf("expected error for %+v", tt)
			}
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if got != tt.want {
			t.Fatalf("got %q want %q", got, tt.want)
		}
	}
}

func protoSchema(msg string) sr.Schema {
	return sr.Schema{
		Schema: `syntax = "proto3"; message ` + msg + ` {}`,
		Type:   sr.TypeProtobuf,
	}
}

func staticSchema(s sr.Schema) func(context.Context) (sr.Schema, error) {
	return func(context.Context) (sr.Schema, error) { return s, nil }
}

func TestLatestCacheTTL(t *testing.T) {
	ctx := context.Background()
	reg := srfake.New()
	t.Cleanup(reg.Close)
	cl, err := sr.NewClient(sr.URLs(reg.URL()))
	if err != nil {
		t.Fatal(err)
	}
	reg.SeedSchema("topic-value", 1, 1, protoSchema("A"))

	r := NewResolver(cl, UseLatestVersion(true), LatestCacheTTL(time.Minute))
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	r.now = func() time.Time { return now }

	ss, err := r.ResolveSubjectSchema(ctx, "topic", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if ss.Version != 1 {
		t.Fatalf("version %d", ss.Version)
	}

	if _, _, err := reg.RegisterSchema("topic-value", protoSchema("B")); err != nil {
		t.Fatal(err)
	}

	ss, err = r.ResolveSubjectSchema(ctx, "topic", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if ss.Version != 1 {
		t.Fatalf("cached version %d", ss.Version)
	}

	now = now.Add(time.Minute)
	ss, err = r.ResolveSubjectSchema(ctx, "topic", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if ss.Version != 2 {
		t.Fatalf("refetched version %d", ss.Version)
	}

	fresh := NewResolver(cl, UseLatestVersion(true), LatestCacheTTL(0))
	fresh.now = func() time.Time { return now }
	if _, _, err := reg.RegisterSchema("topic-value", protoSchema("C")); err != nil {
		t.Fatal(err)
	}
	ss, err = fresh.ResolveSubjectSchema(ctx, "topic", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if ss.Version != 3 {
		t.Fatalf("uncached version %d", ss.Version)
	}
	ss, err = fresh.ResolveSubjectSchema(ctx, "topic", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if ss.Version != 3 {
		t.Fatalf("uncached version %d", ss.Version)
	}
	if _, _, err := reg.RegisterSchema("topic-value", protoSchema("D")); err != nil {
		t.Fatal(err)
	}
	ss, err = fresh.ResolveSubjectSchema(ctx, "topic", "", nil)
	if err != nil {
		t.Fatal(err)
	}
	if ss.Version != 4 {
		t.Fatalf("second uncached version %d", ss.Version)
	}
}

// A pinned schema id must be fetched once, not once per resolve.
func TestUseSchemaIDResolvesOnce(t *testing.T) {
	ctx := context.Background()
	reg := srfake.New()
	t.Cleanup(reg.Close)

	// The pinned path issues two requests the first time: the schema itself,
	// and the subject versions it is registered under so Version is set.
	// Neither may repeat per resolve.
	var byID, total atomic.Int32
	reg.Intercept(func(_ http.ResponseWriter, req *http.Request) bool {
		if strings.HasPrefix(req.URL.Path, "/schemas/ids/") {
			total.Add(1)
			if req.URL.Path == "/schemas/ids/42" {
				byID.Add(1)
			}
		}
		return false
	})

	cl, err := sr.NewClient(sr.URLs(reg.URL()))
	if err != nil {
		t.Fatal(err)
	}
	reg.SeedSchema("topic-value", 7, 42, protoSchema("A"))

	r := NewResolver(cl, UseSchemaID(42))
	for range 10 {
		ss, err := r.ResolveSubjectSchema(ctx, "topic", "", nil)
		if err != nil {
			t.Fatal(err)
		}
		if ss.ID != 42 {
			t.Fatalf("id %d want 42", ss.ID)
		}
		if ss.Version != 7 {
			t.Fatalf("version %d want 7", ss.Version)
		}
	}
	if n := byID.Load(); n != 1 {
		t.Fatalf("SchemaByID calls %d, want 1", n)
	}
	if n := total.Load(); n != 2 {
		t.Fatalf("schema id requests %d for 10 resolves, want 2", n)
	}
}

// Concurrent cold resolves for one subject must collapse into one request.
func TestRegisterSingleFlight(t *testing.T) {
	ctx := context.Background()
	reg := srfake.New()
	t.Cleanup(reg.Close)

	var posts atomic.Int32
	reg.Intercept(func(_ http.ResponseWriter, req *http.Request) bool {
		if req.Method == http.MethodPost && strings.HasSuffix(req.URL.Path, "/versions") {
			posts.Add(1)
		}
		return false
	})

	cl, err := sr.NewClient(sr.URLs(reg.URL()))
	if err != nil {
		t.Fatal(err)
	}
	r := NewResolver(cl)
	schema := protoSchema("A")

	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := r.ResolveSubjectSchema(ctx, "topic", "", staticSchema(schema)); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()

	if n := posts.Load(); n != 1 {
		t.Fatalf("register calls %d, want 1", n)
	}
}

// deriveSchema must not be consulted when the id is pinned or the latest
// version is in use; those paths never need schema text from the caller.
func TestDeriveSchemaNotCalledWhenUnneeded(t *testing.T) {
	ctx := context.Background()
	reg := srfake.New()
	t.Cleanup(reg.Close)
	cl, err := sr.NewClient(sr.URLs(reg.URL()))
	if err != nil {
		t.Fatal(err)
	}
	reg.SeedSchema("topic-value", 1, 3, protoSchema("A"))

	derive := func(called *bool) func(context.Context) (sr.Schema, error) {
		return func(context.Context) (sr.Schema, error) {
			*called = true
			return protoSchema("A"), nil
		}
	}

	for _, tt := range []struct {
		name string
		opt  Opt
	}{
		{"pinned", UseSchemaID(3)},
		{"latest", UseLatestVersion(true)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var called bool
			r := NewResolver(cl, tt.opt)
			if _, err := r.ResolveSubjectSchema(ctx, "topic", "", derive(&called)); err != nil {
				t.Fatal(err)
			}
			if called {
				t.Fatal("deriveSchema was called but the schema text was not needed")
			}
		})
	}
}

func TestReferenceSubject(t *testing.T) {
	ctx := context.Background()
	reg := srfake.New()
	t.Cleanup(reg.Close)
	cl, err := sr.NewClient(sr.URLs(reg.URL()))
	if err != nil {
		t.Fatal(err)
	}

	r := NewResolver(cl, ReferenceSubject(func(name string) string { return "prefix/" + name }))
	ss, err := r.RegisterDependency(ctx, "dep.proto", protoSchema("Dep"))
	if err != nil {
		t.Fatal(err)
	}
	if ss.Subject != "prefix/dep.proto" {
		t.Fatalf("subject %q", ss.Subject)
	}

	plain := NewResolver(cl)
	ss, err = plain.RegisterDependency(ctx, "dep.proto", protoSchema("Dep"))
	if err != nil {
		t.Fatal(err)
	}
	if ss.Subject != "dep.proto" {
		t.Fatalf("subject %q", ss.Subject)
	}
}

func TestStableIDs(t *testing.T) {
	cl, err := sr.NewClient(sr.URLs("http://localhost:1"))
	if err != nil {
		t.Fatal(err)
	}
	if !NewResolver(cl).StableIDs() {
		t.Fatal("register/lookup ids should be stable")
	}
	if !NewResolver(cl, UseSchemaID(1)).StableIDs() {
		t.Fatal("pinned ids should be stable")
	}
	if NewResolver(cl, UseLatestVersion(true)).StableIDs() {
		t.Fatal("latest-version ids should not be stable")
	}
}
