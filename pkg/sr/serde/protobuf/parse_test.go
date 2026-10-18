package protobuf

import (
	"strings"
	"testing"

	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/twmb/franz-go/pkg/sr"

	// Linked so its descriptors are in protoregistry.GlobalFiles, which is
	// what the import-resolution tests below exercise.
	_ "github.com/twmb/franz-go/pkg/sr/serde/protobuf/testpb"
)

func protoSchema(text string) sr.Schema {
	return sr.Schema{Schema: text, Type: sr.TypeProtobuf}
}

// The well-known types ship with protoparse, so they resolve with nothing
// registered as a reference and nothing supplied by the caller.
func TestParseResolvesWellKnownTypes(t *testing.T) {
	for _, imp := range []string{
		"google/protobuf/timestamp.proto",
		"google/protobuf/any.proto",
		"google/protobuf/descriptor.proto",
		"google/protobuf/struct.proto",
		"google/protobuf/wrappers.proto",
	} {
		t.Run(imp, func(t *testing.T) {
			_, err := parseSchema(protoSchema(
				`syntax = "proto3"; import "`+imp+`"; message M { string name = 1; }`,
			), nil)
			if err != nil {
				t.Fatal(err)
			}
		})
	}
}

// Anything whose generated Go package is linked into the binary resolves from
// protoregistry.GlobalFiles, without being a registered schema reference.
// This is what makes vendoring google/type and confluent protos unnecessary.
func TestParseResolvesLinkedFiles(t *testing.T) {
	if _, err := protoregistry.GlobalFiles.FindFileByPath("dep.proto"); err != nil {
		t.Skipf("dep.proto is not globally registered: %v", err)
	}
	fd, err := parseSchema(protoSchema(
		`syntax = "proto3"; package testpb; import "dep.proto"; message M { testpb.Dep d = 1; }`,
	), nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := fd.GetMessageTypes()[0].GetFullyQualifiedName(); got != "testpb.M" {
		t.Fatalf("parsed %q", got)
	}
}

// An import that is neither a registry reference nor linked in must say what
// to do about it, not just that it is missing.
func TestParseUnresolvedImportIsActionable(t *testing.T) {
	_, err := parseSchema(protoSchema(
		`syntax = "proto3"; import "totally/made/up.proto"; message M { string name = 1; }`,
	), nil)
	if err == nil {
		t.Fatal("expected an error for an unresolvable import")
	}
	msg := err.Error()
	for _, want := range []string{"totally/made/up.proto", "blank-import", "Imports option"} {
		if !strings.Contains(msg, want) {
			t.Fatalf("error should mention %q: %v", want, msg)
		}
	}
}

// The Imports escape hatch covers files that are neither references nor
// linked in.
func TestParseUsesSuppliedImports(t *testing.T) {
	deps := map[string]string{
		"extra/thing.proto": `syntax = "proto3"; package extra; message Thing { string id = 1; }`,
	}
	fd, err := parseSchema(protoSchema(
		`syntax = "proto3"; import "extra/thing.proto"; message M { extra.Thing t = 1; }`,
	), deps)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(fd.GetMessageTypes()); n != 1 {
		t.Fatalf("message types %d", n)
	}
}

// Registry references take priority over anything linked in, so a schema
// evolves with the registry rather than with the local build.
func TestParsePrefersReferencesOverLinkedFiles(t *testing.T) {
	deps := map[string]string{
		"dep.proto": `syntax = "proto3"; package testpb; message Dep { string id = 1; int64 extra = 2; }`,
	}
	fd, err := parseSchema(protoSchema(
		`syntax = "proto3"; package testpb; import "dep.proto"; message M { testpb.Dep d = 1; }`,
	), deps)
	if err != nil {
		t.Fatal(err)
	}
	// The linked dep.proto has one field; the reference text above has two.
	if got := len(fd.GetDependencies()[0].GetMessageTypes()[0].GetFields()); got != 2 {
		t.Fatalf("dep.proto resolved with %d fields, want the 2 from the reference text "+
			"(the registry reference must win over the linked descriptor)", got)
	}
}

func TestResolveReferencesDetectsCycles(t *testing.T) {
	// a.proto references b.proto, which references a.proto again.
	schemas := map[string]sr.Schema{
		"a": {
			Schema:     `syntax = "proto3"; message A {}`,
			Type:       sr.TypeProtobuf,
			References: []sr.SchemaReference{{Name: "b.proto", Subject: "b", Version: 1}},
		},
		"b": {
			Schema:     `syntax = "proto3"; message B {}`,
			Type:       sr.TypeProtobuf,
			References: []sr.SchemaReference{{Name: "a.proto", Subject: "a", Version: 1}},
		},
	}
	root := sr.Schema{
		Schema:     `syntax = "proto3"; message Root {}`,
		Type:       sr.TypeProtobuf,
		References: []sr.SchemaReference{{Name: "a.proto", Subject: "a", Version: 1}},
	}

	err := resolveReferences(root, func(subject string, _ int) (sr.Schema, error) {
		return schemas[subject], nil
	}, make(map[string]string))
	if err == nil {
		t.Fatal("expected a circular reference error")
	}
	if !strings.Contains(err.Error(), "circular schema reference") {
		t.Fatalf("err %v", err)
	}
}

// A diamond import must be fetched once per subject/version, not once per
// path through the graph.
func TestResolveReferencesFetchesDiamondOnce(t *testing.T) {
	leaf := sr.Schema{Schema: `syntax = "proto3"; message Leaf {}`, Type: sr.TypeProtobuf}
	mid := func(name string) sr.Schema {
		return sr.Schema{
			Schema:     `syntax = "proto3"; message ` + name + ` {}`,
			Type:       sr.TypeProtobuf,
			References: []sr.SchemaReference{{Name: "leaf.proto", Subject: "leaf", Version: 1}},
		}
	}
	root := sr.Schema{
		Schema: `syntax = "proto3"; message Root {}`,
		Type:   sr.TypeProtobuf,
		References: []sr.SchemaReference{
			{Name: "left.proto", Subject: "left", Version: 1},
			{Name: "right.proto", Subject: "right", Version: 1},
		},
	}

	fetches := make(map[string]int)
	deps := make(map[string]string)
	err := resolveReferences(root, func(subject string, _ int) (sr.Schema, error) {
		fetches[subject]++
		switch subject {
		case "left":
			return mid("Left"), nil
		case "right":
			return mid("Right"), nil
		default:
			return leaf, nil
		}
	}, deps)
	if err != nil {
		t.Fatal(err)
	}
	if fetches["leaf"] != 1 {
		t.Fatalf("leaf fetched %d times, want 1", fetches["leaf"])
	}
	if len(deps) != 3 {
		t.Fatalf("deps %v", deps)
	}
}
