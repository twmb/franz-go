package protobuf

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoparse"

	"github.com/twmb/franz-go/pkg/sr"
)

// rootFileName is the synthetic name the schema under decode is parsed as.
// Schema text fetched from the registry has no file name of its own.
const rootFileName = "schema.proto"

// resolveReferences loads schema and every schema it references, including
// references of references, into deps keyed by file name. A subject/version
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

// unresolvedImport reports an import that is in neither the schema's registry
// references nor this binary, and names the fix rather than just the symptom.
//
// protoparse consults the accessor first and only falls back to LookupImport
// when the accessor fails, but reports the *first* error when every resolver
// fails. So this is the error the accessor returns for anything it does not
// have, and it is what surfaces when the import cannot be found anywhere.
type unresolvedImport struct{ name string }

func (e *unresolvedImport) Error() string {
	return fmt.Sprintf("no schema for import %q: it is not a registered reference of this schema "+
		"and no generated Go package providing it is linked into this binary; either blank-import "+
		"the generated package (for google/type/latlng.proto that is "+
		`_ "google.golang.org/genproto/googleapis/type/latlng") or supply the file with the `+
		"Imports option", e.name)
}

// parseSchema compiles schema text into a file descriptor. Imports resolve in
// the order protoparse consults them: deps (schemas fetched from the registry
// as references, plus anything supplied with the Imports option), then the
// protobuf files linked into this binary, then the well-known google/protobuf
// types that protoparse ships with.
func parseSchema(schema sr.Schema, deps map[string]string) (*desc.FileDescriptor, error) {
	parser := protoparse.Parser{
		Accessor: func(filename string) (io.ReadCloser, error) {
			if filename == rootFileName {
				return io.NopCloser(strings.NewReader(schema.Schema)), nil
			}
			if s, ok := deps[filename]; ok {
				return io.NopCloser(strings.NewReader(s)), nil
			}
			return nil, &unresolvedImport{name: filename}
		},
		// Resolve anything the registry did not supply against the files
		// compiled into this binary. Generated Go code registers its
		// descriptors in protoregistry.GlobalFiles at init, so a schema
		// importing google/type/latlng.proto or confluent/meta.proto resolves
		// as long as the generated package is linked in. On the decode path it
		// always is: the message being decoded must be registered, and its
		// generated file imports everything its own schema imports.
		LookupImport: desc.LoadFileDescriptor,
	}
	fds, err := parser.ParseFiles(rootFileName)
	if err != nil {
		// Surface our own message out of protoparse's position wrapping.
		var u *unresolvedImport
		if errors.As(err, &u) {
			return nil, u
		}
		return nil, err
	}
	return fds[0], nil
}

func messageDescriptorAt(fd *desc.FileDescriptor, indexes []int) (*desc.MessageDescriptor, error) {
	if len(indexes) == 0 {
		return nil, errors.New("empty message indexes")
	}
	var cur *desc.MessageDescriptor
	msgs := fd.GetMessageTypes()
	for _, idx := range indexes {
		if idx < 0 || idx >= len(msgs) {
			return nil, fmt.Errorf("message index %v is out of range for the schema", indexes)
		}
		cur = msgs[idx]
		msgs = cur.GetNestedMessageTypes()
	}
	return cur, nil
}
