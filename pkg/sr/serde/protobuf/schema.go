package protobuf

import (
	"context"
	"strings"
	"sync"

	"github.com/jhump/protoreflect/desc"
	"github.com/jhump/protoreflect/desc/protoprint"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"

	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/pkg/sr/serde"
)

type schemaBuilder struct {
	resolver    *serde.Resolver
	keepCodegen bool
	cache       sync.Map // file name -> sr.Schema
}

func newSchemaBuilder(r *serde.Resolver, keepCodegenOptions bool) *schemaBuilder {
	return &schemaBuilder{resolver: r, keepCodegen: keepCodegenOptions}
}

func (b *schemaBuilder) schemaForMessage(ctx context.Context, md protoreflect.MessageDescriptor) (sr.Schema, error) {
	file, err := desc.WrapFile(md.ParentFile())
	if err != nil {
		return sr.Schema{}, err
	}
	return b.schemaForFile(ctx, file)
}

func (b *schemaBuilder) schemaForFile(ctx context.Context, file *desc.FileDescriptor) (sr.Schema, error) {
	name := file.GetName()
	if v, ok := b.cache.Load(name); ok {
		return v.(sr.Schema), nil
	}
	deps, err := b.printDependencies(file)
	if err != nil {
		return sr.Schema{}, err
	}
	schema, err := b.buildSchema(ctx, file, deps)
	if err != nil {
		return sr.Schema{}, err
	}
	b.cache.Store(name, schema)
	return schema, nil
}

func (b *schemaBuilder) printDependencies(file *desc.FileDescriptor) (map[string]string, error) {
	deps := make(map[string]string)
	printer := protoprint.Printer{OmitComments: protoprint.CommentsAll}
	if err := b.collectDeps(file, deps, printer); err != nil {
		return nil, err
	}
	return deps, nil
}

func (b *schemaBuilder) collectDeps(file *desc.FileDescriptor, deps map[string]string, printer protoprint.Printer) error {
	if _, ok := deps[file.GetName()]; ok {
		return nil
	}
	text, err := b.print(file, printer)
	if err != nil {
		return err
	}
	deps[file.GetName()] = text
	for _, d := range file.GetDependencies() {
		if ignoreFile(d.GetName()) {
			continue
		}
		if err := b.collectDeps(d, deps, printer); err != nil {
			return err
		}
	}
	for _, d := range file.GetPublicDependencies() {
		if ignoreFile(d.GetName()) {
			continue
		}
		if err := b.collectDeps(d, deps, printer); err != nil {
			return err
		}
	}
	return nil
}

func (b *schemaBuilder) print(file *desc.FileDescriptor, printer protoprint.Printer) (string, error) {
	if !b.keepCodegen {
		stripped, err := stripCodegenOptions(file)
		if err != nil {
			return "", err
		}
		file = stripped
	}
	var w strings.Builder
	if err := printer.PrintProtoFile(file, &w); err != nil {
		return "", err
	}
	return w.String(), nil
}

// stripCodegenOptions returns file with its per-language codegen options
// cleared. Those options say nothing about the wire format and differ for the
// same .proto across languages, so leaving them in makes the registered schema
// depend on which language registered it.
//
// This operates on the descriptor rather than the printed text: matching
// "option go_package = ..." lines in output is fragile, and clearing the
// fields is exactly as precise as the descriptor itself.
func stripCodegenOptions(file *desc.FileDescriptor) (*desc.FileDescriptor, error) {
	fdp := file.AsFileDescriptorProto()
	if fdp.GetOptions() == nil {
		return file, nil
	}

	clone := proto.Clone(fdp).(*descriptorpb.FileDescriptorProto)
	o := clone.Options
	o.GoPackage = nil
	o.JavaPackage = nil
	o.JavaOuterClassname = nil
	o.JavaMultipleFiles = nil
	o.JavaStringCheckUtf8 = nil //nolint:staticcheck // cleared precisely because it is a deprecated codegen option
	o.JavaGenericServices = nil
	o.CcGenericServices = nil
	o.CcEnableArenas = nil
	o.PyGenericServices = nil
	o.PhpNamespace = nil
	o.PhpMetadataNamespace = nil
	o.PhpClassPrefix = nil
	o.RubyPackage = nil
	o.CsharpNamespace = nil
	o.SwiftPrefix = nil
	o.ObjcClassPrefix = nil

	// Rebuild against the same dependencies so the descriptor stays linked.
	deps := make([]*desc.FileDescriptor, 0, len(file.GetDependencies()))
	deps = append(deps, file.GetDependencies()...)
	return desc.CreateFileDescriptor(clone, deps...)
}

func (b *schemaBuilder) buildSchema(ctx context.Context, file *desc.FileDescriptor, deps map[string]string) (sr.Schema, error) {
	refs, err := b.importReferences(ctx, file, deps)
	if err != nil {
		return sr.Schema{}, err
	}
	return sr.Schema{
		Schema:     deps[file.GetName()],
		Type:       sr.TypeProtobuf,
		References: refs,
	}, nil
}

func (b *schemaBuilder) importReferences(ctx context.Context, file *desc.FileDescriptor, deps map[string]string) ([]sr.SchemaReference, error) {
	seen := make(map[string]struct{})
	var refs []sr.SchemaReference
	add := func(d *desc.FileDescriptor) error {
		name := d.GetName()
		if ignoreFile(name) {
			return nil
		}
		if _, ok := seen[name]; ok {
			return nil
		}
		seen[name] = struct{}{}
		refSchema, err := b.buildSchema(ctx, d, deps)
		if err != nil {
			return err
		}
		ss, err := b.resolver.RegisterDependency(ctx, name, refSchema)
		if err != nil {
			return err
		}
		refs = append(refs, sr.SchemaReference{
			Name:    name,
			Subject: ss.Subject,
			Version: ss.Version,
		})
		return nil
	}
	for _, d := range file.GetDependencies() {
		if err := add(d); err != nil {
			return nil, err
		}
	}
	for _, d := range file.GetPublicDependencies() {
		if err := add(d); err != nil {
			return nil, err
		}
	}
	return refs, nil
}

// ignoreFile reports imports that are not registered as references. Every
// schema registry client resolves these from its own runtime rather than from
// the registry, so registering them would only create subjects nobody reads.
func ignoreFile(name string) bool {
	return strings.HasPrefix(name, "confluent/") ||
		strings.HasPrefix(name, "google/protobuf/") ||
		strings.HasPrefix(name, "google/type/")
}

// messageIndexArray returns the Confluent message-index path to descriptor:
// its index within the file, followed by the index of each nested message on
// the way down. sr.ConfluentHeader encodes the common [0] case as a single
// zero byte.
func messageIndexArray(descriptor protoreflect.Descriptor) []int {
	return toMessageIndexes(descriptor, 0)
}

// toMessageIndexes walks up to the file, then fills the path back in on the
// way down. depth is only used to size the slice once at the top so the
// appends below do not reallocate.
func toMessageIndexes(descriptor protoreflect.Descriptor, depth int) []int {
	index := descriptor.Index()
	parent, nested := descriptor.Parent().(protoreflect.MessageDescriptor)
	if !nested {
		indexes := make([]int, 1, depth+1)
		indexes[0] = index
		return indexes
	}
	return append(toMessageIndexes(parent, depth+1), index)
}
