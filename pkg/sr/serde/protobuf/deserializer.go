package protobuf

import (
	"context"
	"errors"
	"fmt"

	"github.com/jhump/protoreflect/desc"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"

	"github.com/twmb/franz-go/pkg/sr"
)

// Deserializer decodes Confluent schema-registry framed protobuf payloads. It
// is safe for concurrent use.
//
// Decode strips the wire header and unmarshals into the value the caller
// passes. By default it does not check that the schema id and message indexes
// describe that value; pass [VerifyDecode] to make it do so. DecodeNew always
// resolves the schema id and message indexes, and returns the matching Go
// type.
type Deserializer struct {
	client  *sr.Client
	header  sr.ConfluentHeader
	types   *protoregistry.Types
	imports map[string]string
	verify  bool

	files  *lru[int, *desc.FileDescriptor]
	flight flight[int, *desc.FileDescriptor]
}

// NewDeserializer returns a Deserializer for cl.
func NewDeserializer(cl *sr.Client, opts ...Opt) (*Deserializer, error) {
	if cl == nil {
		return nil, errors.New("schema registry client is required")
	}
	cfg := newCfg(opts)
	if cfg.types == nil {
		cfg.types = new(protoregistry.Types)
	}
	return &Deserializer{
		client:  cl,
		types:   cfg.types,
		imports: cfg.imports,
		verify:  cfg.verifyDecode,
		files:   newLRU[int, *desc.FileDescriptor](maxCacheEntries),
	}, nil
}

// RegisterMessage registers a generated protobuf message type for DecodeNew.
//
// Generated code already registers itself in protoregistry.GlobalTypes, which
// DecodeNew falls back to, so this is only needed for types that are not
// globally registered or to restrict which types may be decoded.
func (d *Deserializer) RegisterMessage(msg proto.Message) error {
	if msg == nil {
		return errors.New("nil protobuf message")
	}
	return d.types.RegisterMessage(msg.ProtoReflect().Type())
}

// Decode unmarshals b into msg. The caller must pass the correct concrete
// type; the schema id and message indexes are not checked against msg unless
// the Deserializer was built with [VerifyDecode].
func (d *Deserializer) Decode(ctx context.Context, b []byte, msg proto.Message) error {
	if msg == nil {
		return errors.New("nil protobuf message")
	}
	id, rest, err := d.header.DecodeID(b)
	if err != nil {
		return err
	}
	index, rest, err := d.header.DecodeIndex(rest, 0)
	if err != nil {
		return err
	}
	if d.verify {
		md, err := d.messageDescriptorForID(ctx, id, index)
		if err != nil {
			return err
		}
		want := string(msg.ProtoReflect().Descriptor().FullName())
		if got := md.GetFullyQualifiedName(); got != want {
			return fmt.Errorf("record was encoded as %q (schema id %d, index %v) but is being decoded into %q", got, id, index, want)
		}
	}
	return proto.Unmarshal(rest, msg)
}

// DecodeNew unmarshals b into a new message instance looked up by schema id
// and message indexes. Types are resolved from any registry passed to
// [MessageTypes] and from types added with RegisterMessage, then from
// protoregistry.GlobalTypes.
func (d *Deserializer) DecodeNew(ctx context.Context, b []byte) (proto.Message, error) {
	id, rest, err := d.header.DecodeID(b)
	if err != nil {
		return nil, err
	}
	index, rest, err := d.header.DecodeIndex(rest, 0)
	if err != nil {
		return nil, err
	}
	md, err := d.messageDescriptorForID(ctx, id, index)
	if err != nil {
		return nil, err
	}
	name := protoreflect.FullName(md.GetFullyQualifiedName())
	mt, err := d.findMessage(name)
	if err != nil {
		return nil, err
	}
	msg := mt.New().Interface()
	if err := proto.Unmarshal(rest, msg); err != nil {
		return nil, err
	}
	return msg, nil
}

func (d *Deserializer) findMessage(name protoreflect.FullName) (protoreflect.MessageType, error) {
	mt, err := d.types.FindMessageByName(name)
	if err == nil {
		return mt, nil
	}
	if !errors.Is(err, protoregistry.NotFound) {
		return nil, err
	}
	mt, err = protoregistry.GlobalTypes.FindMessageByName(name)
	if err != nil {
		if errors.Is(err, protoregistry.NotFound) {
			return nil, fmt.Errorf("message type %q is not registered: import the generated Go package for it, or add it with RegisterMessage", name)
		}
		return nil, err
	}
	return mt, nil
}

func (d *Deserializer) messageDescriptorForID(ctx context.Context, id int, index []int) (*desc.MessageDescriptor, error) {
	fd, err := d.fileForID(ctx, id)
	if err != nil {
		return nil, err
	}
	return messageDescriptorAt(fd, index)
}

func (d *Deserializer) fileForID(ctx context.Context, id int) (*desc.FileDescriptor, error) {
	fd, ok := d.files.get(id)
	if ok {
		return fd, nil
	}

	// Collapse concurrent misses for one id into a single registry fetch;
	// otherwise a cold cache under load fetches once per in-flight record.
	return d.flight.do(id, func() (*desc.FileDescriptor, error) {
		fd, ok := d.files.get(id)
		if ok {
			return fd, nil
		}

		s, err := d.client.SchemaByID(ctx, id)
		if err != nil {
			return nil, err
		}
		if s.Type != sr.TypeProtobuf {
			return nil, fmt.Errorf("schema id %d has type %s, not protobuf", id, s.Type)
		}
		deps := make(map[string]string, len(d.imports))
		for k, v := range d.imports {
			deps[k] = v
		}
		err = resolveReferences(s, func(subject string, version int) (sr.Schema, error) {
			ss, err := d.client.SchemaByVersion(ctx, subject, version)
			if err != nil {
				return sr.Schema{}, err
			}
			return ss.Schema, nil
		}, deps)
		if err != nil {
			return nil, err
		}
		fd, err = parseSchema(s, deps)
		if err != nil {
			return nil, err
		}

		d.files.add(id, fd)
		return fd, nil
	})
}
