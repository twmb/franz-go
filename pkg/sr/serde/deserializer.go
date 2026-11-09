package serde

import (
	"context"
	"errors"

	"github.com/twmb/franz-go/pkg/sr"
)

// Deserializer strips the Confluent schema registry wire header and drives a
// [Decoder] for everything format-specific. It is safe for concurrent use.
//
// Resolving a schema id to schema text is the Deserializer's job, but it is
// done lazily: the registry is consulted only if the Decoder calls the
// [SchemaLoader] it is handed. A format that can unmarshal into a
// caller-supplied value without the schema - as protobuf can - therefore
// makes no registry request at all on that path.
type Deserializer struct {
	dec    Decoder
	header sr.ConfluentHeader
	loader *schemaLoader
}

// NewDeserializer returns a Deserializer that fetches schemas with cl and
// decodes with dec.
func NewDeserializer(cl *sr.Client, dec Decoder) (*Deserializer, error) {
	if cl == nil {
		return nil, errors.New("schema registry client is required")
	}
	if dec == nil {
		return nil, errors.New("decoder is required")
	}
	return &Deserializer{
		dec:    dec,
		loader: newSchemaLoader(cl),
	}, nil
}

// Decode unmarshals b into v.
func (d *Deserializer) Decode(ctx context.Context, b []byte, v any) error {
	w, err := d.wirePayload(b)
	if err != nil {
		return err
	}
	return d.dec.Unmarshal(ctx, w, d.loader, v)
}

// DecodeNew unmarshals b into a newly allocated value whose type the Decoder
// chooses from the schema the value was written with.
func (d *Deserializer) DecodeNew(ctx context.Context, b []byte) (any, error) {
	w, err := d.wirePayload(b)
	if err != nil {
		return nil, err
	}
	return d.dec.UnmarshalNew(ctx, w, d.loader)
}

func (d *Deserializer) wirePayload(b []byte) (WirePayload, error) {
	id, rest, err := d.header.DecodeID(b)
	if err != nil {
		return WirePayload{}, err
	}
	index, rest, err := d.header.DecodeIndex(rest, 0)
	if err != nil {
		return WirePayload{}, err
	}
	return WirePayload{
		ID:      id,
		Index:   index,
		Payload: rest,
	}, nil
}
