package sr

type (
	// EncodingOpt is an option to configure the behavior of Serde.Encode and
	// Serde.Decode.
	EncodingOpt interface {
		serdeOrEncodingOpt()
		apply(*tserde)
	}
	encodingOpt struct{ fn func(*tserde) }

	// SerdeOpt is an option to configure Serde.
	SerdeOpt interface {
		serdeOrEncodingOpt()
		apply(*Serde)
	}
	serdeOpt struct{ fn func(serde *Serde) }

	// SerdeOrEncodingOpt is either a SerdeOpt or EncodingOpt.
	SerdeOrEncodingOpt interface {
		serdeOrEncodingOpt()
	}
)

func (o serdeOpt) serdeOrEncodingOpt() { /* satisfy interface */ }
func (o serdeOpt) apply(s *Serde)      { o.fn(s) }

func (o encodingOpt) serdeOrEncodingOpt() { /* satisfy interface */ }
func (o encodingOpt) apply(t *tserde)     { o.fn(t) }

// EncodeFn allows Serde to encode a value.
func EncodeFn(fn func(any) ([]byte, error)) EncodingOpt {
	return encodingOpt{func(t *tserde) { t.encode = fn }}
}

// AppendEncodeFn allows Serde to encode a value to an existing slice. This
// can be more efficient than EncodeFn; this function is used if it exists.
func AppendEncodeFn(fn func([]byte, any) ([]byte, error)) EncodingOpt {
	return encodingOpt{func(t *tserde) { t.appendEncode = fn }}
}

// DecodeFn allows Serde to decode into a value.
func DecodeFn(fn func([]byte, any) error) EncodingOpt {
	return encodingOpt{func(t *tserde) { t.decode = fn }}
}

// GenerateFn returns a new(Value) that can be decoded into. This function can
// be used to control the instantiation of a new type for DecodeNew.
func GenerateFn(fn func() any) EncodingOpt {
	return encodingOpt{func(t *tserde) { t.gen = fn }}
}

// Index attaches a message index to a value. A single schema ID can be
// registered multiple times with different indices.
//
// This option supports schemas that encode many different values from the same
// schema (namely, protobuf). The index into the schema to encode a
// particular message is specified with `index`.
//
// NOTE: this option must be used for protobuf schemas.
//
// For more information, see where `message-indexes` are described in:
//
//	https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format
func Index(index ...int) EncodingOpt {
	return encodingOpt{func(t *tserde) { t.index = index }}
}

// Header defines the SerdeHeader used to encode and decode the message header.
func Header(header SerdeHeader) SerdeOpt {
	return serdeOpt{func(s *Serde) { s.h = header }}
}
