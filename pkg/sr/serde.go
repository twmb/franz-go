package sr

import (
	"encoding/binary"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
)

// The wire format for encoded types is 0, then big endian uint32 of the ID,
// then the encoded message.
//
// https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format

var (
	// ErrNotRegistered is returned from Serde when attempting to encode a
	// value or decode an ID that has not been registered, or when using
	// Decode with a missing new value function.
	ErrNotRegistered = errors.New("registration is missing for encode/decode")

	// ErrBadHeader is returned from Decode when the input slice is shorter
	// than five bytes, or if the first byte is not the magic 0 byte.
	ErrBadHeader = errors.New("5 byte header for value is missing or does no have 0 magic byte")
)

type (
	// SerdeOpt is an option to configure a Serde.
	SerdeOpt interface{ apply(*tserde) }
	serdeOpt struct{ fn func(*tserde) }
)

func (o serdeOpt) apply(t *tserde) { o.fn(t) }

// EncodeFn allows Serde to encode a value.
func EncodeFn(fn func(interface{}) ([]byte, error)) SerdeOpt {
	return serdeOpt{func(t *tserde) { t.encode = fn }}
}

// AppendEncodeFn allows Serde to encode a value to an existing slice. This
// can be more efficient than EncodeFn; this function is used if it exists.
func AppendEncodeFn(fn func([]byte, interface{}) ([]byte, error)) SerdeOpt {
	return serdeOpt{func(t *tserde) { t.appendEncode = fn }}
}

// DecodeFn allows Serde to decode into a value.
func DecodeFn(fn func([]byte, interface{}) error) SerdeOpt {
	return serdeOpt{func(t *tserde) { t.decode = fn }}
}

// NewValueFn allows Serde to generate a new value to decode into, allowing the use
// of the Decode and MustDecode functions.
func NewValueFn(fn func() interface{}) SerdeOpt {
	return serdeOpt{func(t *tserde) { t.mk = fn }}
}

type tserde struct {
	id           uint32
	encode       func(interface{}) ([]byte, error)
	appendEncode func([]byte, interface{}) ([]byte, error)
	decode       func([]byte, interface{}) error
	mk           func() interface{}
}

// Serde encodes and decodes values according to the schema registry wire
// format. A Serde itself does not perform schema auto-discovery and type
// auto-decoding. To aid in strong typing and validated encoding/decoding, you
// must register IDs and values to how to encode or decode them.
//
// To use a Serde for encoding, you must pre-register schema ids and values you
// will encode, and then you can use the encode functions.
//
// To use a Serde for decoding, you can either pre-register schema ids and
// values you will consume, or you can discover the schema every time you
// receive an ErrNotRegistered error from decode.
type Serde struct {
	ids   atomic.Value // map[int]tserde
	types atomic.Value // map[reflect.Type]tserde
	mu    sync.Mutex
}

var (
	noIDs   = make(map[int]tserde)
	noTypes = make(map[reflect.Type]tserde)
)

func (s *Serde) loadIDs() map[int]tserde {
	ids := s.ids.Load()
	if ids == nil {
		return noIDs
	}
	return ids.(map[int]tserde)
}

func (s *Serde) loadTypes() map[reflect.Type]tserde {
	types := s.types.Load()
	if types == nil {
		return noTypes
	}
	return types.(map[reflect.Type]tserde)
}

// Register registers a schema ID and the value it corresponds to, as well as
// the encoding or decoding functions. You need to register functions depending
// on whether you are only encoding, only decoding, or both.
func (s *Serde) Register(id int, v interface{}, opts ...SerdeOpt) {
	t := tserde{id: uint32(id)}
	for _, opt := range opts {
		opt.apply(&t)
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	{
		dup := make(map[int]tserde)
		for k, v := range s.loadIDs() {
			dup[k] = v
		}
		dup[id] = t
		s.ids.Store(dup)
	}

	{
		dup := make(map[reflect.Type]tserde)
		for k, v := range s.loadTypes() {
			dup[k] = v
		}
		dup[reflect.TypeOf(v)] = t
		s.types.Store(dup)
	}
}

// Encode encodes a value according to the schema registry wire format and
// returns it. If EncodeFn was not used, this returns ErrNotRegistered.
func (s *Serde) Encode(v interface{}) ([]byte, error) {
	return s.AppendEncode(nil, v)
}

// AppendEncode appends an encoded value to b according to the schema registry
// wire format and returns it. If EncodeFn was not used, this returns
// ErrNotRegistered.
func (s *Serde) AppendEncode(b []byte, v interface{}) ([]byte, error) {
	t, ok := s.loadTypes()[reflect.TypeOf(v)]
	if !ok || (t.encode == nil && t.appendEncode == nil) {
		return b, ErrNotRegistered
	}

	b = append(b,
		0,
		byte(t.id>>24),
		byte(t.id>>16),
		byte(t.id>>8),
		byte(t.id>>0),
	)

	if t.appendEncode != nil {
		return t.appendEncode(b, v)
	}
	encoded, err := t.encode(v)
	if err != nil {
		return nil, err
	}
	return append(b, encoded...), nil
}

// MustEncode returns the value of Encode, panicking on error.
func (s *Serde) MustEncode(v interface{}) []byte {
	b, err := s.Encode(v)
	if err != nil {
		panic(err)
	}
	return b
}

// MustAppendEncode returns the value of AppendEncode, panicking on error.
func (s *Serde) MustAppendEncode(b []byte, v interface{}) []byte {
	b, err := s.AppendEncode(b, v)
	if err != nil {
		panic(err)
	}
	return b
}

// Decode decodes b into a value returned by NewValueFn. If DecodeFn or
// NewValueFn options were not used, this returns ErrNotRegistered.
//
// Serde does not handle references in schemas; it is up to you to register the
// full decode function for any top-level ID, regardless of how many other
// schemas are referenced in top-level ID.
func (s *Serde) Decode(b []byte) (interface{}, error) {
	if len(b) < 5 || b[0] != 0 {
		return nil, ErrBadHeader
	}
	id := binary.BigEndian.Uint32(b[1:5])

	t, ok := s.loadIDs()[int(id)]
	if !ok || t.decode == nil || t.mk == nil {
		return b, ErrNotRegistered
	}
	v := t.mk()
	return v, t.decode(b[5:], v)
}

// DecodeInto decodes b into v. If DecodeFn option was not used, this returns
// ErrNotRegistered.
//
// Serde does not handle references in schemas; it is up to you to register the
// full decode function for any top-level ID, regardless of how many other
// schemas are referenced in top-level ID.
func (s *Serde) DecodeInto(b []byte, v interface{}) error {
	if len(b) < 5 || b[0] != 0 {
		return ErrBadHeader
	}
	id := binary.BigEndian.Uint32(b[1:5])

	t, ok := s.loadIDs()[int(id)]
	if !ok || t.decode == nil {
		return ErrNotRegistered
	}
	return t.decode(b[5:], v)
}
