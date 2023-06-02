package sr

import (
	"bytes"
	"encoding/binary"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
)

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
func EncodeFn(fn func(any) ([]byte, error)) SerdeOpt {
	return serdeOpt{func(t *tserde) { t.encode = fn }}
}

// AppendEncodeFn allows Serde to encode a value to an existing slice. This
// can be more efficient than EncodeFn; this function is used if it exists.
func AppendEncodeFn(fn func([]byte, any) ([]byte, error)) SerdeOpt {
	return serdeOpt{func(t *tserde) { t.appendEncode = fn }}
}

// DecodeFn allows Serde to decode into a value.
func DecodeFn(fn func([]byte, any) error) SerdeOpt {
	return serdeOpt{func(t *tserde) { t.decode = fn }}
}

// GenerateFn returns a new(Value) that can be decoded into. This function can
// be used to control the instantiation of a new type for DecodeNew.
func GenerateFn(fn func() any) SerdeOpt {
	return serdeOpt{func(t *tserde) { t.gen = fn }}
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
func Index(index ...int) SerdeOpt {
	return serdeOpt{func(t *tserde) { t.index = index }}
}

type tserde struct {
	id           uint32
	exists       bool
	encode       func(any) ([]byte, error)
	appendEncode func([]byte, any) ([]byte, error)
	decode       func([]byte, any) error
	gen          func() any
	typeof       reflect.Type

	index    []int          // for encoding, an optional index we use
	subindex map[int]tserde // for decoding, we look up sub-indices in the payload
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

	defaults []SerdeOpt
	header   SerdeHeader
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

// SetDefaults sets default options to apply to every registered type. These
// options are always applied first, so you can override them as necessary when
// registering.
//
// This can be useful if you always want to use the same encoding or decoding
// functions.
func (s *Serde) SetDefaults(opts ...SerdeOpt) {
	s.defaults = opts
}

// SetHeader configures which header should be used when encoding and decoding
// values. If the header is set to nil it falls back to ConfluentHeader.
func (s *Serde) SetHeader(header SerdeHeader) {
	s.header = header
}

// Header returns the configured header.
func (s *Serde) Header() SerdeHeader {
	if s.header == nil {
		return ConfluentHeader{}
	}
	return s.header
}

// Register registers a schema ID and the value it corresponds to, as well as
// the encoding or decoding functions. You need to register functions depending
// on whether you are only encoding, only decoding, or both.
func (s *Serde) Register(id int, v any, opts ...SerdeOpt) {
	var t tserde
	for _, opt := range s.defaults {
		opt.apply(&t)
	}
	for _, opt := range opts {
		opt.apply(&t)
	}

	typeof := reflect.TypeOf(v)

	s.mu.Lock()
	defer s.mu.Unlock()

	// Type mapping is easy: we just map the type to the final tserde.
	// We defer the store because we modify the tserde below, and we
	// may delete a type key.
	dupTypes := make(map[reflect.Type]tserde)
	for k, v := range s.loadTypes() {
		dupTypes[k] = v
	}
	defer func() {
		dupTypes[typeof] = t
		s.types.Store(dupTypes)
	}()

	// For IDs, we deeply clone any path that is changing.
	m := tserdeMapClone(s.loadIDs(), id, t.index)
	s.ids.Store(m)

	// Now we have a full path down index initialized (or, the top
	// level map if there is no index). We iterate down the index
	// tree to find the end node we are initializing.
	k := id
	at := m[k]
	for _, idx := range t.index {
		m = at.subindex
		k = idx
		at = m[k]
	}

	// If this value was already registered, we are overriding the
	// previous registration and need to delete the previous type.
	if at.exists {
		delete(dupTypes, at.typeof)
	}

	// Now, we initialize the end node.
	t = tserde{
		id:           uint32(id),
		exists:       true,
		encode:       t.encode,
		appendEncode: t.appendEncode,
		decode:       t.decode,
		gen:          t.gen,
		typeof:       typeof,
		index:        t.index,
		subindex:     at.subindex,
	}
	m[k] = t
}

func tserdeMapClone(m map[int]tserde, at int, index []int) map[int]tserde {
	// We deeply clone the input map (or initialize it).
	dup := make(map[int]tserde, len(m))
	for k, v := range m {
		dup[k] = v
	}

	// If there is an index, we want to ensure the full path to the index
	// is initialized. Our input "at" is what will get us *to* this index,
	// and then we need to recurse down index. The caller then maps down
	// our fully initialized subindex tree and ensures that the terminal
	// tserde (the end of index) is initialized.
	if len(index) > 0 {
		sub := dup[at]
		sub.subindex = tserdeMapClone(sub.subindex, index[0], index[1:])
		dup[at] = sub
	}
	return dup
}

// Encode encodes a value and prepends the header according to the configured
// SerdeHeader. If EncodeFn was not used, this returns ErrNotRegistered.
func (s *Serde) Encode(v any) ([]byte, error) {
	return s.AppendEncode(nil, v)
}

// AppendEncode appends an encoded value to b according to the schema registry
// wire format and returns it. If EncodeFn was not used, this returns
// ErrNotRegistered.
func (s *Serde) AppendEncode(b []byte, v any) ([]byte, error) {
	t, ok := s.loadTypes()[reflect.TypeOf(v)]
	if !ok || (t.encode == nil && t.appendEncode == nil) {
		return b, ErrNotRegistered
	}

	b, err := s.Header().AppendEncode(b, int(t.id), t.index)
	if err != nil {
		return nil, err
	}

	if t.appendEncode != nil {
		return t.appendEncode(b, v)
	}
	encoded, err := t.encode(v)
	if err != nil {
		return nil, err
	}
	return append(b, encoded...), nil
}

// MustEncode returns the value of Encode, panicking on error. This is a
// shortcut for if your encode function cannot error.
func (s *Serde) MustEncode(v any) []byte {
	b, err := s.Encode(v)
	if err != nil {
		panic(err)
	}
	return b
}

// MustAppendEncode returns the value of AppendEncode, panicking on error.
// This is a shortcut for if your encode function cannot error.
func (s *Serde) MustAppendEncode(b []byte, v any) []byte {
	b, err := s.AppendEncode(b, v)
	if err != nil {
		panic(err)
	}
	return b
}

// Decode decodes b into v. If DecodeFn option was not used, this returns
// ErrNotRegistered.
//
// Serde does not handle references in schemas; it is up to you to register the
// full decode function for any top-level ID, regardless of how many other
// schemas are referenced in top-level ID.
func (s *Serde) Decode(b []byte, v any) error {
	b, t, err := s.decodeFind(b)
	if err != nil {
		return err
	}
	return t.decode(b, v)
}

// DecodeNew is the same as Decode, but decodes into a new value rather than
// the input value. If DecodeFn was not used, this returns ErrNotRegistered.
// GenerateFn can be used to control the instantiation of a new value,
// otherwise this uses reflect.New(reflect.TypeOf(v)).Interface().
func (s *Serde) DecodeNew(b []byte) (any, error) {
	b, t, err := s.decodeFind(b)
	if err != nil {
		return nil, err
	}
	var v any
	if t.gen != nil {
		v = t.gen()
	} else {
		v = reflect.New(t.typeof).Interface()
	}
	return v, t.decode(b, v)
}

func (s *Serde) decodeFind(b []byte) ([]byte, tserde, error) {
	h := s.Header()

	id, b, err := h.DecodeID(b)
	if err != nil {
		return nil, tserde{}, err
	}

	t := s.loadIDs()[id]
	if len(t.subindex) > 0 {
		var index []int
		index, b, err = h.DecodeIndex(b)
		if err != nil {
			return nil, tserde{}, err
		}
		for _, idx := range index {
			if t.subindex == nil {
				return nil, tserde{}, ErrNotRegistered
			}
			t = t.subindex[idx]
		}
	}
	if !t.exists {
		return nil, tserde{}, ErrNotRegistered
	}
	return b, t, nil
}

// SerdeHeader encodes and decodes a message header.
type SerdeHeader interface {
	AppendEncode(b []byte, id int, index []int) ([]byte, error)
	DecodeID(in []byte) (id int, out []byte, err error)
	DecodeIndex(in []byte) (index []int, out []byte, err error)
}

// ConfluentHeader is a SerdeHeader that produces the Confluent wire format. It
// starts with 0, then big endian uint32 of the ID, then index (only protobuf),
// then the encoded message.
//
// https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format
type ConfluentHeader struct{}

// AppendEncode appends an encoded header to b according to the Confluent wire
// format and returns it. Error is always nil.
func (ConfluentHeader) AppendEncode(b []byte, id int, index []int) ([]byte, error) {
	b = append(
		b,
		0,
		byte(id>>24),
		byte(id>>16),
		byte(id>>8),
		byte(id>>0),
	)

	if len(index) > 0 {
		if len(index) == 1 && index[0] == 0 {
			b = append(b, 0) // first-index shortcut (one type in the protobuf)
		} else {
			b = binary.AppendVarint(b, int64(len(index)))
			for _, idx := range index {
				b = binary.AppendVarint(b, int64(idx))
			}
		}
	}

	return b, nil
}

// DecodeID strips and decodes the schema ID from b. It returns the ID alongside
// the unread bytes. If the header does not contain the magic byte or b contains
// less than 5 bytes it returns ErrBadHeader.
func (c ConfluentHeader) DecodeID(b []byte) (int, []byte, error) {
	if len(b) < 5 || b[0] != 0 {
		return 0, nil, ErrBadHeader
	}
	id := binary.BigEndian.Uint32(b[1:5])
	return int(id), b[5:], nil
}

// DecodeIndex strips and decodes the index from b. It returns the index
// alongside the unread bytes. It expects b to be the output of DecodeID (schema
// ID should already be stripped away).
func (c ConfluentHeader) DecodeIndex(b []byte) ([]int, []byte, error) {
	buf := bytes.NewBuffer(b)
	l, err := binary.ReadVarint(buf)
	if err != nil {
		return nil, nil, err
	}
	if l == 0 { // length 0 is a shortcut for length 1, index 0
		return []int{0}, buf.Bytes(), nil
	}
	index := make([]int, l)
	for i := range index {
		idx, err := binary.ReadVarint(buf)
		if err != nil {
			return nil, nil, err
		}
		index[i] = int(idx)
	}
	return index, buf.Bytes(), nil
}
