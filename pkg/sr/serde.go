package sr

import (
	"encoding/binary"
	"errors"
	"io"
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
	SerdeOpt interface{ applySerde(*tserde) }
	serdeOpt struct{ fn func(*tserde) }

	// EncodeOpt is an option to configure Serde.Encode and related functions.
	EncodeOpt  interface{ applyEncode(*encodeOpts) }
	encodeOpt  struct{ fn func(*encodeOpts) }
	encodeOpts struct {
		id    int
		index []int
	}

	// SerdeEncodeOpt is an option to configure Serde and Serde.Encode.
	SerdeEncodeOpt interface {
		SerdeOpt
		EncodeOpt
	}
	serdeEncodeOpt struct {
		serdeFn  func(*tserde)
		encodeFn func(*encodeOpts)
	}
)

func (o serdeOpt) applySerde(t *tserde)               { o.fn(t) }
func (o encodeOpt) applyEncode(opts *encodeOpts)      { o.fn(opts) }
func (o serdeEncodeOpt) applySerde(t *tserde)         { o.serdeFn(t) }
func (o serdeEncodeOpt) applyEncode(opts *encodeOpts) { o.encodeFn(opts) }

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
func Index(index ...int) SerdeEncodeOpt {
	return serdeEncodeOpt{
		serdeFn:  func(t *tserde) { t.index = index },
		encodeFn: func(o *encodeOpts) { o.index = index },
	}
}

// ID forces Serde.Encode to use the specified schema ID.
func ID(id int) EncodeOpt {
	return encodeOpt{func(opts *encodeOpts) { opts.id = id }}
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

// Register registers a schema ID and the value it corresponds to, as well as
// the encoding or decoding functions. You need to register functions depending
// on whether you are only encoding, only decoding, or both.
func (s *Serde) Register(id int, v any, opts ...SerdeOpt) {
	var t tserde
	for _, opt := range s.defaults {
		opt.applySerde(&t)
	}
	for _, opt := range opts {
		opt.applySerde(&t)
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

// Encode encodes a value according to the schema registry wire format and
// returns it. If EncodeFn was not used, this returns ErrNotRegistered.
func (s *Serde) Encode(v any, opts ...EncodeOpt) ([]byte, error) {
	return s.AppendEncode(nil, v, opts...)
}

// AppendEncode appends an encoded value to b according to the schema registry
// wire format and returns it. If EncodeFn was not used, this returns
// ErrNotRegistered.
func (s *Serde) AppendEncode(b []byte, v any, opts ...EncodeOpt) ([]byte, error) {
	o := encodeOpts{}
	for _, opt := range opts {
		opt.applyEncode(&o)
	}
	t, err := s.encodeFind(o, v)
	if err != nil {
		return nil, err
	}

	b = append(b,
		0,
		byte(t.id>>24),
		byte(t.id>>16),
		byte(t.id>>8),
		byte(t.id>>0),
	)

	if len(t.index) > 0 {
		if len(t.index) == 1 && t.index[0] == 0 {
			b = append(b, 0) // first-index shortcut (one type in the protobuf)
		} else {
			b = binary.AppendVarint(b, int64(len(t.index)))
			for _, idx := range t.index {
				b = binary.AppendVarint(b, int64(idx))
			}
		}
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

func (s *Serde) encodeFind(opts encodeOpts, v any) (tserde, error) {
	if opts.id > 0 {
		// load tserde based on the supplied ID
		t := s.loadIDs()[opts.id]
		// traverse to the right index, if any is supplied
		for _, i := range opts.index {
			if len(t.subindex) <= i {
				return tserde{}, ErrNotRegistered
			}
			t = t.subindex[i]
		}
		if !t.exists || (t.encode == nil && t.appendEncode == nil) {
			return tserde{}, ErrNotRegistered
		}
		return t, nil
	}
	// get tserde by type
	t := s.loadTypes()[reflect.TypeOf(v)]
	if !t.exists || (t.encode == nil && t.appendEncode == nil) {
		return tserde{}, ErrNotRegistered
	}
	return t, nil
}

// MustEncode returns the value of Encode, panicking on error. This is a
// shortcut for if your encode function cannot error.
func (s *Serde) MustEncode(v any, opts ...EncodeOpt) []byte {
	b, err := s.Encode(v, opts...)
	if err != nil {
		panic(err)
	}
	return b
}

// MustAppendEncode returns the value of AppendEncode, panicking on error.
// This is a shortcut for if your encode function cannot error.
func (s *Serde) MustAppendEncode(b []byte, v any, opts ...EncodeOpt) []byte {
	b, err := s.AppendEncode(b, v, opts...)
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
	} else if t.typeof != nil {
		v = reflect.New(t.typeof).Interface()
	} else {
		return nil, ErrNotRegistered
	}
	return v, t.decode(b, v)
}

func (s *Serde) decodeFind(b []byte) ([]byte, tserde, error) {
	if len(b) < 5 || b[0] != 0 {
		return nil, tserde{}, ErrBadHeader
	}
	id := binary.BigEndian.Uint32(b[1:5])
	b = b[5:]

	t := s.loadIDs()[int(id)]
	if len(t.subindex) > 0 {
		r := bReader{b}
		br := io.ByteReader(&r)
		l, err := binary.ReadVarint(br)
		if l == 0 { // length 0 is a shortcut for length 1, index 0
			t = t.subindex[0]
		}
		for err == nil && t.subindex != nil && l > 0 {
			var idx int64
			idx, err = binary.ReadVarint(br)
			t = t.subindex[int(idx)]
			l--
		}
		if err != nil {
			return nil, t, err
		}
		b = r.b
	}
	if !t.exists || t.decode == nil {
		return nil, tserde{}, ErrNotRegistered
	}
	return b, t, nil
}

type bReader struct{ b []byte }

func (b *bReader) ReadByte() (byte, error) {
	if len(b.b) > 0 {
		r := b.b[0]
		b.b = b.b[1:]
		return r, nil
	}
	return 0, io.EOF
}
