package sr

import (
	"encoding/binary"
	"errors"
	"io"
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
	ErrBadHeader = errors.New("5 byte header for value is missing or does not have 0 magic byte")
)

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

type tserde struct {
	id           uint32
	exists       bool
	encode       func(any) ([]byte, error)
	appendEncode func([]byte, any) ([]byte, error)
	decode       func([]byte, any) error
	gen          func() any
	typeof       reflect.Type

	index         []int          // for encoding, an optional index we use
	subindex      map[int]tserde // for decoding, we look up sub-indices in the payload
	subindexDepth int            // for decoding, we need to know the maximum depth of the subindex map
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

	defaults []EncodingOpt
	h        SerdeHeader
}

var (
	noIDs   = make(map[int]tserde)
	noTypes = make(map[reflect.Type]tserde)
)

// NewSerde returns a new Serde using the supplied default options, which are
// applied to every registered type. These options are always applied first, so
// you can override them as necessary when registering.
//
// This can be useful if you always want to use the same encoding or decoding
// functions.
func NewSerde(opts ...SerdeOrEncodingOpt) *Serde {
	var s Serde
	for _, opt := range opts {
		switch opt := opt.(type) {
		case SerdeOpt:
			opt.apply(&s)
		case EncodingOpt:
			s.defaults = append(s.defaults, opt)
		}
	}
	return &s
}

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

// DecodeID decodes an ID from b, returning the ID and the remaining bytes,
// or an error.
func (s *Serde) DecodeID(b []byte) (id int, out []byte, err error) {
	return s.header().DecodeID(b)
}

// DecodeIndex decodes at most maxLength of a schema index from in, returning
// the index and remaining bytes, or an error. It expects b to be the output of
// DecodeID (schema ID should already be stripped away).
func (s *Serde) DecodeIndex(in []byte, maxLength int) (index []int, out []byte, err error) {
	return s.header().DecodeIndex(in, maxLength)
}

func (s *Serde) header() SerdeHeader {
	if s.h == nil {
		return defaultSerdeHeader
	}
	return s.h
}

// Register registers a schema ID and the value it corresponds to, as well as
// the encoding or decoding functions. You need to register functions depending
// on whether you are only encoding, only decoding, or both.
func (s *Serde) Register(id int, v any, opts ...EncodingOpt) {
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
	dupTypes := make(map[reflect.Type]tserde)
	for k, v := range s.loadTypes() {
		dupTypes[k] = v
	}

	// For IDs, we deeply clone any path that is changing.
	dupIDs := tserdeMapClone(s.loadIDs(), id, t.index)

	// We defer the store because we modify the tserde below, and we
	// may delete a type key.
	defer func() {
		dupTypes[typeof] = t
		s.types.Store(dupTypes)
		s.ids.Store(dupIDs)
	}()

	// Now we have a full path down index initialized (or, the top
	// level map if there is no index). We iterate down the index
	// tree to find the end node we are initializing.
	k := id
	m := dupIDs
	at := m[k]
	depth := len(t.index)
	max := func(i, j int) int {
		if i > j {
			return i
		}
		return j
	}
	for _, idx := range t.index {
		// SAFETY: tserdeMapClone deeply clones all maps through the index, so
		// our modified value is being saved to a new map that is not being read.
		at.subindexDepth = max(at.subindexDepth, depth)
		m[k] = at
		depth--

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
		id:            uint32(id),
		exists:        true,
		encode:        t.encode,
		appendEncode:  t.appendEncode,
		decode:        t.decode,
		gen:           t.gen,
		typeof:        typeof,
		index:         t.index,
		subindex:      at.subindex,
		subindexDepth: at.subindexDepth,
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

// AppendEncode encodes a value and prepends the header according to the
// configured SerdeHeader, appends it to b and returns b. If EncodeFn was not
// registered, this returns ErrNotRegistered.
func (s *Serde) AppendEncode(b []byte, v any) ([]byte, error) {
	t, ok := s.loadTypes()[reflect.TypeOf(v)]
	if !ok || (t.encode == nil && t.appendEncode == nil) {
		return b, ErrNotRegistered
	}
	b, err := s.header().AppendEncode(b, int(t.id), t.index)
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
	} else if t.typeof != nil {
		v = reflect.New(t.typeof).Interface()
	} else {
		return nil, ErrNotRegistered
	}
	return v, t.decode(b, v)
}

func (s *Serde) decodeFind(b []byte) ([]byte, tserde, error) {
	id, b, err := s.DecodeID(b)
	if err != nil {
		return nil, tserde{}, err
	}
	t := s.loadIDs()[id]
	if len(t.subindex) > 0 {
		var index []int
		index, b, err = s.DecodeIndex(b, t.subindexDepth)
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
	if !t.exists || t.decode == nil {
		return nil, tserde{}, ErrNotRegistered
	}
	return b, t, nil
}

// Encode encodes a value and prepends the header. If the encoding function
// fails, this returns an error.
func Encode(v any, h SerdeHeader, id int, index []int, enc func(any) ([]byte, error)) ([]byte, error) {
	return AppendEncode(nil, v, h, id, index, func(b []byte, val any) ([]byte, error) {
		encoded, err := enc(val)
		if err != nil {
			return nil, err
		}
		return append(b, encoded...), nil
	})
}

// AppendEncode encodes a value and prepends the header, appends it to b and
// returns b. If the encoding function fails, this returns an error.
func AppendEncode(b []byte, v any, h SerdeHeader, id int, index []int, enc func([]byte, any) ([]byte, error)) ([]byte, error) {
	b, err := h.AppendEncode(b, id, index)
	if err != nil {
		return nil, err
	}
	return enc(b, v)
}

// SerdeHeader encodes and decodes a message header.
type SerdeHeader interface {
	// AppendEncode encodes a schema ID and optional index to b, returning the
	// updated slice or an error.
	AppendEncode(b []byte, id int, index []int) ([]byte, error)
	// DecodeID decodes an ID from in, returning the ID and the remaining bytes,
	// or an error.
	DecodeID(in []byte) (id int, out []byte, err error)
	// DecodeIndex decodes at most maxLength of a schema index from in,
	// returning the index and remaining bytes, or an error.
	DecodeIndex(in []byte, maxLength int) (index []int, out []byte, err error)
}

var defaultSerdeHeader = new(ConfluentHeader)

// ConfluentHeader is a SerdeHeader that produces the Confluent wire format. It
// starts with 0, then big endian uint32 of the ID, then index (only protobuf),
// then the encoded message.
//
// https://docs.confluent.io/platform/current/schema-registry/serdes-develop/index.html#wire-format
type ConfluentHeader struct{}

// AppendEncode appends an encoded header to b according to the Confluent wire
// format and returns it. Error is always nil.
func (*ConfluentHeader) AppendEncode(b []byte, id int, index []int) ([]byte, error) {
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
func (*ConfluentHeader) DecodeID(b []byte) (int, []byte, error) {
	if len(b) < 5 || b[0] != 0 {
		return 0, nil, ErrBadHeader
	}
	id := binary.BigEndian.Uint32(b[1:5])
	return int(id), b[5:], nil
}

// UpdateID replaces the schema ID in b. If the header does not contain the
// magic byte or b contains less than 5 bytes it returns ErrBadHeader.
func (*ConfluentHeader) UpdateID(b []byte, id uint32) error {
	if len(b) < 5 || b[0] != 0 {
		return ErrBadHeader
	}
	binary.BigEndian.PutUint32(b[1:5], id)
	return nil
}

// DecodeIndex strips and decodes indices from b. It returns the index slice
// alongside the unread bytes. It expects b to be the output of DecodeID (schema
// ID should already be stripped away). If maxLength is greater than 0 and the
// encoded data contains more indices than maxLength the function returns
// ErrNotRegistered.
func (*ConfluentHeader) DecodeIndex(b []byte, maxLength int) ([]int, []byte, error) {
	r := bReader{b}
	br := io.ByteReader(&r)
	l, err := binary.ReadVarint(br)
	if err != nil {
		return nil, nil, err
	}
	if l == 0 { // length 0 is a shortcut for length 1, index 0
		return []int{0}, r.b, nil
	}
	if l < 0 { // index length can't be negative
		return nil, nil, ErrBadHeader
	}
	if maxLength > 0 && int(l) > maxLength { // index count is greater than expected
		return nil, nil, ErrNotRegistered
	}
	index := make([]int, l)
	for i := range index {
		idx, err := binary.ReadVarint(br)
		if err != nil {
			return nil, nil, err
		}
		index[i] = int(idx)
	}
	return index, r.b, nil
}

type bReader struct{ b []byte }

func (s *Serde) IsRegistered(id int, encodingOptFns ...any) bool {
	ids := s.loadIDs()
	t, ok := ids[id]
	if !ok || !t.exists {
		return false
	}

	is := true
	for _, optFn := range encodingOptFns {
		name := namefn(optFn)
		if s, ok := optFn.(string); ok {
			name = s
		}
		switch name {
		case namefn(EncodeFn):
			is = is && t.encode != nil
		case namefn(AppendEncodeFn):
			is = is && t.appendEncode != nil
		case namefn(DecodeFn):
			is = is && t.decode != nil
		case namefn(GenerateFn):
			is = is && t.gen != nil
		}
	}
	return is
}

func (b *bReader) ReadByte() (byte, error) {
	if len(b.b) > 0 {
		r := b.b[0]
		b.b = b.b[1:]
		return r, nil
	}
	return 0, io.EOF
}
