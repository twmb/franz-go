package serde

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/twmb/franz-go/pkg/sr"
)

// testValue is the "message" the fake format encodes: a record name, a
// message-index path, and a payload.
type testValue struct {
	name    string
	index   []int
	payload string
}

// testEncoder is a minimal Encoder that counts which methods the Serializer
// actually calls, so tests can assert on the driver's laziness and caching
// rather than on registry traffic alone.
type testEncoder struct {
	schema sr.Schema

	nRecordName atomic.Int64
	nIndex      atomic.Int64
	nSchemaFor  atomic.Int64
}

func (e *testEncoder) RecordName(v any) (string, error) {
	e.nRecordName.Add(1)
	return v.(*testValue).name, nil
}

func (e *testEncoder) Index(v any) ([]int, error) {
	e.nIndex.Add(1)
	return v.(*testValue).index, nil
}

func (e *testEncoder) SchemaFor(context.Context, any) (sr.Schema, error) {
	e.nSchemaFor.Add(1)
	return e.schema, nil
}

func (e *testEncoder) AppendMarshal(dst []byte, v any) ([]byte, error) {
	return append(dst, v.(*testValue).payload...), nil
}

// verifyingEncoder is a testEncoder that also implements Verifier.
type verifyingEncoder struct {
	*testEncoder
	nVerify atomic.Int64
	err     error
}

func (e *verifyingEncoder) Verify(context.Context, ResolvedSchema, any) error {
	e.nVerify.Add(1)
	return e.err
}

// testDecoder records whether it asked for the schema, which is how the
// Deserializer's laziness is observed.
type testDecoder struct {
	wantSchema bool

	nUnmarshal atomic.Int64
	lastWire   atomic.Pointer[WirePayload]
}

func (d *testDecoder) Unmarshal(ctx context.Context, w WirePayload, s SchemaLoader, v any) error {
	d.nUnmarshal.Add(1)
	d.lastWire.Store(&w)
	if d.wantSchema {
		if _, err := s.Load(ctx, w.ID); err != nil {
			return err
		}
	}
	*v.(*string) = string(w.Payload)
	return nil
}

func (d *testDecoder) UnmarshalNew(ctx context.Context, w WirePayload, s SchemaLoader) (any, error) {
	rs, err := s.Load(ctx, w.ID)
	if err != nil {
		return nil, err
	}
	return &testValue{name: rs.Schema.Schema, index: w.Index, payload: string(w.Payload)}, nil
}

func TestSerializerCachesResolvedIDs(t *testing.T) {
	t.Parallel()

	reg := newCountingRegistry(t)
	enc := &testEncoder{schema: avro("Widget")}
	s, err := NewSerializer(NewResolver(reg.client(t)), enc)
	if err != nil {
		t.Fatalf("NewSerializer: %v", err)
	}

	ctx := context.Background()
	v := &testValue{name: "Widget", index: []int{0}, payload: "hello"}
	if _, err := s.Encode(ctx, "widgets", v); err != nil {
		t.Fatalf("Encode: %v", err)
	}
	if n := reg.total(); n == 0 {
		t.Fatal("the first encode made no observed requests; the request tally is not working")
	}
	reg.reset()

	const warm = 50
	for range warm {
		if _, err := s.Encode(ctx, "widgets", v); err != nil {
			t.Fatalf("Encode: %v", err)
		}
	}

	if n := reg.total(); n != 0 {
		t.Errorf("%d registry requests across %d warm encodes, want 0: %v", n, warm, reg.counts)
	}
	if n := enc.nSchemaFor.Load(); n != 1 {
		t.Errorf("SchemaFor called %d times, want 1; deriving schema text per record is the cost the id cache exists to avoid", n)
	}
	if n := enc.nIndex.Load(); n != 1 {
		t.Errorf("Index called %d times, want 1; a cached id should short-circuit before deriving the index", n)
	}
	// RecordName keys the cache, so it is the one call that cannot be skipped.
	if n := enc.nRecordName.Load(); n != warm+1 {
		t.Errorf("RecordName called %d times, want %d", n, warm+1)
	}
}

func TestSerializerDoesNotCacheIDsWhenFollowingLatest(t *testing.T) {
	t.Parallel()

	reg := newCountingRegistry(t)
	reg.SeedSchema("widgets-value", 1, 1, avro("Widget"))
	enc := &testEncoder{schema: avro("Widget")}
	s, err := NewSerializer(NewResolver(reg.client(t), UseLatestVersion(true), LatestCacheTTL(0)), enc)
	if err != nil {
		t.Fatalf("NewSerializer: %v", err)
	}

	ctx := context.Background()
	v := &testValue{name: "Widget", index: []int{0}, payload: "hello"}
	for range 3 {
		if _, err := s.Encode(ctx, "widgets", v); err != nil {
			t.Fatalf("Encode: %v", err)
		}
	}

	// Following the latest version means a newly registered version must be
	// picked up, so the id cannot be cached for the life of the serializer.
	if n := reg.counts["GET /subjects/widgets-value/versions/latest"]; n != 3 {
		t.Errorf("looked up the latest version %d times across 3 encodes, want 3: %v", n, reg.counts)
	}
	if enc.nSchemaFor.Load() != 0 {
		t.Error("SchemaFor was called while following the latest version; schema text should never be derived on that path")
	}
}

func TestSerializerVerifiesSchemasItDidNotDerive(t *testing.T) {
	t.Parallel()

	reg := newCountingRegistry(t)
	reg.SeedSchema("widgets-value", 1, 1, avro("Widget"))
	enc := &verifyingEncoder{testEncoder: &testEncoder{schema: avro("Widget")}}
	s, err := NewSerializer(NewResolver(reg.client(t), UseSchemaID(1)), enc)
	if err != nil {
		t.Fatalf("NewSerializer: %v", err)
	}

	ctx := context.Background()
	v := &testValue{name: "Widget", index: []int{0}, payload: "hello"}
	for range 10 {
		if _, err := s.Encode(ctx, "widgets", v); err != nil {
			t.Fatalf("Encode: %v", err)
		}
	}

	if enc.nSchemaFor.Load() != 0 {
		t.Error("SchemaFor was called with a pinned schema id; schema text should never be derived on that path")
	}
	if n := enc.nVerify.Load(); n != 1 {
		t.Errorf("Verify called %d times, want 1; the result should be cached per id and record", n)
	}
}

func TestSerializerSurfacesVerificationFailures(t *testing.T) {
	t.Parallel()

	reg := newCountingRegistry(t)
	reg.SeedSchema("widgets-value", 1, 1, avro("Widget"))
	sentinel := errors.New("index points at a different message")
	enc := &verifyingEncoder{testEncoder: &testEncoder{schema: avro("Widget")}, err: sentinel}
	s, err := NewSerializer(NewResolver(reg.client(t), UseSchemaID(1)), enc)
	if err != nil {
		t.Fatalf("NewSerializer: %v", err)
	}

	v := &testValue{name: "Widget", index: []int{3}, payload: "hello"}
	_, err = s.Encode(context.Background(), "widgets", v)
	if !errors.Is(err, sentinel) {
		t.Fatalf("Encode error = %v, want it to wrap the verifier's error", err)
	}
	// A cached failure must keep failing rather than succeeding second time.
	if _, err := s.Encode(context.Background(), "widgets", v); !errors.Is(err, sentinel) {
		t.Fatalf("second Encode error = %v, want the cached verification failure", err)
	}
}

func TestSerializerSkipsVerificationWhenTheEncoderCannotVerify(t *testing.T) {
	t.Parallel()

	reg := newCountingRegistry(t)
	reg.SeedSchema("widgets-value", 1, 1, avro("Widget"))
	// testEncoder deliberately does not implement Verifier.
	s, err := NewSerializer(NewResolver(reg.client(t), UseSchemaID(1)), &testEncoder{schema: avro("Widget")})
	if err != nil {
		t.Fatalf("NewSerializer: %v", err)
	}

	v := &testValue{name: "Widget", index: []int{0}, payload: "hello"}
	if _, err := s.Encode(context.Background(), "widgets", v); err != nil {
		t.Fatalf("Encode: %v", err)
	}
}

func TestSerializerRequiresItsCollaborators(t *testing.T) {
	t.Parallel()

	if _, err := NewSerializer(nil, &testEncoder{}); err == nil {
		t.Error("NewSerializer accepted a nil resolver")
	}
	if _, err := NewSerializer(NewResolver(nil), nil); err == nil {
		t.Error("NewSerializer accepted a nil encoder")
	}
	if _, err := NewDeserializer(nil, &testDecoder{}); err == nil {
		t.Error("NewDeserializer accepted a nil client")
	}
}

// TestDeserializerDoesNotFetchUnlessTheDecoderAsks is the property that keeps
// the common decode path free of registry traffic: a format that can
// unmarshal into a caller-supplied value without consulting the schema should
// cost nothing beyond stripping the header.
func TestDeserializerDoesNotFetchUnlessTheDecoderAsks(t *testing.T) {
	t.Parallel()

	reg := newCountingRegistry(t)
	reg.SeedSchema("widgets-value", 1, 1, avro("Widget"))
	cl := reg.client(t)

	var header sr.ConfluentHeader
	b, err := header.AppendEncode(nil, 1, []int{0})
	if err != nil {
		t.Fatalf("AppendEncode: %v", err)
	}
	b = append(b, "hello"...)

	t.Run("decoder ignores the schema", func(t *testing.T) {
		dec := &testDecoder{wantSchema: false}
		d, err := NewDeserializer(cl, dec)
		if err != nil {
			t.Fatalf("NewDeserializer: %v", err)
		}
		reg.reset()

		var got string
		if err := d.Decode(context.Background(), b, &got); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if got != "hello" {
			t.Errorf("payload = %q, want %q", got, "hello")
		}
		if n := reg.total(); n != 0 {
			t.Errorf("%d registry requests for a decode that never asked for the schema, want 0: %v", n, reg.counts)
		}
	})

	t.Run("decoder asks for the schema", func(t *testing.T) {
		dec := &testDecoder{wantSchema: true}
		d, err := NewDeserializer(cl, dec)
		if err != nil {
			t.Fatalf("NewDeserializer: %v", err)
		}
		reg.reset()

		var got string
		if err := d.Decode(context.Background(), b, &got); err != nil {
			t.Fatalf("Decode: %v", err)
		}
		if n := reg.total(); n == 0 {
			t.Error("no registry requests for a decode that asked for the schema, want at least one")
		}

		// And the result is cached across records.
		reg.reset()
		for range 20 {
			if err := d.Decode(context.Background(), b, &got); err != nil {
				t.Fatalf("Decode: %v", err)
			}
		}
		if n := reg.total(); n != 0 {
			t.Errorf("%d registry requests across 20 warm decodes, want 0: %v", n, reg.counts)
		}
	})
}

// Unpacking the wire header must not allocate beyond what pkg/sr already
// spends doing it. This is the hottest path in a consumer, run once per
// record, and the overwhelmingly common case never looks at the schema.
func TestDeserializerBuildsWirePayloadWithoutAllocating(t *testing.T) {
	// No t.Parallel: AllocsPerRun panics if other tests are running.

	reg := newCountingRegistry(t)
	reg.SeedSchema("widgets-value", 1, 1, avro("Widget"))
	d, err := NewDeserializer(reg.client(t), &testDecoder{})
	if err != nil {
		t.Fatalf("NewDeserializer: %v", err)
	}

	var header sr.ConfluentHeader
	b, err := header.AppendEncode(nil, 1, []int{0})
	if err != nil {
		t.Fatalf("AppendEncode: %v", err)
	}
	b = append(b, "hello"...)

	// Measure against unpacking the header on its own rather than against
	// zero: DecodeIndex allocates the index slice, and that is pkg/sr's cost,
	// not ours. What must be free is everything we add on top.
	var (
		sinkIndex []int
		sink      WirePayload
	)
	base := testing.AllocsPerRun(100, func() {
		_, rest, _ := header.DecodeID(b)
		sinkIndex, _, _ = header.DecodeIndex(rest, 0)
	})
	got := testing.AllocsPerRun(100, func() {
		w, err := d.wirePayload(b)
		if err != nil {
			t.Fatalf("wirePayload: %v", err)
		}
		sink = w
	})

	if sinkIndex == nil || sink.Payload == nil {
		t.Fatal("the measured calls did not produce an index and a payload; this test is not measuring what it claims to")
	}
	if got > base {
		t.Errorf("building a WirePayload costs %v allocations against a header-decode baseline of %v, want no more than the baseline", got, base)
	}
}

func TestDeserializerPassesTheHeaderFieldsToTheDecoder(t *testing.T) {
	t.Parallel()

	reg := newCountingRegistry(t)
	reg.SeedSchema("widgets-value", 1, 7, avro("Widget"))
	cl := reg.client(t)

	var header sr.ConfluentHeader
	b, err := header.AppendEncode(nil, 7, []int{1, 2})
	if err != nil {
		t.Fatalf("AppendEncode: %v", err)
	}
	b = append(b, "payload"...)

	dec := &testDecoder{}
	d, err := NewDeserializer(cl, dec)
	if err != nil {
		t.Fatalf("NewDeserializer: %v", err)
	}
	var got string
	if err := d.Decode(context.Background(), b, &got); err != nil {
		t.Fatalf("Decode: %v", err)
	}

	r := dec.lastWire.Load()
	if r.ID != 7 {
		t.Errorf("WirePayload.ID = %d, want 7", r.ID)
	}
	if len(r.Index) != 2 || r.Index[0] != 1 || r.Index[1] != 2 {
		t.Errorf("WirePayload.Index = %v, want [1 2]", r.Index)
	}
	if string(r.Payload) != "payload" {
		t.Errorf("WirePayload.Payload = %q, want %q", r.Payload, "payload")
	}
}

func TestDeserializerDecodeNewUsesTheWrittenSchema(t *testing.T) {
	t.Parallel()

	reg := newCountingRegistry(t)
	reg.SeedSchema("widgets-value", 1, 1, avro("Widget"))

	var header sr.ConfluentHeader
	b, err := header.AppendEncode(nil, 1, []int{0})
	if err != nil {
		t.Fatalf("AppendEncode: %v", err)
	}
	b = append(b, "hello"...)

	d, err := NewDeserializer(reg.client(t), &testDecoder{})
	if err != nil {
		t.Fatalf("NewDeserializer: %v", err)
	}
	got, err := d.DecodeNew(context.Background(), b)
	if err != nil {
		t.Fatalf("DecodeNew: %v", err)
	}
	v, ok := got.(*testValue)
	if !ok {
		t.Fatalf("DecodeNew returned %T, want *testValue", got)
	}
	if !strings.Contains(v.name, "Widget") {
		t.Errorf("value was built from %q, want the schema the record was written with", v.name)
	}
	if v.payload != "hello" {
		t.Errorf("payload = %q, want %q", v.payload, "hello")
	}
}
