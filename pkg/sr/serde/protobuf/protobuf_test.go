package protobuf_test

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/pkg/sr/serde/protobuf"
	"github.com/twmb/franz-go/pkg/sr/serde/protobuf/testpb"
	"github.com/twmb/franz-go/pkg/sr/srfake"
)

func testClient(t *testing.T) *sr.Client {
	t.Helper()
	reg := srfake.New()
	t.Cleanup(reg.Close)
	cl, err := sr.NewClient(sr.URLs(reg.URL()))
	if err != nil {
		t.Fatal(err)
	}
	return cl
}

func newSer(t *testing.T, cl *sr.Client, opts ...protobuf.Opt) *protobuf.Serializer {
	t.Helper()
	s, err := protobuf.NewSerializer(cl, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func newDeser(t *testing.T, cl *sr.Client, opts ...protobuf.Opt) *protobuf.Deserializer {
	t.Helper()
	d, err := protobuf.NewDeserializer(cl, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestProtobufRoundTrip(t *testing.T) {
	ctx := context.Background()
	cl := testClient(t)

	ser := newSer(t, cl)
	deser := newDeser(t, cl)
	if err := deser.RegisterMessage(&testpb.Widget{}); err != nil {
		t.Fatal(err)
	}

	msg := &testpb.Widget{Name: "hello"}
	b, err := ser.Encode(ctx, "topic", msg)
	if err != nil {
		t.Fatal(err)
	}
	var out testpb.Widget
	if err := deser.Decode(ctx, b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Name != "hello" {
		t.Fatalf("got %+v", &out)
	}

	got, err := deser.DecodeNew(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	if w, ok := got.(*testpb.Widget); !ok || w.Name != "hello" {
		t.Fatalf("DecodeNew: %+v", got)
	}
}

func TestEncodeReusesSchemaID(t *testing.T) {
	ctx := context.Background()
	ser := newSer(t, testClient(t))
	b1, err := ser.Encode(ctx, "topic", &testpb.Widget{Name: "a"})
	if err != nil {
		t.Fatal(err)
	}
	b2, err := ser.Encode(ctx, "topic", &testpb.Widget{Name: "b"})
	if err != nil {
		t.Fatal(err)
	}
	if string(b1[:5]) != string(b2[:5]) {
		t.Fatalf("schema id bytes differ: %x vs %x", b1[:6], b2[:6])
	}
}

// AppendEncode must extend the caller's buffer rather than assume it is empty.
func TestAppendEncodeReusesBuffer(t *testing.T) {
	ctx := context.Background()
	ser := newSer(t, testClient(t))

	want, err := ser.Encode(ctx, "topic", &testpb.Widget{Name: "reuse"})
	if err != nil {
		t.Fatal(err)
	}

	buf := make([]byte, 0, 512)
	for i := range 3 {
		buf, err = ser.AppendEncode(ctx, "topic", &testpb.Widget{Name: "reuse"}, buf[:0])
		if err != nil {
			t.Fatal(err)
		}
		if string(buf) != string(want) {
			t.Fatalf("iteration %d: got %x want %x", i, buf, want)
		}
	}

	// Appending to a non-empty buffer must preserve the existing prefix.
	prefix := []byte("keep me")
	got, err := ser.AppendEncode(ctx, "topic", &testpb.Widget{Name: "reuse"}, prefix)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(prefix)+string(want) {
		t.Fatalf("got %q, want prefix retained", got)
	}
}

func TestLookupOnly(t *testing.T) {
	ctx := context.Background()
	cl := testClient(t)
	ser := newSer(t, cl, protobuf.AutoRegister(false))
	if _, err := ser.Encode(ctx, "topic", &testpb.Widget{Name: "x"}); err == nil {
		t.Fatal("expected lookup failure before register")
	}
	serReg := newSer(t, cl)
	if _, err := serReg.Encode(ctx, "topic", &testpb.Widget{Name: "x"}); err != nil {
		t.Fatal(err)
	}
	if _, err := ser.Encode(ctx, "topic", &testpb.Widget{Name: "y"}); err != nil {
		t.Fatal(err)
	}
}

func TestUseLatestVersion(t *testing.T) {
	ctx := context.Background()
	cl := testClient(t)
	seed := newSer(t, cl)
	if _, err := seed.Encode(ctx, "topic", &testpb.Widget{Name: "v1"}); err != nil {
		t.Fatal(err)
	}
	latest := newSer(t, cl, protobuf.UseLatestVersion(true), protobuf.AutoRegister(false))
	b, err := latest.Encode(ctx, "topic", &testpb.Widget{Name: "v2"})
	if err != nil {
		t.Fatal(err)
	}
	var hdr sr.ConfluentHeader
	id, _, err := hdr.DecodeID(b)
	if err != nil {
		t.Fatal(err)
	}
	if id != 1 {
		t.Fatalf("latest id %d want 1", id)
	}
}

// Encoding against a pinned or latest schema that does not describe the
// message must fail, rather than stamping a message index that points at an
// unrelated type.
func TestRejectsSchemaThatDoesNotMatchMessage(t *testing.T) {
	ctx := context.Background()
	reg := srfake.New()
	t.Cleanup(reg.Close)
	cl, err := sr.NewClient(sr.URLs(reg.URL()))
	if err != nil {
		t.Fatal(err)
	}
	reg.SeedSchema("topic-value", 1, 77, sr.Schema{
		Schema: `syntax = "proto3"; package other; message Unrelated { string q = 1; }`,
		Type:   sr.TypeProtobuf,
	})

	for _, tt := range []struct {
		name string
		opt  protobuf.Opt
	}{
		{"latest", protobuf.UseLatestVersion(true)},
		{"pinned", protobuf.UseSchemaID(77)},
	} {
		t.Run(tt.name, func(t *testing.T) {
			ser := newSer(t, cl, tt.opt, protobuf.AutoRegister(false))
			_, err := ser.Encode(ctx, "topic", &testpb.Widget{Name: "x"})
			if err == nil {
				t.Fatal("encoded against a schema with no matching message")
			}
			if !strings.Contains(err.Error(), "testpb.Widget") {
				t.Fatalf("error should name the message being encoded: %v", err)
			}
		})
	}
}

// A pinned id that does describe the message encodes fine, and resolves once.
func TestUseSchemaIDMatchingMessage(t *testing.T) {
	ctx := context.Background()
	reg := srfake.New()
	t.Cleanup(reg.Close)

	var byID atomic.Int32
	reg.Intercept(func(_ http.ResponseWriter, r *http.Request) bool {
		if r.URL.Path == "/schemas/ids/1" {
			byID.Add(1)
		}
		return false
	})
	cl, err := sr.NewClient(sr.URLs(reg.URL()))
	if err != nil {
		t.Fatal(err)
	}

	if _, err := newSer(t, cl).Encode(ctx, "topic", &testpb.Widget{Name: "seed"}); err != nil {
		t.Fatal(err)
	}

	pinned := newSer(t, cl, protobuf.UseSchemaID(1))
	for range 10 {
		if _, err := pinned.Encode(ctx, "topic", &testpb.Widget{Name: "x"}); err != nil {
			t.Fatal(err)
		}
	}
	if n := byID.Load(); n != 1 {
		t.Fatalf("SchemaByID calls %d for 10 encodes, want 1", n)
	}
}

func TestSecondTopLevelMessageIndex(t *testing.T) {
	ctx := context.Background()
	ser := newSer(t, testClient(t))
	b, err := ser.Encode(ctx, "topic", &testpb.Pizza{Slices: 8})
	if err != nil {
		t.Fatal(err)
	}
	var hdr sr.ConfluentHeader
	id, rest, err := hdr.DecodeID(b)
	if err != nil || id < 1 {
		t.Fatal(err)
	}
	idx, _, err := hdr.DecodeIndex(rest, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(idx) != 1 || idx[0] != 1 {
		t.Fatalf("indexes %v want [1]", idx)
	}
}

func TestNestedMessageIndex(t *testing.T) {
	ctx := context.Background()
	ser := newSer(t, testClient(t))
	b, err := ser.Encode(ctx, "topic", &testpb.Outer_Inner{Value: "in"})
	if err != nil {
		t.Fatal(err)
	}
	var hdr sr.ConfluentHeader
	_, rest, err := hdr.DecodeID(b)
	if err != nil {
		t.Fatal(err)
	}
	idx, _, err := hdr.DecodeIndex(rest, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(idx) != 2 || idx[0] != 0 || idx[1] != 0 {
		t.Fatalf("indexes %v want [0 0]", idx)
	}
}

func TestSchemaReference(t *testing.T) {
	ctx := context.Background()
	cl := testClient(t)
	ser := newSer(t, cl)
	b, err := ser.Encode(ctx, "topic", &testpb.WithDep{Dep: &testpb.Dep{Id: "d"}})
	if err != nil {
		t.Fatal(err)
	}
	subjects, err := cl.Subjects(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var hasDep, hasRoot bool
	for _, s := range subjects {
		if s == "dep.proto" {
			hasDep = true
		}
		if s == "topic-value" {
			hasRoot = true
		}
	}
	if !hasDep || !hasRoot {
		t.Fatalf("subjects %v missing dep or topic-value", subjects)
	}
	deser := newDeser(t, cl)
	if err := deser.RegisterMessage(&testpb.WithDep{}); err != nil {
		t.Fatal(err)
	}
	var out testpb.WithDep
	if err := deser.Decode(ctx, b, &out); err != nil {
		t.Fatal(err)
	}
	if out.Dep.GetId() != "d" {
		t.Fatalf("got %+v", &out)
	}
}

func TestReferenceSubjectNaming(t *testing.T) {
	ctx := context.Background()
	cl := testClient(t)
	ser := newSer(t, cl, protobuf.ReferenceSubject(func(name string) string {
		return "protos/" + name
	}))
	if _, err := ser.Encode(ctx, "topic", &testpb.WithDep{Dep: &testpb.Dep{Id: "d"}}); err != nil {
		t.Fatal(err)
	}
	subjects, err := cl.Subjects(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, s := range subjects {
		if s == "protos/dep.proto" {
			return
		}
	}
	t.Fatalf("subjects %v missing protos/dep.proto", subjects)
}

func TestSubjectStrategies(t *testing.T) {
	ctx := context.Background()
	cl := testClient(t)

	serRec := newSer(t, cl, protobuf.SubjectStrategy(protobuf.RecordName))
	if _, err := serRec.Encode(ctx, "ignored", &testpb.Widget{Name: "r"}); err != nil {
		t.Fatal(err)
	}
	subjects, err := cl.Subjects(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var hasRecord bool
	for _, s := range subjects {
		if s == "testpb.Widget" {
			hasRecord = true
		}
	}
	if !hasRecord {
		t.Fatalf("subjects %v", subjects)
	}
}

// A record-name subject must round trip: the deserializer resolves by schema
// id, so the subject strategy used to produce it is irrelevant to decoding.
func TestRecordNameSubjectRoundTrip(t *testing.T) {
	ctx := context.Background()
	cl := testClient(t)
	ser := newSer(t, cl, protobuf.SubjectStrategy(protobuf.RecordName))
	b, err := ser.Encode(ctx, "ignored", &testpb.Widget{Name: "rn"})
	if err != nil {
		t.Fatal(err)
	}
	deser := newDeser(t, cl)
	if err := deser.RegisterMessage(&testpb.Widget{}); err != nil {
		t.Fatal(err)
	}
	got, err := deser.DecodeNew(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	if w, ok := got.(*testpb.Widget); !ok || w.Name != "rn" {
		t.Fatalf("got %+v", got)
	}
}

func TestKeyAndTopicRecordSubjects(t *testing.T) {
	ctx := context.Background()
	cl := testClient(t)

	keySer := newSer(t, cl, protobuf.SubjectPart(protobuf.PartKey))
	if _, err := keySer.Encode(ctx, "orders", &testpb.Widget{Name: "k"}); err != nil {
		t.Fatal(err)
	}
	recSer := newSer(t, cl, protobuf.SubjectStrategy(protobuf.TopicRecordName))
	if _, err := recSer.Encode(ctx, "orders", &testpb.Widget{Name: "r"}); err != nil {
		t.Fatal(err)
	}
	subjects, err := cl.Subjects(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var hasKey, hasTopicRecord bool
	for _, s := range subjects {
		if s == "orders-key" {
			hasKey = true
		}
		if s == "orders-testpb.Widget" {
			hasTopicRecord = true
		}
	}
	if !hasKey || !hasTopicRecord {
		t.Fatalf("subjects %v", subjects)
	}
}

func TestRegisteredSchemaOmitsCodegenOptions(t *testing.T) {
	ctx := context.Background()
	cl := testClient(t)
	if _, err := newSer(t, cl).Encode(ctx, "topic", &testpb.Widget{Name: "w"}); err != nil {
		t.Fatal(err)
	}
	ss, err := cl.SchemaByVersion(ctx, "topic-value", 1)
	if err != nil {
		t.Fatal(err)
	}
	for _, opt := range []string{"go_package", "java_package", "objc_class_prefix"} {
		if strings.Contains(ss.Schema.Schema, opt) {
			t.Fatalf("schema text contains %s:\n%s", opt, ss.Schema.Schema)
		}
	}
}

// KeepCodegenOptions exists so a schema can match what confluent-kafka-go
// registers, which does not strip them.
func TestKeepCodegenOptions(t *testing.T) {
	ctx := context.Background()
	cl := testClient(t)
	ser := newSer(t, cl, protobuf.KeepCodegenOptions())
	if _, err := ser.Encode(ctx, "topic", &testpb.Widget{Name: "w"}); err != nil {
		t.Fatal(err)
	}
	ss, err := cl.SchemaByVersion(ctx, "topic-value", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(ss.Schema.Schema, "go_package") {
		t.Fatalf("schema text should retain go_package:\n%s", ss.Schema.Schema)
	}
}

func TestBadHeader(t *testing.T) {
	ctx := context.Background()
	deser := newDeser(t, testClient(t))
	for _, b := range [][]byte{nil, {}, {0, 0}, {1, 0, 0, 0, 1}} {
		err := deser.Decode(ctx, b, &testpb.Widget{})
		if !errors.Is(err, sr.ErrBadHeader) {
			t.Fatalf("Decode(%x) err %v", b, err)
		}
		if _, err := deser.DecodeNew(ctx, b); !errors.Is(err, sr.ErrBadHeader) {
			t.Fatalf("DecodeNew(%x) err %v", b, err)
		}
	}
}

func TestDecodeNewRejectsNonProtobuf(t *testing.T) {
	ctx := context.Background()
	reg := srfake.New()
	t.Cleanup(reg.Close)
	cl, err := sr.NewClient(sr.URLs(reg.URL()))
	if err != nil {
		t.Fatal(err)
	}
	reg.SeedSchema("avro-value", 1, 9, sr.Schema{Schema: `{"type":"string"}`, Type: sr.TypeAvro})
	var hdr sr.ConfluentHeader
	b, err := hdr.AppendEncode(nil, 9, []int{0})
	if err != nil {
		t.Fatal(err)
	}
	deser := newDeser(t, cl)
	_, err = deser.DecodeNew(ctx, append(b, 0))
	if err == nil || !strings.Contains(err.Error(), "not protobuf") {
		t.Fatalf("err %v", err)
	}
}

func TestDecodeNewCachesFileDescriptor(t *testing.T) {
	ctx := context.Background()
	reg := srfake.New()
	t.Cleanup(reg.Close)
	var n atomic.Int32
	reg.Intercept(func(_ http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && strings.Count(r.URL.Path, "/") == 3 && strings.Contains(r.URL.Path, "/schemas/ids/") {
			n.Add(1)
		}
		return false
	})
	cl, err := sr.NewClient(sr.URLs(reg.URL()))
	if err != nil {
		t.Fatal(err)
	}
	ser := newSer(t, cl)
	b, err := ser.Encode(ctx, "topic", &testpb.Widget{Name: "w"})
	if err != nil {
		t.Fatal(err)
	}
	deser := newDeser(t, cl)
	if err := deser.RegisterMessage(&testpb.Widget{}); err != nil {
		t.Fatal(err)
	}
	if _, err := deser.DecodeNew(ctx, b); err != nil {
		t.Fatal(err)
	}
	if _, err := deser.DecodeNew(ctx, b); err != nil {
		t.Fatal(err)
	}
	if n.Load() != 1 {
		t.Fatalf("SchemaByID calls %d", n.Load())
	}
}

// Concurrent cold decodes of one id must collapse into a single fetch.
func TestDecodeNewSingleFlight(t *testing.T) {
	ctx := context.Background()
	reg := srfake.New()
	t.Cleanup(reg.Close)
	var n atomic.Int32
	reg.Intercept(func(_ http.ResponseWriter, r *http.Request) bool {
		if r.Method == http.MethodGet && strings.Count(r.URL.Path, "/") == 3 && strings.Contains(r.URL.Path, "/schemas/ids/") {
			n.Add(1)
		}
		return false
	})
	cl, err := sr.NewClient(sr.URLs(reg.URL()))
	if err != nil {
		t.Fatal(err)
	}
	b, err := newSer(t, cl).Encode(ctx, "topic", &testpb.Widget{Name: "w"})
	if err != nil {
		t.Fatal(err)
	}

	deser := newDeser(t, cl)
	var wg sync.WaitGroup
	for range 16 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := deser.DecodeNew(ctx, b); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	if got := n.Load(); got != 1 {
		t.Fatalf("SchemaByID calls %d, want 1", got)
	}
}

// Generated code registers itself globally, so DecodeNew works without any
// explicit RegisterMessage call.
func TestDecodeNewUsesGlobalTypes(t *testing.T) {
	ctx := context.Background()
	cl := testClient(t)
	b, err := newSer(t, cl).Encode(ctx, "topic", &testpb.Widget{Name: "global"})
	if err != nil {
		t.Fatal(err)
	}
	got, err := newDeser(t, cl).DecodeNew(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	if w, ok := got.(*testpb.Widget); !ok || w.Name != "global" {
		t.Fatalf("got %+v", got)
	}
}

func TestVerifyDecode(t *testing.T) {
	ctx := context.Background()
	cl := testClient(t)
	b, err := newSer(t, cl).Encode(ctx, "topic", &testpb.Widget{Name: "w"})
	if err != nil {
		t.Fatal(err)
	}

	// Without verification, decoding into the wrong type silently "works".
	var wrong testpb.Dep
	if err := newDeser(t, cl).Decode(ctx, b, &wrong); err != nil {
		t.Fatalf("unverified decode should not error: %v", err)
	}

	verifying := newDeser(t, cl, protobuf.VerifyDecode())
	if err := verifying.Decode(ctx, b, &testpb.Dep{}); err == nil {
		t.Fatal("verified decode into the wrong type should fail")
	}
	var right testpb.Widget
	if err := verifying.Decode(ctx, b, &right); err != nil {
		t.Fatal(err)
	}
	if right.Name != "w" {
		t.Fatalf("got %+v", &right)
	}
}

func TestNestedReferenceDecodeNew(t *testing.T) {
	ctx := context.Background()
	cl := testClient(t)
	msg := &testpb.Chain{Mid: &testpb.Mid{Leaf: &testpb.Leaf{Id: "leaf"}}}
	b, err := newSer(t, cl).Encode(ctx, "topic", msg)
	if err != nil {
		t.Fatal(err)
	}
	deser := newDeser(t, cl)
	if err := deser.RegisterMessage(&testpb.Chain{}); err != nil {
		t.Fatal(err)
	}
	got, err := deser.DecodeNew(ctx, b)
	if err != nil {
		t.Fatal(err)
	}
	chain, ok := got.(*testpb.Chain)
	if !ok || chain.GetMid().GetLeaf().GetId() != "leaf" {
		t.Fatalf("got %+v", got)
	}
}

func TestConcurrentEncode(t *testing.T) {
	ctx := context.Background()
	ser := newSer(t, testClient(t))
	var (
		wg  sync.WaitGroup
		ids [8]int
	)
	for i := range ids {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			b, err := ser.Encode(ctx, "topic", &testpb.Widget{Name: "c"})
			if err != nil {
				t.Error(err)
				return
			}
			var hdr sr.ConfluentHeader
			id, _, err := hdr.DecodeID(b)
			if err != nil {
				t.Error(err)
				return
			}
			ids[i] = id
		}(i)
	}
	wg.Wait()
	for _, id := range ids {
		if id == 0 || id != ids[0] {
			t.Fatalf("ids %v", ids)
		}
	}
}

func TestNilMessage(t *testing.T) {
	ctx := context.Background()
	cl := testClient(t)
	if _, err := newSer(t, cl).Encode(ctx, "topic", nil); err == nil {
		t.Fatal("expected an error encoding a nil message")
	}
	if err := newDeser(t, cl).Decode(ctx, []byte{0, 0, 0, 0, 1, 0}, nil); err == nil {
		t.Fatal("expected an error decoding into a nil message")
	}
}

func TestNilClient(t *testing.T) {
	if _, err := protobuf.NewSerializer(nil); err == nil {
		t.Fatal("expected an error for a nil client")
	}
	if _, err := protobuf.NewDeserializer(nil); err == nil {
		t.Fatal("expected an error for a nil client")
	}
}

var _ proto.Message = (*testpb.Widget)(nil)
