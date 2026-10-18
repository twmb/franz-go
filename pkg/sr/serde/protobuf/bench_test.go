package protobuf_test

import (
	"context"
	"testing"

	"google.golang.org/protobuf/proto"

	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/pkg/sr/serde/protobuf"
	"github.com/twmb/franz-go/pkg/sr/serde/protobuf/testpb"
	"github.com/twmb/franz-go/pkg/sr/srfake"
)

func benchClient(b *testing.B) *sr.Client {
	b.Helper()
	reg := srfake.New()
	b.Cleanup(reg.Close)
	cl, err := sr.NewClient(sr.URLs(reg.URL()))
	if err != nil {
		b.Fatal(err)
	}
	return cl
}

func benchSer(b *testing.B, opts ...protobuf.Opt) *protobuf.Serializer {
	b.Helper()
	s, err := protobuf.NewSerializer(benchClient(b), opts...)
	if err != nil {
		b.Fatal(err)
	}
	return s
}

// The floor: framing plus marshal, with no schema resolution at all. Encode
// benchmarks are meaningful relative to this.
func BenchmarkBaselineMarshal(b *testing.B) {
	msg := &testpb.Widget{Name: "hello"}
	var hdr sr.ConfluentHeader
	buf := make([]byte, 0, 256)
	b.ReportAllocs()
	for b.Loop() {
		var err error
		buf, err = hdr.AppendEncode(buf[:0], 1, []int{0})
		if err != nil {
			b.Fatal(err)
		}
		buf, err = proto.MarshalOptions{}.MarshalAppend(buf, msg)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// Steady state: the schema is registered and the id is cached, which is what
// a producer does for every record after the first.
func BenchmarkEncode(b *testing.B) {
	ctx := context.Background()
	ser := benchSer(b)
	msg := &testpb.Widget{Name: "hello"}
	if _, err := ser.Encode(ctx, "topic", msg); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := ser.Encode(ctx, "topic", msg); err != nil {
			b.Fatal(err)
		}
	}
}

// As above but reusing the destination buffer, which is how a producer
// avoids an allocation per record.
func BenchmarkAppendEncodeReuse(b *testing.B) {
	ctx := context.Background()
	ser := benchSer(b)
	msg := &testpb.Widget{Name: "hello"}
	buf := make([]byte, 0, 256)
	var err error
	if buf, err = ser.AppendEncode(ctx, "topic", msg, buf); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if buf, err = ser.AppendEncode(ctx, "topic", msg, buf[:0]); err != nil {
			b.Fatal(err)
		}
	}
}

// A message whose schema imports another .proto, so the registered schema
// carries a reference. Steady state should be identical to the simple case.
func BenchmarkEncodeWithReferences(b *testing.B) {
	ctx := context.Background()
	ser := benchSer(b)
	msg := &testpb.Chain{Mid: &testpb.Mid{Leaf: &testpb.Leaf{Id: "leaf"}}}
	if _, err := ser.Encode(ctx, "topic", msg); err != nil {
		b.Fatal(err)
	}
	buf := make([]byte, 0, 256)
	var err error
	b.ReportAllocs()
	for b.Loop() {
		if buf, err = ser.AppendEncode(ctx, "topic", msg, buf[:0]); err != nil {
			b.Fatal(err)
		}
	}
}

// The cold path: derive the schema text, send it to the registry, and resolve
// the id. This happens once per (topic, message type), so a fresh Serializer
// is built per iteration. The registry is shared, because standing up an HTTP
// server per iteration runs the machine out of ephemeral ports.
func benchmarkEncodeCold(b *testing.B, msg proto.Message) {
	ctx := context.Background()
	cl := benchClient(b)
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		ser, err := protobuf.NewSerializer(cl)
		if err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
		if _, err := ser.Encode(ctx, "topic", msg); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkEncodeCold(b *testing.B) {
	benchmarkEncodeCold(b, &testpb.Widget{Name: "hello"})
}

// Cold encode where the schema also has references to register.
func BenchmarkEncodeColdWithReferences(b *testing.B) {
	benchmarkEncodeCold(b, &testpb.Chain{Mid: &testpb.Mid{Leaf: &testpb.Leaf{Id: "leaf"}}})
}

func BenchmarkDecode(b *testing.B) {
	ctx := context.Background()
	cl := benchClient(b)
	ser, err := protobuf.NewSerializer(cl)
	if err != nil {
		b.Fatal(err)
	}
	encoded, err := ser.Encode(ctx, "topic", &testpb.Widget{Name: "hello"})
	if err != nil {
		b.Fatal(err)
	}
	deser, err := protobuf.NewDeserializer(cl)
	if err != nil {
		b.Fatal(err)
	}
	var out testpb.Widget
	b.ReportAllocs()
	for b.Loop() {
		out.Reset()
		if err := deser.Decode(ctx, encoded, &out); err != nil {
			b.Fatal(err)
		}
	}
}

// Decode with header validation turned on, which resolves the schema id to a
// descriptor the first time and then compares cached names.
func BenchmarkDecodeVerified(b *testing.B) {
	ctx := context.Background()
	cl := benchClient(b)
	ser, err := protobuf.NewSerializer(cl)
	if err != nil {
		b.Fatal(err)
	}
	encoded, err := ser.Encode(ctx, "topic", &testpb.Widget{Name: "hello"})
	if err != nil {
		b.Fatal(err)
	}
	deser, err := protobuf.NewDeserializer(cl, protobuf.VerifyDecode())
	if err != nil {
		b.Fatal(err)
	}
	var out testpb.Widget
	if err := deser.Decode(ctx, encoded, &out); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		out.Reset()
		if err := deser.Decode(ctx, encoded, &out); err != nil {
			b.Fatal(err)
		}
	}
}

// DecodeNew resolves the schema id and message index to a registered Go type
// and allocates a fresh message.
func BenchmarkDecodeNew(b *testing.B) {
	ctx := context.Background()
	cl := benchClient(b)
	ser, err := protobuf.NewSerializer(cl)
	if err != nil {
		b.Fatal(err)
	}
	encoded, err := ser.Encode(ctx, "topic", &testpb.Widget{Name: "hello"})
	if err != nil {
		b.Fatal(err)
	}
	deser, err := protobuf.NewDeserializer(cl)
	if err != nil {
		b.Fatal(err)
	}
	if _, err := deser.DecodeNew(ctx, encoded); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := deser.DecodeNew(ctx, encoded); err != nil {
			b.Fatal(err)
		}
	}
}
