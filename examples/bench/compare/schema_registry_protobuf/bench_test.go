// Package srprotobench compares franz-go's schema registry protobuf serde
// against confluent-kafka-go v2's, over the same in-memory registry and the
// same message, so the numbers are directly comparable.
//
// Requires CGO_ENABLED=1: confluent-kafka-go's schemaregistry package reaches
// into its cgo kafka package for a version string, so it does not build
// without cgo even though nothing here talks to a broker.
//
//	CGO_ENABLED=1 go test -bench . -benchmem
package srprotobench

import (
	"context"
	"encoding/binary"
	"testing"

	ckgsr "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry"
	ckgserde "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde"
	ckgprotobuf "github.com/confluentinc/confluent-kafka-go/v2/schemaregistry/serde/protobuf"

	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/pkg/sr/serde/protobuf"
	"github.com/twmb/franz-go/pkg/sr/srfake"

	"srprotobench/pb"
)

const topic = "bench"

func msg() *pb.Widget {
	return &pb.Widget{Name: "hello", Id: 42, Tags: []string{"a", "b"}}
}

// registry stands up one in-memory Confluent-compatible registry that both
// clients talk to.
func registry(tb testing.TB) string {
	tb.Helper()
	reg := srfake.New()
	tb.Cleanup(reg.Close)
	return reg.URL()
}

func franzSerde(tb testing.TB, url string, opts ...protobuf.Opt) (*protobuf.Serializer, *protobuf.Deserializer) {
	tb.Helper()
	cl, err := sr.NewClient(sr.URLs(url))
	if err != nil {
		tb.Fatal(err)
	}
	ser, err := protobuf.NewSerializer(cl, opts...)
	if err != nil {
		tb.Fatal(err)
	}
	deser, err := protobuf.NewDeserializer(cl)
	if err != nil {
		tb.Fatal(err)
	}
	if err := deser.RegisterMessage(&pb.Widget{}); err != nil {
		tb.Fatal(err)
	}
	return ser, deser
}

// schemaID pulls the id back out of the wire bytes: magic byte, then the id
// as a big endian uint32.
func schemaID(b []byte) uint32 { return binary.BigEndian.Uint32(b[1:5]) }

func confluentSerde(tb testing.TB, url string) (*ckgprotobuf.Serializer, *ckgprotobuf.Deserializer) {
	tb.Helper()
	cl, err := ckgsr.NewClient(ckgsr.NewConfig(url))
	if err != nil {
		tb.Fatal(err)
	}
	ser, err := ckgprotobuf.NewSerializer(cl, ckgserde.ValueSerde, ckgprotobuf.NewSerializerConfig())
	if err != nil {
		tb.Fatal(err)
	}
	deser, err := ckgprotobuf.NewDeserializer(cl, ckgserde.ValueSerde, ckgprotobuf.NewDeserializerConfig())
	if err != nil {
		tb.Fatal(err)
	}
	if err := deser.ProtoRegistry.RegisterMessage((&pb.Widget{}).ProtoReflect().Type()); err != nil {
		tb.Fatal(err)
	}
	return ser, deser
}

// TestSerdesInterop points both clients at one registry and checks that each
// can read what the other wrote. This is the interop guarantee that matters:
// the framing and the message indexes have to line up.
func TestSerdesInterop(t *testing.T) {
	ctx := context.Background()
	url := registry(t)

	franzSer, franzDeser := franzSerde(t, url)
	ckgSer, ckgDeser := confluentSerde(t, url)

	franzBytes, err := franzSer.Encode(ctx, topic, msg())
	if err != nil {
		t.Fatal(err)
	}
	ckgBytes, err := ckgSer.Serialize(topic, msg())
	if err != nil {
		t.Fatal(err)
	}

	var into pb.Widget
	if err := franzDeser.Decode(ctx, ckgBytes, &into); err != nil {
		t.Fatalf("franz-go could not decode confluent bytes: %v", err)
	}
	if into.GetName() != "hello" || into.GetId() != 42 {
		t.Errorf("franz-go decoded confluent bytes as %+v", &into)
	}

	out, err := ckgDeser.Deserialize(topic, franzBytes)
	if err != nil {
		t.Fatalf("confluent could not decode franz-go bytes: %v", err)
	}
	if got, ok := out.(*pb.Widget); !ok {
		t.Errorf("confluent decoded franz-go bytes as %T", out)
	} else if got.GetName() != "hello" || got.GetId() != 42 {
		t.Errorf("confluent decoded franz-go bytes as %+v", got)
	}
}

// TestCodegenOptionStripping documents the one visible difference between the
// two clients. franz-go strips go_package and friends before registering,
// because those options describe the producer's codegen rather than the
// message, and a Java producer of the same message would not send them.
// confluent-kafka-go registers the schema verbatim. Against a shared subject
// that is two different schema texts, so two different ids.
//
// KeepCodegenOptions() turns the stripping off for anyone who needs to land
// on the exact same id as an existing confluent-kafka-go producer.
func TestCodegenOptionStripping(t *testing.T) {
	ctx := context.Background()

	t.Run("default strips, so ids differ", func(t *testing.T) {
		url := registry(t)
		ckgSer, _ := confluentSerde(t, url)
		franzSer, _ := franzSerde(t, url)

		ckgBytes, err := ckgSer.Serialize(topic, msg())
		if err != nil {
			t.Fatal(err)
		}
		franzBytes, err := franzSer.Encode(ctx, topic, msg())
		if err != nil {
			t.Fatal(err)
		}
		if schemaID(franzBytes) == schemaID(ckgBytes) {
			t.Errorf("expected differing ids, both got %d", schemaID(franzBytes))
		}
	})

	t.Run("KeepCodegenOptions matches confluent", func(t *testing.T) {
		url := registry(t)
		ckgSer, _ := confluentSerde(t, url)
		franzSer, _ := franzSerde(t, url, protobuf.KeepCodegenOptions())

		ckgBytes, err := ckgSer.Serialize(topic, msg())
		if err != nil {
			t.Fatal(err)
		}
		franzBytes, err := franzSer.Encode(ctx, topic, msg())
		if err != nil {
			t.Fatal(err)
		}
		if string(franzBytes) != string(ckgBytes) {
			t.Errorf("wire bytes differ:\n franz-go: %x\nconfluent: %x", franzBytes, ckgBytes)
		}
	})
}

func BenchmarkFranzGoEncode(b *testing.B) {
	ctx := context.Background()
	ser, _ := franzSerde(b, registry(b))
	m := msg()
	if _, err := ser.Encode(ctx, topic, m); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := ser.Encode(ctx, topic, m); err != nil {
			b.Fatal(err)
		}
	}
}

// franz-go additionally lets the caller supply the destination buffer, which
// confluent-kafka-go has no equivalent for.
func BenchmarkFranzGoAppendEncode(b *testing.B) {
	ctx := context.Background()
	ser, _ := franzSerde(b, registry(b))
	m := msg()
	buf := make([]byte, 0, 256)
	var err error
	if buf, err = ser.AppendEncode(ctx, topic, m, buf); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if buf, err = ser.AppendEncode(ctx, topic, m, buf[:0]); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConfluentEncode(b *testing.B) {
	ser, _ := confluentSerde(b, registry(b))
	m := msg()
	if _, err := ser.Serialize(topic, m); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := ser.Serialize(topic, m); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkFranzGoDecode(b *testing.B) {
	ctx := context.Background()
	ser, deser := franzSerde(b, registry(b))
	encoded, err := ser.Encode(ctx, topic, msg())
	if err != nil {
		b.Fatal(err)
	}
	var out pb.Widget
	b.ReportAllocs()
	for b.Loop() {
		out.Reset()
		if err := deser.Decode(ctx, encoded, &out); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkConfluentDecodeInto(b *testing.B) {
	ser, deser := confluentSerde(b, registry(b))
	encoded, err := ser.Serialize(topic, msg())
	if err != nil {
		b.Fatal(err)
	}
	var out pb.Widget
	if err := deser.DeserializeInto(topic, encoded, &out); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		out.Reset()
		if err := deser.DeserializeInto(topic, encoded, &out); err != nil {
			b.Fatal(err)
		}
	}
}

// The closest confluent equivalent of franz-go's DecodeNew: resolve the type
// from the schema id and allocate a new message.
func BenchmarkFranzGoDecodeNew(b *testing.B) {
	ctx := context.Background()
	ser, deser := franzSerde(b, registry(b))
	encoded, err := ser.Encode(ctx, topic, msg())
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

func BenchmarkConfluentDeserialize(b *testing.B) {
	ser, deser := confluentSerde(b, registry(b))
	encoded, err := ser.Serialize(topic, msg())
	if err != nil {
		b.Fatal(err)
	}
	if _, err := deser.Deserialize(topic, encoded); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	for b.Loop() {
		if _, err := deser.Deserialize(topic, encoded); err != nil {
			b.Fatal(err)
		}
	}
}
