// This example demonstrates using the franz-go schema registry protobuf serde
// with automatic schema registration and Confluent wire-format encoding.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"schema_registry_protobuf/pb"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
	"github.com/twmb/franz-go/pkg/sr/serde/protobuf"
)

//go:generate protoc --go_out=. --go_opt=module=schema_registry_protobuf -I proto proto/widget.proto

var (
	seedBrokers = flag.String("brokers", "localhost:9092", "comma delimited list of seed brokers")
	topic       = flag.String("topic", "test", "topic to produce to and consume from")
	registry    = flag.String("registry", "localhost:8081", "schema registry port to talk to")
)

func die(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func maybeDie(err error, msg string, args ...any) {
	if err != nil {
		die(msg, args...)
	}
}

func main() {
	flag.Parse()
	ctx := context.Background()

	rcl, err := sr.NewClient(sr.URLs(*registry))
	maybeDie(err, "unable to create schema registry client: %v", err)

	ser, err := protobuf.NewSerializer(rcl)
	maybeDie(err, "unable to create protobuf serializer: %v", err)
	deser, err := protobuf.NewDeserializer(rcl)
	maybeDie(err, "unable to create protobuf deserializer: %v", err)
	err = deser.RegisterMessage(&pb.Widget{})
	maybeDie(err, "unable to register widget message: %v", err)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),
		kgo.DefaultProduceTopic(*topic),
		kgo.ConsumeTopics(*topic),
	)
	maybeDie(err, "unable to init kgo client: %v", err)

	for {
		msg := &pb.Widget{Name: fmt.Sprintf("hello-%d", time.Now().Unix())}
		value, err := ser.Encode(ctx, *topic, msg)
		maybeDie(err, "unable to encode: %v", err)

		cl.Produce(
			ctx,
			&kgo.Record{Value: value},
			func(r *kgo.Record, err error) {
				maybeDie(err, "unable to produce: %v", err)
				fmt.Printf("Produced widget record, value bytes: %x\n", r.Value)
			},
		)

		fs := cl.PollFetches(ctx)
		fs.EachRecord(func(r *kgo.Record) {
			var w pb.Widget
			err := deser.Decode(ctx, r.Value, &w)
			maybeDie(err, "unable to decode record value: %v", err)
			fmt.Printf("Consumed widget: %+v, sleeping 1s\n", &w)
		})
		time.Sleep(time.Second)
	}
}
