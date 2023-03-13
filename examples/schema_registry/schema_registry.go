package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/hamba/avro"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sr"
)

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

var schemaText = `{
	"type": "record",
	"name": "simple",
	"namespace": "org.hamba.avro",
	"fields" : [
		{"name": "a", "type": "long"},
		{"name": "b", "type": "string"}
	]
}`

type example struct {
	A int64  `avro:"a"`
	B string `avro:"b"`
}

func main() {
	flag.Parse()

	// Ensure our schema is registered.
	rcl, err := sr.NewClient(sr.URLs(*registry))
	maybeDie(err, "unable to create schema registry client: %v", err)
	ss, err := rcl.CreateSchema(context.Background(), *topic+"-value", sr.Schema{
		Schema: schemaText,
		Type:   sr.TypeAvro,
	})
	maybeDie(err, "unable to create avro schema: %v", err)
	fmt.Printf("created or reusing schema subject %q version %d id %d\n", ss.Subject, ss.Version, ss.ID)

	// Setup our serializer / deserializer.
	avroSchema, err := avro.Parse(schemaText)
	maybeDie(err, "unable to parse avro schema: %v", err)
	var serde sr.Serde
	serde.Register(
		ss.ID,
		example{},
		sr.EncodeFn(func(v any) ([]byte, error) {
			return avro.Marshal(avroSchema, v)
		}),
		sr.DecodeFn(func(b []byte, v any) error {
			return avro.Unmarshal(avroSchema, b, v)
		}),
	)

	// Loop producing & consuming.
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),
		kgo.DefaultProduceTopic(*topic),
		kgo.ConsumeTopics(*topic),
	)
	maybeDie(err, "unable to init kgo client: %v", err)
	for {
		cl.Produce(
			context.Background(),
			&kgo.Record{
				Value: serde.MustEncode(example{
					A: time.Now().Unix(),
					B: "hello",
				}),
			},
			func(r *kgo.Record, err error) {
				maybeDie(err, "unable to produce: %v", err)
				fmt.Printf("Produced simple record, value bytes: %x\n", r.Value)
			},
		)

		fs := cl.PollFetches(context.Background())
		fs.EachRecord(func(r *kgo.Record) {
			var ex example
			err := serde.Decode(r.Value, &ex)
			maybeDie(err, "unable to decode record value: %v")

			fmt.Printf("Consumed example: %+v, sleeping 1s\n", ex)
		})
		time.Sleep(time.Second)
	}
}
