// This example demonstrates the RecordFormatter and RecordReader, which
// provide printf-style formatting for records. These are useful for building
// CLI tools, log processing, and format conversion.
//
// The formatter uses percent verbs:
//
//	%t  topic          %T  topic length
//	%k  key            %K  key length
//	%v  value          %V  value length
//	%p  partition      %o  offset
//	%d  timestamp      %H  number of headers
//	%h  header spec    %e  leader epoch
//
// See NewRecordFormatter documentation for the full specification.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

var (
	seedBrokers = flag.String("brokers", "localhost:9092", "comma delimited list of seed brokers")
	topic       = flag.String("topic", "formatter-example", "topic to produce to and consume from")
	layout      = flag.String("format", "%t [p%p o%o] %d{UnixMilli}: key=%k value=%v headers=%H%h{ %k=%v}\n",
		"record format layout (see RecordFormatter docs)")
)

func die(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, msg, args...)
	os.Exit(1)
}

func main() {
	flag.Parse()

	// Create a formatter from the layout string.
	formatter, err := kgo.NewRecordFormatter(*layout)
	if err != nil {
		die("invalid format layout: %v\n", err)
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),
		kgo.DefaultProduceTopic(*topic),
		kgo.ConsumeTopics(*topic),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		die("unable to create client: %v\n", err)
	}
	defer cl.Close()

	ctx := context.Background()

	// Produce some sample records with headers.
	for i := range 5 {
		r := &kgo.Record{
			Key:   []byte(fmt.Sprintf("key-%d", i)),
			Value: []byte(fmt.Sprintf("value-%d", i)),
			Headers: []kgo.RecordHeader{
				{Key: "source", Value: []byte("example")},
				{Key: "index", Value: []byte(fmt.Sprintf("%d", i))},
			},
			Timestamp: time.Now(),
		}
		cl.Produce(ctx, r, func(_ *kgo.Record, err error) {
			if err != nil {
				fmt.Printf("produce error: %v\n", err)
			}
		})
	}
	if err := cl.Flush(ctx); err != nil {
		die("flush error: %v\n", err)
	}

	// Consume and format the records.
	fmt.Println("--- formatted records ---")
	for {
		fetches := cl.PollRecords(ctx, 10)
		if fetches.IsClientClosed() {
			return
		}
		fetches.EachRecord(func(r *kgo.Record) {
			out := formatter.AppendRecord(nil, r)
			os.Stdout.Write(out)
		})
		if fetches.NumRecords() > 0 {
			break
		}
	}

	// You can also use the RecordReader to parse records from formatted
	// text. Here we create a reader that parses "key\tvalue\n" lines.
	fmt.Println("\n--- record reader demo ---")
	input := strings.NewReader("hello\tworld\ngoodbye\tplanet\n")
	reader, err := kgo.NewRecordReader(input, "%k\t%v\n")
	if err != nil {
		die("invalid reader layout: %v\n", err)
	}
	for {
		r, err := reader.ReadRecord()
		if err != nil {
			break
		}
		r.Topic = *topic
		fmt.Printf("parsed record: key=%s value=%s\n", string(r.Key), string(r.Value))
	}
}
