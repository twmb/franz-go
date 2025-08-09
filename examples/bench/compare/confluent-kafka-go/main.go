package main

import (
	"flag"
	"fmt"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	seedBrokers = flag.String("brokers", "localhost:9092", "comma delimited list of seed brokers")
	topic       = flag.String("topic", "", "topic to produce to or consume from")

	recordBytes   = flag.Int("record-bytes", 100, "bytes per record (producing)")
	noCompression = flag.Bool("no-compression", false, "set to disable snappy compression (producing)")
	threads       = flag.Int("threads", 1, "number of threads to produce messages")

	consume = flag.Bool("consume", false, "if true, consume rather than produce")
	group   = flag.String("group", "", "if non-empty, group to use for consuming rather than direct partition consuming (consuming)")

	rateRecs  int64
	rateBytes int64
)

func printRate() {
	for range time.Tick(time.Second) {
		recs := atomic.SwapInt64(&rateRecs, 0)
		bytes := atomic.SwapInt64(&rateBytes, 0)
		fmt.Printf("%0.2f MiB/s; %0.2fk records/s\n", float64(bytes)/(1024*1024), float64(recs)/1000)
	}
}

func die(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func chk(err error, msg string, args ...any) {
	if err != nil {
		die(msg, args...)
	}
}

func main() {
	flag.Parse()

	go printRate()

	switch *consume {
	case false:
		cfg := &kafka.ConfigMap{
			"bootstrap.servers":  *seedBrokers,
			"enable.idempotence": true,
		}
		if !*noCompression {
			(*cfg)["compression.codec"] = "snappy"
		}
		p, err := kafka.NewProducer(cfg)
		chk(err, "unable to create producer: %v", err)

		go func() {
			for {
				switch ev := (<-p.Events()).(type) {
				case *kafka.Message:
					err := ev.TopicPartition.Error
					chk(err, "produce error: %v", err)
					atomic.AddInt64(&rateRecs, 1)
					atomic.AddInt64(&rateBytes, int64(*recordBytes))
				case kafka.Error:
					if ev.IsFatal() {
						die("fatal err: %v", err)
					}
				}
			}
		}()

		var counter atomic.Int64
		for range *threads {
			go func() {
				for {
					num := counter.Add(1)
					err = p.Produce(&kafka.Message{
						TopicPartition: kafka.TopicPartition{Topic: topic, Partition: kafka.PartitionAny},
						Value:          newValue(num),
					}, nil)
					chk(err, "unable to produce: %v", err)
				}
			}()
		}
		select {}

	case true:
		cfg := &kafka.ConfigMap{
			"bootstrap.servers": *seedBrokers,
			"auto.offset.reset": "earliest",
			"group.id":          *group,
		}
		c, err := kafka.NewConsumer(cfg)
		chk(err, "unable to create consumer: %v", err)

		if err := c.Subscribe(*topic, nil); err != nil {
			die("unable to subscribe to topic: %v", err)
		}
		for {
			ev := c.Poll(100)
			if ev == nil {
				continue
			}
			switch e := ev.(type) {
			case *kafka.Message:
				atomic.AddInt64(&rateRecs, 1)
				atomic.AddInt64(&rateBytes, int64(len(e.Value)))
			case kafka.Error:
				die("consume error: %v", err)
			}
		}
	}
}

func newValue(num int64) []byte {
	var buf [20]byte // max int64 takes 19 bytes, then we add a space
	b := strconv.AppendInt(buf[:0], num, 10)
	b = append(b, ' ')

	s := make([]byte, *recordBytes)

	var n int
	for n != len(s) {
		n += copy(s[n:], b)
	}
	return s
}
