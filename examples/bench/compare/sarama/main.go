package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/IBM/sarama"
)

var (
	seedBrokers = flag.String("brokers", "localhost:9092", "comma delimited list of seed brokers")
	topic       = flag.String("topic", "", "topic to produce to or consume from")

	recordBytes   = flag.Int("record-bytes", 100, "bytes per record (producing)")
	noCompression = flag.Bool("no-compression", false, "set to disable snappy compression (producing)")

	consume = flag.Bool("consume", false, "if true, consume rather than produce")
	group   = flag.String("group", "", "if non-empty, group to use for consuming rather than direct partition consuming (consuming)")

	dialTLS = flag.Bool("tls", false, "if true, use tls for connecting")

	saslMethod = flag.String("sasl-method", "", "if non-empty, sasl method to use (must specify all options; supports plain, scram-sha-256, scram-sha-512)")
	saslUser   = flag.String("sasl-user", "", "if non-empty, username to use for sasl (must specify all options)")
	saslPass   = flag.String("sasl-pass", "", "if non-empty, password to use for sasl (must specify all options)")

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

	brokers := strings.Split(*seedBrokers, ",")

	go printRate()

	cfg := sarama.NewConfig()
	cfg.Version = sarama.V2_8_0_0
	if *dialTLS {
		cfg.Net.TLS.Enable = true
	}
	if *saslMethod != "" || *saslUser != "" || *saslPass != "" {
		if *saslMethod == "" || *saslUser == "" || *saslPass == "" {
			die("all of -sasl-method, -sasl-user, -sasl-pass must be specified if any are")
		}
		method := strings.ToLower(*saslMethod)
		method = strings.ReplaceAll(method, "-", "")
		method = strings.ReplaceAll(method, "_", "")
		cfg.Net.SASL.Enable = true
		cfg.Net.SASL.Version = 1
		cfg.Net.SASL.User = *saslUser
		cfg.Net.SASL.Password = *saslPass
		switch method {
		case "plain":
			cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
		case "scramsha256":
			cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		case "scramsha512":
			cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		default:
			die("unrecognized sasl option %s", *saslMethod)
		}
	}

	switch *consume {
	case false:
		cfg.Producer.RequiredAcks = sarama.WaitForAll
		cfg.Producer.Compression = sarama.CompressionSnappy
		cfg.Producer.Return.Successes = true

		p, err := sarama.NewAsyncProducer(brokers, cfg)
		chk(err, "unable to create producer: %v", err)

		go func() {
			for err := range p.Errors() {
				die("produce error: %v", err)
			}
		}()
		go func() {
			for range p.Successes() {
				atomic.AddInt64(&rateRecs, 1)
				atomic.AddInt64(&rateBytes, int64(*recordBytes))
			}
		}()

		var num int64
		for {
			p.Input() <- &sarama.ProducerMessage{Topic: *topic, Value: sarama.ByteEncoder(newValue(num))}
			num++
		}

	case true:
		cfg.Consumer.Return.Errors = true
		cfg.Consumer.Offsets.Initial = sarama.OffsetOldest

		if *group == "" {
			c, err := sarama.NewConsumer(brokers, cfg)
			chk(err, "unable to create consumer: %v", err)
			ps, err := c.Partitions(*topic)
			chk(err, "unable to get partitions for topic %s: %v", *topic, err)

			for _, p := range ps {
				go func(p int32) {
					pc, err := c.ConsumePartition(*topic, p, sarama.OffsetOldest)
					chk(err, "unable to consume topic %s partition %d: %v", *topic, p, err)
					for {
						msg := <-pc.Messages()
						atomic.AddInt64(&rateRecs, 1)
						atomic.AddInt64(&rateBytes, int64(len(msg.Value)))
					}
				}(p)
			}
			select {}
		}

		g, err := sarama.NewConsumerGroup(brokers, *group, cfg)
		chk(err, "unable to create group consumer: %v", err)

		go func() {
			for err := range g.Errors() {
				chk(err, "consumer group error: %v", err)
			}
		}()

		for {
			err := g.Consume(context.Background(), []string{*topic}, new(consumer))
			chk(err, "consumer group err: %v", err)
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

type consumer struct{}

func (*consumer) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (*consumer) Cleanup(sarama.ConsumerGroupSession) error { return nil }
func (*consumer) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for msg := range claim.Messages() {
		atomic.AddInt64(&rateRecs, 1)
		atomic.AddInt64(&rateBytes, int64(len(msg.Value)))
		sess.MarkMessage(msg, "")
	}
	return nil
}
