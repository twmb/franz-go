package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"
)

var (
	seedBrokers = flag.String("brokers", "localhost:9092", "comma delimited list of seed brokers")
	topic       = flag.String("topic", "", "topic to produce to or consume from")

	recordBytes   = flag.Int("record-bytes", 100, "bytes per record (producing)")
	noCompression = flag.Bool("no-compression", false, "set to disable snappy compression (producing)")
	poolProduce   = flag.Bool("pool", false, "if true, use a sync.Pool to reuse record slices (producing)")

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
	go func() {
		for range time.Tick(time.Second) {
			recs := atomic.SwapInt64(&rateRecs, 0)
			bytes := atomic.SwapInt64(&rateBytes, 0)
			fmt.Printf("%0.2f MiB/s; %0.2fk records/s\n", float64(bytes)/(1024*1024), float64(recs)/1000)
		}
	}()
}

func die(msg string, args ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", args...)
	os.Exit(1)
}

func chk(err error, msg string, args ...interface{}) {
	if err != nil {
		die(msg, args...)
	}
}

func main() {
	flag.Parse()

	brokers := strings.Split(*seedBrokers, ",")

	if *dialTLS {
		kafka.DefaultDialer.TLS = new(tls.Config)
	}
	if *saslMethod != "" || *saslUser != "" || *saslPass != "" {
		if *saslMethod == "" || *saslUser == "" || *saslPass == "" {
			die("all of -sasl-method, -sasl-user, -sasl-pass must be specified if any are")
		}
		method := strings.ToLower(*saslMethod)
		method = strings.ReplaceAll(method, "-", "")
		method = strings.ReplaceAll(method, "_", "")
		switch method {
		case "plain":
			kafka.DefaultDialer.SASLMechanism = plain.Mechanism{
				Username: *saslUser,
				Password: *saslPass,
			}
		case "scramsha256":
			m, err := scram.Mechanism(
				scram.SHA256,
				*saslUser,
				*saslPass,
			)
			chk(err, "scram-sha-256 err: %v", err)
			kafka.DefaultDialer.SASLMechanism = m
		case "scramsha512":
			m, err := scram.Mechanism(
				scram.SHA512,
				*saslUser,
				*saslPass,
			)
			chk(err, "scram-sha-512 err: %v", err)
			kafka.DefaultDialer.SASLMechanism = m
		default:
			die("unrecognized sasl option %s", *saslMethod)
		}
	}

	go printRate()

	switch *consume {
	case false:
		w := &kafka.Writer{
			Addr:         kafka.TCP(brokers...),
			Topic:        *topic,
			RequiredAcks: -1,
			Async:        true,
			Completion: func(ms []kafka.Message, err error) {
				chk(err, "produce err: %v", err)
				for _, m := range ms {
					atomic.AddInt64(&rateRecs, 1)
					atomic.AddInt64(&rateBytes, int64(len(m.Value)))

					if *poolProduce {
						p.Put(&m.Value)
					}
				}
			},
		}
		if !*noCompression {
			w.Compression = kafka.Snappy
		}

		var num int64
		for {
			w.WriteMessages(context.Background(), kafka.Message{
				Value: newValue(num),
			})
			num++
		}

	case true:
		cfg := kafka.ReaderConfig{
			Brokers:         brokers,
			Topic:           *topic,
			ReadLagInterval: -1,
		}
		if *group != "" {
			cfg.GroupID = *group
		}
		r := kafka.NewReader(cfg)
		if *group == "" {
			err := r.SetOffset(kafka.FirstOffset)
			chk(err, "unable to set offset: %v", err)
		}
		for {
			m, err := r.ReadMessage(context.Background())
			chk(err, "unable to consume: %v", err)
			atomic.AddInt64(&rateRecs, 1)
			atomic.AddInt64(&rateBytes, int64(len(m.Value)))
		}

	}
}

var p = sync.Pool{
	New: func() interface{} {
		s := make([]byte, *recordBytes)
		return &s
	},
}

func newValue(num int64) []byte {
	var buf [20]byte // max int64 takes 19 bytes, then we add a space
	b := strconv.AppendInt(buf[:0], num, 10)
	b = append(b, ' ')

	var s []byte
	if *poolProduce {
		s = *(p.Get().(*[]byte))
	} else {
		s = make([]byte, *recordBytes)
	}

	var n int
	for n != len(s) {
		n += copy(s[n:], b)
	}
	return s
}
