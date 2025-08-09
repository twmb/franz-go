package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"slices"
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

	batchSize       = flag.Int("batch-size", 10000, "value for the kafka.Writer.BatchSize field")
	acks            = flag.Int("acks", -1, "value for the Writer.Acks field")
	recordBytes     = flag.Int("record-bytes", 100, "bytes per record (producing)")
	compression     = flag.String("compression", "none", "compression algorithm to use (none,gzip,snappy,lz4,zstd, for producing)")
	poolProduce     = flag.Bool("pool", false, "if true, use a sync.Pool to reuse record slices (producing)")
	maxWriteThreads = flag.Int("max-write-threads", 5, "if idempotency is disabled, the number of produce requests to allow per broker")
	threads         = flag.Int("threads", 1, "number of threads to produce messages")

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
			BatchSize:    *batchSize,
			RequiredAcks: kafka.RequiredAcks(*acks),
			Async:        false, // batching requires this to be false
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
		switch strings.ToLower(*compression) {
		case "", "none":
		case "gzip":
			w.Compression = kafka.Gzip
		case "snappy":
			w.Compression = kafka.Snappy
		case "lz4":
			w.Compression = kafka.Lz4
		case "zstd":
			w.Compression = kafka.Zstd
		default:
			die("unrecognized compression %s", *compression)
		}

		ctx := context.Background()
		producer := NewProducer(w, *batchSize)
		go producer.Run(ctx, *maxWriteThreads)

		var counter atomic.Int64
		for range *threads {
			go func() {
				for {
					num := counter.Add(1)
					producer.Produce(&kafka.Message{Value: newValue(num)})
				}
			}()
		}
		select {}

	case true:
		cfg := kafka.ReaderConfig{
			Brokers:         brokers,
			Topic:           *topic,
			ReadLagInterval: -1,
			CommitInterval:  time.Second * 5,
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
	New: func() any {
		s := make([]byte, *recordBytes)
		return &s
	},
}

func newValue(num int64) []byte {
	var s []byte
	if *poolProduce {
		s = *(p.Get().(*[]byte))
	} else {
		s = make([]byte, *recordBytes)
	}
	formatValue(num, s)

	return s
}

func formatValue(num int64, v []byte) {
	var buf [20]byte // max int64 takes 19 bytes, then we add a space
	b := strconv.AppendInt(buf[:0], num, 10)
	b = append(b, ' ')

	n := copy(v, b)
	for n != len(v) {
		n += copy(v[n:], b)
	}
}

//------------------------------------------------------------------------------

type Producer struct {
	wr           *kafka.Writer
	ch           chan *kafka.Message
	maxBatchSize int
}

func NewProducer(wr *kafka.Writer, maxBatchSize int) *Producer {
	return &Producer{
		wr:           wr,
		ch:           make(chan *kafka.Message, 10*maxBatchSize),
		maxBatchSize: maxBatchSize,
	}
}

func (p *Producer) Produce(msg *kafka.Message) {
	p.ch <- msg
}

func (p *Producer) Run(ctx context.Context, maxThreads int) {
	ch := make(chan []*kafka.Message, maxThreads)

	for range maxThreads {
		go func() {
			msgs := make([]kafka.Message, p.maxBatchSize)

			for batch := range ch {
				for i, msg := range batch {
					msgs[i] = *msg
				}
				p.produceBatch(ctx, msgs[:len(batch)])
			}
		}()
	}

	batch := make([]*kafka.Message, 0, p.maxBatchSize)
	for msg := range p.ch {
		batch = append(batch, msg)
		if len(batch) == p.maxBatchSize {
			ch <- slices.Clone(batch)
			batch = batch[:0]
		}
	}
}

func (p *Producer) produceBatch(
	ctx context.Context, batch []kafka.Message,
) {
	if err := p.wr.WriteMessages(ctx, batch...); err != nil {
		slog.Error("WriteMessages failes", slog.Any("err", err))
	}
}
