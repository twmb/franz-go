package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"net/http"
	_ "net/http/pprof"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

var (
	seedBrokers = flag.String("brokers", "localhost:9092", "comma delimited list of seed brokers")
	topic       = flag.String("topic", "", "topic to produce to or consume from")
	pprofPort   = flag.String("pprof", ":9876", "port to bind to for pprof, if non-empty")

	recordBytes   = flag.Int("record-bytes", 100, "bytes per record (producing)")
	noCompression = flag.Bool("no-compression", false, "set to disable snappy compression (producing)")
	poolProduce   = flag.Bool("pool", false, "if true, use a sync.Pool to reuse record structs/slices (producing)")

	consume = flag.Bool("consume", false, "if true, consume rather than produce")
	group   = flag.String("group", "", "if non-empty, group to use for consuming rather than direct partition consuming (consuming)")

	dialTLS = flag.Bool("tls", false, "if true, use tls for connecting")

	saslMethod = flag.String("sasl-method", "", "if non-empty, sasl method to use (must specify all options; supports plain, scram-sha-256, scram-sha-512, aws_msk_iam)")
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

	if *recordBytes <= 0 {
		die("record bytes must be larger than zero")
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),
		kgo.ProduceTopic(*topic),
		kgo.MaxBufferedRecords(250<<20 / *recordBytes + 1),
		kgo.AllowedConcurrentFetches(3),
		// We have good compression, so we want to limit what we read
		// back because snappy deflation will balloon our memory usage.
		kgo.FetchMaxBytes(5 << 20),
	}
	if *noCompression {
		opts = append(opts, kgo.BatchCompression(kgo.NoCompression()))
	}
	if *dialTLS {
		opts = append(opts, kgo.Dialer((new(tls.Dialer)).DialContext))
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
			opts = append(opts, kgo.SASL(plain.Auth{
				User: *saslUser,
				Pass: *saslPass,
			}.AsMechanism()))
		case "scramsha256":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: *saslUser,
				Pass: *saslPass,
			}.AsSha256Mechanism()))
		case "scramsha512":
			opts = append(opts, kgo.SASL(scram.Auth{
				User: *saslUser,
				Pass: *saslPass,
			}.AsSha512Mechanism()))
		case "awsmskiam":
			opts = append(opts, kgo.SASL(aws.Auth{
				AccessKey: *saslUser,
				SecretKey: *saslPass,
			}.AsManagedStreamingIAMMechanism()))
		default:
			die("unrecognized sasl option %s", *saslMethod)
		}
	}

	cl, err := kgo.NewClient(opts...)
	chk(err, "unable to initialize client: %v", err)

	if *pprofPort != "" {
		go func() {
			err := http.ListenAndServe(*pprofPort, nil)
			chk(err, "unable to run pprof listener: %v", err)
		}()
	}

	go printRate()

	switch *consume {
	case false:
		var num int64
		for {
			cl.Produce(context.Background(), newRecord(num), func(r *kgo.Record, err error) {
				if *poolProduce {
					p.Put(r)
				}
				chk(err, "produce error: %v", err)
				atomic.AddInt64(&rateRecs, 1)
				atomic.AddInt64(&rateBytes, int64(*recordBytes))
			})
			num++
		}
	case true:
		if *group != "" {
			cl.AssignGroup(*group, kgo.GroupTopics(*topic))
		} else {
			cl.AssignPartitions(kgo.ConsumeTopics(kgo.NewOffset().AtStart(), *topic))
		}

		for {
			fetches := cl.PollFetches(context.Background())
			fetches.EachError(func(t string, p int32, err error) {
				chk(err, "topic %s partition %d had error: %v", t, p, err)
			})
			var recs int64
			var bytes int64
			fetches.EachRecord(func(r *kgo.Record) {
				recs++
				bytes += int64(len(r.Value))
			})
			atomic.AddInt64(&rateRecs, recs)
			atomic.AddInt64(&rateBytes, bytes)
		}
	}
}

var p = sync.Pool{
	New: func() interface{} {
		s := make([]byte, *recordBytes)
		return &kgo.Record{Value: s}
	},
}

func newRecord(num int64) *kgo.Record {
	var buf [20]byte // max int64 takes 19 bytes, then we add a space
	b := strconv.AppendInt(buf[:0], num, 10)
	b = append(b, ' ')

	var r *kgo.Record
	if *poolProduce {
		r = p.Get().(*kgo.Record)
	} else {
		r = kgo.SliceRecord(make([]byte, *recordBytes))
	}

	var n int
	for n != len(r.Value) {
		n += copy(r.Value[n:], b)
	}
	return r
}
