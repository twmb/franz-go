package main

import (
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

var (
	seedBrokers = flag.String("brokers", "localhost:9092", "comma delimited list of seed brokers")

	user = flag.String("user", "", "username for PLAIN sasl auth")
	pass = flag.String("pass", "", "password for PLAIN sasl auth")
)

func main() {
	flag.Parse()

	fmt.Println("starting...")

	tlsDialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}}
	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),

		// SASL Options
		kgo.SASL(plain.Auth{
			User: *user,
			Pass: *pass,
		}.AsMechanism()),

		// Configure TLS. Uses SystemCertPool for RootCAs by default.
		kgo.Dialer(tlsDialer.DialContext),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Do something with the client
	// ...
}
