package connecting

import (
	"crypto/tls"
	"fmt"
	"net"
	"time"

	"github.com/twmb/kafka-go/pkg/kgo"
	"github.com/twmb/kafka-go/pkg/sasl/plain"
)

func connectSaslSslPlain() {
	fmt.Println("starting...")

	seeds := []string{"localhost:9092"}
	// SASL Plain credentials
	user := ""
	password := ""

	tlsDialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}}
	opts := []kgo.Opt{
		kgo.SeedBrokers(seeds...),

		// SASL Options
		kgo.SASL(plain.Auth{
			User: user,
			Pass: password,
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
