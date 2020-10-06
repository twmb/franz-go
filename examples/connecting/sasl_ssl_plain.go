package connect

import (
	"context"
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

	opts := []kgo.Opt{
		kgo.SeedBrokers(seeds...),

		// SASL Options
		kgo.SASL(plain.Plain(func(context.Context) (plain.Auth, error) {
			return plain.Auth{
				User: user,
				Pass: password,
			}, nil
		})),

		// Configure TLS. Uses SystemCertPool for RootCAs by default.
		kgo.Dialer(func(ctx context.Context, host string) (net.Conn, error) {
			return (tls.Dialer{
				NetDialer: &net.Dialer{Timeout: 10 * time.Second},
			}).DialContext(ctx, "tcp", host)
		}),
	}

	client, err := kgo.NewClient(opts...)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	// Do something with the client
	// ...
}
