// This example demonstrates SASL authentication with TLS. Run with -method
// to select the SASL mechanism:
//
//   - plain: SASL/PLAIN (requires -user and -pass)
//   - scram-sha-256: SCRAM-SHA-256 (requires -user and -pass)
//   - scram-sha-512: SCRAM-SHA-512 (requires -user and -pass)
//   - aws-msk-iam: AWS MSK IAM (uses AWS SDK default credential chain)
//
// All methods use TLS by default. For plain/scram, the client uses the system
// certificate pool. For AWS MSK IAM, credentials come from the AWS SDK.
package main

import (
	"context"
	"crypto/tls"
	"flag"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

var (
	seedBrokers = flag.String("brokers", "localhost:9092", "comma delimited list of seed brokers")
	method      = flag.String("method", "plain", "SASL method: plain, scram-sha-256, scram-sha-512, aws-msk-iam")
	user        = flag.String("user", "", "username for PLAIN or SCRAM")
	pass        = flag.String("pass", "", "password for PLAIN or SCRAM")
)

func die(msg string, args ...any) {
	fmt.Fprintf(os.Stderr, msg, args...)
	os.Exit(1)
}

func main() {
	flag.Parse()

	tlsDialer := &tls.Dialer{NetDialer: &net.Dialer{Timeout: 10 * time.Second}}
	opts := []kgo.Opt{
		kgo.SeedBrokers(strings.Split(*seedBrokers, ",")...),
		kgo.Dialer(tlsDialer.DialContext),
	}

	switch *method {
	case "plain":
		if *user == "" || *pass == "" {
			die("-user and -pass are required for plain\n")
		}
		opts = append(opts, kgo.SASL(plain.Auth{
			User: *user,
			Pass: *pass,
		}.AsMechanism()))

	case "scram-sha-256":
		if *user == "" || *pass == "" {
			die("-user and -pass are required for scram-sha-256\n")
		}
		opts = append(opts, kgo.SASL(scram.Auth{
			User: *user,
			Pass: *pass,
		}.AsSha256Mechanism()))

	case "scram-sha-512":
		if *user == "" || *pass == "" {
			die("-user and -pass are required for scram-sha-512\n")
		}
		opts = append(opts, kgo.SASL(scram.Auth{
			User: *user,
			Pass: *pass,
		}.AsSha512Mechanism()))

	case "aws-msk-iam":
		sess, err := session.NewSession()
		if err != nil {
			die("unable to initialize aws session: %v\n", err)
		}
		opts = append(opts, kgo.SASL(aws.ManagedStreamingIAM(func(ctx context.Context) (aws.Auth, error) {
			val, err := sess.Config.Credentials.GetWithContext(ctx)
			if err != nil {
				return aws.Auth{}, err
			}
			return aws.Auth{
				AccessKey:    val.AccessKeyID,
				SecretKey:    val.SecretAccessKey,
				SessionToken: val.SessionToken,
				UserAgent:    "franz-go/example/v1.0.0",
			}, nil
		})))

	default:
		die("unknown method %q\n", *method)
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		die("unable to create client: %v\n", err)
	}
	defer cl.Close()

	if err := cl.Ping(context.Background()); err != nil {
		die("unable to ping cluster: %v\n", err)
	}
	fmt.Println("successfully connected and authenticated")
}
