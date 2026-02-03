package kerberos_test

import (
	"context"
	"os"
	"strings"
	"testing"

	"github.com/jcmturner/gokrb5/v8/client"
	"github.com/jcmturner/gokrb5/v8/config"
	"github.com/jcmturner/gokrb5/v8/keytab"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl/kerberos"
)

func TestKerberosAuth(t *testing.T) {
	seeds := os.Getenv("KGO_KERBEROS_SEEDS")
	if seeds == "" {
		t.Skip("KGO_KERBEROS_SEEDS not set")
	}

	keytabPath := os.Getenv("KGO_KERBEROS_KEYTAB")
	if keytabPath == "" {
		t.Skip("KGO_KERBEROS_KEYTAB not set")
	}

	krb5ConfPath := os.Getenv("KGO_KERBEROS_CONF")
	if krb5ConfPath == "" {
		t.Skip("KGO_KERBEROS_CONF not set")
	}

	principal := os.Getenv("KGO_KERBEROS_PRINCIPAL")
	if principal == "" {
		t.Skip("KGO_KERBEROS_PRINCIPAL not set")
	}

	realm := os.Getenv("KGO_KERBEROS_REALM")
	if realm == "" {
		t.Skip("KGO_KERBEROS_REALM not set")
	}

	service := os.Getenv("KGO_KERBEROS_SERVICE")
	if service == "" {
		service = "kafka"
	}

	kt, err := keytab.Load(keytabPath)
	if err != nil {
		t.Fatalf("failed to load keytab: %v", err)
	}

	cfg, err := config.Load(krb5ConfPath)
	if err != nil {
		t.Fatalf("failed to load krb5.conf: %v", err)
	}

	cl := client.NewWithKeytab(principal, realm, kt, cfg)

	auth := kerberos.Auth{
		Client:  cl,
		Service: service,
	}

	kgoClient, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(seeds, ",")...),
		kgo.SASL(auth.AsMechanismWithClose()),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		t.Fatalf("failed to create kgo client: %v", err)
	}
	defer kgoClient.Close()

	ctx := context.Background()

	// Test connectivity by pinging Kafka
	if err := kgoClient.Ping(ctx); err != nil {
		t.Fatalf("failed to ping Kafka: %v", err)
	}
	t.Log("successfully authenticated with Kerberos and pinged Kafka")

	// Produce a test message
	topic := "kerberos-test-topic"
	record := &kgo.Record{
		Topic: topic,
		Value: []byte("test message from kerberos auth"),
	}

	results := kgoClient.ProduceSync(ctx, record)
	if err := results.FirstErr(); err != nil {
		t.Fatalf("failed to produce message: %v", err)
	}
	t.Log("successfully produced message with Kerberos auth")

	// Consume the message back
	kgoClient.AddConsumeTopics(topic)
	fetches := kgoClient.PollFetches(ctx)
	if errs := fetches.Errors(); len(errs) > 0 {
		t.Fatalf("fetch errors: %v", errs)
	}

	var found bool
	fetches.EachRecord(func(r *kgo.Record) {
		if string(r.Value) == "test message from kerberos auth" {
			found = true
		}
	})

	if !found {
		t.Fatal("did not find expected message")
	}
	t.Log("successfully consumed message with Kerberos auth")
}
