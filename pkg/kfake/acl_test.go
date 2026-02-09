package kfake

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

func TestACLsDisabled(t *testing.T) {
	t.Parallel()
	c, err := NewCluster(NumBrokers(1), SeedTopics(1, "test-topic"))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic("test-topic"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := cl.ProduceSync(ctx, kgo.StringRecord("test")).FirstErr(); err != nil {
		t.Fatalf("produce should work without ACLs: %v", err)
	}
}

func TestACLsEnabled(t *testing.T) {
	t.Parallel()
	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(1, "test-topic"),
		EnableACLs(),
		EnableSASL(),
		User("PLAIN", "testuser", "testpass"), // no ACLs
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic("test-topic"),
		kgo.SASL(plain.Plain(func(_ context.Context) (plain.Auth, error) {
			return plain.Auth{User: "testuser", Pass: "testpass"}, nil
		})),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = cl.ProduceSync(ctx, kgo.StringRecord("test")).FirstErr()
	if err == nil {
		t.Fatal("produce should fail without WRITE ACL")
	}
	if !isAuthzError(err) {
		t.Fatalf("expected authorization error, got: %v", err)
	}
}

func TestACLsWithPermission(t *testing.T) {
	t.Parallel()
	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(1, "test-topic"),
		EnableACLs(),
		EnableSASL(),
		User("PLAIN", "testuser", "testpass",
			ACL{Resource: kmsg.ACLResourceTypeTopic, Name: "test-topic", Pattern: kmsg.ACLResourcePatternTypeLiteral, Operation: kmsg.ACLOperationWrite, Allow: true},
			ACL{Resource: kmsg.ACLResourceTypeTopic, Name: "test-topic", Pattern: kmsg.ACLResourcePatternTypeLiteral, Operation: kmsg.ACLOperationDescribe, Allow: true},
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic("test-topic"),
		kgo.SASL(plain.Plain(func(_ context.Context) (plain.Auth, error) {
			return plain.Auth{User: "testuser", Pass: "testpass"}, nil
		})),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := cl.ProduceSync(ctx, kgo.StringRecord("test")).FirstErr(); err != nil {
		t.Fatalf("produce should work with WRITE ACL: %v", err)
	}
}

func TestACLsSuperuser(t *testing.T) {
	t.Parallel()
	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(1, "test-topic"),
		EnableACLs(),
		Superuser("PLAIN", "admin", "adminpass"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic("test-topic"),
		kgo.SASL(plain.Plain(func(_ context.Context) (plain.Auth, error) {
			return plain.Auth{User: "admin", Pass: "adminpass"}, nil
		})),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := cl.ProduceSync(ctx, kgo.StringRecord("test")).FirstErr(); err != nil {
		t.Fatalf("superuser should bypass ACL checks: %v", err)
	}
}

func TestACLCreateDescribeDelete(t *testing.T) {
	t.Parallel()
	c, err := NewCluster(
		NumBrokers(1),
		EnableACLs(),
		Superuser("PLAIN", "admin", "adminpass"),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.SASL(plain.Plain(func(_ context.Context) (plain.Auth, error) {
			return plain.Auth{User: "admin", Pass: "adminpass"}, nil
		})),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	adm := kadm.NewClient(cl)

	countACLs := func(results kadm.DescribeACLsResults) int {
		var n int
		for _, r := range results {
			n += len(r.Described)
		}
		return n
	}

	// Create ACL
	if _, err := adm.CreateACLs(ctx, kadm.NewACLs().
		ResourcePatternType(kadm.ACLPatternLiteral).
		Topics("my-topic").
		Allow("User:alice").
		AllowHosts("*").
		Operations(kadm.OpRead),
	); err != nil {
		t.Fatalf("CreateACLs failed: %v", err)
	}

	// Describe ACLs
	described, err := adm.DescribeACLs(ctx, kadm.NewACLs().
		ResourcePatternType(kadm.ACLPatternLiteral).
		Topics("my-topic").
		Allow("User:alice").
		AllowHosts("*").
		Operations(kadm.OpRead),
	)
	if err != nil {
		t.Fatalf("DescribeACLs failed: %v", err)
	}
	if countACLs(described) != 1 {
		t.Fatalf("expected 1 ACL, got %d", countACLs(described))
	}

	// Delete ACL
	deleted, err := adm.DeleteACLs(ctx, kadm.NewACLs().
		ResourcePatternType(kadm.ACLPatternLiteral).
		Topics("my-topic").
		Allow("User:alice").
		AllowHosts("*").
		Operations(kadm.OpRead),
	)
	if err != nil {
		t.Fatalf("DeleteACLs failed: %v", err)
	}
	var deletedCount int
	for _, d := range deleted {
		deletedCount += len(d.Deleted)
	}
	if deletedCount != 1 {
		t.Fatalf("expected 1 deleted ACL, got %d", deletedCount)
	}

	// Describe again - should be empty
	described, err = adm.DescribeACLs(ctx, kadm.NewACLs().
		ResourcePatternType(kadm.ACLPatternLiteral).
		Topics("my-topic").
		Allow("User:alice").
		AllowHosts("*").
		Operations(kadm.OpRead),
	)
	if err != nil {
		t.Fatalf("DescribeACLs failed: %v", err)
	}
	if n := countACLs(described); n != 0 {
		t.Fatalf("expected 0 ACLs after delete, got %d", n)
	}
}

func TestACLDenyTakesPrecedence(t *testing.T) {
	t.Parallel()
	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(1, "test-topic"),
		EnableACLs(),
		EnableSASL(),
		User("PLAIN", "testuser", "testpass",
			ACL{Resource: kmsg.ACLResourceTypeTopic, Name: "test-topic", Pattern: kmsg.ACLResourcePatternTypeLiteral, Operation: kmsg.ACLOperationWrite, Allow: true},
			ACL{Resource: kmsg.ACLResourceTypeTopic, Name: "test-topic", Pattern: kmsg.ACLResourcePatternTypeLiteral, Operation: kmsg.ACLOperationDescribe, Allow: true},
			ACL{Resource: kmsg.ACLResourceTypeTopic, Name: "test-topic", Pattern: kmsg.ACLResourcePatternTypeLiteral, Operation: kmsg.ACLOperationWrite, Allow: false}, // DENY
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic("test-topic"),
		kgo.SASL(plain.Plain(func(_ context.Context) (plain.Auth, error) {
			return plain.Auth{User: "testuser", Pass: "testpass"}, nil
		})),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = cl.ProduceSync(ctx, kgo.StringRecord("test")).FirstErr()
	if err == nil {
		t.Fatal("produce should fail when DENY ACL is present")
	}
	if !isAuthzError(err) {
		t.Fatalf("expected authorization error, got: %v", err)
	}
}

func TestACLPrefixPattern(t *testing.T) {
	t.Parallel()
	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(1, "test-topic"),
		SeedTopics(1, "test-topic-2"),
		SeedTopics(1, "other-topic"),
		EnableACLs(),
		EnableSASL(),
		User("PLAIN", "testuser", "testpass",
			ACL{Resource: kmsg.ACLResourceTypeTopic, Name: "test-", Pattern: kmsg.ACLResourcePatternTypePrefixed, Operation: kmsg.ACLOperationWrite, Allow: true},
			ACL{Resource: kmsg.ACLResourceTypeTopic, Name: "test-", Pattern: kmsg.ACLResourcePatternTypePrefixed, Operation: kmsg.ACLOperationDescribe, Allow: true},
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.SASL(plain.Plain(func(_ context.Context) (plain.Auth, error) {
			return plain.Auth{User: "testuser", Pass: "testpass"}, nil
		})),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// test-topic should work (matches prefix)
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "test-topic", Value: []byte("test")}).FirstErr(); err != nil {
		t.Fatalf("produce to test-topic should work with prefix ACL: %v", err)
	}

	// test-topic-2 should work (matches prefix)
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "test-topic-2", Value: []byte("test")}).FirstErr(); err != nil {
		t.Fatalf("produce to test-topic-2 should work with prefix ACL: %v", err)
	}

	// other-topic should fail (doesn't match prefix)
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "other-topic", Value: []byte("test")}).FirstErr(); err == nil {
		t.Fatal("produce to other-topic should fail without ACL")
	}
}

func isAuthzError(err error) bool {
	var kerror *kerr.Error
	if errors.As(err, &kerror) {
		return kerror == kerr.TopicAuthorizationFailed ||
			kerror == kerr.GroupAuthorizationFailed ||
			kerror == kerr.ClusterAuthorizationFailed ||
			kerror == kerr.TransactionalIDAuthorizationFailed
	}
	return false
}
