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
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
)

func TestACLsDisabled(t *testing.T) {
	t.Parallel()
	c := newCluster(t, NumBrokers(1), SeedTopics(1, "test-topic"))

	cl := newPlainClient(t, c,
		kgo.DefaultProduceTopic("test-topic"),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := cl.ProduceSync(ctx, kgo.StringRecord("test")).FirstErr(); err != nil {
		t.Fatalf("produce should work without ACLs: %v", err)
	}

	// The ACL requests answer as a broker with no authorizer does.
	req := kmsg.NewPtrDescribeACLsRequest()
	req.ResourceType = kmsg.ACLResourceTypeAny
	req.ResourcePatternType = kmsg.ACLResourcePatternTypeAny
	req.Operation = kmsg.ACLOperationAny
	req.PermissionType = kmsg.ACLPermissionTypeAny
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp.ErrorCode != kerr.SecurityDisabled.Code {
		t.Fatalf("DescribeACLs without ACLs: got error %d, want SECURITY_DISABLED", resp.ErrorCode)
	}
}

func TestACLsEnabled(t *testing.T) {
	t.Parallel()
	c := newCluster(t,
		NumBrokers(2),
		SeedTopics(1, "test-topic"),
		EnableACLs(),
		EnableSASL(),
		User("PLAIN", "testuser", "testpass"), // no ACLs
		User("PLAIN", "topicadmin", "pass",
			ACL{Resource: kmsg.ACLResourceTypeCluster, Name: "kafka-cluster", Pattern: kmsg.ACLResourcePatternTypeLiteral, Operation: kmsg.ACLOperationCreate, Allow: true},
			ACL{Resource: kmsg.ACLResourceTypeCluster, Name: "kafka-cluster", Pattern: kmsg.ACLResourcePatternTypeLiteral, Operation: kmsg.ACLOperationDelete, Allow: true},
		),
	)

	cl := newPlainClient(t, c,
		kgo.DefaultProduceTopic("test-topic"),
		kgo.SASL(plain.Plain(func(_ context.Context) (plain.Auth, error) {
			return plain.Auth{User: "testuser", Pass: "testpass"}, nil
		})),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := cl.ProduceSync(ctx, kgo.StringRecord("test")).FirstErr()
	if err == nil {
		t.Fatal("produce should fail without WRITE ACL")
	}
	if !isAuthzError(err) {
		t.Fatalf("expected authorization error, got: %v", err)
	}

	// A cluster denial on a reassignment answers every partition too.
	apa := kmsg.NewPtrAlterPartitionAssignmentsRequest()
	apaT := kmsg.NewAlterPartitionAssignmentsRequestTopic()
	apaT.Topic = "test-topic"
	apaP := kmsg.NewAlterPartitionAssignmentsRequestTopicPartition()
	apaP.Replicas = []int32{0}
	apaT.Partitions = append(apaT.Partitions, apaP)
	apa.Topics = append(apa.Topics, apaT)
	apaResp, err := apa.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if code := kerr.ClusterAuthorizationFailed.Code; apaResp.ErrorCode != code || len(apaResp.Topics) != 1 || apaResp.Topics[0].Partitions[0].ErrorCode != code {
		t.Errorf("reassignment denial: expected CLUSTER_AUTHORIZATION_FAILED everywhere, got %+v", apaResp)
	}

	// A topic you may not describe is denied without its ID.
	del := kmsg.NewPtrDeleteTopicsRequest()
	delT := kmsg.NewDeleteTopicsRequestTopic()
	delT.Topic = kmsg.StringPtr("test-topic")
	del.Topics = append(del.Topics, delT)
	delResp, err := del.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if st := delResp.Topics[0]; st.ErrorCode != kerr.TopicAuthorizationFailed.Code || st.TopicID != [16]byte{} {
		t.Errorf("delete denial: expected TOPIC_AUTHORIZATION_FAILED with no ID, got %v %x", kerr.ErrorForCode(st.ErrorCode), st.TopicID)
	}

	// Kafka checks the ACL before routing: a broker that is not the
	// controller answers a denied topic TOPIC_AUTHORIZATION_FAILED.
	var other int32
	c.admin(func() { other = 1 - c.controller.node })
	create := kmsg.NewPtrCreateTopicsRequest()
	create.Topics = []kmsg.CreateTopicsRequestTopic{{Topic: "denied", NumPartitions: 1, ReplicationFactor: 1}}
	grow := kmsg.NewPtrCreatePartitionsRequest()
	grow.Topics = []kmsg.CreatePartitionsRequestTopic{{Topic: "test-topic", Count: 2}}
	for _, req := range []kmsg.Request{create, grow, del} {
		if resp := faultReq(t, cl, other, req); !hasErrorCode(resp, kerr.TopicAuthorizationFailed.Code) {
			t.Errorf("%s to a broker that is not the controller: got %+v, want TOPIC_AUTHORIZATION_FAILED", kmsg.NameForKey(req.Key()), resp)
		}
	}

	// CREATE and DELETE on the cluster create and delete any topic.
	// Without DESCRIBE_CONFIGS on the topic, the create answers no
	// counts or configs.
	admin := newPlainClient(t, c, kgo.SASL(plain.Plain(func(_ context.Context) (plain.Auth, error) {
		return plain.Auth{User: "topicadmin", Pass: "pass"}, nil
	})))
	ct := kmsg.NewPtrCreateTopicsRequest()
	ctT := kmsg.NewCreateTopicsRequestTopic()
	ctT.Topic = "admin-created"
	ctT.NumPartitions, ctT.ReplicationFactor = 1, 1
	ct.Topics = append(ct.Topics, ctT)
	ctResp, err := ct.RequestWith(ctx, admin)
	if err != nil {
		t.Fatal(err)
	}
	if st := ctResp.Topics[0]; st.ErrorCode != 0 || st.ConfigErrorCode != kerr.TopicAuthorizationFailed.Code || st.NumPartitions != -1 || len(st.Configs) != 0 {
		t.Errorf("create: got code %v, config code %v, %d partitions, %d configs", kerr.ErrorForCode(st.ErrorCode), kerr.ErrorForCode(st.ConfigErrorCode), st.NumPartitions, len(st.Configs))
	}
	if delResp, err = del.RequestWith(ctx, admin); err != nil {
		t.Fatal(err)
	}
	if code := delResp.Topics[0].ErrorCode; code != 0 {
		t.Errorf("delete with cluster DELETE: %v", kerr.ErrorForCode(code))
	}
}

func TestACLsWithPermission(t *testing.T) {
	t.Parallel()
	c := newCluster(t,
		NumBrokers(1),
		SeedTopics(1, "test-topic", "t2"),
		EnableACLs(),
		EnableSASL(),
		Superuser("PLAIN", "admin", "admin"),
		User("PLAIN", "cluster", "cluster",
			ACL{Resource: kmsg.ACLResourceTypeCluster, Name: "kafka-cluster", Pattern: kmsg.ACLResourcePatternTypeLiteral, Operation: kmsg.ACLOperationDescribe, Allow: true},
		),
		User("PLAIN", "testuser", "testpass",
			// A CIDR host matches the client's address (Kafka 4.4).
			ACL{Resource: kmsg.ACLResourceTypeTopic, Name: "test-topic", Pattern: kmsg.ACLResourcePatternTypeLiteral, Operation: kmsg.ACLOperationWrite, Allow: true, Host: "127.0.0.0/8"},
			ACL{Resource: kmsg.ACLResourceTypeTopic, Name: "test-topic", Pattern: kmsg.ACLResourcePatternTypeLiteral, Operation: kmsg.ACLOperationDescribe, Allow: true},
			ACL{Resource: kmsg.ACLResourceTypeCluster, Name: "kafka-cluster", Operation: kmsg.ACLOperationIdempotentWrite, Allow: true},
			ACL{Resource: kmsg.ACLResourceTypeGroup, Name: "g", Pattern: kmsg.ACLResourcePatternTypeLiteral, Operation: kmsg.ACLOperationDescribe, Allow: true},
		),
	)

	cl := newPlainClient(t, c,
		kgo.DefaultProduceTopic("test-topic"),
		kgo.SASL(plain.Plain(func(_ context.Context) (plain.Auth, error) {
			return plain.Auth{User: "testuser", Pass: "testpass"}, nil
		})),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := cl.ProduceSync(ctx, kgo.StringRecord("test")).FirstErr(); err != nil {
		t.Fatalf("produce should work with WRITE ACL: %v", err)
	}

	// A principal that cannot describe the cluster sees no cluster
	// operations, even ones it is allowed.
	req := kmsg.NewPtrDescribeClusterRequest()
	req.IncludeClusterAuthorizedOperations = true
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp.ClusterAuthorizedOperations != 0 {
		t.Errorf("cluster authorized operations: got %b, want 0", resp.ClusterAuthorizedOperations)
	}

	sasl := func(user string) *kgo.Client {
		return newPlainClient(t, c, kgo.SASL(plain.Auth{User: user, Pass: user}.AsMechanism()))
	}
	adm := kadm.NewClient(sasl("admin"))
	var offsets kadm.Offsets
	offsets.Add(kadm.Offset{Topic: "test-topic", At: 3})
	offsets.Add(kadm.Offset{Topic: "t2", At: 3})
	for _, g := range []string{"g", "h"} {
		if err := adm.CommitAllOffsets(ctx, g, offsets); err != nil {
			t.Fatal(err)
		}
	}

	// Kafka leaves out the topics you may not describe, and fails them
	// when you name them.
	fetch := kmsg.NewPtrOffsetFetchRequest()
	fetch.Groups = []kmsg.OffsetFetchRequestGroup{{Group: "g", MemberEpoch: -1}}
	if all := faultReq(t, cl, 0, fetch).(*kmsg.OffsetFetchResponse).Groups[0]; len(all.Topics) != 1 || all.Topics[0].TopicID != c.TopicInfo("test-topic").TopicID {
		t.Errorf("fetching all offsets: %+v, want only test-topic", all.Topics)
	}
	fetch.Groups[0].Topics = []kmsg.OffsetFetchRequestGroupTopic{{Topic: "t2", TopicID: c.TopicInfo("t2").TopicID, Partitions: []int32{0}}}
	if p := faultReq(t, cl, 0, fetch).(*kmsg.OffsetFetchResponse).Groups[0].Topics[0].Partitions[0]; p.ErrorCode != kerr.TopicAuthorizationFailed.Code {
		t.Errorf("fetching t2: %v, want TOPIC_AUTHORIZATION_FAILED", kerr.ErrorForCode(p.ErrorCode))
	}

	// Kafka lists every group to a caller that may describe the cluster.
	if n := len(faultReq(t, cl, 0, kmsg.NewPtrListGroupsRequest()).(*kmsg.ListGroupsResponse).Groups); n != 1 {
		t.Errorf("testuser listed %d groups, want 1", n)
	}
	if n := len(faultReq(t, sasl("cluster"), 0, kmsg.NewPtrListGroupsRequest()).(*kmsg.ListGroupsResponse).Groups); n != 2 {
		t.Errorf("a cluster describer listed %d groups, want 2", n)
	}
}

func TestACLsSuperuser(t *testing.T) {
	t.Parallel()
	c := newCluster(t,
		NumBrokers(1),
		SeedTopics(1, "test-topic"),
		EnableACLs(),
		Superuser("PLAIN", "admin", "adminpass"),
	)

	cl := newPlainClient(t, c,
		kgo.DefaultProduceTopic("test-topic"),
		kgo.SASL(plain.Plain(func(_ context.Context) (plain.Auth, error) {
			return plain.Auth{User: "admin", Pass: "adminpass"}, nil
		})),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := cl.ProduceSync(ctx, kgo.StringRecord("test")).FirstErr(); err != nil {
		t.Fatalf("superuser should bypass ACL checks: %v", err)
	}
}

func TestACLCreateDescribeDelete(t *testing.T) {
	t.Parallel()
	c := newCluster(t,
		NumBrokers(1),
		EnableACLs(),
		Superuser("PLAIN", "admin", "adminpass"),
	)

	cl := newPlainClient(t, c,
		kgo.SASL(plain.Plain(func(_ context.Context) (plain.Auth, error) {
			return plain.Auth{User: "admin", Pass: "adminpass"}, nil
		})),
	)

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

	// MATCH describes every ACL that applies to a name: the literal name,
	// the literal wildcard, and every prefix of the name.
	for _, b := range []*kadm.ACLBuilder{
		kadm.NewACLs().ResourcePatternType(kadm.ACLPatternLiteral).Topics("*"),
		kadm.NewACLs().ResourcePatternType(kadm.ACLPatternPrefixed).Topics("my-"),
	} {
		results, err := adm.CreateACLs(ctx, b.Allow("User:alice").AllowHosts("*").Operations(kadm.OpRead))
		if err == nil {
			err = results[0].Err
		}
		if err != nil {
			t.Fatalf("CreateACLs failed: %v", err)
		}
	}
	described, err = adm.DescribeACLs(ctx, kadm.NewACLs().
		ResourcePatternType(kadm.ACLPatternMatch).
		Topics("my-topic").
		Allow("User:alice").
		AllowHosts("*").
		Operations(kadm.OpRead),
	)
	if err != nil {
		t.Fatalf("DescribeACLs failed: %v", err)
	}
	if n := countACLs(described); n != 3 {
		t.Fatalf("MATCH my-topic: expected 3 ACLs, got %d", n)
	}

	// Every filter in one delete sees the ACLs from before the request,
	// so an ACL two filters match is listed under both.
	dreq := kmsg.NewPtrDeleteACLsRequest()
	for _, pattern := range []kmsg.ACLResourcePatternType{kmsg.ACLResourcePatternTypeLiteral, kmsg.ACLResourcePatternTypeAny} {
		f := kmsg.NewDeleteACLsRequestFilter()
		f.ResourceType = kmsg.ACLResourceTypeTopic
		f.ResourceName = kmsg.StringPtr("*")
		f.ResourcePatternType = pattern
		f.Operation = kmsg.ACLOperationAny
		f.PermissionType = kmsg.ACLPermissionTypeAny
		dreq.Filters = append(dreq.Filters, f)
	}
	dresp, err := dreq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range dresp.Results {
		if r.ErrorCode != 0 || len(r.MatchingACLs) != 1 {
			t.Errorf("delete filter %d: got error %d with %d ACLs, want 1 ACL", i, r.ErrorCode, len(r.MatchingACLs))
		}
	}

	// Kafka refuses an ACL it cannot hold and accepts every concrete
	// resource type and operation.
	for _, tc := range []struct {
		name    string
		modify  func(*kmsg.CreateACLsRequestCreation)
		wantErr *kerr.Error
	}{
		{"empty name", func(cr *kmsg.CreateACLsRequestCreation) { cr.ResourceName = "" }, kerr.InvalidRequest},
		{"principal without a type", func(cr *kmsg.CreateACLsRequestCreation) { cr.Principal = "alice" }, kerr.InvalidRequest},
		{"empty host", func(cr *kmsg.CreateACLsRequestCreation) { cr.Host = "" }, kerr.InvalidRequest},
		{"bad CIDR", func(cr *kmsg.CreateACLsRequestCreation) { cr.Host = "10.0.0.0/33" }, kerr.InvalidRequest},
		{"cluster not named kafka-cluster", func(cr *kmsg.CreateACLsRequestCreation) {
			cr.ResourceType, cr.ResourceName = kmsg.ACLResourceTypeCluster, "*"
		}, kerr.InvalidRequest},
		{"user resource", func(cr *kmsg.CreateACLsRequestCreation) {
			cr.ResourceType, cr.ResourceName, cr.Operation = kmsg.ACLResourceTypeUser, "User:bob", kmsg.ACLOperationCreateTokens
		}, nil},
	} {
		creq := kmsg.NewPtrCreateACLsRequest()
		cr := kmsg.NewCreateACLsRequestCreation()
		cr.ResourceType = kmsg.ACLResourceTypeTopic
		cr.ResourceName = "v"
		cr.ResourcePatternType = kmsg.ACLResourcePatternTypeLiteral
		cr.Principal = "User:alice"
		cr.Host = "*"
		cr.Operation = kmsg.ACLOperationRead
		cr.PermissionType = kmsg.ACLPermissionTypeAllow
		tc.modify(&cr)
		creq.Creations = append(creq.Creations, cr)
		cresp, err := creq.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		var want int16
		if tc.wantErr != nil {
			want = tc.wantErr.Code
		}
		if got := cresp.Results[0].ErrorCode; got != want {
			t.Errorf("%s: got error %d, want %d", tc.name, got, want)
		}
	}

	// A timed-out creation or deletion keeps its answer, even for an
	// entry that is invalid.
	c.Fault(Fault{Keys: []kmsg.Key{kmsg.CreateACLs, kmsg.DeleteACLs}, Err: kerr.RequestTimedOut, Count: 2})
	unnamed := kmsg.NewPtrCreateACLsRequest()
	unnamed.Creations = []kmsg.CreateACLsRequestCreation{{ResourceType: kmsg.ACLResourceTypeTopic, ResourcePatternType: kmsg.ACLResourcePatternTypeLiteral, Principal: "User:alice", Host: "*", Operation: kmsg.ACLOperationRead, PermissionType: kmsg.ACLPermissionTypeAllow}}
	unknown := kmsg.NewPtrDeleteACLsRequest()
	unknown.Filters = []kmsg.DeleteACLsRequestFilter{{}}
	cr, dr := faultReq(t, cl, 0, unnamed).(*kmsg.CreateACLsResponse), faultReq(t, cl, 0, unknown).(*kmsg.DeleteACLsResponse)
	if cr.Results[0].ErrorCode != kerr.RequestTimedOut.Code || dr.Results[0].ErrorCode != kerr.RequestTimedOut.Code {
		t.Errorf("timed-out invalid creation answered %d, deletion %d; want REQUEST_TIMED_OUT", cr.Results[0].ErrorCode, dr.Results[0].ErrorCode)
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
	c := newCluster(t,
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

	cl := newPlainClient(t, c,
		kgo.DefaultProduceTopic("test-topic"),
		kgo.SASL(plain.Plain(func(_ context.Context) (plain.Auth, error) {
			return plain.Auth{User: "testuser", Pass: "testpass"}, nil
		})),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := cl.ProduceSync(ctx, kgo.StringRecord("test")).FirstErr()
	if err == nil {
		t.Fatal("produce should fail when DENY ACL is present")
	}
	if !isAuthzError(err) {
		t.Fatalf("expected authorization error, got: %v", err)
	}
}

func TestACLPrefixPattern(t *testing.T) {
	t.Parallel()
	c := newCluster(t,
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

	cl := newPlainClient(t, c,
		kgo.SASL(plain.Plain(func(_ context.Context) (plain.Auth, error) {
			return plain.Auth{User: "testuser", Pass: "testpass"}, nil
		})),
	)

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
	if kerror, ok := errors.AsType[*kerr.Error](err); ok {
		return kerror == kerr.TopicAuthorizationFailed ||
			kerror == kerr.GroupAuthorizationFailed ||
			kerror == kerr.ClusterAuthorizationFailed ||
			kerror == kerr.TransactionalIDAuthorizationFailed
	}
	return false
}

// A failed authentication answers SASL_AUTHENTICATION_FAILED before the
// broker closes the connection, so the client fails at once instead of
// retrying a closed connection.
func TestSASLAuthenticateFailure(t *testing.T) {
	t.Parallel()
	c := newCluster(t,
		NumBrokers(1),
		EnableSASL(),
		User("PLAIN", "testuser", "testpass"),
		Superuser("PLAIN", "admin", "adminpass"),
		BrokerConfigs(map[string]string{"connections.max.reauth.ms": "60000"}),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// A credential upserted with more than the default 4096 iterations.
	admin := newPlainClient(t, c, kgo.SASL(plain.Auth{User: "admin", Pass: "adminpass"}.AsMechanism()))
	altered, err := kadm.NewClient(admin).AlterUserSCRAMs(ctx, nil, []kadm.UpsertSCRAM{{
		User: "sc", Mechanism: kadm.ScramSha256, Iterations: 8192, Password: "scpass",
	}})
	if err == nil {
		err = altered["sc"].Err
	}
	if err != nil {
		t.Fatalf("upserting the SCRAM user: %v", err)
	}

	for _, tc := range []struct {
		name    string
		mech    sasl.Mechanism
		wantErr error
	}{
		{"wrong PLAIN password", plain.Auth{User: "testuser", Pass: "wrong"}.AsMechanism(), kerr.SaslAuthenticationFailed},
		{"unknown PLAIN user with an empty password", emptyPlainPassword("ghost"), kerr.SaslAuthenticationFailed},
		{"unknown SCRAM user", scram.Auth{User: "ghost", Pass: "x"}.AsSha256Mechanism(), kerr.SaslAuthenticationFailed},
		{"SCRAM user with 8192 iterations", scram.Auth{User: "sc", Pass: "scpass"}.AsSha256Mechanism(), nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cl := newPlainClient(t, c, kgo.SASL(tc.mech))
			if err := cl.Ping(ctx); !errors.Is(err, tc.wantErr) {
				t.Errorf("got %v, want %v", err, tc.wantErr)
			}
		})
	}

	// Kafka answers a re-authentication that changes the mechanism as if
	// it took it, then closes the connection on the next request.
	cc := &clientConn{c: c}
	var codes [3]int16
	c.admin(func() {
		r0, _ := c.handleSASLHandshake(&clientReq{cc: cc, kreq: &kmsg.SASLHandshakeRequest{Version: 1, Mechanism: "PLAIN"}})
		r1, _ := c.handleSASLAuthenticate(&clientReq{cc: cc, kreq: &kmsg.SASLAuthenticateRequest{Version: 2, SASLAuthBytes: []byte("\x00testuser\x00testpass")}})
		r2, _ := c.handleSASLHandshake(&clientReq{cc: cc, kreq: &kmsg.SASLHandshakeRequest{Version: 1, Mechanism: "SCRAM-SHA-256"}})
		codes = [3]int16{r0.(*kmsg.SASLHandshakeResponse).ErrorCode, r1.(*kmsg.SASLAuthenticateResponse).ErrorCode, r2.(*kmsg.SASLHandshakeResponse).ErrorCode}
	})
	if codes != [3]int16{} || c.handleSASL(&clientReq{cc: cc, kreq: kmsg.NewPtrSASLAuthenticateRequest()}) {
		t.Errorf("re-authenticating with another mechanism answered %v and let the next request through", codes)
	}
}

// emptyPlainPassword sends PLAIN with an empty password, which the plain
// package refuses to send.
type emptyPlainPassword string

func (emptyPlainPassword) Name() string { return "PLAIN" }

func (u emptyPlainPassword) Authenticate(context.Context, string) (sasl.Session, []byte, error) {
	return emptyPlainPassword(""), []byte("\x00" + string(u) + "\x00"), nil
}

func (emptyPlainPassword) Challenge([]byte) (bool, []byte, error) { return true, nil, nil }
