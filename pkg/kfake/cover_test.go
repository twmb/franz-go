package kfake

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func newCoverCluster(t *testing.T, opts ...Opt) *Cluster {
	t.Helper()
	opts = append([]Opt{BrokerConfigs(map[string]string{
		"group.consumer.heartbeat.interval.ms": "100",
	})}, opts...)
	c, err := NewCluster(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func newCoverClient(t *testing.T, c *Cluster, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	opts = append([]kgo.Opt{kgo.SeedBrokers(c.ListenAddrs()...)}, opts...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// TestCreatePartitionsAddsPartitions verifies that CreatePartitions actually
// increases the partition count and that the new partitions are usable.
func TestCreatePartitionsAddsPartitions(t *testing.T) {
	t.Parallel()
	topic := "create-parts-adds"
	c := newCoverCluster(t, NumBrokers(2), SeedTopics(2, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cl := newCoverClient(t, c)
	adm := kadm.NewClient(cl)

	resp, err := adm.CreatePartitions(ctx, 3, topic)
	if err != nil {
		t.Fatal(err)
	}
	if r := resp[topic]; r.Err != nil {
		t.Fatal(r.Err)
	}

	listed, err := adm.ListTopics(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(listed[topic].Partitions); n != 5 {
		t.Fatalf("expected 5 partitions after adding 3, got %d", n)
	}

	// Verify via the cluster API that all 5 partitions have leaders.
	for p := range int32(5) {
		if c.LeaderFor(topic, p) < 0 {
			t.Errorf("partition %d has no leader", p)
		}
	}
}

// TestCreatePartitionsWithExplicitAssignment verifies that CreatePartitions
// with explicit replica assignment creates the partitions with the specified replicas.
func TestCreatePartitionsWithExplicitAssignment(t *testing.T) {
	t.Parallel()
	topic := "create-parts-assign"
	c := newCoverCluster(t, NumBrokers(1), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cl := newCoverClient(t, c)

	req := kmsg.NewCreatePartitionsRequest()
	rt := kmsg.NewCreatePartitionsRequestTopic()
	rt.Topic = topic
	rt.Count = 3 // 1 existing + 2 new

	for range 2 {
		a := kmsg.NewCreatePartitionsRequestTopicAssignment()
		a.Replicas = []int32{0}
		rt.Assignment = append(rt.Assignment, a)
	}
	req.Topics = append(req.Topics, rt)

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Topics[0].ErrorCode != 0 {
		t.Fatal(kerr.ErrorForCode(resp.Topics[0].ErrorCode))
	}

	// Verify both new partitions exist and have node 0 as leader.
	for _, p := range []int32{1, 2} {
		leader := c.LeaderFor(topic, p)
		if leader != 0 {
			t.Errorf("partition %d: expected leader 0, got %d", p, leader)
		}
	}
}

// TestCreatePartitionsErrorCases tests that error conditions are properly rejected.
func TestCreatePartitionsErrorCases(t *testing.T) {
	t.Parallel()
	topic := "create-parts-errors"
	c := newCoverCluster(t, NumBrokers(1), SeedTopics(5, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cl := newCoverClient(t, c)

	cases := []struct {
		name    string
		build   func() kmsg.CreatePartitionsRequest
		wantErr int16
	}{
		{
			name: "decrease partition count",
			build: func() kmsg.CreatePartitionsRequest {
				req := kmsg.NewCreatePartitionsRequest()
				rt := kmsg.NewCreatePartitionsRequestTopic()
				rt.Topic = topic
				rt.Count = 2 // less than current 5
				req.Topics = append(req.Topics, rt)
				return req
			},
			wantErr: kerr.InvalidPartitions.Code,
		},
		{
			name: "unknown topic",
			build: func() kmsg.CreatePartitionsRequest {
				req := kmsg.NewCreatePartitionsRequest()
				rt := kmsg.NewCreatePartitionsRequestTopic()
				rt.Topic = "nonexistent"
				rt.Count = 3
				req.Topics = append(req.Topics, rt)
				return req
			},
			wantErr: kerr.UnknownTopicOrPartition.Code,
		},
		{
			name: "wrong number of assignments",
			build: func() kmsg.CreatePartitionsRequest {
				req := kmsg.NewCreatePartitionsRequest()
				rt := kmsg.NewCreatePartitionsRequestTopic()
				rt.Topic = topic
				rt.Count = 7 // need 2 assignments
				a := kmsg.NewCreatePartitionsRequestTopicAssignment()
				a.Replicas = []int32{0}
				rt.Assignment = append(rt.Assignment, a) // only 1
				req.Topics = append(req.Topics, rt)
				return req
			},
			wantErr: kerr.InvalidReplicaAssignment.Code,
		},
		{
			name: "assignment with unknown broker",
			build: func() kmsg.CreatePartitionsRequest {
				req := kmsg.NewCreatePartitionsRequest()
				rt := kmsg.NewCreatePartitionsRequestTopic()
				rt.Topic = topic
				rt.Count = 6
				a := kmsg.NewCreatePartitionsRequestTopicAssignment()
				a.Replicas = []int32{999}
				rt.Assignment = append(rt.Assignment, a)
				req.Topics = append(req.Topics, rt)
				return req
			},
			wantErr: kerr.InvalidReplicaAssignment.Code,
		},
		{
			name: "assignment with empty replicas",
			build: func() kmsg.CreatePartitionsRequest {
				req := kmsg.NewCreatePartitionsRequest()
				rt := kmsg.NewCreatePartitionsRequestTopic()
				rt.Topic = topic
				rt.Count = 6
				a := kmsg.NewCreatePartitionsRequestTopicAssignment()
				rt.Assignment = append(rt.Assignment, a)
				req.Topics = append(req.Topics, rt)
				return req
			},
			wantErr: kerr.InvalidReplicaAssignment.Code,
		},
		{
			name: "duplicate topics in request",
			build: func() kmsg.CreatePartitionsRequest {
				req := kmsg.NewCreatePartitionsRequest()
				for range 2 {
					rt := kmsg.NewCreatePartitionsRequestTopic()
					rt.Topic = topic
					rt.Count = 6
					req.Topics = append(req.Topics, rt)
				}
				return req
			},
			wantErr: kerr.InvalidRequest.Code,
		},
	}

	for _, tc := range cases {
		req := tc.build()
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if resp.Topics[0].ErrorCode != tc.wantErr {
			t.Errorf("%s: got error %d (%v), want %d (%v)",
				tc.name, resp.Topics[0].ErrorCode, kerr.ErrorForCode(resp.Topics[0].ErrorCode),
				tc.wantErr, kerr.ErrorForCode(tc.wantErr))
		}
	}
}

// TestCreatePartitionsValidateOnly verifies that ValidateOnly returns success
// without actually creating partitions.
func TestCreatePartitionsValidateOnly(t *testing.T) {
	t.Parallel()
	topic := "create-parts-validate"
	c := newCoverCluster(t, NumBrokers(1), SeedTopics(2, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cl := newCoverClient(t, c)

	req := kmsg.NewCreatePartitionsRequest()
	req.ValidateOnly = true
	rt := kmsg.NewCreatePartitionsRequestTopic()
	rt.Topic = topic
	rt.Count = 10
	req.Topics = append(req.Topics, rt)

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Topics[0].ErrorCode != 0 {
		t.Fatal(kerr.ErrorForCode(resp.Topics[0].ErrorCode))
	}

	// Partition count should be unchanged.
	adm := kadm.NewClient(cl)
	listed, err := adm.ListTopics(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if n := len(listed[topic].Partitions); n != 2 {
		t.Fatalf("validate-only changed partition count from 2 to %d", n)
	}
}

// TestDescribeLogDirsReportsStorageSize verifies that DescribeLogDirs returns
// non-zero sizes after producing data, and that it can filter by partition.
func TestDescribeLogDirsReportsStorageSize(t *testing.T) {
	t.Parallel()
	topic := "describe-log-dirs-size"
	c := newCoverCluster(t, NumBrokers(1), SeedTopics(2, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cl := newCoverClient(t, c, kgo.DefaultProduceTopic(topic))
	for range 10 {
		if err := cl.ProduceSync(ctx, kgo.StringRecord("data")).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	// Describe all partitions (nil = all).
	req := kmsg.NewDescribeLogDirsRequest()
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if len(resp.Dirs) == 0 {
		t.Fatal("no dirs returned")
	}

	var totalSize int64
	var partCount int
	for _, dir := range resp.Dirs {
		for _, dt := range dir.Topics {
			if dt.Topic == topic {
				for _, dp := range dt.Partitions {
					totalSize += dp.Size
					partCount++
				}
			}
		}
	}
	if partCount != 2 {
		t.Errorf("expected 2 partitions, got %d", partCount)
	}
	if totalSize == 0 {
		t.Error("expected non-zero total size after producing")
	}

	// Describe only partition 0.
	req2 := kmsg.NewDescribeLogDirsRequest()
	rt := kmsg.NewDescribeLogDirsRequestTopic()
	rt.Topic = topic
	rt.Partitions = []int32{0}
	req2.Topics = append(req2.Topics, rt)

	resp2, err := req2.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	partCount = 0
	for _, dir := range resp2.Dirs {
		for _, dt := range dir.Topics {
			if dt.Topic == topic {
				partCount += len(dt.Partitions)
			}
		}
	}
	if partCount != 1 {
		t.Errorf("expected 1 partition with filter, got %d", partCount)
	}
}

// TestAlterReplicaLogDirsMovesPartition verifies that AlterReplicaLogDirs
// changes the directory for a partition, confirmed via DescribeLogDirs.
func TestAlterReplicaLogDirsMovesPartition(t *testing.T) {
	t.Parallel()
	topic := "alter-log-dirs-move"
	c := newCoverCluster(t, NumBrokers(1), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cl := newCoverClient(t, c)
	newDir := "/data/kafka-logs-moved"

	// Move partition 0 to a new directory.
	req := kmsg.NewAlterReplicaLogDirsRequest()
	rd := kmsg.NewAlterReplicaLogDirsRequestDir()
	rd.Dir = newDir
	rt := kmsg.NewAlterReplicaLogDirsRequestDirTopic()
	rt.Topic = topic
	rt.Partitions = []int32{0}
	rd.Topics = append(rd.Topics, rt)
	req.Dirs = append(req.Dirs, rd)

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	for _, st := range resp.Topics {
		for _, sp := range st.Partitions {
			if sp.ErrorCode != 0 {
				t.Fatalf("alter error: %v", kerr.ErrorForCode(sp.ErrorCode))
			}
		}
	}

	// Verify via DescribeLogDirs that the partition is now in the new dir.
	dlReq := kmsg.NewDescribeLogDirsRequest()
	dlrt := kmsg.NewDescribeLogDirsRequestTopic()
	dlrt.Topic = topic
	dlrt.Partitions = []int32{0}
	dlReq.Topics = append(dlReq.Topics, dlrt)

	dlResp, err := dlReq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	foundInNewDir := false
	for _, dir := range dlResp.Dirs {
		if dir.Dir == newDir {
			for _, dt := range dir.Topics {
				if dt.Topic == topic {
					foundInNewDir = true
				}
			}
		}
	}
	if !foundInNewDir {
		t.Error("partition not found in new directory after AlterReplicaLogDirs")
	}
}

// TestAlterPartitionAssignments verifies that reassignment accepts valid
// requests and properly rejects null-replicas (cancel) and unknown topics.
func TestAlterPartitionAssignments(t *testing.T) {
	t.Parallel()
	topic := "alter-assign"
	c := newCoverCluster(t, NumBrokers(1), SeedTopics(2, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cl := newCoverClient(t, c)

	// Successful reassignment (non-nil replicas accepted).
	req := kmsg.NewAlterPartitionAssignmentsRequest()
	rt := kmsg.NewAlterPartitionAssignmentsRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewAlterPartitionAssignmentsRequestTopicPartition()
	rp.Partition = 0
	rp.Replicas = []int32{0}
	rt.Partitions = append(rt.Partitions, rp)

	// Cancel reassignment (nil replicas) — kfake has none in progress.
	rpCancel := kmsg.NewAlterPartitionAssignmentsRequestTopicPartition()
	rpCancel.Partition = 1
	rpCancel.Replicas = nil
	rt.Partitions = append(rt.Partitions, rpCancel)
	req.Topics = append(req.Topics, rt)

	// Unknown topic.
	rtUnknown := kmsg.NewAlterPartitionAssignmentsRequestTopic()
	rtUnknown.Topic = "no-such-topic"
	rpUnknown := kmsg.NewAlterPartitionAssignmentsRequestTopicPartition()
	rpUnknown.Partition = 0
	rpUnknown.Replicas = []int32{0}
	rtUnknown.Partitions = append(rtUnknown.Partitions, rpUnknown)
	req.Topics = append(req.Topics, rtUnknown)

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}

	// Check per-partition results.
	for _, st := range resp.Topics {
		for _, sp := range st.Partitions {
			switch {
			case st.Topic == topic && sp.Partition == 0:
				if sp.ErrorCode != 0 {
					t.Errorf("partition 0 should succeed, got %v", kerr.ErrorForCode(sp.ErrorCode))
				}
			case st.Topic == topic && sp.Partition == 1:
				if sp.ErrorCode != kerr.NoReassignmentInProgress.Code {
					t.Errorf("cancel should return NoReassignmentInProgress, got %v", kerr.ErrorForCode(sp.ErrorCode))
				}
			case st.Topic == "no-such-topic":
				if sp.ErrorCode != kerr.UnknownTopicOrPartition.Code {
					t.Errorf("unknown topic should return UnknownTopicOrPartition, got %v", kerr.ErrorForCode(sp.ErrorCode))
				}
			}
		}
	}
}

// TestAddRemoveNodeUpdatesCluster verifies that AddNode/RemoveNode actually
// changes the cluster topology and that new nodes are reachable.
func TestAddRemoveNodeUpdatesCluster(t *testing.T) {
	t.Parallel()
	c := newCoverCluster(t, NumBrokers(1), SeedTopics(1, "node-ops"))

	// Add an auto-ID node.
	nodeID, _, err := c.AddNode(-1, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(c.ListenAddrs()) != 2 {
		t.Fatalf("expected 2 brokers, got %d", len(c.ListenAddrs()))
	}

	// Verify we can produce — the new broker should appear in metadata.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cl := newCoverClient(t, c)
	adm := kadm.NewClient(cl)
	meta, err := adm.Metadata(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(meta.Brokers) != 2 {
		t.Errorf("metadata shows %d brokers, want 2", len(meta.Brokers))
	}

	// Add a specific-ID node.
	nodeID2, _, err := c.AddNode(42, 0)
	if err != nil {
		t.Fatal(err)
	}
	if nodeID2 != 42 {
		t.Errorf("expected node ID 42, got %d", nodeID2)
	}

	// Duplicate node ID should fail.
	_, _, err = c.AddNode(42, 0)
	if err == nil {
		t.Fatal("expected error adding duplicate node")
	}

	// Remove both added nodes.
	if err := c.RemoveNode(nodeID); err != nil {
		t.Fatal(err)
	}
	if err := c.RemoveNode(42); err != nil {
		t.Fatal(err)
	}
	if len(c.ListenAddrs()) != 1 {
		t.Fatalf("expected 1 broker after removals, got %d", len(c.ListenAddrs()))
	}

	// Can't remove nonexistent node.
	if err := c.RemoveNode(999); err == nil {
		t.Fatal("expected error removing nonexistent node")
	}
}

// TestMoveTopicPartitionChangesLeader verifies MoveTopicPartition actually
// changes the leader, verified through both the API and produce routing.
func TestMoveTopicPartitionChangesLeader(t *testing.T) {
	t.Parallel()
	topic := "move-partition"
	c := newCoverCluster(t, NumBrokers(2), SeedTopics(1, topic))

	origLeader := c.LeaderFor(topic, 0)
	target := 1 - origLeader // the other node

	if err := c.MoveTopicPartition(topic, 0, target); err != nil {
		t.Fatal(err)
	}
	if got := c.LeaderFor(topic, 0); got != target {
		t.Errorf("leader should be %d, got %d", target, got)
	}

	// Verify it's usable: produce to the partition.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cl := newCoverClient(t, c, kgo.DefaultProduceTopic(topic))
	if err := cl.ProduceSync(ctx, kgo.StringRecord("after-move")).FirstErr(); err != nil {
		t.Fatalf("failed to produce after leader move: %v", err)
	}

	// Error cases.
	if err := c.MoveTopicPartition(topic, 0, 999); err == nil {
		t.Error("expected error moving to nonexistent node")
	}
	if err := c.MoveTopicPartition(topic, 999, 0); err == nil {
		t.Error("expected error moving nonexistent partition")
	}
	if c.LeaderFor("nonexistent", 0) != -1 {
		t.Error("LeaderFor nonexistent topic should return -1")
	}
}

// TestSCRAMCredentialLifecycle verifies the full SCRAM credential lifecycle:
// create → describe → update → describe → delete → describe (not found).
func TestSCRAMCredentialLifecycle(t *testing.T) {
	t.Parallel()
	c := newCoverCluster(t, NumBrokers(1))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cl := newCoverClient(t, c)

	upsert := func(name string, mech int8, iterations int32) {
		t.Helper()
		req := kmsg.NewAlterUserSCRAMCredentialsRequest()
		u := kmsg.NewAlterUserSCRAMCredentialsRequestUpsertion()
		u.Name = name
		u.Mechanism = mech
		u.Iterations = iterations
		u.Salt = []byte("test-salt-value!")
		u.SaltedPassword = []byte("test-salted-password-value!12345")
		req.Upsertions = append(req.Upsertions, u)
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		if resp.Results[0].ErrorCode != 0 {
			t.Fatalf("upsert %s: %v", name, kerr.ErrorForCode(resp.Results[0].ErrorCode))
		}
	}

	describe := func(names ...string) *kmsg.DescribeUserSCRAMCredentialsResponse {
		t.Helper()
		req := kmsg.NewDescribeUserSCRAMCredentialsRequest()
		for _, n := range names {
			u := kmsg.NewDescribeUserSCRAMCredentialsRequestUser()
			u.Name = n
			req.Users = append(req.Users, u)
		}
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		return resp
	}

	// Create credentials for two users with different mechanisms.
	upsert("alice", 1, 4096) // SCRAM-SHA-256
	upsert("bob", 2, 8192)   // SCRAM-SHA-512

	// Describe both — verify mechanism and iterations.
	resp := describe("alice", "bob")
	for _, r := range resp.Results {
		if r.ErrorCode != 0 {
			t.Fatalf("describe %s: %v", r.User, kerr.ErrorForCode(r.ErrorCode))
		}
		if len(r.CredentialInfos) != 1 {
			t.Fatalf("%s: expected 1 credential, got %d", r.User, len(r.CredentialInfos))
		}
		ci := r.CredentialInfos[0]
		switch r.User {
		case "alice":
			if ci.Mechanism != 1 || ci.Iterations != 4096 {
				t.Errorf("alice: mechanism=%d iterations=%d", ci.Mechanism, ci.Iterations)
			}
		case "bob":
			if ci.Mechanism != 2 || ci.Iterations != 8192 {
				t.Errorf("bob: mechanism=%d iterations=%d", ci.Mechanism, ci.Iterations)
			}
		}
	}

	// Describe all (nil users) — should find both.
	respAll := describe()
	if len(respAll.Results) < 2 {
		t.Errorf("describe-all: expected >=2 results, got %d", len(respAll.Results))
	}

	// Update alice's iterations.
	upsert("alice", 1, 8192)
	resp2 := describe("alice")
	if resp2.Results[0].CredentialInfos[0].Iterations != 8192 {
		t.Error("alice iterations should be updated to 8192")
	}

	// Delete alice.
	delReq := kmsg.NewAlterUserSCRAMCredentialsRequest()
	d := kmsg.NewAlterUserSCRAMCredentialsRequestDeletion()
	d.Name = "alice"
	d.Mechanism = 1
	delReq.Deletions = append(delReq.Deletions, d)
	delResp, err := delReq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if delResp.Results[0].ErrorCode != 0 {
		t.Fatal(kerr.ErrorForCode(delResp.Results[0].ErrorCode))
	}

	// Describe alice after delete → ResourceNotFound.
	resp3 := describe("alice")
	if resp3.Results[0].ErrorCode != kerr.ResourceNotFound.Code {
		t.Errorf("alice after delete: expected ResourceNotFound, got %v",
			kerr.ErrorForCode(resp3.Results[0].ErrorCode))
	}
}

// TestSCRAMValidationErrors verifies that invalid SCRAM operations are rejected
// with appropriate error codes.
func TestSCRAMValidationErrors(t *testing.T) {
	t.Parallel()
	c := newCoverCluster(t, NumBrokers(1))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cl := newCoverClient(t, c)

	upsertAndExpect := func(name string, mech int8, iter int32, wantErr int16) {
		t.Helper()
		req := kmsg.NewAlterUserSCRAMCredentialsRequest()
		u := kmsg.NewAlterUserSCRAMCredentialsRequestUpsertion()
		u.Name = name
		u.Mechanism = mech
		u.Iterations = iter
		u.Salt = []byte("s")
		u.SaltedPassword = []byte("p")
		req.Upsertions = append(req.Upsertions, u)
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		if resp.Results[0].ErrorCode != wantErr {
			t.Errorf("upsert(name=%q,mech=%d,iter=%d): got %v, want %v",
				name, mech, iter,
				kerr.ErrorForCode(resp.Results[0].ErrorCode),
				kerr.ErrorForCode(wantErr))
		}
	}

	upsertAndExpect("", 1, 4096, kerr.UnacceptableCredential.Code)     // empty name
	upsertAndExpect("u", 99, 4096, kerr.UnsupportedSaslMechanism.Code) // bad mechanism
	upsertAndExpect("u", 1, 100, kerr.UnacceptableCredential.Code)     // iterations too low
	upsertAndExpect("u", 1, 20000, kerr.UnacceptableCredential.Code)   // iterations too high

	// Delete non-existent credential.
	req := kmsg.NewAlterUserSCRAMCredentialsRequest()
	d := kmsg.NewAlterUserSCRAMCredentialsRequestDeletion()
	d.Name = "ghost"
	d.Mechanism = 1
	req.Deletions = append(req.Deletions, d)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Results[0].ErrorCode != kerr.ResourceNotFound.Code {
		t.Errorf("delete nonexistent: got %v, want ResourceNotFound",
			kerr.ErrorForCode(resp.Results[0].ErrorCode))
	}

	// Delete with invalid mechanism.
	req2 := kmsg.NewAlterUserSCRAMCredentialsRequest()
	d2 := kmsg.NewAlterUserSCRAMCredentialsRequestDeletion()
	d2.Name = "ghost"
	d2.Mechanism = 99
	req2.Deletions = append(req2.Deletions, d2)
	resp2, err := req2.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp2.Results[0].ErrorCode != kerr.UnsupportedSaslMechanism.Code {
		t.Errorf("delete bad mech: got %v", kerr.ErrorForCode(resp2.Results[0].ErrorCode))
	}

	// Delete with empty name.
	req3 := kmsg.NewAlterUserSCRAMCredentialsRequest()
	d3 := kmsg.NewAlterUserSCRAMCredentialsRequestDeletion()
	d3.Name = ""
	d3.Mechanism = 1
	req3.Deletions = append(req3.Deletions, d3)
	resp3, err := req3.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp3.Results[0].ErrorCode != kerr.UnacceptableCredential.Code {
		t.Errorf("delete empty name: got %v", kerr.ErrorForCode(resp3.Results[0].ErrorCode))
	}

	// Duplicate user in same request (delete + upsert same user).
	req4 := kmsg.NewAlterUserSCRAMCredentialsRequest()
	d4 := kmsg.NewAlterUserSCRAMCredentialsRequestDeletion()
	d4.Name = "dupuser"
	d4.Mechanism = 1
	req4.Deletions = append(req4.Deletions, d4)
	u4 := kmsg.NewAlterUserSCRAMCredentialsRequestUpsertion()
	u4.Name = "dupuser"
	u4.Mechanism = 1
	u4.Iterations = 4096
	u4.Salt = []byte("s")
	u4.SaltedPassword = []byte("p")
	req4.Upsertions = append(req4.Upsertions, u4)
	resp4, err := req4.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	foundDup := false
	for _, r := range resp4.Results {
		if r.ErrorCode == kerr.DuplicateResource.Code {
			foundDup = true
		}
	}
	if !foundDup {
		t.Error("expected DuplicateResource for delete+upsert same user")
	}

	// Describe with duplicate user name.
	req5 := kmsg.NewDescribeUserSCRAMCredentialsRequest()
	for range 2 {
		u := kmsg.NewDescribeUserSCRAMCredentialsRequestUser()
		u.Name = "someone"
		req5.Users = append(req5.Users, u)
	}
	resp5, err := req5.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	foundDup = false
	for _, r := range resp5.Results {
		if r.ErrorCode == kerr.DuplicateResource.Code {
			foundDup = true
		}
	}
	if !foundDup {
		t.Error("expected DuplicateResource for describe with duplicate name")
	}
}

// TestAlterConfigsAppliesAndReadsBack verifies that AlterConfigs changes are
// visible through DescribeConfigs.
func TestAlterConfigsAppliesAndReadsBack(t *testing.T) {
	t.Parallel()
	topic := "alter-configs-readback"
	c := newCoverCluster(t, NumBrokers(1), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cl := newCoverClient(t, c)
	adm := kadm.NewClient(cl)

	// Set broker config via AlterConfigs.
	req := kmsg.NewAlterConfigsRequest()
	rr := kmsg.NewAlterConfigsRequestResource()
	rr.ResourceType = kmsg.ConfigResourceTypeBroker
	rc := kmsg.NewAlterConfigsRequestResourceConfig()
	rc.Name = "log.retention.ms"
	v := "3600000"
	rc.Value = &v
	rr.Configs = append(rr.Configs, rc)
	req.Resources = append(req.Resources, rr)

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Resources[0].ErrorCode != 0 {
		t.Fatal(kerr.ErrorForCode(resp.Resources[0].ErrorCode))
	}

	// Read back via DescribeConfigs.
	bcfgs, err := adm.DescribeBrokerConfigs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, rc := range bcfgs {
		for _, c := range rc.Configs {
			if c.Key == "log.retention.ms" && c.Value != nil && *c.Value == "3600000" {
				found = true
			}
		}
	}
	if !found {
		t.Error("log.retention.ms=3600000 not found in DescribeBrokerConfigs after AlterConfigs")
	}

	// Set topic config.
	req2 := kmsg.NewAlterConfigsRequest()
	rr2 := kmsg.NewAlterConfigsRequestResource()
	rr2.ResourceType = kmsg.ConfigResourceTypeTopic
	rr2.ResourceName = topic
	rc2 := kmsg.NewAlterConfigsRequestResourceConfig()
	rc2.Name = "retention.ms"
	v2 := "86400000"
	rc2.Value = &v2
	rr2.Configs = append(rr2.Configs, rc2)
	req2.Resources = append(req2.Resources, rr2)

	resp2, err := req2.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp2.Resources[0].ErrorCode != 0 {
		t.Fatal(kerr.ErrorForCode(resp2.Resources[0].ErrorCode))
	}

	// Read back topic config.
	tcfgs, err := adm.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	found = false
	for _, rc := range tcfgs {
		for _, c := range rc.Configs {
			if c.Key == "retention.ms" && c.Value != nil && *c.Value == "86400000" {
				found = true
			}
		}
	}
	if !found {
		t.Error("retention.ms=86400000 not found after AlterConfigs on topic")
	}

	// Unknown topic → error.
	req3 := kmsg.NewAlterConfigsRequest()
	rr3 := kmsg.NewAlterConfigsRequestResource()
	rr3.ResourceType = kmsg.ConfigResourceTypeTopic
	rr3.ResourceName = "nonexistent"
	req3.Resources = append(req3.Resources, rr3)
	resp3, err := req3.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp3.Resources[0].ErrorCode != kerr.UnknownTopicOrPartition.Code {
		t.Errorf("unknown topic: got %v", kerr.ErrorForCode(resp3.Resources[0].ErrorCode))
	}

	// Unsupported resource type.
	req4 := kmsg.NewAlterConfigsRequest()
	rr4 := kmsg.NewAlterConfigsRequestResource()
	rr4.ResourceType = 99
	req4.Resources = append(req4.Resources, rr4)
	resp4, err := req4.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp4.Resources[0].ErrorCode != kerr.InvalidRequest.Code {
		t.Errorf("bad resource type: got %v", kerr.ErrorForCode(resp4.Resources[0].ErrorCode))
	}

	// ValidateOnly should succeed but not persist.
	req5 := kmsg.NewAlterConfigsRequest()
	req5.ValidateOnly = true
	rr5 := kmsg.NewAlterConfigsRequestResource()
	rr5.ResourceType = kmsg.ConfigResourceTypeBroker
	rc5 := kmsg.NewAlterConfigsRequestResourceConfig()
	rc5.Name = "log.retention.ms"
	v5 := "999"
	rc5.Value = &v5
	rr5.Configs = append(rr5.Configs, rc5)
	req5.Resources = append(req5.Resources, rr5)
	resp5, err := req5.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp5.Resources[0].ErrorCode != 0 {
		t.Fatal(kerr.ErrorForCode(resp5.Resources[0].ErrorCode))
	}
	// Verify value didn't change.
	bcfgs2, err := adm.DescribeBrokerConfigs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, rc := range bcfgs2 {
		for _, c := range rc.Configs {
			if c.Key == "log.retention.ms" && c.Value != nil && *c.Value == "999" {
				t.Error("validate-only should not have changed the config value to 999")
			}
		}
	}
}

// TestIncrementalAlterConfigsOperations verifies SET, DELETE, and validate-only
// operations on broker configs, reading back via DescribeConfigs.
func TestIncrementalAlterConfigsOperations(t *testing.T) {
	t.Parallel()
	topic := "inc-alter-configs-ops"
	c := newCoverCluster(t, NumBrokers(1), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cl := newCoverClient(t, c)
	adm := kadm.NewClient(cl)

	// SET a broker config.
	req := kmsg.NewIncrementalAlterConfigsRequest()
	rr := kmsg.NewIncrementalAlterConfigsRequestResource()
	rr.ResourceType = kmsg.ConfigResourceTypeBroker
	rc := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	rc.Name = "log.retention.ms"
	v := "7200000"
	rc.Value = &v
	rc.Op = kmsg.IncrementalAlterConfigOpSet
	rr.Configs = append(rr.Configs, rc)
	req.Resources = append(req.Resources, rr)

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp.Resources[0].ErrorCode != 0 {
		t.Fatal(kerr.ErrorForCode(resp.Resources[0].ErrorCode))
	}

	// Verify it was set.
	bcfgs, err := adm.DescribeBrokerConfigs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, rc := range bcfgs {
		for _, c := range rc.Configs {
			if c.Key == "log.retention.ms" && c.Value != nil && *c.Value == "7200000" {
				found = true
			}
		}
	}
	if !found {
		t.Error("log.retention.ms not found after incremental SET")
	}

	// DELETE the config.
	req2 := kmsg.NewIncrementalAlterConfigsRequest()
	rr2 := kmsg.NewIncrementalAlterConfigsRequestResource()
	rr2.ResourceType = kmsg.ConfigResourceTypeBroker
	rc2 := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	rc2.Name = "log.retention.ms"
	rc2.Op = kmsg.IncrementalAlterConfigOpDelete
	rr2.Configs = append(rr2.Configs, rc2)
	req2.Resources = append(req2.Resources, rr2)

	resp2, err := req2.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp2.Resources[0].ErrorCode != 0 {
		t.Fatal(kerr.ErrorForCode(resp2.Resources[0].ErrorCode))
	}

	// Verify it was deleted (should not appear as dynamic config).
	bcfgs2, err := adm.DescribeBrokerConfigs(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, rc := range bcfgs2 {
		for _, c := range rc.Configs {
			if c.Key == "log.retention.ms" && c.Value != nil && *c.Value == "7200000" {
				t.Error("log.retention.ms should have been deleted")
			}
		}
	}

	// Topic configs: SET then DELETE.
	req3 := kmsg.NewIncrementalAlterConfigsRequest()
	rr3 := kmsg.NewIncrementalAlterConfigsRequestResource()
	rr3.ResourceType = kmsg.ConfigResourceTypeTopic
	rr3.ResourceName = topic
	rc3 := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	rc3.Name = "retention.ms"
	v3 := "172800000"
	rc3.Value = &v3
	rc3.Op = kmsg.IncrementalAlterConfigOpSet
	rr3.Configs = append(rr3.Configs, rc3)
	req3.Resources = append(req3.Resources, rr3)

	resp3, err := req3.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp3.Resources[0].ErrorCode != 0 {
		t.Fatal(kerr.ErrorForCode(resp3.Resources[0].ErrorCode))
	}

	// Verify topic config via DescribeTopicConfigs.
	tcfgs, err := adm.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	found = false
	for _, rc := range tcfgs {
		for _, c := range rc.Configs {
			if c.Key == "retention.ms" && c.Value != nil && *c.Value == "172800000" {
				found = true
			}
		}
	}
	if !found {
		t.Error("retention.ms not found after incremental SET on topic")
	}

	// DELETE topic config.
	req4 := kmsg.NewIncrementalAlterConfigsRequest()
	rr4 := kmsg.NewIncrementalAlterConfigsRequestResource()
	rr4.ResourceType = kmsg.ConfigResourceTypeTopic
	rr4.ResourceName = topic
	rc4 := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	rc4.Name = "retention.ms"
	rc4.Op = kmsg.IncrementalAlterConfigOpDelete
	rr4.Configs = append(rr4.Configs, rc4)
	req4.Resources = append(req4.Resources, rr4)

	resp4, err := req4.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp4.Resources[0].ErrorCode != 0 {
		t.Fatal(kerr.ErrorForCode(resp4.Resources[0].ErrorCode))
	}

	// Unknown topic.
	req5 := kmsg.NewIncrementalAlterConfigsRequest()
	rr5 := kmsg.NewIncrementalAlterConfigsRequestResource()
	rr5.ResourceType = kmsg.ConfigResourceTypeTopic
	rr5.ResourceName = "nonexistent"
	req5.Resources = append(req5.Resources, rr5)
	resp5, err := req5.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp5.Resources[0].ErrorCode != kerr.UnknownTopicOrPartition.Code {
		t.Errorf("unknown topic: got %v", kerr.ErrorForCode(resp5.Resources[0].ErrorCode))
	}

	// ValidateOnly.
	req6 := kmsg.NewIncrementalAlterConfigsRequest()
	req6.ValidateOnly = true
	rr6 := kmsg.NewIncrementalAlterConfigsRequestResource()
	rr6.ResourceType = kmsg.ConfigResourceTypeBroker
	rc6 := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	rc6.Name = "log.retention.ms"
	v6 := "111"
	rc6.Value = &v6
	rc6.Op = kmsg.IncrementalAlterConfigOpSet
	rr6.Configs = append(rr6.Configs, rc6)
	req6.Resources = append(req6.Resources, rr6)
	resp6, err := req6.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp6.Resources[0].ErrorCode != 0 {
		t.Fatal(kerr.ErrorForCode(resp6.Resources[0].ErrorCode))
	}
}

// TestCreateTopicsEdgeCases covers various CreateTopics error paths and features.
func TestCreateTopicsEdgeCases(t *testing.T) {
	t.Parallel()
	c := newCoverCluster(t, NumBrokers(2), SeedTopics(1, "existing-topic"),
		DefaultNumPartitions(7))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cl := newCoverClient(t, c)
	adm := kadm.NewClient(cl)

	// Duplicate topic names in request.
	req := kmsg.NewCreateTopicsRequest()
	for range 2 {
		rt := kmsg.NewCreateTopicsRequestTopic()
		rt.Topic = "dup"
		rt.NumPartitions = 1
		rt.ReplicationFactor = 1
		req.Topics = append(req.Topics, rt)
	}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	for _, st := range resp.Topics {
		if st.ErrorCode != kerr.InvalidRequest.Code {
			t.Errorf("dup: expected InvalidRequest, got %v", kerr.ErrorForCode(st.ErrorCode))
		}
	}

	// Already exists.
	_, err = adm.CreateTopics(ctx, 1, 1, nil, "existing-topic")
	if err != nil {
		t.Fatal(err)
	}

	// Replication factor too high.
	req2 := kmsg.NewCreateTopicsRequest()
	rt2 := kmsg.NewCreateTopicsRequestTopic()
	rt2.Topic = "high-repl"
	rt2.NumPartitions = 1
	rt2.ReplicationFactor = 10
	req2.Topics = append(req2.Topics, rt2)
	resp2, err := req2.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp2.Topics[0].ErrorCode != kerr.InvalidReplicationFactor.Code {
		t.Errorf("expected InvalidReplicationFactor, got %v", kerr.ErrorForCode(resp2.Topics[0].ErrorCode))
	}

	// Zero partitions.
	req3 := kmsg.NewCreateTopicsRequest()
	rt3 := kmsg.NewCreateTopicsRequestTopic()
	rt3.Topic = "zero"
	rt3.NumPartitions = 0
	rt3.ReplicationFactor = 1
	req3.Topics = append(req3.Topics, rt3)
	resp3, err := req3.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp3.Topics[0].ErrorCode != kerr.InvalidPartitions.Code {
		t.Errorf("expected InvalidPartitions, got %v", kerr.ErrorForCode(resp3.Topics[0].ErrorCode))
	}

	// Default partitions (-1 = use DefaultNumPartitions).
	req4 := kmsg.NewCreateTopicsRequest()
	rt4 := kmsg.NewCreateTopicsRequestTopic()
	rt4.Topic = "default-parts"
	rt4.NumPartitions = -1
	rt4.ReplicationFactor = -1
	req4.Topics = append(req4.Topics, rt4)
	resp4, err := req4.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp4.Topics[0].ErrorCode != 0 {
		t.Fatal(kerr.ErrorForCode(resp4.Topics[0].ErrorCode))
	}
	if resp4.Topics[0].NumPartitions != 7 {
		t.Errorf("expected 7 default partitions, got %d", resp4.Topics[0].NumPartitions)
	}

	// With ReplicaAssignment.
	req5 := kmsg.NewCreateTopicsRequest()
	rt5 := kmsg.NewCreateTopicsRequestTopic()
	rt5.Topic = "assigned"
	rt5.NumPartitions = -1
	rt5.ReplicationFactor = -1
	for i := range int32(2) {
		ra := kmsg.NewCreateTopicsRequestTopicReplicaAssignment()
		ra.Partition = i
		ra.Replicas = []int32{i % 2}
		rt5.ReplicaAssignment = append(rt5.ReplicaAssignment, ra)
	}
	req5.Topics = append(req5.Topics, rt5)
	resp5, err := req5.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp5.Topics[0].ErrorCode != 0 {
		t.Fatal(kerr.ErrorForCode(resp5.Topics[0].ErrorCode))
	}

	// ReplicaAssignment with NumPartitions != -1 → InvalidRequest.
	req6 := kmsg.NewCreateTopicsRequest()
	rt6 := kmsg.NewCreateTopicsRequestTopic()
	rt6.Topic = "bad-assign"
	rt6.NumPartitions = 3
	rt6.ReplicationFactor = 1
	ra6 := kmsg.NewCreateTopicsRequestTopicReplicaAssignment()
	ra6.Partition = 0
	ra6.Replicas = []int32{0}
	rt6.ReplicaAssignment = append(rt6.ReplicaAssignment, ra6)
	req6.Topics = append(req6.Topics, rt6)
	resp6, err := req6.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp6.Topics[0].ErrorCode != kerr.InvalidRequest.Code {
		t.Errorf("expected InvalidRequest, got %v", kerr.ErrorForCode(resp6.Topics[0].ErrorCode))
	}

	// ValidateOnly — topic should not be created.
	req7 := kmsg.NewCreateTopicsRequest()
	req7.ValidateOnly = true
	rt7 := kmsg.NewCreateTopicsRequestTopic()
	rt7.Topic = "validate-only"
	rt7.NumPartitions = 3
	rt7.ReplicationFactor = 1
	req7.Topics = append(req7.Topics, rt7)
	resp7, err := req7.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp7.Topics[0].ErrorCode != 0 {
		t.Fatal(kerr.ErrorForCode(resp7.Topics[0].ErrorCode))
	}
	if ti := c.TopicInfo("validate-only"); ti != nil {
		t.Error("validate-only should not create the topic")
	}

	// With topic configs.
	req8 := kmsg.NewCreateTopicsRequest()
	rt8 := kmsg.NewCreateTopicsRequestTopic()
	rt8.Topic = "with-config"
	rt8.NumPartitions = 1
	rt8.ReplicationFactor = 1
	cfg := kmsg.NewCreateTopicsRequestTopicConfig()
	cfg.Name = "retention.ms"
	rv := "86400000"
	cfg.Value = &rv
	rt8.Configs = append(rt8.Configs, cfg)
	req8.Topics = append(req8.Topics, rt8)
	resp8, err := req8.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp8.Topics[0].ErrorCode != 0 {
		t.Fatal(kerr.ErrorForCode(resp8.Topics[0].ErrorCode))
	}
	// Verify the config was applied.
	tcfgs, err := adm.DescribeTopicConfigs(ctx, "with-config")
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, rc := range tcfgs {
		for _, c := range rc.Configs {
			if c.Key == "retention.ms" && c.Value != nil && *c.Value == "86400000" {
				found = true
			}
		}
	}
	if !found {
		t.Error("retention.ms config not found on created topic")
	}
}

// TestDeleteTopicsByID verifies that DeleteTopics by topic ID actually removes
// the topic so it's no longer accessible.
func TestDeleteTopicsByID(t *testing.T) {
	t.Parallel()
	topic := "delete-by-id"
	c := newCoverCluster(t, NumBrokers(1), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cl := newCoverClient(t, c)

	// Get topic ID.
	metaReq := kmsg.NewMetadataRequest()
	metaResp, err := metaReq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	var topicID [16]byte
	for _, mt := range metaResp.Topics {
		if mt.Topic != nil && *mt.Topic == topic {
			topicID = mt.TopicID
		}
	}
	if topicID == [16]byte{} {
		t.Fatal("topic ID not found")
	}

	// Delete by ID.
	delReq := kmsg.NewDeleteTopicsRequest()
	delReq.Version = 6
	dt := kmsg.NewDeleteTopicsRequestTopic()
	dt.TopicID = topicID
	delReq.Topics = append(delReq.Topics, dt)

	delResp, err := delReq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if delResp.Topics[0].ErrorCode != 0 {
		t.Fatal(kerr.ErrorForCode(delResp.Topics[0].ErrorCode))
	}

	// Verify topic is gone using the cluster API.
	if ti := c.TopicInfo(topic); ti != nil {
		t.Error("topic should be deleted but TopicInfo still returns it")
	}
}

// TestDescribeProducersShowsActiveTransaction verifies that DescribeProducers
// reports active transactional producers and clears them after commit.
func TestDescribeProducersShowsActiveTransaction(t *testing.T) {
	t.Parallel()
	topic := "describe-producers"
	c := newCoverCluster(t, NumBrokers(1), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	producer := newCoverClient(t, c,
		kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID("desc-prod-txn"),
	)
	if err := producer.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := producer.ProduceSync(ctx, kgo.StringRecord("txn-data")).FirstErr(); err != nil {
		t.Fatal(err)
	}

	// Describe — should see active producer.
	cl := newCoverClient(t, c)
	req := kmsg.NewDescribeProducersRequest()
	rt := kmsg.NewDescribeProducersRequestTopic()
	rt.Topic = topic
	rt.Partitions = []int32{0}
	req.Topics = append(req.Topics, rt)

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	var activeCount int
	for _, st := range resp.Topics {
		for _, sp := range st.Partitions {
			activeCount += len(sp.ActiveProducers)
		}
	}
	if activeCount == 0 {
		t.Error("expected active producer during open transaction")
	}

	// Commit transaction.
	if err := producer.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}

	// Describe again — no active producers.
	resp2, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	for _, st := range resp2.Topics {
		for _, sp := range st.Partitions {
			if len(sp.ActiveProducers) > 0 {
				t.Errorf("expected no active producers after commit, got %d", len(sp.ActiveProducers))
			}
		}
	}
}

// TestTxnProduceAndCommit verifies a basic transactional produce + commit flow.
// After commit, a read_committed consumer should see the records.
func TestTxnProduceAndCommit(t *testing.T) {
	t.Parallel()
	topic := "txn-produce-commit"
	c := newCoverCluster(t, NumBrokers(1), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Transactional produce.
	txnCl := newCoverClient(t, c,
		kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID("txn-produce-id"),
	)
	if err := txnCl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		if err := txnCl.ProduceSync(ctx, kgo.StringRecord("txn-data")).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	if err := txnCl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}

	// Read committed consumer should see all 5 records.
	consumer := newCoverClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	var count int
	deadline := time.After(5 * time.Second)
	for count < 5 {
		select {
		case <-deadline:
			t.Fatalf("timeout: got %d/5 committed records", count)
		default:
		}
		fs := consumer.PollRecords(ctx, 10)
		fs.EachRecord(func(r *kgo.Record) { count++ })
	}
}

// TestListTransactionsFindsOngoing verifies that ListTransactions finds
// ongoing transactions by state filter.
func TestListTransactionsFindsOngoing(t *testing.T) {
	t.Parallel()
	topic := "list-txn-ongoing"
	c := newCoverCluster(t, NumBrokers(1), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	txnID := "list-txn-ongoing-id"
	producer := newCoverClient(t, c,
		kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID(txnID),
	)
	if err := producer.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := producer.ProduceSync(ctx, kgo.StringRecord("v")).FirstErr(); err != nil {
		t.Fatal(err)
	}

	adm := kadm.NewClient(newCoverClient(t, c))

	// Unfiltered list.
	listed, err := adm.ListTransactions(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := listed[txnID]; !ok {
		t.Error("transaction not found in unfiltered list")
	}

	// Filtered by state.
	listed2, err := adm.ListTransactions(ctx, nil, []string{"Ongoing"})
	if err != nil {
		t.Fatal(err)
	}
	tx, ok := listed2[txnID]
	if !ok {
		t.Error("transaction not found with Ongoing filter")
	} else if tx.State != "Ongoing" {
		t.Errorf("expected Ongoing state, got %q", tx.State)
	}

	if err := producer.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatal(err)
	}
}

// TestShufflePartitionLeadersIsUsable verifies that after shuffling leaders,
// all partitions remain functional.
func TestShufflePartitionLeadersIsUsable(t *testing.T) {
	t.Parallel()
	topic := "shuffle-leaders"
	c := newCoverCluster(t, NumBrokers(3), SeedTopics(5, topic))

	c.ShufflePartitionLeaders()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Produce to all partitions.
	cl := newCoverClient(t, c, kgo.DefaultProduceTopic(topic))
	for p := range int32(5) {
		r := kgo.StringRecord("after-shuffle")
		r.Partition = p
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Errorf("produce to partition %d after shuffle: %v", p, err)
		}
	}
}

// TestCoordinatorAPIs verifies CoordinatorFor and RehashCoordinators.
func TestCoordinatorAPIs(t *testing.T) {
	t.Parallel()
	c := newCoverCluster(t, NumBrokers(3))

	coord := c.CoordinatorFor("my-group")
	if coord < 0 {
		t.Error("expected valid coordinator")
	}

	c.RehashCoordinators()
	// CoordinatorFor should still return a valid node (may or may not change).
	coord2 := c.CoordinatorFor("my-group")
	if coord2 < 0 {
		t.Error("expected valid coordinator after rehash")
	}
}

// TestOffsetForLeaderEpochCurrentEpoch verifies that querying the current
// epoch returns the high watermark as the end offset.
func TestOffsetForLeaderEpochCurrentEpoch(t *testing.T) {
	t.Parallel()
	topic := "ofle-current"
	c := newCoverCluster(t, NumBrokers(1), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cl := newCoverClient(t, c, kgo.DefaultProduceTopic(topic))
	for range 5 {
		if err := cl.ProduceSync(ctx, kgo.StringRecord("v")).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	adm := kadm.NewClient(cl)
	var req kadm.OffsetForLeaderEpochRequest
	req.Add(topic, 0, 0) // partition 0, epoch 0
	resp, err := adm.OffsetForLeaderEpoch(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	r, ok := resp[topic][0]
	if !ok {
		t.Fatal("no response for partition 0")
	}
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	if r.EndOffset != 5 {
		t.Errorf("expected EndOffset=5 (HWM), got %d", r.EndOffset)
	}
	if r.LeaderEpoch != 0 {
		t.Errorf("expected LeaderEpoch=0, got %d", r.LeaderEpoch)
	}
}

// TestOffsetForLeaderEpochAfterLeaderChange verifies that after a leader
// change (epoch bump), querying for the old epoch returns the offset boundary
// where the epoch changed.
func TestOffsetForLeaderEpochAfterLeaderChange(t *testing.T) {
	t.Parallel()
	topic := "ofle-leader-change"
	c := newCoverCluster(t, NumBrokers(2), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Produce 3 records at epoch 0.
	origLeader := c.LeaderFor(topic, 0)
	cl := newCoverClient(t, c, kgo.DefaultProduceTopic(topic), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	for range 3 {
		if err := cl.ProduceSync(ctx, kgo.StringRecord("e0")).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	// Move partition to the other broker, bumping epoch to 1.
	otherNode := int32(0)
	if origLeader == 0 {
		otherNode = 1
	}
	if err := c.MoveTopicPartition(topic, 0, otherNode); err != nil {
		t.Fatal(err)
	}

	// Produce 2 records at epoch 1 (need a fresh client to pick up new leader).
	cl2 := newCoverClient(t, c, kgo.DefaultProduceTopic(topic), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	for range 2 {
		if err := cl2.ProduceSync(ctx, kgo.StringRecord("e1")).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	// Query for epoch 0: should return the boundary offset (3) where epoch changed.
	req := kmsg.NewOffsetForLeaderEpochRequest()
	req.ReplicaID = -1
	rt := kmsg.NewOffsetForLeaderEpochRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewOffsetForLeaderEpochRequestTopicPartition()
	rp.Partition = 0
	rp.CurrentLeaderEpoch = 1 // current epoch
	rp.LeaderEpoch = 0        // query for epoch 0
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)

	resp, err := req.RequestWith(ctx, cl2)
	if err != nil {
		t.Fatal(err)
	}
	sp := resp.Topics[0].Partitions[0]
	if sp.ErrorCode != 0 {
		t.Fatal(kerr.ErrorForCode(sp.ErrorCode))
	}
	if sp.EndOffset != 3 {
		t.Errorf("expected EndOffset=3 (boundary), got %d", sp.EndOffset)
	}
	if sp.LeaderEpoch != 0 {
		t.Errorf("expected LeaderEpoch=0, got %d", sp.LeaderEpoch)
	}
}

// TestOffsetForLeaderEpochEmptyPartition verifies that querying the epoch
// of a partition with no batches returns offset 0.
func TestOffsetForLeaderEpochEmptyPartition(t *testing.T) {
	t.Parallel()
	topic := "ofle-empty"
	c := newCoverCluster(t, NumBrokers(1), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Move partition to bump epoch, but don't produce. This tests the
	// "no batches" path (lines 97-105 in the handler).
	c.MoveTopicPartition(topic, 0, 0) // epoch 0 → 1
	adm := kadm.NewClient(newCoverClient(t, c))
	var req kadm.OffsetForLeaderEpochRequest
	req.Add(topic, 0, 0) // query for old epoch 0
	resp, err := adm.OffsetForLeaderEpoch(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	r := resp[topic][0]
	if r.Err != nil {
		t.Fatal(r.Err)
	}
	// No batches at any epoch, so the handler returns epoch=current, offset=0.
	if r.EndOffset != 0 {
		t.Errorf("expected EndOffset=0 for empty partition, got %d", r.EndOffset)
	}
}

// TestDeleteTopicsByName verifies that DeleteTopics by name removes the topic.
func TestDeleteTopicsByName(t *testing.T) {
	t.Parallel()
	topic := "delete-by-name"
	c := newCoverCluster(t, NumBrokers(1), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	adm := kadm.NewClient(newCoverClient(t, c))
	resp, err := adm.DeleteTopics(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if r := resp[topic]; r.Err != nil {
		t.Fatal(r.Err)
	}

	if ti := c.TopicInfo(topic); ti != nil {
		t.Error("topic should be deleted but TopicInfo still returns it")
	}
}

// TestDeleteTopicsUnknownByName verifies that deleting a nonexistent topic
// by name returns UnknownTopicOrPartition.
func TestDeleteTopicsUnknownByName(t *testing.T) {
	t.Parallel()
	c := newCoverCluster(t, NumBrokers(1))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	adm := kadm.NewClient(newCoverClient(t, c))
	resp, err := adm.DeleteTopics(ctx, "does-not-exist")
	if err != nil {
		t.Fatal(err)
	}
	r := resp["does-not-exist"]
	if r.Err == nil {
		t.Error("expected error deleting nonexistent topic")
	} else if r.Err != kerr.UnknownTopicOrPartition {
		t.Errorf("expected UnknownTopicOrPartition, got %v", r.Err)
	}
}

// TestListOffsetsEarliestLatest verifies that ListStartOffsets returns the
// log start offset and ListEndOffsets returns the high watermark after producing.
func TestListOffsetsEarliestLatest(t *testing.T) {
	t.Parallel()
	topic := "list-offsets-el"
	c := newCoverCluster(t, NumBrokers(1), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cl := newCoverClient(t, c, kgo.DefaultProduceTopic(topic))
	for range 5 {
		if err := cl.ProduceSync(ctx, kgo.StringRecord("v")).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	adm := kadm.NewClient(cl)

	// ListStartOffsets = earliest (timestamp -2).
	start, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if o, ok := start.Lookup(topic, 0); !ok {
		t.Fatal("partition 0 not in start offsets")
	} else if o.Offset != 0 {
		t.Errorf("expected start offset 0, got %d", o.Offset)
	}

	// ListEndOffsets = latest (timestamp -1).
	end, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if o, ok := end.Lookup(topic, 0); !ok {
		t.Fatal("partition 0 not in end offsets")
	} else if o.Offset != 5 {
		t.Errorf("expected end offset 5, got %d", o.Offset)
	}
}

// TestListOffsetsReadCommittedDuringTxn verifies that during an open
// transaction, ListCommittedOffsets (LSO) is less than ListEndOffsets (HWM).
func TestListOffsetsReadCommittedDuringTxn(t *testing.T) {
	t.Parallel()
	topic := "list-offsets-rc"
	c := newCoverCluster(t, NumBrokers(1), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Produce 3 non-transactional records first.
	cl := newCoverClient(t, c, kgo.DefaultProduceTopic(topic))
	for range 3 {
		if err := cl.ProduceSync(ctx, kgo.StringRecord("non-txn")).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	// Begin a transaction and produce 2 more.
	txnCl := newCoverClient(t, c,
		kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID("list-offsets-rc-txn"),
	)
	if err := txnCl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := txnCl.ProduceSync(ctx, kgo.StringRecord("txn")).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	adm := kadm.NewClient(cl)

	// HWM includes uncommitted txn records.
	end, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	hwm := end[topic][0].Offset

	// LSO should be at or before the first uncommitted txn record.
	committed, err := adm.ListCommittedOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	lso := committed[topic][0].Offset

	if lso >= hwm {
		t.Errorf("expected LSO (%d) < HWM (%d) during open transaction", lso, hwm)
	}

	// Commit the transaction.
	if err := txnCl.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}

	// After commit, LSO should equal HWM.
	committed2, err := adm.ListCommittedOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	end2, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if committed2[topic][0].Offset != end2[topic][0].Offset {
		t.Errorf("after commit: LSO=%d != HWM=%d",
			committed2[topic][0].Offset, end2[topic][0].Offset)
	}
}

// TestTxnAbortDiscardsOffsets verifies that offset commits staged in a
// transaction are discarded when the transaction is aborted.
func TestTxnAbortDiscardsOffsets(t *testing.T) {
	t.Parallel()
	topic := "txn-abort-offsets"
	group := "txn-abort-group"
	txnID := "txn-abort-offsets-id"
	c := newCoverCluster(t, NumBrokers(1), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Produce some records so there's something to "consume".
	cl := newCoverClient(t, c, kgo.DefaultProduceTopic(topic))
	for range 5 {
		if err := cl.ProduceSync(ctx, kgo.StringRecord("v")).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	// Use raw protocol for the full transactional offset commit flow.
	rawCl := newCoverClient(t, c)

	// InitProducerID.
	initReq := kmsg.NewInitProducerIDRequest()
	initReq.TransactionalID = &txnID
	initReq.TransactionTimeoutMillis = 60000
	initResp, err := initReq.RequestWith(ctx, rawCl)
	if err != nil {
		t.Fatal(err)
	}
	if initResp.ErrorCode != 0 {
		t.Fatal(kerr.ErrorForCode(initResp.ErrorCode))
	}
	pid := initResp.ProducerID
	epoch := initResp.ProducerEpoch

	// AddOffsetsToTxn.
	addReq := kmsg.NewAddOffsetsToTxnRequest()
	addReq.TransactionalID = txnID
	addReq.ProducerID = pid
	addReq.ProducerEpoch = epoch
	addReq.Group = group
	addResp, err := addReq.RequestWith(ctx, rawCl)
	if err != nil {
		t.Fatal(err)
	}
	if addResp.ErrorCode != 0 {
		t.Fatal(kerr.ErrorForCode(addResp.ErrorCode))
	}

	// TxnOffsetCommit — stage offset 5 for the group.
	commitReq := kmsg.NewTxnOffsetCommitRequest()
	commitReq.TransactionalID = txnID
	commitReq.Group = group
	commitReq.ProducerID = pid
	commitReq.ProducerEpoch = epoch
	commitReq.Generation = -1
	ct := kmsg.NewTxnOffsetCommitRequestTopic()
	ct.Topic = topic
	cp := kmsg.NewTxnOffsetCommitRequestTopicPartition()
	cp.Partition = 0
	cp.Offset = 5
	ct.Partitions = append(ct.Partitions, cp)
	commitReq.Topics = append(commitReq.Topics, ct)

	commitResp, err := commitReq.RequestWith(ctx, rawCl)
	if err != nil {
		t.Fatal(err)
	}
	if commitResp.Topics[0].Partitions[0].ErrorCode != 0 {
		t.Fatal(kerr.ErrorForCode(commitResp.Topics[0].Partitions[0].ErrorCode))
	}

	// ABORT the transaction.
	endReq := kmsg.NewEndTxnRequest()
	endReq.TransactionalID = txnID
	endReq.ProducerID = pid
	endReq.ProducerEpoch = epoch
	endReq.Commit = false
	endResp, err := endReq.RequestWith(ctx, rawCl)
	if err != nil {
		t.Fatal(err)
	}
	if endResp.ErrorCode != 0 {
		t.Fatal(kerr.ErrorForCode(endResp.ErrorCode))
	}

	// Verify offsets were NOT committed to the group.
	// GROUP_ID_NOT_FOUND is expected — the group was never created because
	// the abort discarded the staged offsets.
	adm := kadm.NewClient(cl)
	offsets, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		if err != kerr.GroupIDNotFound {
			t.Fatal(err)
		}
		return // group doesn't exist = offsets weren't committed
	}
	if o, ok := offsets.Lookup(topic, 0); ok && o.At >= 0 {
		t.Errorf("expected no committed offset after abort, got %d", o.At)
	}
}

// TestTxnAbortRecordsInvisibleToReadCommitted verifies that records produced
// in an aborted transaction are NOT visible to a read_committed consumer.
// This is the core transactional isolation guarantee.
func TestTxnAbortRecordsInvisibleToReadCommitted(t *testing.T) {
	t.Parallel()
	topic := "txn-abort-invisible"
	c := newCoverCluster(t, NumBrokers(1), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Produce 3 committed records.
	cl := newCoverClient(t, c, kgo.DefaultProduceTopic(topic))
	for range 3 {
		if err := cl.ProduceSync(ctx, kgo.StringRecord("committed")).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	// Produce 5 records in a transaction, then ABORT.
	txnCl := newCoverClient(t, c,
		kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID("txn-abort-invisible-id"),
	)
	if err := txnCl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		if err := txnCl.ProduceSync(ctx, kgo.StringRecord("aborted")).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	if err := txnCl.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatal(err)
	}

	// Produce 2 more committed records.
	for range 2 {
		if err := cl.ProduceSync(ctx, kgo.StringRecord("committed2")).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	// read_committed consumer should see only 5 records (3 + 2), not 10.
	consumer := newCoverClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	var count int
	deadline := time.After(3 * time.Second)
	for count < 5 {
		select {
		case <-deadline:
			t.Fatalf("timeout: got %d/5 committed records", count)
		default:
		}
		fs := consumer.PollRecords(ctx, 10)
		fs.EachRecord(func(r *kgo.Record) { count++ })
	}
	// Check no extra (aborted) records leak through.
	shortCtx, shortCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer shortCancel()
	fs := consumer.PollRecords(shortCtx, 10)
	fs.EachRecord(func(r *kgo.Record) { count++ })
	if count != 5 {
		t.Errorf("read_committed consumer saw %d records, want 5 (aborted should be invisible)", count)
	}
}

// TestDeleteRecordsAdvancesStartOffset verifies that after DeleteRecords,
// ListStartOffsets reflects the new log start offset.
func TestDeleteRecordsAdvancesStartOffset(t *testing.T) {
	t.Parallel()
	topic := "delete-records-start"
	c := newCoverCluster(t, NumBrokers(1), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cl := newCoverClient(t, c, kgo.DefaultProduceTopic(topic))
	for range 10 {
		if err := cl.ProduceSync(ctx, kgo.StringRecord("v")).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	adm := kadm.NewClient(cl)

	// Before delete: start=0, end=10.
	start, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if o := start[topic][0].Offset; o != 0 {
		t.Fatalf("expected start=0 before delete, got %d", o)
	}

	// Delete records up to offset 7.
	offsets := kadm.Offsets{}
	offsets.Add(kadm.Offset{Topic: topic, Partition: 0, At: 7})
	dresp, err := adm.DeleteRecords(ctx, offsets)
	if err != nil {
		t.Fatal(err)
	}
	if err := dresp.Error(); err != nil {
		t.Fatal(err)
	}

	// After delete: start should be 7.
	start2, err := adm.ListStartOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if o := start2[topic][0].Offset; o != 7 {
		t.Errorf("expected start=7 after delete, got %d", o)
	}

	// End should still be 10.
	end, err := adm.ListEndOffsets(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	if o := end[topic][0].Offset; o != 10 {
		t.Errorf("expected end=10, got %d", o)
	}

	// Consumer from start should only see records 7-9 (3 records).
	consumer := newCoverClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	var count int
	deadline := time.After(3 * time.Second)
	for count < 3 {
		select {
		case <-deadline:
			t.Fatalf("timeout: got %d/3 records", count)
		default:
		}
		fs := consumer.PollRecords(ctx, 10)
		fs.EachRecord(func(r *kgo.Record) {
			if r.Offset < 7 {
				t.Errorf("got record at offset %d, expected >= 7", r.Offset)
			}
			count++
		})
	}
}
