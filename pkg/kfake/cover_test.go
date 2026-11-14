package kfake

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

// newCoverCluster is newCluster with a heartbeat interval short enough that
// these tests do not wait on the default.
func newCoverCluster(t *testing.T, opts ...Opt) *Cluster {
	t.Helper()
	opts = append([]Opt{BrokerConfigs(map[string]string{
		"group.consumer.heartbeat.interval.ms": "100",
	})}, opts...)
	return newCluster(t, opts...)
}

// TestCreatePartitionsAddsPartitions verifies that CreatePartitions actually
// increases the partition count and that the new partitions are usable.
func TestCreatePartitionsAddsPartitions(t *testing.T) {
	t.Parallel()
	topic := "create-parts-adds"
	c := newCoverCluster(t, NumBrokers(2), SeedTopics(2, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cl := newPlainClient(t, c)
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

	cl := newPlainClient(t, c)

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
	c := newCoverCluster(t, NumBrokers(2), SeedTopics(5, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cl := newPlainClient(t, c)

	for _, tc := range []struct {
		name     string
		topics   []string // nil is just topic
		count    int32
		assign   [][]int32 // nil sends no assignment list
		timedOut bool
		wantErr  int16
	}{
		{name: "decrease partition count", count: 2, wantErr: kerr.InvalidPartitions.Code},
		{name: "unknown topic", topics: []string{"nonexistent"}, count: 3, wantErr: kerr.UnknownTopicOrPartition.Code},
		{name: "wrong number of assignments", count: 7, assign: [][]int32{{0}}, wantErr: kerr.InvalidReplicaAssignment.Code},
		{name: "assignment with unknown broker", count: 6, assign: [][]int32{{999}}, wantErr: kerr.InvalidReplicaAssignment.Code},
		{name: "assignment with empty replicas", count: 6, assign: [][]int32{nil}, wantErr: kerr.InvalidReplicaAssignment.Code},
		{name: "duplicate topics in request", topics: []string{topic, topic}, count: 6, wantErr: kerr.InvalidRequest.Code},
		{name: "duplicate topics fail only themselves", topics: []string{"nonexistent", topic, topic}, count: 6, wantErr: kerr.UnknownTopicOrPartition.Code},
		{name: "same partition count", count: 5, wantErr: kerr.InvalidPartitions.Code},
		{name: "too many new partitions", count: 5 + 10001, wantErr: kerr.PolicyViolation.Code},
		{name: "timeout already passed", count: 6, timedOut: true, wantErr: kerr.RequestTimedOut.Code},
		{name: "assignment with a different replication factor", count: 6, assign: [][]int32{{0}}, wantErr: kerr.InvalidReplicaAssignment.Code},
		{name: "assignment with a broker twice", count: 6, assign: [][]int32{{0, 0}}, wantErr: kerr.InvalidReplicaAssignment.Code},
		{name: "empty non-null assignment list", count: 6, assign: [][]int32{}, wantErr: kerr.InvalidReplicaAssignment.Code},
	} {
		req := kmsg.NewPtrCreatePartitionsRequest()
		if tc.timedOut {
			req.TimeoutMillis = 0
		}
		if tc.topics == nil {
			tc.topics = []string{topic}
		}
		for _, name := range tc.topics {
			rt := kmsg.NewCreatePartitionsRequestTopic()
			rt.Topic, rt.Count = name, tc.count
			if tc.assign != nil {
				rt.Assignment = []kmsg.CreatePartitionsRequestTopicAssignment{}
			}
			for _, replicas := range tc.assign {
				rt.Assignment = append(rt.Assignment, kmsg.CreatePartitionsRequestTopicAssignment{Replicas: replicas})
			}
			req.Topics = append(req.Topics, rt)
		}
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		// We check the answer for the first topic in the request.
		got := int16(-1)
		for _, st := range resp.Topics {
			if st.Topic == req.Topics[0].Topic {
				got = st.ErrorCode
				break
			}
		}
		if got != tc.wantErr {
			t.Errorf("%s: got error %d (%v), want %d (%v)",
				tc.name, got, kerr.ErrorForCode(got),
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

	cl := newPlainClient(t, c)

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
// non-zero sizes and the usable space after producing data, that it can
// filter by partition, and that it lists a dir with no requested partition.
func TestDescribeLogDirsReportsStorageSize(t *testing.T) {
	t.Parallel()
	topic := "describe-log-dirs-size"
	c := newCoverCluster(t, NumBrokers(1), SeedTopics(2, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	cl := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
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
	// The volume total is fixed; usable space is what the data leaves.
	// kgo's merge of a sharded request drops both, so we ask the broker.
	direct, err := req.RequestWith(ctx, cl.Broker(0))
	if err != nil {
		t.Fatal(err)
	}
	if d := direct.Dirs[0]; d.UsableBytes != d.TotalBytes-totalSize {
		t.Errorf("expected usable %d = total %d - used %d", d.UsableBytes, d.TotalBytes, totalSize)
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

	// A dir is listed even when no requested partition is in it. kgo
	// fails a partition it does not know, so we ask the broker.
	req2.Topics[0].Partitions = []int32{99}
	resp2, err = req2.RequestWith(ctx, cl.Broker(0))
	if err != nil {
		t.Fatal(err)
	}
	if len(resp2.Dirs) != 1 || len(resp2.Dirs[0].Topics) != 0 {
		t.Errorf("expected one empty dir for a missing partition, got %+v", resp2.Dirs)
	}
}

// TestAlterReplicaLogDirsMovesPartition verifies that AlterReplicaLogDirs
// moves a broker's own replica between its configured dirs, confirmed via
// DescribeLogDirs, and that each broker reports only its own replicas.
func TestAlterReplicaLogDirsMovesPartition(t *testing.T) {
	t.Parallel()
	topic := "alter-log-dirs-move"
	newDir := "/data/kafka-logs-moved"
	c := newCoverCluster(t, NumBrokers(2), BrokerConfigs(map[string]string{"log.dirs": defLogDir + ", " + newDir}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cl := newPlainClient(t, c)
	if _, err := kadm.NewClient(cl).CreateTopics(ctx, 1, 1, nil, topic); err != nil {
		t.Fatal(err)
	}
	leader := c.LeaderFor(topic, 0)
	other := 1 - leader

	alter := func(node int32, dir string) int16 {
		t.Helper()
		req := kmsg.NewPtrAlterReplicaLogDirsRequest()
		rd := kmsg.NewAlterReplicaLogDirsRequestDir()
		rd.Dir = dir
		rt := kmsg.NewAlterReplicaLogDirsRequestDirTopic()
		rt.Topic = topic
		rt.Partitions = []int32{0}
		rd.Topics = append(rd.Topics, rt)
		req.Dirs = append(req.Dirs, rd)
		resp, err := req.RequestWith(ctx, cl.Broker(int(node)))
		if err != nil {
			t.Fatal(err)
		}
		return resp.Topics[0].Partitions[0].ErrorCode
	}
	if code := alter(other, newDir); code != kerr.ReplicaNotAvailable.Code {
		t.Errorf("non-replica broker: expected REPLICA_NOT_AVAILABLE, got %v", kerr.ErrorForCode(code))
	}
	for _, dir := range []string{"/data/elsewhere", newDir + "/.."} {
		if code := alter(leader, dir); code != kerr.LogDirNotFound.Code {
			t.Errorf("dir %q: expected LOG_DIR_NOT_FOUND, got %v", dir, kerr.ErrorForCode(code))
		}
	}
	if code := alter(leader, newDir); code != 0 {
		t.Fatalf("alter error: %v", kerr.ErrorForCode(code))
	}

	// Every broker lists both dirs; only the leader holds the partition,
	// now in the new dir.
	for _, node := range []int32{leader, other} {
		dlResp, err := kmsg.NewPtrDescribeLogDirsRequest().RequestWith(ctx, cl.Broker(int(node)))
		if err != nil {
			t.Fatal(err)
		}
		if len(dlResp.Dirs) != 2 {
			t.Errorf("node %d: expected 2 dirs, got %d", node, len(dlResp.Dirs))
		}
		var in []string
		for _, dir := range dlResp.Dirs {
			for _, dt := range dir.Topics {
				if dt.Topic == topic {
					in = append(in, dir.Dir)
				}
			}
		}
		if want := []string{newDir}; node == leader && !slices.Equal(in, want) || node == other && len(in) != 0 {
			t.Errorf("node %d: partition in dirs %v", node, in)
		}
	}
}

// TestAlterPartitionAssignments verifies that reassignment accepts valid
// requests and properly rejects null-replicas (cancel), invalid replicas, and
// unknown topics.
func TestAlterPartitionAssignments(t *testing.T) {
	t.Parallel()
	topic := "alter-assign"
	c := newCoverCluster(t, NumBrokers(2), SeedTopics(5, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cl := newPlainClient(t, c)
	if err := c.MoveTopicPartition(topic, 0, 1); err != nil {
		t.Fatal(err)
	}

	// Partition => replicas and the code we expect. A nil list cancels,
	// and kfake has no reassignment in progress.
	cases := map[int32]struct {
		replicas []int32
		want     int16
	}{
		0: {[]int32{0}, 0},
		1: {nil, kerr.NoReassignmentInProgress.Code},
		2: {[]int32{0, 0}, kerr.InvalidReplicaAssignment.Code},
		3: {[]int32{7}, kerr.InvalidReplicaAssignment.Code},
		4: {[]int32{}, kerr.InvalidReplicaAssignment.Code},
	}
	req := kmsg.NewAlterPartitionAssignmentsRequest()
	rt := kmsg.NewAlterPartitionAssignmentsRequestTopic()
	rt.Topic = topic
	for p, tc := range cases {
		rp := kmsg.NewAlterPartitionAssignmentsRequestTopicPartition()
		rp.Partition = p
		rp.Replicas = tc.replicas
		rt.Partitions = append(rt.Partitions, rp)
	}
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
	for _, st := range resp.Topics {
		for _, sp := range st.Partitions {
			want := cases[sp.Partition].want
			if st.Topic == "no-such-topic" {
				want = kerr.UnknownTopicOrPartition.Code
			}
			if sp.ErrorCode != want {
				t.Errorf("%s %d: expected %v, got %v", st.Topic, sp.Partition, kerr.ErrorForCode(want), kerr.ErrorForCode(sp.ErrorCode))
			}
		}
	}
	// The old leader is not in the new replicas, so the first new one leads.
	if got := c.LeaderFor(topic, 0); got != 0 {
		t.Errorf("partition 0: expected leader 0 after reassignment, got %d", got)
	}

	// Without AllowReplicationFactorChange, the replica count must stay.
	req.AllowReplicationFactorChange = false
	req.Topics = req.Topics[:1]
	req.Topics[0].Partitions = req.Topics[0].Partitions[:0]
	rp := kmsg.NewAlterPartitionAssignmentsRequestTopicPartition()
	rp.Replicas = []int32{1}
	req.Topics[0].Partitions = append(req.Topics[0].Partitions, rp)
	if resp, err = req.RequestWith(ctx, cl); err != nil {
		t.Fatal(err)
	}
	if code := resp.Topics[0].Partitions[0].ErrorCode; code != kerr.InvalidReplicationFactor.Code {
		t.Errorf("replication factor change: expected INVALID_REPLICATION_FACTOR, got %v", kerr.ErrorForCode(code))
	}
	if resp.AllowReplicationFactorChange {
		t.Error("response did not echo AllowReplicationFactorChange")
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

	// Verify we can produce: the new broker should appear in metadata.
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cl := newPlainClient(t, c)
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

	// Remove the controller (node 0) and one added node: another node
	// takes over as controller and topic admin still works.
	if err := c.RemoveNode(0); err != nil {
		t.Fatal(err)
	}
	if err := c.RemoveNode(nodeID); err != nil {
		t.Fatal(err)
	}
	if len(c.ListenAddrs()) != 1 {
		t.Fatalf("expected 1 broker after removals, got %d", len(c.ListenAddrs()))
	}
	adm = kadm.NewClient(newPlainClient(t, c))
	if meta, err = adm.Metadata(ctx); err != nil {
		t.Fatal(err)
	}
	if meta.Controller != 42 {
		t.Errorf("expected controller 42 after removing the controller, got %d", meta.Controller)
	}
	created, err := adm.CreateTopics(ctx, 1, 1, nil, "after-controller-removed")
	if err == nil {
		err = created["after-controller-removed"].Err
	}
	if err != nil {
		t.Errorf("create after removing the controller: %v", err)
	}

	// Can't remove nonexistent node.
	if err := c.RemoveNode(999); err == nil {
		t.Fatal("expected error removing nonexistent node")
	}
}

// TestMoveTopicPartitionChangesLeader verifies MoveTopicPartition actually
// changes the leader, verified through both the API and produce routing. A
// fetch parked on the old leader answers NOT_LEADER_OR_FOLLOWER at the move
// rather than at its MaxWait.
func TestMoveTopicPartitionChangesLeader(t *testing.T) {
	t.Parallel()
	topic := "move-partition"
	c := newCoverCluster(t, NumBrokers(2), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	origLeader := c.LeaderFor(topic, 0)
	target := 1 - origLeader // the other node

	fcl := newPlainClient(t, c)
	fetch := fetchByID(0, -1, 10_000, c.TopicInfo(topic).TopicID, 0)
	parked := make(chan kmsg.Response, 1)
	go func() {
		resp, _ := fcl.Broker(int(origLeader)).Request(ctx, fetch)
		parked <- resp
	}()
	// A share fetch parked there waits out its MaxWait, as in Kafka,
	// and answers nothing for the moved partition.
	scl := newPlainClient(t, c) // the parked fetch holds fcl's connection
	group := "move-partition-g"
	memberID, topicID := joinShareGroupRaw(t, scl, group, topic)
	sf := kmsg.NewPtrShareFetchRequest()
	sf.GroupID, sf.MemberID, sf.MaxWaitMillis, sf.MaxRecords = &group, &memberID, 500, 500
	sf.Topics = []kmsg.ShareFetchRequestTopic{{TopicID: topicID, Partitions: []kmsg.ShareFetchRequestTopicPartition{{Partition: 0}}}}
	shareParked := make(chan kmsg.Response, 1)
	go func() {
		resp, _ := scl.Broker(int(origLeader)).Request(ctx, sf)
		shareParked <- resp
	}()
	waitFor(t, 5*time.Second, "the fetches never parked", func() bool {
		var n int
		c.admin(func() {
			pd, _ := c.data.tps.getp(topic, 0)
			n = min(len(pd.watch), len(pd.shareWatch))
		})
		return n > 0
	})

	if err := c.MoveTopicPartition(topic, 0, target); err != nil {
		t.Fatal(err)
	}
	if got := c.LeaderFor(topic, 0); got != target {
		t.Errorf("leader should be %d, got %d", target, got)
	}
	resp, _ := (<-parked).(*kmsg.FetchResponse)
	if resp == nil || len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("parked fetch: got %+v, want one partition", resp)
	}
	if err := kerr.ErrorForCode(resp.Topics[0].Partitions[0].ErrorCode); err != kerr.NotLeaderForPartition {
		t.Errorf("parked fetch: got %v, want NOT_LEADER_OR_FOLLOWER", err)
	}
	if sresp, _ := (<-shareParked).(*kmsg.ShareFetchResponse); sresp == nil || len(sresp.Topics) != 0 {
		t.Errorf("parked share fetch: got %+v, want no partitions", sresp)
	}

	// Verify it's usable: produce to the partition.
	cl := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
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
// create -> describe -> update -> describe -> delete -> describe (not found).
func TestSCRAMCredentialLifecycle(t *testing.T) {
	t.Parallel()
	c := newCoverCluster(t, NumBrokers(1))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cl := newPlainClient(t, c)

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

	// Describe both and verify the mechanism and iterations.
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

	// Describe all (nil users): this should find both.
	respAll := describe()
	if len(respAll.Results) < 2 {
		t.Errorf("describe-all: expected >=2 results, got %d", len(respAll.Results))
	}
	// An empty non-null list also describes everyone, which is what the
	// Java client sends.
	emptyReq := kmsg.NewDescribeUserSCRAMCredentialsRequest()
	emptyReq.Users = []kmsg.DescribeUserSCRAMCredentialsRequestUser{}
	respEmpty, err := emptyReq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if len(respEmpty.Results) < 2 {
		t.Errorf("describe with an empty list: expected >=2 results, got %d", len(respEmpty.Results))
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

	// Describe alice after delete -> ResourceNotFound.
	resp3 := describe("alice")
	if resp3.Results[0].ErrorCode != kerr.ResourceNotFound.Code {
		t.Errorf("alice after delete: expected ResourceNotFound, got %v",
			kerr.ErrorForCode(resp3.Results[0].ErrorCode))
	}
}

// TestSCRAMValidationErrors verifies that invalid SCRAM operations are rejected
// with appropriate error codes, with one answer per user: a user's first
// failure sticks, and a second change to a user is DUPLICATE_RESOURCE and
// changes nothing for that user.
func TestSCRAMValidationErrors(t *testing.T) {
	t.Parallel()
	c := newCoverCluster(t, NumBrokers(1))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cl := newPlainClient(t, c)

	type del struct {
		name string
		mech int8
	}
	type up struct {
		name string
		mech int8
		iter int32
	}
	// credentials returns the mechanisms a user has.
	credentials := func(user string) []int8 {
		t.Helper()
		req := kmsg.NewDescribeUserSCRAMCredentialsRequest()
		u := kmsg.NewDescribeUserSCRAMCredentialsRequestUser()
		u.Name = user
		req.Users = append(req.Users, u)
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		var mechs []int8
		for _, ci := range resp.Results[0].CredentialInfos {
			mechs = append(mechs, ci.Mechanism)
		}
		return mechs
	}

	// The cases run in order against one cluster.
	for _, tc := range []struct {
		name  string
		dels  []del
		ups   []up
		want  map[string]*kerr.Error // nil is success
		after map[string][]int8      // mechanisms each user has afterwards
	}{
		{name: "create sha256", ups: []up{{"u1", 1, 4096}}, want: map[string]*kerr.Error{"u1": nil}},
		{name: "create sha512", ups: []up{{"u1", 2, 4096}}, want: map[string]*kerr.Error{"u1": nil}, after: map[string][]int8{"u1": {1, 2}}},
		{name: "empty name", ups: []up{{"", 1, 4096}}, want: map[string]*kerr.Error{"": kerr.UnacceptableCredential}},
		{name: "bad mechanism", ups: []up{{"u", 99, 4096}}, want: map[string]*kerr.Error{"u": kerr.UnsupportedSaslMechanism}},
		{name: "iterations too low", ups: []up{{"u", 1, 100}}, want: map[string]*kerr.Error{"u": kerr.UnacceptableCredential}},
		{name: "iterations too high", ups: []up{{"u", 1, 20000}}, want: map[string]*kerr.Error{"u": kerr.UnacceptableCredential}},
		{name: "delete nonexistent", dels: []del{{"ghost", 1}}, want: map[string]*kerr.Error{"ghost": kerr.ResourceNotFound}},
		{name: "delete bad mechanism", dels: []del{{"ghost", 99}}, want: map[string]*kerr.Error{"ghost": kerr.UnsupportedSaslMechanism}},
		{name: "delete empty name", dels: []del{{"", 1}}, want: map[string]*kerr.Error{"": kerr.UnacceptableCredential}},
		{
			name:  "two deletions of one user delete nothing",
			dels:  []del{{"u1", 1}, {"u1", 2}},
			want:  map[string]*kerr.Error{"u1": kerr.DuplicateResource},
			after: map[string][]int8{"u1": {1, 2}},
		},
		{
			name:  "an invalid upsert fails a valid one after it",
			ups:   []up{{"u2", 1, 1000}, {"u2", 2, 4096}},
			want:  map[string]*kerr.Error{"u2": kerr.UnacceptableCredential},
			after: map[string][]int8{"u2": nil},
		},
		{
			name:  "three upserts of one user store nothing",
			ups:   []up{{"u3", 1, 4096}, {"u3", 2, 4096}, {"u3", 1, 8192}},
			want:  map[string]*kerr.Error{"u3": kerr.DuplicateResource},
			after: map[string][]int8{"u3": nil},
		},
		{
			name: "deleting a missing credential fails the user before its upsert",
			dels: []del{{"u4", 1}},
			ups:  []up{{"u4", 1, 4096}},
			want: map[string]*kerr.Error{"u4": kerr.ResourceNotFound},
		},
		{
			name:  "delete and upsert of one user",
			dels:  []del{{"u1", 1}},
			ups:   []up{{"u1", 2, 8192}},
			want:  map[string]*kerr.Error{"u1": kerr.DuplicateResource},
			after: map[string][]int8{"u1": {1, 2}},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := kmsg.NewAlterUserSCRAMCredentialsRequest()
			for _, d := range tc.dels {
				rd := kmsg.NewAlterUserSCRAMCredentialsRequestDeletion()
				rd.Name, rd.Mechanism = d.name, d.mech
				req.Deletions = append(req.Deletions, rd)
			}
			for _, u := range tc.ups {
				ru := kmsg.NewAlterUserSCRAMCredentialsRequestUpsertion()
				ru.Name, ru.Mechanism, ru.Iterations = u.name, u.mech, u.iter
				ru.Salt = []byte("s")
				ru.SaltedPassword = []byte("p")
				req.Upsertions = append(req.Upsertions, ru)
			}
			resp, err := req.RequestWith(ctx, cl)
			if err != nil {
				t.Fatal(err)
			}
			if len(resp.Results) != len(tc.want) {
				t.Fatalf("got %d results, want one per user, %d", len(resp.Results), len(tc.want))
			}
			for _, r := range resp.Results {
				want, ok := tc.want[r.User]
				if !ok {
					t.Fatalf("unexpected result for user %q", r.User)
				}
				var wantCode int16
				if want != nil {
					wantCode = want.Code
				}
				if r.ErrorCode != wantCode {
					t.Errorf("user %q: got %v, want %v", r.User, kerr.ErrorForCode(r.ErrorCode), want)
				}
			}
			for user, want := range tc.after {
				if got := credentials(user); !slices.Equal(got, want) {
					t.Errorf("user %q has mechanisms %v, want %v", user, got, want)
				}
			}
		})
	}

	// Describe with duplicate user name.
	req := kmsg.NewDescribeUserSCRAMCredentialsRequest()
	for range 2 {
		u := kmsg.NewDescribeUserSCRAMCredentialsRequestUser()
		u.Name = "someone"
		req.Users = append(req.Users, u)
	}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	foundDup := false
	for _, r := range resp.Results {
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

	cl := newPlainClient(t, c)
	adm := kadm.NewClient(cl)

	// As in Kafka, the value in effect is the first synonym, and a broker
	// value or default is named by the broker config.
	synonyms := func(c kadm.Config) (s string) {
		for _, syn := range c.Synonyms {
			v := "<nil>"
			if syn.Value != nil {
				v = *syn.Value
			}
			s += syn.Key + "=" + v + "/" + syn.Source.String() + " "
		}
		return s
	}
	// expectBroker checks broker 0's value and synonyms for each key.
	expectBroker := func(want map[string]string) {
		t.Helper()
		rcs, err := adm.DescribeBrokerConfigs(ctx, 0)
		if err != nil {
			t.Fatal(err)
		}
		for _, rc := range rcs {
			for _, c := range rc.Configs {
				if w, ok := want[c.Key]; ok {
					if got := c.MaybeValue() + " | " + synonyms(c); got != w {
						t.Errorf("broker %s: got %q, want %q", c.Key, got, w)
					}
					delete(want, c.Key)
				}
			}
		}
		for k := range want {
			t.Errorf("broker describe is missing %s", k)
		}
	}
	// Kafka has no default for log.retention.ms or log.roll.ms.
	expectBroker(map[string]string{
		"log.retention.ms": " | ",
		"log.roll.ms":      " | ",
	})

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
	expectBroker(map[string]string{
		"log.retention.ms": "3600000 | log.retention.ms=3600000/DYNAMIC_BROKER_CONFIG log.retention.hours=168/DEFAULT_CONFIG ",
		"log.roll.ms":      " | ",
	})
	// The empty broker name describes only the cluster-wide dynamic
	// configs, not every default.
	for _, rc := range bcfgs {
		for _, c := range rc.Configs {
			if c.Source != kmsg.ConfigSourceDynamicDefaultBrokerConfig {
				t.Errorf("cluster-wide describe lists %s with source %v", c.Key, c.Source)
			}
		}
	}
	// A topic reports the broker value under the topic config's name.
	tcfgs, err := adm.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	for _, rc := range tcfgs {
		for _, c := range rc.Configs {
			switch c.Key {
			case "log.retention.ms":
				t.Error("topic describe lists the broker name log.retention.ms")
			case "retention.ms":
				if c.MaybeValue() != "3600000" {
					t.Errorf("topic retention.ms = %q, want the broker's 3600000", c.MaybeValue())
				}
			}
		}
	}

	// Set topic config.
	req2 := kmsg.NewAlterConfigsRequest()
	rr2 := kmsg.NewAlterConfigsRequestResource()
	rr2.ResourceType = kmsg.ConfigResourceTypeTopic
	rr2.ResourceName = topic
	rc2 := kmsg.NewAlterConfigsRequestResourceConfig()
	rc2.Name = "retention.ms"
	rc2.Value = new("86400000")
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
	tcfgs, err = adm.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		t.Fatal(err)
	}
	found = false
	for _, rc := range tcfgs {
		for _, c := range rc.Configs {
			want := map[string]string{
				"retention.ms":  "retention.ms=86400000/DYNAMIC_TOPIC_CONFIG log.retention.ms=3600000/DYNAMIC_BROKER_CONFIG log.retention.hours=168/DEFAULT_CONFIG ",
				"segment.bytes": "log.segment.bytes=104857600/DEFAULT_CONFIG ",
			}[c.Key]
			if c.Key == "retention.ms" && c.Value != nil && *c.Value == "86400000" {
				found = true
			}
			if got := synonyms(c); want != "" && got != want {
				t.Errorf("%s synonyms: got %q, want %q", c.Key, got, want)
			}
		}
	}
	if !found {
		t.Error("retention.ms=86400000 not found after AlterConfigs on topic")
	}
	// Without IncludeSynonyms, the describe answers no synonyms.
	sreq := kmsg.NewPtrDescribeConfigsRequest()
	sreq.Resources = []kmsg.DescribeConfigsRequestResource{{ResourceType: kmsg.ConfigResourceTypeTopic, ResourceName: topic}}
	sresp, err := sreq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	for _, rc := range sresp.Resources[0].Configs {
		if len(rc.ConfigSynonyms) > 0 {
			t.Errorf("%s has synonyms %+v without IncludeSynonyms", rc.Name, rc.ConfigSynonyms)
		}
	}

	// Unknown topic -> error.
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
	// Describing an invalid topic name is INVALID_TOPIC_EXCEPTION.
	dreq := kmsg.NewPtrDescribeConfigsRequest()
	for _, name := range []string{"", "bad/name", "nonexistent"} {
		dreq.Resources = append(dreq.Resources, kmsg.DescribeConfigsRequestResource{ResourceType: kmsg.ConfigResourceTypeTopic, ResourceName: name})
	}
	dresp, err := dreq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	for i, want := range []int16{kerr.InvalidTopicException.Code, kerr.InvalidTopicException.Code, kerr.UnknownTopicOrPartition.Code} {
		if got := dresp.Resources[i].ErrorCode; got != want {
			t.Errorf("describe %q: got %v, want %v", dresp.Resources[i].ResourceName, kerr.ErrorForCode(got), kerr.ErrorForCode(want))
		}
	}

	// A known topic config must parse as its type, and a null value or a
	// resource listed twice is refused before anything applies.
	topicReq := func(value *string) kmsg.AlterConfigsRequestResource {
		rr := kmsg.NewAlterConfigsRequestResource()
		rr.ResourceType = kmsg.ConfigResourceTypeTopic
		rr.ResourceName = topic
		rc := kmsg.NewAlterConfigsRequestResourceConfig()
		rc.Name = "retention.ms"
		rc.Value = value
		rr.Configs = append(rr.Configs, rc)
		return rr
	}
	oneConfig := func(typ kmsg.ConfigResourceType, name, key, value string) kmsg.AlterConfigsRequestResource {
		rr := topicReq(&value)
		rr.ResourceType, rr.ResourceName, rr.Configs[0].Name = typ, name, key
		return rr
	}
	for _, step := range []struct {
		name      string
		resources []kmsg.AlterConfigsRequestResource
		want      []int16
	}{
		{"bad value", []kmsg.AlterConfigsRequestResource{topicReq(new("abc"))}, []int16{kerr.InvalidConfig.Code}},
		{"null value", []kmsg.AlterConfigsRequestResource{topicReq(nil)}, []int16{kerr.InvalidRequest.Code}},
		{"resource twice", []kmsg.AlterConfigsRequestResource{topicReq(new("1")), topicReq(new("1"))}, []int16{kerr.InvalidRequest.Code, kerr.InvalidRequest.Code}},
		{"empty topic", []kmsg.AlterConfigsRequestResource{func() kmsg.AlterConfigsRequestResource {
			rr := topicReq(new("1"))
			rr.ResourceName = ""
			return rr
		}()}, []int16{kerr.InvalidRequest.Code}},
		// The configs replace the group's and the subscription's.
		{"group", []kmsg.AlterConfigsRequestResource{oneConfig(kmsg.ConfigResourceTypeGroupConfig, "g", "share.auto.offset.reset", "earliest")}, []int16{0}},
		{"group replaced", []kmsg.AlterConfigsRequestResource{oneConfig(kmsg.ConfigResourceTypeGroupConfig, "g", "share.isolation.level", "read_committed")}, []int16{0}},
		{"bad group config", []kmsg.AlterConfigsRequestResource{oneConfig(kmsg.ConfigResourceTypeGroupConfig, "g", "share.isolation.level", "x")}, []int16{kerr.InvalidConfig.Code}},
		{"client metrics", []kmsg.AlterConfigsRequestResource{oneConfig(kmsg.ConfigResourceTypeClientMetrics, "sub", "metrics", "a")}, []int16{0}},
		{"client metrics replaced", []kmsg.AlterConfigsRequestResource{oneConfig(kmsg.ConfigResourceTypeClientMetrics, "sub", "interval.ms", "10000")}, []int16{0}},
	} {
		req := kmsg.NewAlterConfigsRequest()
		req.Resources = step.resources
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		var got []int16
		for _, r := range resp.Resources {
			got = append(got, r.ErrorCode)
		}
		if !slices.Equal(got, step.want) {
			t.Errorf("%s: got %v, want %v", step.name, got, step.want)
		}
	}
	c.admin(func() {
		if g := c.groupConfigs["g"]; len(g) != 1 || g["share.isolation.level"] == nil || *g["share.isolation.level"] != "read_committed" {
			t.Errorf("group configs %v, want only share.isolation.level=read_committed", g)
		}
		if sub := c.clientMetrics["sub"]; len(sub) != 1 || sub["interval.ms"] == nil || *sub["interval.ms"] != "10000" {
			t.Errorf("subscription %v, want only interval.ms=10000", sub)
		}
	})

	// NewCluster refuses a broker config that AlterConfigs refuses.
	for k, v := range map[string]string{"log.retention.ms": "abc", "offsets.retention.minutes": "0"} {
		if bad, err := NewCluster(NumBrokers(1), BrokerConfigs(map[string]string{k: v})); err == nil {
			bad.Close()
			t.Errorf("NewCluster accepted %s=%s", k, v)
		}
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
	rc5.Value = new("999")
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
	c := newCoverCluster(t, NumBrokers(1), SeedTopics(1, topic), BrokerConfigs(map[string]string{
		"group.consumer.heartbeat.interval.ms": "8000",
		"group.share.delivery.count.limit":     "7",
	}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cl := newPlainClient(t, c)
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
	rc3.Value = new("172800000")
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
	rc6.Value = new("111")
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

	// Group configs: an unset key describes at its effective value, and
	// a group whose last config is deleted is no longer listed.
	const group = "inc-alter-group"
	alterGroup := func(name string, op kmsg.IncrementalAlterConfigOp, value *string) {
		t.Helper()
		req := kmsg.NewIncrementalAlterConfigsRequest()
		rr := kmsg.NewIncrementalAlterConfigsRequestResource()
		rr.ResourceType = kmsg.ConfigResourceTypeGroupConfig
		rr.ResourceName = group
		rc := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
		rc.Name = name
		rc.Value = value
		rc.Op = op
		rr.Configs = append(rr.Configs, rc)
		req.Resources = append(req.Resources, rr)
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		if resp.Resources[0].ErrorCode != 0 {
			t.Fatal(kerr.ErrorForCode(resp.Resources[0].ErrorCode))
		}
	}
	listsGroup := func() bool {
		t.Helper()
		listed, err := kadm.NewClient(cl).ListConfigResources(ctx, kadm.ConfigResourceGroupConfig)
		if err != nil {
			t.Fatal(err)
		}
		return slices.Contains(listed.Names(), group)
	}
	alterGroup("share.isolation.level", kmsg.IncrementalAlterConfigOpSet, new("read_committed"))
	alterGroup("consumer.heartbeat.interval.ms", kmsg.IncrementalAlterConfigOpSet, new("6000"))
	dreq := kmsg.NewPtrDescribeConfigsRequest()
	dreq.IncludeSynonyms = true
	drr := kmsg.NewDescribeConfigsRequestResource()
	drr.ResourceType = kmsg.ConfigResourceTypeGroupConfig
	drr.ResourceName = group
	drr.ConfigNames = []string{} // empty, like null, describes every key
	dreq.Resources = append(dreq.Resources, drr)
	dresp, err := dreq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	got := make(map[string]kmsg.DescribeConfigsResponseResourceConfig)
	for _, rc := range dresp.Resources[0].Configs {
		got[rc.Name] = rc
	}
	// As in Kafka, a key with a broker config lists the broker's value
	// and default under the broker config's name.
	for k, want := range map[string]struct {
		value    string
		source   kmsg.ConfigSource
		synonyms string
	}{
		"share.isolation.level":          {"read_committed", kmsg.ConfigSourceGroupConfig, "share.isolation.level=read_committed/GROUP_CONFIG share.isolation.level=read_uncommitted/DEFAULT_CONFIG "},
		"consumer.heartbeat.interval.ms": {"6000", kmsg.ConfigSourceGroupConfig, "consumer.heartbeat.interval.ms=6000/GROUP_CONFIG group.consumer.heartbeat.interval.ms=8000/DYNAMIC_BROKER_CONFIG group.consumer.heartbeat.interval.ms=5000/DEFAULT_CONFIG "},
		"share.delivery.count.limit":     {"7", kmsg.ConfigSourceDynamicBrokerConfig, "group.share.delivery.count.limit=7/DYNAMIC_BROKER_CONFIG group.share.delivery.count.limit=5/DEFAULT_CONFIG "},
		"consumer.session.timeout.ms":    {"45000", kmsg.ConfigSourceDefaultConfig, "group.consumer.session.timeout.ms=45000/DEFAULT_CONFIG "},
		"share.auto.offset.reset":        {"latest", kmsg.ConfigSourceDefaultConfig, "share.auto.offset.reset=latest/DEFAULT_CONFIG "},
	} {
		rc := got[k]
		v := "<nil>"
		if rc.Value != nil {
			v = *rc.Value
		}
		var syns string
		for _, syn := range rc.ConfigSynonyms {
			sv := "<nil>"
			if syn.Value != nil {
				sv = *syn.Value
			}
			syns += syn.Name + "=" + sv + "/" + syn.Source.String() + " "
		}
		if v != want.value || rc.Source != want.source || syns != want.synonyms {
			t.Errorf("group %s: got %q (%v) %q, want %q (%v) %q", k, v, rc.Source, syns, want.value, want.source, want.synonyms)
		}
	}
	if !listsGroup() {
		t.Error("configured group is not listed")
	}
	alterGroup("consumer.heartbeat.interval.ms", kmsg.IncrementalAlterConfigOpDelete, nil)
	alterGroup("share.isolation.level", kmsg.IncrementalAlterConfigOpDelete, nil)
	if listsGroup() {
		t.Error("group with no configs is still listed")
	}
}

// TestCreateTopicsEdgeCases covers various CreateTopics error paths and features.
func TestCreateTopicsEdgeCases(t *testing.T) {
	t.Parallel()
	c := newCoverCluster(t, NumBrokers(2), SeedTopics(1, "existing-topic"),
		DefaultNumPartitions(7), BrokerConfigs(map[string]string{"log.segment.bytes": "1048576"}))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cl := newPlainClient(t, c)
	adm := kadm.NewClient(cl)

	// Duplicate topic names in request: only the duplicate fails. Of two
	// names that differ only in . and _, the later one fails.
	req := kmsg.NewCreateTopicsRequest()
	for _, name := range []string{"dup_x", "dup.x", "dup.x", "dup-other", "y.z", "y_z"} {
		rt := kmsg.NewCreateTopicsRequestTopic()
		rt.Topic = name
		rt.NumPartitions = 1
		rt.ReplicationFactor = 1
		req.Topics = append(req.Topics, rt)
	}
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	for _, st := range resp.Topics {
		var want int16
		switch st.Topic {
		case "dup.x":
			want = kerr.InvalidRequest.Code
		case "y_z":
			want = kerr.InvalidTopicException.Code
			if st.ErrorMessage == nil || *st.ErrorMessage != "Topic 'y_z' collides with existing topic: y.z" {
				t.Errorf("dup: y_z: got message %v, want Kafka's collision message", st.ErrorMessage)
			}
		}
		if st.ErrorCode != want {
			t.Errorf("dup: %s: expected %v, got %v", st.Topic, kerr.ErrorForCode(want), kerr.ErrorForCode(st.ErrorCode))
		}
	}
	if len(resp.Topics) != 5 {
		t.Errorf("dup: expected one answer per name, got %d", len(resp.Topics))
	}

	// Already exists.
	_, err = adm.CreateTopics(ctx, 1, 1, nil, "existing-topic")
	if err != nil {
		t.Fatal(err)
	}

	// Requests that fail and create nothing.
	assign := func(replicas ...[]int32) func(*kmsg.CreateTopicsRequest, *kmsg.CreateTopicsRequestTopic) {
		return func(_ *kmsg.CreateTopicsRequest, rt *kmsg.CreateTopicsRequestTopic) {
			rt.NumPartitions, rt.ReplicationFactor = -1, -1
			for i, rs := range replicas {
				ra := kmsg.NewCreateTopicsRequestTopicReplicaAssignment()
				ra.Partition = int32(i)
				ra.Replicas = rs
				rt.ReplicaAssignment = append(rt.ReplicaAssignment, ra)
			}
		}
	}
	for _, tc := range []struct {
		name string
		mod  func(*kmsg.CreateTopicsRequest, *kmsg.CreateTopicsRequestTopic)
		want int16
	}{
		{"replication factor too high", func(_ *kmsg.CreateTopicsRequest, rt *kmsg.CreateTopicsRequestTopic) { rt.ReplicationFactor = 10 }, kerr.InvalidReplicationFactor.Code},
		{"replication factor zero", func(_ *kmsg.CreateTopicsRequest, rt *kmsg.CreateTopicsRequestTopic) { rt.ReplicationFactor = 0 }, kerr.InvalidReplicationFactor.Code},
		{"zero partitions", func(_ *kmsg.CreateTopicsRequest, rt *kmsg.CreateTopicsRequestTopic) { rt.NumPartitions = 0 }, kerr.InvalidPartitions.Code},
		{"negative partitions", func(_ *kmsg.CreateTopicsRequest, rt *kmsg.CreateTopicsRequestTopic) { rt.NumPartitions = -2 }, kerr.InvalidPartitions.Code},
		{"too many partitions", func(_ *kmsg.CreateTopicsRequest, rt *kmsg.CreateTopicsRequestTopic) { rt.NumPartitions = 10001 }, kerr.PolicyViolation.Code},
		{"invalid name", func(_ *kmsg.CreateTopicsRequest, rt *kmsg.CreateTopicsRequestTopic) { rt.Topic = "bad name" }, kerr.InvalidTopicException.Code},
		{"timeout already passed", func(req *kmsg.CreateTopicsRequest, _ *kmsg.CreateTopicsRequestTopic) { req.TimeoutMillis = 0 }, kerr.RequestTimedOut.Code},
		{"assignment with a broker twice", assign([]int32{0, 0}), kerr.InvalidReplicaAssignment.Code},
		{"assignment with an unknown broker", assign([]int32{9}), kerr.InvalidReplicaAssignment.Code},
		{"assignment with uneven replica counts", assign([]int32{0}, []int32{0, 1}), kerr.InvalidReplicaAssignment.Code},
		{"ReplicaAssignment with NumPartitions != -1", func(req *kmsg.CreateTopicsRequest, rt *kmsg.CreateTopicsRequestTopic) {
			assign([]int32{0})(req, rt)
			rt.NumPartitions = 3
		}, kerr.InvalidRequest.Code},
	} {
		req := kmsg.NewCreateTopicsRequest()
		rt := kmsg.NewCreateTopicsRequestTopic()
		rt.Topic = "bad-topic"
		rt.NumPartitions = 1
		rt.ReplicationFactor = 1
		tc.mod(&req, &rt)
		req.Topics = append(req.Topics, rt)
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		if got := resp.Topics[0].ErrorCode; got != tc.want {
			t.Errorf("%s: expected %v, got %v", tc.name, kerr.ErrorForCode(tc.want), kerr.ErrorForCode(got))
		}
		if ti := c.TopicInfo(rt.Topic); ti != nil {
			t.Errorf("%s: topic was created", tc.name)
		}
	}
	// Cluster.CreateTopic, which Metadata's auto-creation shares, refuses
	// more than 10000 partitions too, and
	// controller.max.records.per.batch changes the limit.
	if c.CreateTopic("huge", 10001, nil) == nil {
		t.Error("CreateTopic created a topic with 10001 partitions")
	}
	small := newCoverCluster(t, NumBrokers(1), BrokerConfigs(map[string]string{"controller.max.records.per.batch": "2"}))
	if err := small.CreateTopic("two", 2, nil); err != nil {
		t.Errorf("CreateTopic with 2 partitions at a limit of 2: %v", err)
	}
	if small.CreateTopic("three", 3, nil) == nil {
		t.Error("CreateTopic created 3 partitions at a limit of 2")
	}
	// Seeded a.b and a_b collide, and a_b still refuses a new a.b after
	// a.b is deleted.
	seeded := newCoverCluster(t, NumBrokers(1), SeedTopics(1, "a.b", "a_b"))
	if err := seeded.DeleteTopic("a.b"); err != nil {
		t.Fatal(err)
	}
	if err := seeded.CreateTopic("a.b", 1, nil); err == nil {
		t.Error("CreateTopic created a.b next to a_b")
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

	// With ReplicaAssignment: the first replica leads.
	req5 := kmsg.NewCreateTopicsRequest()
	rt5 := kmsg.NewCreateTopicsRequestTopic()
	rt5.Topic = "assigned"
	assign([]int32{1}, []int32{0})(&req5, &rt5)
	req5.Topics = append(req5.Topics, rt5)
	resp5, err := req5.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp5.Topics[0].ErrorCode != 0 {
		t.Fatal(kerr.ErrorForCode(resp5.Topics[0].ErrorCode))
	}
	for p, want := range []int32{1, 0} {
		if got := c.LeaderFor("assigned", int32(p)); got != want {
			t.Errorf("assigned partition %d: expected leader %d, got %d", p, want, got)
		}
	}

	// ValidateOnly: the topic should not be created.
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
	cfg.Value = new("86400000")
	rt8.Configs = append(rt8.Configs, cfg)
	req8.Topics = append(req8.Topics, rt8)
	resp8, err := req8.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if resp8.Topics[0].ErrorCode != 0 {
		t.Fatal(kerr.ErrorForCode(resp8.Topics[0].ErrorCode))
	}
	// The response has every config, sorted, not only the ones we set.
	rcfgs := resp8.Topics[0].Configs
	if !slices.IsSortedFunc(rcfgs, func(l, r kmsg.CreateTopicsResponseTopicConfig) int { return strings.Compare(l.Name, r.Name) }) {
		t.Error("response configs are not sorted")
	}
	srcs := make(map[string]kmsg.ConfigSource)
	for _, rc := range rcfgs {
		srcs[rc.Name] = kmsg.ConfigSource(rc.Source)
	}
	if srcs["retention.ms"] != kmsg.ConfigSourceDynamicTopicConfig || srcs["cleanup.policy"] != kmsg.ConfigSourceDefaultConfig {
		t.Errorf("response config sources: retention.ms %v, cleanup.policy %v", srcs["retention.ms"], srcs["cleanup.policy"])
	}
	// A broker config shows under the topic config's name.
	if _, ok := srcs["log.segment.bytes"]; ok || srcs["segment.bytes"] != kmsg.ConfigSourceDynamicBrokerConfig {
		t.Errorf("response lists log.segment.bytes %v, segment.bytes from %v", ok, srcs["segment.bytes"])
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

	// A known config must parse as its type and must not be null; a name
	// we do not know is accepted.
	for _, step := range []struct {
		topic string
		name  string
		value *string
		want  int16
	}{
		{"bad-config", "retention.ms", new("abc"), kerr.InvalidConfig.Code},
		{"null-config", "retention.ms", nil, kerr.InvalidConfig.Code},
		{"custom-config", "custom.thing", new("abc"), 0},
	} {
		req := kmsg.NewCreateTopicsRequest()
		rt := kmsg.NewCreateTopicsRequestTopic()
		rt.Topic = step.topic
		rt.NumPartitions = 1
		rt.ReplicationFactor = 1
		cfg := kmsg.NewCreateTopicsRequestTopicConfig()
		cfg.Name = step.name
		cfg.Value = step.value
		rt.Configs = append(rt.Configs, cfg)
		req.Topics = append(req.Topics, rt)
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		if got := resp.Topics[0].ErrorCode; got != step.want {
			t.Errorf("%s: got %v, want %v", step.topic, kerr.ErrorForCode(got), kerr.ErrorForCode(step.want))
		}
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

	cl := newPlainClient(t, c)

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

	// Entries Kafka rejects one by one, deleting nothing: a duplicated
	// name or ID, both or neither, an unknown ID, and a name that maps to
	// an ID the request also lists.
	entry := func(name *string, id [16]byte) kmsg.DeleteTopicsRequestTopic {
		dt := kmsg.NewDeleteTopicsRequestTopic()
		dt.Topic, dt.TopicID = name, id
		return dt
	}
	unknownID := [16]byte{1}
	for _, entries := range [][]kmsg.DeleteTopicsRequestTopic{
		{entry(&topic, [16]byte{}), entry(&topic, [16]byte{}), entry(nil, topicID), entry(nil, topicID), entry(&topic, topicID), entry(nil, [16]byte{}), entry(nil, unknownID)},
		{entry(&topic, [16]byte{}), entry(nil, topicID)},
	} {
		req := kmsg.NewDeleteTopicsRequest()
		req.Topics = entries
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		for _, st := range resp.Topics {
			want := kerr.InvalidRequest.Code
			if st.TopicID == unknownID {
				want = kerr.UnknownTopicID.Code
				if st.Topic != nil {
					t.Errorf("unknown ID answered with name %q", *st.Topic)
				}
			}
			if st.ErrorCode != want {
				t.Errorf("entry %v %x: expected %v, got %v", st.Topic, st.TopicID, kerr.ErrorForCode(want), kerr.ErrorForCode(st.ErrorCode))
			}
		}
		if c.TopicInfo(topic) == nil {
			t.Fatal("rejected entries deleted the topic")
		}
	}
	// With no name or ID to look up, a request past its deadline is
	// answered entry by entry.
	dup := kmsg.NewPtrDeleteTopicsRequest()
	dup.TimeoutMillis = 0
	dup.Topics = []kmsg.DeleteTopicsRequestTopic{entry(&topic, [16]byte{}), entry(&topic, [16]byte{})}
	if dr := faultReq(t, cl, 0, dup).(*kmsg.DeleteTopicsResponse); len(dr.Topics) != 1 || dr.Topics[0].ErrorCode != kerr.InvalidRequest.Code {
		t.Errorf("duplicates past the deadline: got %+v, want one INVALID_REQUEST", dr.Topics)
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

// TestDescribeProducers verifies that every replica
// answers DescribeProducers, that a producer stays listed after its
// transaction ends, that its last timestamp is its last batch's, that the
// transaction's start offset shows only while the transaction is open on the
// partition: a WriteTxnMarkers marker ends it there, and a marker below the
// producer's epoch is rejected, and that an idempotent producer expires
// producer.id.expiration.ms after its last batch.
func TestDescribeProducers(t *testing.T) {
	t.Parallel()
	topic := "describe-producers"
	c := newCoverCluster(t, NumBrokers(3), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	producer := newPlainClient(t, c,
		kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID("desc-prod-txn"),
	)
	cl := newPlainClient(t, c)

	// describe asks each of the 3 replicas and wants one producer with
	// the given transaction start offset from each.
	describe := func(wantStart int64) kmsg.DescribeProducersResponseTopicPartitionActiveProducer {
		t.Helper()
		var ap kmsg.DescribeProducersResponseTopicPartitionActiveProducer
		for node := range 3 {
			req := kmsg.NewPtrDescribeProducersRequest()
			rt := kmsg.NewDescribeProducersRequestTopic()
			rt.Topic = topic
			rt.Partitions = []int32{0}
			req.Topics = append(req.Topics, rt)
			kresp, err := cl.Broker(node).Request(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			sp := kresp.(*kmsg.DescribeProducersResponse).Topics[0].Partitions[0]
			if err := kerr.ErrorForCode(sp.ErrorCode); err != nil {
				t.Fatalf("broker %d: %v", node, err)
			}
			if len(sp.ActiveProducers) != 1 || sp.ActiveProducers[0].CurrentTxnStartOffset != wantStart {
				t.Fatalf("broker %d: got producers %+v, want one with transaction start %d", node, sp.ActiveProducers, wantStart)
			}
			ap = sp.ActiveProducers[0]
		}
		return ap
	}

	if err := producer.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := producer.ProduceSync(ctx, &kgo.Record{Value: []byte("txn-data"), Timestamp: time.UnixMilli(1000)}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	first := describe(0)
	if first.LastTimestamp != 1000 {
		t.Fatalf("got last timestamp %d, want the batch's 1000", first.LastTimestamp)
	}
	if err := producer.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}
	// The v5 commit's marker carries the bumped epoch.
	if ap := describe(-1); ap.ProducerEpoch != first.ProducerEpoch+1 || ap.LastSequence != -1 {
		t.Fatalf("after the commit: got epoch %d and last sequence %d, want %d and -1", ap.ProducerEpoch, ap.LastSequence, first.ProducerEpoch+1)
	}

	// The second transaction is at a bumped epoch (KIP-890). Its records
	// start after the first record and its commit marker.
	if err := producer.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := producer.ProduceSync(ctx, kgo.StringRecord("txn-data")).FirstErr(); err != nil {
		t.Fatal(err)
	}
	ap := describe(2)
	adm := kadm.NewClient(cl)
	for _, m := range []struct {
		epoch   int16
		wantErr error
	}{
		{int16(ap.ProducerEpoch) - 1, kerr.InvalidProducerEpoch},
		{int16(ap.ProducerEpoch), nil},
	} {
		written, err := adm.WriteTxnMarkers(ctx, kadm.TxnMarkers{
			ProducerID:    ap.ProducerID,
			ProducerEpoch: m.epoch,
			Topics:        kadm.TopicsSet{topic: {0: {}}},
		})
		if err != nil {
			t.Fatal(err)
		}
		written.EachPartition(func(p kadm.TxnMarkersPartitionResponse) {
			if !errors.Is(p.Err, m.wantErr) {
				t.Fatalf("marker at epoch %d: got %v, want %v", m.epoch, p.Err, m.wantErr)
			}
		})
	}
	describe(-1)
	if err := producer.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatal(err)
	}

	idem := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
	if err := idem.ProduceSync(ctx, &kgo.Record{Value: []byte("idem"), Timestamp: time.UnixMilli(1000)}).FirstErr(); err != nil {
		t.Fatal(err)
	}
	c.admin(func() {
		c.bcfgs["producer.id.expiration.ms"] = new("1000")
		c.pids.expireProducers()
		if len(c.pids.ids) != 1 || c.pids.byTxid["desc-prod-txn"] == nil {
			t.Errorf("after the idempotent producer expired: got %d producers, want only the transactional one", len(c.pids.ids))
		}
	})
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
	txnCl := newPlainClient(t, c,
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
	consumer := newPlainClient(t, c,
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
// ongoing transactions by state filter, that only the coordinator lists a
// transactional ID, and that a pattern matches the whole ID.
func TestListTransactionsFindsOngoing(t *testing.T) {
	t.Parallel()
	topic := "list-txn-ongoing"
	c := newCoverCluster(t, NumBrokers(3), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	txnID := "list-txn-ongoing-id"
	producer := newPlainClient(t, c,
		kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID(txnID),
	)
	if err := producer.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := producer.ProduceSync(ctx, kgo.StringRecord("v")).FirstErr(); err != nil {
		t.Fatal(err)
	}

	cl := newPlainClient(t, c)
	adm := kadm.NewClient(cl)

	// Unfiltered list.
	listed, err := adm.ListTransactions(ctx, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if tx, ok := listed[txnID]; !ok {
		t.Error("transaction not found in unfiltered list")
	} else if want := c.CoordinatorFor(txnID); tx.Coordinator != want {
		t.Errorf("listed by broker %d, want the coordinator %d", tx.Coordinator, want)
	}

	// Each broker lists only what it coordinates. An unknown state name
	// matches nothing and comes back.
	var n int
	for node := range 3 {
		req := kmsg.NewPtrListTransactionsRequest()
		req.StateFilters = []string{"Ongoing", "ongoing"}
		resp, err := req.RequestWith(ctx, cl.Broker(node))
		if err != nil {
			t.Fatal(err)
		}
		n += len(resp.TransactionStates)
		if !slices.Equal(resp.UnknownStateFilters, []string{"ongoing"}) {
			t.Errorf("broker %d: unknown state filters %v, want [ongoing]", node, resp.UnknownStateFilters)
		}
	}
	if n != 1 {
		t.Errorf("brokers listed the transaction %d times, want 1", n)
	}

	// Filtered by state.
	listed2, err := adm.ListTransactions(ctx, nil, []string{"Ongoing"})
	if err != nil {
		t.Fatal(err)
	}
	if tx := listed2[txnID]; tx.State != "Ongoing" {
		t.Errorf("Ongoing filter: got %+v for the transaction, want it listed as Ongoing", tx)
	}

	// A pattern matches the whole ID.
	for pattern, want := range map[string]bool{"list-txn": false, "list-txn.*": true} {
		listed, err := adm.ListTransactionsByTxPattern(ctx, nil, nil, pattern)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := listed[txnID]; ok != want {
			t.Errorf("pattern %q: listed %v, want %v", pattern, ok, want)
		}
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
	cl := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
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

// TestOffsetForLeaderEpochLogEnd verifies that an epoch with no later batch
// ends at the high watermark, with the last batch's epoch, or the requested
// epoch if the log is empty.
func TestOffsetForLeaderEpochLogEnd(t *testing.T) {
	t.Parallel()
	const topic = "ofle-log-end"
	for _, test := range []struct {
		name      string
		produce   int
		trim      bool // delete every record before bumping
		bumps     int
		epoch     int32
		wantEpoch int32
		wantEnd   int64
	}{
		{name: "current epoch", produce: 5, wantEnd: 5},
		{name: "newest logged epoch", produce: 5, bumps: 1, wantEnd: 5},
		{name: "epoch with no batches", produce: 5, bumps: 2, epoch: 1, wantEnd: 5},
		{name: "empty", bumps: 1},
		{name: "trimmed", produce: 5, trim: true, bumps: 1, wantEnd: 5},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			c := newCoverCluster(t, NumBrokers(1), SeedTopics(1, topic))
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			produceN(t, c, topic, test.produce)
			adm := kadm.NewClient(newPlainClient(t, c))
			if test.trim {
				offsets := kadm.Offsets{}
				offsets.Add(kadm.Offset{Topic: topic, Partition: 0, At: int64(test.produce)})
				dresp, err := adm.DeleteRecords(ctx, offsets)
				if err != nil {
					t.Fatal(err)
				}
				if err := dresp.Error(); err != nil {
					t.Fatal(err)
				}
			}
			for range test.bumps {
				if err := c.MoveTopicPartition(topic, 0, 0); err != nil {
					t.Fatal(err)
				}
			}

			var req kadm.OffsetForLeaderEpochRequest
			req.Add(topic, 0, test.epoch)
			resp, err := adm.OffsetForLeaderEpoch(ctx, req)
			if err != nil {
				t.Fatal(err)
			}
			r := resp[topic][0]
			if r.Err != nil {
				t.Fatal(r.Err)
			}
			if r.LeaderEpoch != test.wantEpoch || r.EndOffset != test.wantEnd {
				t.Errorf("got epoch %d end offset %d, want %d %d", r.LeaderEpoch, r.EndOffset, test.wantEpoch, test.wantEnd)
			}
		})
	}
}

// TestOffsetForLeaderEpochAfterLeaderChange verifies that after a leader
// change (epoch bump), querying for the old epoch returns the offset boundary
// where the epoch changed, at v2 as well, the oldest version Kafka 4
// serves. The old leader answers a stale current epoch FENCED_LEADER_EPOCH:
// the epoch is checked before leadership.
func TestOffsetForLeaderEpochAfterLeaderChange(t *testing.T) {
	t.Parallel()
	topic := "ofle-leader-change"
	c := newCoverCluster(t, NumBrokers(2), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Produce 3 records at epoch 0.
	origLeader := c.LeaderFor(topic, 0)
	cl := newPlainClient(t, c, kgo.DefaultProduceTopic(topic), kgo.RecordPartitioner(kgo.ManualPartitioner()))
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
	cl2 := newPlainClient(t, c, kgo.DefaultProduceTopic(topic), kgo.RecordPartitioner(kgo.ManualPartitioner()))
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

	v2 := kversion.Stable()
	v2.SetMaxKeyVersion(int16(kmsg.OffsetForLeaderEpoch), 2)
	for _, cl := range []*kgo.Client{cl2, newPlainClient(t, c, kgo.MaxVersions(v2))} {
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		sp := resp.Topics[0].Partitions[0]
		if sp.ErrorCode != 0 {
			t.Fatal(kerr.ErrorForCode(sp.ErrorCode))
		}
		if sp.EndOffset != 3 {
			t.Errorf("v%d: expected EndOffset=3 (boundary), got %d", resp.Version, sp.EndOffset)
		}
		if sp.LeaderEpoch != 0 {
			t.Errorf("v%d: expected LeaderEpoch=0, got %d", resp.Version, sp.LeaderEpoch)
		}
	}

	req.Topics[0].Partitions[0].CurrentLeaderEpoch = 0
	kresp, err := cl2.Broker(int(origLeader)).Request(ctx, &req)
	if err != nil {
		t.Fatal(err)
	}
	if err := kerr.ErrorForCode(kresp.(*kmsg.OffsetForLeaderEpochResponse).Topics[0].Partitions[0].ErrorCode); err != kerr.FencedLeaderEpoch {
		t.Errorf("stale epoch at the old leader: got %v, want FENCED_LEADER_EPOCH", err)
	}
}

// TestDeleteTopicsByName verifies that DeleteTopics by name removes the topic.
func TestDeleteTopicsByName(t *testing.T) {
	t.Parallel()
	topic := "delete-by-name"
	c := newCoverCluster(t, NumBrokers(1), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	adm := kadm.NewClient(newPlainClient(t, c))
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

	adm := kadm.NewClient(newPlainClient(t, c))
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

	cl := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
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
	cl := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
	for range 3 {
		if err := cl.ProduceSync(ctx, kgo.StringRecord("non-txn")).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	// Begin a transaction and produce 2 more.
	txnCl := newPlainClient(t, c,
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
// transaction are discarded when the transaction is aborted, or when an
// OffsetDelete of the partition comes before the commit, and that a
// regular commit after a staged one keeps its offset when the transaction
// commits.
func TestTxnAbortDiscardsOffsets(t *testing.T) {
	t.Parallel()
	topic := "txn-abort-offsets"
	c := newCoverCluster(t, NumBrokers(1), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Produce some records so there's something to "consume".
	cl := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
	for range 5 {
		if err := cl.ProduceSync(ctx, kgo.StringRecord("v")).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	// Use raw protocol for the full transactional offset commit flow.
	rawCl := newPlainClient(t, c)

	for _, tc := range []struct {
		name         string
		offsetDelete bool // delete the partition's offset, then commit
	}{
		{name: "abort"},
		{name: "offset-delete", offsetDelete: true},
	} {
		group := "txn-abort-group-" + tc.name
		txnID := "txn-abort-offsets-id-" + tc.name

		// The group must exist for OffsetDelete: an admin commit
		// creates it.
		if tc.offsetDelete {
			if _, err := kadm.NewClient(rawCl).CommitOffsets(ctx, group, kadm.OffsetsFromRecords(kgo.Record{Topic: topic, Offset: 1})); err != nil {
				t.Fatalf("%s: admin commit: %v", tc.name, err)
			}
		}

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

		// TxnOffsetCommit: stage offset 5 for the group.
		commitReq := kmsg.NewTxnOffsetCommitRequest()
		commitReq.TransactionalID = txnID
		commitReq.Group = group
		commitReq.ProducerID = pid
		commitReq.ProducerEpoch = epoch
		commitReq.Generation = -1
		ct := kmsg.NewTxnOffsetCommitRequestTopic()
		ct.Topic, ct.TopicID = topic, c.TopicInfo(topic).TopicID
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

		if tc.offsetDelete {
			del := kmsg.NewPtrOffsetDeleteRequest()
			del.Group = group
			dt := kmsg.NewOffsetDeleteRequestTopic()
			dt.Topic = topic
			dt.Partitions = append(dt.Partitions, kmsg.NewOffsetDeleteRequestTopicPartition())
			del.Topics = append(del.Topics, dt)
			delResp, err := del.RequestWith(ctx, rawCl)
			if err != nil {
				t.Fatal(err)
			}
			if delResp.ErrorCode != 0 || delResp.Topics[0].Partitions[0].ErrorCode != 0 {
				t.Fatalf("%s: offset delete: %+v", tc.name, delResp)
			}
		}

		// End the transaction: abort, or commit after the delete.
		endReq := kmsg.NewEndTxnRequest()
		endReq.TransactionalID = txnID
		endReq.ProducerID = pid
		endReq.ProducerEpoch = epoch
		endReq.Commit = tc.offsetDelete
		endResp, err := endReq.RequestWith(ctx, rawCl)
		if err != nil {
			t.Fatal(err)
		}
		if endResp.ErrorCode != 0 {
			t.Fatal(kerr.ErrorForCode(endResp.ErrorCode))
		}

		// Verify offsets were NOT committed to the group. Without the
		// delete, the abort discarded the staged offset.
		if o, ok := groupCommits(c, group)[topic][0]; ok {
			t.Errorf("%s: expected no committed offset, got %d", tc.name, o.Offset)
		}
		if tc.offsetDelete {
			continue
		}

		// After the abort, the next transaction stages offset 3
		// (TxnOffsetCommit v5+ adds the group), then a regular commit
		// sets 4, then the transaction commits: the newer regular
		// commit stays.
		commitReq.ProducerEpoch = endResp.ProducerEpoch
		commitReq.Topics[0].Partitions[0].Offset = 3
		if commitResp, err = commitReq.RequestWith(ctx, rawCl); err != nil {
			t.Fatal(err)
		}
		if code := commitResp.Topics[0].Partitions[0].ErrorCode; code != 0 {
			t.Fatal(kerr.ErrorForCode(code))
		}
		var regular kadm.Offsets
		regular.Add(kadm.Offset{Topic: topic, Partition: 0, At: 4, LeaderEpoch: -1})
		if err := kadm.NewClient(rawCl).CommitAllOffsets(ctx, group, regular); err != nil {
			t.Fatal(err)
		}
		endReq.ProducerEpoch = endResp.ProducerEpoch
		endReq.Commit = true
		if endResp, err = endReq.RequestWith(ctx, rawCl); err != nil {
			t.Fatal(err)
		}
		if endResp.ErrorCode != 0 {
			t.Fatal(kerr.ErrorForCode(endResp.ErrorCode))
		}
		if o := groupCommits(c, group)[topic][0]; o.Offset != 4 {
			t.Errorf("expected the regular commit's offset 4 after the transaction commits, got %d", o.Offset)
		}
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
	cl := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
	for range 3 {
		if err := cl.ProduceSync(ctx, kgo.StringRecord("committed")).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	// Produce 5 records in a transaction, then ABORT.
	txnCl := newPlainClient(t, c,
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
	consumer := newPlainClient(t, c,
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

// TestTxnReinitAbortsOpenTxn verifies that a fresh InitProducerID on a
// transactional ID that has an open transaction aborts it. The producer ID is
// reused across the init, so leaving the transaction open means the next
// commit's marker covers the abandoned records and a read_committed consumer
// sees them.
func TestTxnReinitAbortsOpenTxn(t *testing.T) {
	t.Parallel()
	topic := "txn-reinit-aborts"
	txnID := "txn-reinit-aborts-id"
	c := newCoverCluster(t, NumBrokers(1), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Produce 3 records and walk away without ending the transaction.
	abandoned := newPlainClient(t, c,
		kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID(txnID),
	)
	if err := abandoned.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := abandoned.ProduceSync(ctx, kgo.StringRecord("abandoned")).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}

	// A second client on the same transactional ID inits fresh, taking
	// the ID over, and commits 2 records of its own.
	taker := newPlainClient(t, c,
		kgo.DefaultProduceTopic(topic),
		kgo.TransactionalID(txnID),
	)
	if err := taker.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := taker.ProduceSync(ctx, kgo.StringRecord("committed")).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
	if err := taker.EndTransaction(ctx, kgo.TryCommit); err != nil {
		t.Fatal(err)
	}

	consumer := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadCommitted()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	var got []string
	deadline := time.After(3 * time.Second)
	for len(got) < 2 {
		select {
		case <-deadline:
			t.Fatalf("timeout: got %v, want 2 committed records", got)
		default:
		}
		consumer.PollRecords(ctx, 10).EachRecord(func(r *kgo.Record) { got = append(got, string(r.Value)) })
	}
	shortCtx, shortCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer shortCancel()
	consumer.PollRecords(shortCtx, 10).EachRecord(func(r *kgo.Record) { got = append(got, string(r.Value)) })
	if want := []string{"committed", "committed"}; !slices.Equal(got, want) {
		t.Errorf("read_committed consumer saw %v, want %v", got, want)
	}
}

// TestDeleteRecordsAdvancesStartOffset verifies that after DeleteRecords,
// ListStartOffsets reflects the new log start offset. Deleting below the log
// start offset succeeds and changes nothing, and a compacted topic without
// delete in its cleanup.policy answers POLICY_VIOLATION.
func TestDeleteRecordsAdvancesStartOffset(t *testing.T) {
	t.Parallel()
	topic := "delete-records-start"
	c := newCoverCluster(t, NumBrokers(1), SeedTopics(1, topic))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	cl := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
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

	compacted := "delete-records-compacted"
	if err := c.CreateTopic(compacted, 1, map[string]string{"cleanup.policy": "compact"}); err != nil {
		t.Fatal(err)
	}
	for _, check := range []struct {
		topic   string
		at      int64
		wantErr error
		wantLow int64
	}{
		{topic, 3, nil, 7},
		{compacted, -1, kerr.PolicyViolation, -1},
	} {
		offsets := kadm.Offsets{}
		offsets.Add(kadm.Offset{Topic: check.topic, Partition: 0, At: check.at})
		dresp, err := adm.DeleteRecords(ctx, offsets)
		if err != nil {
			t.Fatal(err)
		}
		r, _ := dresp.Lookup(check.topic, 0)
		if r.Err != check.wantErr || r.LowWatermark != check.wantLow {
			t.Errorf("%s at %d: got %v low watermark %d, want %v %d", check.topic, check.at, r.Err, r.LowWatermark, check.wantErr, check.wantLow)
		}
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
	consumer := newPlainClient(t, c,
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

// A KIP-714 client metrics subscription is a CLIENT_METRICS config resource:
// an IncrementalAlterConfigs SET creates or updates one, DescribeConfigs
// reads it back with every unset key at its default, ListConfigResources
// lists the names, and deleting every key removes it. A name with no
// subscription describes as all defaults, as on Kafka, and is not listed.
func TestClientMetricsSubscriptions(t *testing.T) {
	t.Parallel()
	c := newCluster(t, NumBrokers(1))
	cl := newPlainClient(t, c)
	ctx := context.Background()

	set := func(k, v string) kmsg.IncrementalAlterConfigsRequestResourceConfig {
		rc := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
		rc.Name = k
		rc.Value = &v
		return rc
	}
	del := func(k string) kmsg.IncrementalAlterConfigsRequestResourceConfig {
		rc := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
		rc.Name = k
		rc.Op = kmsg.IncrementalAlterConfigOpDelete
		return rc
	}
	setNil := func(k string) kmsg.IncrementalAlterConfigsRequestResourceConfig {
		rc := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
		rc.Name = k
		return rc
	}

	alter := func(t *testing.T, name string, validateOnly bool, cfgs []kmsg.IncrementalAlterConfigsRequestResourceConfig) int16 {
		t.Helper()
		req := kmsg.NewPtrIncrementalAlterConfigsRequest()
		req.ValidateOnly = validateOnly
		rr := kmsg.NewIncrementalAlterConfigsRequestResource()
		rr.ResourceType = kmsg.ConfigResourceTypeClientMetrics
		rr.ResourceName = name
		rr.Configs = cfgs
		req.Resources = append(req.Resources, rr)
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Resources) != 1 {
			t.Fatalf("alter answered %d resources, want 1", len(resp.Resources))
		}
		return resp.Resources[0].ErrorCode
	}
	describe := func(t *testing.T, name string) (int16, map[string]kmsg.DescribeConfigsResponseResourceConfig) {
		t.Helper()
		req := kmsg.NewPtrDescribeConfigsRequest()
		rr := kmsg.NewDescribeConfigsRequestResource()
		rr.ResourceType = kmsg.ConfigResourceTypeClientMetrics
		rr.ResourceName = name
		req.Resources = append(req.Resources, rr)
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Resources) != 1 {
			t.Fatalf("describe answered %d resources, want 1", len(resp.Resources))
		}
		got := make(map[string]kmsg.DescribeConfigsResponseResourceConfig)
		for _, rc := range resp.Resources[0].Configs {
			got[rc.Name] = rc
		}
		return resp.Resources[0].ErrorCode, got
	}
	list := func(t *testing.T) []string {
		t.Helper()
		listed, err := kadm.NewClient(cl).ListConfigResources(ctx, kadm.ConfigResourceClientMetrics)
		if err == nil {
			err = listed.Err
		}
		if err != nil {
			t.Fatal(err)
		}
		if other := len(listed.Resources) - len(listed.FilterTypes(kadm.ConfigResourceClientMetrics)); other > 0 {
			t.Errorf("listed %d resources of another type: %v", other, listed.Resources)
		}
		return listed.Names()
	}

	// telemetry gets this client's subscription. A push with the ID the
	// previous get answered fails once the subscription changes.
	type subscription struct {
		metrics  []string
		interval int32
	}
	instance := [16]byte{1, 2, 3}
	var lastSub int32
	push := func(t *testing.T, sub int32) int16 {
		t.Helper()
		req := kmsg.NewPtrPushTelemetryRequest()
		req.ClientInstanceID = instance
		req.SubscriptionID = sub
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		return resp.ErrorCode
	}
	telemetry := func(t *testing.T) subscription {
		t.Helper()
		req := kmsg.NewPtrGetTelemetrySubscriptionsRequest()
		req.ClientInstanceID = instance
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		if lastSub != 0 && lastSub != resp.SubscriptionID {
			if code := push(t, lastSub); code != kerr.UnknownSubscriptionID.Code {
				t.Errorf("push with the old subscription ID: got error %d, want UNKNOWN_SUBSCRIPTION_ID", code)
			}
		}
		if code := push(t, resp.SubscriptionID); code != 0 {
			t.Errorf("push with the current subscription ID: got error %d", code)
		}
		lastSub = resp.SubscriptionID
		return subscription{resp.RequestedMetrics, resp.PushIntervalMillis}
	}
	all := []string{"*"} // every metric

	type config struct {
		value  string
		source kmsg.ConfigSource
	}
	dynamic := kmsg.ConfigSourceClientMetricsConfig
	def := kmsg.ConfigSourceDefaultConfig

	// The cases run in order against one cluster: what one creates the
	// next describes, lists, or deletes.
	for _, tc := range []struct {
		name string

		alter        string // subscription to alter, "" for none
		validateOnly bool
		cfgs         []kmsg.IncrementalAlterConfigsRequestResourceConfig
		alterErr     *kerr.Error

		describe string // subscription to describe, "" for none
		want     map[string]config

		list []string // nil skips the check

		telemetry *subscription // nil skips the check
	}{
		{
			name:  "create with every key",
			alter: "s1",
			cfgs: []kmsg.IncrementalAlterConfigsRequestResourceConfig{
				set("metrics", "org.apache.kafka.producer.,org.apache.kafka.consumer."),
				set("interval.ms", "5000"),
				set("match", "client_id=kcl.*,client_software_name=franz-go"),
			},
			describe: "s1",
			want: map[string]config{
				"metrics":     {"org.apache.kafka.producer.,org.apache.kafka.consumer.", dynamic},
				"interval.ms": {"5000", dynamic},
				"match":       {"client_id=kcl.*,client_software_name=franz-go", dynamic},
			},
			list: []string{"s1"},
			// s1 does not match the client: every metric every 30s.
			telemetry: &subscription{all, 30000},
		},
		{
			name:     "create with one key describes the defaults",
			alter:    "s2",
			cfgs:     []kmsg.IncrementalAlterConfigsRequestResourceConfig{set("metrics", "*")},
			describe: "s2",
			want: map[string]config{
				"metrics":     {"*", dynamic},
				"interval.ms": {"300000", def},
				"match":       {"", def},
			},
			list:      []string{"s1", "s2"},
			telemetry: &subscription{all, 300000},
		},
		{
			name:     "update one key keeps the others",
			alter:    "s1",
			cfgs:     []kmsg.IncrementalAlterConfigsRequestResourceConfig{set("interval.ms", "60000")},
			describe: "s1",
			want: map[string]config{
				"metrics":     {"org.apache.kafka.producer.,org.apache.kafka.consumer.", dynamic},
				"interval.ms": {"60000", dynamic},
				"match":       {"client_id=kcl.*,client_software_name=franz-go", dynamic},
			},
		},
		{
			name:     "unknown name describes as all defaults and is not listed",
			describe: "nope",
			want: map[string]config{
				"metrics":     {"", def},
				"interval.ms": {"300000", def},
				"match":       {"", def},
			},
			list: []string{"s1", "s2"},
		},
		{
			name:     "unknown key",
			alter:    "bad",
			cfgs:     []kmsg.IncrementalAlterConfigsRequestResourceConfig{set("push.ms", "1")},
			alterErr: kerr.InvalidRequest,
			list:     []string{"s1", "s2"},
		},
		{
			name:     "interval not a number",
			alter:    "bad",
			cfgs:     []kmsg.IncrementalAlterConfigsRequestResourceConfig{set("interval.ms", "soon")},
			alterErr: kerr.InvalidConfig,
		},
		{
			name:     "interval below 100ms",
			alter:    "bad",
			cfgs:     []kmsg.IncrementalAlterConfigsRequestResourceConfig{set("interval.ms", "99")},
			alterErr: kerr.InvalidRequest,
		},
		{
			name:     "interval above one hour",
			alter:    "bad",
			cfgs:     []kmsg.IncrementalAlterConfigsRequestResourceConfig{set("interval.ms", "3600001")},
			alterErr: kerr.InvalidRequest,
		},
		{
			name:     "match without a value",
			alter:    "bad",
			cfgs:     []kmsg.IncrementalAlterConfigsRequestResourceConfig{set("match", "client_id")},
			alterErr: kerr.InvalidConfig,
		},
		{
			name:     "match on an unknown property",
			alter:    "bad",
			cfgs:     []kmsg.IncrementalAlterConfigsRequestResourceConfig{set("match", "rack=a")},
			alterErr: kerr.InvalidConfig,
		},
		{
			name:     "match with a bad regex",
			alter:    "bad",
			cfgs:     []kmsg.IncrementalAlterConfigsRequestResourceConfig{set("match", "client_id=(")},
			alterErr: kerr.InvalidConfig,
		},
		{
			name:     "nil value",
			alter:    "bad",
			cfgs:     []kmsg.IncrementalAlterConfigsRequestResourceConfig{setNil("metrics")},
			alterErr: kerr.InvalidRequest,
		},
		{
			name:     "empty name",
			alter:    "",
			cfgs:     []kmsg.IncrementalAlterConfigsRequestResourceConfig{set("metrics", "*")},
			alterErr: kerr.InvalidRequest,
		},
		{
			name:         "validate only creates nothing",
			alter:        "vo",
			validateOnly: true,
			cfgs:         []kmsg.IncrementalAlterConfigsRequestResourceConfig{set("metrics", "*")},
			describe:     "vo",
			want: map[string]config{
				"metrics":     {"", def},
				"interval.ms": {"300000", def},
				"match":       {"", def},
			},
			list: []string{"s1", "s2"},
		},
		{
			name:  "deleting every key removes the subscription",
			alter: "s1",
			cfgs: []kmsg.IncrementalAlterConfigsRequestResourceConfig{
				del("metrics"), del("interval.ms"), del("match"),
			},
			describe: "s1",
			want: map[string]config{
				"metrics":     {"", def},
				"interval.ms": {"300000", def},
				"match":       {"", def},
			},
			list: []string{"s2"},
		},
		{
			name:     "deleting one key keeps the subscription",
			alter:    "s2",
			cfgs:     []kmsg.IncrementalAlterConfigsRequestResourceConfig{del("interval.ms")},
			describe: "s2",
			want: map[string]config{
				"metrics":     {"*", dynamic},
				"interval.ms": {"300000", def},
				"match":       {"", def},
			},
			list: []string{"s2"},
		},
		{
			name:      "a matching subscription answers its metrics",
			alter:     "s2",
			cfgs:      []kmsg.IncrementalAlterConfigsRequestResourceConfig{set("metrics", "b.,a.")},
			telemetry: &subscription{[]string{"a.", "b."}, 300000},
		},
		{
			name:      "the smallest matching interval wins",
			alter:     "s3",
			cfgs:      []kmsg.IncrementalAlterConfigsRequestResourceConfig{set("interval.ms", "1000"), set("match", "client_id=k.o")},
			telemetry: &subscription{[]string{"a.", "b."}, 1000},
		},
		{
			name:      "a matching subscription without metrics asks for none",
			alter:     "s2",
			cfgs:      []kmsg.IncrementalAlterConfigsRequestResourceConfig{set("match", "client_id=nope")},
			telemetry: &subscription{nil, 1000},
		},
		{
			name:      "no subscription matches",
			alter:     "s3",
			cfgs:      []kmsg.IncrementalAlterConfigsRequestResourceConfig{set("match", "client_id=nope")},
			telemetry: &subscription{all, 30000},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.alter != "" || tc.cfgs != nil {
				want := int16(0)
				if tc.alterErr != nil {
					want = tc.alterErr.Code
				}
				if got := alter(t, tc.alter, tc.validateOnly, tc.cfgs); got != want {
					t.Fatalf("alter %q: got %v, want %v", tc.alter, kerr.ErrorForCode(got), kerr.ErrorForCode(want))
				}
			}
			if tc.describe != "" {
				code, got := describe(t, tc.describe)
				if code != 0 {
					t.Fatalf("describe %q: %v", tc.describe, kerr.ErrorForCode(code))
				}
				if len(got) != len(tc.want) {
					t.Fatalf("describe %q: got %d configs, want %d", tc.describe, len(got), len(tc.want))
				}
				for k, w := range tc.want {
					rc, ok := got[k]
					if !ok || rc.Value == nil {
						t.Fatalf("describe %q: %s missing", tc.describe, k)
					}
					if *rc.Value != w.value || rc.Source != w.source {
						t.Errorf("describe %q: %s = %q (%v), want %q (%v)", tc.describe, k, *rc.Value, rc.Source, w.value, w.source)
					}
				}
			}
			if tc.list != nil {
				if got := list(t); !slices.Equal(got, tc.list) {
					t.Errorf("list: got %v, want %v", got, tc.list)
				}
			}
			if tc.telemetry != nil {
				if got := telemetry(t); !slices.Equal(got.metrics, tc.telemetry.metrics) || got.interval != tc.telemetry.interval {
					t.Errorf("telemetry: got %v every %dms, want %v every %dms", got.metrics, got.interval, tc.telemetry.metrics, tc.telemetry.interval)
				}
			}
		})
	}

	// Like Kafka, a client that pushed as terminating may push nothing
	// more, until we forget it once it stops asking.
	term := kmsg.NewPtrPushTelemetryRequest()
	term.ClientInstanceID, term.SubscriptionID, term.Terminating = instance, lastSub, true
	if resp, err := term.RequestWith(ctx, cl); err != nil || resp.ErrorCode != 0 {
		t.Fatalf("terminating push: %v %v", err, resp)
	}
	if code := push(t, lastSub); code != kerr.InvalidRequest.Code {
		t.Errorf("push after a terminating push: got error %d, want INVALID_REQUEST", code)
	}
	c.admin(func() { c.telem[instance].t.Reset(0) })
	gone := func() (forgot bool) {
		c.admin(func() { _, known := c.telem[instance]; forgot = !known })
		return forgot
	}
	for deadline := time.Now().Add(5 * time.Second); !gone() && time.Now().Before(deadline); {
		time.Sleep(time.Millisecond)
	}
	if code := push(t, lastSub); code != 0 {
		t.Errorf("push after we forgot the terminated client: got error %d", code)
	}
}
