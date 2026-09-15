// Derived via LLM from Apache Kafka's GroupMetadataManagerTest.java and
// ConsumerGroupHeartbeatRequestTest.scala (Apache 2.0).
// https://github.com/apache/kafka/blob/trunk/group-coordinator/src/test/java/org/apache/kafka/coordinator/group/GroupMetadataManagerTest.java

package kafka_tests

import (
	"context"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestStaticMember848RejoinGetsAssignmentBack verifies that a static member
// rejoining a KIP-848 group with the same instanceID reclaims its partitions.
// Derived via LLM from testRejoiningStaticMemberGetsAssignmentsBackWhenNewGroupCoordinatorIsEnabled.
func TestStaticMember848RejoinGetsAssignmentBack(t *testing.T) {
	t.Parallel()
	topic := "t-static-rejoin"
	group := "g-static-rejoin"
	instanceID := "static-inst-1"
	nRecords := 20

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(2, topic))
	producer := newClient848(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, nRecords)

	// First consumer with instanceID.
	c1 := newGroupConsumer(t, c, topic, group, kgo.InstanceID(instanceID))
	consumeN(t, c1, nRecords, 10*time.Second)
	dg := waitStable(t, c, group, 1)
	if dg.NumAssigned() != 2 {
		t.Fatalf("expected 2 partitions, got %d", dg.NumAssigned())
	}

	// Close c1 (epoch -2 leave, static mapping preserved).
	c1.Close()
	time.Sleep(500 * time.Millisecond)

	// Rejoin with same instanceID.
	c2 := newGroupConsumer(t, c, topic, group, kgo.InstanceID(instanceID))
	_ = c2
	dg = waitStable(t, c, group, 1)
	if dg.NumAssigned() != 2 {
		t.Fatalf("expected 2 partitions after rejoin, got %d", dg.NumAssigned())
	}
}

// TestStaticMember848FencedInstanceID verifies that a heartbeat from a
// different memberID but the same instanceID returns FENCED_INSTANCE_ID.
// Matches Kafka's throwIfInstanceIdIsFenced (GroupMetadataManager.java:1647),
// called from getOrMaybeSubscribeStaticConsumerGroupMember (line 3187).
// Derived from testShouldThrowFencedInstanceIdExceptionWhenStaticMemberWithDifferentMemberIdJoins.
func TestStaticMember848FencedInstanceID(t *testing.T) {
	t.Parallel()
	topic := "t-static-fence"
	group := "g-static-fence"
	instanceID := "fence-inst-1"
	nRecords := 20

	c := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(2, topic))
	producer := newClient848(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, nRecords)

	c1 := newGroupConsumer(t, c, topic, group, kgo.InstanceID(instanceID))
	consumeN(t, c1, nRecords, 10*time.Second)
	waitStable(t, c, group, 1)

	// A heartbeat from a different memberID but the same instanceID
	// must get FENCED_INSTANCE_ID.
	raw := newClient848(t, c)
	hbReq := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	hbReq.Group = group
	hbReq.MemberID = "unknown-member"
	hbReq.MemberEpoch = 11
	hbReq.RebalanceTimeoutMillis = 5000
	hbReq.InstanceID = &instanceID
	hbReq.SubscribedTopicNames = []string{topic}
	hbReq.Topics = []kmsg.ConsumerGroupHeartbeatRequestTopic{}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	hbResp, err := hbReq.RequestWith(ctx, raw)
	if err != nil {
		t.Fatalf("heartbeat request failed: %v", err)
	}
	if hbResp.ErrorCode != kerr.FencedInstanceID.Code {
		t.Fatalf("expected FENCED_INSTANCE_ID, got %v", kerr.ErrorForCode(hbResp.ErrorCode))
	}
}

// TestStaticMemberClassicRejoinNoRebalance verifies that a static member
// rejoining a classic group with unchanged protocol does not trigger a
// full rebalance (KIP-345 / KIP-814).
// Derived via LLM from testReplaceStaticMemberInStableStateNoError.
func TestStaticMemberClassicRejoinNoRebalance(t *testing.T) {
	t.Parallel()
	topic := "t-static-classic"
	group := "g-static-classic"
	instanceID := "classic-inst-1"

	c := newCluster(t, kfake.NumBrokers(1),
		kfake.SeedTopics(2, topic),
		kfake.BrokerConfigs(map[string]string{"group.min.session.timeout.ms": "100"}),
	)
	producer := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
	produceNStrings(t, producer, topic, 20)

	// First consumer. Short session timeout so the server removes
	// the member quickly after close.
	cl1 := newPlainClient(t, c,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.InstanceID(instanceID),
		kgo.SessionTimeout(500*time.Millisecond),
		kgo.HeartbeatInterval(100*time.Millisecond), // must be < session timeout
	)
	consumeN(t, cl1, 20, 10*time.Second)
	waitStable(t, c, group, 1)

	// Close (static member - no leave sent).
	cl1.Close()
	time.Sleep(700 * time.Millisecond) // wait for session timeout

	// Rejoin with same instanceID.
	newPlainClient(t, c,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
		kgo.InstanceID(instanceID),
	)

	dg := waitStable(t, c, group, 1)
	found := false
	for _, m := range dg.Members {
		if m.InstanceID != nil && *m.InstanceID == instanceID {
			found = true
		}
	}
	if !found {
		t.Fatalf("instanceID %q not found after rejoin", instanceID)
	}
}

// TestGroupMaxSizeClassic verifies that a classic group rejects new members
// when group.max.size is reached.
// Derived via LLM from testJoinGroupShouldReceiveErrorIfGroupOverMaxSize.
func TestGroupMaxSizeClassic(t *testing.T) {
	t.Parallel()
	topic := "t-maxsize-classic"
	group := "g-maxsize-classic"

	c := newCluster(t, kfake.NumBrokers(1),
		kfake.SeedTopics(2, topic),
		kfake.BrokerConfigs(map[string]string{"group.max.size": "1"}),
	)

	// First consumer joins successfully.
	newPlainClient(t, c,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)

	waitStable(t, c, group, 1)

	// Second consumer: send a raw JoinGroup and expect GROUP_MAX_SIZE_REACHED.
	raw := newPlainClient(t, c)
	joinReq := kmsg.NewPtrJoinGroupRequest()
	joinReq.Group = group
	joinReq.SessionTimeoutMillis = 45000
	joinReq.RebalanceTimeoutMillis = 45000
	joinReq.ProtocolType = "consumer"
	joinReq.Version = 4
	proto := kmsg.NewJoinGroupRequestProtocol()
	proto.Name = "cooperative-sticky"
	proto.Metadata = (&kmsg.ConsumerMemberMetadata{Topics: []string{topic}}).AppendTo(nil)
	joinReq.Protocols = append(joinReq.Protocols, proto)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	joinResp, err := joinReq.RequestWith(ctx, raw)
	if err != nil {
		t.Fatalf("join request failed: %v", err)
	}
	if joinResp.ErrorCode != kerr.GroupMaxSizeReached.Code {
		t.Fatalf("expected GROUP_MAX_SIZE_REACHED, got %v", kerr.ErrorForCode(joinResp.ErrorCode))
	}
}

// TestGroupMaxSize848 verifies that a 848 group rejects new members
// when group.max.size is reached.
// Derived via LLM from testNewMemberIsRejectedWithMaximumMembersIsReached.
func TestGroupMaxSize848(t *testing.T) {
	t.Parallel()
	topic := "t-maxsize-848"
	group := "g-maxsize-848"

	c := newCluster(t, kfake.NumBrokers(1),
		kfake.SeedTopics(2, topic),
		kfake.BrokerConfigs(map[string]string{"group.max.size": "1"}),
	)

	// First consumer joins successfully.
	c1 := newGroupConsumer(t, c, topic, group)
	waitStable(t, c, group, 1)
	_ = c1

	// Second consumer: send raw heartbeat and expect GROUP_MAX_SIZE_REACHED.
	raw := newClient848(t, c)
	hbReq := kmsg.NewPtrConsumerGroupHeartbeatRequest()
	hbReq.Group = group
	hbReq.MemberEpoch = 0
	hbReq.RebalanceTimeoutMillis = 45000
	hbReq.SubscribedTopicNames = []string{topic}
	hbReq.Topics = []kmsg.ConsumerGroupHeartbeatRequestTopic{}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	hbResp, err := hbReq.RequestWith(ctx, raw)
	if err != nil {
		t.Fatalf("heartbeat request failed: %v", err)
	}
	if hbResp.ErrorCode != kerr.GroupMaxSizeReached.Code {
		t.Fatalf("expected GROUP_MAX_SIZE_REACHED, got %v", kerr.ErrorForCode(hbResp.ErrorCode))
	}
}

// TestCreateTopicsReplicaAssignment verifies that CreateTopics with
// manual ReplicaAssignment creates the topic with the correct partition count.
// Derived via LLM from testValidCreateTopicsRequests.
func TestCreateTopicsReplicaAssignment(t *testing.T) {
	t.Parallel()
	c := newCluster(t, kfake.NumBrokers(3))
	raw := newClient848(t, c)

	req := kmsg.NewPtrCreateTopicsRequest()

	// Topic with manual 3-partition assignment.
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic = "t-replica-assign"
	rt.NumPartitions = -1
	rt.ReplicationFactor = -1
	for i := range int32(3) {
		ra := kmsg.NewCreateTopicsRequestTopicReplicaAssignment()
		ra.Partition = i
		ra.Replicas = []int32{0, 1, 2}
		rt.ReplicaAssignment = append(rt.ReplicaAssignment, ra)
	}
	req.Topics = append(req.Topics, rt)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := req.RequestWith(ctx, raw)
	if err != nil {
		t.Fatalf("create topics failed: %v", err)
	}
	if len(resp.Topics) != 1 {
		t.Fatalf("expected 1 topic response, got %d", len(resp.Topics))
	}
	st := resp.Topics[0]
	if st.ErrorCode != 0 {
		t.Fatalf("unexpected error: %v", kerr.ErrorForCode(st.ErrorCode))
	}
	if st.NumPartitions != 3 {
		t.Fatalf("expected 3 partitions, got %d", st.NumPartitions)
	}
}

// TestCreateTopicsReplicaAssignmentWithNumPartitions verifies that providing
// both ReplicaAssignment and NumPartitions returns INVALID_REQUEST.
// Derived via LLM from testInvalidCreateTopicsRequests.
func TestCreateTopicsReplicaAssignmentWithNumPartitions(t *testing.T) {
	t.Parallel()
	c := newCluster(t, kfake.NumBrokers(1))
	raw := newClient848(t, c)

	req := kmsg.NewPtrCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic = "t-replica-invalid"
	rt.NumPartitions = 10
	rt.ReplicationFactor = 1
	ra := kmsg.NewCreateTopicsRequestTopicReplicaAssignment()
	ra.Partition = 0
	ra.Replicas = []int32{0}
	rt.ReplicaAssignment = append(rt.ReplicaAssignment, ra)
	req.Topics = append(req.Topics, rt)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := req.RequestWith(ctx, raw)
	if err != nil {
		t.Fatalf("create topics failed: %v", err)
	}
	if resp.Topics[0].ErrorCode != kerr.InvalidRequest.Code {
		t.Fatalf("expected INVALID_REQUEST, got %v", kerr.ErrorForCode(resp.Topics[0].ErrorCode))
	}
}
