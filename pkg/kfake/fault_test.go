package kfake

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// hasErrorCode reports whether any error code anywhere in resp is code.
func hasErrorCode(resp kmsg.Response, code int16) bool {
	var walk func(v reflect.Value) bool
	walk = func(v reflect.Value) bool {
		switch v.Kind() {
		case reflect.Pointer, reflect.Interface:
			return !v.IsNil() && walk(v.Elem())
		case reflect.Slice, reflect.Array:
			for i := range v.Len() {
				if walk(v.Index(i)) {
					return true
				}
			}
		case reflect.Struct:
			t := v.Type()
			for i := range v.NumField() {
				f := t.Field(i)
				if !f.IsExported() {
					continue
				}
				if f.Type.Kind() == reflect.Int16 && (f.Name == "ErrorCode" || f.Name == "AcknowledgeErrorCode") {
					if v.Field(i).Int() == int64(code) {
						return true
					}
					continue
				}
				if walk(v.Field(i)) {
					return true
				}
			}
		default:
		}
		return false
	}
	return walk(reflect.ValueOf(resp))
}

// coverageReq builds a minimal request of the key that names something we can
// fault. It returns nil for a key we cannot exercise.
func coverageReq(key int16, topic string, id [16]byte, group, txnID string) kmsg.Request {
	part := func() kmsg.Request { return kmsg.RequestForKey(key) }
	switch kmsg.Key(key) {
	case kmsg.Produce:
		req := kmsg.NewPtrProduceRequest()
		req.Acks = -1
		req.TimeoutMillis = 1000
		rt := kmsg.NewProduceRequestTopic()
		rt.Topic, rt.TopicID = topic, id
		rp := kmsg.NewProduceRequestTopicPartition()
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)
		return req
	case kmsg.Fetch:
		req := kmsg.NewPtrFetchRequest()
		req.MaxWaitMillis = 100
		req.MinBytes = 1
		req.SessionEpoch = -1
		rt := kmsg.NewFetchRequestTopic()
		rt.Topic, rt.TopicID = topic, id
		rp := kmsg.NewFetchRequestTopicPartition()
		rp.CurrentLeaderEpoch = -1
		rp.PartitionMaxBytes = 1 << 20
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)
		return req
	case kmsg.ListOffsets:
		req := kmsg.NewPtrListOffsetsRequest()
		req.ReplicaID = -1
		rt := kmsg.NewListOffsetsRequestTopic()
		rt.Topic = topic
		rp := kmsg.NewListOffsetsRequestTopicPartition()
		rp.Timestamp, rp.CurrentLeaderEpoch = -1, -1
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)
		return req
	case kmsg.Metadata:
		req := kmsg.NewPtrMetadataRequest()
		rt := kmsg.NewMetadataRequestTopic()
		rt.Topic, rt.TopicID = kmsg.StringPtr(topic), id
		req.Topics = append(req.Topics, rt)
		return req
	case kmsg.OffsetCommit:
		req := kmsg.NewPtrOffsetCommitRequest()
		req.Group = group
		req.Generation = -1
		rt := kmsg.NewOffsetCommitRequestTopic()
		rt.Topic, rt.TopicID = topic, id
		rp := kmsg.NewOffsetCommitRequestTopicPartition()
		rp.LeaderEpoch = -1
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)
		return req
	case kmsg.OffsetFetch:
		req := kmsg.NewPtrOffsetFetchRequest()
		rg := kmsg.NewOffsetFetchRequestGroup()
		rg.Group = group
		rg.MemberEpoch = -1
		rt := kmsg.NewOffsetFetchRequestGroupTopic()
		rt.Topic, rt.TopicID = topic, id
		rt.Partitions = append(rt.Partitions, 0)
		rg.Topics = append(rg.Topics, rt)
		req.Groups = append(req.Groups, rg)
		return req
	case kmsg.FindCoordinator:
		req := kmsg.NewPtrFindCoordinatorRequest()
		req.CoordinatorKeys = []string{group}
		req.CoordinatorKey = group
		return req
	case kmsg.JoinGroup:
		req := kmsg.NewPtrJoinGroupRequest()
		req.Group = group
		req.SessionTimeoutMillis = 10000
		req.RebalanceTimeoutMillis = 10000
		req.ProtocolType = "consumer"
		p := kmsg.NewJoinGroupRequestProtocol()
		p.Name = "range"
		req.Protocols = append(req.Protocols, p)
		return req
	case kmsg.Heartbeat:
		req := kmsg.NewPtrHeartbeatRequest()
		req.Group = group
		req.Generation = -1
		return req
	case kmsg.LeaveGroup:
		req := kmsg.NewPtrLeaveGroupRequest()
		req.Group = group
		m := kmsg.NewLeaveGroupRequestMember()
		m.MemberID = "m"
		req.Members = append(req.Members, m)
		return req
	case kmsg.SyncGroup:
		req := kmsg.NewPtrSyncGroupRequest()
		req.Group = group
		req.Generation = -1
		return req
	case kmsg.DescribeGroups:
		req := kmsg.NewPtrDescribeGroupsRequest()
		req.Groups = []string{group}
		return req
	case kmsg.CreateTopics:
		req := kmsg.NewPtrCreateTopicsRequest()
		rt := kmsg.NewCreateTopicsRequestTopic()
		rt.Topic = "coverage-created"
		rt.NumPartitions, rt.ReplicationFactor = 1, 1
		req.Topics = append(req.Topics, rt)
		return req
	case kmsg.DeleteTopics:
		req := kmsg.NewPtrDeleteTopicsRequest()
		rt := kmsg.NewDeleteTopicsRequestTopic()
		rt.Topic = kmsg.StringPtr("coverage-deleted")
		req.Topics = append(req.Topics, rt)
		req.TopicNames = []string{"coverage-deleted"}
		return req
	case kmsg.DeleteRecords:
		req := kmsg.NewPtrDeleteRecordsRequest()
		rt := kmsg.NewDeleteRecordsRequestTopic()
		rt.Topic = topic
		rp := kmsg.NewDeleteRecordsRequestTopicPartition()
		rp.Offset = -1
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)
		return req
	case kmsg.InitProducerID:
		req := kmsg.NewPtrInitProducerIDRequest()
		req.TransactionalID = kmsg.StringPtr(txnID)
		req.TransactionTimeoutMillis = 10000
		req.ProducerID, req.ProducerEpoch = -1, -1
		return req
	case kmsg.OffsetForLeaderEpoch:
		req := kmsg.NewPtrOffsetForLeaderEpochRequest()
		req.ReplicaID = -1
		rt := kmsg.NewOffsetForLeaderEpochRequestTopic()
		rt.Topic = topic
		rp := kmsg.NewOffsetForLeaderEpochRequestTopicPartition()
		rp.CurrentLeaderEpoch = -1
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)
		return req
	case kmsg.AddPartitionsToTxn:
		req := kmsg.NewPtrAddPartitionsToTxnRequest()
		req.Version = 3 // v4+ is the broker to broker batched shape
		req.TransactionalID = txnID
		req.ProducerEpoch = -1
		rt := kmsg.NewAddPartitionsToTxnRequestTopic()
		rt.Topic = topic
		rt.Partitions = []int32{0}
		req.Topics = append(req.Topics, rt)
		return req
	case kmsg.AddOffsetsToTxn:
		req := kmsg.NewPtrAddOffsetsToTxnRequest()
		req.TransactionalID = txnID
		req.Group = group
		req.ProducerEpoch = -1
		return req
	case kmsg.EndTxn:
		req := kmsg.NewPtrEndTxnRequest()
		req.TransactionalID = txnID
		req.ProducerEpoch = -1
		return req
	case kmsg.WriteTxnMarkers:
		req := kmsg.NewPtrWriteTxnMarkersRequest()
		m := kmsg.NewWriteTxnMarkersRequestMarker()
		mt := kmsg.NewWriteTxnMarkersRequestMarkerTopic()
		mt.Topic = topic
		mt.Partitions = []int32{0}
		m.Topics = append(m.Topics, mt)
		req.Markers = append(req.Markers, m)
		return req
	case kmsg.TxnOffsetCommit:
		req := kmsg.NewPtrTxnOffsetCommitRequest()
		req.TransactionalID = txnID
		req.Group = group
		req.ProducerEpoch = -1
		req.Generation = -1
		rt := kmsg.NewTxnOffsetCommitRequestTopic()
		rt.Topic = topic
		rp := kmsg.NewTxnOffsetCommitRequestTopicPartition()
		rp.LeaderEpoch = -1
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)
		return req
	case kmsg.CreateACLs:
		req := kmsg.NewPtrCreateACLsRequest()
		cr := kmsg.NewCreateACLsRequestCreation()
		cr.ResourceType = kmsg.ACLResourceTypeTopic
		cr.ResourceName = topic
		cr.ResourcePatternType = kmsg.ACLResourcePatternTypeLiteral
		cr.Principal = "User:coverage"
		cr.Host = "*"
		cr.Operation = kmsg.ACLOperationRead
		cr.PermissionType = kmsg.ACLPermissionTypeAllow
		req.Creations = append(req.Creations, cr)
		return req
	case kmsg.DeleteACLs:
		req := kmsg.NewPtrDeleteACLsRequest()
		f := kmsg.NewDeleteACLsRequestFilter()
		f.ResourceType = kmsg.ACLResourceTypeTopic
		f.ResourceName = kmsg.StringPtr(topic)
		f.ResourcePatternType = kmsg.ACLResourcePatternTypeLiteral
		f.Operation = kmsg.ACLOperationAny
		f.PermissionType = kmsg.ACLPermissionTypeAny
		req.Filters = append(req.Filters, f)
		return req
	case kmsg.DescribeConfigs:
		req := kmsg.NewPtrDescribeConfigsRequest()
		rr := kmsg.NewDescribeConfigsRequestResource()
		rr.ResourceType = kmsg.ConfigResourceTypeTopic
		rr.ResourceName = topic
		req.Resources = append(req.Resources, rr)
		return req
	case kmsg.AlterConfigs:
		req := kmsg.NewPtrAlterConfigsRequest()
		rr := kmsg.NewAlterConfigsRequestResource()
		rr.ResourceType = kmsg.ConfigResourceTypeTopic
		rr.ResourceName = topic
		req.Resources = append(req.Resources, rr)
		return req
	case kmsg.IncrementalAlterConfigs:
		req := kmsg.NewPtrIncrementalAlterConfigsRequest()
		rr := kmsg.NewIncrementalAlterConfigsRequestResource()
		rr.ResourceType = kmsg.ConfigResourceTypeTopic
		rr.ResourceName = topic
		req.Resources = append(req.Resources, rr)
		return req
	case kmsg.AlterReplicaLogDirs:
		req := kmsg.NewPtrAlterReplicaLogDirsRequest()
		rd := kmsg.NewAlterReplicaLogDirsRequestDir()
		rd.Dir = "/kfake"
		rt := kmsg.NewAlterReplicaLogDirsRequestDirTopic()
		rt.Topic = topic
		rt.Partitions = []int32{0}
		rd.Topics = append(rd.Topics, rt)
		req.Dirs = append(req.Dirs, rd)
		return req
	case kmsg.CreatePartitions:
		req := kmsg.NewPtrCreatePartitionsRequest()
		rt := kmsg.NewCreatePartitionsRequestTopic()
		rt.Topic = topic
		rt.Count = 5
		req.Topics = append(req.Topics, rt)
		return req
	case kmsg.DeleteGroups:
		req := kmsg.NewPtrDeleteGroupsRequest()
		req.Groups = []string{group}
		return req
	case kmsg.ElectLeaders:
		req := kmsg.NewPtrElectLeadersRequest()
		rt := kmsg.NewElectLeadersRequestTopic()
		rt.Topic = topic
		rt.Partitions = []int32{0}
		req.Topics = append(req.Topics, rt)
		return req
	case kmsg.AlterPartitionAssignments:
		req := kmsg.NewPtrAlterPartitionAssignmentsRequest()
		rt := kmsg.NewAlterPartitionAssignmentsRequestTopic()
		rt.Topic = topic
		rp := kmsg.NewAlterPartitionAssignmentsRequestTopicPartition()
		rp.Replicas = []int32{0}
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)
		return req
	case kmsg.OffsetDelete:
		req := kmsg.NewPtrOffsetDeleteRequest()
		req.Group = group
		rt := kmsg.NewOffsetDeleteRequestTopic()
		rt.Topic = topic
		rp := kmsg.NewOffsetDeleteRequestTopicPartition()
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)
		return req
	case kmsg.AlterClientQuotas:
		req := kmsg.NewPtrAlterClientQuotasRequest()
		e := kmsg.NewAlterClientQuotasRequestEntry()
		ent := kmsg.NewAlterClientQuotasRequestEntryEntity()
		ent.Type = "client-id"
		ent.Name = kmsg.StringPtr("coverage")
		e.Entity = append(e.Entity, ent)
		op := kmsg.NewAlterClientQuotasRequestEntryOp()
		op.Key = "producer_byte_rate"
		op.Value = 1000
		e.Ops = append(e.Ops, op)
		req.Entries = append(req.Entries, e)
		return req
	case kmsg.DescribeUserSCRAMCredentials:
		req := kmsg.NewPtrDescribeUserSCRAMCredentialsRequest()
		u := kmsg.NewDescribeUserSCRAMCredentialsRequestUser()
		u.Name = "coverage"
		req.Users = append(req.Users, u)
		return req
	case kmsg.AlterUserSCRAMCredentials:
		req := kmsg.NewPtrAlterUserSCRAMCredentialsRequest()
		u := kmsg.NewAlterUserSCRAMCredentialsRequestUpsertion()
		u.Name = "coverage"
		u.Mechanism = 2
		u.Iterations = 4096
		u.Salt = []byte("salt")
		u.SaltedPassword = []byte("pass")
		req.Upsertions = append(req.Upsertions, u)
		return req
	case kmsg.UpdateFeatures:
		req := kmsg.NewPtrUpdateFeaturesRequest()
		fu := kmsg.NewUpdateFeaturesRequestFeatureUpdate()
		fu.Feature = "transaction.version"
		fu.MaxVersionLevel = 2
		req.FeatureUpdates = append(req.FeatureUpdates, fu)
		return req
	case kmsg.DescribeProducers:
		req := kmsg.NewPtrDescribeProducersRequest()
		rt := kmsg.NewDescribeProducersRequestTopic()
		rt.Topic = topic
		rt.Partitions = []int32{0}
		req.Topics = append(req.Topics, rt)
		return req
	case kmsg.DescribeTransactions:
		req := kmsg.NewPtrDescribeTransactionsRequest()
		req.TransactionalIDs = []string{txnID}
		return req
	case kmsg.ConsumerGroupHeartbeat:
		req := kmsg.NewPtrConsumerGroupHeartbeatRequest()
		req.Group = group
		req.MemberID = "00000000-0000-0000-0000-000000000001"
		req.MemberEpoch = 0
		req.RebalanceTimeoutMillis = 10000
		req.SubscribedTopicNames = []string{topic}
		return req
	case kmsg.ConsumerGroupDescribe:
		req := kmsg.NewPtrConsumerGroupDescribeRequest()
		req.Groups = []string{group}
		return req
	case kmsg.DescribeTopicPartitions:
		req := kmsg.NewPtrDescribeTopicPartitionsRequest()
		rt := kmsg.NewDescribeTopicPartitionsRequestTopic()
		rt.Topic = topic
		req.Topics = append(req.Topics, rt)
		return req
	case kmsg.ShareGroupHeartbeat:
		req := kmsg.NewPtrShareGroupHeartbeatRequest()
		req.GroupID = group
		req.MemberID = "00000000-0000-0000-0000-000000000002"
		req.MemberEpoch = 0
		req.SubscribedTopicNames = []string{topic}
		return req
	case kmsg.ShareGroupDescribe:
		req := kmsg.NewPtrShareGroupDescribeRequest()
		req.GroupIDs = []string{group}
		return req
	case kmsg.ShareFetch:
		req := kmsg.NewPtrShareFetchRequest()
		req.GroupID = kmsg.StringPtr(group)
		req.MemberID = kmsg.StringPtr("00000000-0000-0000-0000-000000000003")
		req.ShareSessionEpoch = 0
		req.MaxWaitMillis = 100
		req.MaxRecords = 100
		rt := kmsg.NewShareFetchRequestTopic()
		rt.TopicID = id
		rp := kmsg.NewShareFetchRequestTopicPartition()
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)
		return req
	case kmsg.ShareAcknowledge:
		req := kmsg.NewPtrShareAcknowledgeRequest()
		req.GroupID = kmsg.StringPtr(group)
		req.MemberID = kmsg.StringPtr("00000000-0000-0000-0000-000000000003")
		req.ShareSessionEpoch = 1
		rt := kmsg.NewShareAcknowledgeRequestTopic()
		rt.TopicID = id
		rp := kmsg.NewShareAcknowledgeRequestTopicPartition()
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)
		return req
	case kmsg.DescribeShareGroupOffsets:
		req := kmsg.NewPtrDescribeShareGroupOffsetsRequest()
		rg := kmsg.NewDescribeShareGroupOffsetsRequestGroup()
		rg.GroupID = group
		rt := kmsg.NewDescribeShareGroupOffsetsRequestGroupTopic()
		rt.Topic = topic
		rt.Partitions = []int32{0}
		rg.Topics = append(rg.Topics, rt)
		req.Groups = append(req.Groups, rg)
		return req
	case kmsg.AlterShareGroupOffsets:
		req := kmsg.NewPtrAlterShareGroupOffsetsRequest()
		req.GroupID = group
		rt := kmsg.NewAlterShareGroupOffsetsRequestTopic()
		rt.Topic = topic
		rp := kmsg.NewAlterShareGroupOffsetsRequestTopicPartition()
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)
		return req
	case kmsg.DeleteShareGroupOffsets:
		req := kmsg.NewPtrDeleteShareGroupOffsetsRequest()
		req.GroupID = group
		rt := kmsg.NewDeleteShareGroupOffsetsRequestTopic()
		rt.Topic = topic
		req.Topics = append(req.Topics, rt)
		return req
	default:
		// ApiVersions, SASL, telemetry, and the cluster and listing
		// requests carry nothing to name.
		return part()
	}
}

// Every request we handle can be faulted.
func TestFaultCoverage(t *testing.T) {
	t.Parallel()
	const topic, group, txnID = "t", "g", "x"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic, "coverage-deleted"))
	cl := newPlainClient(t, c)
	id := c.TopicInfo(topic).TopicID

	// A join creates the group the later requests need.
	join := coverageReq(int16(kmsg.JoinGroup), topic, id, group, txnID)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if _, err := cl.Broker(0).Request(ctx, join); err != nil {
		t.Fatalf("seeding the group: %v", err)
	}

	keys := make([]int16, 0, len(apiVersionsKeys))
	apiVersionsMu.Lock()
	for k := range apiVersionsKeys {
		keys = append(keys, k)
	}
	apiVersionsMu.Unlock()

	for _, key := range keys {
		t.Run(kmsg.NameForKey(key), func(t *testing.T) {
			req := coverageReq(key, topic, id, group, txnID)
			if req == nil {
				t.Skip("no minimal request")
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()

			// The client sends its own ApiVersions when it opens a
			// connection. We open the connection before installing
			// the fault.
			if _, err := cl.Broker(0).Request(ctx, kmsg.NewPtrApiVersionsRequest()); err != nil {
				t.Fatalf("opening the connection: %v", err)
			}

			h := c.Fault(Fault{Keys: []kmsg.Key{kmsg.Key(key)}, Count: -1, Err: kerr.UnknownServerError})
			defer h.Remove()

			resp, err := cl.Broker(0).Request(ctx, req)
			if err != nil {
				t.Fatalf("request: %v", err)
			}
			if n := h.Hits(); n == 0 {
				t.Errorf("fault never fired")
			}
			// We answer AddPartitionsToTxn in its v3 shape while
			// advertising v5, so a raw request negotiated at v5
			// carries no partitions for us to answer.
			if kmsg.Key(key) == kmsg.AddPartitionsToTxn {
				return
			}
			if !hasErrorCode(resp, kerr.UnknownServerError.Code) {
				t.Errorf("response carries no injected error code")
			}
		})
	}
}

// faultReq sends req to a node and returns the response.
func faultReq(t *testing.T, cl *kgo.Client, node int32, req kmsg.Request) kmsg.Response {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := cl.Broker(int(node)).Request(ctx, req)
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	return resp
}

func listOffsetsReq(topic string, partitions ...int32) *kmsg.ListOffsetsRequest {
	req := kmsg.NewPtrListOffsetsRequest()
	req.ReplicaID = -1
	rt := kmsg.NewListOffsetsRequestTopic()
	rt.Topic = topic
	for _, p := range partitions {
		rp := kmsg.NewListOffsetsRequestTopicPartition()
		rp.Partition = p
		rp.Timestamp, rp.CurrentLeaderEpoch = -1, -1
		rt.Partitions = append(rt.Partitions, rp)
	}
	req.Topics = append(req.Topics, rt)
	return req
}

func listOffsetsCode(t *testing.T, cl *kgo.Client, node int32, topic string, p int32) int16 {
	t.Helper()
	resp := faultReq(t, cl, node, listOffsetsReq(topic, p)).(*kmsg.ListOffsetsResponse)
	if len(resp.Topics) != 1 || len(resp.Topics[0].Partitions) != 1 {
		t.Fatalf("list offsets answered %d topics", len(resp.Topics))
	}
	return resp.Topics[0].Partitions[0].ErrorCode
}

func joinGroup(t *testing.T, cl *kgo.Client, group string) {
	t.Helper()
	req := kmsg.NewPtrJoinGroupRequest()
	req.Group = group
	req.SessionTimeoutMillis = 10000
	req.RebalanceTimeoutMillis = 10000
	req.ProtocolType = "consumer"
	p := kmsg.NewJoinGroupRequestProtocol()
	p.Name = "range"
	req.Protocols = append(req.Protocols, p)
	faultReq(t, cl, 0, req)
}

// A partition selector fails that partition and leaves the rest alone.
func TestFaultPartition(t *testing.T) {
	t.Parallel()
	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(3, topic))
	cl := newPlainClient(t, c, kgo.RecordPartitioner(kgo.ManualPartitioner()))

	h := c.Fault(Fault{
		Keys:       []kmsg.Key{kmsg.Produce},
		Topic:      topic,
		Partitions: []int32{2},
		Err:        kerr.PolicyViolation,
		Count:      -1,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for p := int32(0); p < 3; p++ {
		r := &kgo.Record{Topic: topic, Partition: p, Value: []byte("v")}
		err := cl.ProduceSync(ctx, r).FirstErr()
		if p == 2 {
			if !errors.Is(err, kerr.PolicyViolation) {
				t.Errorf("partition 2: got %v, want policy violation", err)
			}
		} else if err != nil {
			t.Errorf("partition %d: %v", p, err)
		}
	}
	if n := h.Hits(); n == 0 {
		t.Error("fault never fired")
	}
}

// A group selector fails every partition of every topic the group commits.
func TestFaultGroupCommits(t *testing.T) {
	t.Parallel()
	const topic, group, other = "t", "g", "other"
	c := newCluster(t, NumBrokers(1), SeedTopics(3, topic))
	cl := newPlainClient(t, c)
	joinGroup(t, cl, group)
	joinGroup(t, cl, other)

	h := c.Fault(Fault{Group: group, Err: kerr.NotCoordinator, Count: -1})

	commit := func(group string) *kmsg.OffsetCommitResponse {
		req := kmsg.NewPtrOffsetCommitRequest()
		req.Group = group
		req.Generation = -1
		rt := kmsg.NewOffsetCommitRequestTopic()
		rt.Topic = topic
		rt.TopicID = c.TopicInfo(topic).TopicID
		for p := int32(0); p < 3; p++ {
			rp := kmsg.NewOffsetCommitRequestTopicPartition()
			rp.Partition, rp.Offset, rp.LeaderEpoch = p, 1, -1
			rt.Partitions = append(rt.Partitions, rp)
		}
		req.Topics = append(req.Topics, rt)
		return faultReq(t, cl, 0, req).(*kmsg.OffsetCommitResponse)
	}

	resp := commit(group)
	for _, st := range resp.Topics {
		for _, sp := range st.Partitions {
			if sp.ErrorCode != kerr.NotCoordinator.Code {
				t.Errorf("%s partition %d answered %d, want not coordinator", st.Topic, sp.Partition, sp.ErrorCode)
			}
		}
	}
	resp = commit(other)
	for _, st := range resp.Topics {
		for _, sp := range st.Partitions {
			if sp.ErrorCode != 0 {
				t.Errorf("other group partition %d answered %d", sp.Partition, sp.ErrorCode)
			}
		}
	}
	if n := h.Hits(); n != 1 {
		t.Errorf("fault fired %d times != 1", n)
	}
}

// A group selector fails one group's heartbeats and not another's.
func TestFaultGroupHeartbeat(t *testing.T) {
	t.Parallel()
	const group, other = "g", "other"
	c := newCluster(t, NumBrokers(1))
	cl := newPlainClient(t, c)
	joinGroup(t, cl, group)
	joinGroup(t, cl, other)

	h := c.Fault(Fault{Keys: []kmsg.Key{kmsg.Heartbeat}, Group: group, Err: kerr.CoordinatorLoadInProgress, Count: -1})

	beat := func(group string) int16 {
		req := kmsg.NewPtrHeartbeatRequest()
		req.Group = group
		req.Generation = -1
		return faultReq(t, cl, 0, req).(*kmsg.HeartbeatResponse).ErrorCode
	}
	if code := beat(group); code != kerr.CoordinatorLoadInProgress.Code {
		t.Errorf("faulted group answered %d", code)
	}
	if code := beat(other); code == kerr.CoordinatorLoadInProgress.Code {
		t.Error("the other group was faulted")
	}
	if n := h.Hits(); n != 1 {
		t.Errorf("fault fired %d times != 1", n)
	}
}

// A group selector picks one group out of a describe.
func TestFaultDescribeGroups(t *testing.T) {
	t.Parallel()
	const group, other = "g", "other"
	c := newCluster(t, NumBrokers(1))
	cl := newPlainClient(t, c)
	joinGroup(t, cl, group)
	joinGroup(t, cl, other)

	c.Fault(Fault{Group: group, Err: kerr.GroupAuthorizationFailed, Count: -1})

	req := kmsg.NewPtrDescribeGroupsRequest()
	req.Groups = []string{group, other}
	resp := faultReq(t, cl, 0, req).(*kmsg.DescribeGroupsResponse)
	if len(resp.Groups) != 2 {
		t.Fatalf("describe answered %d groups", len(resp.Groups))
	}
	for _, g := range resp.Groups {
		switch g.Group {
		case group:
			if g.ErrorCode != kerr.GroupAuthorizationFailed.Code {
				t.Errorf("faulted group answered %d", g.ErrorCode)
			}
		case other:
			if g.ErrorCode != 0 {
				t.Errorf("other group answered %d", g.ErrorCode)
			}
		}
	}
}

// TopLevel fails the whole request and answers it at once.
func TestFaultTopLevel(t *testing.T) {
	t.Parallel()
	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c)
	id := c.TopicInfo(topic).TopicID

	h := c.Fault(Fault{Keys: []kmsg.Key{kmsg.Fetch}, TopLevel: true, Err: kerr.FetchSessionTopicIDError, Count: -1})

	req := kmsg.NewPtrFetchRequest()
	req.MaxWaitMillis = 30000
	req.MinBytes = 1
	req.SessionEpoch = -1
	rt := kmsg.NewFetchRequestTopic()
	rt.Topic, rt.TopicID = topic, id
	rp := kmsg.NewFetchRequestTopicPartition()
	rp.CurrentLeaderEpoch = -1
	rp.PartitionMaxBytes = 1 << 20
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)

	start := time.Now()
	resp := faultReq(t, cl, 0, req).(*kmsg.FetchResponse)
	if took := time.Since(start); took > 5*time.Second {
		t.Errorf("fetch took %s: a top-level fault must not wait out MaxWait", took)
	}
	if resp.ErrorCode != kerr.FetchSessionTopicIDError.Code {
		t.Errorf("top-level code %d != %d", resp.ErrorCode, kerr.FetchSessionTopicIDError.Code)
	}
	if len(resp.Topics) != 0 {
		t.Errorf("top-level fault answered %d topics", len(resp.Topics))
	}
	if n := h.Hits(); n != 1 {
		t.Errorf("fault fired %d times != 1", n)
	}
}

// A transaction selector reaches every request that names the transaction.
func TestFaultTxnID(t *testing.T) {
	t.Parallel()
	const topic, txnID, group = "t", "x", "g"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c)

	h := c.Fault(Fault{TxnID: txnID, Err: kerr.ProducerFenced, Count: -1})

	init := kmsg.NewPtrInitProducerIDRequest()
	init.TransactionalID = kmsg.StringPtr(txnID)
	init.TransactionTimeoutMillis = 10000
	init.ProducerID, init.ProducerEpoch = -1, -1
	if code := faultReq(t, cl, 0, init).(*kmsg.InitProducerIDResponse).ErrorCode; code != kerr.ProducerFenced.Code {
		t.Errorf("init producer id answered %d", code)
	}

	end := kmsg.NewPtrEndTxnRequest()
	end.TransactionalID = txnID
	end.ProducerEpoch = -1
	if code := faultReq(t, cl, 0, end).(*kmsg.EndTxnResponse).ErrorCode; code != kerr.ProducerFenced.Code {
		t.Errorf("end txn answered %d", code)
	}

	tc := kmsg.NewPtrTxnOffsetCommitRequest()
	tc.TransactionalID = txnID
	tc.Group = group
	tc.ProducerEpoch, tc.Generation = -1, -1
	tct := kmsg.NewTxnOffsetCommitRequestTopic()
	tct.Topic = topic
	tcp := kmsg.NewTxnOffsetCommitRequestTopicPartition()
	tcp.LeaderEpoch = -1
	tct.Partitions = append(tct.Partitions, tcp)
	tc.Topics = append(tc.Topics, tct)
	tcr := faultReq(t, cl, 0, tc).(*kmsg.TxnOffsetCommitResponse)
	if len(tcr.Topics) != 1 || len(tcr.Topics[0].Partitions) != 1 || tcr.Topics[0].Partitions[0].ErrorCode != kerr.ProducerFenced.Code {
		t.Errorf("txn offset commit answered %v", tcr.Topics)
	}

	if n := h.Hits(); n != 3 {
		t.Errorf("fault fired %d times != 3", n)
	}
}

// A node selector fires only at that node.
func TestFaultNode(t *testing.T) {
	t.Parallel()
	const topic = "t"
	c := newCluster(t, NumBrokers(2), SeedTopics(1, topic))
	cl := newPlainClient(t, c)
	if err := c.MoveTopicPartition(topic, 0, 1); err != nil {
		t.Fatalf("move: %v", err)
	}

	h := c.Fault(Fault{Keys: []kmsg.Key{kmsg.ListOffsets}, Nodes: []int32{1}, Topic: topic, Err: kerr.PolicyViolation, Count: -1})
	if code := listOffsetsCode(t, cl, 0, topic, 0); code == kerr.PolicyViolation.Code {
		t.Error("a node 1 fault fired at node 0")
	}
	if code := listOffsetsCode(t, cl, 1, topic, 0); code != kerr.PolicyViolation.Code {
		t.Errorf("node 1 answered %d", code)
	}
	if n := h.Hits(); n != 1 {
		t.Errorf("fault fired %d times != 1", n)
	}
}

// FindCoordinator matches a group key against Group and a transaction key
// against TxnID.
func TestFaultFindCoordinator(t *testing.T) {
	t.Parallel()
	c := newCluster(t, NumBrokers(1))
	cl := newPlainClient(t, c)

	c.Fault(
		Fault{Group: "g", Err: kerr.CoordinatorNotAvailable, Count: -1},
		Fault{TxnID: "x", Err: kerr.CoordinatorLoadInProgress, Count: -1},
	)

	find := func(typ int8, key string) int16 {
		req := kmsg.NewPtrFindCoordinatorRequest()
		req.CoordinatorType = typ
		req.CoordinatorKey = key
		req.CoordinatorKeys = []string{key}
		resp := faultReq(t, cl, 0, req).(*kmsg.FindCoordinatorResponse)
		if len(resp.Coordinators) == 1 {
			return resp.Coordinators[0].ErrorCode
		}
		return resp.ErrorCode
	}
	if code := find(0, "g"); code != kerr.CoordinatorNotAvailable.Code {
		t.Errorf("group key answered %d", code)
	}
	if code := find(1, "x"); code != kerr.CoordinatorLoadInProgress.Code {
		t.Errorf("transaction key answered %d", code)
	}
	if code := find(0, "untouched"); code != 0 {
		t.Errorf("unselected group key answered %d", code)
	}
	if code := find(1, "untouched"); code != 0 {
		t.Errorf("unselected transaction key answered %d", code)
	}
}

// Resource names a config resource.
func TestFaultResourceConfigs(t *testing.T) {
	t.Parallel()
	const t1, t2 = "t1", "t2"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, t1, t2))
	cl := newPlainClient(t, c)

	c.Fault(Fault{Resource: t1, Err: kerr.InvalidConfig, Count: -1})

	req := kmsg.NewPtrDescribeConfigsRequest()
	for _, name := range []string{t1, t2} {
		rr := kmsg.NewDescribeConfigsRequestResource()
		rr.ResourceType = kmsg.ConfigResourceTypeTopic
		rr.ResourceName = name
		req.Resources = append(req.Resources, rr)
	}
	resp := faultReq(t, cl, 0, req).(*kmsg.DescribeConfigsResponse)
	if len(resp.Resources) != 2 {
		t.Fatalf("describe answered %d resources", len(resp.Resources))
	}
	for _, r := range resp.Resources {
		switch r.ResourceName {
		case t1:
			if r.ErrorCode != kerr.InvalidConfig.Code {
				t.Errorf("faulted resource answered %d", r.ErrorCode)
			}
		case t2:
			if r.ErrorCode != 0 {
				t.Errorf("other resource answered %d", r.ErrorCode)
			}
		}
	}
}

// Resource names an ACL creation.
func TestFaultResourceACLs(t *testing.T) {
	t.Parallel()
	c := newCluster(t, NumBrokers(1))
	cl := newPlainClient(t, c)

	c.Fault(Fault{Resource: "faulted", Err: kerr.InvalidRequest, Count: -1})

	req := kmsg.NewPtrCreateACLsRequest()
	for _, name := range []string{"faulted", "fine"} {
		cr := kmsg.NewCreateACLsRequestCreation()
		cr.ResourceType = kmsg.ACLResourceTypeTopic
		cr.ResourceName = name
		cr.ResourcePatternType = kmsg.ACLResourcePatternTypeLiteral
		cr.Principal = "User:someone"
		cr.Host = "*"
		cr.Operation = kmsg.ACLOperationRead
		cr.PermissionType = kmsg.ACLPermissionTypeAllow
		req.Creations = append(req.Creations, cr)
	}
	resp := faultReq(t, cl, 0, req).(*kmsg.CreateACLsResponse)
	if len(resp.Results) != 2 {
		t.Fatalf("create answered %d results", len(resp.Results))
	}
	if resp.Results[0].ErrorCode != kerr.InvalidRequest.Code {
		t.Errorf("faulted creation answered %d", resp.Results[0].ErrorCode)
	}
	if resp.Results[1].ErrorCode != 0 {
		t.Errorf("other creation answered %d", resp.Results[1].ErrorCode)
	}
}

// A selector a request does not carry never matches.
func TestFaultSelectorNotCarried(t *testing.T) {
	t.Parallel()
	const topic, group = "t", "g"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c)
	joinGroup(t, cl, group)

	h := c.Fault(Fault{Topic: topic, Err: kerr.UnknownServerError, Count: -1})

	req := kmsg.NewPtrHeartbeatRequest()
	req.Group = group
	req.Generation = -1
	if code := faultReq(t, cl, 0, req).(*kmsg.HeartbeatResponse).ErrorCode; code == kerr.UnknownServerError.Code {
		t.Error("a topic fault failed a heartbeat")
	}
	if n := h.Hits(); n != 0 {
		t.Errorf("fault fired %d times on a request with no topic", n)
	}
}

// Count budgets requests, and Remove ends a budgetless fault.
func TestFaultCount(t *testing.T) {
	t.Parallel()
	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c)

	once := c.Fault(Fault{Keys: []kmsg.Key{kmsg.ListOffsets}, Topic: topic, Err: kerr.PolicyViolation})
	if code := listOffsetsCode(t, cl, 0, topic, 0); code != kerr.PolicyViolation.Code {
		t.Errorf("first request answered %d", code)
	}
	if code := listOffsetsCode(t, cl, 0, topic, 0); code != 0 {
		t.Errorf("second request answered %d: Count 0 is one request", code)
	}
	if n := once.Hits(); n != 1 {
		t.Errorf("one-shot fault fired %d times", n)
	}
	once.Remove() // removing an exhausted fault is fine

	forever := c.Fault(Fault{Keys: []kmsg.Key{kmsg.ListOffsets}, Topic: topic, Err: kerr.PolicyViolation, Count: -1})
	for i := range 3 {
		if code := listOffsetsCode(t, cl, 0, topic, 0); code != kerr.PolicyViolation.Code {
			t.Errorf("request %d answered %d", i, code)
		}
	}
	forever.Remove()
	forever.Remove() // twice is fine
	if code := listOffsetsCode(t, cl, 0, topic, 0); code != 0 {
		t.Errorf("request after removal answered %d", code)
	}
	if n := forever.Hits(); n != 3 {
		t.Errorf("removed fault fired %d times != 3", n)
	}
}

// One handle covers every fault it installed, and the earliest fault that
// matches answers.
func TestFaultBatch(t *testing.T) {
	t.Parallel()
	const t1, t2 = "t1", "t2"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, t1, t2))
	cl := newPlainClient(t, c)

	h := c.Fault(
		Fault{Topic: t1, Err: kerr.PolicyViolation, Count: -1},
		Fault{Topic: t1, Err: kerr.InvalidRequest, Count: -1},
		Fault{Topic: t2, Err: kerr.NotEnoughReplicas, Count: -1},
	)
	if code := listOffsetsCode(t, cl, 0, t1, 0); code != kerr.PolicyViolation.Code {
		t.Errorf("t1 answered %d, want the fault added first", code)
	}
	if code := listOffsetsCode(t, cl, 0, t2, 0); code != kerr.NotEnoughReplicas.Code {
		t.Errorf("t2 answered %d", code)
	}
	if n := h.Hits(); n != 2 {
		t.Errorf("handle counted %d hits != 2", n)
	}
	h.Remove()
	if code := listOffsetsCode(t, cl, 0, t1, 0); code != 0 {
		t.Errorf("t1 after removal answered %d", code)
	}
	if code := listOffsetsCode(t, cl, 0, t2, 0); code != 0 {
		t.Errorf("t2 after removal answered %d", code)
	}
}

// An idempotent producer retries a timed-out append. The record lands once.
func TestFaultAfterApplyIdempotent(t *testing.T) {
	t.Parallel()
	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c,
		kgo.RetryBackoffFn(func(int) time.Duration { return 50 * time.Millisecond }),
	)

	h := c.Fault(Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: topic, Err: kerr.RequestTimedOut})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r := &kgo.Record{Topic: topic, Value: []byte("v")}
	if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
	if n := h.Hits(); n != 1 {
		t.Errorf("fault fired %d times != 1", n)
	}
	if i := c.PartitionInfo(topic, 0); i.HighWatermark != 1 {
		t.Errorf("high watermark %d != 1: the timed out append was not deduplicated", i.HighWatermark)
	}
	if r.Offset != 0 {
		t.Errorf("record landed at offset %d != 0", r.Offset)
	}
}

// A producer with no producer ID retries a timed-out append. The record
// lands twice.
func TestFaultAfterApplyDuplicates(t *testing.T) {
	t.Parallel()
	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c,
		kgo.DisableIdempotentWrite(),
		kgo.RetryBackoffFn(func(int) time.Duration { return 50 * time.Millisecond }),
	)

	c.Fault(Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: topic, Err: kerr.RequestTimedOut})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("v")}).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
	if i := c.PartitionInfo(topic, 0); i.HighWatermark != 2 {
		t.Errorf("high watermark %d != 2: the timed out append did not happen", i.HighWatermark)
	}
}

// NOT_ENOUGH_REPLICAS_AFTER_APPEND is the produce error a broker answers
// after appending.
func TestFaultAfterApplyNotEnoughReplicas(t *testing.T) {
	t.Parallel()
	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c,
		kgo.RetryBackoffFn(func(int) time.Duration { return 50 * time.Millisecond }),
	)

	c.Fault(Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: topic, Err: kerr.NotEnoughReplicasAfterAppend})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("v")}).FirstErr(); err != nil {
		t.Fatalf("produce: %v", err)
	}
	if i := c.PartitionInfo(topic, 0); i.HighWatermark != 1 {
		t.Errorf("high watermark %d != 1", i.HighWatermark)
	}
}

// A timed-out commit is stored. The next fetch reads it.
func TestFaultAfterApplyOffsetCommit(t *testing.T) {
	t.Parallel()
	const topic, group = "t", "g"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c)
	id := c.TopicInfo(topic).TopicID

	h := c.Fault(Fault{Keys: []kmsg.Key{kmsg.OffsetCommit}, Group: group, Err: kerr.RequestTimedOut})

	commit := kmsg.NewPtrOffsetCommitRequest()
	commit.Group = group
	commit.Generation = -1
	ct := kmsg.NewOffsetCommitRequestTopic()
	ct.Topic, ct.TopicID = topic, id
	cp := kmsg.NewOffsetCommitRequestTopicPartition()
	cp.Offset, cp.LeaderEpoch = 7, -1
	ct.Partitions = append(ct.Partitions, cp)
	commit.Topics = append(commit.Topics, ct)
	cresp := faultReq(t, cl, 0, commit).(*kmsg.OffsetCommitResponse)
	if len(cresp.Topics) != 1 || len(cresp.Topics[0].Partitions) != 1 {
		t.Fatalf("commit answered %d topics", len(cresp.Topics))
	}
	if code := cresp.Topics[0].Partitions[0].ErrorCode; code != kerr.RequestTimedOut.Code {
		t.Errorf("commit answered %d, want request timed out", code)
	}
	if n := h.Hits(); n != 1 {
		t.Errorf("fault fired %d times != 1", n)
	}

	fetch := kmsg.NewPtrOffsetFetchRequest()
	fg := kmsg.NewOffsetFetchRequestGroup()
	fg.Group = group
	fg.MemberEpoch = -1
	ft := kmsg.NewOffsetFetchRequestGroupTopic()
	ft.Topic, ft.TopicID = topic, id
	ft.Partitions = []int32{0}
	fg.Topics = append(fg.Topics, ft)
	fetch.Groups = append(fetch.Groups, fg)
	fresp := faultReq(t, cl, 0, fetch).(*kmsg.OffsetFetchResponse)
	if len(fresp.Groups) != 1 || len(fresp.Groups[0].Topics) != 1 || len(fresp.Groups[0].Topics[0].Partitions) != 1 {
		t.Fatalf("fetch answered %v", fresp.Groups)
	}
	if offset := fresp.Groups[0].Topics[0].Partitions[0].Offset; offset != 7 {
		t.Errorf("fetched offset %d != 7: the timed out commit was not stored", offset)
	}
}

// Wait blocks until the faults have answered.
func TestFaultWait(t *testing.T) {
	t.Parallel()
	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c)

	h := c.Fault(Fault{Keys: []kmsg.Key{kmsg.ListOffsets}, Topic: topic, Err: kerr.PolicyViolation, Count: 2})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Wait blocks until a request lands.
	done := make(chan error, 1)
	go func() { done <- h.Wait(ctx, 1) }()
	select {
	case err := <-done:
		t.Fatalf("Wait returned %v before any request", err)
	case <-time.After(50 * time.Millisecond):
	}
	listOffsetsCode(t, cl, 0, topic, 0)
	if err := <-done; err != nil {
		t.Fatalf("Wait: %v", err)
	}

	// A hit that already happened satisfies Wait at once.
	if err := h.Wait(ctx, 1); err != nil {
		t.Fatalf("Wait on a hit that already landed: %v", err)
	}

	// Waiting for the whole budget returns when the fault is used up.
	listOffsetsCode(t, cl, 0, topic, 0)
	if err := h.Wait(ctx, 2); err != nil {
		t.Fatalf("Wait for the budget: %v", err)
	}
	if code := listOffsetsCode(t, cl, 0, topic, 0); code != 0 {
		t.Errorf("request after the budget answered %d", code)
	}

	// A context that is done ends the wait.
	dead, deadCancel := context.WithCancel(context.Background())
	deadCancel()
	if err := h.Wait(dead, 3); !errors.Is(err, context.Canceled) {
		t.Errorf("Wait on a canceled context: %v", err)
	}
}

// A timed-out delete still deletes the records.
func TestFaultAfterApplyDeleteRecords(t *testing.T) {
	t.Parallel()
	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for range 3 {
		if err := cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("v")}).FirstErr(); err != nil {
			t.Fatalf("produce: %v", err)
		}
	}

	h := c.Fault(Fault{Keys: []kmsg.Key{kmsg.DeleteRecords}, Topic: topic, Err: kerr.RequestTimedOut})

	del := kmsg.NewPtrDeleteRecordsRequest()
	dt := kmsg.NewDeleteRecordsRequestTopic()
	dt.Topic = topic
	dp := kmsg.NewDeleteRecordsRequestTopicPartition()
	dp.Offset = 2
	dt.Partitions = append(dt.Partitions, dp)
	del.Topics = append(del.Topics, dt)
	dresp := faultReq(t, cl, 0, del).(*kmsg.DeleteRecordsResponse)
	if len(dresp.Topics) != 1 || len(dresp.Topics[0].Partitions) != 1 {
		t.Fatalf("delete answered %v", dresp.Topics)
	}
	if code := dresp.Topics[0].Partitions[0].ErrorCode; code != kerr.RequestTimedOut.Code {
		t.Errorf("delete answered %d, want request timed out", code)
	}
	if n := h.Hits(); n != 1 {
		t.Errorf("fault fired %d times != 1", n)
	}
	if o := c.PartitionInfo(topic, 0).LogStartOffset; o != 2 {
		t.Errorf("log start offset %d != 2: the timed out delete did not happen", o)
	}
}

// A timed-out alter still sets the config.
func TestFaultAfterApplyAlterConfigs(t *testing.T) {
	t.Parallel()
	const topic, name, value = "t", "max.message.bytes", "999999"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c)

	h := c.Fault(Fault{Keys: []kmsg.Key{kmsg.IncrementalAlterConfigs}, Resource: topic, Err: kerr.RequestTimedOut})

	alter := kmsg.NewPtrIncrementalAlterConfigsRequest()
	ar := kmsg.NewIncrementalAlterConfigsRequestResource()
	ar.ResourceType = kmsg.ConfigResourceTypeTopic
	ar.ResourceName = topic
	ac := kmsg.NewIncrementalAlterConfigsRequestResourceConfig()
	ac.Name, ac.Value = name, kmsg.StringPtr(value)
	ar.Configs = append(ar.Configs, ac)
	alter.Resources = append(alter.Resources, ar)
	aresp := faultReq(t, cl, 0, alter).(*kmsg.IncrementalAlterConfigsResponse)
	if len(aresp.Resources) != 1 {
		t.Fatalf("alter answered %d resources != 1", len(aresp.Resources))
	}
	if code := aresp.Resources[0].ErrorCode; code != kerr.RequestTimedOut.Code {
		t.Errorf("alter answered %d, want request timed out", code)
	}
	if n := h.Hits(); n != 1 {
		t.Errorf("fault fired %d times != 1", n)
	}

	describe := kmsg.NewPtrDescribeConfigsRequest()
	dr := kmsg.NewDescribeConfigsRequestResource()
	dr.ResourceType = kmsg.ConfigResourceTypeTopic
	dr.ResourceName = topic
	describe.Resources = append(describe.Resources, dr)
	dresp := faultReq(t, cl, 0, describe).(*kmsg.DescribeConfigsResponse)
	if len(dresp.Resources) != 1 {
		t.Fatalf("describe answered %d resources != 1", len(dresp.Resources))
	}
	var got string
	for _, cfg := range dresp.Resources[0].Configs {
		if cfg.Name == name && cfg.Value != nil {
			got = *cfg.Value
		}
	}
	if got != value {
		t.Errorf("%s is %q != %q: the timed out alter did not happen", name, got, value)
	}
}

// initTxn starts a transactional producer and returns its ID and epoch.
func initTxn(t *testing.T, cl *kgo.Client, txnID string) (int64, int16) {
	t.Helper()
	req := kmsg.NewPtrInitProducerIDRequest()
	req.TransactionalID = kmsg.StringPtr(txnID)
	req.TransactionTimeoutMillis = 10000
	req.ProducerID, req.ProducerEpoch = -1, -1
	resp := faultReq(t, cl, 0, req).(*kmsg.InitProducerIDResponse)
	if resp.ErrorCode != 0 {
		t.Fatalf("init producer id answered %d", resp.ErrorCode)
	}
	return resp.ProducerID, resp.ProducerEpoch
}

// A timed-out transactional commit still stores the offsets. Committing the
// transaction makes them visible.
func TestFaultAfterApplyTxnOffsetCommit(t *testing.T) {
	t.Parallel()
	const topic, txnID, group = "t", "x", "g"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c)
	id := c.TopicInfo(topic).TopicID

	pid, epoch := initTxn(t, cl, txnID)

	add := kmsg.NewPtrAddOffsetsToTxnRequest()
	add.TransactionalID = txnID
	add.ProducerID, add.ProducerEpoch = pid, epoch
	add.Group = group
	if code := faultReq(t, cl, 0, add).(*kmsg.AddOffsetsToTxnResponse).ErrorCode; code != 0 {
		t.Fatalf("add offsets answered %d", code)
	}

	h := c.Fault(Fault{Keys: []kmsg.Key{kmsg.TxnOffsetCommit}, Group: group, Err: kerr.RequestTimedOut})

	commit := kmsg.NewPtrTxnOffsetCommitRequest()
	commit.TransactionalID = txnID
	commit.Group = group
	commit.ProducerID, commit.ProducerEpoch = pid, epoch
	commit.Generation = -1
	ct := kmsg.NewTxnOffsetCommitRequestTopic()
	ct.Topic = topic
	cp := kmsg.NewTxnOffsetCommitRequestTopicPartition()
	cp.Offset, cp.LeaderEpoch = 7, -1
	ct.Partitions = append(ct.Partitions, cp)
	commit.Topics = append(commit.Topics, ct)
	cresp := faultReq(t, cl, 0, commit).(*kmsg.TxnOffsetCommitResponse)
	if len(cresp.Topics) != 1 || len(cresp.Topics[0].Partitions) != 1 {
		t.Fatalf("txn offset commit answered %v", cresp.Topics)
	}
	if code := cresp.Topics[0].Partitions[0].ErrorCode; code != kerr.RequestTimedOut.Code {
		t.Errorf("txn offset commit answered %d, want request timed out", code)
	}
	if n := h.Hits(); n != 1 {
		t.Errorf("fault fired %d times != 1", n)
	}

	end := kmsg.NewPtrEndTxnRequest()
	end.TransactionalID = txnID
	end.ProducerID, end.ProducerEpoch = pid, epoch
	end.Commit = true
	if code := faultReq(t, cl, 0, end).(*kmsg.EndTxnResponse).ErrorCode; code != 0 {
		t.Fatalf("end txn answered %d", code)
	}

	fetch := kmsg.NewPtrOffsetFetchRequest()
	fg := kmsg.NewOffsetFetchRequestGroup()
	fg.Group = group
	fg.MemberEpoch = -1
	ft := kmsg.NewOffsetFetchRequestGroupTopic()
	ft.Topic, ft.TopicID = topic, id
	ft.Partitions = []int32{0}
	fg.Topics = append(fg.Topics, ft)
	fetch.Groups = append(fetch.Groups, fg)
	fresp := faultReq(t, cl, 0, fetch).(*kmsg.OffsetFetchResponse)
	if len(fresp.Groups) != 1 || len(fresp.Groups[0].Topics) != 1 || len(fresp.Groups[0].Topics[0].Partitions) != 1 {
		t.Fatalf("offset fetch answered %v", fresp.Groups)
	}
	if offset := fresp.Groups[0].Topics[0].Partitions[0].Offset; offset != 7 {
		t.Errorf("fetched offset %d != 7: the timed out commit was not stored", offset)
	}
}

// A control function can install a fault. It applies to the requests that
// follow.
func TestFaultFromControl(t *testing.T) {
	t.Parallel()
	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c)

	var (
		seen atomic.Int64
		h    atomic.Pointer[FaultHandle]
	)
	c.ControlKey(int16(kmsg.Produce), func(kmsg.Request) (kmsg.Response, error, bool) {
		if seen.Add(1) == 2 {
			h.Store(c.Fault(Fault{Keys: []kmsg.Key{kmsg.Produce}, Topic: topic, Err: kerr.PolicyViolation, Count: -1}))
		}
		return nil, nil, false
	})

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	produce := func() error {
		return cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("v")}).FirstErr()
	}
	if err := produce(); err != nil {
		t.Fatalf("produce before the fault: %v", err)
	}
	_ = produce() // the control installs the fault while handling this one
	if err := produce(); !errors.Is(err, kerr.PolicyViolation) {
		t.Errorf("produce after the fault: %v, want policy violation", err)
	}
	hh := h.Load()
	if hh == nil {
		t.Fatal("the control never installed the fault")
	}
	if n := hh.Hits(); n == 0 {
		t.Error("the fault never fired")
	}
}

// A control installs a fault while sleeping. It does not deadlock and the
// fault applies.
func TestFaultFromSleepingControl(t *testing.T) {
	t.Parallel()
	const topic = "t"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	cl := newPlainClient(t, c)

	var (
		seen atomic.Int64
		h    atomic.Pointer[FaultHandle]
	)
	c.ControlKey(int16(kmsg.ListOffsets), func(kmsg.Request) (kmsg.Response, error, bool) {
		if seen.Add(1) == 1 {
			c.SleepControl(func() {
				h.Store(c.Fault(Fault{Keys: []kmsg.Key{kmsg.ListOffsets}, Topic: topic, Err: kerr.PolicyViolation, Count: -1}))
			})
		}
		return nil, nil, false
	})

	listOffsetsCode(t, cl, 0, topic, 0) // the slept control answers rather than hanging
	if code := listOffsetsCode(t, cl, 0, topic, 0); code != kerr.PolicyViolation.Code {
		t.Errorf("list offsets after the fault answered %d", code)
	}
	hh := h.Load()
	if hh == nil {
		t.Fatal("the control never installed the fault")
	}
	if n := hh.Hits(); n == 0 {
		t.Error("the fault never fired")
	}
}

// A timed-out delete still deletes the group.
func TestFaultAfterApplyDeleteGroups(t *testing.T) {
	t.Parallel()
	const group = "g"
	c := newCluster(t, NumBrokers(1))
	cl := newPlainClient(t, c)
	joinGroup(t, cl, group)

	h := c.Fault(Fault{Keys: []kmsg.Key{kmsg.DeleteGroups}, Group: group, Err: kerr.RequestTimedOut})

	del := kmsg.NewPtrDeleteGroupsRequest()
	del.Groups = []string{group}
	dresp := faultReq(t, cl, 0, del).(*kmsg.DeleteGroupsResponse)
	if len(dresp.Groups) != 1 {
		t.Fatalf("delete answered %d groups", len(dresp.Groups))
	}
	if code := dresp.Groups[0].ErrorCode; code != kerr.RequestTimedOut.Code {
		t.Errorf("delete answered %d, want request timed out", code)
	}
	if n := h.Hits(); n != 1 {
		t.Errorf("fault fired %d times != 1", n)
	}

	list := faultReq(t, cl, 0, kmsg.NewPtrListGroupsRequest()).(*kmsg.ListGroupsResponse)
	for _, g := range list.Groups {
		if g.Group == group {
			t.Error("the group still exists: the timed out delete did not happen")
		}
	}
}

// A timed-out create still creates the topic.
func TestFaultAfterApplyCreateTopics(t *testing.T) {
	t.Parallel()
	const topic = "t"
	c := newCluster(t, NumBrokers(1))
	cl := newPlainClient(t, c)

	h := c.Fault(Fault{Keys: []kmsg.Key{kmsg.CreateTopics}, Topic: topic, Err: kerr.RequestTimedOut})

	create := kmsg.NewPtrCreateTopicsRequest()
	ct := kmsg.NewCreateTopicsRequestTopic()
	ct.Topic = topic
	ct.NumPartitions = 1
	ct.ReplicationFactor = 1
	create.Topics = append(create.Topics, ct)
	cresp := faultReq(t, cl, 0, create).(*kmsg.CreateTopicsResponse)
	if len(cresp.Topics) != 1 {
		t.Fatalf("create answered %d topics", len(cresp.Topics))
	}
	if code := cresp.Topics[0].ErrorCode; code != kerr.RequestTimedOut.Code {
		t.Errorf("create answered %d, want request timed out", code)
	}
	if n := h.Hits(); n != 1 {
		t.Errorf("fault fired %d times != 1", n)
	}
	if c.TopicInfo(topic) == nil {
		t.Error("the topic does not exist: the timed out create did not happen")
	}
}
