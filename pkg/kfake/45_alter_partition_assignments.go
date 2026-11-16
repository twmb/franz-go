package kfake

import (
	"slices"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// AlterPartitionAssignments: v0-1
//
// KIP-455: Admin API to reassign partitions.
//
// Behavior:
// * ACL: ALTER on CLUSTER
// * If Replicas is null, this is a cancel request and returns NO_REASSIGNMENT_IN_PROGRESS
//   (kfake never has reassignments in progress)
// * A reassignment completes at once. Replicas are not tracked per partition
//   (they follow the leader), so the only effect is that a leader outside the
//   new replicas hands off to the first of them
//
// Version notes:
// * v1: AllowReplicationFactorChange

func init() { regKey(45, 0, 1) }

func (c *Cluster) handleAlterPartitionAssignments(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.AlterPartitionAssignmentsRequest)
	resp := req.ResponseKind().(*kmsg.AlterPartitionAssignmentsResponse)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	// A request-level error goes at the top level and on every
	// partition, as Kafka does.
	errResp := func(code int16) (kmsg.Response, error) {
		resp.ErrorCode = code
		for _, rt := range req.Topics {
			st := kmsg.NewAlterPartitionAssignmentsResponseTopic()
			st.Topic = rt.Topic
			for _, rp := range rt.Partitions {
				sp := kmsg.NewAlterPartitionAssignmentsResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = code
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil
	}

	if e := c.denyCluster(creq, kmsg.ACLOperationAlter, faultKey{}); e != nil {
		if creq.skipsWork(e) {
			return errResp(e.Code)
		}
		resp.ErrorCode = e.Code
	}
	if len(req.Topics) == 0 {
		return resp, nil
	}
	if req.TimeoutMillis <= 0 { // the deadline passed before the controller ran it
		return errResp(kerr.RequestTimedOut.Code)
	}
	resp.AllowReplicationFactorChange = req.AllowReplicationFactorChange
	defer c.persistTopicsState() // the leaders and epochs we move

	for _, rt := range req.Topics {
		st := kmsg.NewAlterPartitionAssignmentsResponseTopic()
		st.Topic = rt.Topic

		t := c.data.tps[rt.Topic]
		rf := min(c.data.treplicas[rt.Topic], len(c.bs))
		for _, rp := range rt.Partitions {
			sp := kmsg.NewAlterPartitionAssignmentsResponseTopicPartition()
			sp.Partition = rp.Partition

			e := creq.faults.check(faultKey{topic: rt.Topic}.part(rp.Partition))
			pd, ok := t[rp.Partition]
			switch {
			case e != nil && creq.skipsWork(e):
			case !ok:
				sp.ErrorCode = kerr.UnknownTopicOrPartition.Code
			case rp.Replicas == nil:
				// Null replicas means cancel reassignment; kfake never has
				// any reassignments in progress.
				sp.ErrorCode = kerr.NoReassignmentInProgress.Code
			case !c.validReplicas(rp.Replicas):
				sp.ErrorCode = kerr.InvalidReplicaAssignment.Code
			case !req.AllowReplicationFactorChange && len(rp.Replicas) != rf:
				sp.ErrorCode = kerr.InvalidReplicationFactor.Code
			case !slices.Contains(rp.Replicas, pd.leader.node):
				c.setLeader(rt.Topic, rp.Partition, pd, c.nodeBroker(rp.Replicas[0]))
			}
			if e != nil {
				sp.ErrorCode = e.Code
			}
			st.Partitions = append(st.Partitions, sp)
		}

		resp.Topics = append(resp.Topics, st)
	}

	return resp, nil
}
