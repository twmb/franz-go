package kfake

import (
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ListPartitionReassignments: v0
//
// KIP-455: Admin API to list in-progress partition reassignments.
//
// Behavior:
// * ACL: DESCRIBE on CLUSTER
// * Since kfake never has reassignments in progress, always returns empty
//
// Version notes:
// * v0 only

func init() { regKey(46, 0, 0) }

func (c *Cluster) handleListPartitionReassignments(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.ListPartitionReassignmentsRequest)
	resp := req.ResponseKind().(*kmsg.ListPartitionReassignmentsResponse)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	// A denial echoes each requested partition, as Kafka does.
	if e := c.denyCluster(creq, kmsg.ACLOperationDescribe, faultKey{}); e != nil {
		resp.ErrorCode = e.Code
		for _, rt := range req.Topics {
			st := kmsg.NewListPartitionReassignmentsResponseTopic()
			st.Topic = rt.Topic
			for _, p := range rt.Partitions {
				sp := kmsg.NewListPartitionReassignmentsResponseTopicPartition()
				sp.Partition = p
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil
	}

	// kfake never has any reassignments in progress, so we always return
	// an empty Topics slice. The response Topics field is already nil/empty
	// by default, which is correct.

	return resp, nil
}
