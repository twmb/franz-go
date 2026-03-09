package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// DescribeShareGroupOffsets: v0-1 (KIP-932, KIP-1226)
//
// Returns the Share-Partition Start Offset (SPSO) and lag for each
// requested partition. Lag (v1+) is the difference between the high
// watermark and the SPSO.

func init() { regKey(90, 0, 1) }

func (c *Cluster) handleDescribeShareGroupOffsets(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.DescribeShareGroupOffsetsRequest)
	resp := req.ResponseKind().(*kmsg.DescribeShareGroupOffsetsResponse)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	for i := range req.Groups {
		rg := &req.Groups[i]
		rsg := kmsg.NewDescribeShareGroupOffsetsResponseGroup()
		rsg.GroupID = rg.GroupID

		// ACL: require GROUP DESCRIBE.
		if !c.allowedACL(creq, rg.GroupID, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationDescribe) {
			rsg.ErrorCode = kerr.GroupAuthorizationFailed.Code
			resp.Groups = append(resp.Groups, rsg)
			continue
		}

		sg := c.shareGroups.gs[rg.GroupID]
		if sg == nil {
			rsg.ErrorCode = kerr.GroupIDNotFound.Code
		} else {
			sg.mu.Lock()
		}
		for j := range rg.Topics {
			rt := &rg.Topics[j]
			rst := kmsg.NewDescribeShareGroupOffsetsResponseGroupTopic()
			rst.Topic = rt.Topic
			rst.TopicID = c.data.t2id[rt.Topic]

			// ACL: per-topic DESCRIBE check.
			if !c.allowedACL(creq, rt.Topic, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationDescribe) {
				for _, partition := range rt.Partitions {
					rsp := kmsg.NewDescribeShareGroupOffsetsResponseGroupTopicPartition()
					rsp.Partition = partition
					rsp.ErrorCode = kerr.TopicAuthorizationFailed.Code
					rsp.StartOffset = -1
					rst.Partitions = append(rst.Partitions, rsp)
				}
				rsg.Topics = append(rsg.Topics, rst)
				continue
			}

			for _, partition := range rt.Partitions {
				rsp := kmsg.NewDescribeShareGroupOffsetsResponseGroupTopicPartition()
				rsp.Partition = partition

				pd, ok := c.data.tps.getp(rt.Topic, partition)
				if !ok {
					rsp.ErrorCode = kerr.UnknownTopicOrPartition.Code
					rsp.StartOffset = -1
					rst.Partitions = append(rst.Partitions, rsp)
					continue
				}

				rsp.LeaderEpoch = pd.epoch
				if sg == nil {
					// Group doesn't exist -- no share state.
					rsp.StartOffset = -1
				} else if sp, ok := sg.partitions.getp(rt.Topic, partition); !ok {
					// No share state yet -- SPSO not initialized.
					rsp.StartOffset = -1
				} else {
					rsp.StartOffset = sp.spso
					lag := pd.highWatermark - sp.spso
					if lag < 0 {
						lag = 0
					}
					rsp.Lag = lag
				}

				rst.Partitions = append(rst.Partitions, rsp)
			}

			rsg.Topics = append(rsg.Topics, rst)
		}
		if sg != nil {
			sg.mu.Unlock()
		}

		resp.Groups = append(resp.Groups, rsg)
	}

	return resp, nil
}
