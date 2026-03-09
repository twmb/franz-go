package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// AlterShareGroupOffsets: v0 (KIP-932)
//
// Resets the SPSO for partitions in an empty share group. Clears all
// in-flight record state and delivery counts for altered partitions.

func init() { regKey(91, 0, 0) }

func (c *Cluster) handleAlterShareGroupOffsets(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.AlterShareGroupOffsetsRequest)
	resp := req.ResponseKind().(*kmsg.AlterShareGroupOffsetsResponse)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	// ACL: require GROUP READ (Kafka uses READ, not ALTER).
	if !c.allowedACL(creq, req.GroupID, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationRead) {
		resp.ErrorCode = kerr.GroupAuthorizationFailed.Code
		return resp, nil
	}

	sg := c.shareGroups.gs[req.GroupID]
	if sg == nil {
		resp.ErrorCode = kerr.GroupIDNotFound.Code
		return resp, nil
	}

	// Pre-lookup topic IDs and valid partitions while in run()
	// where c.data is safe to read. The waitControl closure runs
	// in manage(); c.data could be mutated by admin callbacks that
	// waitControl drains while blocked.
	type alterTopicInfo struct {
		id    uuid
		valid map[int32]struct{}
	}
	topicInfo := make(map[string]alterTopicInfo, len(req.Topics))
	for _, rt := range req.Topics {
		info := alterTopicInfo{id: c.data.t2id[rt.Topic], valid: make(map[int32]struct{})}
		for _, rp := range rt.Partitions {
			if _, ok := c.data.tps.getp(rt.Topic, rp.Partition); ok {
				info.valid[rp.Partition] = struct{}{}
			}
		}
		topicInfo[rt.Topic] = info
	}

	if !sg.waitControl(func() {
		if len(sg.members) > 0 {
			resp.ErrorCode = kerr.NonEmptyGroup.Code
			return
		}

		sg.mu.Lock()
		for i := range req.Topics {
			rt := &req.Topics[i]
			rst := kmsg.NewAlterShareGroupOffsetsResponseTopic()
			rst.Topic = rt.Topic
			rst.TopicID = topicInfo[rt.Topic].id

			for j := range rt.Partitions {
				rp := &rt.Partitions[j]
				rsp := kmsg.NewAlterShareGroupOffsetsResponseTopicPartition()
				rsp.Partition = rp.Partition

				if _, ok := topicInfo[rt.Topic].valid[rp.Partition]; !ok {
					rsp.ErrorCode = kerr.UnknownTopicOrPartition.Code
					rst.Partitions = append(rst.Partitions, rsp)
					continue
				}

				// Reset SPSO, scan cursor, and all record state.
				sp, ok := sg.partitions.getp(rt.Topic, rp.Partition)
				if ok {
					sp.spso = rp.StartOffset
					sp.scanOffset = rp.StartOffset
					sp.records = make(map[int64]*shareRecord)
				} else {
					sg.partitions.mkp(rt.Topic, rp.Partition, func() *sharePartition {
						return &sharePartition{
							spso:       rp.StartOffset,
							scanOffset: rp.StartOffset,
							records:    make(map[int64]*shareRecord),
						}
					})
				}

				rst.Partitions = append(rst.Partitions, rsp)
			}

			resp.Topics = append(resp.Topics, rst)
		}
		sg.mu.Unlock()
	}) {
		resp.ErrorCode = kerr.GroupIDNotFound.Code
	}

	return resp, nil
}
