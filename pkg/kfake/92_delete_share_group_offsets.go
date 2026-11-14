package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// DeleteShareGroupOffsets: v0 (KIP-932)
//
// Behavior:
// * Removes all share partition state for specified topics
// * Only works on empty groups (no active members)
// * The group stays, as in Kafka

func init() { regKey(92, 0, 0) }

func (c *Cluster) handleDeleteShareGroupOffsets(creq *clientReq) (kmsg.Response, error) {
	var (
		req  = creq.kreq.(*kmsg.DeleteShareGroupOffsetsRequest)
		resp = req.ResponseKind().(*kmsg.DeleteShareGroupOffsetsResponse)
	)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	// ACL: require GROUP DELETE.
	if e := c.deny(creq, req.GroupID, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationDelete, faultKey{group: req.GroupID, misrouted: !c.isCoordinator(creq, req.GroupID)}); e != nil {
		resp.ErrorCode = e.Code
		if creq.skipsWork(e) { // a timed-out delete still deletes
			return resp, nil
		}
	}
	if ke := c.validateGroup(creq, req.GroupID); ke != nil {
		resp.ErrorCode = ke.Code
		return resp, nil
	}

	sg := c.shareGroups.get(req.GroupID)
	if sg == nil {
		resp.ErrorCode = kerr.GroupIDNotFound.Code
		return resp, nil
	}

	if len(sg.members) > 0 {
		resp.ErrorCode = kerr.NonEmptyGroup.Code
		return resp, nil
	}

	for i := range req.Topics {
		rt := &req.Topics[i]
		rst := kmsg.NewDeleteShareGroupOffsetsResponseTopic()
		rst.Topic = rt.Topic
		id := c.data.t2id[rt.Topic]
		rst.TopicID = id

		e := c.deny(creq, rt.Topic, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationRead, faultKey{group: req.GroupID, topic: rt.Topic, topicID: id})
		if e != nil {
			rst.ErrorCode = e.Code
			if creq.skipsWork(e) { // a timed-out delete still deletes
				rst.TopicID = uuid{} // Kafka names no ID for a topic it refuses
				resp.Topics = append(resp.Topics, rst)
				continue
			}
		}
		_, exists := c.data.tps[rt.Topic]
		_, hasState := sg.partitions[rt.Topic]
		switch {
		case e != nil:
		case !exists:
			rst.ErrorCode = kerr.UnknownTopicOrPartition.Code
		case !hasState:
			rst.ErrorCode = kerr.UnknownTopicOrPartition.Code
			rst.ErrorMessage = kmsg.StringPtr("There is no offset information to delete.")
		}
		delete(sg.partitions, rt.Topic)
		resp.Topics = append(resp.Topics, rst)
	}

	return resp, nil
}
