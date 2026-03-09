package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// DeleteShareGroupOffsets: v0 (KIP-932)
//
// Removes all share partition state for the specified topics in an
// empty share group.

func init() { regKey(92, 0, 0) }

func (c *Cluster) handleDeleteShareGroupOffsets(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.DeleteShareGroupOffsetsRequest)
	resp := req.ResponseKind().(*kmsg.DeleteShareGroupOffsetsResponse)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	sg := c.shareGroups.gs[req.GroupID]
	if sg == nil {
		resp.ErrorCode = kerr.GroupIDNotFound.Code
		return resp, nil
	}

	// Pre-lookup topic IDs while in run() where c.data is safe.
	t2id := make(map[string]uuid, len(req.Topics))
	for _, rt := range req.Topics {
		t2id[rt.Topic] = c.data.t2id[rt.Topic]
	}

	if !sg.waitControl(func() {
		if len(sg.members) > 0 {
			resp.ErrorCode = kerr.NonEmptyGroup.Code
			return
		}

		sg.mu.Lock()
		for i := range req.Topics {
			rt := &req.Topics[i]
			rst := kmsg.NewDeleteShareGroupOffsetsResponseTopic()
			rst.Topic = rt.Topic
			rst.TopicID = t2id[rt.Topic]
			delete(sg.partitions, rt.Topic)
			resp.Topics = append(resp.Topics, rst)
		}
		sg.mu.Unlock()
	}) {
		resp.ErrorCode = kerr.GroupIDNotFound.Code
	}

	return resp, nil
}
