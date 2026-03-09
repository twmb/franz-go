package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ShareAcknowledge: v0-2 (KIP-932, KIP-1222)

func init() { regKey(79, 0, 2) }

func (c *Cluster) handleShareAcknowledge(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.ShareAcknowledgeRequest)
	resp := req.ResponseKind().(*kmsg.ShareAcknowledgeResponse)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	groupID := ""
	if req.GroupID != nil {
		groupID = *req.GroupID
	}

	sg := c.shareGroups.gs[groupID]
	if sg == nil {
		resp.ErrorCode = kerr.GroupIDNotFound.Code
		return resp, nil
	}

	id2t := c.data.id2t
	topicIdx := make(map[uuid]int)

	sg.mu.Lock()
	for i := range req.Topics {
		rt := &req.Topics[i]
		topicName := id2t[rt.TopicID]
		if topicName == "" {
			continue
		}

		for j := range rt.Partitions {
			rp := &rt.Partitions[j]

			pd, ok := c.data.tps.getp(topicName, rp.Partition)
			if !ok {
				idx := getOrAddShareAckTopic(resp, topicIdx, rt.TopicID)
				sp := kmsg.NewShareAcknowledgeResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = kerr.UnknownTopicOrPartition.Code
				resp.Topics[idx].Partitions = append(resp.Topics[idx].Partitions, sp)
				continue
			}

			shp := sg.getSharePartition(topicName, rp.Partition, pd)
			errCode := shp.processStandaloneAcks(rp.AcknowledgementBatches)

			idx := getOrAddShareAckTopic(resp, topicIdx, rt.TopicID)
			sp := kmsg.NewShareAcknowledgeResponseTopicPartition()
			sp.Partition = rp.Partition
			sp.ErrorCode = errCode
			resp.Topics[idx].Partitions = append(resp.Topics[idx].Partitions, sp)
		}
	}
	sg.mu.Unlock()

	return resp, nil
}

func getOrAddShareAckTopic(resp *kmsg.ShareAcknowledgeResponse, idx map[uuid]int, tid uuid) int {
	i, ok := idx[tid]
	if !ok {
		i = len(resp.Topics)
		idx[tid] = i
		t := kmsg.NewShareAcknowledgeResponseTopic()
		t.TopicID = tid
		resp.Topics = append(resp.Topics, t)
	}
	return i
}
