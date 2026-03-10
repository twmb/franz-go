package kfake

import (
	"math"

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

	resp.AcquisitionLockTimeoutMillis = c.shareRecordLockDurationMs()

	groupID := ""
	if req.GroupID != nil {
		groupID = *req.GroupID
	}
	memberID := ""
	if req.MemberID != nil {
		memberID = *req.MemberID
	}

	// ACL: require GROUP READ.
	if !c.allowedACL(creq, groupID, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationRead) {
		resp.ErrorCode = kerr.GroupAuthorizationFailed.Code
		return resp, nil
	}

	// Validate memberID format (non-empty, <=36 chars).
	if memberID == "" || len(memberID) > 36 {
		resp.ErrorCode = kerr.InvalidRequest.Code
		return resp, nil
	}

	sg := c.shareGroups.gs[groupID]
	if sg == nil {
		resp.ErrorCode = kerr.GroupIDNotFound.Code
		return resp, nil
	}

	// Session epoch validation. ShareAcknowledge uses the same share
	// session as ShareFetch: the client tracks a single epoch that
	// increments on each successful ShareFetch or ShareAcknowledge.
	sessionKey := shareSessionKey{
		group:    groupID,
		memberID: memberID,
		broker:   creq.cc.b.node,
	}

	if req.ShareSessionEpoch == -1 {
		// Close session -- still process the acks below.
		defer delete(c.shareSessions, sessionKey)
	} else if req.ShareSessionEpoch == 0 {
		// Epoch 0 is for opening a new session via ShareFetch, not
		// ShareAcknowledge.
		resp.ErrorCode = kerr.InvalidShareSessionEpoch.Code
		return resp, nil
	} else {
		session := c.shareSessions[sessionKey]
		if session == nil {
			resp.ErrorCode = kerr.ShareSessionNotFound.Code
			return resp, nil
		}
		if req.ShareSessionEpoch != session.epoch {
			resp.ErrorCode = kerr.InvalidShareSessionEpoch.Code
			return resp, nil
		}
	}

	supportsRenew := req.Version >= 2
	id2t := c.data.id2t
	topicIdx := make(map[uuid]int)

	sg.mu.Lock()
	for i := range req.Topics {
		rt := &req.Topics[i]
		topicName := id2t[rt.TopicID]
		if topicName == "" {
			continue
		}

		// ACL: per-topic READ check.
		if !c.allowedACL(creq, topicName, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationRead) {
			idx := getOrAddShareAckTopic(resp, topicIdx, rt.TopicID)
			for j := range rt.Partitions {
				sp := kmsg.NewShareAcknowledgeResponseTopicPartition()
				sp.Partition = rt.Partitions[j].Partition
				sp.ErrorCode = kerr.TopicAuthorizationFailed.Code
				resp.Topics[idx].Partitions = append(resp.Topics[idx].Partitions, sp)
			}
			continue
		}

		for j := range rt.Partitions {
			rp := &rt.Partitions[j]

			// Validate ack batches for this partition.
			errCode := int16(0)
			prevEnd := int64(-1)
			maxType := int8(3)
			if supportsRenew {
				maxType = 4
			}
			for _, batch := range rp.AcknowledgementBatches {
				if ec := validateOneAckBatch(batch.FirstOffset, batch.LastOffset, batch.AcknowledgeTypes, &prevEnd, maxType, req.IsRenewAck); ec != 0 {
					errCode = ec
					break
				}
			}
			if errCode != 0 {
				idx := getOrAddShareAckTopic(resp, topicIdx, rt.TopicID)
				sp := kmsg.NewShareAcknowledgeResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = errCode
				resp.Topics[idx].Partitions = append(resp.Topics[idx].Partitions, sp)
				continue
			}

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
			var ackErr int16
			for _, batch := range rp.AcknowledgementBatches {
				if ec := shp.processAcks(memberID, batch.FirstOffset, batch.LastOffset, batch.AcknowledgeTypes); ec != 0 {
					ackErr = ec
					break
				}
			}
			shp.advanceSPSO()

			idx := getOrAddShareAckTopic(resp, topicIdx, rt.TopicID)
			sp := kmsg.NewShareAcknowledgeResponseTopicPartition()
			sp.Partition = rp.Partition
			sp.ErrorCode = ackErr
			resp.Topics[idx].Partitions = append(resp.Topics[idx].Partitions, sp)

			// Wake any pending ShareFetch watchers on this partition.
			fireShareWatchers(pd)
		}
	}
	sg.mu.Unlock()

	// Advance session epoch (for non-close requests), same as ShareFetch.
	if req.ShareSessionEpoch != -1 {
		session := c.shareSessions[sessionKey]
		if session != nil {
			if session.epoch == math.MaxInt32 {
				session.epoch = 1
			} else {
				session.epoch++
			}
		}
	}

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
