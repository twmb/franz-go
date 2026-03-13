package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ShareAcknowledge: v0-2 (KIP-932, KIP-1222)
//
// Behavior:
// * Acknowledges records previously acquired via ShareFetch
// * Shares the same session as ShareFetch (epoch incremented on success)
// * On session close (epoch -1), remaining acquired records are released
// * Ack types: 0=Gap, 1=Accept, 2=Release, 3=Reject, 4=Renew (v2+)
//
// Version notes:
// * v0: Initial share acknowledge (KIP-932)
// * v2: IsRenewAck and type 4 (Renew) for lock renewal (KIP-1222)

func init() { regKey(79, 0, 2) }

func (c *Cluster) handleShareAcknowledge(creq *clientReq) (kmsg.Response, error) {
	var (
		req  = creq.kreq.(*kmsg.ShareAcknowledgeRequest)
		resp = req.ResponseKind().(*kmsg.ShareAcknowledgeResponse)
	)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	resp.AcquisitionLockTimeoutMillis = c.shareRecordLockDurationMs()

	var groupID, memberID string
	if req.GroupID != nil {
		groupID = *req.GroupID
	}
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

	// Session epoch validation. ShareAcknowledge shares the same session
	// as ShareFetch: the client tracks a single epoch that increments on
	// each successful ShareFetch or ShareAcknowledge.
	sgs := &c.shareGroups
	sessionKey := shareSessionKey{
		group:    groupID,
		memberID: memberID,
		broker:   creq.cc.b.node,
	}

	closingSession := req.ShareSessionEpoch == -1
	if closingSession {
		if sgs.sessions[sessionKey] == nil {
			resp.ErrorCode = kerr.ShareSessionNotFound.Code
			return resp, nil
		}
		defer delete(sgs.sessions, sessionKey)
	} else if req.ShareSessionEpoch == 0 {
		// Epoch 0 opens a new session via ShareFetch, not
		// ShareAcknowledge.
		resp.ErrorCode = kerr.InvalidShareSessionEpoch.Code
		return resp, nil
	} else {
		session := sgs.sessions[sessionKey]
		if session == nil {
			resp.ErrorCode = kerr.ShareSessionNotFound.Code
			return resp, nil
		}
		if req.ShareSessionEpoch != session.epoch {
			resp.ErrorCode = kerr.InvalidShareSessionEpoch.Code
			return resp, nil
		}
	}

	// Response-building closure (matching produce handler style).
	topicIdx := make(map[uuid]int)
	donep := func(tid uuid, p int32, ec int16) {
		i, ok := topicIdx[tid]
		if !ok {
			i = len(resp.Topics)
			topicIdx[tid] = i
			t := kmsg.NewShareAcknowledgeResponseTopic()
			t.TopicID = tid
			resp.Topics = append(resp.Topics, t)
		}
		sp := kmsg.NewShareAcknowledgeResponseTopicPartition()
		sp.Partition = p
		sp.ErrorCode = ec
		resp.Topics[i].Partitions = append(resp.Topics[i].Partitions, sp)
	}

	maxAckType := int8(3)
	if req.Version >= 2 && req.IsRenewAck {
		maxAckType = 4
	}

	ackTs := ackTopicsFromAcknowledge(req.Topics)
	maxDelivery := c.shareMaxDeliveryAttempts()

	sg.mu.Lock()
	toFire, _ := c.processShareAcks(creq, sg, memberID, ackTs, maxAckType, donep)

	// On session close, release remaining acquired records (matching
	// Kafka's releaseSession behavior).
	var released bool
	if closingSession {
		released = sg.releaseRecordsForMemberLocked(memberID, maxDelivery)
	}
	sg.mu.Unlock()

	// Fire watchers outside the lock for ack-released records.
	for _, pd := range toFire {
		fireShareWatchers(pd)
	}
	if released {
		sg.fireAllShareWatchers()
	}

	// Advance session epoch (for non-close requests).
	if !closingSession {
		if session := sgs.sessions[sessionKey]; session != nil {
			session.epoch = max(1, session.epoch+1)
		}
	}

	return resp, nil
}
