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

	if req.GroupID == nil {
		resp.ErrorCode = kerr.InvalidRequest.Code
		return resp, nil
	}
	groupID := *req.GroupID
	var memberID string
	if req.MemberID != nil {
		memberID = *req.MemberID
	}
	resp.AcquisitionLockTimeoutMillis = c.shareRecordLockDurationMs(groupID)

	// ACL: require GROUP READ.
	if e := c.deny(creq, groupID, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationRead, faultKey{group: groupID}); e != nil {
		resp.ErrorCode = e.Code
		return resp, nil
	}

	// Validate memberID format (non-empty, <=36 chars).
	if memberID == "" || len(memberID) > 36 {
		resp.ErrorCode = kerr.InvalidRequest.Code
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

	maxAckType := shareAckReject
	if req.Version >= 2 && req.IsRenewAck {
		maxAckType = shareAckRenew
	}

	// Response-building closure (matching produce handler style).
	topicIdx := make(map[uuid]int)
	onPartition := func(tid uuid, p int32, ec int16) {
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
		// Kafka fills CurrentLeader only for leader errors, with -1/-1
		// when it does not know the leader. Every other partition gets
		// 0/0.
		if ec == kerr.NotLeaderForPartition.Code || ec == kerr.FencedLeaderEpoch.Code {
			sp.CurrentLeader.LeaderID = -1
			sp.CurrentLeader.LeaderEpoch = -1
		}
		resp.Topics[i].Partitions = append(resp.Topics[i].Partitions, sp)
	}

	// Session close: process acks, release remaining acquired records,
	// delete session. A member may have multiple sessions (one per
	// broker), so we only release records for this session's partitions.
	if req.ShareSessionEpoch == -1 {
		session := sgs.sessions[sessionKey]
		if session == nil {
			resp.ErrorCode = kerr.ShareSessionNotFound.Code
			return resp, nil
		}
		sg := sgs.get(groupID) // non-nil: DeleteGroups drops a group's sessions with it
		ackTs := ackTopicsFromAcknowledge(req.Topics)
		toFire := sg.processShareAcks(creq, memberID, ackTs, maxAckType, onPartition)
		released := sg.releaseRecordsForSession(memberID, session)
		fireAll(toFire)
		if released {
			sg.fireAllShareWatchers()
		}
		delete(sgs.sessions, sessionKey)
		return resp, nil
	}

	// Epoch 0 opens a new session via ShareFetch, not ShareAcknowledge.
	if req.ShareSessionEpoch == 0 {
		resp.ErrorCode = kerr.InvalidShareSessionEpoch.Code
		return resp, nil
	}

	session := sgs.sessions[sessionKey]
	if session == nil {
		resp.ErrorCode = kerr.ShareSessionNotFound.Code
		return resp, nil
	}
	if req.ShareSessionEpoch != session.epoch {
		resp.ErrorCode = kerr.InvalidShareSessionEpoch.Code
		return resp, nil
	}

	// Like Kafka, we check only the session. Its group exists: only
	// DeleteGroups drops a share group, and it drops the group's sessions
	// with it.
	sg := sgs.get(groupID)
	ackTs := ackTopicsFromAcknowledge(req.Topics)
	toFire := sg.processShareAcks(creq, memberID, ackTs, maxAckType, onPartition)
	fireAll(toFire)

	session.bumpEpoch()
	return resp, nil
}
