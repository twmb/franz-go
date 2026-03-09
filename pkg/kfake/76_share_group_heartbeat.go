package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ShareGroupHeartbeat: v0-1 (KIP-932)

func init() { regKey(76, 0, 1) }

func (c *Cluster) handleShareGroupHeartbeat(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.ShareGroupHeartbeatRequest)
	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	if kerr := c.validateGroup(creq, req.GroupID); kerr != nil {
		resp := req.ResponseKind().(*kmsg.ShareGroupHeartbeatResponse)
		resp.ErrorCode = kerr.Code
		return resp, nil
	}

	// ACL: require GROUP READ on the share group.
	if !c.allowedACL(creq, req.GroupID, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationRead) {
		resp := req.ResponseKind().(*kmsg.ShareGroupHeartbeatResponse)
		resp.ErrorCode = kerr.GroupAuthorizationFailed.Code
		return resp, nil
	}

	// Validate memberID format: non-empty, <=36 chars.
	if req.MemberID == "" || len(req.MemberID) > 36 {
		resp := req.ResponseKind().(*kmsg.ShareGroupHeartbeatResponse)
		resp.ErrorCode = kerr.InvalidRequest.Code
		return resp, nil
	}

	// Validate memberEpoch range: must be >= -1.
	if req.MemberEpoch < -1 {
		resp := req.ResponseKind().(*kmsg.ShareGroupHeartbeatResponse)
		resp.ErrorCode = kerr.InvalidRequest.Code
		return resp, nil
	}

	// Epoch 0 (join) requires subscribedTopicNames.
	if req.MemberEpoch == 0 && len(req.SubscribedTopicNames) == 0 {
		resp := req.ResponseKind().(*kmsg.ShareGroupHeartbeatResponse)
		resp.ErrorCode = kerr.InvalidRequest.Code
		return resp, nil
	}

	// Hijack to the share group's manage goroutine.
	c.shareGroups.handleHeartbeat(creq)
	return nil, nil
}
