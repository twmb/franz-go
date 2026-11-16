package kfake

import (
	"github.com/twmb/franz-go/pkg/kmsg"
)

// LeaveGroup: v0-5
//
// Version notes:
// * v1: ThrottleMillis
// * v3: Batch members leave with InstanceID (KIP-345)
// * v4: Flexible versions
// * v5: Reason field (KIP-800)

func init() { regKey(13, 0, 5) }

func (c *Cluster) handleLeaveGroup(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.LeaveGroupRequest)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	g, err := c.groups.memberReq(creq, req.Group, kmsg.ACLOperationRead)
	if err != nil {
		resp := req.ResponseKind().(*kmsg.LeaveGroupResponse)
		resp.ErrorCode = err.Code
		return resp, nil
	}
	return g.handleLeave(creq), nil
}
