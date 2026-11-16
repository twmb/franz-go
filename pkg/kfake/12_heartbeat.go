package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Heartbeat: v0-4
//
// Version notes:
// * v1: ThrottleMillis
// * v3: InstanceID for static membership (KIP-345)
// * v4: Flexible versions

func init() { regKey(12, 0, 4) }

func (c *Cluster) handleHeartbeat(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.HeartbeatRequest)
	resp := req.ResponseKind().(*kmsg.HeartbeatResponse)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	g, err := c.groups.memberReq(creq, req.Group, kmsg.ACLOperationRead)
	switch {
	case err != nil:
		resp.ErrorCode = err.Code
	case g == nil:
		resp.ErrorCode = kerr.UnknownMemberID.Code
	default:
		return g.handleHeartbeat(creq), nil
	}
	return resp, nil
}
