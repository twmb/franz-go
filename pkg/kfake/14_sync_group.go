package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// SyncGroup: v0-5
//
// Version notes:
// * v1: ThrottleMillis
// * v3: InstanceID for static membership (KIP-345)
// * v4: Flexible versions
// * v5: ProtocolType and Protocol in request/response

func init() { regKey(14, 0, 5) }

func (c *Cluster) handleSyncGroup(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.SyncGroupRequest)
	resp := req.ResponseKind().(*kmsg.SyncGroupResponse)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	// From v5, Kafka requires both, and checks them before the ACL.
	if req.Version >= 5 && (req.ProtocolType == nil || req.Protocol == nil) {
		resp.ErrorCode = kerr.InconsistentGroupProtocol.Code
		return resp, nil
	}

	g, err := c.groups.memberReq(creq, req.Group, kmsg.ACLOperationRead)
	switch {
	case err != nil:
		resp.ErrorCode = err.Code
	case g == nil:
		resp.ErrorCode = kerr.UnknownMemberID.Code
	default:
		return g.handleSync(creq), nil
	}
	return resp, nil
}
