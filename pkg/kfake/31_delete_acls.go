package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// DeleteACLs: v0-3
//
// Behavior:
// * Deletes ACLs matching filters
// * Returns deleted ACLs
// * Requires ALTER on CLUSTER
// * SECURITY_DISABLED without EnableACLs, as a broker with no authorizer
//
// Version notes:
// * v1: PatternType field added
// * v2: Flexible versions

func init() { regKey(31, 0, 3) }

func (c *Cluster) handleDeleteACLs(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.DeleteACLsRequest)
	resp := req.ResponseKind().(*kmsg.DeleteACLsResponse)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	// Every filter sees the ACLs from before the request, so an ACL that
	// two filters match is listed under both. We delete after the loop.
	var deleted []acl
	for _, rf := range req.Filters {
		result := kmsg.DeleteACLsResponseResult{}
		var name string
		if rf.ResourceName != nil {
			name = *rf.ResourceName
		}
		fe := c.denyCluster(creq, kmsg.ACLOperationAlter, faultKey{resource: name})
		if fe != nil {
			result.ErrorCode = fe.Code
			result.ErrorMessage = kmsg.StringPtr(fe.Message)
			if creq.skipsWork(fe) { // a timed-out deletion still deletes the ACLs
				resp.Results = append(resp.Results, result)
				continue
			}
		}
		if !c.cfg.enableACLs {
			if fe == nil {
				result.ErrorCode = kerr.SecurityDisabled.Code
				result.ErrorMessage = kmsg.StringPtr("No Authorizer is configured.")
			}
			resp.Results = append(resp.Results, result)
			continue
		}

		filter := aclFilter{
			resourceType: rf.ResourceType,
			pattern:      rf.ResourcePatternType,
			operation:    rf.Operation,
			permission:   rf.PermissionType,
			resourceName: rf.ResourceName,
			principal:    rf.Principal,
			host:         rf.Host,
		}

		if !filter.validate() {
			if fe == nil { // a timed-out deletion keeps its answer
				result.ErrorCode = kerr.InvalidRequest.Code
				result.ErrorMessage = kmsg.StringPtr("DeleteAclsRequest contains UNKNOWN elements")
			}
			resp.Results = append(resp.Results, result)
			continue
		}

		matched := c.acls.describe(filter)
		deleted = append(deleted, matched...)
		if fe != nil { // a timed-out answer lists nothing
			matched = nil
		}
		for _, a := range matched {
			result.MatchingACLs = append(result.MatchingACLs, kmsg.DeleteACLsResponseResultMatchingACL{
				ResourceType:        a.resourceType,
				ResourceName:        a.resourceName,
				ResourcePatternType: a.pattern,
				Principal:           a.principal,
				Host:                a.host,
				Operation:           a.operation,
				PermissionType:      a.permission,
			})
		}
		resp.Results = append(resp.Results, result)
	}
	c.acls.remove(deleted)

	c.persistACLsState()
	return resp, nil
}
