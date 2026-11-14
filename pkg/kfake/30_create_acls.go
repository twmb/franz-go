package kfake

import (
	"net/netip"
	"strings"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// CreateACLs: v0-3
//
// Behavior:
// * Creates ACLs
// * Requires ALTER on CLUSTER
// * SECURITY_DISABLED without EnableACLs, as a broker with no authorizer
//
// Version notes:
// * v1: PatternType field added
// * v2: Flexible versions

func init() { regKey(30, 0, 3) }

func (c *Cluster) handleCreateACLs(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.CreateACLsRequest)
	resp := req.ResponseKind().(*kmsg.CreateACLsResponse)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	// Kafka authorizes the whole request before it creates anything, so
	// a creation that denies the requester ALTER does not fail the
	// creations after it: we add every ACL after the loop.
	var created []acl
	for _, cr := range req.Creations {
		result := kmsg.CreateACLsResponseResult{}
		fe := c.denyCluster(creq, kmsg.ACLOperationAlter, faultKey{resource: cr.ResourceName})
		if fe != nil {
			result.ErrorCode = fe.Code
			result.ErrorMessage = kmsg.StringPtr(fe.Message)
			if creq.skipsWork(fe) { // a timed-out creation still creates the ACL
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

		if err := validateACLCreation(cr); err != "" {
			if fe == nil { // a timed-out creation keeps its answer
				result.ErrorCode = kerr.InvalidRequest.Code
				result.ErrorMessage = kmsg.StringPtr(err)
			}
			resp.Results = append(resp.Results, result)
			continue
		}

		created = append(created, acl{
			principal:    cr.Principal,
			host:         cr.Host,
			resourceType: cr.ResourceType,
			resourceName: cr.ResourceName,
			pattern:      cr.ResourcePatternType,
			operation:    cr.Operation,
			permission:   cr.PermissionType,
		})
		resp.Results = append(resp.Results, result)
	}
	for _, a := range created {
		c.acls.add(a)
	}

	c.persistACLsState()
	return resp, nil
}

// aclOperationTwoPhaseCommit is TWO_PHASE_COMMIT (KIP-939), the highest
// operation Kafka accepts, which kmsg does not name yet.
const aclOperationTwoPhaseCommit kmsg.ACLOperation = 15

// validateACLCreation returns the INVALID_REQUEST message for an ACL we do
// not store, or "" if the ACL is valid. The messages are Kafka's, except the
// first four, which are ours, and the CIDR one, which leaves out Java's
// parse error.
func validateACLCreation(cr kmsg.CreateACLsRequestCreation) string {
	switch {
	case cr.ResourceType <= kmsg.ACLResourceTypeAny || cr.ResourceType > kmsg.ACLResourceTypeUser:
		return "invalid resource type"
	case cr.ResourcePatternType != kmsg.ACLResourcePatternTypeLiteral && cr.ResourcePatternType != kmsg.ACLResourcePatternTypePrefixed:
		return "invalid pattern type"
	case cr.Operation <= kmsg.ACLOperationAny || cr.Operation > aclOperationTwoPhaseCommit:
		return "invalid operation"
	case cr.PermissionType != kmsg.ACLPermissionTypeDeny && cr.PermissionType != kmsg.ACLPermissionTypeAllow:
		return "invalid permission type"
	case cr.ResourceType == kmsg.ACLResourceTypeCluster && cr.ResourceName != aclClusterName:
		return "The only valid name for the CLUSTER resource is " + aclClusterName
	case cr.ResourceName == "":
		return "Invalid empty resource name"
	case !strings.Contains(cr.Principal, ":"):
		return "Could not parse principal from `" + cr.Principal + "` (no colon is present separating the principal type from the principal name)"
	case cr.Host == "":
		return "Host pattern cannot be null or empty"
	}
	if strings.Contains(cr.Host, "/") {
		if _, err := netip.ParsePrefix(cr.Host); err != nil {
			return "Invalid CIDR notation '" + cr.Host + "'"
		}
	}
	return ""
}
