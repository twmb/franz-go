package kfake

import (
	"strconv"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// DescribeClientQuotas: v0-1
//
// KIP-546: Client quota configuration APIs.
//
// Behavior:
// * ACL: DESCRIBE_CONFIGS on CLUSTER
// * A filter Kafka refuses answers its error: an empty, repeated, or unknown
//   entity type, a match string where the match type takes none or none
//   where it takes one, an unknown match type, or ip with user or client-id
// * Returns stored quota configurations matching the filter
// * Quotas are stored but not enforced by kfake
//
// Version notes:
// * v0: Initial version
// * v1: Flexible versions

func init() { regKey(48, 0, 1) }

func (c *Cluster) handleDescribeClientQuotas(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.DescribeClientQuotasRequest)
	resp := req.ResponseKind().(*kmsg.DescribeClientQuotasResponse)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	if e := c.denyCluster(creq, kmsg.ACLOperationDescribeConfigs, faultKey{}); e != nil {
		resp.ErrorCode = e.Code
		return resp, nil
	}
	if e, msg := validQuotaFilter(req.Components); e != nil {
		resp.ErrorCode = e.Code
		resp.ErrorMessage = &msg
		return resp, nil
	}
	// Only an error answers null entries: the Java client fails on a
	// null list in a successful response.
	resp.Entries = []kmsg.DescribeClientQuotasResponseEntry{}

	for _, quota := range c.quotas {
		if matchesQuotaFilter(quota, req.Components, req.Strict) {
			for _, e := range quota.entity {
				if e.name != nil {
					if fe := creq.faults.check(faultKey{resource: *e.name}); fe != nil {
						resp.ErrorCode, resp.Entries = fe.Code, nil
						return resp, nil
					}
				}
			}
			entry := kmsg.NewDescribeClientQuotasResponseEntry()
			for _, e := range quota.entity {
				ee := kmsg.NewDescribeClientQuotasResponseEntryEntity()
				ee.Type = e.entityType
				ee.Name = e.name
				entry.Entity = append(entry.Entity, ee)
			}
			for k, v := range quota.values {
				ev := kmsg.NewDescribeClientQuotasResponseEntryValue()
				ev.Key = k
				ev.Value = v
				entry.Values = append(entry.Values, ev)
			}
			resp.Entries = append(resp.Entries, entry)
		}
	}

	return resp, nil
}

// validQuotaFilter returns the error and message Kafka answers a filter it
// refuses, or nil.
func validQuotaFilter(components []kmsg.DescribeClientQuotasRequestComponent) (*kerr.Error, string) {
	seen := make(map[string]bool)
	for _, comp := range components {
		switch {
		case comp.EntityType == "":
			return kerr.InvalidRequest, "Invalid empty entity type."
		case seen[comp.EntityType]:
			return kerr.InvalidRequest, "Entity type " + comp.EntityType + " cannot appear more than once in the filter."
		case comp.EntityType != "ip" && comp.EntityType != "user" && comp.EntityType != "client-id":
			return kerr.UnsupportedVersion, "Unsupported entity type " + comp.EntityType
		case comp.MatchType == 0 && comp.Match == nil:
			return kerr.InvalidRequest, "Request specified MATCH_TYPE_EXACT, but set match string to null."
		case comp.MatchType == 1 && comp.Match != nil:
			return kerr.InvalidRequest, "Request specified MATCH_TYPE_DEFAULT, but also specified a match string."
		case comp.MatchType == 2 && comp.Match != nil:
			return kerr.InvalidRequest, "Request specified MATCH_TYPE_SPECIFIED, but also specified a match string."
		case comp.MatchType < 0 || comp.MatchType > 2:
			return kerr.InvalidRequest, "Unknown match type " + strconv.Itoa(int(comp.MatchType))
		}
		seen[comp.EntityType] = true
	}
	if seen["ip"] && (seen["user"] || seen["client-id"]) {
		return kerr.InvalidRequest, "Invalid entity filter component combination. IP filter component should not be used with user or clientId filter component."
	}
	return nil, ""
}

func matchesQuotaFilter(quota quotaEntry, components []kmsg.DescribeClientQuotasRequestComponent, strict bool) bool {
	if len(components) == 0 {
		return !strict
	}

	// Build a map of entity types in this quota
	entityMap := make(map[string]*string)
	for _, e := range quota.entity {
		entityMap[e.entityType] = e.name
	}

	// Check each filter component
	for _, comp := range components {
		name, exists := entityMap[comp.EntityType]

		switch comp.MatchType {
		case 0: // Exact match on name
			if !exists || name == nil || *name != *comp.Match {
				return false
			}
		case 1: // Default match (name is null)
			if !exists || name != nil {
				return false
			}
		case 2: // Any entity of the type, a default (null name) included
			if !exists {
				return false
			}
		default:
			return false
		}
	}

	// In strict mode, quota must not have entity types not in the filter
	if strict {
		filterTypes := make(map[string]struct{})
		for _, comp := range components {
			filterTypes[comp.EntityType] = struct{}{}
		}
		for _, e := range quota.entity {
			if _, ok := filterTypes[e.entityType]; !ok {
				return false
			}
		}
	}

	return true
}
