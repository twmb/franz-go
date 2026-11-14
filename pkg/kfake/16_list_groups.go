package kfake

import (
	"slices"
	"strings"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// ListGroups: v0-5
//
// Version notes:
// * v1: ThrottleMillis
// * v3: Flexible versions
// * v4: StatesFilter (KIP-518)
// * v5: TypesFilter for new consumer protocol (KIP-848)

func init() { regKey(16, 0, 5) }

func (c *Cluster) handleListGroups(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.ListGroupsRequest)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	resp := c.groups.handleList(creq)

	// Include share groups, which are tracked separately from the
	// 'groups' struct.
	clusterDescribe := c.allowedClusterACL(creq, kmsg.ACLOperationDescribe)
	for _, sg := range c.shareGroups.gs {
		if c.coordinator(sg.name).node != creq.cc.b.node {
			continue
		}
		state := "Stable"
		if len(sg.members) == 0 {
			state = "Empty"
		}
		if !listFilterMatch(req, state, "share") {
			continue
		}
		if c.listDenied(creq, clusterDescribe, sg.name) {
			continue
		}
		lg := kmsg.NewListGroupsResponseGroup()
		lg.Group = sg.name
		lg.ProtocolType = "share"
		lg.GroupState = state
		lg.GroupType = "share"
		resp.Groups = append(resp.Groups, lg)
	}

	return resp, nil
}

// listFilterMatch reports whether a group passes the request's filters.
// Kafka compares states ignoring case and surrounding space, and types
// ignoring case.
func listFilterMatch(req *kmsg.ListGroupsRequest, state, typ string) bool {
	if len(req.StatesFilter) > 0 && !slices.ContainsFunc(req.StatesFilter, func(s string) bool { return strings.EqualFold(strings.TrimSpace(s), state) }) {
		return false
	}
	return len(req.TypesFilter) == 0 || slices.ContainsFunc(req.TypesFilter, func(t string) bool { return strings.EqualFold(t, typ) })
}

// listDenied reports whether ListGroups leaves out group. Kafka lists
// every group to a caller that may describe the cluster, and otherwise
// only the groups the caller may describe.
func (c *Cluster) listDenied(creq *clientReq, clusterDescribe bool, group string) bool {
	k := faultKey{group: group}
	if clusterDescribe {
		return creq.faults.check(k) != nil
	}
	return c.deny(creq, group, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationDescribe, k) != nil
}
