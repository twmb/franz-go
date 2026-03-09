package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ShareGroupDescribe: v0-1 (KIP-932)

func init() { regKey(77, 0, 1) }

func (c *Cluster) handleShareGroupDescribe(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.ShareGroupDescribeRequest)
	resp := req.ResponseKind().(*kmsg.ShareGroupDescribeResponse)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	for _, groupID := range req.GroupIDs {
		rg := kmsg.NewShareGroupDescribeResponseGroup()
		rg.GroupID = groupID

		sg := c.shareGroups.gs[groupID]
		if sg == nil {
			rg.ErrorCode = kerr.GroupIDNotFound.Code
			resp.Groups = append(resp.Groups, rg)
			continue
		}

		// Snapshot id2t before entering manage() via waitControl.
		// c.data is only safe to read in run(), and waitControl's
		// adminCh drain could mutate c.data concurrently with
		// the manage() closure.
		id2t := make(map[uuid]string, len(c.data.id2t))
		for k, v := range c.data.id2t {
			id2t[k] = v
		}

		if !sg.waitControl(func() {
			if len(sg.members) == 0 {
				rg.GroupState = "Empty"
			} else {
				rg.GroupState = "Stable"
			}
			rg.GroupEpoch = sg.groupEpoch
			rg.AssignmentEpoch = sg.groupEpoch

			for _, m := range sg.members {
				sm := kmsg.NewShareGroupDescribeResponseGroupMember()
				sm.MemberID = m.memberID
				sm.RackID = m.rackID
				sm.MemberEpoch = m.memberEpoch
				sm.ClientID = m.clientID
				sm.ClientHost = m.clientHost
				sm.SubscribedTopicNames = m.subscribedTopics

				a := kmsg.NewShareGroupDescribeResponseGroupMemberAssignment()
				for tid, parts := range m.assignment {
					tp := kmsg.NewShareGroupDescribeResponseGroupMemberAssignmentTopicPartition()
					tp.TopicID = tid
					tp.Topic = id2t[tid]
					tp.Partitions = parts
					a.TopicPartitions = append(a.TopicPartitions, tp)
				}
				sm.Assignment = a
				rg.Members = append(rg.Members, sm)
			}
		}) {
			rg.ErrorCode = kerr.GroupIDNotFound.Code
		}

		resp.Groups = append(resp.Groups, rg)
	}

	return resp, nil
}
