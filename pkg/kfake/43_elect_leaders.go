package kfake

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ElectLeaders: v0-2
//
// Behavior:
// * PREFERRED rotates partition leader to the next broker, bumps epoch
// * Single-broker clusters keep the leader, but still bump the epoch
// * UNCLEAN answers ELECTION_NOT_NEEDED: every partition has a leader
// * Null topics triggers election for all partitions
// * ACL: Cluster ALTER required
//
// Version notes:
// * v0: Initial version
// * v1: ElectionType field (KIP-460), top-level ErrorCode
// * v2: Flexible versions

func init() { regKey(43, 0, 2) }

func (c *Cluster) handleElectLeaders(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.ElectLeadersRequest)
	resp := req.ResponseKind().(*kmsg.ElectLeadersResponse)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	// A request-level error goes at the top level and on every named
	// partition, as Kafka does; asking for all partitions answers none.
	errResp := func(code int16, msg *string) (kmsg.Response, error) {
		resp.ErrorCode = code
		for _, rt := range req.Topics {
			st := kmsg.NewElectLeadersResponseTopic()
			st.Topic = rt.Topic
			for _, p := range rt.Partitions {
				sp := kmsg.NewElectLeadersResponseTopicPartition()
				sp.Partition = p
				sp.ErrorCode, sp.ErrorMessage = code, msg
				st.Partitions = append(st.Partitions, sp)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil
	}

	if e := c.denyCluster(creq, kmsg.ACLOperationAlter, faultKey{}); e != nil {
		if creq.skipsWork(e) {
			return errResp(e.Code, nil)
		}
		// A timed-out election still elects, and then answers the
		// request-level error.
		defer func() {
			resp.Topics = nil
			errResp(e.Code, nil)
		}()
	}
	switch {
	case req.Topics != nil && len(req.Topics) == 0:
		return resp, nil
	case req.TimeoutMillis <= 0: // the deadline passed before the controller ran it
		return errResp(kerr.RequestTimedOut.Code, nil)
	case req.ElectionType != 0 && req.ElectionType != 1:
		return errResp(kerr.InvalidRequest.Code, kmsg.StringPtr(fmt.Sprintf("Unknown election type %d", req.ElectionType)))
	}
	unclean := req.ElectionType == 1
	defer c.persistTopicsState() // the leaders and epochs we elect

	// elect answers one partition. A fault answers first; the work's own
	// answer does not replace it.
	elect := func(st *kmsg.ElectLeadersResponseTopic, t string, ps map[int32]*partData, p int32, all bool) {
		sp := kmsg.NewElectLeadersResponseTopicPartition()
		sp.Partition = p
		e := creq.faults.check(faultKey{topic: t}.part(p))
		pd, ok := ps[p]
		switch {
		case e != nil && creq.skipsWork(e):
		case !ok:
			// Kafka names what it does not have.
			msg := "No such topic as " + t
			if _, exists := c.data.tps.gett(t); exists {
				msg = fmt.Sprintf("No such partition as %s-%d", t, p)
			}
			sp.ErrorCode, sp.ErrorMessage = kerr.UnknownTopicOrPartition.Code, &msg
		case unclean:
			// Electing all partitions leaves out those that
			// need no election.
			if all && e == nil {
				return
			}
			sp.ErrorCode, sp.ErrorMessage = kerr.ElectionNotNeeded.Code, kmsg.StringPtr("Leader election not needed for topic partition.")
		default:
			// A deliberate test hook that differs from Kafka, which
			// answers ELECTION_NOT_NEEDED when the preferred replica
			// already leads.
			c.setLeader(t, p, pd, c.bs[(pd.leader.bsIdx+1)%len(c.bs)])
		}
		if e != nil {
			sp.ErrorCode, sp.ErrorMessage = e.Code, nil
		}
		st.Partitions = append(st.Partitions, sp)
	}

	if req.Topics == nil {
		for t, ps := range c.data.tps {
			st := kmsg.NewElectLeadersResponseTopic()
			st.Topic = t
			for p := range ps {
				elect(&st, t, ps, p, true)
			}
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil
	}
	for _, rt := range req.Topics {
		st := kmsg.NewElectLeadersResponseTopic()
		st.Topic = rt.Topic
		ps, _ := c.data.tps.gett(rt.Topic)
		for _, p := range rt.Partitions {
			elect(&st, rt.Topic, ps, p, false)
		}
		resp.Topics = append(resp.Topics, st)
	}
	return resp, nil
}
