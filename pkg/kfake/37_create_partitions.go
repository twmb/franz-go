package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// CreatePartitions: v0-3
//
// Behavior:
// * Must be sent to the controller
// * Only supports increasing partition count; an equal or lower count fails
// * A duplicated name answers INVALID_REQUEST; the other topics proceed
// * More than 10000 new partitions in one request fails it with POLICY_VIOLATION
// * Supports explicit replica assignment (first replica is leader)
// * ValidateOnly: validates request without creating partitions
//
// Version notes:
// * v1: ThrottleMillis
// * v2: Flexible versions
// * v3: No changes

func init() { regKey(37, 0, 3) }

func (c *Cluster) handleCreatePartitions(creq *clientReq) (kmsg.Response, error) {
	var (
		b    = creq.cc.b
		req  = creq.kreq.(*kmsg.CreatePartitionsRequest)
		resp = req.ResponseKind().(*kmsg.CreatePartitionsResponse)
	)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	// A fault can answer a topic before the work runs. The work's own
	// answer for that topic must not add an entry or replace the code.
	answered := make(map[string]int)
	donet := func(t string, errCode int16) *kmsg.CreatePartitionsResponseTopic {
		if i, ok := answered[t]; ok {
			return &resp.Topics[i]
		}
		answered[t] = len(resp.Topics)
		st := kmsg.NewCreatePartitionsResponseTopic()
		st.Topic = t
		st.ErrorCode = errCode
		resp.Topics = append(resp.Topics, st)
		return &resp.Topics[len(resp.Topics)-1]
	}
	// failAll answers every topic the request names, a duplicate
	// included, replacing the answers so far.
	failAll := func(errCode int16) (kmsg.Response, error) {
		resp.Topics = resp.Topics[:0]
		for _, rt := range req.Topics {
			st := kmsg.NewCreatePartitionsResponseTopic()
			st.Topic = rt.Topic
			st.ErrorCode = errCode
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil
	}

	misrouted := b != c.controller

	// Kafka answers a duplicated name with INVALID_REQUEST and runs the
	// rest of the request.
	counts := make(map[string]int)
	for _, rt := range req.Topics {
		counts[rt.Topic]++
	}

	var (
		alters   []*kmsg.CreatePartitionsRequestTopic
		newParts int64
	)
	for i := range req.Topics {
		rt := &req.Topics[i]
		if counts[rt.Topic] > 1 {
			donet(rt.Topic, kerr.InvalidRequest.Code)
			continue
		}
		if e := c.deny(creq, rt.Topic, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationAlter, faultKey{topic: rt.Topic, misrouted: misrouted}); e != nil {
			donet(rt.Topic, e.Code)
			if creq.skipsWork(e) { // a timed-out create still adds the partitions
				continue
			}
		}
		if misrouted {
			donet(rt.Topic, kerr.NotController.Code)
			continue
		}
		alters = append(alters, rt)
		if t, ok := c.data.tps.gett(rt.Topic); ok && int(rt.Count) > len(t) {
			newParts += int64(int(rt.Count) - len(t))
		}
	}

	if misrouted {
		return resp, nil
	}

	// Kafka fails the whole request if its deadline passed before the
	// controller ran it, or if it would add too many partitions. The
	// controller runs only if a topic is left.
	if len(alters) > 0 {
		switch {
		case req.TimeoutMillis <= 0:
			return failAll(kerr.RequestTimedOut.Code)
		case newParts > int64(c.maxPartitionsPerRequest()):
			return failAll(kerr.PolicyViolation.Code)
		}
	}

	for _, rt := range alters {
		t, ok := c.data.tps.gett(rt.Topic)
		if !ok {
			donet(rt.Topic, kerr.UnknownTopicOrPartition.Code)
			continue
		}
		if rt.Count <= int32(len(t)) { // Kafka: an equal count is no increase
			donet(rt.Topic, kerr.InvalidPartitions.Code)
			continue
		}

		existingPartitions := len(t)
		numNewPartitions := int(rt.Count) - existingPartitions

		// A non-nil assignment, even an empty one, must give every
		// new partition a valid replica list of the topic's length.
		if rt.Assignment != nil {
			rf := min(c.data.treplicas[rt.Topic], len(c.bs))
			valid := len(rt.Assignment) == numNewPartitions
			for _, a := range rt.Assignment {
				valid = valid && c.validReplicas(a.Replicas) && len(a.Replicas) == rf
			}
			if !valid {
				donet(rt.Topic, kerr.InvalidReplicaAssignment.Code)
				continue
			}
		}

		if !req.ValidateOnly {
			for i := range numNewPartitions {
				partNum := int32(existingPartitions) + int32(i)
				pd := c.data.tps.mkp(rt.Topic, partNum, c.newPartData(partNum))
				pd.t = rt.Topic
				// An explicit assignment's first replica is the leader.
				if len(rt.Assignment) > 0 {
					pd.leader = c.nodeBroker(rt.Assignment[i].Replicas[0])
				}
			}
		}
		donet(rt.Topic, 0)
	}

	if !req.ValidateOnly {
		c.notifyTopicChange()
		c.persistTopicsState()
	}

	return resp, nil
}
