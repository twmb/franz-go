package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// CreatePartitions: v0-3
//
// Behavior:
// * Must be sent to the controller
// * Only supports increasing partition count, not decreasing
// * Assignment not supported (returns InvalidReplicaAssignment)
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

	donet := func(t string, errCode int16) *kmsg.CreatePartitionsResponseTopic {
		st := kmsg.NewCreatePartitionsResponseTopic()
		st.Topic = t
		st.ErrorCode = errCode
		resp.Topics = append(resp.Topics, st)
		return &resp.Topics[len(resp.Topics)-1]
	}
	donets := func(errCode int16) {
		for _, rt := range req.Topics {
			donet(rt.Topic, errCode)
		}
	}

	if b != c.controller {
		donets(kerr.NotController.Code)
		return resp, nil
	}

	uniq := make(map[string]struct{})
	for _, rt := range req.Topics {
		if _, ok := uniq[rt.Topic]; ok {
			donets(kerr.InvalidRequest.Code)
			return resp, nil
		}
		uniq[rt.Topic] = struct{}{}
	}

	for _, rt := range req.Topics {
		if !c.allowedACL(creq, rt.Topic, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationAlter) {
			donet(rt.Topic, kerr.TopicAuthorizationFailed.Code)
			continue
		}
		t, ok := c.data.tps.gett(rt.Topic)
		if !ok {
			donet(rt.Topic, kerr.UnknownTopicOrPartition.Code)
			continue
		}
		if len(rt.Assignment) > 0 {
			donet(rt.Topic, kerr.InvalidReplicaAssignment.Code)
			continue
		}
		if rt.Count < int32(len(t)) {
			donet(rt.Topic, kerr.InvalidPartitions.Code)
			continue
		}
		for i := int32(len(t)); i < rt.Count; i++ {
			c.data.tps.mkp(rt.Topic, i, c.newPartData(i))
		}
		donet(rt.Topic, 0)
	}

	return resp, nil
}
