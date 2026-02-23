package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// OffsetCommit: v0-10
//
// Version notes:
// * v1: Generation, MemberID
// * v2: RetentionTimeMillis (removed in v5)
// * v3: ThrottleMillis
// * v6: LeaderEpoch in request
// * v7: InstanceID - currently returns error if set
// * v8: Flexible versions
// * v10: TopicID replaces Topic (KIP-848 / KAFKA-19186)

func init() { regKey(8, 0, 10) }

func (c *Cluster) handleOffsetCommit(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.OffsetCommitRequest)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	// v10: resolve TopicIDs to topic names before dispatching to the
	// group goroutine. We run in Cluster.run() so c.data is safe.
	if req.Version >= 10 {
		for i := range req.Topics {
			t := &req.Topics[i]
			name, ok := c.data.id2t[t.TopicID]
			if !ok {
				resp := req.ResponseKind().(*kmsg.OffsetCommitResponse)
				fillOffsetCommit(req, resp, kerr.UnknownTopicID.Code)
				return resp, nil
			}
			t.Topic = name
		}
	}

	c.groups.handleOffsetCommit(creq)
	return nil, nil
}
