package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Metadata: v0-13
//
// Behavior:
// * Null topics returns all topics (v1+), as do empty topics at v0
// * Empty topics returns no topics
// * AllowAutoTopicCreation is respected if cluster config allows it, and is
//   always on below v4; creating needs cluster or topic CREATE
// * Topics can be requested by name or by ID (v10+)
//
// Version notes:
// * v1: ControllerID, IsInternal, Rack
// * v2: ClusterID
// * v4: AllowAutoTopicCreation
// * v5: OfflineReplicas
// * v7: LeaderEpoch
// * v8: AuthorizedOperations (ACLs)
// * v9: Flexible versions
// * v10: TopicID
// * v13: Top-level ErrorCode for rebootstrapping (KIP-1102) - not implemented

func init() { regKey(3, 0, 13) }

func (c *Cluster) handleMetadata(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.MetadataRequest)
	resp := req.ResponseKind().(*kmsg.MetadataResponse)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	for _, b := range c.bs {
		sb := kmsg.NewMetadataResponseBroker()
		sb.NodeID = b.node
		sb.Host, sb.Port = b.hostport()
		sb.Rack = &brokerRack
		resp.Brokers = append(resp.Brokers, sb)
	}

	resp.ClusterID = &c.cfg.clusterID
	resp.ControllerID = c.controller.node

	tidx := make(map[tkey]int)

	donet := func(t string, id uuid, errCode int16) *kmsg.MetadataResponseTopic {
		key := tkey{t: t}
		if t == "" {
			key.id = id // each nameless ID gets its own entry
		}
		if i, ok := tidx[key]; ok {
			return &resp.Topics[i]
		}
		tidx[key] = len(resp.Topics)
		st := kmsg.NewMetadataResponseTopic()
		if t != "" {
			st.Topic = kmsg.StringPtr(t)
		}
		st.TopicID = id
		st.ErrorCode = errCode
		// We say nothing more about a topic you may not describe.
		if t != "" && errCode != kerr.TopicAuthorizationFailed.Code {
			if v, ok := c.data.tcfgs[t]["kfake.is_internal"]; ok && v != nil && *v == "true" {
				st.IsInternal = true
			}
			if req.IncludeTopicAuthorizedOperations {
				st.AuthorizedOperations = c.topicAuthorizedOps(creq, t)
			}
		}
		resp.Topics = append(resp.Topics, st)
		return &resp.Topics[len(resp.Topics)-1]
	}
	donep := func(t string, id uuid, p int32, errCode int16) *kmsg.MetadataResponseTopicPartition {
		sp := kmsg.NewMetadataResponseTopicPartition()
		sp.Partition = p
		sp.ErrorCode = errCode
		st := donet(t, id, 0)
		st.Partitions = append(st.Partitions, sp)
		return &st.Partitions[len(st.Partitions)-1]
	}
	okp := func(t string, id uuid, p int32, pd *partData) {
		if e := creq.faults.check(faultKey{topic: t, topicID: id}.part(p)); e != nil {
			donep(t, id, p, e.Code).Leader = -1
			return
		}
		nreplicas := c.data.treplicas[t]
		if nreplicas > len(c.bs) {
			nreplicas = len(c.bs)
		}

		sp := donep(t, id, p, 0)
		sp.Leader = pd.leader.node
		sp.LeaderEpoch = pd.epoch

		for i := 0; i < nreplicas; i++ {
			idx := (pd.leader.bsIdx + i) % len(c.bs)
			sp.Replicas = append(sp.Replicas, c.bs[idx].node)
		}
		sp.ISR = sp.Replicas
	}

	allowAuto := (req.AllowAutoTopicCreation || req.Version < 4) && c.cfg.allowAutoTopic
	var created bool
	// Dedupe after name resolution so a request that specifies both a
	// topic name and its TopicID produces one response entry with one
	// set of partitions (not a merged entry with partitions duplicated).
	seenTopic := make(map[string]bool, len(req.Topics))
	for _, rt := range req.Topics {
		var topic string
		var ok bool
		// If topic ID is present, we ignore any provided topic.
		// Topics with no topic and no ID are ignored.
		if rt.TopicID != noID {
			if topic, ok = c.data.id2t[rt.TopicID]; !ok {
				code := kerr.UnknownTopicID.Code
				if e := creq.faults.check(faultKey{topicID: rt.TopicID}); e != nil {
					code = e.Code
				}
				donet("", rt.TopicID, code)
				continue
			}
		} else if rt.Topic == nil {
			continue
		} else {
			topic = *rt.Topic
		}

		if e := c.deny(creq, topic, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationDescribe, faultKey{topic: topic}); e != nil {
			if rt.TopicID != noID && e == kerr.TopicAuthorizationFailed {
				donet("", rt.TopicID, e.Code) // asked by ID, denied without the name
			} else {
				donet(topic, rt.TopicID, e.Code)
			}
			continue
		}

		ps, ok := c.data.tps.gett(topic)
		if !ok {
			if allowAuto && !c.allowedClusterACL(creq, kmsg.ACLOperationCreate) {
				if e := c.deny(creq, topic, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationCreate, faultKey{topic: topic}); e != nil {
					donet(topic, noID, e.Code)
					continue
				}
			}
			if !validTopicName(topic) {
				donet(topic, noID, kerr.InvalidTopicException.Code)
				continue
			}
			if !allowAuto {
				donet(topic, noID, kerr.UnknownTopicOrPartition.Code)
				continue
			}
			// Kafka checks the name, then asks the controller to
			// create the topic and answers UNKNOWN_TOPIC_OR_PARTITION
			// while it does. We create the topic at once, and answer
			// a topic the controller would refuse the same way.
			ct := kmsg.NewCreateTopicsRequestTopic()
			ct.Topic = topic
			ct.NumPartitions = -1
			ct.ReplicationFactor = -1
			if _, _, _, e := c.createTopic(&ct, nil, false); e != nil {
				donet(topic, noID, kerr.UnknownTopicOrPartition.Code)
				continue
			}
			created = true
			ps, _ = c.data.tps.gett(topic)
		}

		if seenTopic[topic] {
			continue
		}
		seenTopic[topic] = true

		id := c.data.t2id[topic]
		for p, pd := range ps {
			okp(topic, id, p, pd)
		}
	}
	if (req.Topics == nil || req.Version == 0 && len(req.Topics) == 0) && c.data.tps != nil {
		for topic, ps := range c.data.tps {
			// Listing all topics leaves out a topic you may not
			// describe; a fault answers the topic with its error.
			if e := c.deny(creq, topic, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationDescribe, faultKey{topic: topic}); e != nil {
				if e != kerr.TopicAuthorizationFailed {
					donet(topic, c.data.t2id[topic], e.Code)
				}
				continue
			}
			id := c.data.t2id[topic]
			for p, pd := range ps {
				okp(topic, id, p, pd)
			}
		}
	}

	if req.IncludeClusterAuthorizedOperations {
		resp.AuthorizedOperations = c.clusterAuthorizedOps(creq)
	}

	if created {
		c.notifyTopicChange()
		c.refreshCompactTicker()
		c.persistTopicsState()
	}

	return resp, nil
}

// validTopicName reports whether Kafka accepts t as a topic name: 1 to 249
// of [a-zA-Z0-9._-], other than "." and "..".
func validTopicName(t string) bool {
	if t == "" || t == "." || t == ".." || len(t) > 249 {
		return false
	}
	for i := range len(t) {
		switch b := t[i]; {
		case b >= 'a' && b <= 'z', b >= 'A' && b <= 'Z', b >= '0' && b <= '9', b == '.', b == '_', b == '-':
		default:
			return false
		}
	}
	return true
}

// tkey keys a response's topic entries: a topic by its name, or by its ID if
// it has no name, so that no name collides with an ID.
type tkey struct {
	t  string
	id uuid
}
