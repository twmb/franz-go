package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// DeleteTopics: v0-6
//
// Behavior:
// * Must be sent to the controller
// * Deletes topics by name (v0-5) or by name/ID (v6+)
// * A duplicated name or ID, or an entry with both or neither, answers
//   INVALID_REQUEST and is not deleted
// * ACL: DELETE on CLUSTER, or DESCRIBE and DELETE on the topic
// * Wakes any watching fetchers on deletion
//
// Version notes:
// * v1: ThrottleMillis
// * v4: Flexible versions
// * v5: ErrorMessage in response
// * v6: Topics array with TopicID support

func init() { regKey(20, 0, 6) }

func (c *Cluster) handleDeleteTopics(creq *clientReq) (kmsg.Response, error) {
	var (
		b   = creq.cc.b
		req = creq.kreq.(*kmsg.DeleteTopicsRequest)
	)
	resp := req.ResponseKind().(*kmsg.DeleteTopicsResponse)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	// A topic gets one answer. A fault can answer a topic before the
	// work runs, and a request can name a topic twice; a later answer
	// adds no entry and keeps the first code. We key an answer by what
	// the request named the topic with: its name (a string) or its ID (a
	// uuid). An entry with both or neither is keyed by its index.
	answered := make(map[any]bool)
	answer := func(k any, t *string, id uuid, errCode int16) {
		if answered[k] {
			return
		}
		answered[k] = true
		st := kmsg.NewDeleteTopicsResponseTopic()
		st.Topic = t
		st.TopicID = id
		st.ErrorCode = errCode
		resp.Topics = append(resp.Topics, st)
	}

	if req.Version <= 5 {
		for _, topic := range req.TopicNames {
			rt := kmsg.NewDeleteTopicsRequestTopic()
			rt.Topic = kmsg.StringPtr(topic)
			req.Topics = append(req.Topics, rt)
		}
	}

	misrouted := b != c.controller

	// DELETE on CLUSTER allows deleting (and so describing) any topic.
	clusterDelete := c.allowedClusterACL(creq, kmsg.ACLOperationDelete)
	allowed := func(topic string, op kmsg.ACLOperation) bool {
		return clusterDelete || c.allowedACL(creq, topic, kmsg.ACLResourceTypeTopic, op)
	}

	// Kafka answers INVALID_REQUEST for an entry with both a name and
	// an ID or with neither, and once for a name or ID the request
	// lists more than once; it deletes none of those.
	nnames := make(map[string]int)
	nids := make(map[uuid]int)
	for _, rt := range req.Topics {
		switch {
		case rt.Topic != nil && rt.TopicID == noID:
			nnames[*rt.Topic]++
		case rt.Topic == nil && rt.TopicID != noID:
			nids[rt.TopicID]++
		}
	}

	// Kafka fails the whole request if its deadline passed before the
	// controller looked up a topic. It looks up only the names and IDs
	// the request lists once.
	var lookup bool
	for _, n := range nnames {
		lookup = lookup || n == 1
	}
	for _, n := range nids {
		lookup = lookup || n == 1
	}
	if !misrouted && req.TimeoutMillis <= 0 && lookup {
		for i, rt := range req.Topics {
			answer(i, rt.Topic, rt.TopicID, kerr.RequestTimedOut.Code)
		}
		return resp, nil
	}

	// We resolve IDs before names, as Kafka does, so a name that maps to
	// an ID the request also lists is caught below.
	var (
		byID       = make(map[uuid]string) // topics to delete, named by ID
		names      []string                // names to resolve
		nameDelete []string                // topics to delete, named by name
	)
	for i, rt := range req.Topics {
		switch {
		case (rt.Topic != nil) == (rt.TopicID != noID):
			answer(i, rt.Topic, rt.TopicID, kerr.InvalidRequest.Code)
		case rt.Topic != nil && nnames[*rt.Topic] > 1:
			answer(*rt.Topic, rt.Topic, noID, kerr.InvalidRequest.Code)
		case rt.TopicID != noID && nids[rt.TopicID] > 1:
			answer(rt.TopicID, nil, rt.TopicID, kerr.InvalidRequest.Code)
		case rt.Topic != nil:
			names = append(names, *rt.Topic)
		default:
			id := rt.TopicID
			topic, ok := c.data.id2t[id]
			if !ok {
				if misrouted {
					answer(id, nil, id, kerr.NotController.Code)
					continue
				}
				if e := creq.faults.check(faultKey{topicID: id}); e != nil {
					answer(id, nil, id, e.Code)
				}
				answer(id, nil, id, kerr.UnknownTopicID.Code)
				continue
			}
			// The name is returned only if you may describe it.
			if !allowed(topic, kmsg.ACLOperationDelete) {
				var name *string
				if allowed(topic, kmsg.ACLOperationDescribe) {
					name = &topic
				}
				answer(id, name, id, kerr.TopicAuthorizationFailed.Code)
				continue
			}
			if misrouted {
				answer(id, nil, id, kerr.NotController.Code)
				continue
			}
			if e := creq.faults.check(faultKey{topic: topic, topicID: id}); e != nil {
				answer(id, nil, id, e.Code)
				if creq.skipsWork(e) { // a timed-out delete still deletes the topic
					continue
				}
			}
			byID[id] = topic
		}
	}
	for _, topic := range names {
		id, ok := c.data.t2id[topic]
		// A topic you may describe but not delete answers whether it
		// exists; one you may not describe never does.
		if !allowed(topic, kmsg.ACLOperationDescribe) || ok && !allowed(topic, kmsg.ACLOperationDelete) {
			answer(topic, &topic, noID, kerr.TopicAuthorizationFailed.Code)
			continue
		}
		if misrouted {
			answer(topic, &topic, noID, kerr.NotController.Code)
			continue
		}
		if e := creq.faults.check(faultKey{topic: topic}); e != nil {
			answer(topic, &topic, noID, e.Code)
			if creq.skipsWork(e) {
				continue
			}
		}
		if !ok {
			answer(topic, &topic, noID, kerr.UnknownTopicOrPartition.Code)
			continue
		}
		if _, dup := byID[id]; dup || nids[id] > 1 {
			delete(byID, id)
			answer(topic, &topic, id, kerr.InvalidRequest.Code)
			continue
		}
		answer(topic, &topic, id, 0)
		nameDelete = append(nameDelete, topic)
	}

	for id, topic := range byID {
		answer(id, &topic, id, 0)
		c.deleteTopic(topic, id)
	}
	for _, topic := range nameDelete {
		c.deleteTopic(topic, c.data.t2id[topic])
	}
	if len(byID)+len(nameDelete) > 0 {
		c.notifyTopicChange()
		c.refreshCompactTicker()
		c.persistTopicsState()
	}
	return resp, nil
}

// deleteTopic wakes the topic's watching fetchers and tears the topic down:
// its data, its files, and everything else keyed by the topic. The caller
// runs notifyTopicChange, refreshCompactTicker and persistTopicsState once,
// after its whole batch.
func (c *Cluster) deleteTopic(topic string, id uuid) {
	t, ok := c.data.tps.gett(topic)
	if !ok {
		return
	}
	for _, pd := range t {
		for watch := range pd.watch {
			watch.deleted()
		}
	}
	// Close active segment files before removing partition directories.
	for p, pd := range t {
		pd.closeAllFiles(false)
		pdir := partDir(c.storageDir, topic, p)
		if err := c.fs.RemoveAll(pdir); err != nil {
			c.cfg.logger.Logf(LogLevelWarn, "delete topic %s partition %d dir: %v", topic, p, err)
		}
	}
	delete(c.data.tps, topic)
	delete(c.data.id2t, id)
	delete(c.data.t2id, topic)
	delete(c.data.treplicas, topic)
	delete(c.data.tcfgs, topic)
	c.data.dropNorm(topic)
	// Producer state is per-log and dies with the topic: a recreated topic
	// rehydrates empty state, and handleProduce decides what each version
	// accepts from a producer it has no state for. Transactional
	// REGISTRATIONS survive (the coordinator is name-keyed on a real
	// broker); endTx re-resolves current partition data when writing
	// markers.
	for _, pidinf := range c.pids.ids {
		delete(pidinf.windows, topic)
		delete(pidinf.txPartFirstOffsets, topic)
		delete(pidinf.txPartBytes, topic)
	}
	// Share-partition state is topic-ID-keyed on a real broker and dies
	// with the topic; kfake keys by name, so clear it explicitly: a
	// recreated topic starts share consumption fresh (SPSO per group
	// config, no acquired records).
	for _, sg := range c.shareGroups.gs {
		delete(sg.partitions, topic)
	}
	c.dropGroupCommits(topic)
}
