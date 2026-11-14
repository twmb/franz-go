package kfake

import (
	"slices"
	"strings"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// CreateTopics: v0-7
//
// Behavior:
// * Must be sent to the controller
// * ReplicaAssignment: uses len(assignment) as partition count; NumPartitions
//   and ReplicationFactor must be -1 when provided
// * A name that differs only in . vs _ from an existing topic, or from an
//   earlier topic in the request, is rejected
// * A duplicated name answers INVALID_REQUEST; the other topics proceed
// * More than 10000 new partitions in one request fails it with POLICY_VIOLATION
// * ValidateOnly (v1+): performs validation without creating topics
//
// Version notes:
// * v1: ValidateOnly, ErrorMessage in response
// * v2: ThrottleMillis
// * v4: Creation defaults (KIP-464) - using broker defaults for -1 values
// * v5: Configs in response (KIP-525), flexible versions
// * v7: TopicID in response

func init() { regKey(19, 0, 7) }

func (c *Cluster) handleCreateTopics(creq *clientReq) (kmsg.Response, error) {
	var (
		b   = creq.cc.b
		req = creq.kreq.(*kmsg.CreateTopicsRequest)
	)
	resp := req.ResponseKind().(*kmsg.CreateTopicsResponse)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	// Check if user has CREATE on CLUSTER (allows creating any topic)
	clusterCreate := c.allowedClusterACL(creq, kmsg.ACLOperationCreate)

	// A fault can answer a topic before the work runs. The work's own
	// answer for that topic must not add an entry or replace the code.
	answered := make(map[string]int)
	donet := func(t string, errCode int16) *kmsg.CreateTopicsResponseTopic {
		if i, ok := answered[t]; ok {
			return &resp.Topics[i]
		}
		answered[t] = len(resp.Topics)
		st := kmsg.NewCreateTopicsResponseTopic()
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
			st := kmsg.NewCreateTopicsResponseTopic()
			st.Topic = rt.Topic
			st.ErrorCode = errCode
			resp.Topics = append(resp.Topics, st)
		}
		return resp, nil
	}

	misrouted := b != c.controller

	// Kafka answers a duplicated name, and the metadata log's name, with
	// INVALID_REQUEST before checking anything else, and creates the rest.
	counts := make(map[string]int)
	for _, rt := range req.Topics {
		counts[rt.Topic]++
	}

	var (
		creates  []*kmsg.CreateTopicsRequestTopic
		newParts int64
	)
	for i := range req.Topics {
		rt := &req.Topics[i]
		if counts[rt.Topic] > 1 || rt.Topic == clusterMetadataTopic {
			donet(rt.Topic, kerr.InvalidRequest.Code)
			continue
		}
		// ACL check: cluster CREATE or topic CREATE, then routing
		tk := faultKey{topic: rt.Topic, misrouted: misrouted}
		var e *kerr.Error
		if clusterCreate {
			e = creq.faults.check(tk)
		} else {
			e = c.deny(creq, rt.Topic, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationCreate, tk)
		}
		if e != nil {
			donet(rt.Topic, e.Code)
			if creq.skipsWork(e) { // a timed-out create still creates the topic
				continue
			}
		}
		if misrouted {
			donet(rt.Topic, kerr.NotController.Code)
			continue
		}
		creates = append(creates, rt)
		switch {
		case len(rt.ReplicaAssignment) > 0:
			newParts += int64(len(rt.ReplicaAssignment))
		case rt.NumPartitions == -1:
			newParts += int64(c.cfg.defaultNumParts)
		case rt.NumPartitions > 0:
			newParts += int64(rt.NumPartitions)
		}
	}

	if misrouted {
		return resp, nil
	}

	// Kafka fails the whole request if its deadline passed before the
	// controller ran it, or if it would create too many partitions. The
	// controller runs only if a topic is left.
	if len(creates) > 0 {
		switch {
		case req.TimeoutMillis <= 0:
			return failAll(kerr.RequestTimedOut.Code)
		case newParts > maxPartitionsPerRequest:
			return failAll(kerr.PolicyViolation.Code)
		}
	}

	claimed := make(map[string]string) // normalized name => the entry that took it
	for _, rt := range creates {
		nparts, nreplicas, configs, e := c.createTopic(rt, claimed, req.ValidateOnly)
		if e != nil {
			st := donet(rt.Topic, e.Code)
			if other := c.collidesWith(rt.Topic, claimed); e == kerr.InvalidTopicException && other != "" && st.ErrorCode == e.Code {
				st.ErrorMessage = kmsg.StringPtr("Topic '" + rt.Topic + "' collides with existing topic: " + other)
			}
			continue
		}
		claimed[normalizeTopicName(rt.Topic)] = rt.Topic
		st := donet(rt.Topic, 0)
		if !req.ValidateOnly {
			st.TopicID = c.data.t2id[rt.Topic]
		}
		// Without DESCRIBE_CONFIGS on the topic, Kafka leaves out what
		// the topic was created with.
		if !c.allowedACL(creq, rt.Topic, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationDescribeConfigs) {
			st.ConfigErrorCode = kerr.TopicAuthorizationFailed.Code
			continue
		}
		st.NumPartitions = int32(nparts)
		st.ReplicationFactor = int16(nreplicas)
		st.Configs = c.createdTopicConfigs(rt.Topic, configs, req.ValidateOnly)
	}

	if !req.ValidateOnly {
		c.notifyTopicChange()
		c.refreshCompactTicker()
		c.persistTopicsState()
	}

	return resp, nil
}

// maxPartitionsPerRequest is Kafka's default controller.max.records.per.batch:
// a CreateTopics or CreatePartitions that would add more partitions fails.
const maxPartitionsPerRequest = 10000

// clusterMetadataTopic is the metadata log's name, which Kafka refuses to
// create.
const clusterMetadataTopic = "__cluster_metadata"

// createdTopicConfigs returns every config a newly created topic has, sorted
// by name, as CreateTopics v5+ answers. A validate-only create stores nothing,
// so we add the request's configs ourselves.
func (c *Cluster) createdTopicConfigs(t string, reqConfigs map[string]*string, validateOnly bool) []kmsg.CreateTopicsResponseTopicConfig {
	var cfgs []kmsg.CreateTopicsResponseTopicConfig
	idx := make(map[string]int)
	add := func(k string, v *string, src kmsg.ConfigSource, _ bool) {
		rc := kmsg.NewCreateTopicsResponseTopicConfig()
		rc.Name, rc.Value, rc.Source = k, v, int8(src)
		if i, ok := idx[k]; ok { // a later source overrides
			cfgs[i] = rc
			return
		}
		idx[k] = len(cfgs)
		cfgs = append(cfgs, rc)
	}
	c.data.configs(t, add)
	if validateOnly {
		for k, v := range reqConfigs {
			add(k, v, kmsg.ConfigSourceDynamicTopicConfig, false)
		}
	}
	slices.SortFunc(cfgs, func(l, r kmsg.CreateTopicsResponseTopicConfig) int { return strings.Compare(l.Name, r.Name) })
	return cfgs
}

// createTopic runs CreateTopics's per-topic work: it validates the name, the
// counts and any manual assignment, then creates the topic. inReq maps a
// normalized name to the earlier entry of the same request that took it, and
// is nil outside a request. A validate-only create runs every check and
// creates nothing.
//
// The counts and configs we return are the ones we used, which the response
// reports. The caller runs notifyTopicChange, refreshCompactTicker and
// persistTopicsState once, after its whole batch.
func (c *Cluster) createTopic(rt *kmsg.CreateTopicsRequestTopic, inReq map[string]string, validateOnly bool) (int, int, map[string]*string, *kerr.Error) {
	if rt.Topic == clusterMetadataTopic {
		return 0, 0, nil, kerr.InvalidRequest
	}
	if !validTopicName(rt.Topic) {
		return 0, 0, nil, kerr.InvalidTopicException
	}
	if _, ok := c.data.tps.gett(rt.Topic); ok {
		return 0, 0, nil, kerr.TopicAlreadyExists
	}
	if c.collidesWith(rt.Topic, inReq) != "" {
		return 0, 0, nil, kerr.InvalidTopicException
	}
	var nparts, nreplicas int
	if len(rt.ReplicaAssignment) > 0 {
		// When ReplicaAssignment is provided, NumPartitions and
		// ReplicationFactor must be -1 (per Kafka validation).
		if rt.NumPartitions != -1 || rt.ReplicationFactor != -1 {
			return 0, 0, nil, kerr.InvalidRequest
		}
		// Every partition needs a valid replica list of the same
		// length, and the IDs are consecutive from 0.
		ids := make(map[int32]struct{}, len(rt.ReplicaAssignment))
		for _, ra := range rt.ReplicaAssignment {
			if _, dup := ids[ra.Partition]; dup {
				return 0, 0, nil, kerr.InvalidReplicaAssignment
			}
			ids[ra.Partition] = struct{}{}
			if !c.validReplicas(ra.Replicas) || len(ra.Replicas) != len(rt.ReplicaAssignment[0].Replicas) {
				return 0, 0, nil, kerr.InvalidReplicaAssignment
			}
		}
		for i := range int32(len(rt.ReplicaAssignment)) {
			if _, ok := ids[i]; !ok {
				return 0, 0, nil, kerr.InvalidReplicaAssignment
			}
		}
		nparts = len(rt.ReplicaAssignment)
		nreplicas = len(rt.ReplicaAssignment[0].Replicas)
	} else {
		switch {
		case rt.ReplicationFactor == 0 || rt.ReplicationFactor < -1:
			return 0, 0, nil, kerr.InvalidReplicationFactor
		case rt.NumPartitions == 0 || rt.NumPartitions < -1:
			return 0, 0, nil, kerr.InvalidPartitions
		case int(rt.ReplicationFactor) > len(c.bs):
			return 0, 0, nil, kerr.InvalidReplicationFactor
		}
		nparts = int(rt.NumPartitions)
		if nparts < 0 {
			nparts = c.cfg.defaultNumParts
		}
		nreplicas = int(rt.ReplicationFactor)
		if nreplicas < 0 {
			nreplicas = min(3, len(c.bs))
		}
	}
	// A request caps the partitions it adds before it gets here; this
	// caps a create from Metadata or Cluster.CreateTopic.
	if nparts > maxPartitionsPerRequest {
		return 0, 0, nil, kerr.PolicyViolation
	}

	configs := make(map[string]*string)
	for _, rc := range rt.Configs {
		if !validTopicConfig(rc.Name, rc.Value) {
			return 0, 0, nil, kerr.InvalidConfig
		}
		configs[rc.Name] = rc.Value
	}

	// ValidateOnly (v1+): skip actual creation
	if !validateOnly {
		c.data.mkt(rt.Topic, nparts, nreplicas, configs)
		// The first replica of a manual assignment leads, as in Kafka.
		t := c.data.tps[rt.Topic]
		for _, ra := range rt.ReplicaAssignment {
			t[ra.Partition].leader = c.nodeBroker(ra.Replicas[0])
		}
	}
	return nparts, nreplicas, configs, nil
}

// collidesWith returns the topic that t collides with, or "": an existing
// topic, or an earlier entry of the same request, whose name differs from t
// only in . and _. Unlike Kafka, which creates both of two such names in one
// request, we refuse the later one.
func (c *Cluster) collidesWith(t string, inReq map[string]string) string {
	normalized := normalizeTopicName(t)
	for _, other := range c.data.tnorms[normalized] {
		if other != t {
			return other
		}
	}
	if other, ok := inReq[normalized]; ok && other != t {
		return other
	}
	return ""
}

// validReplicas reports whether replicas is a manual assignment Kafka
// accepts: not empty, every broker exists, and no broker twice.
func (c *Cluster) validReplicas(replicas []int32) bool {
	if len(replicas) == 0 {
		return false
	}
	for i, r := range replicas {
		// The unknown-broker check first bounds the duplicate scan
		// to the broker count.
		if c.nodeBroker(r) == nil || slices.Contains(replicas[:i], r) {
			return false
		}
	}
	return true
}

// nodeBroker returns the broker with the given node ID, or nil.
func (c *Cluster) nodeBroker(node int32) *broker {
	for _, b := range c.bs {
		if b.node == node {
			return b
		}
	}
	return nil
}
