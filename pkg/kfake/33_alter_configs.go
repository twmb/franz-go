package kfake

import (
	"strconv"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// AlterConfigs: v0-2
//
// Supported resource types:
// * TOPIC (2)
// * BROKER (4)
// * GROUP (32)
//
// Behavior:
// * Replaces all configs with the provided set (non-incremental)
// * ValidateOnly mode supported
// * A resource listed twice, a config named twice in a resource, or a null
//   value answers INVALID_REQUEST
// * A topic config of a known name must parse as its type; any name is
//   accepted
//
// Version notes:
// * v2: Flexible versions
//
// Note: Deprecated in favor of IncrementalAlterConfigs (44)

func init() { regKey(33, 0, 2) }

func (c *Cluster) handleAlterConfigs(creq *clientReq) (kmsg.Response, error) {
	var (
		b    = creq.cc.b
		req  = creq.kreq.(*kmsg.AlterConfigsRequest)
		resp = req.ResponseKind().(*kmsg.AlterConfigsResponse)
	)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	type resource struct {
		n string
		t kmsg.ConfigResourceType
	}
	answered := make(map[resource]bool)
	doner := func(n string, t kmsg.ConfigResourceType, errCode int16) {
		// A fault can answer a resource before the work runs. The
		// work's own answer for that resource must not add an entry or
		// replace the code.
		if answered[resource{n, t}] {
			return
		}
		st := kmsg.NewAlterConfigsResponseResource()
		st.ResourceName = n
		st.ResourceType = t
		st.ErrorCode = errCode
		resp.Resources = append(resp.Resources, st)
	}

	pre := make([]alterResource, len(req.Resources))
	for i, rr := range req.Resources {
		pre[i] = alterResource{typ: rr.ResourceType, name: rr.ResourceName}
		for _, rc := range rr.Configs {
			pre[i].keys = append(pre[i].keys, rc.Name)
			if rc.Value == nil {
				pre[i].nulls = append(pre[i].nulls, rc.Name)
			}
		}
	}
	preErrs := alterConfigsPrecheck(pre)

outer:
	for i := range req.Resources {
		rr := &req.Resources[i]
		if msg := preErrs[i]; msg != "" {
			st := kmsg.NewAlterConfigsResponseResource()
			st.ResourceName = rr.ResourceName
			st.ResourceType = rr.ResourceType
			st.ErrorCode = kerr.InvalidRequest.Code
			st.ErrorMessage = &msg
			resp.Resources = append(resp.Resources, st)
			continue
		}
		switch rr.ResourceType {
		case kmsg.ConfigResourceTypeBroker:
			if e := c.denyCluster(creq, kmsg.ACLOperationAlterConfigs, brokerConfigFaultKey(b, rr.ResourceName)); e != nil {
				doner(rr.ResourceName, rr.ResourceType, e.Code)
				answered[resource{rr.ResourceName, rr.ResourceType}] = true
				if creq.skipsWork(e) { // a timed-out alter still applies
					continue outer
				}
			}
			if rr.ResourceName != "" {
				iid, err := strconv.Atoi(rr.ResourceName)
				if err != nil || int32(iid) != b.node {
					doner(rr.ResourceName, rr.ResourceType, kerr.InvalidRequest.Code)
					continue outer
				}
			}
			newBcfgs := make(map[string]*string, len(rr.Configs))
			var invalid bool
			for i := range rr.Configs {
				rc := &rr.Configs[i]
				if !validateBrokerConfig(rc.Name, rc.Value) {
					invalid = true
				}
				newBcfgs[rc.Name] = rc.Value
			}
			if invalid {
				doner(rr.ResourceName, rr.ResourceType, kerr.InvalidRequest.Code)
				continue
			}
			doner(rr.ResourceName, rr.ResourceType, 0)
			if req.ValidateOnly {
				continue
			}
			c.bcfgs = newBcfgs
			c.persistBrokerConfigsState()

		case kmsg.ConfigResourceTypeTopic:
			if e := c.deny(creq, rr.ResourceName, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationAlterConfigs, faultKey{topic: rr.ResourceName, resource: rr.ResourceName}); e != nil {
				doner(rr.ResourceName, rr.ResourceType, e.Code)
				answered[resource{rr.ResourceName, rr.ResourceType}] = true
				if creq.skipsWork(e) { // a timed-out alter still applies
					continue
				}
			}
			if rr.ResourceName == "" { // Kafka refuses the default topic resource
				doner(rr.ResourceName, rr.ResourceType, kerr.InvalidRequest.Code)
				continue
			}
			if _, ok := c.data.tps.gett(rr.ResourceName); !ok {
				doner(rr.ResourceName, rr.ResourceType, kerr.UnknownTopicOrPartition.Code)
				continue
			}
			var invalid bool
			for i := range rr.Configs {
				rc := &rr.Configs[i]
				invalid = invalid || !c.data.setTopicConfig(rr.ResourceName, rc.Name, rc.Value, true)
			}
			if invalid {
				doner(rr.ResourceName, rr.ResourceType, kerr.InvalidConfig.Code)
				continue
			}
			doner(rr.ResourceName, rr.ResourceType, 0)
			if req.ValidateOnly {
				continue
			}
			delete(c.data.tcfgs, rr.ResourceName)
			for i := range rr.Configs {
				rc := &rr.Configs[i]
				c.data.setTopicConfig(rr.ResourceName, rc.Name, rc.Value, false)
			}
			c.persistTopicsState()

		case kmsg.ConfigResourceTypeGroupConfig:
			if e := c.deny(creq, rr.ResourceName, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationAlterConfigs, faultKey{group: rr.ResourceName, resource: rr.ResourceName}); e != nil {
				doner(rr.ResourceName, rr.ResourceType, e.Code)
				answered[resource{rr.ResourceName, rr.ResourceType}] = true
				if creq.skipsWork(e) { // a timed-out alter still applies
					continue
				}
			}
			ops := setOps(rr.Configs)
			if code := c.groupConfigsCode(rr.ResourceName, ops); code != 0 {
				doner(rr.ResourceName, rr.ResourceType, code)
				continue
			}
			doner(rr.ResourceName, rr.ResourceType, 0)
			if !req.ValidateOnly {
				delete(c.groupConfigs, rr.ResourceName) // the configs replace the group's
				c.setGroupConfigs(rr.ResourceName, ops)
			}

		default:
			doner(rr.ResourceName, rr.ResourceType, kerr.InvalidRequest.Code)
		}
	}

	c.refreshCompactTicker()
	c.shareGroups.refreshSweepTicker()
	return resp, nil
}

// setOps returns legacy AlterConfigs configs as Set ops.
func setOps(configs []kmsg.AlterConfigsRequestResourceConfig) []kmsg.IncrementalAlterConfigsRequestResourceConfig {
	ops := make([]kmsg.IncrementalAlterConfigsRequestResourceConfig, len(configs))
	for i, rc := range configs {
		ops[i] = kmsg.IncrementalAlterConfigsRequestResourceConfig{Name: rc.Name, Op: kmsg.IncrementalAlterConfigOpSet, Value: rc.Value}
	}
	return ops
}
