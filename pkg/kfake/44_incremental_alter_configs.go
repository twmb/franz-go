package kfake

import (
	"cmp"
	"maps"
	"strconv"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// IncrementalAlterConfigs: v0-1
//
// Supported resource types:
// * TOPIC (2)
// * BROKER (4)
// * CLIENT_METRICS (16)
// * GROUP (32)
//
// Supported operations:
// * SET (0)
// * DELETE (1)
// * APPEND (2)
// * SUBTRACT (3)
//
// Behavior:
// * A resource listed twice, a config named twice in a resource, or a null
//   value for an op other than DELETE answers INVALID_REQUEST
// * A topic config of a known name must parse as its type; any name is
//   accepted
// * APPEND and SUBTRACT start from the default when the config is unset
//
// Version notes:
// * v0: Initial version
// * v1: Flexible versions

func init() { regKey(44, 0, 1) }

func (c *Cluster) handleIncrementalAlterConfigs(creq *clientReq) (kmsg.Response, error) {
	var (
		b    = creq.cc.b
		req  = creq.kreq.(*kmsg.IncrementalAlterConfigsRequest)
		resp = req.ResponseKind().(*kmsg.IncrementalAlterConfigsResponse)
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
		st := kmsg.NewIncrementalAlterConfigsResponseResource()
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
			if rc.Value == nil && rc.Op != kmsg.IncrementalAlterConfigOpDelete {
				pre[i].nulls = append(pre[i].nulls, rc.Name)
			}
		}
	}
	preErrs := alterConfigsPrecheck(pre)

outer:
	for i := range req.Resources {
		rr := &req.Resources[i]
		if msg := preErrs[i]; msg != "" {
			st := kmsg.NewIncrementalAlterConfigsResponseResource()
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
			// We apply every op to a clone: a ValidateOnly request
			// must not leave the ops it walked behind.
			dup := maps.Clone(c.bcfgs)
			var code int16
			for i := range rr.Configs {
				rc := &rr.Configs[i]
				switch rc.Op {
				case kmsg.IncrementalAlterConfigOpSet:
					if !validateBrokerConfig(rc.Name, rc.Value) {
						code = kerr.InvalidRequest.Code
					}
					dup[rc.Name] = rc.Value
				case kmsg.IncrementalAlterConfigOpDelete:
					delete(dup, rc.Name)
				case kmsg.IncrementalAlterConfigOpAppend, kmsg.IncrementalAlterConfigOpSubtract:
					if !isListConfig(rc.Name) {
						code = kerr.InvalidConfig.Code
						continue
					}
					var def string
					if _, ok := validBrokerConfigs[rc.Name]; ok {
						def = configDefaults[rc.Name]
					}
					dup[rc.Name] = configListOp(rc.Op, dup[rc.Name], def, rc.Value)
				default:
					code = kerr.InvalidRequest.Code
				}
			}
			if code != 0 {
				doner(rr.ResourceName, rr.ResourceType, code)
				continue
			}
			doner(rr.ResourceName, rr.ResourceType, 0)
			if req.ValidateOnly {
				continue
			}
			c.bcfgs = dup
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
			var code int16
			for i := range rr.Configs {
				rc := &rr.Configs[i]
				switch rc.Op {
				case kmsg.IncrementalAlterConfigOpSet:
					if !c.data.setTopicConfig(rr.ResourceName, rc.Name, rc.Value, true) {
						code = kerr.InvalidConfig.Code
					}
				case kmsg.IncrementalAlterConfigOpDelete:
				case kmsg.IncrementalAlterConfigOpAppend, kmsg.IncrementalAlterConfigOpSubtract:
					if !isListConfig(rc.Name) {
						code = kerr.InvalidConfig.Code
					}
				default:
					code = kerr.InvalidRequest.Code
				}
			}
			if code != 0 {
				doner(rr.ResourceName, rr.ResourceType, code)
				continue
			}
			doner(rr.ResourceName, rr.ResourceType, 0)
			if req.ValidateOnly {
				continue
			}
			for i := range rr.Configs {
				rc := &rr.Configs[i]
				switch rc.Op {
				case kmsg.IncrementalAlterConfigOpSet:
					c.data.setTopicConfig(rr.ResourceName, rc.Name, rc.Value, false)
				case kmsg.IncrementalAlterConfigOpDelete:
					delete(c.data.tcfgs[rr.ResourceName], rc.Name)
				case kmsg.IncrementalAlterConfigOpAppend, kmsg.IncrementalAlterConfigOpSubtract:
					var def string
					if _, ok := validTopicConfigs[rc.Name]; ok {
						def = configDefaults[rc.Name]
					}
					current := c.data.tcfgs[rr.ResourceName][rc.Name]
					c.data.setTopicConfig(rr.ResourceName, rc.Name, configListOp(rc.Op, current, def, rc.Value), false)
				}
			}
			c.persistTopicsState()

		case kmsg.ConfigResourceTypeClientMetrics:
			// A subscription is a cluster resource, like a broker:
			// AlterConfigs on CLUSTER. A SET on a new name creates
			// the subscription, and deleting its every key removes
			// it, which is how kafka-client-metrics.sh --delete works.
			if e := c.denyCluster(creq, kmsg.ACLOperationAlterConfigs, faultKey{resource: rr.ResourceName}); e != nil {
				doner(rr.ResourceName, rr.ResourceType, e.Code)
				answered[resource{rr.ResourceName, rr.ResourceType}] = true
				if creq.skipsWork(e) { // a timed-out alter still applies
					continue outer
				}
			}
			if rr.ResourceName == "" {
				doner(rr.ResourceName, rr.ResourceType, kerr.InvalidRequest.Code)
				continue
			}
			// We apply every op to a clone and validate what results,
			// as Kafka does: a ValidateOnly request must not leave
			// the ops it walked behind.
			dup := maps.Clone(c.clientMetrics[rr.ResourceName])
			if dup == nil {
				dup = make(map[string]*string)
			}
			if e := alterClientMetrics(dup, rr.Configs); e != nil {
				doner(rr.ResourceName, rr.ResourceType, e.Code)
				continue
			}
			doner(rr.ResourceName, rr.ResourceType, 0)
			if !req.ValidateOnly {
				c.setClientMetrics(rr.ResourceName, dup)
			}

		case kmsg.ConfigResourceTypeGroupConfig:
			if e := c.deny(creq, rr.ResourceName, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationAlterConfigs, faultKey{group: rr.ResourceName, resource: rr.ResourceName}); e != nil {
				doner(rr.ResourceName, rr.ResourceType, e.Code)
				answered[resource{rr.ResourceName, rr.ResourceType}] = true
				if creq.skipsWork(e) { // a timed-out alter still applies
					continue
				}
			}
			if code := c.groupConfigsCode(rr.ResourceName, rr.Configs); code != 0 {
				doner(rr.ResourceName, rr.ResourceType, code)
				continue
			}
			doner(rr.ResourceName, rr.ResourceType, 0)
			if req.ValidateOnly {
				continue
			}
			c.setGroupConfigs(rr.ResourceName, rr.Configs)

		default:
			doner(rr.ResourceName, rr.ResourceType, kerr.InvalidRequest.Code)
		}
	}

	c.refreshCompactTicker()
	c.shareGroups.refreshSweepTicker()
	return resp, nil
}

// groupConfigsCode returns the error Kafka answers for group config ops.
// Group configs are scalar (e.g. share.auto.offset.reset); the protocol's
// Append/Subtract ops are list-valued and not meaningful here.
//
// Per-group config names are UNPREFIXED -- the "group." prefix is only for
// broker-level defaults. Real Kafka returns INVALID_CONFIG for unknown names
// and values it rejects, except in a DELETE, which has nothing to remove. An
// unknown op and the empty name are INVALID_REQUEST.
func (c *Cluster) groupConfigsCode(group string, configs []kmsg.IncrementalAlterConfigsRequestResourceConfig) int16 {
	var code int16
	if group == "" {
		code = kerr.InvalidRequest.Code
	}
	for i := range configs {
		rc := &configs[i]
		switch rc.Op {
		case kmsg.IncrementalAlterConfigOpDelete:
		case kmsg.IncrementalAlterConfigOpSet:
			if !validGroupConfigs[rc.Name] || !c.validGroupConfigValue(rc.Name, rc.Value) {
				code = cmp.Or(code, kerr.InvalidConfig.Code)
			}
		case kmsg.IncrementalAlterConfigOpAppend, kmsg.IncrementalAlterConfigOpSubtract:
			code = cmp.Or(code, kerr.InvalidConfig.Code)
		default:
			code = kerr.InvalidRequest.Code
		}
	}
	return code
}

// setGroupConfigs applies group config ops: a Set writes the value, a Delete
// drops it. Any other op is rejected before we are called. A group with no
// config left is not listed, as in Kafka. Share group behavior follows these
// configs, so we refresh the share sweep after.
func (c *Cluster) setGroupConfigs(group string, configs []kmsg.IncrementalAlterConfigsRequestResourceConfig) {
	if c.groupConfigs == nil {
		c.groupConfigs = make(map[string]map[string]*string)
	}
	gc := c.groupConfigs[group]
	if gc == nil {
		gc = make(map[string]*string)
		c.groupConfigs[group] = gc
	}
	for i := range configs {
		rc := &configs[i]
		switch rc.Op {
		case kmsg.IncrementalAlterConfigOpSet:
			gc[rc.Name] = rc.Value
		case kmsg.IncrementalAlterConfigOpDelete:
			delete(gc, rc.Name)
		case kmsg.IncrementalAlterConfigOpAppend, kmsg.IncrementalAlterConfigOpSubtract:
			// rejected before we are called
		}
	}
	if len(gc) == 0 {
		delete(c.groupConfigs, group)
	}
	c.persistDynamicConfigsState()
	c.shareGroups.refreshSweepTicker()
}

// setClientMetrics stores sub as the client metrics subscription name, or
// removes the subscription if sub is empty.
func (c *Cluster) setClientMetrics(name string, sub map[string]*string) {
	if _, ok := c.clientMetrics[name]; ok || len(sub) > 0 {
		c.telemVersion++
	}
	if len(sub) == 0 {
		delete(c.clientMetrics, name)
	} else {
		if c.clientMetrics == nil {
			c.clientMetrics = make(map[string]map[string]*string)
		}
		c.clientMetrics[name] = sub
	}
	c.persistDynamicConfigsState()
}

// alterClientMetrics applies configs to a client metrics subscription and
// returns the error Kafka answers if a key, op, or resulting value is not
// valid. Kafka answers INVALID_REQUEST for an unknown key or op and
// INVALID_CONFIG for a list op on interval.ms, the one scalar key.
func alterClientMetrics(sub map[string]*string, configs []kmsg.IncrementalAlterConfigsRequestResourceConfig) *kerr.Error {
	for i := range configs {
		rc := &configs[i]
		if _, ok := validClientMetricsConfigs[rc.Name]; !ok {
			return kerr.InvalidRequest
		}
		switch rc.Op {
		case kmsg.IncrementalAlterConfigOpSet:
			sub[rc.Name] = rc.Value
		case kmsg.IncrementalAlterConfigOpDelete:
			delete(sub, rc.Name)
		case kmsg.IncrementalAlterConfigOpAppend, kmsg.IncrementalAlterConfigOpSubtract:
			if !isListConfig(rc.Name) {
				return kerr.InvalidConfig
			}
			sub[rc.Name] = configListOp(rc.Op, sub[rc.Name], "", rc.Value)
		default:
			return kerr.InvalidRequest
		}
	}
	for k, v := range sub {
		if e := validateClientMetricsConfig(k, v); e != nil {
			return e
		}
	}
	return nil
}
