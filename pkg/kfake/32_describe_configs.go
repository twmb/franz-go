package kfake

import (
	"maps"
	"slices"
	"strconv"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// DescribeConfigs: v0-4
//
// Supported resource types:
// * TOPIC (2)
// * BROKER (4): the empty name describes only the cluster-wide dynamic
//   configs, as Kafka does
// * CLIENT_METRICS (16)
// * GROUP (32): every key at its effective value
//
// Version notes:
// * v1: ConfigSynonyms in response
// * v2: ThrottleMillis
// * v3: ConfigType and ConfigDocumentation in response
// * v4: Flexible versions

func init() { regKey(32, 0, 4) }

func (c *Cluster) handleDescribeConfigs(creq *clientReq) (kmsg.Response, error) {
	var (
		b    = creq.cc.b
		req  = creq.kreq.(*kmsg.DescribeConfigsRequest)
		resp = req.ResponseKind().(*kmsg.DescribeConfigsResponse)
	)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	doner := func(n string, t kmsg.ConfigResourceType, errCode int16) *kmsg.DescribeConfigsResponseResource {
		st := kmsg.NewDescribeConfigsResponseResource()
		st.ResourceName = n
		st.ResourceType = t
		st.ErrorCode = errCode
		resp.Resources = append(resp.Resources, st)
		return &resp.Resources[len(resp.Resources)-1]
	}

	rfn := func(r *kmsg.DescribeConfigsResponseResource) func(k string, v *string, src kmsg.ConfigSource, sensitive bool) {
		nameIdxs := make(map[string]int)
		return func(k string, v *string, src kmsg.ConfigSource, sensitive bool) {
			rc := kmsg.NewDescribeConfigsResponseResourceConfig()
			rc.Name = k
			rc.Value = v
			rc.Source = src
			rc.ReadOnly = rc.Source == kmsg.ConfigSourceStaticBrokerConfig
			rc.IsDefault = rc.Source == kmsg.ConfigSourceDefaultConfig || rc.Source == kmsg.ConfigSourceStaticBrokerConfig
			rc.IsSensitive = sensitive
			rc.ConfigType = configTypes[k] // v3+: defaults to 0 (UNKNOWN) if not in map

			// We walk configs from static to default to dynamic. As
			// in Kafka, the value in effect is the first synonym; a
			// value that overrides an earlier one goes before its
			// synonyms. Kafka lists no synonym for a null default.
			if v != nil {
				syn := kmsg.NewDescribeConfigsResponseResourceConfigConfigSynonym()
				syn.Name, syn.Source = k, src
				if !sensitive {
					syn.Value = v
				}
				rc.ConfigSynonyms = []kmsg.DescribeConfigsResponseResourceConfigConfigSynonym{syn}
			}
			if idx, ok := nameIdxs[k]; ok {
				rc.ConfigSynonyms = append(rc.ConfigSynonyms, r.Configs[idx].ConfigSynonyms...)
				r.Configs[idx] = rc
				return
			}
			nameIdxs[k] = len(r.Configs)
			r.Configs = append(r.Configs, rc)
		}
	}
	filter := func(rr *kmsg.DescribeConfigsRequestResource, r *kmsg.DescribeConfigsResponseResource) {
		if !req.IncludeSynonyms { // like Kafka, we answer synonyms only when asked
			for i := range r.Configs {
				r.Configs[i].ConfigSynonyms = nil
			}
		}
		if len(rr.ConfigNames) == 0 { // null or empty describes every key
			return
		}
		names := make(map[string]struct{})
		for _, name := range rr.ConfigNames {
			names[name] = struct{}{}
		}
		keep := r.Configs[:0]
		for _, rc := range r.Configs {
			if _, ok := names[rc.Name]; ok {
				keep = append(keep, rc)
			}
		}
		r.Configs = keep
	}

outer:
	for i := range req.Resources {
		rr := &req.Resources[i]
		switch rr.ResourceType {
		case kmsg.ConfigResourceTypeBroker:
			if e := c.denyCluster(creq, kmsg.ACLOperationDescribeConfigs, brokerConfigFaultKey(b, rr.ResourceName)); e != nil {
				doner(rr.ResourceName, rr.ResourceType, e.Code)
				continue outer
			}
			if rr.ResourceName == "" {
				r := doner(rr.ResourceName, rr.ResourceType, 0)
				emit := rfn(r)
				for _, k := range slices.Sorted(maps.Keys(c.bcfgs)) {
					emit(k, c.bcfgs[k], kmsg.ConfigSourceDynamicDefaultBrokerConfig, false)
				}
				filter(rr, r)
				continue
			}
			id, err := strconv.Atoi(rr.ResourceName)
			if err != nil || int32(id) != b.node {
				doner(rr.ResourceName, rr.ResourceType, kerr.InvalidRequest.Code)
				continue outer
			}
			r := doner(rr.ResourceName, rr.ResourceType, 0)
			c.brokerConfigs(int32(id), rfn(r))
			// A set log.retention.ms or log.roll.ms lists its
			// hours config at the 168 default after it.
			for i := range r.Configs {
				rc := &r.Configs[i]
				if d := topicDefaultSynonyms[validBrokerConfigs[rc.Name]]; d.hours && rc.Value != nil {
					syn := kmsg.NewDescribeConfigsResponseResourceConfigConfigSynonym()
					syn.Name, syn.Value, syn.Source = d.k, kmsg.StringPtr("168"), kmsg.ConfigSourceDefaultConfig
					rc.ConfigSynonyms = append(rc.ConfigSynonyms, syn)
				}
			}
			filter(rr, r)

		case kmsg.ConfigResourceTypeTopic:
			if e := c.deny(creq, rr.ResourceName, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationDescribeConfigs, faultKey{topic: rr.ResourceName, resource: rr.ResourceName}); e != nil {
				doner(rr.ResourceName, rr.ResourceType, e.Code)
				continue
			}
			if !validTopicName(rr.ResourceName) {
				doner(rr.ResourceName, rr.ResourceType, kerr.InvalidTopicException.Code)
				continue
			}
			if _, ok := c.data.tps.gett(rr.ResourceName); !ok {
				doner(rr.ResourceName, rr.ResourceType, kerr.UnknownTopicOrPartition.Code)
				continue
			}
			r := doner(rr.ResourceName, rr.ResourceType, 0)
			c.data.configs(rr.ResourceName, rfn(r))
			for i := range r.Configs {
				r.Configs[i].ConfigSynonyms = c.topicSynonyms(&r.Configs[i])
			}
			filter(rr, r)

		case kmsg.ConfigResourceTypeClientMetrics:
			// A subscription is a cluster resource, like a broker:
			// DescribeConfigs on CLUSTER. A name with no
			// subscription answers every key at its default, as
			// Kafka does; only an empty name is an error.
			if e := c.denyCluster(creq, kmsg.ACLOperationDescribeConfigs, faultKey{resource: rr.ResourceName}); e != nil {
				doner(rr.ResourceName, rr.ResourceType, e.Code)
				continue
			}
			if rr.ResourceName == "" {
				doner(rr.ResourceName, rr.ResourceType, kerr.InvalidRequest.Code)
				continue
			}
			sub := c.clientMetrics[rr.ResourceName]
			r := doner(rr.ResourceName, rr.ResourceType, 0)
			emit := rfn(r)
			for _, k := range slices.Sorted(maps.Keys(validClientMetricsConfigs)) {
				if v, dynamic := sub[k]; dynamic {
					emit(k, v, kmsg.ConfigSourceClientMetricsConfig, false)
					continue
				}
				def := validClientMetricsConfigs[k]
				emit(k, &def, kmsg.ConfigSourceDefaultConfig, false)
				r.Configs[len(r.Configs)-1].ConfigSynonyms = nil // Kafka lists only a subscription's own value
			}
			filter(rr, r)

		case kmsg.ConfigResourceTypeGroupConfig:
			if e := c.deny(creq, rr.ResourceName, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationDescribeConfigs, faultKey{group: rr.ResourceName, resource: rr.ResourceName}); e != nil {
				doner(rr.ResourceName, rr.ResourceType, e.Code)
				continue
			}
			if rr.ResourceName == "" {
				doner(rr.ResourceName, rr.ResourceType, kerr.InvalidRequest.Code)
				continue
			}
			// An unset key falls back to its broker config, then
			// to its default. As in Kafka, the synonyms list the
			// group's value, then the broker's under the broker
			// config's name; a key with no broker config lists its
			// own default.
			r := doner(rr.ResourceName, rr.ResourceType, 0)
			emit := rfn(r)
			for _, k := range slices.Sorted(maps.Keys(validGroupConfigs)) {
				b := "group." + k
				def, ok := configDefaults[b]
				if !ok {
					b, def = k, groupConfigDefaults[k]
				}
				var syns []kmsg.DescribeConfigsResponseResourceConfigConfigSynonym
				add := func(name string, v *string, src kmsg.ConfigSource) {
					syn := kmsg.NewDescribeConfigsResponseResourceConfigConfigSynonym()
					syn.Name, syn.Value, syn.Source = name, v, src
					syns = append(syns, syn)
				}
				if v, ok := c.groupConfigs[rr.ResourceName][k]; ok {
					add(k, v, kmsg.ConfigSourceGroupConfig)
				}
				if v := c.bcfgs[b]; v != nil {
					add(b, v, kmsg.ConfigSourceDynamicBrokerConfig)
				}
				add(b, &def, kmsg.ConfigSourceDefaultConfig)
				emit(k, syns[0].Value, syns[0].Source, false)
				r.Configs[len(r.Configs)-1].ConfigSynonyms = syns
			}
			filter(rr, r)

		default:
			doner(rr.ResourceName, rr.ResourceType, kerr.InvalidRequest.Code)
		}
	}

	return resp, nil
}

// brokerConfigFaultKey is the fault key for a BROKER config resource. A name
// that is another node's ID is answered INVALID_REQUEST here, so the resource
// is misrouted.
func brokerConfigFaultKey(b *broker, name string) faultKey {
	id, err := strconv.Atoi(name)
	return faultKey{resource: name, misrouted: name != "" && err == nil && int32(id) != b.node}
}

// topicSynonyms returns a topic config's synonyms as Kafka lists them: the
// topic's value, then the broker config's, then the broker's static default.
func (c *Cluster) topicSynonyms(rc *kmsg.DescribeConfigsResponseResourceConfig) []kmsg.DescribeConfigsResponseResourceConfigConfigSynonym {
	var syns []kmsg.DescribeConfigsResponseResourceConfigConfigSynonym
	add := func(name string, v *string, src kmsg.ConfigSource) {
		syn := kmsg.NewDescribeConfigsResponseResourceConfigConfigSynonym()
		syn.Name, syn.Value, syn.Source = name, v, src
		syns = append(syns, syn)
	}
	if rc.Source == kmsg.ConfigSourceDynamicTopicConfig {
		add(rc.Name, rc.Value, rc.Source)
	}
	b := validTopicConfigs[rc.Name]
	bv := c.bcfgs[b]
	if b != "" && bv != nil {
		add(b, bv, kmsg.ConfigSourceDynamicBrokerConfig)
	}
	if d, ok := topicDefaultSynonyms[rc.Name]; ok && (!d.hours || bv != nil) {
		v := configDefaults[rc.Name]
		if d.hours {
			v = "168"
		}
		add(d.k, &v, kmsg.ConfigSourceDefaultConfig)
	}
	return syns
}
