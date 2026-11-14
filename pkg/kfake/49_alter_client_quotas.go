package kfake

import (
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// AlterClientQuotas: v0-1
//
// KIP-546: Client quota configuration APIs.
//
// Behavior:
// * ACL: ALTER_CONFIGS on CLUSTER
// * Stores quota configurations (not enforced by kfake)
// * Supports set and remove operations
// * Validates entities, keys, and values as Kafka does; an invalid entry
//   changes nothing
//
// Version notes:
// * v0: Initial version
// * v1: Flexible versions

func init() { regKey(49, 0, 1) }

func (c *Cluster) handleAlterClientQuotas(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.AlterClientQuotasRequest)
	resp := req.ResponseKind().(*kmsg.AlterClientQuotasResponse)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	clusterErr := c.denyCluster(creq, kmsg.ACLOperationAlterConfigs, faultKey{})
	if clusterErr != nil && creq.skipsWork(clusterErr) { // a timed-out alter still applies
		for _, entry := range req.Entries {
			re := kmsg.NewAlterClientQuotasResponseEntry()
			re.ErrorCode = clusterErr.Code
			re.ErrorMessage = kmsg.StringPtr(clusterErr.Message)
			for _, e := range entry.Entity {
				ee := kmsg.NewAlterClientQuotasResponseEntryEntity()
				ee.Type = e.Type
				ee.Name = e.Name
				re.Entity = append(re.Entity, ee)
			}
			resp.Entries = append(resp.Entries, re)
		}
		return resp, nil
	}

	firsts := make(map[string]int) // entity key => index of its answer
	for _, entry := range req.Entries {
		entry.Entity = lastOfEachType(entry.Entity)
		// Like Kafka, a second entry for an entity changes nothing and
		// fails the entity's one answer.
		key := makeQuotaEntity(entry.Entity).key()
		if i, ok := firsts[key]; ok {
			resp.Entries[i].ErrorCode = kerr.InvalidRequest.Code
			resp.Entries[i].ErrorMessage = kmsg.StringPtr("Ignoring duplicate entity")
			continue
		}
		firsts[key] = len(resp.Entries)

		re := kmsg.NewAlterClientQuotasResponseEntry()
		faultErr := clusterErr
		for _, e := range entry.Entity {
			ee := kmsg.NewAlterClientQuotasResponseEntryEntity()
			ee.Type = e.Type
			ee.Name = e.Name
			re.Entity = append(re.Entity, ee)
			if e.Name != nil && faultErr == nil {
				faultErr = creq.faults.check(faultKey{resource: *e.Name})
			}
		}
		if faultErr != nil {
			re.ErrorCode = faultErr.Code
			re.ErrorMessage = kmsg.StringPtr(faultErr.Message)
			if creq.skipsWork(faultErr) { // a timed-out alter still applies
				resp.Entries = append(resp.Entries, re)
				continue
			}
		}
		if msg := validateQuotaEntry(entry); msg != "" {
			if faultErr == nil {
				re.ErrorCode = kerr.InvalidRequest.Code
				re.ErrorMessage = kmsg.StringPtr(msg)
			}
			resp.Entries = append(resp.Entries, re)
			continue
		}

		if !req.ValidateOnly {
			// Build quota key and entity
			qe := makeQuotaEntity(entry.Entity)
			key := qe.key()

			// Get or create quota entry
			quota, exists := c.quotas[key]
			if !exists {
				quota = quotaEntry{
					entity: qe,
					values: make(map[string]float64),
				}
			}

			// Apply operations
			for _, op := range entry.Ops {
				if op.Remove {
					delete(quota.values, op.Key)
				} else {
					quota.values[op.Key] = op.Value
				}
			}

			// Store or delete quota entry
			if len(quota.values) == 0 {
				delete(c.quotas, key)
			} else {
				c.quotas[key] = quota
			}
		}

		resp.Entries = append(resp.Entries, re)
	}

	if !req.ValidateOnly {
		c.persistQuotasState()
	}
	return resp, nil
}

// userClientQuotaKeyMax and ipQuotaKeyMax hold the keys Kafka accepts for
// an entity with a user or client-id, and for an ip entity. A key with a
// max takes only integral values up to it; a key with max 0 takes any
// positive value.
var (
	userClientQuotaKeyMax = map[string]float64{
		"producer_byte_rate":       math.MaxInt64,
		"consumer_byte_rate":       math.MaxInt64,
		"request_percentage":       0,
		"controller_mutation_rate": 0,
	}
	ipQuotaKeyMax = map[string]float64{
		"connection_creation_rate": math.MaxInt32,
	}
)

// validateQuotaEntry returns Kafka's INVALID_REQUEST message for an entry it
// does not apply, or "" if the entry is valid. A remove skips the value check.
func validateQuotaEntry(entry kmsg.AlterClientQuotasRequestEntry) string {
	if len(entry.Entity) == 0 {
		return "Invalid empty client quota entity"
	}
	var hasIP, hasUserOrClient bool
	for _, e := range entry.Entity {
		switch e.Type {
		case "user", "client-id":
			hasUserOrClient = true
		case "ip":
			hasIP = true
		default:
			return "Unhandled client quota entity type: " + e.Type
		}
		if e.Name != nil && *e.Name == "" {
			return "Empty " + e.Type + " not supported"
		}
	}
	keys := userClientQuotaKeyMax
	if hasIP {
		if hasUserOrClient {
			return "Invalid quota entity combination, IP entity should not be combined with User or ClientId"
		}
		keys = ipQuotaKeyMax
	}
	seen := make(map[string]bool, len(entry.Ops))
	for _, op := range entry.Ops {
		if seen[op.Key] {
			return "Duplicate quota key " + op.Key
		}
		seen[op.Key] = true
		if op.Remove {
			continue
		}
		maxv, ok := keys[op.Key]
		v := op.Value
		switch {
		case !ok:
			return "Invalid configuration key " + op.Key
		case math.IsNaN(v) || math.IsInf(v, 0):
			return "Quota " + op.Key + " must be a finite number"
		case v <= 0:
			return "Quota " + op.Key + " must be greater than 0"
		case maxv > 0 && v > maxv:
			return "Proposed value for " + op.Key + " is too large"
		case maxv > 0 && math.Abs(math.Mod(v, 1)) > 1e-6:
			return op.Key + " cannot be a fractional value."
		}
	}
	return ""
}

// lastOfEachType returns the last component of each entity type in es, as
// Kafka reads an entity into a map.
func lastOfEachType(es []kmsg.AlterClientQuotasRequestEntryEntity) []kmsg.AlterClientQuotasRequestEntryEntity {
	last := make(map[string]int, len(es))
	for i, e := range es {
		last[e.Type] = i
	}
	var keep []kmsg.AlterClientQuotasRequestEntryEntity
	for i, e := range es {
		if last[e.Type] == i {
			keep = append(keep, e)
		}
	}
	return keep
}

// quotaEntityComponent represents a single entity component (type + name).
type quotaEntityComponent struct {
	entityType string
	name       *string
}

// quotaEntity is an ordered list of entity components.
type quotaEntity []quotaEntityComponent

// quotaEntry stores quota values for an entity.
type quotaEntry struct {
	entity quotaEntity
	values map[string]float64
}

func makeQuotaEntity(entities []kmsg.AlterClientQuotasRequestEntryEntity) quotaEntity {
	qe := make(quotaEntity, len(entities))
	for i, e := range entities {
		qe[i] = quotaEntityComponent{
			entityType: e.Type,
			name:       e.Name,
		}
	}
	// Sort by entity type for consistent key generation
	sort.Slice(qe, func(i, j int) bool {
		return qe[i].entityType < qe[j].entityType
	})
	return qe
}

// key generates a unique string key for the quota entity. Each name is quoted,
// so that no name spells the default or another entity.
func (qe quotaEntity) key() string {
	var parts []string
	for _, e := range qe {
		name := "default"
		if e.name != nil {
			name = strconv.Quote(*e.name)
		}
		parts = append(parts, e.entityType+"="+name)
	}
	return strings.Join(parts, ",")
}
