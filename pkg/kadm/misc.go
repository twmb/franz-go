package kadm

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

// FindCoordinatorResponse contains information for the coordinator for a group
// or transactional ID.
type FindCoordinatorResponse struct {
	Name       string // Name is the coordinator key this response is for.
	NodeID     int32  // NodeID is the node ID of the coordinator for this key.
	Host       string // Host is the host of the coordinator for this key.
	Port       int32  // Port is the port of the coordinator for this key.
	Err        error  // Err is any error encountered when requesting the coordinator.
	ErrMessage string // ErrMessage a potential extra message describing any error.
}

// FindCoordinatorResponses contains responses to finding coordinators for
// groups or transactions.
type FindCoordinatorResponses map[string]FindCoordinatorResponse

// AllFailed returns whether all responses are errored.
func (rs FindCoordinatorResponses) AllFailed() bool {
	var n int
	rs.EachError(func(FindCoordinatorResponse) { n++ })
	return n == len(rs)
}

// Sorted returns all coordinator responses sorted by name.
func (rs FindCoordinatorResponses) Sorted() []FindCoordinatorResponse {
	s := make([]FindCoordinatorResponse, 0, len(rs))
	for _, r := range rs {
		s = append(s, r)
	}
	sort.Slice(s, func(i, j int) bool { return s[i].Name < s[j].Name })
	return s
}

// EachError calls fn for every response that has a non-nil error.
func (rs FindCoordinatorResponses) EachError(fn func(FindCoordinatorResponse)) {
	for _, r := range rs {
		if r.Err != nil {
			fn(r)
		}
	}
}

// Each calls fn for every response.
func (rs FindCoordinatorResponses) Each(fn func(FindCoordinatorResponse)) {
	for _, r := range rs {
		fn(r)
	}
}

// Error iterates over all responses and returns the first error encountered,
// if any.
func (rs FindCoordinatorResponses) Error() error {
	for _, r := range rs {
		if r.Err != nil {
			return r.Err
		}
	}
	return nil
}

// Ok returns true if there are no errors. This is a shortcut for rs.Error() ==
// nil.
func (rs FindCoordinatorResponses) Ok() bool {
	return rs.Error() == nil
}

// FindGroupCoordinators returns the coordinator for all requested group names.
func (cl *Client) FindGroupCoordinators(ctx context.Context, groups ...string) FindCoordinatorResponses {
	return cl.findCoordinators(ctx, 0, groups...)
}

// FindTxnCoordinators returns the coordinator for all requested transactional
// IDs.
func (cl *Client) FindTxnCoordinators(ctx context.Context, txnIDs ...string) FindCoordinatorResponses {
	return cl.findCoordinators(ctx, 1, txnIDs...)
}

func (cl *Client) findCoordinators(ctx context.Context, kind int8, names ...string) FindCoordinatorResponses {
	resps := make(FindCoordinatorResponses)
	if len(names) == 0 {
		return resps
	}

	req := kmsg.NewPtrFindCoordinatorRequest()
	req.CoordinatorType = kind
	req.CoordinatorKeys = names

	keyErr := func(k string, err error) {
		resps[k] = FindCoordinatorResponse{
			Name: k,
			Err:  err,
		}
	}
	allKeysErr := func(req *kmsg.FindCoordinatorRequest, err error) {
		for _, k := range req.CoordinatorKeys {
			keyErr(k, err)
		}
	}

	shards := cl.cl.RequestSharded(ctx, req)
	for _, shard := range shards {
		req := shard.Req.(*kmsg.FindCoordinatorRequest)
		if shard.Err != nil {
			allKeysErr(req, shard.Err)
			continue
		}
		resp := shard.Resp.(*kmsg.FindCoordinatorResponse)
		if err := maybeAuthErr(resp.ErrorCode); err != nil {
			allKeysErr(req, err)
			continue
		}
		for _, c := range resp.Coordinators {
			if err := maybeAuthErr(c.ErrorCode); err != nil {
				keyErr(c.Key, err)
				continue
			}
			resps[c.Key] = FindCoordinatorResponse{
				Name:       c.Key,
				NodeID:     c.NodeID,
				Host:       c.Host,
				Port:       c.Port,
				Err:        kerr.ErrorForCode(c.ErrorCode),
				ErrMessage: unptrStr(c.ErrorMessage),
			}
		}
	}
	return resps
}

type minmax struct {
	min, max int16
}

// BrokerApiVersions contains the API versions for a single broker.
type BrokerApiVersions struct {
	NodeID int32 // NodeID is the node this API versions response is for.

	raw         *kmsg.ApiVersionsResponse
	keyVersions map[int16]minmax

	Err error // Err is non-nil if the API versions request failed.
}

// Raw returns the raw API versions response.
func (v *BrokerApiVersions) Raw() *kmsg.ApiVersionsResponse {
	return v.raw
}

// KeyVersions returns the broker's min and max version for an API key and
// whether this broker supports the request.
func (v *BrokerApiVersions) KeyVersions(key int16) (min, max int16, exists bool) {
	vs, exists := v.keyVersions[key]
	return vs.min, vs.max, exists
}

// KeyVersions returns the broker's min version for an API key and whether this
// broker supports the request.
func (v *BrokerApiVersions) KeyMinVersion(key int16) (min int16, exists bool) {
	min, _, exists = v.KeyVersions(key)
	return min, exists
}

// KeyVersions returns the broker's max version for an API key and whether this
// broker supports the request.
func (v *BrokerApiVersions) KeyMaxVersion(key int16) (max int16, exists bool) {
	_, max, exists = v.KeyVersions(key)
	return max, exists
}

// EachKeySorted calls fn for every API key in the broker response, from the
// smallest API key to the largest.
func (v *BrokerApiVersions) EachKeySorted(fn func(key, min, max int16)) {
	type kmm struct {
		k, min, max int16
	}
	kmms := make([]kmm, 0, len(v.keyVersions))
	for key, minmax := range v.keyVersions {
		kmms = append(kmms, kmm{key, minmax.min, minmax.max})
	}
	sort.Slice(kmms, func(i, j int) bool { return kmms[i].k < kmms[j].k })
	for _, kmm := range kmms {
		fn(kmm.k, kmm.min, kmm.max)
	}
}

// VersionGuess returns the best guess of Kafka that this broker is. This is a
// shorcut for:
//
//	kversion.FromApiVersionsResponse(v.Raw()).VersionGuess(opt...)
//
// Check the kversion.VersionGuess API docs for more details.
func (v *BrokerApiVersions) VersionGuess(opt ...kversion.VersionGuessOpt) string {
	return kversion.FromApiVersionsResponse(v.raw).VersionGuess(opt...)
}

// BrokerApiVersions contains API versions for all brokers that are reachable
// from a metadata response.
type BrokersApiVersions map[int32]BrokerApiVersions

// Sorted returns all broker responses sorted by node ID.
func (vs BrokersApiVersions) Sorted() []BrokerApiVersions {
	s := make([]BrokerApiVersions, 0, len(vs))
	for _, v := range vs {
		s = append(s, v)
	}
	sort.Slice(s, func(i, j int) bool { return s[i].NodeID < s[j].NodeID })
	return s
}

// Each calls fn for every broker response.
func (vs BrokersApiVersions) Each(fn func(BrokerApiVersions)) {
	for _, v := range vs {
		fn(v)
	}
}

// ApiVersions queries every broker in a metadata response for their API
// versions. This returns an error only if the metadata request fails.
func (cl *Client) ApiVersions(ctx context.Context) (BrokersApiVersions, error) {
	m, err := cl.BrokerMetadata(ctx)
	if err != nil {
		return nil, err
	}

	req := kmsg.NewPtrApiVersionsRequest()
	req.ClientSoftwareName = "kadm"
	req.ClientSoftwareVersion = softwareVersion()

	var mu sync.Mutex
	var wg sync.WaitGroup
	vs := make(BrokersApiVersions, len(m.Brokers))
	for _, n := range m.Brokers.NodeIDs() {
		n := n
		wg.Add(1)
		go func() {
			defer wg.Done()
			v := BrokerApiVersions{NodeID: n, keyVersions: make(map[int16]minmax)}
			v.raw, v.Err = req.RequestWith(ctx, cl.cl.Broker(int(n)))

			mu.Lock()
			defer mu.Unlock()
			defer func() { vs[n] = v }()
			if v.Err != nil {
				return
			}

			v.Err = kerr.ErrorForCode(v.raw.ErrorCode)
			for _, k := range v.raw.ApiKeys {
				v.keyVersions[k.ApiKey] = minmax{
					min: k.MinVersion,
					max: k.MaxVersion,
				}
			}
		}()
	}
	wg.Wait()

	return vs, nil
}

// ClientQuotaEntityComponent is a quota entity component.
type ClientQuotaEntityComponent struct {
	Type string  // Type is the entity type ("user", "client-id", "ip").
	Name *string // Name is the entity name, or null if the default.
}

// String returns key=value, or key=<default> if value is nil.
func (d ClientQuotaEntityComponent) String() string {
	if d.Name == nil {
		return d.Type + "=<default>"
	}
	return fmt.Sprintf("%s=%s", d.Type, *d.Name)
}

// ClientQuotaEntity contains the components that make up a single entity.
type ClientQuotaEntity []ClientQuotaEntityComponent

// String returns {key=value, key=value}, joining all entities with a ", " and
// wrapping in braces.
func (ds ClientQuotaEntity) String() string {
	var ss []string
	for _, d := range ds {
		ss = append(ss, d.String())
	}
	return "{" + strings.Join(ss, ", ") + "}"
}

// ClientQuotaValue is a quota name and value.
type ClientQuotaValue struct {
	Key   string  // Key is the quota configuration key.
	Value float64 // Value is the quota configuration value.
}

// String returns key=value.
func (d ClientQuotaValue) String() string {
	return fmt.Sprintf("%s=%f", d.Key, d.Value)
}

// ClientQuotaValues contains all client quota values.
type ClientQuotaValues []ClientQuotaValue

// QuotasMatchType specifies how to match a described client quota entity.
//
// 0 means to match the name exactly: user=foo will only match components of
// entity type "user" and entity name "foo".
//
// 1 means to match the default of the name: entity type "user" with a default
// match will return the default quotas for user entities.
//
// 2 means to match any name: entity type "user" with any matching will return
// both names and defaults.
type QuotasMatchType = kmsg.QuotasMatchType

// DescribeClientQuotaComponent is an input entity component to describing
// client quotas: we define the type of quota ("client-id", "user"), how to
// match, and the match name if needed.
type DescribeClientQuotaComponent struct {
	Type      string          // Type is the type of entity component to describe ("user", "client-id", "ip").
	MatchName *string         // MatchName is the name to match again; this is only needed when MatchType is 0 (exact).
	MatchType QuotasMatchType // MatchType is how to match an entity.
}

// DescribedClientQuota contains a described quota. A single quota is made up
// of multiple entities and multiple values, for example, "user=foo" is one
// component of the entity, and "client-id=bar" is another.
type DescribedClientQuota struct {
	Entity ClientQuotaEntity // Entity is the entity of this described client quota.
	Values ClientQuotaValues // Values contains the quota valies for this entity.
}

// DescribedClientQuota contains client quotas that were described.
type DescribedClientQuotas []DescribedClientQuota

// DescribeClientQuotas describes client quotas. If strict is true, the
// response includes only the requested components.
func (cl *Client) DescribeClientQuotas(ctx context.Context, strict bool, entityComponents []DescribeClientQuotaComponent) (DescribedClientQuotas, error) {
	req := kmsg.NewPtrDescribeClientQuotasRequest()
	req.Strict = strict
	for _, entity := range entityComponents {
		rc := kmsg.NewDescribeClientQuotasRequestComponent()
		rc.EntityType = entity.Type
		rc.Match = entity.MatchName
		rc.MatchType = entity.MatchType
		req.Components = append(req.Components, rc)
	}
	resp, err := req.RequestWith(ctx, cl.cl)
	if err != nil {
		return nil, err
	}
	if err := maybeAuthErr(resp.ErrorCode); err != nil {
		return nil, err
	}
	if err := kerr.ErrorForCode(resp.ErrorCode); err != nil {
		return nil, err
	}
	var qs DescribedClientQuotas
	for _, entry := range resp.Entries {
		var q DescribedClientQuota
		for _, e := range entry.Entity {
			q.Entity = append(q.Entity, ClientQuotaEntityComponent{
				Type: e.Type,
				Name: e.Name,
			})
		}
		for _, v := range entry.Values {
			q.Values = append(q.Values, ClientQuotaValue{
				Key:   v.Key,
				Value: v.Value,
			})
		}
		qs = append(qs, q)
	}
	return qs, nil
}

// AlterClientQuotaOp sets or remove a client quota.
type AlterClientQuotaOp struct {
	Key    string  // Key is the quota configuration key to set or remove.
	Value  float64 // Value is the quota configuration value to set or remove.
	Remove bool    // Remove, if true, removes this quota rather than sets it.
}

// AlterClientQuotaEntry pairs an entity with quotas to set or remove.
type AlterClientQuotaEntry struct {
	Entity ClientQuotaEntity    // Entity is the entity to alter quotas for.
	Ops    []AlterClientQuotaOp // Ops are quotas to set or remove.
}

// AlteredClientQuota is the result for a single entity that was altered.
type AlteredClientQuota struct {
	Entity     ClientQuotaEntity // Entity is the entity this result is for.
	Err        error             // Err is non-nil if the alter operation on this entity failed.
	ErrMessage string            // ErrMessage is an optional additional message on error.
}

// AlteredClientQuotas contains results for all altered entities.
type AlteredClientQuotas []AlteredClientQuota

// AlterClientQuotas alters quotas for the input entries. You may consider
// checking ValidateAlterClientQuotas before using this method.
func (cl *Client) AlterClientQuotas(ctx context.Context, entries []AlterClientQuotaEntry) (AlteredClientQuotas, error) {
	return cl.alterClientQuotas(ctx, false, entries)
}

// ValidateAlterClientQuotas validates an alter client quota request. This
// returns exactly what AlterClientQuotas returns, but does not actually alter
// quotas.
func (cl *Client) ValidateAlterClientQuotas(ctx context.Context, entries []AlterClientQuotaEntry) (AlteredClientQuotas, error) {
	return cl.alterClientQuotas(ctx, true, entries)
}

func (cl *Client) alterClientQuotas(ctx context.Context, validate bool, entries []AlterClientQuotaEntry) (AlteredClientQuotas, error) {
	req := kmsg.NewPtrAlterClientQuotasRequest()
	req.ValidateOnly = validate
	for _, entry := range entries {
		re := kmsg.NewAlterClientQuotasRequestEntry()
		for _, c := range entry.Entity {
			rec := kmsg.NewAlterClientQuotasRequestEntryEntity()
			rec.Type = c.Type
			rec.Name = c.Name
			re.Entity = append(re.Entity, rec)
		}
		for _, op := range entry.Ops {
			reo := kmsg.NewAlterClientQuotasRequestEntryOp()
			reo.Key = op.Key
			reo.Value = op.Value
			reo.Remove = op.Remove
			re.Ops = append(re.Ops, reo)
		}
		req.Entries = append(req.Entries, re)
	}
	resp, err := req.RequestWith(ctx, cl.cl)
	if err != nil {
		return nil, err
	}
	var as AlteredClientQuotas
	for _, entry := range resp.Entries {
		var e ClientQuotaEntity
		for _, c := range entry.Entity {
			e = append(e, ClientQuotaEntityComponent{
				Type: c.Type,
				Name: c.Name,
			})
		}
		a := AlteredClientQuota{
			Entity:     e,
			Err:        kerr.ErrorForCode(entry.ErrorCode),
			ErrMessage: unptrStr(entry.ErrorMessage),
		}
		as = append(as, a)
	}
	return as, nil
}
