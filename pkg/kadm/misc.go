package kadm

import (
	"context"
	"regexp"
	"runtime/debug"
	"sort"
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

var (
	reVersion     *regexp.Regexp
	reVersionOnce sync.Once
)

// Copied from kgo, but we use the kadm package version.
func softwareVersion() string {
	info, ok := debug.ReadBuildInfo()
	if ok {
		reVersionOnce.Do(func() { reVersion = regexp.MustCompile(`^[a-zA-Z0-9](?:[a-zA-Z0-9.-]*[a-zA-Z0-9])?$`) })
		for _, dep := range info.Deps {
			if dep.Path == "github.com/twmb/franz-go/pkg/kadm" {
				if reVersion.MatchString(dep.Version) {
					return dep.Version
				}
			}
		}
	}
	return "unknown"
}
