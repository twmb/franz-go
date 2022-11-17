package kadm

import (
	"context"
	"sort"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
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
