package kfake

import (
	"context"
	"reflect"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// We check faults wherever we check authorization, and per partition in
// every request that carries partitions.

// Fault fails matching requests with Err in place of the real answer, with
// every field being an AND filter of what the fault should apply to. Faults
// are an easier way to inject targeted failures than Control.
//
// A selector that a request does not carry never matches: a fault with a
// Topic does not fail a heartbeat. Requests with a top-level ErrorCode can
// have that error code set by setting TopLevel to true (some requests set
// either a top-level code OR a per-resource code).
//
// A faulted entity is rejected before the broker acts on it, except with
// REQUEST_TIMED_OUT, on the requests a real broker can answer that way after
// applying them, or NOT_ENOUGH_REPLICAS_AFTER_APPEND on produce: those are
// answered and the work still happens. Faults are checked in the order added
// and the first match answers; a ControlKey control that answers a request
// bypasses them.
type Fault struct {
	Keys  []kmsg.Key // which requests this should apply to; nil means all
	Nodes []int32    // which nodes this should apply to; nil means all

	Topic      string   // "" means all
	TopicID    [16]byte // zero means all
	Partitions []int32  // which partitions this should apply to; nil means all
	Group      string   // "" means all; a FindCoordinator group key matches this
	TxnID      string   // "" means all; a FindCoordinator transaction key matches this
	TopLevel   bool     // fail the request's top-level ErrorCode rather than its entities, ignored for requests without one

	// Resource names an entity no selector above reaches: a config
	// resource, a client quota entity, a SCRAM user, a log dir, a feature,
	// a member in a LeaveGroup, or the resource name of an ACL creation or
	// filter. "" means all.
	Resource string

	Err   *kerr.Error // nil defaults to UNKNOWN_SERVER_ERROR
	Count int         // requests to fault; 0 means one, -1 means until Remove is called
}

// FaultHandle refers to the faults installed by one Fault call.
type FaultHandle struct {
	c  *Cluster
	fs []*fault
}

type fault struct {
	keys       []kmsg.Key
	nodes      []int32
	topic      string
	topicID    uuid
	partitions []int32
	group      string
	txnID      string
	resource   string
	topLevel   bool
	err        *kerr.Error
	hits       atomic.Int64

	// left is the requests remaining, -1 for unlimited, and removed is
	// set once we drop the fault. Both are guarded by c.faultsMu.
	left    int
	removed bool
}

// Fault installs faults and returns a handle to them. Fault may be called
// from a control function.
func (c *Cluster) Fault(faults ...Fault) *FaultHandle {
	h := &FaultHandle{c: c}
	c.faultsMu.Lock()
	defer c.faultsMu.Unlock()
	if c.faultCond == nil {
		c.faultCond = sync.NewCond(&c.faultsMu)
	}
	for _, in := range faults {
		f := &fault{
			keys:       slices.Clone(in.Keys),
			nodes:      slices.Clone(in.Nodes),
			topic:      in.Topic,
			topicID:    in.TopicID,
			partitions: slices.Clone(in.Partitions),
			group:      in.Group,
			txnID:      in.TxnID,
			resource:   in.Resource,
			topLevel:   in.TopLevel,
			err:        in.Err,
			left:       in.Count,
		}
		if f.err == nil {
			f.err = kerr.UnknownServerError
		}
		switch {
		case in.Count == 0:
			f.left = 1
		case in.Count < 0:
			f.left = -1
		}
		h.fs = append(h.fs, f)
		c.faults = append(c.faults, f)
	}
	return h
}

// Wait blocks until the faults have answered at least n requests in total,
// or until ctx is done, returning ctx's error.
func (h *FaultHandle) Wait(ctx context.Context, n int) error {
	quit := false
	done := make(chan struct{})
	go func() {
		h.c.faultsMu.Lock()
		defer h.c.faultsMu.Unlock()
		defer close(done)

		for !quit && h.Hits() < n {
			h.c.faultCond.Wait()
		}
	}()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		h.c.faultsMu.Lock()
		quit = true
		h.c.faultsMu.Unlock()
		h.c.faultCond.Broadcast()
		return ctx.Err()
	}
}

// Hits is the count of requests the faults have answered so far.
func (h *FaultHandle) Hits() int {
	var n int64
	for _, f := range h.fs {
		n += f.hits.Load()
	}
	return int(n)
}

// Remove uninstalls faults associated with this handle.
func (h *FaultHandle) Remove() {
	h.c.faultsMu.Lock()
	defer h.c.faultsMu.Unlock()
	for _, f := range h.fs {
		h.c.rmFaultLocked(f)
	}
}

func (c *Cluster) rmFaultLocked(f *fault) {
	if f.removed {
		return
	}
	f.removed = true
	c.faults = slices.DeleteFunc(c.faults, func(o *fault) bool { return o == f })
}

// faultKey is what a site knows about the entity it is answering for. A fault
// that selects on something the key does not name never matches there.
type faultKey struct {
	topic     string
	topicID   uuid
	partition int32
	hasPart   bool
	group     string
	txnID     string
	resource  string
}

// part returns k naming a partition.
func (k faultKey) part(p int32) faultKey {
	k.partition, k.hasPart = p, true
	return k
}

func (f *fault) matches(k faultKey) bool {
	if f.topic != "" && f.topic != k.topic {
		return false
	}
	if f.topicID != noID && f.topicID != k.topicID {
		return false
	}
	if len(f.partitions) > 0 && (!k.hasPart || !slices.Contains(f.partitions, k.partition)) {
		return false
	}
	if f.group != "" && f.group != k.group {
		return false
	}
	if f.txnID != "" && f.txnID != k.txnID {
		return false
	}
	if f.resource != "" && f.resource != k.resource {
		return false
	}
	return true
}

// faultCheck is the faults that can match one request; hits records the ones
// this request has already hit.
type faultCheck struct {
	c    *Cluster
	key  int16
	fs   []*fault
	hits []*fault
}

// faultsFor returns the faults to consider for creq, or nil if none can match.
func (c *Cluster) faultsFor(creq *clientReq) *faultCheck {
	c.faultsMu.Lock()
	defer c.faultsMu.Unlock()
	if len(c.faults) == 0 {
		return nil
	}
	key := creq.kreq.Key()
	kkey := kmsg.Key(key)
	var fs []*fault
	for _, f := range c.faults {
		if len(f.nodes) > 0 && !slices.Contains(f.nodes, creq.cc.b.node) {
			continue
		}
		if len(f.keys) > 0 && !slices.Contains(f.keys, kkey) {
			continue
		}
		fs = append(fs, f)
	}
	if len(fs) == 0 {
		return nil
	}
	return &faultCheck{c: c, key: key, fs: fs}
}

// check returns the error to answer the entity named by k with, if a fault
// matches it.
func (fc *faultCheck) check(k faultKey) *kerr.Error {
	if fc == nil {
		return nil
	}
	for _, f := range fc.fs {
		// A TopLevel fault answers only in topLevel.
		if f.topLevel || !f.matches(k) {
			continue
		}
		if !fc.hit(f) {
			continue
		}
		return f.err
	}
	return nil
}

// afterApply is the requests a broker can answer REQUEST_TIMED_OUT after
// applying them, and the codes it answers with.
var afterApply = map[int16][]*kerr.Error{
	int16(kmsg.Produce):                   {kerr.RequestTimedOut, kerr.NotEnoughReplicasAfterAppend},
	int16(kmsg.WriteTxnMarkers):           {kerr.RequestTimedOut, kerr.NotEnoughReplicasAfterAppend},
	int16(kmsg.DeleteRecords):             {kerr.RequestTimedOut},
	int16(kmsg.OffsetCommit):              {kerr.RequestTimedOut},
	int16(kmsg.TxnOffsetCommit):           {kerr.RequestTimedOut},
	int16(kmsg.DeleteGroups):              {kerr.RequestTimedOut},
	int16(kmsg.AlterShareGroupOffsets):    {kerr.RequestTimedOut},
	int16(kmsg.DeleteShareGroupOffsets):   {kerr.RequestTimedOut},
	int16(kmsg.CreateTopics):              {kerr.RequestTimedOut},
	int16(kmsg.DeleteTopics):              {kerr.RequestTimedOut},
	int16(kmsg.CreatePartitions):          {kerr.RequestTimedOut},
	int16(kmsg.ElectLeaders):              {kerr.RequestTimedOut},
	int16(kmsg.AlterPartitionAssignments): {kerr.RequestTimedOut},
	int16(kmsg.AlterConfigs):              {kerr.RequestTimedOut},
	int16(kmsg.IncrementalAlterConfigs):   {kerr.RequestTimedOut},
	int16(kmsg.CreateACLs):                {kerr.RequestTimedOut},
	int16(kmsg.DeleteACLs):                {kerr.RequestTimedOut},
	int16(kmsg.AlterClientQuotas):         {kerr.RequestTimedOut},
	int16(kmsg.AlterUserSCRAMCredentials): {kerr.RequestTimedOut},
	int16(kmsg.UpdateFeatures):            {kerr.RequestTimedOut},
}

// skipsWork reports whether answering e skips the entity's work; the
// afterApply codes do not.
func (creq *clientReq) skipsWork(e *kerr.Error) bool {
	return !slices.Contains(afterApply[creq.kreq.Key()], e)
}

// entityless is the requests that carry nothing a fault can select on.
var entityless = map[int16]bool{
	int16(kmsg.ApiVersions):               true,
	int16(kmsg.SASLHandshake):             true,
	int16(kmsg.SASLAuthenticate):          true,
	int16(kmsg.GetTelemetrySubscriptions): true,
	int16(kmsg.PushTelemetry):             true,
	int16(kmsg.DescribeCluster):           true,
	int16(kmsg.ListGroups):                true,
	int16(kmsg.ListTransactions):          true,
}

// topLevel answers a request before its handler runs: a TopLevel fault sets
// the top-level error code of any request that has one, and on a request with
// nothing to select on (entityless) any fault with no entity selectors does
// the same.
func (fc *faultCheck) topLevel(kreq kmsg.Request) kmsg.Response {
	if fc == nil {
		return nil
	}
	for _, f := range fc.fs {
		answers := f.topLevel || entityless[fc.key] && f.matches(faultKey{})
		if !answers {
			continue
		}
		resp := kreq.ResponseKind()
		code, ok := topLevelCode(resp)
		if !ok {
			continue // this request has no top-level code
		}
		if !fc.hit(f) {
			continue
		}
		code.SetInt(int64(f.err.Code))
		return resp
	}
	return nil
}

// topLevelCode returns the response's top-level ErrorCode field, if it has
// one.
func topLevelCode(kresp kmsg.Response) (reflect.Value, bool) {
	v := reflect.ValueOf(kresp)
	if v.Kind() != reflect.Pointer || v.IsNil() {
		return reflect.Value{}, false
	}
	code := v.Elem().FieldByName("ErrorCode")
	if !code.IsValid() || code.Kind() != reflect.Int16 {
		return reflect.Value{}, false
	}
	return code, true
}

// hit takes a unit of f's budget for this request. A request with several
// matching entities hits the fault once: the first entity takes the unit,
// later ones are still answered. A fault with nothing left removes itself.
func (fc *faultCheck) hit(f *fault) bool {
	if slices.Contains(fc.hits, f) {
		return true
	}
	fc.c.faultsMu.Lock()
	defer fc.c.faultsMu.Unlock()
	if f.removed || f.left == 0 {
		return false
	}
	if f.left > 0 {
		f.left--
		if f.left == 0 {
			fc.c.rmFaultLocked(f)
		}
	}
	f.hits.Add(1)
	fc.hits = append(fc.hits, f)
	fc.c.faultCond.Broadcast()
	return true
}

// anyHit reports whether anything in this request was faulted.
func (fc *faultCheck) anyHit() bool { return fc != nil && len(fc.hits) > 0 }

// deny answers the error to fail the entity named by k with, either because
// the user is not allowed the operation or because a fault matches.
func (c *Cluster) deny(creq *clientReq, resource string, rt kmsg.ACLResourceType, op kmsg.ACLOperation, k faultKey) *kerr.Error {
	if !c.allowedACL(creq, resource, rt, op) {
		return aclDenied(rt)
	}
	return creq.faults.check(k)
}

// denyCluster is deny for an operation on the cluster itself.
func (c *Cluster) denyCluster(creq *clientReq, op kmsg.ACLOperation) *kerr.Error {
	if !c.allowedClusterACL(creq, op) {
		return kerr.ClusterAuthorizationFailed
	}
	return creq.faults.check(faultKey{})
}

func aclDenied(rt kmsg.ACLResourceType) *kerr.Error {
	switch rt {
	case kmsg.ACLResourceTypeTopic:
		return kerr.TopicAuthorizationFailed
	case kmsg.ACLResourceTypeGroup:
		return kerr.GroupAuthorizationFailed
	case kmsg.ACLResourceTypeTransactionalId:
		return kerr.TransactionalIDAuthorizationFailed
	default:
		return kerr.ClusterAuthorizationFailed
	}
}
