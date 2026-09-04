package kfake

import (
	"slices"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kerr"
)

// Fault answers matching partitions of matching requests with Err instead of
// the real response; the rest of each request is answered normally.
//
// A request matches if its key is in Keys (nil: any supported request kind),
// it arrived at Node (-1: any node), and it addresses a partition matching
// Topic or TopicID and Partition (-1: any partition of that topic). Match by
// TopicID to fault a stale ID a client still uses after a recreation.
//
// Note that the zero values of Node and Partition are node 0 and partition 0,
// not "any": kfake uses raw IDs everywhere, so you must ask for "any" with -1.
//
// Set exactly one of Topic or TopicID. If both are set, the ID wins; if
// neither is set, nothing matches and Fired stays 0. A TopicID only matches
// requests that address a topic by ID: Fetch v13+, Produce v13+, ShareFetch,
// and ShareAcknowledge.
//
// We fault Produce, Fetch, ListOffsets, OffsetForLeaderEpoch, ShareFetch, and
// ShareAcknowledge; all other requests are answered normally. A nil Err is
// UNKNOWN_SERVER_ERROR.
//
// Count is a budget of requests: each matching request consumes one, and every
// matching partition in it is failed. Zero means until removed.
type Fault struct {
	Keys      []int16
	Node      int32
	Topic     string
	TopicID   [16]byte
	Partition int32
	Err       *kerr.Error
	Count     int
}

// FaultHandle is a handle to an installed fault.
type FaultHandle struct {
	c *Cluster
	f *fault
}

type fault struct {
	keys      []int16
	node      int32
	topic     string
	topicID   uuid
	partition int32
	err       *kerr.Error
	fired     atomic.Int64

	// left is the requests remaining, -1 for unlimited, and removed is
	// set once we drop the fault. Both are guarded by c.faultsMu.
	left    int
	removed bool
}

// AddFault installs f and returns its handle. We fault inside handlers, so a
// ControlKey control that answers a request itself bypasses faults, while one
// that passes through sees them.
func (c *Cluster) AddFault(f Fault) *FaultHandle {
	ff := &fault{
		keys:      slices.Clone(f.Keys),
		node:      f.Node,
		topic:     f.Topic,
		topicID:   f.TopicID,
		partition: f.Partition,
		err:       f.Err,
		left:      f.Count,
	}
	if ff.err == nil {
		ff.err = kerr.UnknownServerError
	}
	if ff.left <= 0 {
		ff.left = -1
	}
	c.faultsMu.Lock()
	defer c.faultsMu.Unlock()
	c.faults = append(c.faults, ff)
	return &FaultHandle{c: c, f: ff}
}

// Fired returns how many requests the fault has answered.
func (h *FaultHandle) Fired() int { return int(h.f.fired.Load()) }

// Remove uninstalls the fault. This is safe to call twice, and safe after the
// fault exhausted its Count and removed itself.
func (h *FaultHandle) Remove() {
	h.c.faultsMu.Lock()
	defer h.c.faultsMu.Unlock()
	h.c.rmFaultLocked(h.f)
}

func (c *Cluster) rmFaultLocked(f *fault) {
	if f.removed {
		return
	}
	f.removed = true
	c.faults = slices.DeleteFunc(c.faults, func(o *fault) bool { return o == f })
}

// faultCheck is the faults that can match one request. A request consumes at
// most one unit from each fault it hits, no matter how many of its partitions
// match, so we remember what we charged.
type faultCheck struct {
	c       *Cluster
	fs      []*fault
	charged []*fault
}

// faultsFor returns the faults to consider for creq, or nil if none can match.
func (c *Cluster) faultsFor(creq *clientReq) *faultCheck {
	c.faultsMu.Lock()
	defer c.faultsMu.Unlock()
	if len(c.faults) == 0 {
		return nil
	}
	key := creq.kreq.Key()
	var fs []*fault
	for _, f := range c.faults {
		if f.node != -1 && f.node != creq.cc.b.node {
			continue
		}
		if len(f.keys) > 0 && !slices.Contains(f.keys, key) {
			continue
		}
		fs = append(fs, f)
	}
	if len(fs) == 0 {
		return nil
	}
	return &faultCheck{c: c, fs: fs}
}

// err returns the error to answer a partition with, or nil to answer it for
// real. The id is the topic ID the request addressed the topic by, if any.
func (fc *faultCheck) err(t string, id uuid, p int32) *kerr.Error {
	if fc == nil {
		return nil
	}
	for _, f := range fc.fs {
		if f.partition != -1 && f.partition != p {
			continue
		}
		if f.topicID != noID {
			if f.topicID != id {
				continue
			}
		} else if f.topic == "" || f.topic != t {
			continue
		}
		if !fc.charge(f) {
			continue
		}
		return f.err
	}
	return nil
}

// fired reports whether anything in this request was faulted.
func (fc *faultCheck) fired() bool { return fc != nil && len(fc.charged) > 0 }

// charge takes a unit of f's budget for this request, if we have not taken one
// already. A fault with nothing left removes itself.
func (fc *faultCheck) charge(f *fault) bool {
	if slices.Contains(fc.charged, f) {
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
	f.fired.Add(1)
	fc.charged = append(fc.charged, f)
	return true
}
