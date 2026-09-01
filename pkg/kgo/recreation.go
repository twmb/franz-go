package kgo

import (
	"maps"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// recreationGate arms the strongest tier of topic recreation handling: a
// topic deleted and recreated under the same name, yielding a new topic ID.
//
// Armed, the metadata merge adopts a recreated topic's new ID on a single
// wire corroboration. Arming requires every broker we have negotiated with
// to support fetch v13, which puts topic IDs on the fetch wire: a stale-ID
// fetch then fails with UNKNOWN_TOPIC_ID rather than silently reading the
// new incarnation. Below v13 fetches go by name and cannot corroborate, so
// nothing swaps a cursor there. Share sessions are ID-addressed at every
// version and swap regardless of the gate.
//
// We re-evaluate on every metadata update. A broker negotiating below fetch
// v13 (a rolling upgrade) disarms us; we re-arm when it leaves or
// renegotiates. Brokers we have never connected to do not count: versions
// negotiate on first connect, before any fetch can be sent to them.
type recreationGate struct {
	armed atomic.Bool
}

// cleanStaleID2T drops id2t entries of prior topic incarnations once nothing
// references them. The metadata merge adds a recreated topic's new ID
// alongside the old entry: the old must survive while any cursor or recBuf
// still carries it, because their in-flight and retried requests are keyed
// by it.
func (cl *Client) cleanStaleID2T(latest map[string]*metadataTopic, tpsProducer, tpsConsumer topicsPartitionsData) {
	m := cl.id2tMap()
	var stale [][16]byte
	for id, name := range m {
		mt, ok := latest[name]
		if !ok || mt.id == id || mt.id == ([16]byte{}) {
			continue // name not in this response, entry current, or response ID-less: not provably stale
		}
		if topicIDReferenced(tpsProducer, name, id) || topicIDReferenced(tpsConsumer, name, id) {
			continue
		}
		stale = append(stale, id)
	}
	if len(stale) == 0 {
		return
	}
	merged := make(map[[16]byte]string, len(m))
	maps.Copy(merged, m)
	for _, id := range stale {
		delete(merged, id)
	}
	cl.id2t.Store(merged)
}

// topicIDReferenced returns whether any partition of the topic still carries
// the given topic ID. topicID fields are written only at partition creation
// or by the metadata merge, and we run on that same goroutine, so we can
// read them without locks.
func topicIDReferenced(tps topicsPartitionsData, name string, id [16]byte) bool {
	td := tps.loadTopic(name)
	if td == nil {
		return false
	}
	for _, tp := range td.partitions {
		switch {
		case tp.records != nil && tp.records.topicID == id:
			return true
		case tp.cursor != nil && tp.cursor.topicID == id:
			return true
		case tp.shareCursor != nil && tp.shareCursor.topicID == id:
			return true
		}
	}
	return false
}

// evalRecreationGate re-evaluates the gate against the current broker list
// and negotiated versions. This is called once per metadata update, after
// the update refreshes the broker list and before any topic merge consults
// the gate.
func (cl *Client) evalRecreationGate() {
	// A user MaxVersions cap below fetch v13 pins fetches to by-name
	// requests regardless of broker support; the gate can never arm.
	if mv := cl.cfg.maxVersions; mv != nil {
		if v, ok := mv.LookupMaxKeyVersion(int16(kmsg.Fetch)); !ok || v < 13 {
			cl.recreation.armed.Store(false)
			return
		}
	}

	var (
		armed     = true
		seen      bool
		disarmID  int32
		disarmMax int16
	)
	cl.brokersMu.RLock()
	for _, brokers := range [][]*broker{
		cl.brokers,
		cl.loadSeeds(),
	} {
		for _, b := range brokers {
			v := b.loadVersions()
			if v == nil {
				continue
			}
			seen = true
			if max := v.maxVersion(int16(kmsg.Fetch)); max < 13 {
				armed = false
				disarmID, disarmMax = b.meta.NodeID, max
			}
		}
	}
	cl.brokersMu.RUnlock()
	armed = armed && seen

	if was := cl.recreation.armed.Swap(armed); was == armed {
		return
	}
	if armed {
		cl.cfg.logger.Log(LogLevelInfo, "topic recreation handling armed; all connected brokers support fetch v13 (topic IDs on the fetch wire)")
	} else {
		cl.cfg.logger.Log(LogLevelInfo, "topic recreation handling disarmed; a broker below fetch v13 appeared and by-name fetches cannot distinguish topic incarnations",
			"broker", logID(disarmID),
			"max_fetch_version", disarmMax,
		)
	}
}
