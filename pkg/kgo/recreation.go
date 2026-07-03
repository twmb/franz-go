package kgo

import (
	"maps"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// recreationGate arms client-wide handling of topic recreation (a topic
// deleted and recreated with the same name, yielding a new topic ID).
//
// When armed, the metadata merge is allowed to adopt a recreated topic's new
// ID and reset consumption per the configured policy. Arming requires every
// broker we have negotiated ApiVersions with to support fetch v13, which
// puts topic IDs on the fetch wire: a stale-ID fetch of a recreated topic
// fails with UNKNOWN_TOPIC_ID and can never silently read records from the
// new incarnation. Below v13, fetches go by name and cannot distinguish
// incarnations, so adopting a new ID while holding a live position risks
// silently misreading the new incarnation; the gate stays disarmed and
// recreation behavior is unchanged (fetches stall loudly).
//
// The gate is re-evaluated on every metadata update: a broker negotiating
// below fetch v13 (e.g. mid rolling upgrade) disarms it, and it re-arms when
// that broker leaves the cluster or renegotiates at v13+. Brokers we have
// never connected to do not count against the gate: versions negotiate on
// first connect, before any fetch to that broker can be sent.
type recreationGate struct {
	armed atomic.Bool
}

// cleanStaleID2T drops id2t entries of prior topic incarnations once nothing
// references them. When the recreation gate is armed, the metadata merge adds
// a recreated topic's new ID alongside the old entry: the old one must
// survive while any cursor or recBuf still carries the old ID (their
// in-flight and retried requests are still keyed by it), and becomes garbage
// once every holder has swapped or been purged.
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
// the given topic ID on its recBuf, cursor, or shareCursor. topicID fields
// are written only at partition creation or by the metadata merge itself
// (single goroutine), so reading them here is race-free.
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

// evalRecreationGate re-evaluates the recreation gate against the current
// broker list and negotiated versions, logging arm and disarm transitions.
// This is called once per metadata update, after the update refreshes the
// broker list and before topic merges consult the gate.
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
