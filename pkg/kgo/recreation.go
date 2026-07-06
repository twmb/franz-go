package kgo

import (
	"errors"
	"fmt"
	"maps"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// recreationStableIDAge is how long a topic ID must have been our
// consistently-held truth before a metadata response reporting a DIFFERENT
// ID is believed outright, with no further corroboration: metadata
// staleness is a seconds-scale phenomenon (propagation skew, one behind
// broker), so a change against a minute-old ID is a recreation, not a stale
// broker resurfacing a prior view. Below this age the change could still be
// propagation skew from our own recent adoption, and the corroboration
// rules apply (a broker rejection, or two consecutive metadata responses
// agreeing). A var only so tests can shorten it.
var recreationStableIDAge = time.Minute

// idStableLongEnough reports whether an ID adopted at the given time has
// been held long enough that a change to it is trusted outright.
func idStableLongEnough(agreedAt time.Time) bool {
	return !agreedAt.IsZero() && time.Since(agreedAt) >= recreationStableIDAge
}

// previouslyHeld reports whether id is one this partition already held.
// Topic IDs are random and never reused, so a change BACK to a prior ID is
// never a fresh recreation: it is stale metadata or split brain, and only
// wire evidence (a broker rejecting the id we currently hold) may adopt it.
// Both trust shortcuts - an aged ID and two consecutive metadata updates -
// are stale-servable and yield to this check.
func previouslyHeld(prior *[2][16]byte, id [16]byte) bool {
	return prior[0] == id || prior[1] == id
}

// holdPriorID records id as previously held, ahead of adopting a new one.
func holdPriorID(prior *[2][16]byte, id [16]byte) {
	prior[1], prior[0] = prior[0], id
}

// swapRecreatedConsumer is the consumer side of adopting a recreated topic:
// reposition per reset (the new topic's beginning when the recreation is
// certain; the nearest-timestamp loss reset when it is inferred and loss
// remains a hypothesis), or, under NoResetOffset, freeze the partition with
// a surfaced error (SetOffsets resumes). The why clause names the evidence
// in the surfaced error; kvs extend the swap log line.
func (cl *Client) swapRecreatedConsumer(topic string, part int, oldTP, newTP *topicPartition, css *consumerSessionStopper, reset Offset, why string, lvl LogLevel, msg string, kvs ...any) {
	rp := &reset
	if cl.cfg.resetOffset.noReset {
		rp = nil
		cl.consumer.addFakeReadyForDraining(topic, int32(part),
			fmt.Errorf("%s (automatic resets are disabled via NoResetOffset; resume via SetOffsets): %w", why, kerr.UnknownTopicID),
			"metadata refresh sees topic recreation with resets disabled")
	}
	cl.cfg.logger.Log(lvl, msg, append([]any{"topic", topic, "partition", part}, kvs...)...)
	oldTP.swapRecreatedCursorTo(newTP, css, rp)
}

// resolveDeferredOOR resolves an out-of-range reset the fetch path deferred
// for one classification round (cursor.oorPending); reaching here means the
// metadata merge corroborated no recreation. When probing is allowed and
// the log SHRANK below a consumed epoch, one OffsetForLeaderEpoch probe
// classifies the reset as honestly as the wire allows: no history of our
// epoch is almost certainly a recreation, our epoch ending below our
// position is substantial truncation or a recreation, and group commits
// fence + reseed either way. Otherwise this is the plain reset the fetch
// would have done without the deferral (probing is skipped when the topic
// vanished from metadata: the load simply retries against the missing
// topic, exactly as an undeferred reset would).
func (cl *Client) resolveDeferredOOR(css *consumerSessionStopper, c *cursor, topic string, part int32, probe bool) {
	shape := c.oorPending.Swap(oorNone)
	if shape == oorNone {
		return
	}
	css.stop()
	// With the session stopped, cursor fields are safely readable and
	// writable; capture them before unset wipes them.
	pos, epoch := c.offset, c.lastConsumedEpoch
	reset := cl.oorResetOffset(c)
	c.unset()
	if probe && shape == oorAboveEnd && epoch >= 0 && cl.supportsOffsetForLeaderEpoch() {
		css.recreated.add(topic, part)
		css.reloadOffsets.addLoad(topic, part, loadTypeEpoch, offsetLoad{
			replica:     -1,
			oorClassify: true,
			oorReset:    reset,
			Offset:      Offset{at: pos, epoch: epoch},
		})
		return
	}
	css.reloadOffsets.addLoad(topic, part, loadTypeList, offsetLoad{
		replica: -1,
		Offset:  reset,
	})
}

// recreationResetOffset is where consumption restarts on a classified topic
// recreation: the BEGINNING of the new incarnation, deliberately not
// ConsumeResetOffset. A subscription is a point in time and everything
// after, and everything in a replacement topic arrived after that point;
// ConsumeResetOffset governs positions within one topic's lifetime (e.g.
// where a brand-new subscription starts), not what a replacement topic
// starts at. NoResetOffset still opts out entirely (frozen + surfaced
// error + SetOffsets).
var recreationResetOffset = NewOffset().AtStart()

// errRecreationUnsureBatch fails buffered records whose produce outcome
// cannot be known across a topic recreation. Produced records carry it in
// their promise error.
var errRecreationUnsureBatch = errors.New("topic was deleted and recreated: a produce of this data went out addressed by topic name without a conclusive response, so it may or may not exist in the new topic; failing rather than risking a duplicate")

// errRecreationAbortTxn poisons the producer ID when a topic this
// transaction produced to was deleted and recreated: committing could
// silently cover writes that evaporated with the old incarnation (or landed
// in the new one out of transaction control), so the transaction must fail.
// Wrapping kerr.TransactionAbortable makes the existing classification
// apply: GroupTransactSession ends abort, and direct users retry
// EndTransaction(TryAbort). maybeRecoverProducerID additionally recognizes
// this sentinel itself, in both recovery modes: the poison is
// client-synthesized (the broker saw nothing fatal), so recovering the
// producer ID after the abort is always safe.
var errRecreationAbortTxn = fmt.Errorf("topic was deleted and recreated during the transaction; the transaction cannot commit safely across topic incarnations: %w", kerr.TransactionAbortable)

// errRecreationEpochGuard strips fetched records whose leader epoch
// regressed below what we already consumed: by name, the position points
// into a recreated topic's new incarnation (or a rolled-back log). Within
// one incarnation, epochs never decrease along the log, so this cannot fire
// on normal consumption.
var errRecreationEpochGuard = errors.New("fetched records regressed the leader epoch: topic recreation, or a rolled back log")

// errRecreationShareAck reports acknowledgments invalidated at a topic
// recreation swap: the records were acquired from an incarnation whose
// broker-side acquisition state died with it. Wraps the error the wire
// would have returned for an ack addressed to the dead incarnation's ID.
var errRecreationShareAck = fmt.Errorf("topic was deleted and recreated; these records were acquired from the prior incarnation, whose share state is gone: %w", kerr.UnknownTopicID)

// recreationGate arms the strongest tier of topic recreation handling (a
// topic deleted and recreated with the same name, yielding a new topic ID).
//
// When armed, the metadata merge adopts a recreated topic's new ID on a
// single wire corroboration. Arming requires every broker we have negotiated
// ApiVersions with to support fetch v13, which puts topic IDs on the fetch
// wire: a stale-ID fetch of a recreated topic fails with UNKNOWN_TOPIC_ID
// (the corroboration) and can never silently read records from the new
// incarnation. Below v13, fetches go by name and cannot corroborate, so the
// merge instead adopts once two consecutive metadata updates agree on the
// new ID (or immediately on produce-wire evidence), accepting a bounded
// by-name window that the fetch-side epoch guard and out-of-range
// classification shrink; with no IDs anywhere, a persistent leader epoch
// rewind is the remaining, opportunistic, signal. Share sessions are
// ID-addressed at every version and swap on wire corroboration regardless
// of the gate.
//
// The gate is re-evaluated on every metadata update: a broker negotiating
// below fetch v13 (e.g. mid rolling upgrade) disarms it, and it re-arms when
// that broker leaves the cluster or renegotiates at v13+. Brokers we have
// never connected to do not count against the gate: versions negotiate on
// first connect, before any fetch to that broker can be sent.
type recreationGate struct {
	armed atomic.Bool

	// confirmNow asks the metadata loop for one quick confirmation round:
	// a fresh suspected recreation was just observed (pendingRecreateID
	// newly set), and the second, confirming update should follow in the
	// quick-retry cadence rather than waiting out a full MetadataMinAge.
	confirmNow atomic.Bool
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
// are written only at partition creation or by the metadata merge itself,
// and this runs on that same metadata-update goroutine, so reading them here
// without locks is race-free.
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
