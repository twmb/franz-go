package kgo

import (
	"errors"
	"fmt"
	"maps"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// holdPriorID records id as previously held, ahead of adopting a new one.
func holdPriorID(prior *[2][16]byte, id [16]byte) {
	prior[1], prior[0] = prior[0], id
}

// swapRecreatedConsumer adopts a recreated topic on the consumer side: we
// reposition to reset, or, under NoResetOffset, freeze the partition and
// surface why (you resume with SetOffsets).
func (cl *Client) swapRecreatedConsumer(topic string, part int32, oldTP, newTP *topicPartition, css *consumerSessionStopper, reset Offset, why string) {
	rp := &reset
	if cl.cfg.resetOffset.noReset {
		rp = nil
		cl.consumer.addFakeReadyForDraining(topic, part,
			fmt.Errorf("%s (automatic resets are disabled via NoResetOffset; resume via SetOffsets): %w", why, kerr.UnknownTopicID),
			"metadata refresh sees topic recreation with resets disabled")
	}
	oldTP.swapRecreatedCursorTo(newTP, css, rp)
}

// mergeRecreatedRecBuf adopts a recreated topic on a producing partition,
// returning whether the merge is done with this partition. An ID change is a
// recreation, or rarely a flap from stale metadata. Below ID-ful metadata
// every ID is zero, nothing here fires, and producing is unchanged.
func (cl *Client) mergeRecreatedRecBuf(topic string, part int32, oldTP, newTP *topicPartition, retryWhy *multiUpdateWhy) bool {
	var noID [16]byte
	rb := oldTP.records
	newID := newTP.records.topicID
	if newID == noID {
		return false
	}

	rb.mu.Lock()
	oldID := rb.topicID
	if oldID == noID {
		// First sight of an ID for this topic, e.g. a broker upgrade
		// brought ID-ful metadata. Nothing is keyed to the zero ID, so
		// we adopt freely.
		rb.topicID, oldID = newID, newID
	}
	regressed := rb.offsetRegressed
	corroborated := rb.unknownFailures > 0 || regressed || rb.idMismatched
	exposed := rb.addedToTxn.Load() || len(rb.batches) > 0 || rb.inflight != 0
	rb.mu.Unlock()

	if oldID == newID {
		return false
	}

	// Transactions fail on the FIRST observation, not on corroboration:
	// if this partition is exposed to the transaction (added to it, or
	// batches buffered or in flight), we poison the producer ID now. A
	// spurious abort on a metadata flap is loud and recoverable, whereas
	// waiting would let a commit racing the evidence cover writes that
	// evaporated with the old incarnation. This is also what lets
	// commit-time verification trust any recent metadata pass rather than
	// fetching its own. The swap below still waits for the adopt rules.
	if cl.cfg.txnID != nil && exposed {
		if cur := cl.producer.id.Load().(*producerID); cur.err == nil {
			cl.failProducerID(cur.id, cur.epoch, errRecreationAbortTxn)
			cl.cfg.logger.Log(LogLevelWarn, "topic recreation observed with an active transaction exposed to it; failing the transaction",
				"topic", topic,
				"partition", part,
				"old_id", topicID(oldID),
				"new_id", topicID(newID),
			)
		}
	}

	// We adopt only on produce-wire evidence: a stale-incarnation
	// rejection, an acked offset regression, or commit-time verification.
	if !corroborated {
		*newTP = *oldTP
		// Keep draining: produce attempts must not stay parked on the
		// failing flag while we wait for corroboration.
		newTP.records.clearFailing()
		retryWhy.add(topic, part, errRecreationPending)
		return true
	}
	cl.cfg.logger.Log(LogLevelInfo, "topic recreation detected, adopting the new topic ID for producing",
		"topic", topic,
		"partition", part,
		"old_id", topicID(oldID),
		"new_id", topicID(newID),
		"restarting_sequences", !regressed,
	)
	oldTP.swapRecreatedRecBufTo(newTP)
	return true
}

// mergeRecreatedCursor adopts a recreated topic on a consuming partition,
// returning whether the merge is done with this partition.
//
// We act only on corroboration: a fetch the current leader rejected by ID.
// Until then we keep everything as is and let the stale-ID fetch corroborate;
// that rejection re-triggers metadata urgently, so the swap lands one update
// later.
func (cl *Client) mergeRecreatedCursor(topic string, part int32, oldTP, newTP *topicPartition, css *consumerSessionStopper, retryWhy *multiUpdateWhy) bool {
	var noID [16]byte
	c := oldTP.cursor
	newID, oldID := newTP.cursor.topicID, c.topicID
	if newID == noID || oldID == noID {
		return false
	}
	if newID == oldID {
		return false
	}

	if c.unknownIDFails.Load() == 0 {
		*newTP = *oldTP
		retryWhy.add(topic, part, errRecreationPending)
		return true
	}
	cl.cfg.logger.Log(LogLevelInfo, "topic recreation detected, adopting the new topic ID and restarting from the new topic's beginning",
		"topic", topic,
		"partition", part,
		"old_id", topicID(oldID),
		"new_id", topicID(newID),
		"new_leader", newTP.leader,
		"new_leader_epoch", newTP.leaderEpoch,
	)
	cl.swapRecreatedConsumer(topic, part, oldTP, newTP, css, recreationResetOffset, "topic was deleted and recreated")
	return true
}

// mergeRecreatedShareCursor adopts a recreated topic on a share consuming
// partition, returning whether the merge is done with this partition. Share
// sessions are ID-addressed at every broker version, so a share swaps on wire
// corroboration no matter what the fetch gate says.
func (cl *Client) mergeRecreatedShareCursor(topic string, part int32, oldTP, newTP *topicPartition, retryWhy *multiUpdateWhy) bool {
	var noID [16]byte
	sc := oldTP.shareCursor
	newID, oldID := newTP.shareCursor.topicID, sc.topicID
	if newID == noID || oldID == noID || newID == oldID {
		return false
	}
	if sc.unknownIDFails.Load() == 0 {
		*newTP = *oldTP
		retryWhy.add(topic, part, errRecreationPending)
		return true
	}
	cl.cfg.logger.Log(LogLevelInfo, "topic recreation detected, adopting the new topic ID for share consuming and invalidating acknowledgments of the prior incarnation",
		"topic", topic,
		"partition", part,
		"old_id", topicID(oldID),
		"new_id", topicID(newID),
	)
	oldTP.swapRecreatedShareCursorTo(cl, newTP)
	return true
}

// recreationResetOffset is where consumption restarts on a classified
// recreation: the beginning of the new topic, *not* ConsumeResetOffset. A
// subscription is a point in time and everything after, and everything in a
// replacement topic arrived after that point. ConsumeResetOffset governs
// where you start within one topic's lifetime. NoResetOffset still opts out
// entirely.
var recreationResetOffset = NewOffset().AtStart()

// errRecreationUnsureBatch fails buffered records whose produce outcome
// cannot be known across a topic recreation. Produced records carry it in
// their promise error.
var errRecreationUnsureBatch = errors.New("topic was deleted and recreated: a produce of this data went out addressed by topic name without a conclusive response, so it may or may not exist in the new topic; failing rather than risking a duplicate")

// errRecreationAbortTxn poisons the producer ID when a topic this
// transaction produced to was recreated: committing could cover writes that
// evaporated with the old incarnation. Wrapping kerr.TransactionAbortable
// reuses the existing classification, so GroupTransactSession aborts and
// you retry EndTransaction with TryAbort. maybeRecoverProducerID recognizes
// the sentinel: we synthesized it and the broker saw nothing fatal, so
// recovering after the abort is always safe.
var errRecreationAbortTxn = fmt.Errorf("topic was deleted and recreated during the transaction; the transaction cannot commit safely across topic incarnations: %w", kerr.TransactionAbortable)

// errRecreationShareAck reports acknowledgments invalidated at a recreation
// swap: the records were acquired from an incarnation whose broker side
// acquisition state died with it. This wraps the error the wire would have
// returned for an ack addressed to the dead incarnation's ID.
var errRecreationShareAck = fmt.Errorf("topic was deleted and recreated; these records were acquired from the prior incarnation, whose share state is gone: %w", kerr.UnknownTopicID)

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
