package kgo

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
)

// Recreation tunables. Everything else keys off broker facts: wire
// rejections, ID equality, epoch shapes.
const (
	// recreationRejectionGrace is how many consecutive rejections of the
	// topic ID we hold we absorb before surfacing them, and before a
	// prior ID that metadata keeps reporting is believed over it. A
	// broker rejecting our ID says only that its own view differs, which
	// a leader that has not yet learned a recreation says about the
	// correct ID too; paced, this many rejections outlast metadata
	// propagation, so by then the lagging view was ours.
	recreationRejectionGrace = 5

	// recreationMetadataBackoff paces refetches of a partition whose
	// fetch just triggered a metadata update (a rejected ID, or withheld
	// records), so that counts of those fetches measure metadata rounds
	// rather than round trips.
	recreationMetadataBackoff = 250 * time.Millisecond
)

// mergeRecreatedRecBuf adopts a recreated topic on a producing partition,
// returning whether the merge is done with this partition. Below ID-ful
// metadata every ID is zero, nothing here fires, and producing is unchanged.
func (cl *Client) mergeRecreatedRecBuf(topic string, part int32, oldTP, newTP *topicPartition) bool {
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
	rejections := rb.unknownFailures
	rb.mu.Unlock()

	if oldID == newID {
		return false
	}
	// A prior ID is a lagging broker's view; we ignore the update until
	// the ID we hold has been rejected for the whole grace.
	if slices.Contains(rb.priorIDs, newID) && rejections < recreationRejectionGrace {
		*newTP = *oldTP
		// Keep draining: produce attempts must not stay parked on the
		// failing flag over an ignored update.
		newTP.records.clearFailing()
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
// A never seen ID is a recreation and we adopt it outright, unless the
// cursor has no position yet: swapped that early, a racing old-incarnation
// committed offset would be applied to the new topic rather than rejected,
// so we retry until the position lands. A prior ID is a lagging broker's
// view, refused until the ID we hold has been rejected for the whole grace.
func (cl *Client) mergeRecreatedCursor(topic string, part int32, oldTP, newTP *topicPartition, css *consumerSessionStopper, retryWhy *multiUpdateWhy) bool {
	var noID [16]byte
	c := oldTP.cursor
	newID, oldID := newTP.cursor.topicID, c.topicID
	if newID == noID || oldID == noID || newID == oldID {
		return false
	}
	if slices.Contains(c.priorIDs, newID) && c.unknownIDFails.Load() < recreationRejectionGrace {
		*newTP = *oldTP
		return true
	}
	if !c.positioned.Load() {
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
	// We reposition to the new topic's beginning, or, under NoResetOffset,
	// freeze the partition and surface why (you resume with SetOffsets).
	reset := !cl.cfg.resetOffset.noReset
	if !reset {
		cl.consumer.addFakeReadyForDraining(topic, part,
			fmt.Errorf("topic was deleted and recreated (automatic resets are disabled via NoResetOffset; resume via SetOffsets): %w", kerr.UnknownTopicID),
			"metadata refresh sees topic recreation with resets disabled")
	}
	oldTP.swapRecreatedCursorTo(newTP, css, reset)
	return true
}

// recreationResetOffset is where consumption restarts on a detected
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
