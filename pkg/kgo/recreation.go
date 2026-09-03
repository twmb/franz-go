package kgo

import (
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
