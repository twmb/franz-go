package kgo

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type readerFrom interface {
	ReadFrom([]byte) error
}

// A source consumes from an individual broker.
//
// As long as there is at least one active cursor, a source aims to have *one*
// buffered fetch at all times. As soon as the fetch is taken, a source issues
// another fetch in the background.
type source struct {
	cl     *Client // our owning client, for cfg, metadata triggering, context, etc.
	nodeID int32   // the node ID of the broker this sink belongs to

	// Tracks how many _failed_ fetch requests we have in a row (unable to
	// receive a response). Any response, even responses with an ErrorCode
	// set, are successful. This field is used for backoff purposes.
	consecutiveFailures int

	fetchState workLoop
	sem        chan struct{} // closed when fetchable, recreated when a buffered fetch exists
	buffered   bufferedFetch // contains a fetch the source has buffered for polling

	session fetchSession // supports fetch sessions as per KIP-227

	cursorsMu    sync.Mutex
	cursors      []*cursor // contains all partitions being consumed on this source
	cursorsStart int       // incremented every fetch req to ensure all partitions are fetched
}

func (cl *Client) newSource(nodeID int32) *source {
	s := &source{
		cl:     cl,
		nodeID: nodeID,
		sem:    make(chan struct{}),
	}
	close(s.sem)
	return s
}

func (s *source) addCursor(add *cursor) {
	s.cursorsMu.Lock()
	add.cursorsIdx = len(s.cursors)
	s.cursors = append(s.cursors, add)
	s.cursorsMu.Unlock()

	// Adding a new cursor may allow a new partition to be fetched.
	// We do not need to cancel any current fetch nor kill the session,
	// since adding a cursor is non-destructive to work in progress.
	// If the session is currently stopped, this is a no-op.
	s.maybeConsume()
}

// Removes a cursor from the source.
//
// The caller should do this with a stopped session if necessary, which
// should clear any buffered fetch and reset the source's session.
func (s *source) removeCursor(rm *cursor) {
	s.cursorsMu.Lock()
	defer s.cursorsMu.Unlock()

	if rm.cursorsIdx != len(s.cursors)-1 {
		s.cursors[rm.cursorsIdx], s.cursors[len(s.cursors)-1] = s.cursors[len(s.cursors)-1], nil
		s.cursors[rm.cursorsIdx].cursorsIdx = rm.cursorsIdx
	} else {
		s.cursors[rm.cursorsIdx] = nil // do not let the memory hang around
	}

	s.cursors = s.cursors[:len(s.cursors)-1]
	if s.cursorsStart == len(s.cursors) {
		s.cursorsStart = 0
	}
}

// cursor is where we are consuming from for an individual partition.
//
// NOTE if adding fields here, check if they need to be handled when migrating
// the cursor between sources on a metadata update. This would be the case for
// any field that belongs in a topicPartition but is copied to the cursor; if
// enough fields like this exist, we can just use a topicPartition pointer
// directly, which would only be modified with the session stopped.
type cursor struct {
	topic     string
	partition int32

	keepControl bool // whether to keep control records

	cursorsIdx int // updated under source mutex

	// The source we are currently on. This is modified in two scenarios:
	//
	//  * by metadata when the consumer session is completely stopped
	//
	//  * by a fetch when handling a fetch response that returned preferred
	//  replicas
	//
	// This is additionally read within a session when cursor is
	// transitioning from used to usable.
	source *source

	// useState is an atomic that has two states: unusable and usable.  A
	// cursor can be used in a fetch request if it is in the usable state.
	// Once used, the cursor is unusable, and will be set back to usable
	// one the request lifecycle is complete (a usable fetch response, or
	// once listing offsets or loading epochs completes).
	//
	// A cursor can be set back to unusable when sources are stopped. This
	// can be done if a group loses a partition, for example.
	//
	// The used state is exclusively updated by either building a fetch
	// request or when the source is stopped.
	useState uint32

	// Our leader; if metadata sees this change, the metadata update
	// migrates us to a different source and updates this with the session
	// stopped.
	leader int32

	// What our cursor believes to be the epoch of the leader for this
	// partition. For KIP-320, if a broker receives a fetch request where
	// the current leader epoch does not match the brokers, either the
	// broker is behind and returns UnknownLeaderEpoch, or we are behind
	// and the broker returns FencedLeaderEpoch. For the former, we back
	// off and retry. For the latter, we update our metadata.
	leaderEpoch int32

	// NOTE if adding new fields, see the note preceeding the struct.

	// cursorOffset is our epoch/offset that we are consuming. When a fetch
	// request is issued, we "freeze" a view of the offset and of the
	// leader epoch (see cursorOffsetNext for why the leader epoch). When a
	// buffered fetch is taken, we update the cursor.
	cursorOffset
}

// cursorOffset tracks offsets/epochs for a cursor.
type cursorOffset struct {
	// What the cursor is at: we request this offset next.
	offset int64

	// The epoch of the last record we consumed. Also used for KIP-320, if
	// we are fenced or we have an offset out of range error, we go into
	// the OffsetForLeaderEpoch recovery. The last consumed epoch tells the
	// broker which offset we want: either (a) the next offset if the last
	// consumed epoch is the current epoch, or (b) the offset of the first
	// record in the next epoch. This allows for exact offset resetting and
	// data loss detection.
	//
	// See kmsg.OffsetForLeaderEpochResponseTopicPartition for more
	// details.
	lastConsumedEpoch int32
}

// use, for fetch requests, freezes a view of the cursorOffset.
func (c *cursor) use() *cursorOffsetNext {
	// A source using a cursor has exclusive access to the use field by
	// virtue of that source building a request during a live session,
	// or by virtue of the session being stopped.
	c.useState = 0
	return &cursorOffsetNext{
		cursorOffset:       c.cursorOffset,
		from:               c,
		currentLeaderEpoch: c.leaderEpoch,
	}
}

// unset transitions a cursor to an unusable state when the cursor is no longer
// to be consumed. This is called exclusively after sources are stopped.
// This also unsets the cursor offset, which is assumed to be unused now.
func (c *cursor) unset() {
	c.useState = 0
	c.setOffset(cursorOffset{
		offset:            -1,
		lastConsumedEpoch: -1,
	})
}

// usable returns whether a cursor can be used for building a fetch request.
func (c *cursor) usable() bool {
	return atomic.LoadUint32(&c.useState) == 1
}

// allowUsable allows a cursor to be fetched, and is called either in assigning
// offsets, or when a buffered fetch is taken or discarded,  or when listing /
// epoch loading finishes.
func (c *cursor) allowUsable() {
	atomic.SwapUint32(&c.useState, 1)
	c.source.maybeConsume()
}

// setOffset sets the cursors offset which will be used the next time a fetch
// request is built. This function is called under the source mutex while the
// source is stopped, and the caller is responsible for calling maybeConsume
// after.
func (c *cursor) setOffset(o cursorOffset) {
	c.cursorOffset = o
}

// cursorOffsetNext is updated while processing a fetch response.
//
// When a buffered fetch is taken, we update a cursor with the final values in
// the modified cursor offset.
type cursorOffsetNext struct {
	cursorOffset
	from *cursor

	// The leader epoch at the time we took this cursor offset snapshot. We
	// need to copy this rather than accessing it through `from` because a
	// fetch request can be canceled while it is being written (and reading
	// the epoch).
	//
	// The leader field itself is only read within the context of a session
	// while the session is alive, thus it needs no such guard.
	//
	// Basically, any field read in AppendTo needs to be copied into
	// cursorOffsetNext.
	currentLeaderEpoch int32
}

type cursorOffsetPreferred struct {
	cursorOffsetNext
	preferredReplica int32
}

// Moves a cursor from one source to another. This is done while handling
// a fetch response, which means within the context of a live session.
func (p *cursorOffsetPreferred) move() {
	c := p.from
	defer c.allowUsable()

	// Before we migrate the cursor, we check if the destination source
	// exists. If not, we do not migrate and instead force a metadata.

	c.source.cl.sinksAndSourcesMu.Lock()
	sns, exists := c.source.cl.sinksAndSources[p.preferredReplica]
	c.source.cl.sinksAndSourcesMu.Unlock()

	if !exists {
		c.source.cl.triggerUpdateMetadataNow()
		return
	}

	// This remove clears the source's session and buffered fetch, although
	// we will not have a buffered fetch since moving replicas is called
	// before buffering a fetch.
	c.source.removeCursor(c)
	c.source = sns.source
	c.source.addCursor(c)
}

type cursorPreferreds []cursorOffsetPreferred

func (cs cursorPreferreds) eachPreferred(fn func(cursorOffsetPreferred)) {
	for _, c := range cs {
		fn(c)
	}
}

type usedOffsets map[string]map[int32]*cursorOffsetNext

func (os usedOffsets) eachOffset(fn func(*cursorOffsetNext)) {
	for _, ps := range os {
		for _, o := range ps {
			fn(o)
		}
	}
}

func (os usedOffsets) finishUsingAllWith(fn func(*cursorOffsetNext)) {
	os.eachOffset(func(o *cursorOffsetNext) { fn(o); o.from.allowUsable() })
}

func (os usedOffsets) finishUsingAll() {
	os.eachOffset(func(o *cursorOffsetNext) { o.from.allowUsable() })
}

// bufferedFetch is a fetch response waiting to be consumed by the client.
type bufferedFetch struct {
	fetch Fetch

	doneFetch   chan<- struct{} // when unbuffered, we send down this
	usedOffsets usedOffsets     // what the offsets will be next if this fetch is used
}

// takeBuffered drains a buffered fetch and updates offsets.
func (s *source) takeBuffered() Fetch {
	return s.takeBufferedFn(func(usedOffsets usedOffsets) {
		usedOffsets.finishUsingAllWith(func(o *cursorOffsetNext) {
			o.from.setOffset(o.cursorOffset)
		})
	})
}

func (s *source) discardBuffered() {
	s.takeBufferedFn(usedOffsets.finishUsingAll)
}

func (s *source) takeBufferedFn(offsetFn func(usedOffsets)) Fetch {
	r := s.buffered
	s.buffered = bufferedFetch{}
	offsetFn(r.usedOffsets)
	r.doneFetch <- struct{}{}
	close(s.sem)
	return r.fetch
}

// createReq actually creates a fetch request.
func (s *source) createReq() *fetchRequest {
	req := &fetchRequest{
		maxWait:        s.cl.cfg.maxWait,
		minBytes:       s.cl.cfg.minBytes,
		maxBytes:       s.cl.cfg.maxBytes,
		maxPartBytes:   s.cl.cfg.maxPartBytes,
		rack:           s.cl.cfg.rack,
		isolationLevel: s.cl.cfg.isolationLevel,

		// We copy a view of the session for the request, which allows
		// us to reset the source (resetting only its fields without
		// modifying the prior map) while the request may be reading
		// its copy of the original fields.
		session: s.session,
	}

	s.cursorsMu.Lock()
	defer s.cursorsMu.Unlock()

	cursorIdx := s.cursorsStart
	for i := 0; i < len(s.cursors); i++ {
		c := s.cursors[cursorIdx]
		cursorIdx = (cursorIdx + 1) % len(s.cursors)
		if !c.usable() {
			continue
		}
		req.addCursor(c)
	}

	// We could have lost our only record buffer just before we grabbed the
	// source lock above.
	if len(s.cursors) > 0 {
		s.cursorsStart = (s.cursorsStart + 1) % len(s.cursors)
	}

	return req
}

func (s *source) maybeConsume() {
	if s.fetchState.maybeBegin() {
		go s.loopFetch()
	}
}

func (s *source) loopFetch() {
	consumer := &s.cl.consumer
	session := consumer.loadSession()

	if session == noConsumerSession {
		s.fetchState.hardFinish()
		return
	}

	session.incWorker()
	defer session.decWorker()

	// After our add, check quickly **without** another select case to
	// determine if this context was truly canceled. Any other select that
	// has another select case could theoretically race with the other case
	// also being selected.
	select {
	case <-session.ctx.Done():
		s.fetchState.hardFinish()
		return
	default:
	}

	// We receive on canFetch when we can fetch, and we send back when we
	// are done fetching.
	canFetch := make(chan chan<- struct{}, 1)

	again := true
	for again {
		select {
		case <-session.ctx.Done():
			s.fetchState.hardFinish()
			return
		case <-s.sem:
		}

		select {
		case <-session.ctx.Done():
			s.fetchState.hardFinish()
			return
		case session.desireFetch() <- canFetch:
		}

		select {
		case <-session.ctx.Done():
			s.fetchState.hardFinish()
			return
		case doneFetch := <-canFetch:
			again = s.fetchState.maybeFinish(s.fetch(session, doneFetch))
		}
	}

}

// fetch is the main logic center of fetching messages.
//
// This is a long function, made much longer by winded documentation, that
// contains a lot of the side effects of fetching and updating. The function
// consists of two main bulks of logic:
//
//   * First, issue a request that can be killed if the source needs to be
//   stopped. Processing the response modifies no state on the source.
//
//   * Second, we keep the fetch response and update everything relevant
//   (session, trigger some list or epoch updates, buffer the fetch).
//
// One small part between the first and second step is to update preferred
// replicas. We always keep the preferred replicas from the fetch response
// *even if* the source needs to be stopped. The knowledge of which preferred
// replica to use would not be out of date even if the consumer session is
// changing.
func (s *source) fetch(consumerSession *consumerSession, doneFetch chan<- struct{}) (fetched bool) {
	req := s.createReq()

	// For all returns, if we do not buffer our fetch, then we want to
	// ensure our used offsets are usable again.
	var alreadySentToDoneFetch bool
	var buffered bool
	defer func() {
		if !buffered {
			if req.numOffsets > 0 {
				req.usedOffsets.finishUsingAll()
			}
			if !alreadySentToDoneFetch {
				doneFetch <- struct{}{}
			}
		}
	}()

	if req.numOffsets == 0 { // cursors could have been set unusable
		return
	}

	// If our fetch is killed, we want to cancel waiting for the response.
	var (
		kresp       kmsg.Response
		requested   = make(chan struct{})
		ctx, cancel = context.WithCancel(consumerSession.ctx)
	)
	defer cancel()

	br, err := s.cl.brokerOrErr(ctx, s.nodeID, ErrUnknownBroker)
	if err != nil {
		close(requested)
	} else {
		br.do(ctx, req, func(k kmsg.Response, e error) {
			kresp, err = k, e
			close(requested)
		})
	}

	select {
	case <-requested:
		fetched = true
	case <-ctx.Done():
		return
	}

	// If we had an error, we backoff. Killing a fetch quits the backoff,
	// but that is fine; we may just re-request too early and fall into
	// another backoff.
	if err != nil {
		// We preemptively allow more fetches (since we are not buffering)
		// and reset our session because of the error (who knows if kafka
		// processed the request but the client failed to receive it).
		doneFetch <- struct{}{}
		alreadySentToDoneFetch = true
		s.session.reset()

		s.cl.triggerUpdateMetadata()
		s.consecutiveFailures++
		after := time.NewTimer(s.cl.cfg.retryBackoff(s.consecutiveFailures))
		defer after.Stop()
		select {
		case <-after.C:
		case <-ctx.Done():
		}
		return
	}
	s.consecutiveFailures = 0

	resp := kresp.(*kmsg.FetchResponse)

	var (
		fetch         Fetch
		reloadOffsets listOrEpochLoads
		preferreds    cursorPreferreds
		updateMeta    bool
		handled       = make(chan struct{})
	)

	// Theoretically, handleReqResp could take a bit of CPU time due to
	// decompressing and processing the response. We do this in a goroutine
	// to allow the session to be canceled at any moment.
	//
	// Processing the response only needs the source's nodeID and client.
	go func() {
		defer close(handled)
		fetch, reloadOffsets, preferreds, updateMeta = s.handleReqResp(req, resp)
	}()

	select {
	case <-handled:
	case <-ctx.Done():
		return
	}

	// The logic below here should be relatively quick.

	deleteReqUsedOffset := func(topic string, partition int32) {
		t := req.usedOffsets[topic]
		delete(t, partition)
		if len(t) == 0 {
			delete(req.usedOffsets, topic)
		}
	}

	// Before updating the source, we move all cursors that have new
	// preferred replicas and remove them from being tracked in our req
	// offsets. We also remove the reload offsets from our req offsets.
	//
	// These two removals transition responsibility for finishing using the
	// cursor from the request's used offsets to the new source or the
	// reloading.
	preferreds.eachPreferred(func(c cursorOffsetPreferred) {
		c.move()
		deleteReqUsedOffset(c.from.topic, c.from.partition)
	})
	reloadOffsets.each(deleteReqUsedOffset)

	// The session on the request was updated; we keep those updates.
	s.session = req.session

	// handleReqResp only parses the body of the response, not the top
	// level error code.
	//
	// The top level error code is related to fetch sessions only, and if
	// there was an error, the body was empty (so processing is basically a
	// no-op). We process the fetch session error now.
	switch err := kerr.ErrorForCode(resp.ErrorCode); err {
	case kerr.FetchSessionIDNotFound:
		if s.session.epoch == 0 {
			// If the epoch was zero, the broker did not even
			// establish a session for us (and thus is maxed on
			// sessions). We stop trying.
			s.cl.cfg.logger.Log(LogLevelInfo, "session failed with SessionIDNotFound while trying to establish a session; broker likely maxed on sessions; continuing on without using sessions")
			s.session.kill()
		} else {
			s.cl.cfg.logger.Log(LogLevelInfo, "received SessionIDNotFound from our in use session, our session was likely evicted; resetting session")
			s.session.reset()
		}
		return
	case kerr.InvalidFetchSessionEpoch:
		s.cl.cfg.logger.Log(LogLevelInfo, "resetting fetch session", "err", err)
		s.session.reset()
		return
	}

	if resp.Version < 7 {
		// If the version is less than 7, we cannot use fetch sessions,
		// so we kill them on the first response.
		s.session.kill()
	} else if resp.SessionID > 0 {
		s.session.bumpEpoch(resp.SessionID)
	}

	// If we moved any partitions to preferred replicas, we reset the
	// session. We do this after bumping the epoch just to ensure that we
	// have truly reset the session. (TODO switch to usingForgottenTopics)
	if len(preferreds) > 0 {
		s.session.reset()
	}

	if updateMeta {
		s.cl.triggerUpdateMetadataNow()
	}

	reloadOffsets.loadWithSessionNow(consumerSession)

	if len(fetch.Topics) > 0 {
		buffered = true
		s.buffered = bufferedFetch{
			fetch:       fetch,
			doneFetch:   doneFetch,
			usedOffsets: req.usedOffsets,
		}
		s.sem = make(chan struct{})
		s.cl.consumer.addSourceReadyForDraining(s)
	}
	return
}

// Parses a fetch response into a Fetch, offsets to reload, and whether
// metadata needs updating.
//
// This only uses a source's broker and client, and thus does not need
// the source mutex.
//
// This function, and everything it calls, is side effect free.
func (s *source) handleReqResp(req *fetchRequest, resp *kmsg.FetchResponse) (Fetch, listOrEpochLoads, cursorPreferreds, bool) {
	var (
		f = Fetch{
			Topics: make([]FetchTopic, 0, len(resp.Topics)),
		}
		reloadOffsets listOrEpochLoads
		preferreds    []cursorOffsetPreferred
		updateMeta    bool
	)
	for _, rt := range resp.Topics {
		topic := rt.Topic
		// We always include all cursors on this source in the fetch;
		// we should not receive any topics or partitions we do not
		// expect.
		topicOffsets, ok := req.usedOffsets[topic]
		if !ok {
			continue
		}

		fetchTopic := FetchTopic{
			Topic:      topic,
			Partitions: make([]FetchPartition, 0, len(rt.Partitions)),
		}

		for i := range rt.Partitions {
			rp := &rt.Partitions[i]
			partition := rp.Partition
			partOffset, ok := topicOffsets[partition]
			if !ok {
				continue
			}

			// If we are fetching from the replica already, Kafka replies with a -1
			// preferred read replica. If Kafka replies with a preferred replica,
			// it sends no records.
			if preferred := rp.PreferredReadReplica; resp.Version >= 11 && preferred >= 0 {
				preferreds = append(preferreds, cursorOffsetPreferred{
					*partOffset,
					preferred,
				})
				continue
			}

			fetchTopic.Partitions = append(fetchTopic.Partitions, partOffset.processRespPartition(resp.Version, rp, s.cl.decompressor))
			fp := &fetchTopic.Partitions[len(fetchTopic.Partitions)-1]
			updateMeta = updateMeta || fp.Err != nil

			switch fp.Err {
			default:
				// - bad auth
				// - unsupported compression
				// - unsupported message version
				// - unknown error
				// - or, no error

			case kerr.UnknownTopicOrPartition,
				kerr.NotLeaderForPartition,
				kerr.ReplicaNotAvailable,
				kerr.KafkaStorageError,
				kerr.UnknownLeaderEpoch, // our meta is newer than broker we fetched from
				kerr.OffsetNotAvailable: // fetched from out of sync replica or a behind in-sync one (KIP-392: case 1 and case 2)

				fp.Err = nil // recoverable with client backoff; hide the error

			case kerr.OffsetOutOfRange:
				fp.Err = nil

				// If we are out of range, we reset to what we can.
				// With Kafka >= 2.1.0, we should only get offset out
				// of range if we fetch before the start, but a user
				// could start past the end and want to reset to
				// the end. We respect that.
				//
				// KIP-392 (case 3) specifies that if we are consuming
				// from a follower, then if our offset request is before
				// the low watermark, we list offsets from the follower.
				//
				// KIP-392 (case 4) specifies that if we are consuming
				// a follower and our request is larger than the high
				// watermark, then we should first check for truncation
				// from the leader and then if we still get out of
				// range, reset with list offsets.
				//
				// It further goes on to say that "out of range errors
				// due to ISR propagation delays should be extremely
				// rare". Rather than falling back to listing offsets,
				// we stay in a cycle of validating the leader epoch
				// until the follower has caught up.

				if s.nodeID == partOffset.from.leader { // non KIP-392 case
					reloadOffsets.addLoad(topic, partition, loadTypeList, offsetLoad{
						replica: -1,
						Offset:  s.cl.cfg.resetOffset,
					})
				} else if partOffset.offset < fp.LogStartOffset { // KIP-392 case 3
					reloadOffsets.addLoad(topic, partition, loadTypeList, offsetLoad{
						replica: s.nodeID,
						Offset:  s.cl.cfg.resetOffset,
					})
				} else { // partOffset.offset > fp.HighWatermark, KIP-392 case 4
					reloadOffsets.addLoad(topic, partition, loadTypeEpoch, offsetLoad{
						replica: -1,
						Offset: Offset{
							at:    partOffset.offset,
							epoch: partOffset.lastConsumedEpoch,
						},
					})
				}

			case kerr.FencedLeaderEpoch:
				fp.Err = nil

				// With fenced leader epoch, we notify an error only
				// if necessary after we find out if loss occurred.
				// If we have consumed nothing, then we got unlucky
				// by being fenced right after we grabbed metadata.
				// We just refresh metadata and try again.
				if partOffset.lastConsumedEpoch >= 0 {
					reloadOffsets.addLoad(topic, partition, loadTypeEpoch, offsetLoad{
						replica: -1,
						Offset: Offset{
							at:    partOffset.offset,
							epoch: partOffset.lastConsumedEpoch,
						},
					})
				}
			}
		}

		if len(fetchTopic.Partitions) > 0 {
			f.Topics = append(f.Topics, fetchTopic)
		}
	}

	return f, reloadOffsets, preferreds, updateMeta
}

// processRespPartition processes all records in all potentially compressed
// batches (or message sets).
func (o *cursorOffsetNext) processRespPartition(version int16, rp *kmsg.FetchResponseTopicPartition, decompressor *decompressor) FetchPartition {
	fp := FetchPartition{
		Partition:        rp.Partition,
		Err:              kerr.ErrorForCode(rp.ErrorCode),
		HighWatermark:    rp.HighWatermark,
		LastStableOffset: rp.LastStableOffset,
		LogStartOffset:   rp.LogStartOffset,
	}

	aborter := buildAborter(rp)

	// A response could contain any of message v0, message v1, or record
	// batches, and this is solely dictated by the magic byte (not the
	// fetch response version). The magic byte is located at byte 17.
	//
	// 1 thru 8: int64 offset / first offset
	// 9 thru 12: int32 length
	// 13 thru 16: crc (magic 0 or 1), or partition leader epoch (magic 2)
	// 17: magic
	//
	// We decode and validate similarly for messages and record batches, so
	// we "abstract" away the high level stuff into a check function just
	// below, and then switch based on the magic for how to process.
	var (
		in = rp.RecordBatches

		r           readerFrom
		length      int32
		lengthField *int32
		crcField    *int32
		crcTable    *crc32.Table
		crcAt       int

		check = func() bool {
			if err := r.ReadFrom(in[:length]); err != nil {
				return false
			}
			if length := int32(len(in[12:length])); length != *lengthField {
				fp.Err = fmt.Errorf("encoded length %d does not match read length %d", *lengthField, length)
				return false
			}
			if crcCalc := int32(crc32.Checksum(in[crcAt:length], crcTable)); crcCalc != *crcField {
				fp.Err = fmt.Errorf("encoded crc %x does not match calculated crc %x", *crcField, crcCalc)
				return false
			}
			return true
		}
	)

	for len(in) > 17 && fp.Err == nil {
		length = int32(binary.BigEndian.Uint32(in[8:]))
		length += 12 // for the int64 offset we skipped and int32 length field itself
		if len(in) < int(length) {
			break
		}

		switch magic := in[16]; magic {
		case 0:
			m := new(kmsg.MessageV0)
			lengthField = &m.MessageSize
			crcField = &m.CRC
			crcTable = crc32.IEEETable
			crcAt = 16
			r = m
		case 1:
			m := new(kmsg.MessageV1)
			lengthField = &m.MessageSize
			crcField = &m.CRC
			crcTable = crc32.IEEETable
			crcAt = 16
			r = m
		case 2:
			rb := new(kmsg.RecordBatch)
			lengthField = &rb.Length
			crcField = &rb.CRC
			crcTable = crc32c
			crcAt = 21
			r = rb

		}

		if !check() {
			break
		}

		in = in[length:]

		switch t := r.(type) {
		case *kmsg.MessageV0:
			o.processV0OuterMessage(&fp, t, decompressor)
		case *kmsg.MessageV1:
			o.processV1OuterMessage(&fp, t, decompressor)
		case *kmsg.RecordBatch:
			o.processRecordBatch(&fp, t, aborter, decompressor)
		}
	}

	return fp
}

type aborter map[int64][]int64

func buildAborter(rp *kmsg.FetchResponseTopicPartition) aborter {
	if len(rp.AbortedTransactions) == 0 {
		return nil
	}
	a := make(aborter)
	for _, abort := range rp.AbortedTransactions {
		a[abort.ProducerID] = append(a[abort.ProducerID], abort.FirstOffset)
	}
	return a
}

func (a aborter) shouldAbortBatch(b *kmsg.RecordBatch) bool {
	if len(a) == 0 || b.Attributes&0b0001_0000 == 0 {
		return false
	}
	pidAborts := a[b.ProducerID]
	if len(pidAborts) == 0 {
		return false
	}
	// If the first offset in this batch is less than the first offset
	// aborted, then this batch is not aborted.
	if b.FirstOffset < pidAborts[0] {
		return false
	}
	return true
}

func (a aborter) trackAbortedPID(producerID int64) {
	remaining := a[producerID][1:]
	if len(remaining) == 0 {
		delete(a, producerID)
	} else {
		a[producerID] = remaining
	}
}

//////////////////////////////////////
// processing records to fetch part //
//////////////////////////////////////

// readRawRecords reads n records from in and returns them, returning
// kbin.ErrNotEnoughData if in does not contain enough data.
func readRawRecords(n int, in []byte) ([]kmsg.Record, error) {
	rs := make([]kmsg.Record, n)
	for i := 0; i < n; i++ {
		length, used := kbin.Varint(in)
		total := used + int(length)
		if used == 0 || length < 0 || len(in) < total {
			return nil, kbin.ErrNotEnoughData
		}
		if err := (&rs[i]).ReadFrom(in[:total]); err != nil {
			return nil, err
		}
		in = in[total:]
	}
	return rs, nil
}

func (o *cursorOffsetNext) processRecordBatch(
	fp *FetchPartition,
	batch *kmsg.RecordBatch,
	aborter aborter,
	decompressor *decompressor,
) {
	if batch.Magic != 2 {
		fp.Err = fmt.Errorf("unknown batch magic %d", batch.Magic)
		return
	}
	rawRecords := batch.Records
	if compression := byte(batch.Attributes & 0x0007); compression != 0 {
		var err error
		if rawRecords, err = decompressor.decompress(rawRecords, compression); err != nil {
			fp.Err = fmt.Errorf("unable to decompress batch: %v", err)
			return
		}
	}

	lastOffset := batch.FirstOffset + int64(batch.LastOffsetDelta)
	if lastOffset < o.offset {
		// If the last offset in this batch is less than what we asked
		// for, we got a batch that we entirely do not need. We can
		// avoid all work (although we should not get this batch).
		return
	}

	nextAskOffset := lastOffset + 1
	defer func() {
		if o.offset < nextAskOffset {
			// KAFKA-5443: compacted topics preserve the last offset in a
			// batch, even if the last record is removed, meaning that
			// using offsets from records alone may not get us to the next
			// offset we need to ask for.
			o.offset = nextAskOffset
		}
	}()

	krecords, err := readRawRecords(int(batch.NumRecords), rawRecords)
	if err != nil {
		fp.Err = fmt.Errorf("invalid record batch: %v, on offset %d, asking next for offset %d", err, o.offset, nextAskOffset)
		return
	}

	abortBatch := aborter.shouldAbortBatch(batch)
	var lastRecord *Record
	for i := range krecords {
		record := recordToRecord(
			o.from.topic,
			fp.Partition,
			batch,
			&krecords[i],
		)
		lastRecord = record
		o.maybeKeepRecord(fp, record, abortBatch)
	}

	if abortBatch && lastRecord != nil && lastRecord.Attrs.IsControl() {
		aborter.trackAbortedPID(batch.ProducerID)
	}
}

// Processes an outer v1 message. There could be no inner message, which makes
// this easy, but if not, we decompress and process each inner message as
// either v0 or v1. We only expect the inner message to be v1, but technically
// a crazy pipeline could have v0 anywhere.
func (o *cursorOffsetNext) processV1OuterMessage(
	fp *FetchPartition,
	message *kmsg.MessageV1,
	decompressor *decompressor,
) {
	compression := byte(message.Attributes & 0x0003)
	if compression == 0 {
		o.processV1Message(fp, message)
		return
	}

	rawInner, err := decompressor.decompress(message.Value, compression)
	if err != nil {
		fp.Err = fmt.Errorf("unable to decompress messages: %v", err)
		return
	}

	var innerMessages []readerFrom
	for len(rawInner) > 17 { // magic at byte 17
		length := int32(binary.BigEndian.Uint32(rawInner[8:]))
		length += 12 // skip offset and length fields
		if len(rawInner) < int(length) {
			break
		}

		var (
			magic = rawInner[16]

			msg         readerFrom
			lengthField *int32
			crcField    *int32
		)

		switch magic {
		case 0:
			m := new(kmsg.MessageV0)
			msg = m
			lengthField = &m.MessageSize
			crcField = &m.CRC
		case 1:
			m := new(kmsg.MessageV1)
			msg = m
			lengthField = &m.MessageSize
			crcField = &m.CRC

		default:
			fp.Err = fmt.Errorf("message set v1 has inner message with invalid magic %d", magic)
			break
		}

		if err := msg.ReadFrom(rawInner[:length]); err != nil {
			break
		}
		if length := int32(len(rawInner[12:length])); length != *lengthField {
			fp.Err = fmt.Errorf("encoded length %d does not match read length %d", *lengthField, length)
			break
		}
		if crcCalc := int32(crc32.ChecksumIEEE(rawInner[16:length])); crcCalc != *crcField {
			fp.Err = fmt.Errorf("encoded crc %x does not match calculated crc %x", *crcField, crcCalc)
			break
		}
		innerMessages = append(innerMessages, msg)
		rawInner = rawInner[length:]
	}
	if len(innerMessages) == 0 {
		return
	}

	firstOffset := message.Offset - int64(len(innerMessages)) + 1
	for i := range innerMessages {
		innerMessage := innerMessages[i]
		switch innerMessage := innerMessage.(type) {
		case *kmsg.MessageV0:
			innerMessage.Offset = firstOffset + int64(i)
			if !o.processV0Message(fp, innerMessage) {
				return
			}
		case *kmsg.MessageV1:
			innerMessage.Offset = firstOffset + int64(i)
			if !o.processV1Message(fp, innerMessage) {
				return
			}
		}
	}
}

func (o *cursorOffsetNext) processV1Message(
	fp *FetchPartition,
	message *kmsg.MessageV1,
) bool {
	if message.Magic != 1 {
		fp.Err = fmt.Errorf("unknown message magic %d", message.Magic)
		return false
	}
	if message.Attributes != 0 {
		fp.Err = fmt.Errorf("unknown attributes on uncompressed message %d", message.Attributes)
		return false
	}
	record := v1MessageToRecord(o.from.topic, fp.Partition, message)
	o.maybeKeepRecord(fp, record, false)
	return true
}

// Processes an outer v0 message. We expect inner messages to be entirely v0 as
// well, so this only tries v0 always.
func (o *cursorOffsetNext) processV0OuterMessage(
	fp *FetchPartition,
	message *kmsg.MessageV0,
	decompressor *decompressor,
) {
	compression := byte(message.Attributes & 0x0003)
	if compression == 0 {
		o.processV0Message(fp, message)
		return
	}

	rawInner, err := decompressor.decompress(message.Value, compression)
	if err != nil {
		fp.Err = fmt.Errorf("unable to decompress messages: %v", err)
		return
	}

	var innerMessages []kmsg.MessageV0
	for len(rawInner) > 17 { // magic at byte 17
		length := int32(binary.BigEndian.Uint32(rawInner[8:]))
		length += 12 // skip offset and length fields
		if len(rawInner) < int(length) {
			break
		}
		var m kmsg.MessageV0
		if err := m.ReadFrom(rawInner[:length]); err != nil {
			break
		}
		if length := int32(len(rawInner[12:length])); length != m.MessageSize {
			fp.Err = fmt.Errorf("encoded length %d does not match read length %d", m.MessageSize, length)
			break
		}
		if crcCalc := int32(crc32.ChecksumIEEE(rawInner[16:length])); crcCalc != m.CRC {
			fp.Err = fmt.Errorf("encoded crc %x does not match calculated crc %x", m.CRC, crcCalc)
			break
		}
		innerMessages = append(innerMessages, m)
		rawInner = rawInner[length:]
	}
	if len(innerMessages) == 0 {
		return
	}

	firstOffset := message.Offset - int64(len(innerMessages)) + 1
	for i := range innerMessages {
		innerMessage := &innerMessages[i]
		innerMessage.Offset = firstOffset + int64(i)
		if !o.processV0Message(fp, innerMessage) {
			return
		}
	}
}

func (o *cursorOffsetNext) processV0Message(
	fp *FetchPartition,
	message *kmsg.MessageV0,
) bool {
	if message.Magic != 0 {
		fp.Err = fmt.Errorf("unknown message magic %d", message.Magic)
		return false
	}
	if message.Attributes != 0 {
		fp.Err = fmt.Errorf("unknown attributes on uncompressed message %d", message.Attributes)
		return false
	}
	record := v0MessageToRecord(o.from.topic, fp.Partition, message)
	o.maybeKeepRecord(fp, record, false)
	return true
}

// maybeKeepRecord keeps a record if it is within our range of offsets to keep.
//
// If the record is being aborted or the record is a control record and the
// client does not want to keep control records, this does not keep the record.
func (o *cursorOffsetNext) maybeKeepRecord(fp *FetchPartition, record *Record, abort bool) {
	if record.Offset < o.offset {
		// We asked for offset 5, but that was in the middle of a
		// batch; we got offsets 0 thru 4 that we need to skip.
		return
	}

	// We only keep control records if specifically requested.
	if record.Attrs.IsControl() && !o.from.keepControl {
		abort = true
	}
	if !abort {
		fp.Records = append(fp.Records, record)
	}

	// The record offset may be much larger than our expected offset if the
	// topic is compacted.
	o.offset = record.Offset + 1
	o.lastConsumedEpoch = record.LeaderEpoch
}

///////////////////////////////
// kmsg.Record to kgo.Record //
///////////////////////////////

func timeFromMillis(millis int64) time.Time {
	return time.Unix(0, millis*1e6)
}

// recordToRecord converts a kmsg.RecordBatch's Record to a kgo Record.
func recordToRecord(
	topic string,
	partition int32,
	batch *kmsg.RecordBatch,
	record *kmsg.Record,
) *Record {
	h := make([]RecordHeader, 0, len(record.Headers))
	for _, kv := range record.Headers {
		h = append(h, RecordHeader{
			Key:   kv.Key,
			Value: kv.Value,
		})
	}

	return &Record{
		Key:           record.Key,
		Value:         record.Value,
		Headers:       h,
		Timestamp:     timeFromMillis(batch.FirstTimestamp + int64(record.TimestampDelta)),
		Topic:         topic,
		Partition:     partition,
		Attrs:         RecordAttrs{uint8(batch.Attributes)},
		ProducerID:    batch.ProducerID,
		ProducerEpoch: batch.ProducerEpoch,
		LeaderEpoch:   batch.PartitionLeaderEpoch,
		Offset:        batch.FirstOffset + int64(record.OffsetDelta),
	}
}

func messageAttrsToRecordAttrs(attrs int8, v0 bool) RecordAttrs {
	uattrs := uint8(attrs)
	timestampType := uattrs & 0b0000_0100
	uattrs = uattrs&0b0000_0011 | timestampType<<1
	if v0 {
		uattrs = uattrs | 0b1000_0000
	}
	return RecordAttrs{uattrs}
}

func v0MessageToRecord(
	topic string,
	partition int32,
	message *kmsg.MessageV0,
) *Record {
	return &Record{
		Key:           message.Key,
		Value:         message.Value,
		Topic:         topic,
		Partition:     partition,
		Attrs:         messageAttrsToRecordAttrs(message.Attributes, true),
		ProducerID:    -1,
		ProducerEpoch: -1,
		LeaderEpoch:   -1,
		Offset:        message.Offset,
	}
}

func v1MessageToRecord(
	topic string,
	partition int32,
	message *kmsg.MessageV1,
) *Record {
	return &Record{
		Key:           message.Key,
		Value:         message.Value,
		Timestamp:     timeFromMillis(message.Timestamp),
		Topic:         topic,
		Partition:     partition,
		Attrs:         messageAttrsToRecordAttrs(message.Attributes, false),
		ProducerID:    -1,
		ProducerEpoch: -1,
		LeaderEpoch:   -1,
		Offset:        message.Offset,
	}
}

//////////////////
// fetchRequest //
//////////////////

type fetchRequest struct {
	version      int16
	maxWait      int32
	minBytes     int32
	maxBytes     int32
	maxPartBytes int32
	rack         string

	isolationLevel int8

	numOffsets  int
	usedOffsets usedOffsets

	// Session is a copy of the source session at the time a request is
	// built. If the source is reset, the session it has is reset at the
	// field level only. Our view of the original session is still valid.
	session fetchSession
}

func (f *fetchRequest) addCursor(c *cursor) {
	if f.usedOffsets == nil {
		f.usedOffsets = make(usedOffsets)
	}
	partitions := f.usedOffsets[c.topic]
	if partitions == nil {
		partitions = make(map[int32]*cursorOffsetNext)
		f.usedOffsets[c.topic] = partitions
	}
	partitions[c.partition] = c.use()
	f.numOffsets++
}

func (*fetchRequest) Key() int16           { return 1 }
func (*fetchRequest) MaxVersion() int16    { return 12 }
func (f *fetchRequest) SetVersion(v int16) { f.version = v }
func (f *fetchRequest) GetVersion() int16  { return f.version }
func (f *fetchRequest) IsFlexible() bool   { return f.version >= 12 } // version 12+ is flexible
func (f *fetchRequest) AppendTo(dst []byte) []byte {
	req := kmsg.FetchRequest{
		Version:        f.version,
		ReplicaID:      -1,
		MaxWaitMillis:  f.maxWait,
		MinBytes:       f.minBytes,
		MaxBytes:       f.maxBytes,
		IsolationLevel: f.isolationLevel,
		SessionID:      f.session.id,
		SessionEpoch:   f.session.epoch,
		Rack:           f.rack,
	}

	for topic, partitions := range f.usedOffsets {

		var reqTopic *kmsg.FetchRequestTopic
		sessionTopic := f.session.lookupTopic(topic)

		for partition, cursorOffsetNext := range partitions {
			if !sessionTopic.hasPartitionAt(
				partition,
				cursorOffsetNext.offset,
				cursorOffsetNext.currentLeaderEpoch,
			) {

				if reqTopic == nil {
					req.Topics = append(req.Topics, kmsg.FetchRequestTopic{
						Topic: topic,
					})
					reqTopic = &req.Topics[len(req.Topics)-1]
				}

				reqTopic.Partitions = append(reqTopic.Partitions, kmsg.FetchRequestTopicPartition{
					Partition:          partition,
					CurrentLeaderEpoch: cursorOffsetNext.currentLeaderEpoch,
					FetchOffset:        cursorOffsetNext.offset,
					LastFetchedEpoch:   -1,
					LogStartOffset:     -1,
					PartitionMaxBytes:  f.maxPartBytes,
				})
			}
		}
	}

	return req.AppendTo(dst)
}
func (*fetchRequest) ReadFrom([]byte) error {
	panic("unreachable -- the client never uses ReadFrom on its internal fetchRequest")
}
func (f *fetchRequest) ResponseKind() kmsg.Response {
	return &kmsg.FetchResponse{Version: f.version}
}

// fetchSessions, introduced in KIP-227, allow us to send less information back
// and forth to a Kafka broker. Rather than relying on forgotten topics to
// remove partitions from a session, we just simply reset the session.
type fetchSession struct {
	id    int32
	epoch int32

	used map[string]map[int32]fetchSessionOffsetEpoch // what we have in the session so far

	killed bool // if we cannot use a session anymore
}

func (s *fetchSession) kill() {
	s.id = 0
	s.epoch = -1
	s.used = nil
	s.killed = true
}

// reset resets the session by setting the next request to use epoch 0.
// We do not reset the ID; using epoch 0 for an existing ID unregisters the
// prior session.
func (s *fetchSession) reset() {
	if s.killed {
		return
	}
	s.epoch = 0
	s.used = nil
}

// bumpEpoch bumps the epoch and saves the session id.
//
// Kafka replies with the session ID of the session to use. When it does, we
// start from epoch 1, wrapping back to 1 if we go negative.
func (s *fetchSession) bumpEpoch(id int32) {
	if s.killed {
		return
	}
	if id != s.id {
		s.epoch = 0 // new session: reset to 0 for the increment below
	}
	s.epoch++
	if s.epoch < 0 {
		s.epoch = 1 // we wrapped: reset back to 1 to continue this session
	}
	s.id = id
}

func (s *fetchSession) lookupTopic(topic string) fetchSessionTopic {
	if s.killed {
		return nil
	}
	if s.used == nil {
		s.used = make(map[string]map[int32]fetchSessionOffsetEpoch)
	}
	t := s.used[topic]
	if t == nil {
		t = make(map[int32]fetchSessionOffsetEpoch)
		s.used[topic] = t
	}
	return t
}

type fetchSessionOffsetEpoch struct {
	offset int64
	epoch  int32
}

type fetchSessionTopic map[int32]fetchSessionOffsetEpoch

func (s fetchSessionTopic) hasPartitionAt(partition int32, offset int64, epoch int32) bool {
	if s == nil { // if we are nil, the session was killed
		return false
	}
	at, exists := s[partition]
	now := fetchSessionOffsetEpoch{offset, epoch}
	s[partition] = now
	return exists && at == now
}
