package kgo

import (
	"fmt"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

type source struct {
	cl *Client // our owning client, for cfg, metadata triggering, context, etc.
	b  *broker // the broker this sink belongs to

	inflightSem chan struct{} // capacity of 1
	fillState   workLoop

	consecutiveFailures int

	// session supports fetch sessions as per KIP-227. This is updated
	// serially when creating a request and when handling a req response.
	// As such, modifications to it do not need to be under a lock.
	session fetchSession

	mu sync.Mutex // guards all below

	cursors      []*cursor // contains all partitions being consumed on this source
	cursorsStart int       // incremented every fetch req to ensure all partitions are fetched

	clearSessionBeforeNextFetch bool // set whenever a cursor is removed

	// buffered contains a fetch that the sink has received but that the
	// client user has not yet polled. On poll, partitions that actually
	// are not owned by the client anymore are removed before being
	// returned.
	buffered bufferedFetch
}

func newSource(
	cl *Client,
	b *broker,
) *source {
	return &source{
		cl: cl,
		b:  b,

		inflightSem: make(chan struct{}, 1),
	}
}

func (s *source) addCursor(add *cursor) {
	s.mu.Lock()
	add.cursorsIdx = len(s.cursors)
	s.cursors = append(s.cursors, add)
	s.mu.Unlock()

	// We always clear the failing, since this could have been from moving
	// a failing partition from one source to another (clearing the fail).
	add.clearFailing()
}

func (s *source) removeCursor(rm *cursor) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.clearSessionBeforeNextFetch = true

	if rm.cursorsIdx != len(s.cursors)-1 {
		s.cursors[rm.cursorsIdx], s.cursors[len(s.cursors)-1] =
			s.cursors[len(s.cursors)-1], nil

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
// All code on cursors takes special care to only work for the _current_
// assignment; this means that outdated requests that return cannot return old
// data. See the seqOffset field for more info.
type cursor struct {
	topic     string
	partition int32

	keepControl bool // whether to keep control records

	mu sync.Mutex

	source     *source
	cursorsIdx int

	leader           int32
	preferredReplica int32

	// seqOffset is our epoch/offset that we are consuming, with a
	// corresponding "seq" from group assignments / manual assignments.
	// When a fetch request is issued, we "freeze" a view of the offset
	// and only actually use the response (and update the cursor's
	// offset) if the consumer seq has yet changed.
	seqOffset

	// inUse is true whenever the cursor is chosen for an in flight
	// fetch request and reset to false if the fetch response has no usable
	// records or when the buffered usable records are taken.
	inUse bool

	// failing is true when we encounter a partition error.
	// It is always cleared on metadata update.
	failing bool

	// loadingOffsets is true when we are resetting offsets with
	// ListOffsets or with OffsetsForLeaderEpoch.
	//
	// This is unconditionally reset whenever assigning partitions
	// or when the requests mentioned in the prior sentence finish.
	loadingOffsets bool

	// needLoadEpoch is true when the cursor moved sources; if true in
	// createReq, then the source sets the epoch to be checked (similar to
	// what happens in FencedLeaderEpoch on response).
	//
	// We wait until createReq rather than checking immediately because the
	// cursor could still be in use / in flight on the old source during
	// the move.
	//
	// We do this regardless of the seq since epoch validation is strictly
	// beneficial; and worrying about the seq would make this unnecessarily
	// complicated.
	needLoadEpoch bool
}

func (c *cursor) maybeSetPreferredReplica(preferredReplica, currentLeader int32) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.leader != currentLeader {
		return false
	}

	c.source.cl.brokersMu.RLock()
	broker, exists := c.source.cl.brokers[preferredReplica]
	c.source.cl.brokersMu.RUnlock()

	if !exists {
		c.source.cl.triggerUpdateMetadataNow()
		return false
	}

	c.preferredReplica = preferredReplica
	c.source.removeCursor(c)
	c.source = broker.source
	c.source.addCursor(c)

	return true
}

// seqOffset tracks offsets/epochs with a cursor's seq.
type seqOffset struct {
	offset             int64
	lastConsumedEpoch  int32
	currentLeaderEpoch int32
	seq                uint64
}

// seqOffsetFrom is updated while processing a fetch response. Once the response
// is taken, we update the cursor's offset only if the seq is the same.
type seqOffsetFrom struct {
	seqOffset
	from *cursor

	currentLeader           int32
	currentPreferredReplica int32
}

// use, for fetch requests, freezes a view of the offset/epoch for the cursor's
// current seq. When the resulting fetch response is finally taken, we update
// the cursor's offset/epoch only if the cursor's seq is still the same.
func (c *cursor) use() *seqOffsetFrom {
	c.inUse = true
	return &seqOffsetFrom{
		seqOffset: c.seqOffset,
		from:      c,

		currentLeader:           c.leader,
		currentPreferredReplica: c.preferredReplica,
	}
}

// triggerConsume is called under the cursor's lock whenever the cursor maybe
// needs to be consumed.
func (c *cursor) triggerConsume() {
	if c.offset != -1 && c.source != nil { // source could be nil if we loaded a failing partition
		c.source.maybeConsume()
	}
}

// setOffset sets the cursors offset and seq, doing nothing if the seq is
// out of date.
func (c *cursor) setOffset(
	currentEpoch int32,
	clearLoading bool,
	offset int64,
	epoch int32,
	fromSeq uint64,
) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if fromSeq < c.seq {
		return
	}
	if fromSeq > c.seq {
		c.failing = false
		c.seq = fromSeq
	} else if currentEpoch < c.currentLeaderEpoch {
		// If the seq is the same but the current epoch is stale, this set
		// is from something out of date and a prior set bumped the epoch.
		return
	}
	c.inUse = false
	if clearLoading {
		c.loadingOffsets = false
	}

	c.offset = offset
	c.lastConsumedEpoch = epoch
	c.currentLeaderEpoch = currentEpoch

	c.triggerConsume()
}

// resetOffset is like setOffset, but strictly for bumping the cursor's seq.
func (c *cursor) resetOffset(fromSeq uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if fromSeq < c.seq {
		return
	}
	c.seq = fromSeq
	c.failing = false
	c.inUse = false
	c.loadingOffsets = false
	c.triggerConsume()
}

// restartOffset resets a cursor to usable and triggers the source to
// begin consuming again. This is only called in unuseAll, where a cursor
// was used in a fetch that ultimately returned no new data.
func (c *cursor) setUnused(fromSeq uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// fromSeq could be less than seq if this setOffset is from a
	// takeBuffered after assignment invalidation.
	if fromSeq < c.seq {
		return
	}

	c.inUse = false
	c.triggerConsume()
}

// setFailing is called once a partition has an error response. The cursor is
// not used until a metadata update clears the failing state.
func (c *cursor) setFailing(fromSeq uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if fromSeq < c.seq {
		return
	}
	c.failing = true
}

// clearFailing, called on metadata update or when a cursor is added to a source,
// clears any failing state.
func (c *cursor) clearFailing() {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.failing = false
	c.triggerConsume()
}

func (c *cursor) setLoadingOffsets(fromSeq uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.setLoadingOffsetsLocked(fromSeq)
}

func (c *cursor) setLoadingOffsetsLocked(fromSeq uint64) {
	if fromSeq < c.seq {
		return
	}
	c.loadingOffsets = true
}

// bufferedFetch is a fetch response waiting to be consumed by the client, as
// well as offsets to update cursors to once the fetch is taken.
type bufferedFetch struct {
	fetch      Fetch
	seq        uint64
	reqOffsets map[string]map[int32]*seqOffsetFrom
}

// takeBuffered drains a buffered fetch and updates offsets.
func (s *source) takeBuffered() (Fetch, uint64) {
	r := s.buffered
	s.buffered = bufferedFetch{}
	s.updateOffsets(r.reqOffsets)
	return r.fetch, r.seq
}

// updateOffsets is called when a buffered fetch is taken; we update all
// cursor offsets and set them usable for new fetches.
func (s *source) updateOffsets(reqOffsets map[string]map[int32]*seqOffsetFrom) {
	for _, partitions := range reqOffsets {
		for _, o := range partitions {
			o.from.setOffset(o.currentLeaderEpoch, false, o.offset, o.lastConsumedEpoch, o.seq)
		}
	}
	<-s.inflightSem
}

// unuseAll is called when a fetch returns no data; we set all the
// cursors to usable again so we can reissue a new fetch.
func (s *source) unuseAll(reqOffsets map[string]map[int32]*seqOffsetFrom) {
	for _, partitions := range reqOffsets {
		for _, o := range partitions {
			o.from.setUnused(o.seq)
		}
	}
	<-s.inflightSem
}

func (s *source) createReq() (req *fetchRequest, again bool) {
	req = &fetchRequest{
		maxWait:        s.cl.cfg.maxWait,
		maxBytes:       s.cl.cfg.maxBytes,
		maxPartBytes:   s.cl.cfg.maxPartBytes,
		rack:           s.cl.cfg.rack,
		isolationLevel: s.cl.cfg.isolationLevel,

		session: &s.session,
	}

	var reloadOffsets offsetsLoad
	defer func() {
		if !reloadOffsets.isEmpty() {
			reloadOffsets.mergeInto(&s.cl.consumer)
		}
	}()

	s.mu.Lock()
	defer s.mu.Unlock()

	if s.clearSessionBeforeNextFetch {
		s.session.reset()
		s.clearSessionBeforeNextFetch = false
	}

	cursorIdx := s.cursorsStart
	for i := 0; i < len(s.cursors); i++ {
		c := s.cursors[cursorIdx]
		cursorIdx = (cursorIdx + 1) % len(s.cursors)

		// Ensure this cursor cannot be moved across topicPartitions
		// while we using its fields.
		c.mu.Lock()

		// If the offset is -1, a metadata update added a cursor to
		// this s, but it is not yet in use.
		//
		// If we are in use or failing or loading, then we do not want
		// to use the cursor.
		if c.offset == -1 || c.inUse || c.failing || c.loadingOffsets {
			c.mu.Unlock()
			continue
		}

		// KIP-320: if we needLoadEpoch, we just migrated from one
		// broker to another. We need to validate the leader epoch on
		// the new broker to see if we experienced data loss before we
		// can use this cursor.
		//
		// However, we only do this if our last consumed epoch is >= 0,
		// otherwise we have not consumed anything at all and will just
		// rely on out of range errors.
		if c.needLoadEpoch {
			c.needLoadEpoch = false
			if c.lastConsumedEpoch >= 0 {
				c.setLoadingOffsetsLocked(c.seq)
				reloadOffsets.epoch.setLoadOffset(c.topic, c.partition, Offset{
					request:      c.offset,
					epoch:        c.lastConsumedEpoch,
					currentEpoch: c.currentLeaderEpoch,
				}, -1, req.maxSeq)
				c.mu.Unlock()
				continue
			}
		}

		again = true
		req.fetchCursorLocked(c)
		c.mu.Unlock()
	}

	// We could have lost our only record buffer just before we grabbed the
	// lock above.
	if len(s.cursors) > 0 {
		s.cursorsStart = (s.cursorsStart + 1) % len(s.cursors)
	}

	return req, again
}

func (s *source) maybeConsume() {
	if s.fillState.maybeBegin() {
		go s.fill()
	}
}

func (s *source) fill() {
	time.Sleep(time.Millisecond)

	again := true
	for again {
		s.inflightSem <- struct{}{}

		var req *fetchRequest
		req, again = s.createReq()

		if req.numOffsets == 0 { // must be at least one if a cursor was usable
			again = s.fillState.maybeFinish(again)
			<-s.inflightSem
			continue
		}

		s.b.do(
			s.cl.ctx,
			req,
			func(resp kmsg.Response, err error) {
				s.handleReqResp(req, resp, err)
			},
		)
		again = s.fillState.maybeFinish(again)
	}
}

func (s *source) backoff() {
	s.consecutiveFailures++
	after := time.NewTimer(s.cl.cfg.retryBackoff(s.consecutiveFailures))
	defer after.Stop()
	select {
	case <-after.C:
	case <-s.cl.ctx.Done():
	}
}

func (s *source) handleReqResp(req *fetchRequest, kresp kmsg.Response, err error) {
	if err != nil {
		s.backoff() // backoff before unuseAll to avoid inflight race
		s.unuseAll(req.offsets)
		s.cl.triggerUpdateMetadata()
		return
	}
	s.consecutiveFailures = 0

	resp := kresp.(*kmsg.FetchResponse)

	// If our session errored, we reset the session and retry the request
	// without delay.
	switch err := kerr.ErrorForCode(resp.ErrorCode); err {
	case kerr.FetchSessionIDNotFound:
		// If the session ID is not found, we may have had our session
		// evicted or we may not be able to establish a session because
		// the broker is maxed out and our session would be worse than
		// existing ones.
		//
		// We never try to establish a session again.
		if s.session.epoch == 0 {
			s.session.id = -1
			s.session.epoch = -1
			s.cl.cfg.logger.Log(LogLevelInfo,
				"session failed with SessionIDNotFound while trying to establish a session; broker likely maxed on sessions; continuing on without using sessions")
		}
		fallthrough
	case kerr.InvalidFetchSessionEpoch:
		if s.session.id != -1 {
			s.cl.cfg.logger.Log(LogLevelInfo, "resetting fetch session", "err", err)
			s.session.reset()
		}
		s.unuseAll(req.offsets)
		return
	}

	s.session.bumpEpoch(resp.SessionID)

	newFetch := Fetch{
		Topics: make([]FetchTopic, 0, len(resp.Topics)),
	}

	// If any partition errors with OffsetOutOfRange, we reload the offset
	// for that partition a per to the client's configured offset policy.
	var reloadOffsets offsetsLoad
	var needsMetaUpdate bool
	for _, rTopic := range resp.Topics {
		topic := rTopic.Topic
		topicOffsets, ok := req.offsets[topic]
		if !ok {
			continue
		}

		fetchTopic := FetchTopic{
			Topic:      topic,
			Partitions: make([]FetchPartition, 0, len(rTopic.Partitions)),
		}

		for i := range rTopic.Partitions {
			rPartition := &rTopic.Partitions[i]
			partition := rPartition.Partition
			partOffset, ok := topicOffsets[partition]
			if !ok {
				continue
			}

			fetchPart, partNeedsMetaUpdate, migrating := partOffset.processRespPartition(topic, resp.Version, rPartition, s.cl.decompressor, &s.session)
			if migrating {
				continue
			}

			fetchTopic.Partitions = append(fetchTopic.Partitions, fetchPart)
			needsMetaUpdate = needsMetaUpdate || partNeedsMetaUpdate

			// If we are out of range, we reset to what we can.
			// With Kafka >= 2.1.0, we should only get offset out
			// of range if we fetch before the start, but a user
			// user could start past the end and want to reset to
			// the end. We respect that.
			//
			// KIP-392 (case 3) specifies that if we are consuming
			// from a follower, then if our offset request is
			// before the low watermark, we list offsets from the
			// follower.
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
			// we will set in a cycle of validating the leader
			// epoch until the follower has caught up.
			if fetchPart.Err == kerr.OffsetOutOfRange {
				partOffset.from.setLoadingOffsets(partOffset.seq)
				if partOffset.currentPreferredReplica == -1 {
					reloadOffsets.list.setLoadOffset(topic, partition, s.cl.cfg.resetOffset, -1, req.maxSeq)
				} else if partOffset.offset < fetchPart.LogStartOffset {
					reloadOffsets.list.setLoadOffset(topic, partition, s.cl.cfg.resetOffset, s.b.meta.NodeID, req.maxSeq)
				} else { // partOffset.offset > fetchPart.HighWatermark
					reloadOffsets.epoch.setLoadOffset(topic, partition, Offset{
						request:      partOffset.offset,
						epoch:        partOffset.lastConsumedEpoch,
						currentEpoch: partOffset.currentLeaderEpoch,
					}, -1, req.maxSeq)
				}

			} else if fetchPart.Err == kerr.FencedLeaderEpoch {
				// With fenced leader epoch, we notify an error only if
				// necessary after we find out if loss occurred.
				fetchPart.Err = nil
				// If we have consumed nothing, then we got unlucky
				// by being fenced right after we grabbed metadata.
				// We just refresh metadata and try again.
				if partOffset.lastConsumedEpoch >= 0 {
					partOffset.from.setLoadingOffsets(partOffset.seq)
					reloadOffsets.epoch.setLoadOffset(topic, partition, Offset{
						request:      partOffset.offset,
						epoch:        partOffset.lastConsumedEpoch,
						currentEpoch: partOffset.currentLeaderEpoch,
					}, -1, req.maxSeq)
				}
			}
		}

		if len(fetchTopic.Partitions) > 0 {
			newFetch.Topics = append(newFetch.Topics, fetchTopic)
		}
	}

	if !reloadOffsets.isEmpty() {
		reloadOffsets.mergeInto(&s.cl.consumer)
	}

	if needsMetaUpdate {
		s.cl.triggerUpdateMetadataNow()
		s.cl.cfg.logger.Log(LogLevelInfo, "fetch had a partition error; trigging metadata update and resetting the session")
		s.session.reset()
	}

	if len(newFetch.Topics) > 0 {
		s.buffered = bufferedFetch{
			fetch:      newFetch,
			seq:        req.maxSeq,
			reqOffsets: req.offsets,
		}

		s.cl.consumer.addSourceReadyForDraining(req.maxSeq, s)
	} else {
		s.updateOffsets(req.offsets)
	}
}

// processRespPartition processes all records in all potentially compressed
// batches (or message sets) and returns a fetch partition containing those
// records.
//
// This returns that a metadata update is needed if any part has a recoverable
// error.
//
// Recoverable errors are stripped; if a partition has a recoverable error
// with no records, it should be discarded.
func (o *seqOffsetFrom) processRespPartition(
	topic string,
	version int16,
	rPartition *kmsg.FetchResponseTopicPartition,
	decompressor *decompressor,
	session *fetchSession,
) (
	fetchPart FetchPartition,
	needsMetaUpdate bool,
	migrating bool,
) {
	fetchPart = FetchPartition{
		Partition:        rPartition.Partition,
		Err:              kerr.ErrorForCode(rPartition.ErrorCode),
		HighWatermark:    rPartition.HighWatermark,
		LastStableOffset: rPartition.LastStableOffset,
		LogStartOffset:   rPartition.LogStartOffset,
	}

	// If we are fetching from the replica already, Kafka replies with a -1
	// preferred read replica. If Kafka replies with a preferred replica,
	// it sends no records.
	preferredReplica := rPartition.PreferredReadReplica
	if version >= 11 && preferredReplica >= 0 {
		if o.currentPreferredReplica != preferredReplica {
			o.from.setUnused(o.seq)
			if didSet := o.from.maybeSetPreferredReplica(preferredReplica, o.currentLeader); didSet {
				return FetchPartition{}, false, true
			}
		}
	}

	keepControl := o.from.keepControl

	switch version {
	case 0, 1:
		msgs, err := kmsg.ReadV0Messages(rPartition.RecordBatches)
		if err != nil {
			fetchPart.Err = err
		}
		o.processV0Messages(topic, &fetchPart, msgs, decompressor)
	case 2, 3:
		msgs, err := kmsg.ReadV1Messages(rPartition.RecordBatches)
		if err != nil {
			fetchPart.Err = err
		}
		o.processV1Messages(topic, &fetchPart, msgs, decompressor)
	default:
		batches, err := kmsg.ReadRecordBatches(rPartition.RecordBatches)
		if err != nil {
			fetchPart.Err = err
		}
		var numPartitionRecords int
		for i := range batches {
			numPartitionRecords += int(batches[i].NumRecords)
		}
		fetchPart.Records = make([]*Record, 0, numPartitionRecords)
		aborter := buildAborter(rPartition)
		for i := range batches {
			o.processRecordBatch(topic, &fetchPart, &batches[i], keepControl, aborter, decompressor)
			if fetchPart.Err != nil {
				break
			}
		}
	}

	if fetchPart.Err != nil {
		needsMetaUpdate = true
	}

	switch fetchPart.Err {
	case nil:
		// do nothing

	case kerr.UnknownTopicOrPartition,
		kerr.NotLeaderForPartition,
		kerr.ReplicaNotAvailable,
		kerr.KafkaStorageError,
		kerr.UnknownLeaderEpoch, // our meta is newer than broker we fetched from
		kerr.OffsetNotAvailable: // fetched from out of sync replica or a behind in-sync one (KIP-392: case 1 and case 2)

		fetchPart.Err = nil // recoverable with client backoff; hide the error
		fallthrough

	default:
		// - bad auth
		// - unsupported compression
		// - unsupported message version
		// - unknown error
		o.from.setFailing(o.seq)
	}

	return fetchPart, needsMetaUpdate, false
}

type aborter map[int64][]int64

func buildAborter(rPartition *kmsg.FetchResponseTopicPartition) aborter {
	if len(rPartition.AbortedTransactions) == 0 {
		return nil
	}
	a := make(aborter)
	for _, abort := range rPartition.AbortedTransactions {
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

func (o *seqOffset) processRecordBatch(
	topic string,
	fetchPart *FetchPartition,
	batch *kmsg.RecordBatch,
	keepControl bool,
	aborter aborter,
	decompressor *decompressor,
) {
	if batch.Magic != 2 {
		fetchPart.Err = fmt.Errorf("unknown batch magic %d", batch.Magic)
		return
	}
	rawRecords := batch.Records
	if compression := byte(batch.Attributes & 0x0007); compression != 0 {
		var err error
		if rawRecords, err = decompressor.decompress(rawRecords, compression); err != nil {
			fetchPart.Err = fmt.Errorf("unable to decompress batch: %v", err)
			return
		}
	}
	krecords, err := kmsg.ReadRecords(int(batch.NumRecords), rawRecords)
	if err != nil {
		fetchPart.Err = fmt.Errorf("invalid record batch: %v", err)
		return
	}

	abortBatch := aborter.shouldAbortBatch(batch)
	var lastRecord *Record
	for i := range krecords {
		record := recordToRecord(
			topic,
			fetchPart.Partition,
			batch,
			&krecords[i],
		)
		lastRecord = record
		o.maybeAddRecord(fetchPart, record, keepControl, abortBatch)
	}

	if abortBatch && lastRecord != nil && lastRecord.Attrs.IsControl() {
		aborter.trackAbortedPID(batch.ProducerID)
	}
}

func (o *seqOffset) processV1Messages(
	topic string,
	fetchPart *FetchPartition,
	messages []kmsg.MessageV1,
	decompressor *decompressor,
) {
	for i := range messages {
		message := &messages[i]
		compression := byte(message.Attributes & 0x0003)
		if compression == 0 {
			if !o.processV1Message(topic, fetchPart, message) {
				return
			}
			continue
		}

		rawMessages, err := decompressor.decompress(message.Value, compression)
		if err != nil {
			fetchPart.Err = fmt.Errorf("unable to decompress messages: %v", err)
			return
		}
		innerMessages, err := kmsg.ReadV1Messages(rawMessages)
		if err != nil {
			fetchPart.Err = err
		}
		if len(innerMessages) == 0 {
			return
		}
		firstOffset := message.Offset - int64(len(innerMessages)) + 1
		for i := range innerMessages {
			innerMessage := &innerMessages[i]
			innerMessage.Offset = firstOffset + int64(i)
			if !o.processV1Message(topic, fetchPart, innerMessage) {
				return
			}
		}
	}
}

func (o *seqOffset) processV1Message(
	topic string,
	fetchPart *FetchPartition,
	message *kmsg.MessageV1,
) bool {
	if message.Magic != 1 {
		fetchPart.Err = fmt.Errorf("unknown message magic %d", message.Magic)
		return false
	}
	if message.Attributes != 0 {
		fetchPart.Err = fmt.Errorf("unknown attributes on uncompressed message %d", message.Attributes)
		return false
	}
	record := v1MessageToRecord(topic, fetchPart.Partition, message)
	o.maybeAddRecord(fetchPart, record, false, false)
	return true
}

func (o *seqOffset) processV0Messages(
	topic string,
	fetchPart *FetchPartition,
	messages []kmsg.MessageV0,
	decompressor *decompressor,
) {
	for i := range messages {
		message := &messages[i]
		compression := byte(message.Attributes & 0x0003)
		if compression == 0 {
			if !o.processV0Message(topic, fetchPart, message) {
				return
			}
			continue
		}

		rawMessages, err := decompressor.decompress(message.Value, compression)
		if err != nil {
			fetchPart.Err = fmt.Errorf("unable to decompress messages: %v", err)
			return
		}
		innerMessages, err := kmsg.ReadV0Messages(rawMessages)
		if err != nil {
			fetchPart.Err = err
		}
		if len(innerMessages) == 0 {
			return
		}
		firstOffset := message.Offset - int64(len(innerMessages)) + 1
		for i := range innerMessages {
			innerMessage := &innerMessages[i]
			innerMessage.Offset = firstOffset + int64(i)
			if !o.processV0Message(topic, fetchPart, innerMessage) {
				return
			}
		}
	}
}

func (o *seqOffset) processV0Message(
	topic string,
	fetchPart *FetchPartition,
	message *kmsg.MessageV0,
) bool {
	if message.Magic != 0 {
		fetchPart.Err = fmt.Errorf("unknown message magic %d", message.Magic)
		return false
	}
	if message.Attributes != 0 {
		fetchPart.Err = fmt.Errorf("unknown attributes on uncompressed message %d", message.Attributes)
		return false
	}
	record := v0MessageToRecord(topic, fetchPart.Partition, message)
	o.maybeAddRecord(fetchPart, record, false, false)
	return true
}

// maybeAddRecord keeps a record if it is within our range of offsets to keep.
// However, if the record is being aborted, or the record is a control record
// and the client does not want to keep control records, this does not keep
// the record and instead only updates the seqOffset metadata.
func (o *seqOffset) maybeAddRecord(fetchPart *FetchPartition, record *Record, keepControl bool, abort bool) {
	if record.Offset < o.offset {
		// We asked for offset 5, but that was in the middle of a
		// batch; we got offsets 0 thru 4 that we need to skip.
		return
	}

	// We only keep control records if specifically requested.
	if record.Attrs.IsControl() && !keepControl {
		abort = true
	}
	if !abort {
		fetchPart.Records = append(fetchPart.Records, record)
	}

	// The record offset may be much larger than our expected offset if the
	// topic is compacted. That is fine; we ensure increasing offsets and
	// only keep the resulting offset if the seq is the same.
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
		Key:         record.Key,
		Value:       record.Value,
		Headers:     h,
		Timestamp:   timeFromMillis(batch.FirstTimestamp + int64(record.TimestampDelta)),
		Attrs:       RecordAttrs{uint8(batch.Attributes)},
		Topic:       topic,
		Partition:   partition,
		LeaderEpoch: batch.PartitionLeaderEpoch,
		Offset:      batch.FirstOffset + int64(record.OffsetDelta),
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
		Key:         message.Key,
		Value:       message.Value,
		Attrs:       messageAttrsToRecordAttrs(message.Attributes, true),
		Topic:       topic,
		Partition:   partition,
		LeaderEpoch: -1,
		Offset:      message.Offset,
	}
}

func v1MessageToRecord(
	topic string,
	partition int32,
	message *kmsg.MessageV1,
) *Record {
	return &Record{
		Key:         message.Key,
		Value:       message.Value,
		Timestamp:   timeFromMillis(message.Timestamp),
		Attrs:       messageAttrsToRecordAttrs(message.Attributes, false),
		Topic:       topic,
		Partition:   partition,
		LeaderEpoch: -1,
		Offset:      message.Offset,
	}
}

//////////////////
// fetchRequest //
//////////////////

type fetchRequest struct {
	version      int16
	maxWait      int32
	maxBytes     int32
	maxPartBytes int32
	rack         string

	isolationLevel int8

	maxSeq     uint64
	numOffsets int
	offsets    map[string]map[int32]*seqOffsetFrom

	session *fetchSession
}

func (f *fetchRequest) fetchCursorLocked(c *cursor) {
	if f.offsets == nil {
		f.offsets = make(map[string]map[int32]*seqOffsetFrom)
	}
	partitions := f.offsets[c.topic]
	if partitions == nil {
		partitions = make(map[int32]*seqOffsetFrom)
		f.offsets[c.topic] = partitions
	}
	o := c.use()

	// AssignPartitions or AssignGroup could have been called in the middle
	// of our req being built, invalidating part of the req. We invalidate
	// by only tracking the latest seq.
	if o.seq > f.maxSeq {
		f.maxSeq = o.seq
		f.numOffsets = 0
	}
	f.numOffsets++
	partitions[c.partition] = o

	if o.seq > f.session.seq {
		f.session.resetWithSeq(o.seq)
	}
}

func (*fetchRequest) Key() int16           { return 1 }
func (*fetchRequest) MaxVersion() int16    { return 11 }
func (f *fetchRequest) SetVersion(v int16) { f.version = v }
func (f *fetchRequest) GetVersion() int16  { return f.version }
func (f *fetchRequest) IsFlexible() bool   { return false } // version 11 is not flexible
func (f *fetchRequest) AppendTo(dst []byte) []byte {
	req := kmsg.FetchRequest{
		Version:        f.version,
		ReplicaID:      -1,
		MaxWaitMillis:  f.maxWait,
		MinBytes:       1,
		MaxBytes:       f.maxBytes,
		IsolationLevel: f.isolationLevel,
		SessionID:      f.session.id,
		SessionEpoch:   f.session.epoch,
		Topics:         make([]kmsg.FetchRequestTopic, 0, len(f.offsets)),
		Rack:           f.rack,
	}

	for topic, partitions := range f.offsets {
		req.Topics = append(req.Topics, kmsg.FetchRequestTopic{
			Topic:      topic,
			Partitions: make([]kmsg.FetchRequestTopicPartition, 0, len(partitions)),
		})
		reqTopic := &req.Topics[len(req.Topics)-1]

		if f.session.used == nil {
			f.session.used = make(map[string]map[int32]EpochOffset)
		}
		partsInSession := f.session.used[topic]
		if partsInSession == nil {
			partsInSession = make(map[int32]EpochOffset)
			f.session.used[topic] = partsInSession
		}

		for partition, seqOffset := range partitions {
			if seqOffset.seq < f.maxSeq {
				continue // all offsets in the fetch must come from the same seq
			}

			sessionOffset, partInSession := partsInSession[partition]
			epochOffset := EpochOffset{
				Epoch:  seqOffset.currentLeaderEpoch,
				Offset: seqOffset.offset,
			}

			if !partInSession || sessionOffset != epochOffset {
				reqTopic.Partitions = append(reqTopic.Partitions, kmsg.FetchRequestTopicPartition{
					Partition:          partition,
					CurrentLeaderEpoch: seqOffset.currentLeaderEpoch,
					FetchOffset:        seqOffset.offset,
					LogStartOffset:     -1,
					PartitionMaxBytes:  f.maxPartBytes,
				})
				partsInSession[partition] = epochOffset
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
	// seq corresponds to the max seq in the last fetch request.
	// Whenever this is bumped, we reset the session.
	seq uint64

	id    int32
	epoch int32

	// used is what we used last in the session. If we issue a fetch
	// request and the partition we want to fetch is for the same
	// offset/epoch as the last fetch, we elide writing the partition.
	used map[string]map[int32]EpochOffset
}

// resetWithSeq bumps the fetchSession's seq and resets the session.
func (s *fetchSession) resetWithSeq(seq uint64) {
	s.seq = seq
	s.reset()
}

// reset resets the session by setting the next request to use epoch 0.
func (s *fetchSession) reset() {
	s.epoch = 0
	s.used = nil
}

// bumpEpoch bumps the epoch and saves the session id.
func (s *fetchSession) bumpEpoch(id int32) {
	s.epoch++
	if s.epoch < 0 {
		s.epoch = 1
	}
	s.id = id
}
