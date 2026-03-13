package kfake

import (
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ShareFetch: v0-2 (KIP-932, KIP-1206, KIP-1222)
//
// Behavior:
// * Acquires unread records from share-group partitions and returns them
// * Share sessions (similar to fetch sessions) track partition sets across requests
// * Piggybacked acknowledgements can be included in any ShareFetch request
// * Record acquisition locks are tracked per-member with configurable timeout
// * MaxWait long-poll: if no records available, waits up to MaxWaitMillis
//
// Session management:
// * epoch 0: Create new session, populate from request Topics
// * epoch >0: Incremental - Topics are ADDED, ForgottenTopicsData REMOVED
// * epoch -1: Close session, process final acks, release acquired records
// * Watcher re-invocation reuses existing session
//
// Version notes:
// * v0: Initial share fetch (KIP-932)
// * v1: BatchSize, ShareAcquireMode for BATCH_OPTIMIZED (KIP-1206)
// * v2: IsRenewAck for lock renewal without fetching (KIP-1222)

func init() { regKey(78, 0, 2) }

// tpKey identifies a (topicID, partition) pair for response dedup.
type tpKey struct {
	tid uuid
	p   int32
}

func (c *Cluster) handleShareFetch(creq *clientReq, w *watchShareFetch) (kmsg.Response, error) {
	var (
		req  = creq.kreq.(*kmsg.ShareFetchRequest)
		resp = req.ResponseKind().(*kmsg.ShareFetchResponse)
	)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	var groupID, memberID string
	if req.GroupID != nil {
		groupID = *req.GroupID
	}
	if req.MemberID != nil {
		memberID = *req.MemberID
	}

	resp.AcquisitionLockTimeoutMillis = c.shareRecordLockDurationMs()

	// ACL: require GROUP READ.
	if !c.allowedACL(creq, groupID, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationRead) {
		resp.ErrorCode = kerr.GroupAuthorizationFailed.Code
		return resp, nil
	}

	// Validate memberID format (real Kafka does NOT check group membership
	// here, only format: non-empty, <=36 chars).
	if memberID == "" || len(memberID) > 36 {
		resp.ErrorCode = kerr.InvalidRequest.Code
		return resp, nil
	}

	// KIP-1222: when isRenewAck is set, all fetch params must be zero.
	if req.Version >= 2 && req.IsRenewAck {
		if req.MaxBytes != 0 || req.MinBytes != 0 || req.MaxRecords != 0 || req.MaxWaitMillis != 0 {
			resp.ErrorCode = kerr.InvalidRequest.Code
			return resp, nil
		}
	}

	sg := c.shareGroups.gs[groupID]
	// id2t is safe to read directly: we're in run() and no admin
	// callbacks can mutate c.data concurrently within this handler.
	id2t := c.data.id2t
	maxDelivery := c.shareMaxDeliveryAttempts()

	maxAckType := int8(3) // v0-v1: Gap, Accept, Release, Reject
	if req.Version >= 2 && req.IsRenewAck {
		maxAckType = 4 // v2+ with IsRenewAck: also allow Renew
	}

	// Determine isolation level for this group (matching Kafka's
	// share.isolation.level group config, default READ_UNCOMMITTED).
	readCommitted := false
	if gc := c.groupConfigs[groupID]; gc != nil {
		if v := gc["share.isolation.level"]; v != nil && *v == "read_committed" {
			readCommitted = true
		}
	}

	sessionKey := shareSessionKey{
		group:    groupID,
		memberID: memberID,
		broker:   creq.cc.b.node,
	}

	// Response-building closures (matching produce handler style).
	topicIdx := make(map[uuid]int)
	addTopic := func(tid uuid) int {
		i, ok := topicIdx[tid]
		if !ok {
			i = len(resp.Topics)
			topicIdx[tid] = i
			t := kmsg.NewShareFetchResponseTopic()
			t.TopicID = tid
			resp.Topics = append(resp.Topics, t)
		}
		return i
	}
	donep := func(tid uuid, p int32, errCode int16) {
		idx := addTopic(tid)
		sp := kmsg.NewShareFetchResponseTopicPartition()
		sp.Partition = p
		sp.ErrorCode = errCode
		resp.Topics[idx].Partitions = append(resp.Topics[idx].Partitions, sp)
	}
	ackErrFn := func(tid uuid, p int32, ec int16) {
		if ec == 0 {
			return // success — fetch phase handles the response entry
		}
		idx := addTopic(tid)
		sp := kmsg.NewShareFetchResponseTopicPartition()
		sp.Partition = p
		sp.AcknowledgeErrorCode = ec
		resp.Topics[idx].Partitions = append(resp.Topics[idx].Partitions, sp)
	}

	// Session management.
	sgs := &c.shareGroups
	var session *shareSession
	if w != nil {
		// Watcher re-invocation: session was already validated.
		// If the session was overwritten by a new epoch-0 request,
		// this watcher is stale -- discard silently.
		session = sgs.sessions[sessionKey]
		if session == nil || session != w.session {
			return resp, nil
		}
	} else if req.ShareSessionEpoch == -1 {
		// Session close: process piggybacked acks, release remaining
		// acquired records, remove session (matching Kafka's
		// releaseSession).
		session = sgs.sessions[sessionKey]
		if session == nil {
			resp.ErrorCode = kerr.ShareSessionNotFound.Code
			return resp, nil
		}
		if sg != nil {
			ackTs := ackTopicsFromFetch(req.Topics)
			sg.mu.Lock()
			toFire, _ := c.processShareAcks(creq, sg, memberID, ackTs, maxAckType, ackErrFn)
			ensureAckedParts(resp, ackTs, addTopic)
			released := sg.releaseRecordsForMemberLocked(memberID, maxDelivery)
			sg.mu.Unlock()
			for _, pd := range toFire {
				fireShareWatchers(pd)
			}
			if released {
				sg.fireAllShareWatchers()
			}
		}
		delete(sgs.sessions, sessionKey)
		return resp, nil
	} else if req.ShareSessionEpoch == 0 {
		var ec int16
		session, sg, ec = sgs.createSession(sessionKey, req, creq.cc)
		if ec != 0 {
			resp.ErrorCode = ec
			return resp, nil
		}
	} else {
		var ec int16
		session, ec = sgs.updateSession(sessionKey, req.ShareSessionEpoch, req.Topics, req.ForgottenTopicsData)
		if ec != 0 {
			resp.ErrorCode = ec
			return resp, nil
		}
	}

	// After session validation, sg must be non-nil: epoch 0 creates it
	// above, and epoch > 0 / watcher paths require a prior epoch 0.
	if sg == nil {
		sg = c.shareGroups.getOrCreate(groupID)
	}

	// KIP-1222: when isRenewAck is set, skip fetch entirely -- only
	// process acks. Time spent fetching might exceed the renewed lock.
	if req.Version >= 2 && req.IsRenewAck {
		sg.mu.Lock()
		var toFire []*partData
		if w == nil {
			ackTs := ackTopicsFromFetch(req.Topics)
			toFire, _ = c.processShareAcks(creq, sg, memberID, ackTs, maxAckType, ackErrFn)
			ensureAckedParts(resp, ackTs, addTopic)
		}
		sg.mu.Unlock()
		for _, pd := range toFire {
			fireShareWatchers(pd)
		}
		session.epoch = max(1, session.epoch+1)
		return resp, nil
	}

	maxRecords := req.MaxRecords
	if maxRecords < 0 {
		maxRecords = 500 // sensible default
	}

	// BATCH_OPTIMIZED mode (ShareAcquireMode=0, KIP-1206): BatchSize
	// controls response splitting, not acquisition limit.
	batchSize := int32(0)
	if req.ShareAcquireMode == 0 && req.BatchSize > 0 {
		batchSize = req.BatchSize
	}

	type fetchTarget struct {
		topicID   uuid
		topic     string
		partition int32
		pd        *partData
	}
	type acquiredPart struct {
		topicID   uuid
		pd        *partData
		partition int32
		ranges    []kmsg.ShareFetchResponseTopicPartitionAcquiredRecord
	}

	var (
		totalRecords   int32
		targets        []fetchTarget
		acquiredParts  []acquiredPart
		includeBrokers bool
		hadAcks        bool
		ackToFire      []*partData
		maxRecordLocks = c.shareMaxRecordLocks()
	)

	// Lock the share group's partition state for ack processing and
	// record acquisition. Batch I/O happens after unlocking.
	sg.mu.Lock()

	// Process piggybacked acks first (only on initial invocation).
	var ackTs []ackTopic
	if w == nil {
		ackTs = ackTopicsFromFetch(req.Topics)
		ackToFire, hadAcks = c.processShareAcks(creq, sg, memberID, ackTs, maxAckType, ackErrFn)
	}

	// Build target list from session partitions.
	for topicID, parts := range session.partitions {
		topicName := id2t[topicID]
		if topicName == "" {
			for p := range parts {
				donep(topicID, p, kerr.UnknownTopicID.Code)
			}
			continue
		}
		if !c.allowedACL(creq, topicName, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationRead) {
			for p := range parts {
				donep(topicID, p, kerr.TopicAuthorizationFailed.Code)
			}
			continue
		}
		for p := range parts {
			pd, ok := c.data.tps.getp(topicName, p)
			if !ok {
				donep(topicID, p, kerr.UnknownTopicOrPartition.Code)
				continue
			}
			if pd.leader.node != creq.cc.b.node {
				idx := addTopic(topicID)
				sp := kmsg.NewShareFetchResponseTopicPartition()
				sp.Partition = p
				sp.ErrorCode = kerr.NotLeaderForPartition.Code
				sp.CurrentLeader.LeaderID = pd.leader.node
				sp.CurrentLeader.LeaderEpoch = pd.epoch
				resp.Topics[idx].Partitions = append(resp.Topics[idx].Partitions, sp)
				includeBrokers = true
				continue
			}
			targets = append(targets, fetchTarget{topicID, topicName, p, pd})
		}
	}

	// Rotate targets for fairness (matching Kafka's
	// PartitionRotateStrategy.ROUND_ROBIN using session epoch).
	if len(targets) > 1 && session.epoch > 0 {
		rotateAt := int(session.epoch) % len(targets)
		if rotateAt > 0 {
			rotated := make([]fetchTarget, len(targets))
			copy(rotated, targets[rotateAt:])
			copy(rotated[len(targets)-rotateAt:], targets[:rotateAt])
			targets = rotated
		}
	}

	// Acquire records from rotated targets.
	for _, tgt := range targets {
		remaining := maxRecords - totalRecords
		if remaining <= 0 {
			continue
		}
		shp := sg.getSharePartition(tgt.topic, tgt.partition, tgt.pd)
		hwm := tgt.pd.highWatermark
		if readCommitted {
			hwm = tgt.pd.lastStableOffset
		}
		acquiredRanges, _ := shp.acquireRecords(tgt.pd, hwm, remaining, &acquireOpts{
			memberID:         memberID,
			maxDeliveryCount: maxDelivery,
			maxRecordLocks:   maxRecordLocks,
			readCommitted:    readCommitted,
		})
		if len(acquiredRanges) == 0 {
			continue
		}
		if batchSize > 0 {
			acquiredRanges = splitAcquiredRanges(acquiredRanges, batchSize)
		}
		for _, ar := range acquiredRanges {
			totalRecords += int32(ar.LastOffset - ar.FirstOffset + 1)
		}
		acquiredParts = append(acquiredParts, acquiredPart{
			topicID:   tgt.topicID,
			pd:        tgt.pd,
			partition: tgt.partition,
			ranges:    acquiredRanges,
		})
	}

	sg.mu.Unlock()

	// Fire watchers outside the lock for ack-released records.
	for _, pd := range ackToFire {
		fireShareWatchers(pd)
	}

	// Read batch bytes outside the lock -- this may do disk I/O in
	// persistence mode and we don't want to block the sweep timer.
	for _, ap := range acquiredParts {
		firstAcq := ap.ranges[0].FirstOffset
		lastAcq := ap.ranges[len(ap.ranges)-1].LastOffset

		var rawBytes []byte
		segIdx, metaIdx, ok, atEnd := ap.pd.searchOffset(firstAcq)
		if ok && !atEnd {
			for si := segIdx; si < len(ap.pd.segments); si++ {
				seg := &ap.pd.segments[si]
				start := 0
				if si == segIdx {
					start = metaIdx
				}
				done := false
				for bi := start; bi < len(seg.index); bi++ {
					m := &seg.index[bi]
					if m.firstOffset > lastAcq {
						done = true
						break
					}
					raw, err := c.readBatchRaw(ap.pd, si, m)
					if err != nil {
						done = true
						break
					}
					rawBytes = append(rawBytes, raw...)
				}
				if done {
					break
				}
			}
		}

		idx := addTopic(ap.topicID)
		sp := kmsg.NewShareFetchResponseTopicPartition()
		sp.Partition = ap.partition
		sp.Records = rawBytes
		sp.AcquiredRecords = ap.ranges
		resp.Topics[idx].Partitions = append(resp.Topics[idx].Partitions, sp)
	}

	// Incremental response filtering (matching Java's
	// CachedSharePartition.maybeUpdateResponseData).
	filterIncrementalResponse(resp, session, addTopic)

	// Ensure all piggybacked ack partitions appear in the response.
	// The client uses partition presence to confirm ack processing.
	if len(ackTs) > 0 {
		ensureAckedParts(resp, ackTs, addTopic)
	}

	// If no records acquired and this is the initial invocation, consider
	// waiting for new data (MinBytes/MaxWait long-poll). Don't wait when
	// piggybacked acks were present (client needs ack confirmation
	// immediately, not after MaxWait delay). When blocked by
	// maxRecordLocks, we still create a watcher -- fireShareWatchers
	// fires when acks free the window, so the watcher wakes up promptly
	// instead of the client busy-looping with empty fetches.
	if totalRecords == 0 && w == nil && !hadAcks {
		wait := time.Duration(req.MaxWaitMillis) * time.Millisecond
		deadline := creq.at.Add(wait)
		remaining := time.Until(deadline)
		if remaining > 0 && len(targets) > 0 {
			wsf := &watchShareFetch{
				creq:    creq,
				session: session,
			}
			wsf.cb = func() {
				select {
				case c.shareGroups.watchFetchCh <- wsf:
				case <-c.die:
				}
			}
			for _, tgt := range targets {
				tgt.pd.shareWatch[wsf] = struct{}{}
				wsf.in = append(wsf.in, tgt.pd)
			}
			wsf.t = time.AfterFunc(remaining, wsf.cb)
			return nil, nil
		}
	}

	if includeBrokers {
		for _, b := range c.bs {
			ne := kmsg.NewShareFetchResponseNodeEndpoint()
			ne.NodeID = b.node
			ne.Host, ne.Port = b.hostport()
			ne.Rack = &brokerRack
			resp.NodeEndpoints = append(resp.NodeEndpoints, ne)
		}
	}

	session.epoch = max(1, session.epoch+1)
	return resp, nil
}

// filterIncrementalResponse updates requiresUpdate flags and adds empty
// entries for partitions that must appear even without data (matching
// Java's CachedSharePartition.maybeUpdateResponseData).
func filterIncrementalResponse(resp *kmsg.ShareFetchResponse, session *shareSession, addTopic func(uuid) int) {
	responded := make(map[tpKey]bool) // value: true if had error
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			responded[tpKey{t.TopicID, p.Partition}] = p.ErrorCode != 0
		}
	}
	for key, hadError := range responded {
		ps := session.partitions[key.tid]
		if ps == nil {
			continue
		}
		if csp := ps[key.p]; csp != nil {
			csp.requiresUpdate = hadError
		}
	}
	for tid, parts := range session.partitions {
		for p, csp := range parts {
			if csp == nil || !csp.requiresUpdate {
				continue
			}
			if _, ok := responded[tpKey{tid, p}]; ok {
				continue
			}
			idx := addTopic(tid)
			sp := kmsg.NewShareFetchResponseTopicPartition()
			sp.Partition = p
			resp.Topics[idx].Partitions = append(resp.Topics[idx].Partitions, sp)
			csp.requiresUpdate = false
		}
	}
}

// ensureAckedParts ensures every partition in ackTs appears in the response.
// The client uses partition presence to confirm ack processing; missing
// partitions cause "dropping piggybacked acks" warnings.
func ensureAckedParts(resp *kmsg.ShareFetchResponse, ackTs []ackTopic, addTopic func(uuid) int) {
	present := make(map[tpKey]struct{})
	for _, t := range resp.Topics {
		for _, p := range t.Partitions {
			present[tpKey{t.TopicID, p.Partition}] = struct{}{}
		}
	}
	for _, at := range ackTs {
		for _, ap := range at.partitions {
			key := tpKey{at.topicID, ap.partition}
			if _, ok := present[key]; ok {
				continue
			}
			idx := addTopic(at.topicID)
			sp := kmsg.NewShareFetchResponseTopicPartition()
			sp.Partition = ap.partition
			resp.Topics[idx].Partitions = append(resp.Topics[idx].Partitions, sp)
		}
	}
}

// splitAcquiredRanges splits acquired record ranges into sub-batches of at
// most batchSize offsets. Matches Java's SharePartition.createBatches for
// BATCH_OPTIMIZED mode (KIP-1206).
func splitAcquiredRanges(ranges []kmsg.ShareFetchResponseTopicPartitionAcquiredRecord, batchSize int32) []kmsg.ShareFetchResponseTopicPartitionAcquiredRecord {
	var out []kmsg.ShareFetchResponseTopicPartitionAcquiredRecord
	for _, r := range ranges {
		count := int32(r.LastOffset - r.FirstOffset + 1)
		if count <= batchSize {
			out = append(out, r)
			continue
		}
		for off := r.FirstOffset; off <= r.LastOffset; {
			end := min(off+int64(batchSize)-1, r.LastOffset)
			ar := kmsg.NewShareFetchResponseTopicPartitionAcquiredRecord()
			ar.FirstOffset = off
			ar.LastOffset = end
			ar.DeliveryCount = r.DeliveryCount
			out = append(out, ar)
			off = end + 1
		}
	}
	return out
}
