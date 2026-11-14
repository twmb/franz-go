package kfake

import (
	"slices"
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
// * MaxRecords and MaxBytes cap what one fetch acquires; zero of either
//   acquires nothing
//
// Session management:
// * epoch 0: Create new session, populate from request Topics
// * epoch >0: Incremental - Topics are ADDED, ForgottenTopicsData REMOVED
// * epoch -1: Close session, process final acks, release acquired records
// * Watcher re-invocation reuses existing session
//
// Version notes:
// * v0: Initial share fetch (KIP-932)
// * v1: MaxRecords, BatchSize
// * v2: ShareAcquireMode for BATCH_OPTIMIZED (KIP-1206), IsRenewAck for
//   lock renewal without fetching (KIP-1222)

func init() { regKey(78, 0, 2) }

func (c *Cluster) handleShareFetch(creq *clientReq, w *watchShareFetch) (kmsg.Response, error) {
	var (
		req  = creq.kreq.(*kmsg.ShareFetchRequest)
		resp = req.ResponseKind().(*kmsg.ShareFetchResponse)
	)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	if req.GroupID == nil {
		resp.ErrorCode = kerr.InvalidRequest.Code
		return resp, nil
	}
	groupID := *req.GroupID
	var memberID string
	if req.MemberID != nil {
		memberID = *req.MemberID
	}

	resp.AcquisitionLockTimeoutMillis = c.shareRecordLockDurationMs(groupID)
	fc := creq.faults

	// ACL: require GROUP READ.
	if e := c.deny(creq, groupID, kmsg.ACLResourceTypeGroup, kmsg.ACLOperationRead, faultKey{group: groupID}); e != nil {
		resp.ErrorCode = e.Code
		return resp, nil
	}

	// Validate memberID format (real Kafka does NOT check group membership
	// here, only format: non-empty, <=36 chars).
	if memberID == "" || len(memberID) > 36 {
		resp.ErrorCode = kerr.InvalidRequest.Code
		return resp, nil
	}

	// MaxRecords exists from v1 and cannot be negative. Zero is valid:
	// the fetch acquires nothing.
	if req.Version >= 1 && req.MaxRecords < 0 {
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

	// No share group under this id (none yet, a deleted one, or a
	// consumer group) means there is nothing to fetch from, and we do
	// not make one here. Kafka never asks the group coordinator here; its
	// share coordinator refuses the uninitialized partitions one by one
	// instead. We answer the same top-level GROUP_ID_NOT_FOUND that
	// ShareGroupHeartbeat does.
	sg := c.shareGroups.get(groupID)
	if sg == nil {
		resp.ErrorCode = kerr.GroupIDNotFound.Code
		return resp, nil
	}
	id2t := c.data.id2t
	maxDelivery := c.shareMaxDeliveryAttempts(groupID)

	maxAckType := shareAckReject
	if req.Version >= 2 && req.IsRenewAck {
		maxAckType = shareAckRenew
	}

	// Determine isolation level for this group (default READ_UNCOMMITTED).
	readCommitted := c.groupConfig(groupID, "share.isolation.level") == "read_committed"

	sessionKey := shareSessionKey{
		group:    groupID,
		memberID: memberID,
		broker:   creq.cc.b.node,
	}

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
	// Kafka answers a partition once: the result of a piggybacked ack
	// goes into the entry of the partition's fetch.
	partIdx := make(map[tpKey]int)
	donep := func(tid uuid, p int32, errCode int16) *kmsg.ShareFetchResponseTopicPartition {
		ps := &resp.Topics[addTopic(tid)].Partitions
		i, ok := partIdx[tpKey{tid, p}]
		if !ok {
			i = len(*ps)
			partIdx[tpKey{tid, p}] = i
			sp := kmsg.NewShareFetchResponseTopicPartition()
			sp.Partition = p
			*ps = append(*ps, sp)
		}
		sp := &(*ps)[i]
		if errCode != 0 {
			sp.ErrorCode = errCode
			// Kafka fills CurrentLeader only for leader errors, with
			// -1/-1 when it does not know the leader. Every other
			// partition gets 0/0.
			if errCode == kerr.NotLeaderForPartition.Code || errCode == kerr.FencedLeaderEpoch.Code {
				sp.CurrentLeader.LeaderID = -1
				sp.CurrentLeader.LeaderEpoch = -1
			}
		}
		return sp
	}
	// Kafka holds ack results until the fetch completes, so a fetch that
	// waits keeps its failed acks to answer them when it completes.
	var ackErrs map[tpKey]int16
	onAck := func(tid uuid, p int32, ec int16) {
		if ec == 0 {
			return // success - fetch phase handles the response entry
		}
		donep(tid, p, 0).AcknowledgeErrorCode = ec
		if ackErrs == nil {
			ackErrs = make(map[tpKey]int16)
		}
		ackErrs[tpKey{tid, p}] = ec
	}
	// Session management.
	sgs := &c.shareGroups
	var session *shareSession
	if w != nil {
		// Watcher re-invocation: session was already validated.
		// If the session was overwritten by a new epoch-0 request,
		// this watcher is stale -- return an empty response so the
		// client retries with a fresh epoch-0.
		session = sgs.sessions[sessionKey]
		if session == nil || session != w.session {
			return resp, nil
		}
	} else if req.ShareSessionEpoch == -1 {
		// Session close: process piggybacked acks, release remaining
		// acquired records for this session's partitions only (not all
		// partitions - the member may have other sessions on other brokers).
		session = sgs.sessions[sessionKey]
		if session == nil {
			resp.ErrorCode = kerr.ShareSessionNotFound.Code
			return resp, nil
		}
		ackTs := ackTopicsFromFetch(req.Topics)
		toFire := sg.processShareAcks(creq, memberID, ackTs, maxAckType, onAck)
		released := sg.releaseRecordsForSession(memberID, session)
		ensureAckedParts(resp, ackTs, addTopic)
		// fireAllShareWatchers fires every partition in the
		// group, which is a superset of the partitions in
		// toFire. When released is false, we only wake the
		// specific partitions that were ack-released.
		if released {
			sg.fireAllShareWatchers()
		} else {
			fireAll(toFire)
		}
		delete(sgs.sessions, sessionKey)
		return resp, nil
	} else if req.ShareSessionEpoch == 0 {
		var ec int16
		session, ec = sgs.createSession(sessionKey, req, creq.cc)
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

	// KIP-1222: when isRenewAck is set, skip fetch entirely -- only
	// process acks. Time spent fetching might exceed the renewed lock.
	if req.Version >= 2 && req.IsRenewAck {
		var toFire []*partData
		if w == nil {
			ackTs := ackTopicsFromFetch(req.Topics)
			toFire = sg.processShareAcks(creq, memberID, ackTs, maxAckType, onAck)
			ensureAckedParts(resp, ackTs, addTopic)
		}
		fireAll(toFire)
		return resp, nil
	}

	// Like Kafka, a fetch for zero records or zero bytes acquires
	// nothing and does not wait. v0 has no MaxRecords; we use the
	// client default.
	maxRecords := req.MaxRecords
	if req.Version == 0 {
		maxRecords = defShareMaxRecords
	}
	fetchNothing := maxRecords == 0 || req.MaxBytes == 0

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
		totalBytes     int64
		targets        []fetchTarget
		acquiredParts  []acquiredPart
		toFire         []*partData
		maxRecordLocks = c.shareMaxRecordLocks(groupID)
	)

	var ackTs []ackTopic
	if w == nil {
		ackTs = ackTopicsFromFetch(req.Topics)
	}

	// Process piggybacked acks first (skipped on watcher re-invocation,
	// since acks were already processed in the initial call). The
	// re-invocation answers the failed acks the initial call kept.
	if len(ackTs) > 0 {
		toFire = sg.processShareAcks(creq, memberID, ackTs, maxAckType, onAck)
	} else if w != nil {
		for k, ec := range w.ackErrs {
			onAck(k.tid, k.p, ec)
		}
	}

	// Build target list from session partitions.
	for topicID, parts := range session.partitions {
		topicName := id2t[topicID]
		if name := session.names[topicID]; topicName == "" && name != "" && c.data.t2id[name] == (uuid{}) {
			// Deleted since it joined the session, and not
			// recreated: like Kafka, we know it by the name and
			// answer UNKNOWN_TOPIC_OR_PARTITION below.
			topicName = name
		}
		if topicName == "" {
			for p := range parts {
				code := kerr.UnknownTopicID.Code
				if e := fc.check(faultKey{group: groupID, topicID: topicID}.part(p)); e != nil {
					code = e.Code
				}
				donep(topicID, p, code)
			}
			continue
		}
		tk := faultKey{group: groupID, topic: topicName, topicID: topicID}
		if e := c.deny(creq, topicName, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationRead, tk); e != nil {
			for p := range parts {
				donep(topicID, p, e.Code)
			}
			continue
		}
		for p := range parts {
			if e := fc.check(tk.part(p)); e != nil {
				donep(topicID, p, e.Code)
				continue
			}
			pd, ok := c.data.tps.getp(topicName, p)
			if !ok {
				donep(topicID, p, kerr.UnknownTopicOrPartition.Code)
				continue
			}
			if pd.leader.node != creq.cc.b.node {
				sp := donep(topicID, p, kerr.NotLeaderForPartition.Code)
				sp.CurrentLeader.LeaderID = pd.leader.node
				sp.CurrentLeader.LeaderEpoch = pd.epoch
				// Kafka sends the endpoint of each leader it names.
				if !slices.ContainsFunc(resp.NodeEndpoints, func(ne kmsg.ShareFetchResponseNodeEndpoint) bool { return ne.NodeID == pd.leader.node }) {
					ne := kmsg.NewShareFetchResponseNodeEndpoint()
					ne.NodeID = pd.leader.node
					ne.Host, ne.Port = pd.leader.hostport()
					ne.Rack = &brokerRack
					resp.NodeEndpoints = append(resp.NodeEndpoints, ne)
				}
				continue
			}
			targets = append(targets, fetchTarget{topicID, topicName, p, pd})
		}
	}

	// Fire watchers for ack-released records. They run after we return.
	fireAll(toFire)

	// park waits for records until MaxWait (the MinBytes/MaxWait
	// long-poll), if time and a partition to wait on are left, and
	// returns whether it parked. Like Kafka, a wake that frees nothing
	// for us waits again. Even when piggybacked acks were present, the
	// fetch may delay up to MaxWait (ack results are held until the
	// fetch completes or times out). When blocked by maxRecordLocks, we
	// still create a watcher -- fireShareWatchers fires when acks free
	// the window, so the watcher wakes up promptly instead of the client
	// busy-looping with empty fetches. We decide before building the
	// response, so the session's requiresUpdate flags are left for the
	// response that completes the fetch.
	remainingWait := time.Until(creq.at.Add(time.Duration(req.MaxWaitMillis) * time.Millisecond))
	canPark := !fetchNothing && !fc.anyHit() && remainingWait > 0 && len(targets) > 0 // a faulted partition completes the fetch at once
	park := func() bool {
		if !canPark {
			return false
		}
		wsf := &watchShareFetch{
			creq:    creq,
			session: session,
			ackTs:   ackTs,
			ackErrs: ackErrs,
		}
		if w != nil {
			wsf.ackTs = w.ackTs
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
		wsf.t = time.AfterFunc(remainingWait, wsf.cb)
		return true
	}

	// Like Kafka, we acquire nothing while the targets hold fewer than
	// MinBytes, at most MaxBytes, from where each next acquires, counting
	// at most an even share of MaxBytes for each. The first target also
	// counts the bytes the even split leaves over.
	if minBytes := int64(min(req.MinBytes, req.MaxBytes)); canPark && minBytes > 0 {
		var avail int64
		share, extra := int64(req.MaxBytes)/int64(len(targets)), int64(req.MaxBytes)%int64(len(targets))
		for _, tgt := range targets {
			if avail >= minBytes {
				break
			}
			shp := sg.getSharePartition(tgt.topic, tgt.partition, tgt.pd)
			end := tgt.pd.highWatermark
			if readCommitted {
				end = tgt.pd.lastStableOffset
			}
			avail += bytesFrom(tgt.pd, max(shp.scanOffset, shp.spso), end, min(share+extra, minBytes-avail))
			extra = 0
		}
		if avail < minBytes {
			park()
			return nil, nil
		}
	}

	// Rotate targets for fairness using session epoch as the
	// round-robin start index, avoiding a copy.
	startIdx := 0
	if n := len(targets); n > 1 && session.epoch > 0 {
		startIdx = int(session.epoch) % n
	}

	// Acquire records from rotated targets.
	for i := range targets {
		tgt := &targets[(startIdx+i)%len(targets)]
		remaining := maxRecords - totalRecords
		if remaining <= 0 || fetchNothing {
			continue
		}
		shp := sg.getSharePartition(tgt.topic, tgt.partition, tgt.pd)
		hwm := tgt.pd.highWatermark
		if readCommitted {
			hwm = tgt.pd.lastStableOffset
		}
		acquiredRanges, nbytes := shp.acquireRecords(
			tgt.pd,
			hwm,
			remaining,
			int64(req.MaxBytes)-totalBytes,
			totalBytes == 0,
			memberID,
			maxDelivery,
			maxRecordLocks,
			readCommitted,
			req.ShareAcquireMode == 0,
		)
		totalBytes += nbytes
		if len(acquiredRanges) == 0 {
			continue
		}
		if batchSize > 0 {
			acquiredRanges = splitAcquiredRanges(tgt.pd, acquiredRanges, batchSize)
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

	// With nothing acquired, wait for records.
	if totalRecords == 0 && park() {
		return nil, nil
	}

	for _, ap := range acquiredParts {
		firstAcq := ap.ranges[0].FirstOffset
		lastAcq := ap.ranges[len(ap.ranges)-1].LastOffset

		var rawBytes []byte
		segIdx, metaIdx, ok, atEnd := ap.pd.searchOffset(firstAcq)
		if ok && !atEnd {
		readBatches:
			for si := segIdx; si < len(ap.pd.segments); si++ {
				seg := &ap.pd.segments[si]
				start := 0
				if si == segIdx {
					start = metaIdx
				}
				for bi := start; bi < len(seg.index); bi++ {
					m := &seg.index[bi]
					if m.firstOffset > lastAcq {
						break readBatches
					}
					raw, err := c.readBatchRaw(ap.pd, si, m)
					if err != nil {
						break readBatches
					}
					rawBytes = append(rawBytes, raw...)
				}
			}
		}

		sp := donep(ap.topicID, ap.partition, 0)
		sp.Records = rawBytes
		sp.AcquiredRecords = ap.ranges
	}

	// Incremental response filtering: update session requiresUpdate
	// from response entries, then emit empty entries for session
	// partitions that still need update but weren't in the response.
	responded := make(map[tpKey]struct{})
	for _, t := range resp.Topics {
		ps := session.partitions[t.TopicID]
		for _, p := range t.Partitions {
			responded[tpKey{t.TopicID, p.Partition}] = struct{}{}
			if ps != nil {
				if _, ok := ps[p.Partition]; ok {
					ps[p.Partition] = p.ErrorCode != 0
				}
			}
		}
	}
	for tid, parts := range session.partitions {
		for p, needsUpdate := range parts {
			if !needsUpdate {
				continue
			}
			if _, ok := responded[tpKey{tid, p}]; ok {
				continue
			}
			donep(tid, p, 0)
			parts[p] = false
		}
	}

	// Ensure all piggybacked ack partitions appear in the response.
	// The client uses partition presence to confirm ack processing.
	// On watcher re-invocation, ackTs is nil (acks were already
	// processed), so use the watcher's saved ack topics instead.
	ensureAcks := ackTs
	if len(ensureAcks) == 0 && w != nil {
		ensureAcks = w.ackTs
	}
	if len(ensureAcks) > 0 {
		ensureAckedParts(resp, ensureAcks, addTopic)
	}

	return resp, nil
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

// splitAcquiredRanges splits each acquired range longer than batchSize for
// BATCH_OPTIMIZED mode (KIP-1206), as Kafka's createBatches does: a split
// happens only at a log batch base at least batchSize past the current
// range's start, so a log batch is never split.
func splitAcquiredRanges(pd *partData, ranges []kmsg.ShareFetchResponseTopicPartitionAcquiredRecord, batchSize int32) []kmsg.ShareFetchResponseTopicPartitionAcquiredRecord {
	var out []kmsg.ShareFetchResponseTopicPartitionAcquiredRecord
	add := func(first, last int64, dc int16) {
		ar := kmsg.NewShareFetchResponseTopicPartitionAcquiredRecord()
		ar.FirstOffset = first
		ar.LastOffset = last
		ar.DeliveryCount = dc
		out = append(out, ar)
	}
	for _, r := range ranges {
		if r.LastOffset-r.FirstOffset+1 <= int64(batchSize) {
			out = append(out, r)
			continue
		}
		cur := r.FirstOffset
		si, mi, ok, _ := pd.searchOffset(r.FirstOffset)
		for ok {
			base := pd.segments[si].index[mi].firstOffset
			if base > r.LastOffset {
				break
			}
			if base-cur >= int64(batchSize) {
				add(cur, base-1, r.DeliveryCount)
				cur = base
			}
			for mi++; ok && mi == len(pd.segments[si].index); {
				mi = 0
				si++
				ok = si < len(pd.segments)
			}
		}
		add(cur, r.LastOffset, r.DeliveryCount)
	}
	return out
}

// bytesFrom returns the bytes of the batches that hold offsets from from up
// to end, at most limit.
func bytesFrom(pd *partData, from, end, limit int64) int64 {
	segIdx, metaIdx, ok, atEnd := pd.searchOffset(max(from, pd.logStartOffset))
	if !ok || atEnd {
		return 0
	}
	var n int64
	for si := segIdx; si < len(pd.segments); si++ {
		seg := &pd.segments[si]
		for bi := metaIdx; bi < len(seg.index); bi++ {
			if n >= limit || seg.index[bi].firstOffset >= end {
				return min(n, limit)
			}
			n += int64(seg.index[bi].nbytes)
		}
		metaIdx = 0
	}
	return min(n, limit)
}
