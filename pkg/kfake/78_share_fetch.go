package kfake

import (
	"math"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ShareFetch: v0-2 (KIP-932, KIP-1206, KIP-1222)

func init() { regKey(78, 0, 2) }

func (c *Cluster) handleShareFetch(creq *clientReq, w *watchShareFetch) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.ShareFetchRequest)
	resp := req.ResponseKind().(*kmsg.ShareFetchResponse)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	groupID := ""
	if req.GroupID != nil {
		groupID = *req.GroupID
	}
	memberID := ""
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

	// Look up existing share group. Unlike ShareGroupHeartbeat (which
	// creates on join) or AlterShareGroupOffsets (which creates to store
	// state), ShareFetch should not create a group -- Java's ShareFetch
	// operates via SharePartitionManager without creating a group
	// coordinator entry. We defer to getOrCreate only at epoch 0 (new
	// session) to ensure partition state has a home. For epoch -1 and
	// >0 the group must already exist (the session was opened earlier).
	sg := c.shareGroups.gs[groupID]

	id2t := c.data.id2t
	supportsRenew := req.Version >= 2
	maxDelivery := c.shareMaxDeliveryAttempts()

	// Determine isolation level for this group (matching Kafka's
	// share.isolation.level group config, default READ_UNCOMMITTED).
	readCommitted := false
	if gc := c.groupConfigs[groupID]; gc != nil {
		if v := gc["share.isolation.level"]; v != nil && *v == "read_committed" {
			readCommitted = true
		}
	}

	// Handle session management (skip on watcher re-invocation).
	sessionKey := shareSessionKey{
		group:    groupID,
		memberID: memberID,
		broker:   creq.cc.b.node,
	}

	type tpKey struct {
		tid uuid
		p   int32
	}
	var session *shareSession
	if w != nil {
		// Watcher re-invocation: session was already validated.
		// If the session was overwritten by a new epoch-0 request,
		// this watcher is stale -- discard silently.
		session = c.shareSessions[sessionKey]
		if session == nil || session != w.session {
			return resp, nil
		}
	} else {
		if req.ShareSessionEpoch == -1 {
			// Close session. Validate it exists first (matching
			// Kafka's SharePartitionManager behavior).
			session := c.shareSessions[sessionKey]
			if session == nil {
				resp.ErrorCode = kerr.ShareSessionNotFound.Code
				return resp, nil
			}
			// Process piggybacked acks, then release remaining
			// acquired records (matching Kafka's releaseSession).
			if sg != nil {
				sg.mu.Lock()
				sf := &shareFetchCtx{
					c: c, creq: creq, sg: sg, req: req, resp: resp,
					topicIdx: make(map[uuid]int),
					memberID: memberID, id2t: id2t, supportsRenew: supportsRenew, maxDelivery: maxDelivery,
				}
				sf.processAcksLocked()
				sf.ensureAckPartsInResponse()
				released := sg.releaseRecordsForMemberLocked(memberID, maxDelivery)
				sg.mu.Unlock()
				if released {
					sg.fireAllShareWatchers(c)
				}
			}
			delete(c.shareSessions, sessionKey)
			resp.AcquisitionLockTimeoutMillis = 0
			return resp, nil
		}

		session = c.shareSessions[sessionKey]
		if req.ShareSessionEpoch == 0 {
			// Create the share group on epoch 0 so partition
			// state has a home during acquisition.
			sg = c.shareGroups.getOrCreate(groupID)
			// Reject piggybacked acks BEFORE creating session
			// (matching Kafka: checks before maybeCreateSession).
			for i := range req.Topics {
				for j := range req.Topics[i].Partitions {
					if len(req.Topics[i].Partitions[j].AcknowledgementBatches) > 0 {
						resp.ErrorCode = kerr.InvalidRequest.Code
						return resp, nil
					}
				}
			}
			// Remove old session before capacity check so that
			// re-creating an existing session doesn't hit the limit
			// (matching Kafka's cache.remove(key) before maybeCreateSession).
			delete(c.shareSessions, sessionKey)
			// Check session capacity (matching Kafka's
			// ShareSessionCache.maybeCreateSession).
			if int32(len(c.shareSessions)) >= c.shareMaxShareSessions() {
				resp.ErrorCode = kerr.ShareSessionLimitReached.Code
				return resp, nil
			}
			// New session -- start at epoch 0; the increment at the
			// bottom of this handler advances it to 1 for the next
			// request. Populate session partitions from request Topics.
			session = &shareSession{
				epoch:      0,
				partitions: make(map[uuid]map[int32]*cachedSharePart),
				cc:         creq.cc,
			}
			for i := range req.Topics {
				rt := &req.Topics[i]
				ps := session.partitions[rt.TopicID]
				if ps == nil {
					ps = make(map[int32]*cachedSharePart)
					session.partitions[rt.TopicID] = ps
				}
				for j := range rt.Partitions {
					ps[rt.Partitions[j].Partition] = &cachedSharePart{requiresUpdate: true}
				}
			}
			c.shareSessions[sessionKey] = session
			// Watch the connection for disconnect cleanup
			// (matching Kafka's ConnectionDisconnectListener).
			if _, watched := c.shareConnWatch[creq.cc]; !watched {
				c.shareConnWatch[creq.cc] = struct{}{}
				cc := creq.cc
				go func() {
					select {
					case <-cc.done:
						select {
						case c.shareDisconnCh <- cc:
						case <-c.die:
						}
					case <-c.die:
					}
				}()
			}
		} else if session == nil {
			resp.ErrorCode = kerr.ShareSessionNotFound.Code
			return resp, nil
		} else if req.ShareSessionEpoch != session.epoch {
			resp.ErrorCode = kerr.InvalidShareSessionEpoch.Code
			return resp, nil
		} else {
			// Incremental session (epoch > 0): merge request
			// Topics into session (ADD), remove ForgottenTopicsData.
			// New partitions get requiresUpdate=true so they appear
			// in the response even with no data (matching Kafka's
			// CachedSharePartition.requiresUpdateInResponse).
			for i := range req.Topics {
				rt := &req.Topics[i]
				ps := session.partitions[rt.TopicID]
				if ps == nil {
					ps = make(map[int32]*cachedSharePart)
					session.partitions[rt.TopicID] = ps
				}
				for j := range rt.Partitions {
					p := rt.Partitions[j].Partition
					if ps[p] == nil {
						ps[p] = &cachedSharePart{requiresUpdate: true}
					}
				}
			}
			for i := range req.ForgottenTopicsData {
				ft := &req.ForgottenTopicsData[i]
				ps := session.partitions[ft.TopicID]
				if ps == nil {
					continue
				}
				for _, p := range ft.Partitions {
					delete(ps, p)
				}
				if len(ps) == 0 {
					delete(session.partitions, ft.TopicID)
				}
			}
		}
	}

	// After session validation, sg must be non-nil: epoch 0 creates it
	// above, and epoch > 0 / watcher paths require a prior epoch 0.
	// Defensive fallback in case of unexpected state.
	if sg == nil {
		sg = c.shareGroups.getOrCreate(groupID)
	}

	// KIP-1222: when isRenewAck is set, skip fetch entirely -- only
	// process acks. The rationale is that time spent fetching might
	// exceed the renewed lock timeout.
	if req.Version >= 2 && req.IsRenewAck {
		sg.mu.Lock()
		if w == nil {
			sf := &shareFetchCtx{
				c:             c,
				creq:          creq,
				sg:            sg,
				req:           req,
				resp:          resp,
				topicIdx:      make(map[uuid]int),
				memberID:      memberID,
				id2t:          id2t,
				supportsRenew: supportsRenew,
				maxDelivery:   maxDelivery,
			}
			sf.processAcksLocked()
			sf.ensureAckPartsInResponse()
		}
		sg.mu.Unlock()
		if session.epoch == math.MaxInt32 {
			session.epoch = 1
		} else {
			session.epoch++
		}
		return resp, nil
	}

	maxRecords := req.MaxRecords
	if maxRecords < 0 {
		maxRecords = 500 // sensible default
	}

	// BATCH_OPTIMIZED mode (ShareAcquireMode=0, KIP-1206): BatchSize
	// controls response splitting, not acquisition limit. Java acquires
	// up to the remaining maxRecords budget per partition, then splits
	// the acquired ranges into sub-batches of BatchSize in the response
	// (matching SharePartition.createBatches).
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

	topicIdx := make(map[uuid]int)
	var totalRecords int32
	var targets []fetchTarget
	var acquiredParts []acquiredPart
	var includeBrokers bool
	var hadPiggybackedAcks bool
	maxRecordLocks := c.shareMaxRecordLocks()

	// Lock the share group's partition state for ack processing and
	// record acquisition. Batch I/O happens after unlocking.
	sg.mu.Lock()

	// Process piggybacked acks first (only on initial invocation).
	var sf *shareFetchCtx
	if w == nil {
		sf = &shareFetchCtx{
			c: c, creq: creq, sg: sg, req: req, resp: resp,
			topicIdx: topicIdx,
			memberID: memberID, id2t: id2t, supportsRenew: supportsRenew, maxDelivery: maxDelivery,
		}
		sf.processAcksLocked()
		for i := range req.Topics {
			for j := range req.Topics[i].Partitions {
				if len(req.Topics[i].Partitions[j].AcknowledgementBatches) > 0 {
					hadPiggybackedAcks = true
					break
				}
			}
			if hadPiggybackedAcks {
				break
			}
		}
	}

	// Phase 1: Build target list from session partitions. The session
	// holds the full set of partitions to fetch from (populated on
	// epoch 0, updated incrementally on epoch > 0).
	for topicID, parts := range session.partitions {
		topicName := id2t[topicID]
		if topicName == "" {
			// Topic ID no longer resolves (deleted mid-session).
			// Return UNKNOWN_TOPIC_ID for all partitions (matching
			// Kafka's ShareSessionContext response filtering).
			idx := getOrAddShareFetchTopic(resp, topicIdx, topicID)
			for p := range parts {
				sp := kmsg.NewShareFetchResponseTopicPartition()
				sp.Partition = p
				sp.ErrorCode = kerr.UnknownTopicID.Code
				resp.Topics[idx].Partitions = append(resp.Topics[idx].Partitions, sp)
			}
			continue
		}

		// ACL: per-topic READ check.
		if !c.allowedACL(creq, topicName, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationRead) {
			idx := getOrAddShareFetchTopic(resp, topicIdx, topicID)
			for p := range parts {
				sp := kmsg.NewShareFetchResponseTopicPartition()
				sp.Partition = p
				sp.ErrorCode = kerr.TopicAuthorizationFailed.Code
				resp.Topics[idx].Partitions = append(resp.Topics[idx].Partitions, sp)
			}
			continue
		}

		for p := range parts {
			// Only fetch from partitions on this broker.
			pd, ok := c.data.tps.getp(topicName, p)
			if !ok {
				idx := getOrAddShareFetchTopic(resp, topicIdx, topicID)
				sp := kmsg.NewShareFetchResponseTopicPartition()
				sp.Partition = p
				sp.ErrorCode = kerr.UnknownTopicOrPartition.Code
				resp.Topics[idx].Partitions = append(resp.Topics[idx].Partitions, sp)
				continue
			}
			if pd.leader.node != creq.cc.b.node {
				idx := getOrAddShareFetchTopic(resp, topicIdx, topicID)
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

	// Phase 2: Rotate targets for fairness (matching Kafka's
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

	// Phase 3: Acquire records from rotated targets.
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

		// BATCH_OPTIMIZED: split acquired ranges into sub-batches
		// of batchSize (matching Java's SharePartition.createBatches).
		// Splits align to batch boundaries in the log.
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

		idx := getOrAddShareFetchTopic(resp, topicIdx, ap.topicID)
		sp := kmsg.NewShareFetchResponseTopicPartition()
		sp.Partition = ap.partition
		sp.Records = rawBytes
		sp.AcquiredRecords = ap.ranges
		resp.Topics[idx].Partitions = append(resp.Topics[idx].Partitions, sp)
	}

	// Incremental response filtering (matching Java's
	// CachedSharePartition.maybeUpdateResponseData). After building
	// the response, update requiresUpdate flags and add empty entries
	// for partitions that must appear even without data.
	{
		responded := make(map[tpKey]bool) // value: true if had error
		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				responded[tpKey{t.TopicID, p.Partition}] = p.ErrorCode != 0
			}
		}
		// Update flags for partitions already in the response.
		for key, hadError := range responded {
			ps := session.partitions[key.tid]
			if ps == nil {
				continue
			}
			if csp := ps[key.p]; csp != nil {
				csp.requiresUpdate = hadError
			}
		}
		// Add empty entries for requiresUpdate partitions not in
		// response (e.g., previously had error, now cleared).
		for tid, parts := range session.partitions {
			for p, csp := range parts {
				if csp == nil || !csp.requiresUpdate {
					continue
				}
				key := tpKey{tid, p}
				if _, ok := responded[key]; ok {
					continue
				}
				idx := getOrAddShareFetchTopic(resp, topicIdx, tid)
				sp := kmsg.NewShareFetchResponseTopicPartition()
				sp.Partition = p
				resp.Topics[idx].Partitions = append(resp.Topics[idx].Partitions, sp)
				csp.requiresUpdate = false
			}
		}
	}

	// Ensure all request partitions with piggybacked acks appear in the
	// response. The client uses partition presence to confirm ack
	// processing; missing partitions cause "dropping piggybacked acks".
	if sf != nil {
		sf.ensureAckPartsInResponse()
	}

	// If no records acquired and this is the initial invocation, consider
	// waiting for new data (MinBytes/MaxWait long-poll). Don't wait when
	// piggybacked acks were present (client needs ack confirmation
	// immediately, not after MaxWait delay). When blocked by
	// maxRecordLocks, we still create a watcher -- fireShareWatchers
	// fires when acks free the window, so the watcher wakes up promptly
	// instead of the client busy-looping with empty fetches.
	if totalRecords == 0 && w == nil && !hadPiggybackedAcks {
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
				case c.watchShareFetchCh <- wsf:
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

	// Advance session epoch.
	if session.epoch == math.MaxInt32 {
		session.epoch = 1
	} else {
		session.epoch++
	}

	return resp, nil
}

func getOrAddShareFetchTopic(resp *kmsg.ShareFetchResponse, idx map[uuid]int, tid uuid) int {
	i, ok := idx[tid]
	if !ok {
		i = len(resp.Topics)
		idx[tid] = i
		t := kmsg.NewShareFetchResponseTopic()
		t.TopicID = tid
		resp.Topics = append(resp.Topics, t)
	}
	return i
}

// shareFetchCtx bundles common state for ShareFetch processing,
// avoiding long parameter lists across helper functions.
type shareFetchCtx struct {
	c             *Cluster
	creq          *clientReq
	sg            *shareGroup
	req           *kmsg.ShareFetchRequest
	resp          *kmsg.ShareFetchResponse
	topicIdx      map[uuid]int // shared with caller to avoid duplicate topic entries
	memberID      string
	id2t          map[uuid]string
	supportsRenew bool
	maxDelivery   int32
}

// ensureAckPartsInResponse checks that every request partition which had
// piggybacked ack batches appears in the response. The client uses
// partition presence to confirm ack processing; missing partitions cause
// "dropping piggybacked acks" warnings and lost ack confirmations.
func (sf *shareFetchCtx) ensureAckPartsInResponse() {
	type tpk struct {
		tid uuid
		p   int32
	}
	present := make(map[tpk]struct{})
	for _, t := range sf.resp.Topics {
		for _, p := range t.Partitions {
			present[tpk{t.TopicID, p.Partition}] = struct{}{}
		}
	}
	for i := range sf.req.Topics {
		rt := &sf.req.Topics[i]
		for j := range rt.Partitions {
			if len(rt.Partitions[j].AcknowledgementBatches) == 0 {
				continue
			}
			key := tpk{rt.TopicID, rt.Partitions[j].Partition}
			if _, ok := present[key]; ok {
				continue
			}
			idx := getOrAddShareFetchTopic(sf.resp, sf.topicIdx, rt.TopicID)
			sp := kmsg.NewShareFetchResponseTopicPartition()
			sp.Partition = rt.Partitions[j].Partition
			sf.resp.Topics[idx].Partitions = append(sf.resp.Topics[idx].Partitions, sp)
		}
	}
}

// processAcksLocked processes piggybacked acknowledgements in a ShareFetch
// request. Validates ack batches per-partition, reports errors via
// AcknowledgeErrorCode, and validates member ownership. After processing,
// fires share watchers on affected partitions so that other waiting
// consumers see released records.
//
// Must be called with sg.mu held.
func (sf *shareFetchCtx) processAcksLocked() {
	topicIdx := sf.topicIdx
	for i := range sf.req.Topics {
		rt := &sf.req.Topics[i]
		topicName := sf.id2t[rt.TopicID]
		if topicName == "" {
			// UNKNOWN_TOPIC_ID for piggybacked acks on
			// deleted topics (matching KafkaApis.scala).
			hasAcks := false
			for j := range rt.Partitions {
				if len(rt.Partitions[j].AcknowledgementBatches) > 0 {
					hasAcks = true
					break
				}
			}
			if hasAcks {
				idx := getOrAddShareFetchTopic(sf.resp, topicIdx, rt.TopicID)
				for j := range rt.Partitions {
					if len(rt.Partitions[j].AcknowledgementBatches) == 0 {
						continue
					}
					sp := kmsg.NewShareFetchResponseTopicPartition()
					sp.Partition = rt.Partitions[j].Partition
					sp.AcknowledgeErrorCode = kerr.UnknownTopicID.Code
					sf.resp.Topics[idx].Partitions = append(sf.resp.Topics[idx].Partitions, sp)
				}
			}
			continue
		}

		// ACL: per-topic READ check for piggybacked acks
		// (matching KafkaApis.scala topic authorization).
		if !sf.c.allowedACL(sf.creq, topicName, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationRead) {
			hasAcks := false
			for j := range rt.Partitions {
				if len(rt.Partitions[j].AcknowledgementBatches) > 0 {
					hasAcks = true
					break
				}
			}
			if hasAcks {
				idx := getOrAddShareFetchTopic(sf.resp, topicIdx, rt.TopicID)
				for j := range rt.Partitions {
					if len(rt.Partitions[j].AcknowledgementBatches) == 0 {
						continue
					}
					sp := kmsg.NewShareFetchResponseTopicPartition()
					sp.Partition = rt.Partitions[j].Partition
					sp.AcknowledgeErrorCode = kerr.TopicAuthorizationFailed.Code
					sf.resp.Topics[idx].Partitions = append(sf.resp.Topics[idx].Partitions, sp)
				}
			}
			continue
		}

		for j := range rt.Partitions {
			rp := &rt.Partitions[j]
			if len(rp.AcknowledgementBatches) == 0 {
				continue
			}

			// Validate ack batches for this partition.
			errCode := int16(0)
			prevEnd := int64(-1)
			maxType := int8(3)
			if sf.supportsRenew {
				maxType = 4
			}
			for _, batch := range rp.AcknowledgementBatches {
				if ec := validateOneAckBatch(batch.FirstOffset, batch.LastOffset, batch.AcknowledgeTypes, &prevEnd, maxType, sf.req.IsRenewAck); ec != 0 {
					errCode = ec
					break
				}
			}
			if errCode != 0 {
				idx := getOrAddShareFetchTopic(sf.resp, topicIdx, rt.TopicID)
				sp := kmsg.NewShareFetchResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.AcknowledgeErrorCode = errCode
				sf.resp.Topics[idx].Partitions = append(sf.resp.Topics[idx].Partitions, sp)
				continue
			}

			pd, ok := sf.c.data.tps.getp(topicName, rp.Partition)
			if !ok {
				idx := getOrAddShareFetchTopic(sf.resp, topicIdx, rt.TopicID)
				sp := kmsg.NewShareFetchResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.AcknowledgeErrorCode = kerr.UnknownTopicOrPartition.Code
				sf.resp.Topics[idx].Partitions = append(sf.resp.Topics[idx].Partitions, sp)
				continue
			}
			shp := sf.sg.getSharePartition(topicName, rp.Partition, pd)
			// Atomic validate-then-apply (matching Kafka's
			// rollbackOrProcessStateUpdates): validate all
			// batches first; if any fails, reject the whole
			// partition without applying anything.
			for _, batch := range rp.AcknowledgementBatches {
				if ec := shp.validateAcks(sf.memberID, batch.FirstOffset, batch.LastOffset); ec != 0 {
					errCode = ec
					break
				}
			}
			if errCode != 0 {
				idx := getOrAddShareFetchTopic(sf.resp, topicIdx, rt.TopicID)
				sp := kmsg.NewShareFetchResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.AcknowledgeErrorCode = errCode
				sf.resp.Topics[idx].Partitions = append(sf.resp.Topics[idx].Partitions, sp)
				continue
			}
			for _, batch := range rp.AcknowledgementBatches {
				shp.processAcks(sf.memberID, batch.FirstOffset, batch.LastOffset, batch.AcknowledgeTypes, sf.maxDelivery)
			}
			shp.advanceSPSO()
			fireShareWatchers(pd)
		}
	}
}

// validateOneAckBatch validates a single acknowledgement batch per Kafka's
// validateAcknowledgementBatches (KafkaApis.scala:4059-4104). prevEnd tracks
// the previous batch's last offset (pass -1 initially). Updated on success.
func validateOneAckBatch(first, last int64, ackTypes []int8, prevEnd *int64, maxType int8, isRenewAck bool) int16 {
	if first > last {
		return kerr.InvalidRequest.Code
	}
	if first <= *prevEnd {
		return kerr.InvalidRequest.Code
	}
	if len(ackTypes) == 0 {
		return kerr.InvalidRequest.Code
	}
	if len(ackTypes) > 1 && int64(len(ackTypes)) != last-first+1 {
		return kerr.InvalidRequest.Code
	}
	for _, at := range ackTypes {
		if at < 0 || at > maxType {
			return kerr.InvalidRequest.Code
		}
		if at == 4 && !isRenewAck {
			return kerr.InvalidRequest.Code
		}
	}
	*prevEnd = last
	return 0
}

// splitAcquiredRanges splits acquired record ranges into sub-batches of at
// most batchSize offsets, aligning splits to batch boundaries in the log.
// This matches Java's SharePartition.createBatches for BATCH_OPTIMIZED mode
// (KIP-1206). In kfake each record is typically its own batch, so splits
// simply cut at batchSize offset intervals.
func splitAcquiredRanges(ranges []kmsg.ShareFetchResponseTopicPartitionAcquiredRecord, batchSize int32) []kmsg.ShareFetchResponseTopicPartitionAcquiredRecord {
	var out []kmsg.ShareFetchResponseTopicPartitionAcquiredRecord
	for _, r := range ranges {
		count := int32(r.LastOffset - r.FirstOffset + 1)
		if count <= batchSize {
			out = append(out, r)
			continue
		}
		// Split into chunks of batchSize.
		for off := r.FirstOffset; off <= r.LastOffset; {
			end := off + int64(batchSize) - 1
			if end > r.LastOffset {
				end = r.LastOffset
			}
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
