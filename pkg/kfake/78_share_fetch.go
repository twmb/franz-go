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

	// Validate the share group exists.
	sg := c.shareGroups.gs[groupID]
	if sg == nil {
		resp.ErrorCode = kerr.GroupIDNotFound.Code
		return resp, nil
	}

	id2t := c.data.id2t
	supportsRenew := req.Version >= 2
	maxDelivery := c.shareMaxDeliveryAttempts()

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
	var addedParts map[tpKey]struct{} // partitions newly added to session (for response filtering)

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
			sg.mu.Lock()
			c.processShareFetchAcksLocked(sg, req, memberID, id2t, supportsRenew, maxDelivery, resp)
			released := sg.releaseRecordsForMemberLocked(memberID, maxDelivery)
			sg.mu.Unlock()
			if released {
				sg.fireAllShareWatchers(c)
			}
			delete(c.shareSessions, sessionKey)
			return resp, nil
		}

		session = c.shareSessions[sessionKey]
		if req.ShareSessionEpoch == 0 {
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
				partitions: make(map[uuid]map[int32]struct{}),
				cc:         creq.cc,
			}
			for i := range req.Topics {
				rt := &req.Topics[i]
				ps := session.partitions[rt.TopicID]
				if ps == nil {
					ps = make(map[int32]struct{})
					session.partitions[rt.TopicID] = ps
				}
				for j := range rt.Partitions {
					ps[rt.Partitions[j].Partition] = struct{}{}
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
			// Track newly-added partitions for response filtering:
			// added partitions appear in the response even with no
			// data (matching Kafka's requiresUpdateInResponse).
			for i := range req.Topics {
				rt := &req.Topics[i]
				ps := session.partitions[rt.TopicID]
				if ps == nil {
					ps = make(map[int32]struct{})
					session.partitions[rt.TopicID] = ps
				}
				for j := range rt.Partitions {
					p := rt.Partitions[j].Partition
					if _, exists := ps[p]; !exists {
						if addedParts == nil {
							addedParts = make(map[tpKey]struct{})
						}
						addedParts[tpKey{rt.TopicID, p}] = struct{}{}
					}
					ps[p] = struct{}{}
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

	// KIP-1222: when isRenewAck is set, skip fetch entirely -- only
	// process acks. The rationale is that time spent fetching might
	// exceed the renewed lock timeout.
	if req.Version >= 2 && req.IsRenewAck {
		sg.mu.Lock()
		if w == nil {
			c.processShareFetchAcksLocked(sg, req, memberID, id2t, supportsRenew, maxDelivery, resp)
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
	if maxRecords <= 0 {
		maxRecords = 500
	}

	// BATCH_OPTIMIZED mode (ShareAcquireMode=0, KIP-1206): when
	// BatchSize is set, use it as the per-partition acquire limit.
	perPartLimit := maxRecords
	if req.ShareAcquireMode == 0 && req.BatchSize > 0 {
		perPartLimit = req.BatchSize
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
	maxRecordLocks := c.shareMaxRecordLocks()

	// Lock the share group's partition state for ack processing and
	// record acquisition. Batch I/O happens after unlocking.
	sg.mu.Lock()

	// Process piggybacked acks first (only on initial invocation).
	if w == nil {
		c.processShareFetchAcksLocked(sg, req, memberID, id2t, supportsRenew, maxDelivery, resp)
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
		if remaining > perPartLimit {
			remaining = perPartLimit
		}

		shp := sg.getSharePartition(tgt.topic, tgt.partition, tgt.pd)
		acquiredRanges := shp.acquireRecords(memberID, tgt.pd, tgt.pd.highWatermark, remaining, maxDelivery, maxRecordLocks)
		if len(acquiredRanges) == 0 {
			continue
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

	// Response filtering: for incremental sessions, newly-added
	// partitions appear in the response even with no data (matching
	// Kafka's CachedSharePartition.requiresUpdateInResponse).
	if len(addedParts) > 0 {
		responded := make(map[tpKey]struct{})
		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				responded[tpKey{t.TopicID, p.Partition}] = struct{}{}
			}
		}
		for key := range addedParts {
			if _, ok := responded[key]; ok {
				continue
			}
			idx := getOrAddShareFetchTopic(resp, topicIdx, key.tid)
			sp := kmsg.NewShareFetchResponseTopicPartition()
			sp.Partition = key.p
			resp.Topics[idx].Partitions = append(resp.Topics[idx].Partitions, sp)
		}
	}

	// If no records acquired and this is the initial invocation, consider
	// waiting for new data (MinBytes/MaxWait long-poll).
	if totalRecords == 0 && w == nil {
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

// processShareFetchAcksLocked processes piggybacked acknowledgements in a
// ShareFetch request. Validates ack batches per-partition (gap #5) and
// reports errors via AcknowledgeErrorCode (gap #6). Validates member
// ownership (gap #2). After processing, fires share watchers on affected
// partitions so that other waiting consumers see released records.
//
// Must be called with sg.mu held.
func (c *Cluster) processShareFetchAcksLocked(sg *shareGroup, req *kmsg.ShareFetchRequest, memberID string, id2t map[uuid]string, supportsRenew bool, maxDelivery int32, resp *kmsg.ShareFetchResponse) {
	topicIdx := make(map[uuid]int)
	for i := range req.Topics {
		rt := &req.Topics[i]
		topicName := id2t[rt.TopicID]
		if topicName == "" {
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
			if supportsRenew {
				maxType = 4
			}
			for _, batch := range rp.AcknowledgementBatches {
				if ec := validateOneAckBatch(batch.FirstOffset, batch.LastOffset, batch.AcknowledgeTypes, &prevEnd, maxType, req.IsRenewAck); ec != 0 {
					errCode = ec
					break
				}
			}
			if errCode != 0 {
				idx := getOrAddShareFetchTopic(resp, topicIdx, rt.TopicID)
				sp := kmsg.NewShareFetchResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.AcknowledgeErrorCode = errCode
				resp.Topics[idx].Partitions = append(resp.Topics[idx].Partitions, sp)
				continue
			}

			pd, ok := c.data.tps.getp(topicName, rp.Partition)
			if !ok {
				continue
			}
			shp := sg.getSharePartition(topicName, rp.Partition, pd)
			var ackErr int16
			for _, batch := range rp.AcknowledgementBatches {
				if ec := shp.processAcks(memberID, batch.FirstOffset, batch.LastOffset, batch.AcknowledgeTypes, maxDelivery); ec != 0 {
					ackErr = ec
					break
				}
			}
			if ackErr != 0 {
				idx := getOrAddShareFetchTopic(resp, topicIdx, rt.TopicID)
				sp := kmsg.NewShareFetchResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.AcknowledgeErrorCode = ackErr
				resp.Topics[idx].Partitions = append(resp.Topics[idx].Partitions, sp)
				continue
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
	if first < *prevEnd {
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
