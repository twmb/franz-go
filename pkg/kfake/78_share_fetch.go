package kfake

import (
	"math"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ShareFetch: v0-2 (KIP-932, KIP-1206, KIP-1222)

func init() { regKey(78, 0, 2) }

func (c *Cluster) handleShareFetch(creq *clientReq) (kmsg.Response, error) {
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

	// Validate the share group exists.
	sg := c.shareGroups.gs[groupID]
	if sg == nil {
		resp.ErrorCode = kerr.GroupIDNotFound.Code
		return resp, nil
	}

	// Handle session management.
	sessionKey := shareSessionKey{
		group:    groupID,
		memberID: memberID,
		broker:   creq.cc.b.node,
	}

	if req.ShareSessionEpoch == -1 {
		// Close session.
		delete(c.shareSessions.sessions, sessionKey)
		return resp, nil
	}

	session := c.shareSessions.sessions[sessionKey]
	if req.ShareSessionEpoch == 0 {
		// New session -- start at epoch 0; the increment at the
		// bottom of this handler advances it to 1 for the next
		// request.
		session = &shareSession{epoch: 0}
		c.shareSessions.sessions[sessionKey] = session
	} else if session == nil {
		resp.ErrorCode = kerr.ShareSessionNotFound.Code
		return resp, nil
	} else if req.ShareSessionEpoch != session.epoch {
		resp.ErrorCode = kerr.InvalidShareSessionEpoch.Code
		return resp, nil
	}

	maxRecords := req.MaxRecords
	if maxRecords <= 0 {
		maxRecords = 500
	}

	id2t := c.data.id2t
	t2id := c.data.t2id

	// Lock the share group's partition state for the duration of ack
	// processing and record acquisition. This protects against concurrent
	// access from the manage goroutine's sweep timer.
	sg.mu.Lock()

	// Process piggybacked acks first.
	sg.processShareFetchAcks(req, id2t, c)

	// Fetch records for each requested topic-partition.
	topicIdx := make(map[uuid]int)
	var totalRecords int32

	for i := range req.Topics {
		rt := &req.Topics[i]
		topicName := id2t[rt.TopicID]
		if topicName == "" {
			continue
		}

		for j := range rt.Partitions {
			rp := &rt.Partitions[j]

			// Only fetch from partitions on this broker.
			pd, ok := c.data.tps.getp(topicName, rp.Partition)
			if !ok {
				idx := getOrAddShareFetchTopic(resp, topicIdx, rt.TopicID)
				sp := kmsg.NewShareFetchResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = kerr.UnknownTopicOrPartition.Code
				resp.Topics[idx].Partitions = append(resp.Topics[idx].Partitions, sp)
				continue
			}
			if pd.leader.node != creq.cc.b.node {
				idx := getOrAddShareFetchTopic(resp, topicIdx, rt.TopicID)
				sp := kmsg.NewShareFetchResponseTopicPartition()
				sp.Partition = rp.Partition
				sp.ErrorCode = kerr.NotLeaderForPartition.Code
				resp.Topics[idx].Partitions = append(resp.Topics[idx].Partitions, sp)
				continue
			}

			shp := sg.getSharePartition(topicName, rp.Partition, pd)

			// Acquire records.
			remaining := maxRecords - totalRecords
			if remaining <= 0 {
				continue
			}
			acquiredRanges := shp.acquireRecords(memberID, pd.highWatermark, remaining, c.shareMaxDeliveryAttempts())
			if len(acquiredRanges) == 0 {
				continue
			}

			// Count records acquired.
			for _, ar := range acquiredRanges {
				totalRecords += int32(ar.LastOffset - ar.FirstOffset + 1)
			}

			// Read raw batch bytes covering the acquired range.
			firstAcq := acquiredRanges[0].FirstOffset
			lastAcq := acquiredRanges[len(acquiredRanges)-1].LastOffset

			var rawBytes []byte
			segIdx, metaIdx, ok, atEnd := pd.searchOffset(firstAcq)
			if !ok || atEnd {
				// Should not happen -- we just acquired from available records.
				continue
			}

		segments:
			for si := segIdx; si < len(pd.segments); si++ {
				seg := &pd.segments[si]
				start := 0
				if si == segIdx {
					start = metaIdx
				}
				for bi := start; bi < len(seg.index); bi++ {
					m := &seg.index[bi]
					if m.firstOffset > lastAcq {
						break segments
					}
					raw, err := c.readBatchRaw(pd, si, m)
					if err != nil {
						break segments
					}
					rawBytes = append(rawBytes, raw...)
				}
			}

			idx := getOrAddShareFetchTopic(resp, topicIdx, t2id[topicName])
			sp := kmsg.NewShareFetchResponseTopicPartition()
			sp.Partition = rp.Partition
			sp.Records = rawBytes
			sp.AcquiredRecords = acquiredRanges
			resp.Topics[idx].Partitions = append(resp.Topics[idx].Partitions, sp)
		}
	}

	sg.mu.Unlock()

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

// processShareFetchAcks processes piggybacked acknowledgements in a ShareFetch request.
func (sg *shareGroup) processShareFetchAcks(req *kmsg.ShareFetchRequest, id2t map[uuid]string, c *Cluster) {
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
			pd, ok := c.data.tps.getp(topicName, rp.Partition)
			if !ok {
				continue
			}
			shp := sg.getSharePartition(topicName, rp.Partition, pd)
			shp.processAcks(rp.AcknowledgementBatches)
		}
	}
}
