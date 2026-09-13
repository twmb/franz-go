package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ListOffsets: v0-10
//
// Timestamp special values:
// * -2: Earliest offset (log start offset)
// * -1: Latest offset (high watermark or LSO depending on isolation level)
// * -3: Max timestamp offset (KIP-734, v7+)
//
// Version notes:
// * v2: IsolationLevel for read_committed
// * v4: CurrentLeaderEpoch for fencing, LeaderEpoch in response
// * v6: Flexible versions
// * v7: Timestamp -3 for max timestamp (KIP-734)
// * v8: Timestamp -4 for local log start (KIP-405) - tiered storage, not implemented
// * v9: Timestamp -5 for remote storage offset (KIP-1005) - tiered storage, not implemented
// * v10: TimeoutMillis for remote storage lookups - not implemented

func init() { regKey(2, 0, 10) }

func (c *Cluster) handleListOffsets(creq *clientReq) (kmsg.Response, error) {
	var (
		b   = creq.cc.b
		req = creq.kreq.(*kmsg.ListOffsetsRequest)
	)
	resp := req.ResponseKind().(*kmsg.ListOffsetsResponse)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	tidx := make(map[string]int)
	donet := func(t string) *kmsg.ListOffsetsResponseTopic {
		if i, ok := tidx[t]; ok {
			return &resp.Topics[i]
		}
		tidx[t] = len(resp.Topics)
		st := kmsg.NewListOffsetsResponseTopic()
		st.Topic = t
		resp.Topics = append(resp.Topics, st)
		return &resp.Topics[len(resp.Topics)-1]
	}
	donep := func(t string, p int32, errCode int16) *kmsg.ListOffsetsResponseTopicPartition {
		sp := kmsg.NewListOffsetsResponseTopicPartition()
		sp.Partition = p
		sp.ErrorCode = errCode
		st := donet(t)
		st.Partitions = append(st.Partitions, sp)
		return &st.Partitions[len(st.Partitions)-1]
	}

	for _, rt := range req.Topics {
		tk := faultKey{topic: rt.Topic}
		if e := c.deny(creq, rt.Topic, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationDescribe, tk); e != nil {
			for _, rp := range rt.Partitions {
				donep(rt.Topic, rp.Partition, e.Code)
			}
			continue
		}
		ps, ok := c.data.tps.gett(rt.Topic)
		for _, rp := range rt.Partitions {
			if e := creq.faults.check(tk.part(rp.Partition)); e != nil {
				donep(rt.Topic, rp.Partition, e.Code)
				continue
			}
			if !ok {
				donep(rt.Topic, rp.Partition, kerr.UnknownTopicOrPartition.Code)
				continue
			}
			pd, ok := ps[rp.Partition]
			if !ok {
				donep(rt.Topic, rp.Partition, kerr.UnknownTopicOrPartition.Code)
				continue
			}
			if pd.leader != b {
				donep(rt.Topic, rp.Partition, kerr.NotLeaderForPartition.Code)
				continue
			}
			if le := rp.CurrentLeaderEpoch; le != -1 {
				if le < pd.epoch {
					donep(rt.Topic, rp.Partition, kerr.FencedLeaderEpoch.Code)
					continue
				} else if le > pd.epoch {
					donep(rt.Topic, rp.Partition, kerr.UnknownLeaderEpoch.Code)
					continue
				}
			}

			sp := donep(rt.Topic, rp.Partition, 0)
			sp.LeaderEpoch = pd.epoch
			switch rp.Timestamp {
			case -2:
				sp.Offset = pd.logStartOffset
				// The epoch accompanying a listed offset is the epoch
				// of the record at that offset (a real broker answers
				// from its leader-epoch cache), not the partition's
				// current epoch: a freshly reset consumer must not
				// believe it consumed an epoch above the historical
				// records it is about to read.
				if segIdx, metaIdx, ok, atEnd := pd.searchOffset(pd.logStartOffset); ok && !atEnd {
					sp.LeaderEpoch = pd.segments[segIdx].index[metaIdx].epoch
				}
			case -1:
				if req.IsolationLevel == 1 {
					sp.Offset = pd.lastStableOffset
				} else {
					sp.Offset = pd.highWatermark
				}
			case -3:
				// KIP-734: the first record with the max timestamp.
				offset, timestamp, epoch, found, err := c.offsetOfMaxTimestamp(pd)
				if err != nil {
					sp.ErrorCode = kerr.CorruptMessage.Code
					continue
				}
				if found {
					sp.Offset = offset
					sp.Timestamp = timestamp
					sp.LeaderEpoch = epoch
				} else {
					sp.Offset = -1
					sp.Timestamp = -1
				}
			default:
				offset, timestamp, epoch, found, err := c.offsetForTimestamp(pd, rp.Timestamp)
				if err != nil {
					sp.ErrorCode = kerr.CorruptMessage.Code
					continue
				}
				if found {
					sp.Offset = offset
					sp.Timestamp = timestamp
					sp.LeaderEpoch = epoch
				} else {
					sp.Offset = -1
				}
			}
		}
	}
	return resp, nil
}

// offsetOfMaxTimestamp answers ListOffsets -3 the way a real broker does
// (RecordBatch.offsetOfMaxTimestamp): the first record, in offset order,
// whose timestamp is the partition's max timestamp. Returns found == false
// for an empty partition, or if the max timestamp batch's header names a
// timestamp none of its records carry.
func (c *Cluster) offsetOfMaxTimestamp(pd *partData) (offset, timestamp int64, epoch int32, found bool, err error) {
	m := pd.maxTimestampBatch()
	if m == nil {
		return 0, 0, 0, false, nil
	}
	batch, err := c.readBatchFull(pd, pd.maxTimestampSeg, m)
	if err != nil {
		return 0, 0, 0, false, err
	}
	recs, err := BatchRecords(batch.RecordBatch)
	if err != nil {
		return 0, 0, 0, false, err
	}
	for _, rec := range recs {
		if batch.FirstTimestamp+rec.TimestampDelta64 == m.maxTimestamp {
			return batch.FirstOffset + int64(rec.OffsetDelta), m.maxTimestamp, m.epoch, true, nil
		}
	}
	return 0, 0, 0, false, nil
}

// offsetForTimestamp answers a ListOffsets timestamp query the way a real
// broker does (FileRecords.searchForTimestamp): the first batch in offset
// order whose max timestamp reaches ts, then the first record in it at or
// after ts and at or after the log start offset. If every such record in
// that batch was deleted from below, the scan moves on to the next batch.
// Returns found == false if no record qualifies.
func (c *Cluster) offsetForTimestamp(pd *partData, ts int64) (offset, timestamp int64, epoch int32, found bool, err error) {
	segIdx, metaIdx, meta := pd.findBatchMeta(ts, func(m *batchMeta) int64 { return m.maxEarlierTimestamp })
	if meta == nil {
		return 0, 0, 0, false, nil
	}
	pd.eachBatchMetaFrom(segIdx, metaIdx, func(si, _ int, m *batchMeta) bool {
		if m.maxTimestamp < ts {
			return true
		}
		var batch *partBatch
		if batch, err = c.readBatchFull(pd, si, m); err != nil {
			return false
		}
		var recs []kmsg.Record
		if recs, err = BatchRecords(batch.RecordBatch); err != nil {
			return false
		}
		for _, rec := range recs {
			recTimestamp := batch.FirstTimestamp + rec.TimestampDelta64
			recOffset := batch.FirstOffset + int64(rec.OffsetDelta)
			if recTimestamp >= ts && recOffset >= pd.logStartOffset {
				offset, timestamp, epoch, found = recOffset, recTimestamp, m.epoch, true
				return false
			}
		}
		return true
	})
	return offset, timestamp, epoch, found, err
}
