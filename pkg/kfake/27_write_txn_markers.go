package kfake

import (
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// WriteTxnMarkers: v0-2
//
// Broker-to-broker request used by the transaction coordinator to write
// commit/abort markers to partition logs. kfake is both coordinator and
// broker, so EndTxn already writes markers; this handler exists mostly
// for admin tooling that sends WriteTxnMarkers directly (e.g. kadm).
//
// Behavior:
// * ACL: ALTER or CLUSTER_ACTION on CLUSTER (Kafka 3.8+)
// * Writes a control batch per requested partition with the given pid+epoch
// * Updates abortedTxns tracking and recalculates LSO
// * Ends the producer's transaction on the partition, not at the coordinator
// * A marker below the producer's epoch on the partition is
//   INVALID_PRODUCER_EPOCH, as is a TV2 marker at that epoch while the
//   transaction is open on the partition
// * CoordinatorEpoch is not validated (kfake is single-coord); it is written into the marker value
//
// Version notes:
// * v1: Flexible versions
// * v2: TransactionVersion field (KIP-1228)

func init() { regKey(27, 0, 2) }

func (c *Cluster) handleWriteTxnMarkers(creq *clientReq) (kmsg.Response, error) {
	req := creq.kreq.(*kmsg.WriteTxnMarkersRequest)
	resp := req.ResponseKind().(*kmsg.WriteTxnMarkersResponse)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	alter := c.allowedClusterACL(creq, kmsg.ACLOperationAlter)
	for _, m := range req.Markers {
		respMarker := kmsg.NewWriteTxnMarkersResponseMarker()
		respMarker.ProducerID = m.ProducerID
		var txnID string
		pidinf := c.pids.ids[m.ProducerID]
		if pidinf != nil {
			txnID = pidinf.txid
		}
		tv2 := req.Version >= 2 && m.TransactionVersion >= 2

		for _, mt := range m.Topics {
			respTopic := kmsg.NewWriteTxnMarkersResponseMarkerTopic()
			respTopic.Topic = mt.Topic

			ps, topicExists := c.data.tps.gett(mt.Topic)
			for _, p := range mt.Partitions {
				respPart := kmsg.NewWriteTxnMarkersResponseMarkerTopicPartition()
				respPart.Partition = p

				// setErr sets the code unless a fault already answered.
				setErr := func(code int16) {
					if respPart.ErrorCode == 0 {
						respPart.ErrorCode = code
					}
				}
				pd, ok := ps[p]
				k := faultKey{txnID: txnID, topic: mt.Topic, misrouted: ok && pd.leader != creq.cc.b}
				var e *kerr.Error
				if alter {
					e = creq.faults.check(k.part(p))
				} else {
					e = c.denyCluster(creq, kmsg.ACLOperationClusterAction, k.part(p))
				}
				if e != nil {
					respPart.ErrorCode = e.Code
					if creq.skipsWork(e) { // a timed-out marker is still written
						respTopic.Partitions = append(respTopic.Partitions, respPart)
						continue
					}
				}
				switch {
				case !topicExists, !ok:
					setErr(kerr.UnknownTopicOrPartition.Code)
				case pd.leader != creq.cc.b:
					setErr(kerr.NotLeaderForPartition.Code)
				case pidinf.staleMarker(pd, m.ProducerEpoch, tv2):
					setErr(kerr.InvalidProducerEpoch.Code)
				default:
					if off := c.writeTxnMarker(pd, m.ProducerID, m.ProducerEpoch, m.Committed, m.CoordinatorEpoch); off < 0 {
						setErr(kerr.UnknownServerError.Code)
					}
				}
				respTopic.Partitions = append(respTopic.Partitions, respPart)
			}
			respMarker.Topics = append(respMarker.Topics, respTopic)
		}
		resp.Markers = append(resp.Markers, respMarker)
	}

	return resp, nil
}

// writeTxnMarker writes a commit/abort control batch for (pid, epoch) to
// the partition, updates aborted-txn tracking, and recalculates the LSO.
// Returns the control batch offset, or -1 on persist failure.
func (c *Cluster) writeTxnMarker(pd *partData, pid int64, epoch int16, commit bool, coordinatorEpoch int32) int64 {
	b, nbytes := txnMarkerBatch(pid, epoch, commit, coordinatorEpoch)

	firstOffset, hadTxn := pd.uncommittedPIDs[pid]
	delete(pd.uncommittedPIDs, pid)

	controlOffset := c.pushBatch(pd, nbytes, b, false)
	if controlOffset < 0 {
		pd.recalculateLSO()
		return -1
	}
	if !commit && hadTxn {
		pd.abortedTxns = append(pd.abortedTxns, abortedTxnEntry{
			producerID:  pid,
			firstOffset: firstOffset,
			lastOffset:  controlOffset,
		})
	}
	pd.recalculateLSO()

	// The marker ends the producer's transaction on this partition: the
	// partition no longer has an open transaction to describe or restore,
	// and a later EndTxn must not count its bytes again. The coordinator
	// still has the partition in the transaction. Either marker moves
	// the LSO past the transaction's bytes, which a read_committed fetch
	// counts, as Kafka does.
	if pidinf := c.pids.ids[pid]; pidinf != nil {
		if txnBytes, _ := pidinf.txPartBytes.getp(pd.t, pd.p); txnBytes != nil && *txnBytes > 0 {
			for w := range pd.watch {
				if w.readCommitted {
					w.addBytes(pd, *txnBytes)
				}
			}
		}
		pidinf.txPartFirstOffsets.delp(pd.t, pd.p)
		pidinf.txPartBytes.delp(pd.t, pd.p)
		if pw, ok := pidinf.windows.getp(pd.t, pd.p); ok {
			pw.lastTimestamp = b.MaxTimestamp
		}
	}
	return controlOffset
}

// staleMarker reports whether a marker's epoch is below the producer's last
// epoch on the partition. A TV2 marker must be above it, unless it repeats
// the marker of a transaction that already ended.
func (pidinf *pidinfo) staleMarker(pd *partData, epoch int16, tv2 bool) bool {
	if pidinf == nil {
		return false
	}
	pw, ok := pidinf.windows.getp(pd.t, pd.p)
	if !ok || !pw.seen {
		return false
	}
	_, open := pd.uncommittedPIDs[pidinf.id]
	return epoch < pw.epoch || tv2 && epoch == pw.epoch && open
}
