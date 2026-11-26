package kfake

import (
	"encoding/binary"
	"errors"
	"hash/crc32"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Produce: v3-13
//
// Behavior:
// * Writes are immediate - TimeoutMillis is ignored (no replication to wait on)
// * Only RecordBatch format is supported (v3+), not MessageSets (v0-2)
// * All batch-level validation is performed (CRC, attributes, sequences, etc.)
// * Idempotent and transactional produces are fully supported
//
// Version notes:
// * v3: RecordBatch format, transactions
// * v5: LogStartOffset in response
// * v8: ErrorMessage in response (KIP-467)
// * v9: Flexible versions
// * v12: KIP-890 implicit partition addition for transactions
// * v13: TopicID in request/response (KIP-516)

func init() { regKey(0, 3, 13) }

func (c *Cluster) handleProduce(creq *clientReq) (kmsg.Response, error) {
	var (
		b    = creq.cc.b
		req  = creq.kreq.(*kmsg.ProduceRequest)
		resp = req.ResponseKind().(*kmsg.ProduceResponse)
	)
	type tpid struct {
		t  string
		id [16]byte
	}
	var (
		tdone = make(map[tpid][]kmsg.ProduceResponseTopicPartition)
		// v13 names a topic by ID alone, whether or not we have
		// resolved its name yet.
		id = func(t kmsg.ProduceRequestTopic) tpid {
			if req.Version >= 13 {
				return tpid{id: t.TopicID}
			}
			return tpid{t: t.Topic}
		}
	)

	if err := c.checkReqVersion(req.Key(), req.Version); err != nil {
		return nil, err
	}

	// A fault can answer a partition before the work runs. The work's own
	// answer for that partition must not add an entry or replace the code.
	// Every unknown v13 topic ID has an empty name, so the ID is part of
	// the key.
	type tpidp struct {
		tpid
		p int32
	}
	answered := make(map[tpidp]int)
	donep := func(t kmsg.ProduceRequestTopic, p kmsg.ProduceRequestTopicPartition, errCode int16, errMsg string) *kmsg.ProduceResponseTopicPartition {
		ps := tdone[id(t)]
		if i, ok := answered[tpidp{id(t), p.Partition}]; ok {
			return &ps[i]
		}
		answered[tpidp{id(t), p.Partition}] = len(ps)
		sp := kmsg.NewProduceResponseTopicPartition()
		sp.Partition = p.Partition
		sp.ErrorCode = errCode
		if req.Version >= 8 && errMsg != "" {
			sp.ErrorMessage = &errMsg
		}
		ps = append(ps, sp)
		tdone[id(t)] = ps
		return &ps[len(ps)-1]
	}
	// fail answers an error from the append itself with the log start
	// offset, as Kafka does, except MESSAGE_TOO_LARGE, CORRUPT_MESSAGE, and
	// KAFKA_STORAGE_ERROR, which Kafka answers with -1.
	fail := func(t kmsg.ProduceRequestTopic, p kmsg.ProduceRequestTopicPartition, pd *partData, errCode int16, errMsg string) *kmsg.ProduceResponseTopicPartition {
		sp := donep(t, p, errCode, errMsg)
		if errCode != kerr.MessageTooLarge.Code && errCode != kerr.CorruptMessage.Code && errCode != kerr.KafkaStorageError.Code {
			sp.LogStartOffset = pd.logStartOffset
		}
		return sp
	}
	donet := func(t kmsg.ProduceRequestTopic, errCode int16, errMsg string) {
		for _, p := range t.Partitions {
			donep(t, p, errCode, errMsg)
		}
	}
	donets := func(errCode int16, errMsg string) {
		for _, t := range req.Topics {
			donet(t, errCode, errMsg)
		}
	}
	var includeBrokers bool
	// toresp answers the request. An acks=0 client reads no response, so
	// on any error, the request's or a partition's, Kafka closes the
	// connection: that is the only way the client learns to refresh its
	// metadata.
	toresp := func() (kmsg.Response, error) {
		if req.Acks == 0 {
			for _, ps := range tdone {
				for _, p := range ps {
					if p.ErrorCode != 0 {
						return nil, errors.New("closing the connection after a failed acks=0 produce")
					}
				}
			}
			return nil, nil
		}
		for id, partitions := range tdone {
			st := kmsg.NewProduceResponseTopic()
			st.Topic = id.t
			st.TopicID = id.id
			st.Partitions = partitions
			resp.Topics = append(resp.Topics, st)
		}
		if includeBrokers {
			for _, b := range c.bs {
				sb := kmsg.NewProduceResponseBroker()
				sb.NodeID = b.node
				sb.Host, sb.Port = b.hostport()
				sb.Rack = &brokerRack
				resp.Brokers = append(resp.Brokers, sb)
			}
		}
		return resp, nil
	}

	// If any batch is transactional (a batch's attributes are at byte
	// 21), Kafka checks the request's transactional ID before anything
	// else, and fails the whole request if it has none, even without an
	// authorizer.
	var anyTxnal bool
	for _, rt := range req.Topics {
		for _, rp := range rt.Partitions {
			anyTxnal = anyTxnal || len(rp.Records) > 22 && binary.BigEndian.Uint16(rp.Records[21:])&0x0010 != 0
		}
	}
	var txnID string
	if req.TransactionID != nil {
		txnID = *req.TransactionID
	}
	switch {
	case !anyTxnal:
	case req.TransactionID == nil:
		donets(kerr.TransactionalIDAuthorizationFailed.Code, "")
		return toresp()
	default:
		if e := c.deny(creq, txnID, kmsg.ACLResourceTypeTransactionalId, kmsg.ACLOperationWrite, faultKey{txnID: txnID}); e != nil {
			donets(e.Code, e.Message)
			if creq.skipsWork(e) { // a timed-out append still appends
				return toresp()
			}
		}
	}

	switch req.Acks {
	case -1, 0, 1:
	default:
		donets(kerr.InvalidRequiredAcks.Code, "Invalid required acks.")
		return toresp()
	}

	now := time.Now().UnixMilli()
	for _, rt := range req.Topics {
		if req.Version >= 13 {
			topic, ok := c.data.id2t[rt.TopicID]
			if !ok {
				for _, rp := range rt.Partitions {
					if e := creq.faults.check(faultKey{topicID: rt.TopicID, txnID: txnID}.part(rp.Partition)); e != nil {
						donep(rt, rp, e.Code, e.Message)
						continue
					}
					donep(rt, rp, kerr.UnknownTopicID.Code, "Unknown topic ID.")
				}
				continue
			}
			rt.Topic = topic
		}
		var (
			maxMessageBytes = c.data.maxMessageBytes(rt.Topic)
			compacted       = c.data.isCompactTopic(rt.Topic)
			logAppendTime   = c.data.logAppendTime(rt.Topic)
		)
		for _, rp := range rt.Partitions {
			pd, ok := c.data.tps.getp(rt.Topic, rp.Partition)
			k := faultKey{topic: rt.Topic, topicID: rt.TopicID, txnID: txnID, misrouted: ok && pd.leader != b}
			if e := c.deny(creq, rt.Topic, kmsg.ACLResourceTypeTopic, kmsg.ACLOperationWrite, k.part(rp.Partition)); e != nil {
				donep(rt, rp, e.Code, e.Message)
				if creq.skipsWork(e) { // a timed-out append still appends
					continue
				}
			}
			if !ok {
				donep(rt, rp, kerr.UnknownTopicOrPartition.Code, "Unknown topic or partition.")
				continue
			}
			// Kafka answers INVALID_RECORD for empty records before it
			// checks leadership. Kafka 3.8+ never answers null records
			// (KAFKA-21676); we answer null records the same as empty
			// records.
			if len(rp.Records) == 0 {
				donep(rt, rp, kerr.InvalidRecord.Code, "")
				continue
			}
			if pd.leader != b {
				p := donep(rt, rp, kerr.NotLeaderForPartition.Code, "Not leader for partition.")
				p.CurrentLeader.LeaderID = pd.leader.node
				p.CurrentLeader.LeaderEpoch = pd.epoch
				includeBrokers = true
				continue
			}

			// Check message size before parsing
			if len(rp.Records) > maxMessageBytes {
				fail(rt, rp, pd, kerr.MessageTooLarge.Code, "The message is too large.")
				continue
			}

			var b kmsg.RecordBatch
			if err := b.ReadFrom(rp.Records); err != nil {
				fail(rt, rp, pd, kerr.CorruptMessage.Code, "Corrupt message.")
				continue
			}
			if b.FirstOffset != 0 {
				fail(rt, rp, pd, kerr.InvalidRecord.Code, "First offset in batch must be zero.")
				continue
			}
			if int(b.Length) != len(rp.Records)-12 {
				fail(rt, rp, pd, kerr.CorruptMessage.Code, "Batch length mismatch.")
				continue
			}
			// PartitionLeaderEpoch is not validated: real Kafka brokers
			// overwrite this field with the current leader epoch. Clients
			// send different values (franz-go sends -1, Sarama sends 0).

			if b.Magic != 2 {
				fail(rt, rp, pd, kerr.CorruptMessage.Code, "Unsupported message format version.")
				continue
			}
			if b.CRC != int32(crc32.Checksum(rp.Records[21:], crc32c)) { // crc starts at byte 21
				fail(rt, rp, pd, kerr.CorruptMessage.Code, "CRC mismatch.")
				continue
			}
			attrs := uint16(b.Attributes)
			if attrs&0x0020 != 0 {
				fail(rt, rp, pd, kerr.InvalidRecord.Code, "Client cannot send control batches.")
				continue
			}
			if attrs&0x0007 > 4 {
				fail(rt, rp, pd, kerr.CorruptMessage.Code, "Invalid compression type.")
				continue
			}
			if b.LastOffsetDelta != b.NumRecords-1 {
				fail(rt, rp, pd, kerr.InvalidRecord.Code, "Last offset delta mismatch.")
				continue
			}

			// Compaction keeps the last record for each key, so
			// Kafka rejects a record with no key on a compacted
			// topic.
			if compacted {
				const msg = "Compacted topic cannot accept message without key."
				var recErrs []kmsg.ProduceResponseTopicPartitionErrorRecord
				var i int32
				if err := forEachBatchRecord(b, func(rec kmsg.Record) bool {
					if rec.Key == nil {
						re := kmsg.NewProduceResponseTopicPartitionErrorRecord()
						re.RelativeOffset = i
						re.ErrorMessage = kmsg.StringPtr(msg)
						recErrs = append(recErrs, re)
					}
					i++
					return true
				}); errors.Is(err, errCorruptRecord) {
					fail(rt, rp, pd, kerr.InvalidRecord.Code, "") // as Kafka answers a record it cannot read
					continue
				} else if err != nil {
					fail(rt, rp, pd, kerr.CorruptMessage.Code, "Corrupt message.")
					continue
				}
				if len(recErrs) > 0 {
					sp := fail(rt, rp, pd, kerr.InvalidRecord.Code, msg)
					if req.Version >= 8 {
						sp.ErrorRecords = recErrs
					}
					continue
				}
			}

			// Kafka reads every record of a produced batch and
			// refuses a batch whose count is not the number of
			// records it has. We advance the high watermark by the
			// count, so a false count would leave offsets with no
			// record. The compacted check above read every record
			// already.
			if !compacted {
				if n, err := batchRecordCount(b); err != nil && !errors.Is(err, errCorruptRecord) {
					fail(rt, rp, pd, kerr.CorruptMessage.Code, "Corrupt message.")
					continue
				} else if err != nil || n != int(b.NumRecords) {
					fail(rt, rp, pd, kerr.InvalidRecord.Code, "") // as Kafka answers a record it cannot read
					continue
				}
			}

			// Kafka stamps the batch with the topic's timestamp
			// type, whatever type the client set, and a
			// LogAppendTime batch with the broker's clock.
			appendTime := int64(-1)
			stamped := b.Attributes &^ 0x0008
			if logAppendTime {
				stamped |= 0x0008
				b.FirstTimestamp = now
				b.MaxTimestamp = now
				appendTime = now
			}
			if stamped != b.Attributes || appendTime >= 0 {
				b.Attributes = stamped
				b.CRC = int32(crc32.Checksum(b.AppendTo(nil)[21:], crc32c))
			}

			txnal := attrs&0x0010 != 0

			// Transactional batches must have a valid producer ID
			if txnal && b.ProducerID < 0 {
				fail(rt, rp, pd, kerr.InvalidProducerIDMapping.Code, "Invalid producer ID mapping for transactional batch.")
				continue
			}

			var errCode int16
			var dup bool
			var baseOffset, lso int64

			if b.ProducerID >= 0 {
				// Kafka has the coordinator check a transactional
				// batch's producer ID against the request's
				// transactional ID, then its epoch, before it adds or
				// verifies the partition. It answers an epoch mismatch
				// either way with INVALID_PRODUCER_EPOCH.
				if p := c.pids.getpid(b.ProducerID); txnal && (p == nil || p.txid != txnID) {
					donep(rt, rp, kerr.InvalidProducerIDMapping.Code, "")
					continue
				} else if txnal && p.epoch != b.ProducerEpoch {
					donep(rt, rp, kerr.InvalidProducerEpoch.Code, "")
					continue
				}

				// For KIP-890 (v12+), pass pd to implicitly add
				// the partition to the transaction if not yet added.
				var implicit *partData
				if txnal && req.Version >= 12 {
					implicit = pd
				}
				pidinf, window := c.pids.get(b.ProducerID, rt.Topic, rp.Partition, implicit)

				// For non-transactional idempotent produce with no
				// existing producer state, implicitly create it.
				// Apache Kafka and Redpanda accept the first batch
				// for an unknown (PID, epoch) with any sequence
				// number; this seeds producer state on demand.
				// See twmb/franz-go#1281.
				if !txnal && pidinf == nil && b.ProducerEpoch != -1 {
					pidinf, window = c.pids.getOrCreateNonTx(b.ProducerID, b.ProducerEpoch, rt.Topic, rp.Partition)
				}

				// Kafka rejects a non-transactional batch only on a
				// partition holding the producer's records of a
				// transaction that is not yet committed or aborted.
				if _, open := pd.uncommittedPIDs[b.ProducerID]; open && !txnal {
					errCode = kerr.InvalidTxnState.Code
				}

				// A partition not in the transaction. As in Kafka, only
				// v11 answers TRANSACTION_ABORTABLE: older clients do not
				// know that error and get INVALID_TXN_STATE, and v12+
				// adds the partition itself, so it never gets here.
				// Kafka fails the batch before the append, with no log
				// start offset.
				if txnal && window == nil {
					code, msg := kerr.InvalidTxnState.Code, "Partition was not added to the transaction"
					if req.Version == 11 {
						code, msg = kerr.TransactionAbortable.Code, ""
					}
					donep(rt, rp, code, msg)
					continue
				}

				// Brokers below 2.5 (KIP-360) reject an append that
				// continues its sequence into a log that never saw
				// the producer, transactional or not. 2.5+ accepts
				// any first sequence for unknown producer state.
				if errCode == 0 && window != nil && !window.seen && b.FirstSequence != 0 && c.maxVersion(int16(kmsg.InitProducerID)) < 3 {
					errCode = kerr.UnknownProducerID.Code
				}

				// KAFKA-15591: if a partition's log has never held a
				// record, we accept only a first sequence of zero
				// from a producer we have no state for. Otherwise a
				// client can have two produces in flight to a just
				// created partition. We can handle them out of order,
				// and the earlier one is then lost with no error. A
				// partition that once held records accepts any first
				// sequence, even after the records are gone.
				if errCode == 0 && c.rejectsNeverWrittenNonzeroSeq() && window != nil && !window.seen && b.FirstSequence != 0 && pd.highWatermark == 0 {
					errCode = kerr.OutOfOrderSequenceNumber.Code
				}

				if errCode == 0 {
					switch {
					case window == nil && b.ProducerEpoch != -1:
						errCode = kerr.InvalidTxnState.Code
					case window != nil && !txnal && window.seen && b.ProducerEpoch < window.epoch:
						// Kafka keeps producer state per partition and
						// rejects only an epoch below the one stored
						// there. An idempotent producer bumps its epoch
						// itself (KIP-360), so a partition that has not
						// yet seen the bump accepts the old epoch. A
						// higher epoch must restart at sequence 0, see
						// pushAndValidate.
						errCode = kerr.InvalidProducerEpoch.Code
					default:
						var seqOk bool
						seqOk, dup, baseOffset = window.pushAndValidate(b.ProducerEpoch, b.FirstSequence, b.NumRecords, pd.highWatermark, b.MaxTimestamp)
						if !seqOk {
							errCode = kerr.OutOfOrderSequenceNumber.Code
						}
					}
				}
				if errCode == 0 && !dup {
					// Validation passed, push the batch.
					baseOffset = pd.highWatermark
					lso = pd.logStartOffset

					if txnal {
						// Track per-partition first offset for AbortedTransactions index.
						ptr := pidinf.txPartFirstOffsets.mkp(rt.Topic, rp.Partition, func() *int64 {
							v := int64(-1)
							return &v
						})
						if *ptr == -1 {
							*ptr = baseOffset
						}
					}

					if c.pushBatch(pd, len(rp.Records), b, txnal) < 0 {
						errCode = kerr.KafkaStorageError.Code
					} else if txnal {
						pidinf.txBatchCount++
						// Track bytes for readCommitted watcher accounting at commit time
						bytesPtr := pidinf.txPartBytes.mkp(rt.Topic, rp.Partition, func() *int { return new(int) })
						*bytesPtr += len(rp.Records)
					}
				}
			} else {
				// Non-idempotent produce, no pids validation needed
				baseOffset = pd.highWatermark
				lso = pd.logStartOffset
				if c.pushBatch(pd, len(rp.Records), b, txnal) < 0 {
					errCode = kerr.KafkaStorageError.Code // as Kafka answers a failed append
				}
			}

			if errCode != 0 {
				fail(rt, rp, pd, errCode, "")
				continue
			}
			if dup {
				sp := donep(rt, rp, 0, "")
				sp.BaseOffset = baseOffset // original offset from dup detection
				sp.LogStartOffset = pd.logStartOffset
				// Kafka answers the original batch's max timestamp,
				// whatever the timestamp type.
				if si, mi, ok, atEnd := pd.searchOffset(baseOffset); ok && !atEnd {
					sp.LogAppendTime = pd.segments[si].index[mi].maxTimestamp
				}
				continue
			}

			sp := donep(rt, rp, 0, "")
			sp.BaseOffset = baseOffset
			sp.LogAppendTime = appendTime
			sp.LogStartOffset = lso
		}
	}

	return toresp()
}
