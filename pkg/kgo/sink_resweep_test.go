package kgo

import (
	"context"
	"testing"
)

// TestAuditTxnRecheckRewindUnmarksAddedToTxn guards the producer-ID/epoch
// recheck rewind in produce(): when a TV1 transactional drain stages a request
// and then bails because the producer ID changed (a parallel sink bumped the
// epoch on EndTxn or failed the id), undoStagedBatches must un-mark addedToTxn
// for the partitions THIS request newly added to the transaction.
//
// If it does not, the partition stays marked-added client-side while its
// AddPartitionsToTxn was never sent. txnReqBuilder.add skips already-added
// partitions, so the NEXT drain produces to that partition without ever adding
// it to the coordinator's transaction: a verifying broker rejects the produce
// with INVALID_TXN_STATE, and a non-verifying broker leaves the records in a
// transaction the coordinator never learned the partition belongs to.
//
// The companion invariant (from 07525c50, R3 sink-sweep B1): a partition added
// to the transaction by an EARLIER request is a broker-acked fact deliberately
// absent from this request's txnReq, and must keep its membership across the
// rewind -- else EndTransaction's anyAdded walk skips EndTxn entirely.
//
// The full end-to-end trigger is a TOCTOU race between produce()'s producerID()
// and createReq() that is not deterministically reproducible (the recheck's own
// comment describes it); this drives the real createReq + undoStagedBatches
// directly and asserts the rewind contract both ways.
func TestAuditTxnRecheckRewindUnmarksAddedToTxn(t *testing.T) {
	t.Parallel()

	cl, err := NewClient(
		SeedBrokers("127.0.0.1:1"), // metadata never loads; we drive the sink directly
		TransactionalID("audit-recheck"),
		ManualFlushing(), // keep buffered batches from spawning a real drain
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	p := &cl.producer
	p.id.Store(&producerID{id: 5, epoch: 0, err: nil})

	s := cl.newSink(1)
	s.produceVersion.Store(11) // TV1: stays on the AddPartitionsToTxn (pv12==false) path

	newRecBuf := func(partition int32) *recBuf {
		r := &recBuf{
			cl:                  cl,
			topic:               "t",
			partition:           partition,
			maxRecordBatchBytes: 1 << 20,
			recBufsIdx:          -1,
			lastAckedOffset:     -1,
			sink:                s,
		}
		r.lingerFn = r.unlingerAndManuallyDrain // mirror metadata.go recBuf creation
		s.addRecBuf(r)
		r.bufferRecord(promisedRec{
			ctx:     context.Background(),
			promise: func(*Record, error) {},
			Record:  &Record{Value: []byte("v"), Context: context.Background()},
		}, false)
		return r
	}

	// rNew is drained for the first time by this request; rOld was already
	// added to the transaction by an earlier request. Both batches drain, but
	// txnReqBuilder.add records only rNew (rOld's addedToTxn is already true).
	rNew := newRecBuf(0)
	rOld := newRecBuf(1)
	rOld.addedToTxn.Store(true)

	// Make Close able to fail the buffered records (they live in the sink; mirror
	// the topics view a metadata load would build so the cleanup reaches them).
	p.topics.storeTopics([]string{"t"})
	tpNew := &topicPartition{records: rNew}
	tpOld := &topicPartition{records: rOld}
	p.topics.load()["t"].v.Store(&topicPartitionsData{
		topic:              "t",
		partitions:         []*topicPartition{tpNew, tpOld},
		writablePartitions: []*topicPartition{tpNew, tpOld},
	})

	req, txnReq, _ := s.createReq(5, 0)
	if len(req.batches.bs) == 0 {
		t.Fatal("createReq staged no batches")
	}
	if txnReq == nil {
		t.Fatal("createReq built no AddPartitionsToTxn request")
	}
	if !txnReqContains(txnReq, "t", 0) {
		t.Fatal("newly-drained partition t/0 missing from the AddPartitionsToTxn request")
	}
	if txnReqContains(txnReq, "t", 1) {
		t.Fatal("already-added partition t/1 must not be re-added to the AddPartitionsToTxn request")
	}
	if !rNew.addedToTxn.Load() || !rOld.addedToTxn.Load() {
		t.Fatal("both partitions should be marked addedToTxn after createReq")
	}
	if rNew.inflight != 1 || rOld.inflight != 1 {
		t.Fatalf("expected inflight 1 on both after createReq, got new=%d old=%d", rNew.inflight, rOld.inflight)
	}

	// The producer-ID/epoch recheck fired: rewind the staged drain.
	req.undoStagedBatches(txnReq)

	if rNew.addedToTxn.Load() {
		t.Fatal("addedToTxn still set on the newly-added partition after rewind: " +
			"the next drain skips its AddPartitionsToTxn and the broker rejects the produce (INVALID_TXN_STATE)")
	}
	if !rOld.addedToTxn.Load() {
		t.Fatal("addedToTxn cleared on an earlier-acked partition after rewind: " +
			"EndTransaction's anyAdded walk would skip EndTxn (07525c50 invariant)")
	}
	if rNew.inflight != 0 || rOld.inflight != 0 {
		t.Fatalf("inflight not rewound, got new=%d old=%d", rNew.inflight, rOld.inflight)
	}
	if rNew.batchDrainIdx != 0 || rOld.batchDrainIdx != 0 {
		t.Fatalf("batchDrainIdx not rewound, got new=%d old=%d", rNew.batchDrainIdx, rOld.batchDrainIdx)
	}
}
