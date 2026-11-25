package kgo

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
)

// We do not know the version that a produce request is written at while we
// build the request, so createReq counts each part at the largest size that
// any version writes the part at. We build requests of many shapes and check
// that no version writes more bytes than createReq counted.
//
// In the last rows, a batch's partition moves to a sink of another version
// after the batch is built. A batch built for a record batch version is
// bounded by its record batch size, so its message set can be larger than
// any request: the request takes the batch alone, and if we write the
// request as a message set, appendReq refuses the request and the records
// fail. A batch built before the version is known stays within the batch
// limit as a message set. The other tests of a fresh sink cover merged
// batches, which are never sized as message sets.
func TestProduceRequestCount(t *testing.T) {
	t.Parallel()
	long := strings.Repeat("t", 249)
	for _, tc := range []struct {
		name       string
		opts       []Opt
		topics     []string
		partitions int32 // per topic
		records    int   // per partition, each with a one byte value
		limit      int32 // of a request; a batch's limit is half

		built, moved int32 // the produce version of the sink the batches are built on, and moved to
		batches      int   // in the request
		refused      bool  // written at the moved version, the request is over the limit
	}{
		{name: "short topics by ID", topics: []string{"a", "b"}, partitions: 3, records: 1, limit: 1 << 20, built: 13, moved: 13, batches: 6},
		{name: "long topics counted for IDs", topics: []string{long, long[1:]}, partitions: 3, records: 1, limit: 1 << 20, built: 13, moved: 13, batches: 6},
		{name: "long topics, flexible", topics: []string{long, long[1:]}, partitions: 3, records: 1, limit: 1 << 20, built: 9, moved: 9, batches: 6},
		{name: "long topics, version unknown", topics: []string{long, long[1:]}, partitions: 3, records: 1, limit: 1 << 20, built: -1, moved: -1, batches: 6},
		{name: "message sets", topics: []string{long, "b"}, partitions: 3, records: 1, limit: 1 << 20, built: 2, moved: 2, batches: 6},
		{name: "130 partitions", topics: []string{"a"}, partitions: 130, records: 1, limit: 1 << 20, built: 13, moved: 13, batches: 130},
		{name: "130 partitions, non-flexible", topics: []string{"a"}, partitions: 130, records: 1, limit: 1 << 20, built: 3, moved: 3, batches: 130},
		{
			name: "long client and transactional IDs", opts: []Opt{ClientID(long), TransactionalID(long)},
			topics: []string{"a", long}, partitions: 2, records: 1, limit: 1 << 20, built: 9, moved: 9, batches: 4,
		},
		{name: "full request", topics: []string{"a", long}, partitions: 20, records: 1, limit: 2048, built: 13, moved: 13, batches: 22},

		// About 900 bytes as a record batch, and 3500 as a message set.
		{name: "moved to an unknown version", topics: []string{"a"}, partitions: 1, records: 100, limit: 2048, built: 9, moved: -1, batches: 1},
		{name: "moved to message sets", topics: []string{"a"}, partitions: 1, records: 100, limit: 2048, built: 9, moved: 2, batches: 1, refused: true},
		{name: "built at an unknown version", topics: []string{"a"}, partitions: 1, records: 100, limit: 2048, built: -1, moved: -1, batches: 1},
	} {
		// Uncompressed, a batch is written at its largest size.
		cl, s, r0 := sinkHarness(t, append([]Opt{ProducerBatchCompression(NoCompression())}, tc.opts...)...)
		cl.cfg.maxBrokerWriteBytes = tc.limit
		s.removeRecBuf(r0)
		s.produceVersion.Store(tc.built)
		fresh := cl.newSink(2)
		fresh.produceVersion.Store(tc.moved)
		failed := make(chan error, 1)
		for _, topic := range tc.topics {
			for partition := range tc.partitions {
				r := &recBuf{cl: cl, topic: topic, topicID: [16]byte{1}, partition: partition, maxRecordBatchBytes: tc.limit / 2, recBufsIdx: -1, lastAckedOffset: -1, sink: s}
				r.lingerFn = r.unlingerAndManuallyDrain
				s.addRecBuf(r)
				for range tc.records {
					r.bufferRecord(promisedRec{ctx: context.Background(), promise: func(_ *Record, err error) {
						select {
						case failed <- err:
						default:
						}
					}, Record: &Record{Value: []byte("v"), Context: context.Background()}}, false)
				}
				for _, b := range r.batches {
					if tc.built < 3 && b.v1wireLength > r.maxRecordBatchBytes {
						t.Errorf("%s: a batch is %d bytes as a message set, over the limit of %d", tc.name, b.v1wireLength, r.maxRecordBatchBytes)
					}
				}
				s.removeRecBuf(r)
				r.mu.Lock()
				r.sink = fresh
				r.mu.Unlock()
				fresh.addRecBuf(r)
			}
		}

		req, _, _ := fresh.createReq(5, 0)
		var batches int
		for _, partitions := range req.batches.bs {
			batches += len(partitions)
		}
		if batches != tc.batches || batches > 1 && req.wireLength > tc.limit {
			t.Errorf("%s: request has %d batches counted at %d bytes, want %d batches within %d bytes", tc.name, batches, req.wireLength, tc.batches, tc.limit)
		}
		lo := int16(3) // the count of a sink at v3 or higher is for record batches only
		if tc.moved < 3 {
			lo = 0
		}
		for v := lo; v <= 13; v++ {
			req.SetVersion(v)
			if n := int32(len(cl.reqFormatter.AppendRequest(nil, req, 0))); n > req.wireLength {
				t.Errorf("%s: v%d writes %d bytes, more than the %d counted", tc.name, v, n, req.wireLength)
			}
		}

		if !tc.refused {
			continue
		}
		req.SetVersion(int16(tc.moved))
		_, err := (&brokerCxn{cl: cl}).appendReq(req)
		if !errors.Is(err, errRequestTooLarge) {
			t.Errorf("%s: appendReq returned %v, want errRequestTooLarge", tc.name, err)
			continue
		}
		fresh.handleReqClientErr(req, err)
		select {
		case err := <-failed:
			if !errors.Is(err, kerr.MessageTooLarge) {
				t.Errorf("%s: records failed with %v, want MESSAGE_TOO_LARGE", tc.name, err)
			}
		case <-time.After(5 * time.Second):
			t.Errorf("%s: records did not fail", tc.name)
		}
	}
}

// TestAuditTxnRecheckRewindUnmarksAddedToTxn guards the producer-ID/epoch
// recheck rewind in produce(): when a TV1 transactional drain stages a request
// and then bails because the producer ID changed (a parallel sink bumped the
// epoch on EndTxn or failed the id), undoStagedBatches must un-mark addedToTxn
// for the partitions this request newly added to the transaction.
//
// If it does not, the partition stays marked-added client-side while its
// AddPartitionsToTxn was never sent. txnReqBuilder.add skips already-added
// partitions, so the next drain produces to that partition without ever adding
// it to the coordinator's transaction: a verifying broker rejects the produce
// with INVALID_TXN_STATE, and a non-verifying broker leaves the records in a
// transaction the coordinator never learned the partition belongs to.
//
// The companion invariant: a partition added to the transaction by an earlier
// request is a broker-acked fact deliberately absent from this request's
// txnReq, and must keep its membership across the rewind -- else
// EndTransaction's anyAdded walk skips EndTxn entirely.
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
