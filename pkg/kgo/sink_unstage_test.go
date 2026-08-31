package kgo

import (
	"context"
	"errors"
	"testing"
)

// Drives createReq / undoStagedBatches / finishBatch directly to guard the
// drain index accounting from #1385: undoStagedBatches must un-stage only the
// batch its request staged, because older requests for the same recBuf can
// still be inflight and each of their successes pops the head batch and
// decrements the drain index.

// unstageSinkHarness returns a client, a sink, and one recBuf on that sink,
// with the producer topics wired so Close can fail any still-buffered records.
func unstageSinkHarness(t *testing.T) (*Client, *sink, *recBuf) {
	t.Helper()

	cl, err := NewClient(
		SeedBrokers("127.0.0.1:1"), // metadata never loads; we drive the sink directly
		ManualFlushing(),           // keep buffered batches from spawning a real drain
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)

	cl.producer.id.Store(&producerID{id: 5, epoch: 0, err: nil})

	s := cl.newSink(1)
	s.produceVersion.Store(9)

	r := &recBuf{
		cl:                  cl,
		topic:               "t",
		partition:           0,
		maxRecordBatchBytes: 1 << 20,
		recBufsIdx:          -1,
		lastAckedOffset:     -1,
		sink:                s,
	}
	r.lingerFn = r.unlingerAndManuallyDrain // mirror metadata.go recBuf creation
	s.addRecBuf(r)

	cl.producer.topics.storeTopics([]string{"t"})
	tp := &topicPartition{records: r}
	cl.producer.topics.load()["t"].v.Store(&topicPartitionsData{
		topic:              "t",
		partitions:         []*topicPartition{tp},
		writablePartitions: []*topicPartition{tp},
	})

	return cl, s, r
}

func bufferOne(t *testing.T, r *recBuf) *recBatch {
	t.Helper()
	r.bufferRecord(promisedRec{
		ctx:     context.Background(),
		promise: func(*Record, error) {},
		Record:  &Record{Value: []byte("v"), Context: context.Background()},
	}, false)
	return r.batches[len(r.batches)-1]
}

func stageOne(t *testing.T, s *sink) *produceRequest {
	t.Helper()
	req, _, _ := s.createReq(5, 0)
	if n := len(req.batches.bs); n != 1 {
		t.Fatalf("createReq staged %d topics, expected 1", n)
	}
	return req
}

func TestUndoStagedBatchesWithInflight(t *testing.T) {
	t.Parallel()

	cl, s, r := unstageSinkHarness(t)

	// Three batches, three staged requests: A and B "inflight", C undone.
	b0 := bufferOne(t, r)
	stageOne(t, s)
	r.okOnSink = true // as if A's predecessor acked; allows pipelining
	b1 := bufferOne(t, r)
	stageOne(t, s)
	b2 := bufferOne(t, r)
	reqC := stageOne(t, s)

	if r.batchDrainIdx != 3 || r.inflight != 3 || r.seq != 3 {
		t.Fatalf("after staging: idx=%d inflight=%d seq=%d, expected 3/3/3", r.batchDrainIdx, r.inflight, r.seq)
	}

	// The producer-ID recheck bailed: C is never issued.
	reqC.undoStagedBatches(nil)
	if r.batchDrainIdx != 2 || r.inflight != 2 || r.seq != 2 {
		t.Fatalf("after undo: idx=%d inflight=%d seq=%d, expected 2/2/2", r.batchDrainIdx, r.inflight, r.seq)
	}

	// A and B succeed; each pops the head and decrements the drain index.
	for _, b := range []*recBatch{b0, b1} {
		r.mu.Lock()
		cl.finishBatch(b, 5, 0, 0, nil)
		b.decInflight()
		r.mu.Unlock()
	}
	if r.batchDrainIdx != 0 || r.inflight != 0 || len(r.batches) != 1 {
		t.Fatalf("after finishing A and B: idx=%d inflight=%d len=%d, expected 0/0/1", r.batchDrainIdx, r.inflight, len(r.batches))
	}

	// The next drain re-stages the undone batch; on the unfixed code the
	// drain index is -2 here and this indexes out of range.
	req := stageOne(t, s)
	staged := req.batches.bs["t"][0]
	if staged.recBatch != b2 {
		t.Fatal("restage did not pick up the undone batch")
	}
	if staged.seq != 2 || r.batchDrainIdx != 1 {
		t.Fatalf("restage: seq=%d idx=%d, expected 2/1", staged.seq, r.batchDrainIdx)
	}
}

func TestUndoStagedBatchesAfterFailAllRecords(t *testing.T) {
	t.Parallel()

	_, s, r := unstageSinkHarness(t)

	bufferOne(t, r)
	req := stageOne(t, s)

	// The buffer fails while the request is being built (e.g. a fatal error
	// on another request): the staged batch is gone before the undo runs.
	r.mu.Lock()
	r.failAllRecords(errors.New("fatal"))
	r.mu.Unlock()

	req.undoStagedBatches(nil)
	if r.batchDrainIdx != 0 || r.inflight != 0 || r.batches != nil {
		t.Fatalf("after undo of a failed buffer: idx=%d inflight=%d len=%d, expected 0/0/0", r.batchDrainIdx, r.inflight, len(r.batches))
	}
}

func TestCreateReqSkipsPendingSeqResetWithInflight(t *testing.T) {
	t.Parallel()

	cl, s, r := unstageSinkHarness(t)

	b0 := bufferOne(t, r)
	stageOne(t, s)
	r.okOnSink = true
	bufferOne(t, r)

	// A sequence reset lands (resetAllProducerSequences) while b0 is still
	// inflight. Staging b1 now would ship a nonzero sequence under the new
	// producer id, so createReq must skip the recBuf entirely.
	r.mu.Lock()
	r.needSeqReset = true
	r.mu.Unlock()
	req, _, _ := s.createReq(5, 0)
	if len(req.batches.bs) != 0 {
		t.Fatal("createReq staged a batch while a seq reset was pending with inflight batches")
	}

	// Once nothing is inflight, the head stages and consumes the reset.
	r.mu.Lock()
	cl.finishBatch(b0, 5, 0, 0, nil)
	b0.decInflight()
	r.mu.Unlock()

	req = stageOne(t, s)
	if staged := req.batches.bs["t"][0]; staged.seq != 0 {
		t.Fatalf("staged seq %d after reset, expected 0", staged.seq)
	}
	if r.needSeqReset {
		t.Fatal("needSeqReset not consumed by staging the first batch")
	}
}
