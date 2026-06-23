package kfake

// Round 21 of the FRANZ_AUDIT.md program: read_committed correctness.
// This exercises the public kgo.ProcessFetchPartition with wire bytes a real
// broker can serialize. It builds raw v2 record batches (see the buildBatch*
// helpers, sharing the CRC-patching shape of hostile_broker_test.go) so a test
// can hand a fetch response to the filtering code exactly as a broker would.

import (
	"encoding/binary"
	"hash/crc32"
	"slices"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// buildBatchV2 hand-builds a v2 record batch with the given producer id, first
// offset, and attributes (e.g. 0x10 transactional, 0x30 transactional+control),
// patching Length and a valid Castagnoli CRC.
func buildBatchV2(producerID, firstOffset int64, attributes int16, numRecords int32, records []byte) []byte {
	rb := kmsg.RecordBatch{
		FirstOffset:          firstOffset,
		PartitionLeaderEpoch: -1,
		Magic:                2,
		Attributes:           attributes,
		LastOffsetDelta:      numRecords - 1,
		ProducerID:           producerID,
		ProducerEpoch:        0,
		FirstSequence:        0,
		NumRecords:           numRecords,
		Records:              records,
	}
	raw := rb.AppendTo(nil)
	binary.BigEndian.PutUint32(raw[8:12], uint32(len(raw)-12))
	crc := crc32.Checksum(raw[21:], crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(raw[17:21], crc)
	return raw
}

// buildDataRecord builds one normal (non-control) record with the given offset
// delta and value.
func buildDataRecord(offsetDelta int32, value string) []byte {
	r := kmsg.Record{OffsetDelta: offsetDelta, Value: []byte(value)}
	// Record.Length is the byte count after the length varint; AppendTo writes
	// it verbatim. Serialize once with Length=0 (a 1-byte varint) to measure,
	// then set Length (still a 1-byte varint for these small records).
	raw := r.AppendTo(nil)
	r.Length = int32(len(raw) - 1)
	return r.AppendTo(nil)
}

// buildCommitMarkerRecord builds one control record whose key marks it a COMMIT
// marker (key is int16 version then int16 type; type 1 == commit).
func buildCommitMarkerRecord(offsetDelta int32) []byte {
	r := kmsg.Record{OffsetDelta: offsetDelta, Key: []byte{0, 0, 0, 1}}
	raw := r.AppendTo(nil)
	r.Length = int32(len(raw) - 1)
	return r.AppendTo(nil)
}

// TestAuditReadCommittedUnorderedAbortedTxns: a read_committed fetch whose
// AbortedTransactions list is NOT sorted by first offset for a single producer.
//
// Apache Kafka happens to return a producer's aborted transactions in
// first-offset order, but the wire contract does not require it and Redpanda
// does not: its newest in-memory aborted ranges (highest offsets) are
// concatenated ahead of older on-disk snapshot ranges, so one producer's
// entries can arrive highest-first-offset first. franz-go's shouldAbortBatch
// trusted aborter[pid][0] to be the smallest first offset, so the lower aborted
// transaction slipped past the `FirstOffset < pidAborts[0]` guard and its
// records were surfaced to the application -- a read_committed violation
// (aborted records delivered as committed).
//
// Here producer 1 has txn A (aborted, offsets 0-1), txn B (committed, 3-4), and
// txn C (aborted, 6-7), but the broker lists txn C's first offset (6) before
// txn A's (0). Only txn B's records may survive. Pre-fix the response also
// surfaces txn A's "a0"/"a1"; post-fix (buildAborter sorts each producer's
// first offsets) only "b0"/"b1" remain.
func TestAuditReadCommittedUnorderedAbortedTxns(t *testing.T) {
	t.Parallel()

	const pid = 1

	var batches []byte
	// txn A (ABORTED): data offsets 0-1, abort marker offset 2.
	batches = append(batches, buildBatchV2(pid, 0, 0b0001_0000, 2,
		append(buildDataRecord(0, "a0"), buildDataRecord(1, "a1")...))...)
	batches = append(batches, buildBatchV2(pid, 2, 0b0011_0000, 1, buildAbortMarkerRecord(0))...)
	// txn B (COMMITTED): data offsets 3-4, commit marker offset 5.
	batches = append(batches, buildBatchV2(pid, 3, 0b0001_0000, 2,
		append(buildDataRecord(0, "b0"), buildDataRecord(1, "b1")...))...)
	batches = append(batches, buildBatchV2(pid, 5, 0b0011_0000, 1, buildCommitMarkerRecord(0))...)
	// txn C (ABORTED): data offsets 6-7, abort marker offset 8.
	batches = append(batches, buildBatchV2(pid, 6, 0b0001_0000, 2,
		append(buildDataRecord(0, "c0"), buildDataRecord(1, "c1")...))...)
	batches = append(batches, buildBatchV2(pid, 8, 0b0011_0000, 1, buildAbortMarkerRecord(0))...)

	rp := kmsg.NewFetchResponseTopicPartition()
	rp.Partition = 0
	// Redpanda order: highest first offset (txn C, newest/in-memory) before the
	// lower one (txn A, older/snapshot). Both are for the same producer.
	mk := func(first int64) kmsg.FetchResponseTopicPartitionAbortedTransaction {
		at := kmsg.NewFetchResponseTopicPartitionAbortedTransaction()
		at.ProducerID = pid
		at.FirstOffset = first
		return at
	}
	rp.AbortedTransactions = []kmsg.FetchResponseTopicPartitionAbortedTransaction{mk(6), mk(0)}
	rp.RecordBatches = batches

	opts := kgo.ProcessFetchPartitionOpts{Offset: 0, IsolationLevel: kgo.ReadCommitted()}
	fp, _ := kgo.ProcessFetchPartition(opts, &rp, nil, nil)

	var got []string
	for _, r := range fp.Records {
		got = append(got, string(r.Value))
	}
	want := []string{"b0", "b1"} // only the committed transaction may survive
	if !slices.Equal(got, want) {
		t.Fatalf("read_committed surfaced %v, want %v; aborted records leaked because AbortedTransactions arrived out of first-offset order", got, want)
	}
}
