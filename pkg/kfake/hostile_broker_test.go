package kfake

// Round 19 of the FRANZ_AUDIT.md program: hostile-broker defense-in-depth.
// These exercise the public kgo.ProcessFetchPartition with wire bytes a buggy
// or malicious broker can serialize. Tests whose fatal message starts with BUG
// REPRODUCED panic (crashing the fetch goroutine, and the process) against the
// pre-fix client and flip to a graceful parse against the fix.

import (
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestAuditFetchNegativeBatchLengthNoPanic: a batch whose 4-byte length field
// has the high bit set decodes to a negative int32 length. Pre-fix, the
// `len(in) < int(length)` truncation guard is false for a negative length, so
// check() runs in[:length] and panics with a negative slice bound. The sibling
// length-parse sites (broker.go parseReadSize, compression.go xerial) already
// guard `size < 0`.
func TestAuditFetchNegativeBatchLengthNoPanic(t *testing.T) {
	t.Parallel()

	in := make([]byte, 30)
	binary.BigEndian.PutUint32(in[8:12], 0x80000000) // batch length field; <0 as int32
	in[16] = 2                                       // record-batch magic, so we reach check()

	rp := kmsg.NewFetchResponseTopicPartition()
	rp.RecordBatches = in

	fp, _ := kgo.ProcessFetchPartition(kgo.ProcessFetchPartitionOpts{Offset: 0}, &rp, nil, nil)
	if len(fp.Records) != 0 {
		t.Fatalf("expected no records from a corrupt batch, got %d", len(fp.Records))
	}
}

// buildControlBatchBytes hand-builds a v2 control record batch: transactional
// (attr bit 4) + control (attr bit 5), with the given producer ID and the given
// pre-serialized control records, patching Length and a valid Castagnoli CRC.
func buildControlBatchBytes(producerID int64, numRecords int32, records []byte) []byte {
	rb := kmsg.RecordBatch{
		FirstOffset:          0,
		PartitionLeaderEpoch: -1,
		Magic:                2,
		Attributes:           0b0011_0000, // transactional | control
		LastOffsetDelta:      numRecords - 1,
		ProducerID:           producerID,
		ProducerEpoch:        0,
		FirstSequence:        -1,
		NumRecords:           numRecords,
		Records:              records,
	}
	raw := rb.AppendTo(nil)
	binary.BigEndian.PutUint32(raw[8:12], uint32(len(raw)-12))
	crc := crc32.Checksum(raw[21:], crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(raw[17:21], crc)
	return raw
}

// buildAbortMarkerRecord builds one control record whose key marks it as an
// ABORT marker (key is int16 version then int16 type; type 0 == abort).
func buildAbortMarkerRecord(offsetDelta int32) []byte {
	r := kmsg.Record{
		OffsetDelta: offsetDelta,
		Key:         []byte{0, 0, 0, 0}, // version=0, type=0 (abort)
	}
	// Record.Length is the byte count after the length varint; AppendTo writes
	// it verbatim. Serialize once with Length=0 (a 1-byte varint) to measure,
	// then set Length (still a 1-byte varint for this small record).
	raw := r.AppendTo(nil)
	r.Length = int32(len(raw) - 1)
	return r.AppendTo(nil)
}

// TestAuditFetchDoubleAbortMarkerNoPanic: a read_committed fetch whose control
// batch carries TWO abort markers for a producer that has ONE pending
// aborted-transaction entry. Pre-fix, the per-record loop calls
// aborter.trackAbortedPID once per marker record; the second call reslices
// a[pid][1:] on an already-deleted (nil) slice and panics the fetch goroutine.
// Java removes the producer id from a Set (idempotent) and inspects only a
// control batch's FIRST record.
func TestAuditFetchDoubleAbortMarkerNoPanic(t *testing.T) {
	t.Parallel()

	records := append(buildAbortMarkerRecord(0), buildAbortMarkerRecord(1)...)
	batch := buildControlBatchBytes(1, 2, records)

	rp := kmsg.NewFetchResponseTopicPartition()
	rp.Partition = 0
	at := kmsg.NewFetchResponseTopicPartitionAbortedTransaction()
	at.ProducerID = 1
	at.FirstOffset = 0
	rp.AbortedTransactions = []kmsg.FetchResponseTopicPartitionAbortedTransaction{at}
	rp.RecordBatches = batch

	opts := kgo.ProcessFetchPartitionOpts{Offset: 0, IsolationLevel: kgo.ReadCommitted()}
	fp, _ := kgo.ProcessFetchPartition(opts, &rp, nil, nil)
	if len(fp.Records) != 0 {
		t.Fatalf("expected control records to be dropped, got %d", len(fp.Records))
	}
}
