package kgo

import (
	"bytes"
	"compress/gzip"
	"encoding/binary"
	"hash/crc32"
	"testing"

	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// encodeLegacyMsg encodes one v0/v1 message: offset, size, then crc over
// magic..end. For magic 1, ts sits between attributes and the key.
func encodeLegacyMsg(magic int8, offset int64, attrs int8, ts int64, value []byte) []byte {
	var body []byte
	body = append(body, byte(magic), byte(attrs))
	if magic == 1 {
		body = kbin.AppendInt64(body, ts)
	}
	body = kbin.AppendInt32(body, -1) // nil key
	body = kbin.AppendNullableBytes(body, value)

	msg := kbin.AppendInt64(nil, offset)
	msg = kbin.AppendInt32(msg, int32(4+len(body))) // crc + body
	msg = kbin.AppendUint32(msg, crc32.ChecksumIEEE(body))
	return append(msg, body...)
}

// wrapLegacySet gzips the concatenated inner messages into a compressed
// wrapper message of the given magic at the given wrapper offset.
func wrapLegacySet(magic int8, wrapperOffset int64, inner ...[]byte) []byte {
	var raw []byte
	for _, in := range inner {
		raw = append(raw, in...)
	}
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	gz.Write(raw)
	gz.Close()
	return encodeLegacyMsg(magic, wrapperOffset, 0x01 /* gzip */, 0, buf.Bytes())
}

func processLegacySet(t *testing.T, askOffset int64, set []byte) (FetchPartition, int64) {
	t.Helper()
	rp := &kmsg.FetchResponseTopicPartition{
		Partition:     0,
		HighWatermark: 1000,
		RecordBatches: set,
	}
	opts := ProcessFetchPartitionOpts{
		Offset:    askOffset,
		Topic:     "t",
		Partition: 0,
	}
	return ProcessFetchPartition(opts, rp, DefaultDecompressor(), nil)
}

// The log cleaner preserves retained inner messages' original offsets, so
// compacted compressed v0/v1 message sets legally contain offset gaps. The
// stored offsets are the truth: v1 wrappers store inner offsets relative to
// the set's first offset with the wrapper carrying the last inner message's
// absolute offset; v0 wrappers store absolute inner offsets. Kafka and
// librdkafka both honor the stored offsets; we used to relabel contiguously
// counting back from the wrapper offset, mislabeling everything before a
// gap, so a mid-set commit skipped real records for any other client that
// resumed from it.
func TestLegacyMessageSetInnerOffsets(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name    string
		ask     int64
		set     []byte
		want    []int64
		wantErr bool
	}{
		{
			// Compacted v1: originals 10..20 with 13..19 removed;
			// retained relatives [0 1 2 10], wrapper carries the
			// last absolute (20). Old code yielded 17 18 19 20.
			name: "v1 compacted gap",
			ask:  10,
			set: wrapLegacySet(1, 20,
				encodeLegacyMsg(1, 0, 0, 0, []byte("a")),
				encodeLegacyMsg(1, 1, 0, 0, []byte("b")),
				encodeLegacyMsg(1, 2, 0, 0, []byte("c")),
				encodeLegacyMsg(1, 10, 0, 0, []byte("d")),
			),
			want: []int64{10, 11, 12, 20},
		},
		{
			// The common non-compacted case: relatives 0..2,
			// wrapper 12 -> absolutes 10..12.
			name: "v1 contiguous",
			ask:  10,
			set: wrapLegacySet(1, 12,
				encodeLegacyMsg(1, 0, 0, 0, []byte("a")),
				encodeLegacyMsg(1, 1, 0, 0, []byte("b")),
				encodeLegacyMsg(1, 2, 0, 0, []byte("c")),
			),
			want: []int64{10, 11, 12},
		},
		{
			// Resuming mid-set past a gap: only offsets >= 12 kept.
			name: "v1 mid-set resume",
			ask:  12,
			set: wrapLegacySet(1, 20,
				encodeLegacyMsg(1, 0, 0, 0, []byte("a")),
				encodeLegacyMsg(1, 1, 0, 0, []byte("b")),
				encodeLegacyMsg(1, 2, 0, 0, []byte("c")),
				encodeLegacyMsg(1, 10, 0, 0, []byte("d")),
			),
			want: []int64{12, 20},
		},
		{
			// Wrapper offset 0: inner offsets are used as-is
			// (certain versions of librdkafka produced this).
			name: "v1 wrapper offset zero",
			ask:  0,
			set: wrapLegacySet(1, 0,
				encodeLegacyMsg(1, 0, 0, 0, []byte("a")),
				encodeLegacyMsg(1, 1, 0, 0, []byte("b")),
			),
			want: []int64{0, 1},
		},
		{
			// A wrapper offset below the last inner offset is an
			// invalid set: error, no guessing.
			name: "v1 wrapper below last inner",
			ask:  0,
			set: wrapLegacySet(1, 5,
				encodeLegacyMsg(1, 0, 0, 0, []byte("a")),
				encodeLegacyMsg(1, 10, 0, 0, []byte("b")),
			),
			wantErr: true,
		},
		{
			// v0 wrappers carry ABSOLUTE inner offsets (magic-0-era
			// brokers rewrote sets server-side); gaps preserved.
			name: "v0 compacted gap",
			ask:  10,
			set: wrapLegacySet(0, 20,
				encodeLegacyMsg(0, 10, 0, 0, []byte("a")),
				encodeLegacyMsg(0, 11, 0, 0, []byte("b")),
				encodeLegacyMsg(0, 12, 0, 0, []byte("c")),
				encodeLegacyMsg(0, 20, 0, 0, []byte("d")),
			),
			want: []int64{10, 11, 12, 20},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fp, next := processLegacySet(t, tc.ask, tc.set)
			if tc.wantErr {
				if fp.Err == nil {
					t.Fatal("expected an error, got none")
				}
				return
			}
			if fp.Err != nil {
				t.Fatalf("unexpected error: %v", fp.Err)
			}
			var got []int64
			for _, r := range fp.Records {
				got = append(got, r.Offset)
			}
			if len(got) != len(tc.want) {
				t.Fatalf("got offsets %v, want %v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Fatalf("got offsets %v, want %v", got, tc.want)
				}
			}
			if wantNext := tc.want[len(tc.want)-1] + 1; next != wantNext {
				t.Errorf("got next offset %d, want %d", next, wantNext)
			}
		})
	}
}

// encodeV2Batch builds a v2 RecordBatch frame with a correct CRC. records is
// the raw records section; numRecords is what the batch header CLAIMS.
func encodeV2Batch(firstOffset int64, lastOffsetDelta, numRecords int32, records []byte) []byte {
	b := kmsg.RecordBatch{
		FirstOffset:          firstOffset,
		PartitionLeaderEpoch: 0,
		Magic:                2,
		LastOffsetDelta:      lastOffsetDelta,
		FirstTimestamp:       0,
		MaxTimestamp:         0,
		ProducerID:           -1,
		ProducerEpoch:        -1,
		FirstSequence:        -1,
		NumRecords:           numRecords,
		Records:              records,
	}
	raw := b.AppendTo(nil)
	// Fix up Length (bytes after offset+length fields) and CRC (crc32c
	// over the bytes after the CRC field, i.e. from attributes onward,
	// which is byte 21).
	binary.BigEndian.PutUint32(raw[8:], uint32(len(raw)-12))
	crc := crc32.Checksum(raw[21:], crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(raw[17:], crc)
	return raw
}

// A CRC-valid v2 batch claiming records with ZERO record bytes is corrupt:
// the record-count clamp used to make it decode zero records "successfully"
// and the KAFKA-5443 defer then advanced the offset past the batch's whole
// claimed range, silently skipping offsets the broker asserts hold records.
// The legitimate compacted-empty batch claims zero records and must still
// advance.
func TestEmptyBatchClaimingRecords(t *testing.T) {
	t.Parallel()

	process := func(set []byte, ask int64) (FetchPartition, int64) {
		rp := &kmsg.FetchResponseTopicPartition{
			Partition:     0,
			HighWatermark: 1000,
			RecordBatches: set,
		}
		opts := ProcessFetchPartitionOpts{Offset: ask, Topic: "t", Partition: 0}
		return ProcessFetchPartition(opts, rp, DefaultDecompressor(), nil)
	}

	// Corrupt: claims 5 records, zero record bytes.
	fp, next := process(encodeV2Batch(10, 4, 5, nil), 10)
	if fp.Err == nil {
		t.Error("corrupt batch (claimed records, no bytes) produced no error")
	}
	if next != 10 {
		t.Errorf("corrupt batch advanced the offset to %d; want unmoved at 10", next)
	}

	// Legitimate KAFKA-5443 compacted-empty batch: claims zero, advances.
	fp, next = process(encodeV2Batch(10, 4, 0, nil), 10)
	if fp.Err != nil {
		t.Errorf("compacted-empty batch errored: %v", fp.Err)
	}
	if next != 15 {
		t.Errorf("compacted-empty batch advanced to %d; want 15 (past the preserved last offset)", next)
	}
}
