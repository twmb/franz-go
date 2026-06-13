package kfake

// Round 19 of the FRANZ_AUDIT.md program: hostile-broker defense-in-depth.
// These exercise the public kgo.ProcessFetchPartition with wire bytes a buggy
// or malicious broker can serialize. Tests whose fatal message starts with BUG
// REPRODUCED panic (crashing the fetch goroutine, and the process) against the
// pre-fix client and flip to a graceful parse against the fix.

import (
	"encoding/binary"
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
	in[16] = 2                                        // record-batch magic, so we reach check()

	rp := kmsg.NewFetchResponseTopicPartition()
	rp.RecordBatches = in

	fp, _ := kgo.ProcessFetchPartition(kgo.ProcessFetchPartitionOpts{Offset: 0}, &rp, nil, nil)
	if len(fp.Records) != 0 {
		t.Fatalf("expected no records from a corrupt batch, got %d", len(fp.Records))
	}
}
