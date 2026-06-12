package kfake

// Round 5 of the FRANZ_AUDIT.md program: subsystem sweep of pkg/kgo/source.go
// + record_and_fetch.go (the fetch path: sessions, OOR/OFLE arms, per-partition
// error routing, batch parsing). Tests whose fatal message starts with BUG
// REPRODUCED fail against the pre-fix client and flip to passing with the
// source-sweep fixes.

import (
	"context"
	"encoding/binary"
	"hash/crc32"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// buildRecordBatchBytes hand-builds the wire bytes of a single v2 record batch
// with the given NumRecords and raw record bytes, patching the Length and a
// valid Castagnoli CRC so the batch passes the client's default CRC check. A
// malicious or buggy broker can serialize any NumRecords with a correct CRC;
// CRC validation only defends against accidental in-transit corruption.
func buildRecordBatchBytes(numRecords int32, records []byte) []byte {
	rb := kmsg.RecordBatch{
		FirstOffset:          0,
		PartitionLeaderEpoch: -1,
		Magic:                2,
		Attributes:           0,
		LastOffsetDelta:      0,
		FirstTimestamp:       0,
		MaxTimestamp:         0,
		ProducerID:           -1,
		ProducerEpoch:        -1,
		FirstSequence:        -1,
		NumRecords:           numRecords,
		Records:              records,
	}
	raw := rb.AppendTo(nil)
	// Length covers every byte after FirstOffset(8)+Length(4).
	binary.BigEndian.PutUint32(raw[8:12], uint32(len(raw)-12))
	// The batch CRC (Castagnoli) covers from Attributes (byte 21) onward.
	crc := crc32.Checksum(raw[21:], crc32.MakeTable(crc32.Castagnoli))
	binary.BigEndian.PutUint32(raw[17:21], crc)
	return raw
}

// TestAuditFetchNegativeRecordCountNoPanic verifies that a record batch
// claiming a negative record count is rejected as a corrupt batch rather than
// panicking. Pre-fix, processRecordBatch passes batch.NumRecords straight to
// ensureLen, whose s[:n] panics for n<0, crashing the fetch goroutine (and the
// process). Java's DefaultRecordBatch guards this with an InvalidRecordException
// ("Found invalid record count").
func TestAuditFetchNegativeRecordCountNoPanic(t *testing.T) {
	t.Parallel()

	rp := kmsg.NewFetchResponseTopicPartition()
	rp.Partition = 0
	rp.RecordBatches = buildRecordBatchBytes(-1, nil)

	// Pre-fix this call panics inside ensureLen; an unrecovered panic fails
	// the test. Post-fix it returns a parse error with no records.
	fp, _ := kgo.ProcessFetchPartition(kgo.ProcessFetchPartitionOpts{Offset: 0}, &rp, nil, nil)
	if fp.Err == nil {
		t.Fatal("BUG REPRODUCED: a batch with a negative record count was accepted instead of erroring")
	}
	if len(fp.Records) != 0 {
		t.Fatalf("expected no records from a corrupt batch, got %d", len(fp.Records))
	}
}

// TestAuditFetchHugeRecordCountBounded verifies that a record batch claiming far
// more records than its bytes can hold does not drive a giant up-front
// allocation. The count is clamped to the available record bytes (every record
// is >= 1 byte), so this completes promptly rather than trying to allocate
// ~1M kmsg.Record structs from a 16-byte batch.
func TestAuditFetchHugeRecordCountBounded(t *testing.T) {
	t.Parallel()

	rp := kmsg.NewFetchResponseTopicPartition()
	rp.Partition = 0
	rp.RecordBatches = buildRecordBatchBytes(1<<20, []byte{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0})

	fp, _ := kgo.ProcessFetchPartition(kgo.ProcessFetchPartitionOpts{Offset: 0}, &rp, nil, nil)
	// The 16 garbage bytes decode to no valid records; the point is that we
	// neither panic nor over-allocate.
	if len(fp.Records) != 0 {
		t.Fatalf("expected no decodable records from garbage bytes, got %d", len(fp.Records))
	}
}

// TestAuditPreferredReplicaUnknownBrokerNoStrand verifies that a fetch response
// redirecting a partition to a preferred read replica the client has not yet
// learned from metadata does NOT permanently strand the cursor.
//
// Trace (pre-fix): the client adds the cursor to the fetch request (use() =>
// unusable), the broker responds with PreferredReadReplica = an unknown node,
// fetch() deletes the cursor from the request's used offsets and calls move().
// move() finds no source for the unknown node, triggers a metadata update, and
// returns WITHOUT re-enabling the cursor. The cursor is now unusable and owned
// by nobody: the leader is unchanged so no migration re-enables it, and a
// metadata refresh that merely learns the new broker never touches cursor
// usability. The partition is silently never consumed again.
func TestAuditPreferredReplicaUnknownBrokerNoStrand(t *testing.T) {
	t.Parallel()

	const (
		topic            = "strand-topic"
		producedMessages = 5
		unknownBroker    = int32(999999)
	)

	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	produceN(t, c, topic, producedMessages)

	ti := c.TopicInfo(topic)
	pi := c.PartitionInfo(topic, 0)

	// The first fetch the consumer issues is hijacked into a preferred-replica
	// move to an unknown broker, stranding the cursor pre-fix. Every later
	// fetch is served normally by kfake so the post-fix client (which keeps
	// consuming from the leader) can make progress.
	var stranded atomic.Bool
	c.ControlKey(int16(kmsg.Fetch), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		req := kreq.(*kmsg.FetchRequest)
		if req.Version < 11 {
			t.Errorf("test needs fetch v11+ for preferred read replicas, got v%d", req.Version)
			return nil, nil, false
		}
		if stranded.Swap(true) {
			return nil, nil, false // already stranded once; serve real data
		}

		resp := req.ResponseKind().(*kmsg.FetchResponse)
		rt := kmsg.NewFetchResponseTopic()
		rt.Topic = topic
		rt.TopicID = ti.TopicID
		rp := kmsg.NewFetchResponseTopicPartition()
		rp.Partition = 0
		rp.ErrorCode = 0
		rp.HighWatermark = pi.HighWatermark
		rp.LastStableOffset = pi.LastStableOffset
		rp.LogStartOffset = 0
		rp.PreferredReadReplica = unknownBroker // a node the client has never seen
		rt.Partitions = append(rt.Partitions, rp)
		resp.Topics = append(resp.Topics, rt)
		return resp, nil, true
	})

	cl := newPlainClient(t, c,
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableFetchSessions(),
		kgo.Rack("audit-rack"),
		kgo.FetchMaxWait(100*time.Millisecond),
	)

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	var got int
	for ctx.Err() == nil && got < producedMessages {
		got += cl.PollFetches(ctx).NumRecords()
	}
	if got < producedMessages {
		t.Fatalf("BUG REPRODUCED: consumed %d/%d records; the cursor was stranded unusable by a preferred-replica move to an unknown broker", got, producedMessages)
	}
}
