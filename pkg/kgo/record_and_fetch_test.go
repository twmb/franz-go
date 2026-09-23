package kgo

import (
	"context"
	"math/rand/v2"
	"reflect"
	"testing"

	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestEachTopicPreservesTopicID verifies that Fetches.EachTopic carries
// FetchTopic.TopicID through its grouping.
//
// A topic's partitions are led by different brokers, so the same topic is
// returned in separate Fetch entries (one per broker response). EachTopic
// groups those entries by topic name. Before the fix, the multi-fetch
// (len(fs) >= 2) grouping path rebuilt FetchTopic with a hard-coded zero
// TopicID, so any caller reading TopicID got zero whenever more than one
// broker replied -- nearly always in a real cluster, yet never in a
// single-broker test (which hits the len(fs) == 1 path that preserves the
// ID). The grouped TopicID must match the per-fetch TopicID.
func TestEachTopicPreservesTopicID(t *testing.T) {
	t.Parallel()

	fooID := [16]byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	barID := [16]byte{16, 15, 14, 13, 12, 11, 10, 9, 8, 7, 6, 5, 4, 3, 2, 1}

	collect := func(fs Fetches) (ids map[string][16]byte, nparts map[string]int) {
		ids = make(map[string][16]byte)
		nparts = make(map[string]int)
		fs.EachTopic(func(ft FetchTopic) {
			ids[ft.Topic] = ft.TopicID
			nparts[ft.Topic] += len(ft.Partitions)
		})
		return
	}

	t.Run("spread across multiple fetches", func(t *testing.T) {
		t.Parallel()
		// "foo" is led on two brokers, so it arrives in two Fetch entries;
		// both carry its TopicID. "bar" is in a single Fetch entry. This is
		// the multi-fetch grouping path (len(fs) == 2).
		fs := Fetches{
			{Topics: []FetchTopic{
				{Topic: "foo", TopicID: fooID, Partitions: []FetchPartition{{Partition: 0}}},
				{Topic: "bar", TopicID: barID, Partitions: []FetchPartition{{Partition: 0}}},
			}},
			{Topics: []FetchTopic{
				{Topic: "foo", TopicID: fooID, Partitions: []FetchPartition{{Partition: 1}}},
			}},
		}
		ids, nparts := collect(fs)
		if ids["foo"] != fooID { // dropped to zero pre-fix
			t.Errorf("foo TopicID: got %v, want %v", ids["foo"], fooID)
		}
		if ids["bar"] != barID {
			t.Errorf("bar TopicID: got %v, want %v", ids["bar"], barID)
		}
		if nparts["foo"] != 2 { // both fetches' partitions grouped
			t.Errorf("foo partitions: got %d, want 2", nparts["foo"])
		}
	})

	t.Run("single fetch preserves id", func(t *testing.T) {
		t.Parallel()
		// The len(fs) == 1 fast path already preserved the ID; guard it
		// against regressions.
		fs := Fetches{
			{Topics: []FetchTopic{
				{Topic: "foo", TopicID: fooID, Partitions: []FetchPartition{{Partition: 0}}},
			}},
		}
		ids, _ := collect(fs)
		if ids["foo"] != fooID {
			t.Errorf("foo TopicID: got %v, want %v", ids["foo"], fooID)
		}
	})

	t.Run("no id stays zero", func(t *testing.T) {
		t.Parallel()
		// A pre-3.1 broker (or a share fetch) returns no TopicID; EachTopic
		// must not fabricate one.
		fs := Fetches{
			{Topics: []FetchTopic{
				{Topic: "foo", Partitions: []FetchPartition{{Partition: 0}}},
			}},
			{Topics: []FetchTopic{
				{Topic: "foo", Partitions: []FetchPartition{{Partition: 1}}},
			}},
		}
		ids, _ := collect(fs)
		if ids["foo"] != noID {
			t.Errorf("foo TopicID: got %v, want zero", ids["foo"])
		}
	})
}

// TestNewRecordAttrs verifies NewRecordAttrs round-trips through the accessors.
func TestNewRecordAttrs(t *testing.T) {
	t.Parallel()

	for _, tc := range []struct {
		name string
		opts RecordAttrsOpts
	}{
		{"zero", RecordAttrsOpts{}},
		{"snappy create-time", RecordAttrsOpts{Codec: CodecSnappy}},
		{"zstd log-append-time", RecordAttrsOpts{Codec: CodecZstd, TimestampType: 1}},
		{"no-timestamp", RecordAttrsOpts{TimestampType: -1}},
		{"transactional", RecordAttrsOpts{Codec: CodecGzip, Transactional: true}},
		{"control", RecordAttrsOpts{Control: true}},
		{"all set", RecordAttrsOpts{Codec: CodecLz4, TimestampType: 1, Transactional: true, Control: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := NewRecordAttrs(tc.opts)
			if got := a.CompressionType(); got != uint8(tc.opts.Codec) {
				t.Errorf("CompressionType: got %d, want %d", got, uint8(tc.opts.Codec))
			}
			if got := a.TimestampType(); got != tc.opts.TimestampType {
				t.Errorf("TimestampType: got %d, want %d", got, tc.opts.TimestampType)
			}
			if got := a.IsTransactional(); got != tc.opts.Transactional {
				t.Errorf("IsTransactional: got %t, want %t", got, tc.opts.Transactional)
			}
			if got := a.IsControl(); got != tc.opts.Control {
				t.Errorf("IsControl: got %t, want %t", got, tc.opts.Control)
			}
		})
	}
}

// A record length whose varint overflows makes kbin.Varint report a negative
// number of bytes read; that must stop decoding rather than slice in[:total]
// with a negative total.
func TestReadRawRecordsOverflowingLength(t *testing.T) {
	in := []byte{0xff, 0xff, 0xff, 0xff, 0xff, 0x01, 0x02, 0x03}
	if n := readRawRecordsDirect(context.Background(), make([]Record, 2), in, "t", 0, new(kmsg.RecordBatch)); n != 0 {
		t.Errorf("decoded %d records, want none", n)
	}
}

// rawTestRecords encodes n random records with kmsg, mixing nil and empty
// keys, values, and header fields and varying header counts, returning the
// encoding and where each record ends in it.
func rawTestRecords(rng *rand.Rand, n int) ([]kmsg.Record, []byte, []int) {
	bytesOrNil := func() []byte {
		switch rng.IntN(4) {
		case 0:
			return nil
		case 1:
			return []byte{}
		default:
			b := make([]byte, 1+rng.IntN(40))
			for i := range b {
				b[i] = byte(rng.Uint32())
			}
			return b
		}
	}
	var (
		krs  []kmsg.Record
		raw  []byte
		ends []int
	)
	for i := range n {
		r := kmsg.Record{
			TimestampDelta64: rng.Int64N(1<<40) - 1<<39,
			OffsetDelta:      int32(i),
			Key:              bytesOrNil(),
			Value:            bytesOrNil(),
		}
		for range rng.IntN(4) {
			r.Headers = append(r.Headers, kmsg.Header{Key: string(bytesOrNil()), Value: bytesOrNil()})
		}
		r.Length = int32(len(r.AppendTo(nil)) - 1)
		raw = r.AppendTo(raw)
		krs = append(krs, r)
		ends = append(ends, len(raw))
	}
	return krs, raw, ends
}

func TestReadRawRecordsDirect(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	rng := rand.New(rand.NewPCG(1, 2))
	for range 500 {
		n := rng.IntN(20)
		krs, raw, ends := rawTestRecords(rng, n)
		batch := &kmsg.RecordBatch{
			FirstOffset:          rng.Int64N(1 << 40),
			Attributes:           int16(rng.IntN(1 << 7)), // bits 0-6 are defined; kgo uses bit 7 internally for v0 messages
			FirstTimestamp:       rng.Int64N(1 << 42),
			MaxTimestamp:         rng.Int64N(1 << 42),
			ProducerID:           rng.Int64N(1 << 20),
			ProducerEpoch:        int16(rng.IntN(1 << 15)),
			PartitionLeaderEpoch: rng.Int32N(1 << 20),
		}
		if rng.IntN(10) == 0 {
			batch.FirstOffset = -1
		}

		// Every record decodes to what we encoded.
		rs := make([]Record, n+rng.IntN(3))
		if got := readRawRecordsDirect(ctx, rs, raw, "topic", 7, batch); got != n {
			t.Fatalf("decoded %d records, want %d", got, n)
		}
		for i, kr := range krs {
			r := &rs[i]
			wantOffset := batch.FirstOffset + int64(kr.OffsetDelta)
			if batch.FirstOffset == -1 {
				wantOffset = -1
			}
			wantTs := batch.FirstTimestamp + kr.TimestampDelta64
			if batch.Attributes&0b1000 != 0 { // log append time
				wantTs = batch.MaxTimestamp
			}
			if !reflect.DeepEqual(r.Key, kr.Key) ||
				!reflect.DeepEqual(r.Value, kr.Value) ||
				r.Topic != "topic" ||
				r.Partition != 7 ||
				r.Attrs != (RecordAttrs{uint8(batch.Attributes)}) ||
				r.ProducerID != batch.ProducerID ||
				r.ProducerEpoch != batch.ProducerEpoch ||
				r.LeaderEpoch != batch.PartitionLeaderEpoch ||
				r.Offset != wantOffset ||
				r.Timestamp.UnixMilli() != wantTs ||
				r.Context != ctx {
				t.Fatalf("record %d decoded to %+v from %+v in %+v", i, *r, kr, *batch)
			}
			if len(r.Headers) != len(kr.Headers) || (len(kr.Headers) == 0 && r.Headers != nil) {
				t.Fatalf("record %d headers %v, want %v", i, r.Headers, kr.Headers)
			}
			for j, kh := range kr.Headers {
				if h := r.Headers[j]; h.Key != kh.Key || !reflect.DeepEqual(h.Value, kh.Value) {
					t.Fatalf("record %d header %d is %+v, want %+v", i, j, h, kh)
				}
			}
		}

		// Truncating anywhere decodes exactly the whole records before
		// the cut, and fewer slots than records decodes that many.
		if len(raw) > 0 {
			cut := rng.IntN(len(raw))
			want := 0
			for want < n && ends[want] <= cut {
				want++
			}
			if got := readRawRecordsDirect(ctx, make([]Record, n), raw[:cut], "topic", 7, batch); got != want {
				t.Fatalf("decoded %d records cut at %d, want %d", got, cut, want)
			}
		}
		if n > 0 {
			slots := rng.IntN(n)
			if got := readRawRecordsDirect(ctx, make([]Record, slots), raw, "topic", 7, batch); got != slots {
				t.Fatalf("decoded %d records into %d slots", got, slots)
			}
		}
	}
}

// Any input decodes without panicking, into at most as many records as we
// have room for.
func FuzzReadRawRecordsDirect(f *testing.F) {
	rng := rand.New(rand.NewPCG(3, 4))
	for range 20 {
		_, raw, _ := rawTestRecords(rng, rng.IntN(10))
		f.Add(raw, uint8(rng.IntN(12)))
	}
	f.Add([]byte{0xff, 0xff, 0xff, 0xff, 0xff, 0x01}, uint8(2))
	f.Fuzz(func(t *testing.T, raw []byte, n uint8) {
		if got := readRawRecordsDirect(context.Background(), make([]Record, n), raw, "t", 0, new(kmsg.RecordBatch)); got > int(n) {
			t.Fatalf("decoded %d records into %d slots", got, n)
		}
	})
}
