package kfake

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// A fetch across many partitions returns batches only while the response has
// room, beyond the first batch, as a broker does.
func TestSyntheticFetchResponseBounded(t *testing.T) {
	t.Parallel()

	const nparts = 20
	c, err := NewCluster(NumBrokers(1), SeedTopics(nparts, "t"), SyntheticFetch(SyntheticBatch{}))
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx := context.Background()
	meta, err := kmsg.NewPtrMetadataRequest().RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	req := kmsg.NewPtrFetchRequest()
	req.MaxBytes = 3 << 20
	rt := kmsg.NewFetchRequestTopic()
	rt.Topic = "t"
	rt.TopicID = meta.Topics[0].TopicID
	for p := range int32(nparts) {
		rp := kmsg.NewFetchRequestTopicPartition()
		rp.Partition = p
		rp.PartitionMaxBytes = 1 << 20
		rt.Partitions = append(rt.Partitions, rp)
	}
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}

	var total, first, withData int
	for _, rt := range resp.Topics {
		for _, rp := range rt.Partitions {
			if n := len(rp.RecordBatches); n > 0 {
				if first == 0 {
					first = n
				}
				total += n
				withData++
			}
		}
	}
	if withData == 0 || withData == nparts {
		t.Errorf("got data from %d of %d partitions, want some but not all", withData, nparts)
	}
	if total > max(int(req.MaxBytes), first) {
		t.Errorf("got %d bytes of batches, over the %d max", total, req.MaxBytes)
	}
}

func TestSyntheticFetch(t *testing.T) {
	t.Parallel()

	given := timestampBatch([]int64{1, 2, 3})
	short := make([]byte, batchHeaderSize-1)
	badMagic := bytes.Clone(given)
	badMagic[16] = 1

	for _, test := range []struct {
		name    string
		b       SyntheticBatch
		wantErr string // when set, NewCluster must fail and name this

		atEnd       bool    // consume from syntheticEnd rather than from 0
		wantRecords int     // records per served batch, 0 takes what we built
		wantValue   int     // bytes per generated value
		givenValue  []byte  // exact value of every record of a given batch
		wantFrac    float64 // compressed batch over uncompressed, 0 skips
	}{
		{
			name:      "default",
			wantValue: 100,
		},
		{
			name:        "random half zstd",
			b:           SyntheticBatch{Records: 500, RecordBytes: 200, RandomFrac: 0.5, Compression: kgo.ZstdCompression()},
			wantRecords: 500,
			wantValue:   200,
			wantFrac:    0.5,
		},
		{
			name:        "given batch",
			b:           SyntheticBatch{Batch: given},
			wantRecords: 3,
			givenValue:  []byte("v"),
		},
		{
			name:        "at end",
			b:           SyntheticBatch{Records: 10, RecordBytes: 16},
			atEnd:       true,
			wantRecords: 10,
			wantValue:   16,
		},
		{
			name:    "batch with another field",
			b:       SyntheticBatch{Batch: given, Records: 1},
			wantErr: "another field is set alongside it",
		},
		{
			name:    "batch too short",
			b:       SyntheticBatch{Batch: short},
			wantErr: "below the 61 byte v2 record batch header",
		},
		{
			name:    "batch bad magic",
			b:       SyntheticBatch{Batch: badMagic},
			wantErr: "has magic 1",
		},
		{
			name:    "frac below zero",
			b:       SyntheticBatch{RandomFrac: -0.1},
			wantErr: "RandomFrac -0.1 is outside [0, 1]",
		},
		{
			name:    "frac above one",
			b:       SyntheticBatch{RandomFrac: 1.5},
			wantErr: "RandomFrac 1.5 is outside [0, 1]",
		},
		{
			name:    "negative records",
			b:       SyntheticBatch{Records: -1},
			wantErr: "Records -1 is negative",
		},
		{
			name:    "negative record bytes",
			b:       SyntheticBatch{RecordBytes: -1},
			wantErr: "RecordBytes -1 is negative",
		},
		{
			name:    "above the byte cap",
			b:       SyntheticBatch{Records: 1 << 20, RecordBytes: 1 << 10},
			wantErr: "above the 67108864 byte cap",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			c, err := NewCluster(NumBrokers(1), SeedTopics(1, "t"), SyntheticFetch(test.b))
			if test.wantErr != "" {
				if err == nil {
					c.Close()
					t.Fatalf("cluster started, want error %q", test.wantErr)
				}
				if !strings.Contains(err.Error(), test.wantErr) {
					t.Fatalf("error %q does not contain %q", err, test.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(c.Close)

			records := int(c.cfg.synthetic.records)
			if test.wantRecords != 0 && records != test.wantRecords {
				t.Fatalf("batch holds %d records, want %d", records, test.wantRecords)
			}
			reset, start := kgo.NewOffset().AtStart(), int64(0)
			if test.atEnd {
				reset, start = kgo.NewOffset().AtEnd(), syntheticEnd
			}
			cl := newPlainClient(t, c, kgo.ConsumeTopics("t"), kgo.ConsumeResetOffset(reset))
			for i, r := range consumeN(t, cl, 2*records+1, 20*time.Second) {
				if want := start + int64(i); r.Offset != want {
					t.Fatalf("record %d is at offset %d, want %d", i, r.Offset, want)
				}
				if test.givenValue != nil {
					if !bytes.Equal(r.Value, test.givenValue) {
						t.Fatalf("record %d has value %q, want %q", i, r.Value, test.givenValue)
					}
					continue
				}
				if len(r.Value) != test.wantValue {
					t.Fatalf("record %d has a %d byte value, want %d", i, len(r.Value), test.wantValue)
				}
				split := test.wantValue - int(float64(test.wantValue)*test.b.RandomFrac)
				want := make([]byte, split)
				formatValue(int64(i%records), want)
				if !bytes.Equal(r.Value[:split], want) {
					t.Fatalf("record %d begins %q, want %q", i, r.Value[:split], want)
				}
			}

			if test.wantFrac == 0 {
				return
			}
			plain := test.b
			plain.Compression = kgo.CompressionCodec{}
			s := &syntheticFetch{spec: plain}
			if err := s.init(); err != nil {
				t.Fatal(err)
			}
			frac := float64(len(c.cfg.synthetic.batch)) / float64(len(s.batch))
			if lo, hi := test.wantFrac*0.7, test.wantFrac+0.25; frac < lo || frac > hi {
				t.Errorf("compressed batch is %.2f of uncompressed, want within [%.2f, %.2f]", frac, lo, hi)
			}
		})
	}
}

func BenchmarkConsumeSynthetic(b *testing.B) {
	const recordBytes = 100
	for _, codec := range []struct {
		name string
		c    kgo.CompressionCodec
	}{
		{"none", kgo.NoCompression()},
		{"gzip", kgo.GzipCompression()},
		{"snappy", kgo.SnappyCompression()},
		{"lz4", kgo.Lz4Compression()},
		{"zstd", kgo.ZstdCompression()},
	} {
		b.Run(codec.name, func(b *testing.B) {
			c, err := NewCluster(
				NumBrokers(1),
				SeedTopics(4, "t"),
				BlackholeProduce(),
				SyntheticFetch(SyntheticBatch{
					RecordBytes: recordBytes,
					RandomFrac:  0.3,
					Compression: codec.c,
				}),
			)
			if err != nil {
				b.Fatal(err)
			}
			defer c.Close()
			cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...), kgo.ConsumeTopics("t"))
			if err != nil {
				b.Fatal(err)
			}
			defer cl.Close()

			b.SetBytes(recordBytes)
			b.ResetTimer()
			for n := 0; n < b.N; {
				fs := cl.PollFetches(context.Background())
				if errs := fs.Errors(); len(errs) > 0 {
					b.Fatal(errs)
				}
				n += fs.NumRecords()
			}
			b.ReportMetric(float64(b.Elapsed().Nanoseconds())/float64(b.N), "ns/record")
		})
	}
}
