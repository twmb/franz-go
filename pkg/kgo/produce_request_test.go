package kgo

import (
	"bytes"
	"hash/crc32"
	"testing"

	"github.com/twmb/franz-go/pkg/kbin"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// This file contains golden tests against kmsg AppendTo's to ensure our custom
// encoding is correct.

func TestPromisedNumberedRecordAppendTo(t *testing.T) {
	t.Parallel()
	// golden
	kmsgRec := kmsg.Record{
		Length:         1,
		TimestampDelta: 2,
		OffsetDelta:    3,
		Key:            []byte("key"),
		Value:          []byte("value"),
		Headers: []kmsg.Header{
			{Key: "header key 1", Value: []byte("header value 1")},
			{Key: "header key 2", Value: []byte("header value 2")},
		},
	}

	// input
	const pnrOffsetDelta = 3
	pnrRec := promisedNumberedRecord{
		recordNumbers: recordNumbers{
			lengthField:    1,
			timestampDelta: 2,
		},
		promisedRec: promisedRec{
			Record: &Record{
				Key:   []byte("key"),
				Value: []byte("value"),
				Headers: []RecordHeader{
					{Key: "header key 1", Value: []byte("header value 1")},
					{Key: "header key 2", Value: []byte("header value 2")},
				},
			},
		},
	}

	// compare
	exp := kmsgRec.AppendTo(nil)
	got := pnrRec.appendTo(nil, pnrOffsetDelta)

	if !bytes.Equal(got, exp) {
		t.Error("got != exp")
	}
}

func TestRecBatchAppendTo(t *testing.T) {
	t.Parallel()
	// golden, uncompressed
	kbatch := kmsg.RecordBatch{
		FirstOffset:          0,
		Length:               0, // set below,
		PartitionLeaderEpoch: -1,
		Magic:                2,
		CRC:                  0,      // fill below
		Attributes:           0x0010, // transactional bit set
		LastOffsetDelta:      1,
		FirstTimestamp:       20,
		MaxTimestamp:         24, // recBatch timestamp delta will be 4
		ProducerID:           12,
		ProducerEpoch:        11,
		FirstSequence:        10,
		NumRecords:           2,
		Records: append(
			(&kmsg.Record{
				Length:         1,
				TimestampDelta: 2,
				OffsetDelta:    0, // must be zero
				Key:            []byte("key 1"),
				Value:          []byte("value 1"),
				Headers: []kmsg.Header{
					{Key: "header key 1", Value: []byte("header value 1")},
					{Key: "header key 2", Value: []byte("header value 2")},
				},
			}).AppendTo(nil),
			(&kmsg.Record{
				Length:         3,
				TimestampDelta: 4,
				OffsetDelta:    1, // must be one
				Key:            []byte("key 2"),
				Value:          []byte("value 2"),
			}).AppendTo(nil)...),
	}

	// input
	ourBatch := seqRecBatch{
		seq: 10,
		recBatch: &recBatch{
			firstTimestamp: 20,
			records: []promisedNumberedRecord{
				{
					recordNumbers: recordNumbers{
						lengthField:    1,
						timestampDelta: 2,
					},
					promisedRec: promisedRec{
						Record: &Record{
							Key:   []byte("key 1"),
							Value: []byte("value 1"),
							Headers: []RecordHeader{
								{"header key 1", []byte("header value 1")},
								{"header key 2", []byte("header value 2")},
							},
						},
					},
				},
				{
					recordNumbers: recordNumbers{
						lengthField:    3,
						timestampDelta: 4,
					},
					promisedRec: promisedRec{
						Record: &Record{
							Key:   []byte("key 2"),
							Value: []byte("value 2"),
						},
					},
				},
			},
		},
	}

	version := int16(99)
	ourBatch.wireLength = 4 + int32(len(kbatch.AppendTo(nil))) // length prefix; required for flexible versioning

	// After compression, we fix the length & crc on kbatch.
	fixFields := func() {
		rawBatch := kbatch.AppendTo(nil)
		kbatch.Length = int32(len(rawBatch[8+4:]))                       // skip first offset (int64) and length
		kbatch.CRC = int32(crc32.Checksum(rawBatch[8+4+4+1+4:], crc32c)) // skip thru crc
	}

	var compressor *compressor
	var checkNum int
	check := func() {
		exp := kbatch.AppendTo(nil)
		gotFull, _ := ourBatch.appendTo(nil, version, 12, 11, true, compressor)
		lengthPrefix := 4
		ourBatchSize := (&kbin.Reader{Src: gotFull}).Int32()
		if version >= 9 {
			r := &kbin.Reader{Src: gotFull}
			ourBatchSize = int32(r.Uvarint()) - 1
			lengthPrefix = len(gotFull) - len(r.Src)
		}
		got := gotFull[lengthPrefix:]
		if ourBatchSize != int32(len(got)) {
			t.Errorf("check %d: incorrect record prefixing written length %d != actual %d", checkNum, ourBatchSize, len(got))
		}

		if !bytes.Equal(got, exp) {
			t.Errorf("check %d: got != exp\n%v\n%v\n", checkNum, got, exp)
		}
		checkNum++
	}

	// ***Uncompressed record batch check***

	fixFields()
	check()

	// ***Compressed record batch check***

	compressor, _ = newCompressor(CompressionCodec{codec: 2}) // snappy
	{
		kbatch.Attributes |= 0x0002 // snappy
		kbatch.Records, _ = compressor.compress(sliceWriters.Get().(*sliceWriter), kbatch.Records, version)
	}

	fixFields()
	check()

	// ***As a produce request***
	txid := "tx"
	kmsgReq := kmsg.ProduceRequest{
		Version:       version,
		TransactionID: &txid,
		Acks:          -1,
		TimeoutMillis: 1000,
		Topics: []kmsg.ProduceRequestTopic{{
			Topic: "topic",
			Partitions: []kmsg.ProduceRequestTopicPartition{{
				Partition: 1,
				Records:   kbatch.AppendTo(nil),
			}},
		}},
	}
	ourReq := produceRequest{
		version:       version,
		txnID:         &txid,
		acks:          -1,
		timeout:       1000,
		producerID:    12,
		producerEpoch: 11,
		compressor:    compressor,
	}
	ourReq.batches.addSeqBatch("topic", 1, ourBatch)

	exp := kmsgReq.AppendTo(nil)
	got := ourReq.AppendTo(nil)

	if !bytes.Equal(got, exp) {
		t.Errorf("produce request: got != exp\n%v\n%v\n", got, exp)
	}
}

func TestMessageSetAppendTo(t *testing.T) {
	t.Parallel()
	// golden v0, uncompressed
	kset01 := kmsg.MessageV0{
		Offset: 0,
		Key:    []byte("loooooong key 1"), // all keys/values have looooong prefix to allow compression to be shorter
		Value:  []byte("loooooong value 1"),
	}
	kset01.MessageSize = int32(len(kset01.AppendTo(nil)[12:]))
	kset01.CRC = int32(crc32.ChecksumIEEE(kset01.AppendTo(nil)[16:]))

	kset02 := kmsg.MessageV0{
		Offset: 1,
		Key:    []byte("loooooong key 2"),
		Value:  []byte("loooooong value 2"),
	}
	kset02.CRC = int32(crc32.ChecksumIEEE(kset02.AppendTo(nil)[16:]))
	kset02.MessageSize = int32(len(kset02.AppendTo(nil)[12:]))

	// golden v1, uncompressed
	kset11 := kmsg.MessageV1{
		Offset:    0,
		Magic:     1,
		Timestamp: 12,
		Key:       []byte("loooooong key 1"),
		Value:     []byte("loooooong value 1"),
	}
	kset11.CRC = int32(crc32.ChecksumIEEE(kset11.AppendTo(nil)[16:]))
	kset11.MessageSize = int32(len(kset11.AppendTo(nil)[12:]))

	kset12 := kmsg.MessageV1{
		Offset:    1,
		Magic:     1,
		Timestamp: 13,
		Key:       []byte("loooooong key 2"),
		Value:     []byte("loooooong value 2"),
	}
	kset12.CRC = int32(crc32.ChecksumIEEE(kset12.AppendTo(nil)[16:]))
	kset12.MessageSize = int32(len(kset12.AppendTo(nil)[12:]))

	var (
		kset0raw      = append(kset01.AppendTo(nil), kset02.AppendTo(nil)...) // for comparing & compressing
		kset1raw      = append(kset11.AppendTo(nil), kset12.AppendTo(nil)...) // for comparing & compressing
		compressor, _ = newCompressor(CompressionCodec{codec: 2})             // snappy
	)

	// golden v0, compressed
	kset0c := kmsg.MessageV0{
		Offset:     1,
		Attributes: 0x02,
	}
	kset0c.Value, _ = compressor.compress(sliceWriters.Get().(*sliceWriter), kset0raw, 1) // version 0, 1 use message set 0
	kset0c.CRC = int32(crc32.ChecksumIEEE(kset0c.AppendTo(nil)[16:]))
	kset0c.MessageSize = int32(len(kset0c.AppendTo(nil)[12:]))

	// golden v1 compressed
	kset1c := kmsg.MessageV1{
		Offset:     1,
		Magic:      1,
		Attributes: 0x02,
		Timestamp:  kset11.Timestamp,
	}
	kset1c.Value, _ = compressor.compress(sliceWriters.Get().(*sliceWriter), kset1raw, 2) // version 2 use message set 1
	kset1c.CRC = int32(crc32.ChecksumIEEE(kset1c.AppendTo(nil)[16:]))
	kset1c.MessageSize = int32(len(kset1c.AppendTo(nil)[12:]))

	// input
	ourBatch := seqRecBatch{
		recBatch: &recBatch{
			firstTimestamp: 12,
			records: []promisedNumberedRecord{
				{
					recordNumbers: recordNumbers{
						lengthField:    1,
						timestampDelta: 0,
					},
					promisedRec: promisedRec{
						Record: &Record{
							Key:   []byte("loooooong key 1"),
							Value: []byte("loooooong value 1"),
						},
					},
				},
				{
					recordNumbers: recordNumbers{
						lengthField:    3,
						timestampDelta: 1,
					},
					promisedRec: promisedRec{
						Record: &Record{
							Key:   []byte("loooooong key 2"),
							Value: []byte("loooooong value 2"),
						},
					},
				},
			},
		},
	}

	var (
		kset0rawc = kset0c.AppendTo(nil)
		kset1rawc = kset1c.AppendTo(nil)

		got0raw, _ = ourBatch.appendToAsMessageSet(nil, 1, nil)
		got1raw, _ = ourBatch.appendToAsMessageSet(nil, 2, nil)

		got0rawc, _ = ourBatch.appendToAsMessageSet(nil, 1, compressor)
		got1rawc, _ = ourBatch.appendToAsMessageSet(nil, 2, compressor)
	)

	for i, pair := range []struct {
		got []byte
		exp []byte
	}{
		{got0raw, kset0raw},
		{got1raw, kset1raw},
		{got0rawc, kset0rawc},
		{got1rawc, kset1rawc},
	} {
		gotFull := pair.got
		ourBatchSize := (&kbin.Reader{Src: gotFull}).Int32()
		got := gotFull[4:]
		if ourBatchSize != int32(len(got)) {
			t.Errorf("check %d: incorrect record prefixing written length %d != actual %d", i, ourBatchSize, len(got))
		}

		if !bytes.Equal(got, pair.exp) {
			t.Errorf("check %d: got != exp\n%v\n%v", i, got, pair.exp)
		}
	}

	// ***As a produce request***
	kmsgReq := kmsg.ProduceRequest{
		Version:       0,
		Acks:          -1,
		TimeoutMillis: 1000,
		Topics: []kmsg.ProduceRequestTopic{{
			Topic: "topic",
			Partitions: []kmsg.ProduceRequestTopicPartition{{
				Partition: 1,
				Records:   kset0rawc,
			}},
		}},
	}
	ourReq := produceRequest{
		version:    0,
		acks:       -1,
		timeout:    1000,
		compressor: compressor,
	}
	ourReq.batches.addSeqBatch("topic", 1, ourBatch)

	exp := kmsgReq.AppendTo(nil)
	got := ourReq.AppendTo(nil)

	if !bytes.Equal(got, exp) {
		t.Errorf("produce request: got != exp\n%v\n%v\n", got, exp)
	}
}

func BenchmarkAppendBatch(b *testing.B) {
	// ** ourReq and ourBatch copied from above, with longer values **
	txid := "tx"
	ourReq := produceRequest{
		version:       99,
		txnID:         &txid,
		acks:          -1,
		timeout:       1000,
		producerID:    12,
		producerEpoch: 11,
	}
	ourBatch := seqRecBatch{
		seq: 10,
		recBatch: &recBatch{
			firstTimestamp: 20,
			records: []promisedNumberedRecord{
				{
					recordNumbers: recordNumbers{
						lengthField:    1,
						timestampDelta: 2,
					},
					promisedRec: promisedRec{
						Record: &Record{
							Key:   []byte("key 1"),
							Value: bytes.Repeat([]byte("value 1"), 1000),
							Headers: []RecordHeader{
								{"header key 1", []byte("header value 1")},
								{"header key 2", []byte("header value 2")},
							},
						},
					},
				},
				{
					recordNumbers: recordNumbers{
						lengthField:    3,
						timestampDelta: 4,
					},
					promisedRec: promisedRec{
						Record: &Record{
							Key:   []byte("key 2"),
							Value: bytes.Repeat([]byte("value 2"), 1000),
						},
					},
				},
			},
		},
	}
	ourReq.batches.addSeqBatch("topic 1", 1, ourBatch)
	ourReq.batches.addSeqBatch("topic 1", 2, ourBatch)
	ourReq.batches.addSeqBatch("topic 1", 3, ourBatch)
	ourReq.batches.addSeqBatch("topic 1", 4, ourBatch)
	ourReq.batches.addSeqBatch("topic 2", 1, ourBatch)
	ourReq.batches.addSeqBatch("topic 2", 2, ourBatch)

	buf := make([]byte, 10<<10) // broker's reuse input buffers, so we do so here as well
	for _, pair := range []struct {
		name  string
		codec int8
	}{
		{"no compression", 0},
		{"gzip", 1},
		{"snappy", 2},
		{"lz4", 3},
		{"zstd", 4},
	} {
		b.Run(pair.name, func(b *testing.B) {
			compressor, _ := newCompressor(CompressionCodec{codec: pair.codec})
			ourReq.compressor = compressor
			for i := 0; i < b.N; i++ {
				buf = ourReq.AppendTo(buf[:0])
			}
			b.Log(len(buf))
		})
	}
}
