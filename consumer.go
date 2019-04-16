package kgo

import (
	"time"

	"github.com/twmb/kgo/kmsg"
)

func recordToRecord(
	partition int32,
	batch *kmsg.RecordBatch,
	record *kmsg.Record,
) *Record {
	h := make([]RecordHeader, 0, len(record.Headers))
	for _, kv := range record.Headers {
		h = append(h, RecordHeader{
			Key:   kv.Key,
			Value: kv.Value,
		})
	}
	r := &Record{
		Key:     record.Key,
		Value:   record.Value,
		Headers: h,
	}

	r.Timestamp = time.Unix(0, batch.FirstTimestamp+int64(record.TimestampDelta))

	if batch.Attributes&0x0008 == 1 {
		r.TimestampType = TimestampLogAppendTime()
	}

	r.Partition = partition
	r.Offset = batch.FirstOffset + int64(record.OffsetDelta)

	return r
}
