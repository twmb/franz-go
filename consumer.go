package kgo

import (
	"time"

	"github.com/twmb/kgo/kmsg"
)

func recordToRecord(
	topic string,
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
		Key:       record.Key,
		Value:     record.Value,
		Headers:   h,
		Topic:     topic,
		Partition: partition,
	}

	r.Timestamp = time.Unix(0, batch.FirstTimestamp+int64(record.TimestampDelta))

	if batch.Attributes&0x0008 != 0 {
		r.TimestampType = TimestampLogAppendTime()
	}

	r.Offset = batch.FirstOffset + int64(record.OffsetDelta)

	return r
}

func (c *Client) ConsumeGroup(
	group string,
	topics []string,
	balancer GroupBalancer,
	opts ...OptGroup,
) (*GroupConsumer, error) {
	gc := &GroupConsumer{
		groupID:  group,
		topics:   topics,
		balancer: balancer,
	}
	return gc, nil
}

type (
	OptGroup interface {
		Opt
		apply(*groupOpt)
	}

	groupOpt struct{ fn func(cfg *groupCfg) }

	groupCfg struct {
		userData []byte
		// TODO autocommit
		// OnAssign
		// OnRevoke
		// WithErrCh
		// SessionTimeout
		// RebalanceTimeout
		// MemberID

		// UserInfo?
	}

	GroupConsumer struct {
		groupID  string
		topics   []string
		balancer GroupBalancer

		memberID   string
		generation int32
	}

	partitionRecords struct {
		partition int32
		records   []*Record
	}

	TopicRecords struct {
		topic string
		parts []partitionRecords

		// high watermark
		// LSO
		// error code
	}
)

func (t *TopicRecords) Topic() string { return t.topic }
func (t *TopicRecords) Records() []*Record {
	sz := 0
	for _, part := range t.parts {
		sz += len(part.records)
	}
	all := make([]*Record, 0, sz)
	for _, part := range t.parts {
		all = append(all, part.records...)
	}
	return all
}

func (g *GroupConsumer) Poll() (*TopicRecords, error) {
	// can talk to multiple brokers behind the scenes
	//
	// for each broker,
	//   one in-flight (or buffered) fetch
	//   returned immediately from poll
	//
	// In go,
	// we want one goroutine per broker???
	// but partitions can move brokers
	// so effectively one broker can come and go
	return nil, nil
}

// TODO commit

func (g *GroupConsumer) LeaveGroup() {}

func (g *GroupConsumer) MemberID() string {
	return ""
}
