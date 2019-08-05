package kgo

import "github.com/twmb/kgo/kmsg"

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

type memberMetadata struct {
	id       string
	topics   []string
	userdata []byte
}

type simpleTopicPartitions struct {
	topic      string
	partitions []int32
}

// we always use "consumer"
type GroupBalancer interface {
	protocolName() string // "range"

	balance(memberData []memberMetadata, partitions []simpleTopicPartitions) *kmsg.GroupMemberAssignment
}

func balancerMetadata(topics []string, userdata []byte) []byte {
	return (&kmsg.GroupMemberMetadata{
		Version:  0,
		Topics:   topics,
		UserData: userdata,
	}).AppendTo(nil)
}

func RangeBalancer() GroupBalancer {
	return new(rangeBalancer)
}

type rangeBalancer struct{}

func (*rangeBalancer) protocolName() string { return "range" }
func (*rangeBalancer) balance([]memberMetadata, []simpleTopicPartitions) *kmsg.GroupMemberAssignment {
	return nil
}
