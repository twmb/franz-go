package kadm

import (
	"context"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ListTopics issues a metadata request and returns TopicDetails. Specific
// topics to describe can be passed as additional arguments. If no topics are
// specified, all topics are requested.
//
// This returns an error if the request fails to be issued, or an *AuthErr.
func (cl *Client) ListTopics(
	ctx context.Context,
	topics ...string,
) (TopicDetails, error) {
	m, err := cl.Metadata(ctx, topics...)
	if err != nil {
		return nil, err
	}
	return m.Topics, nil
}

// CreateTopicResponse contains the response for an individual created topic.
type CreateTopicResponse struct {
	Topic string  // Topic is the topic that was created.
	ID    TopicID // ID is the topic ID for this topic, if talking to Kafka v2.8+.
	Err   error   // Err is any error preventing this topic from being created.
}

// CreateTopics issues a create topics request with the given partitions,
// replication factor, and (optional) configs for every topic. Under the hood,
// this uses the default 60s request timeout and lets Kafka choose where to
// place partitions.
//
// This package includes a StringPtr function to aid in building config values.
//
// This does not return an error on authorization failures, instead,
// authorization failures are included in the responses. This only returns an
// error if the request fails to be issued. You may consider checking
// ValidateCreateTopics before using this method.
func (cl *Client) CreateTopics(
	ctx context.Context,
	partitions int32,
	replicationFactor int16,
	configs map[string]*string,
	topics ...string,
) ([]CreateTopicResponse, error) {
	return cl.createTopics(ctx, false, partitions, replicationFactor, configs, topics)
}

// ValidateCreateTopics validates a create topics request with the given
// partitions, replication factor, and (optional) configs for every topic.
//
// This package includes a StringPtr function to aid in building config values.
//
// This uses the same logic as CreateTopics, but with the request's
// ValidateOnly field set to true. The response is the same response you would
// receive from CreateTopics, but no topics are actually created.
func (cl *Client) ValidateCreateTopics(
	ctx context.Context,
	partitions int32,
	replicationFactor int16,
	configs map[string]*string,
	topics ...string,
) ([]CreateTopicResponse, error) {
	return cl.createTopics(ctx, true, partitions, replicationFactor, configs, topics)
}

func (cl *Client) createTopics(ctx context.Context, dry bool, p int32, rf int16, configs map[string]*string, topics []string) ([]CreateTopicResponse, error) {
	if len(topics) == 0 {
		return nil, nil
	}

	req := kmsg.NewCreateTopicsRequest()
	req.ValidateOnly = true
	for _, t := range topics {
		rt := kmsg.NewCreateTopicsRequestTopic()
		rt.Topic = t
		rt.NumPartitions = p
		rt.ReplicationFactor = rf
		for k, v := range configs {
			rc := kmsg.NewCreateTopicsRequestTopicConfig()
			rc.Name = k
			rc.Value = v
			rt.Configs = append(rt.Configs, rc)
		}
		req.Topics = append(req.Topics, rt)
	}

	resp, err := req.RequestWith(ctx, cl.cl)
	if err != nil {
		return nil, err
	}

	var rs []CreateTopicResponse
	for _, t := range resp.Topics {
		rs = append(rs, CreateTopicResponse{
			Topic: t.Topic,
			ID:    t.TopicID,
			Err:   kerr.ErrorForCode(t.ErrorCode),
		})
	}
	return rs, nil
}

// DeleteTopicResponse contains the response for an individual deleted topic.
type DeleteTopicResponse struct {
	Topic string  // Topic is the topic that was deleted, if not using topic IDs.
	ID    TopicID // ID is the topic ID for this topic, if talking to Kafka v2.8+ and using topic IDs.
	Err   error   // Err is any error preventing this topic from being deleted.
}

// DeleteTopics issues a delete topics request for the given topic names with a
// 60s timeout.
//
// This does not return an error on authorization failures, instead,
// authorization failures are included in the responses. This only returns an
// error if the request fails to be issued.
func (cl *Client) DeleteTopics(ctx context.Context, topics ...string) ([]DeleteTopicResponse, error) {
	if len(topics) == 0 {
		return nil, nil
	}

	req := kmsg.NewDeleteTopicsRequest()
	req.TopicNames = topics
	for _, t := range topics {
		rt := kmsg.NewDeleteTopicsRequestTopic()
		rt.Topic = kmsg.StringPtr(t)
		req.Topics = append(req.Topics, rt)
	}

	resp, err := req.RequestWith(ctx, cl.cl)
	if err != nil {
		return nil, err
	}

	var rs []DeleteTopicResponse
	for _, t := range resp.Topics {
		var topic string
		if t.Topic != nil {
			topic = *t.Topic
		}
		rs = append(rs, DeleteTopicResponse{
			Topic: topic,
			ID:    t.TopicID,
			Err:   kerr.ErrorForCode(t.ErrorCode),
		})
	}
	return rs, nil
}

// DeleteRecordsResponse contains the response for an individual partition from
// a delete records request.
type DeleteRecordsResponse struct {
	Topic        string // Topic is the topic this response is for.
	Partition    int32  // Partition is the partition this response is for.
	LowWatermark int64  // LowWatermark is the new earliest / start offset for this partition if the request was successful.
	Err          error  // Err is any error preventing the delete records request from being successful for this partition.
}

// DeleteRecordsResponses contains per-partition responses to a delete records request.
type DeleteRecordsResponses map[string]map[int32]DeleteRecordsResponse

// Each calls fn for every delete records response.
func (ds DeleteRecordsResponses) Each(fn func(DeleteRecordsResponse)) {
	for _, ps := range ds {
		for _, d := range ps {
			fn(d)
		}
	}
}

// DeleteRecords issues a delete records request for the given offsets. Per
// offset, only the Offset field needs to be set.
//
// To delete records, Kafka sets the LogStartOffset for partitions to the
// requested offset. All segments whose max partition is before the requested
// offset are deleted, and any records within the segment before the requested
// offset can no longer be read.
//
// This does not return an error on authorization failures, instead,
// authorization failures are included in the responses.
//
// This may return *ShardErrors.
func (cl *Client) DeleteRecords(ctx context.Context, os Offsets) (DeleteRecordsResponses, error) {
	if len(os) == 0 {
		return nil, nil
	}

	req := kmsg.NewPtrDeleteRecordsRequest()
	for t, ps := range os {
		rt := kmsg.NewDeleteRecordsRequestTopic()
		rt.Topic = t
		for p, o := range ps {
			rp := kmsg.NewDeleteRecordsRequestTopicPartition()
			rp.Partition = p
			rp.Offset = o.Offset
		}
	}

	shards := cl.cl.RequestSharded(ctx, req)
	rs := make(DeleteRecordsResponses)
	return rs, shardErrEach(req, shards, func(kr kmsg.Response) error {
		resp := kr.(*kmsg.DeleteRecordsResponse)
		for _, t := range resp.Topics {
			rt := make(map[int32]DeleteRecordsResponse)
			rs[t.Topic] = rt
			for _, p := range t.Partitions {
				rt[p.Partition] = DeleteRecordsResponse{
					Topic:        t.Topic,
					Partition:    p.Partition,
					LowWatermark: p.LowWatermark,
					Err:          kerr.ErrorForCode(p.ErrorCode),
				}
			}
		}
		return nil
	})
}

// CreatePartitionsResponse contains the response for an individual topic from
// a create partitions request.
type CreatePartitionsResponse struct {
	Topic string // Topic is the topic this response is for.
	Err   error  // Err is non-nil if partitions were unable to be added to this topic.
}

// CreatePartitions issues a create partitions request for the given topics,
// adding "add" partitions to each topic. This request lets Kafka choose where
// the new partitions should be.
//
// This does not return an error on authorization failures for the create
// partitions request itself, instead, authorization failures are included in
// the responses. Before adding partitions, this request must issue a metadata
// request to learn the current count of partitions. If that fails, this
// returns the metadata request error. You may consider checking
// ValidateCreatePartitions before using this method.
func (cl *Client) CreatePartitions(ctx context.Context, add int, topics ...string) ([]CreatePartitionsResponse, error) {
	return cl.createPartitions(ctx, false, add, topics)
}

// ValidateCreatePartitions validates a create partitions request for adding
// "add" partitions to the given topics.
//
// This uses the same logic as CreatePartitions, but with the request's
// ValidateOnly field set to true. The response is the same response you would
// receive from CreatePartitions, but no partitions are actually added.
func (cl *Client) ValidateCreatePartitions(ctx context.Context, add int, topics ...string) ([]CreatePartitionsResponse, error) {
	return cl.createPartitions(ctx, true, add, topics)
}

func (cl *Client) createPartitions(ctx context.Context, dry bool, add int, topics []string) ([]CreatePartitionsResponse, error) {
	if len(topics) == 0 {
		return nil, nil
	}

	td, err := cl.ListTopics(ctx, topics...)
	if err != nil {
		return nil, err
	}

	req := kmsg.NewCreatePartitionsRequest()
	req.ValidateOnly = dry
	for _, t := range topics {
		rt := kmsg.NewCreatePartitionsRequestTopic()
		rt.Topic = t
		rt.Count = int32(len(td[t].Partitions) + add)
	}

	resp, err := req.RequestWith(ctx, cl.cl)
	if err != nil {
		return nil, err
	}

	var rs []CreatePartitionsResponse
	for _, t := range resp.Topics {
		rs = append(rs, CreatePartitionsResponse{
			Topic: t.Topic,
			Err:   kerr.ErrorForCode(t.ErrorCode),
		})
	}
	return rs, nil
}
