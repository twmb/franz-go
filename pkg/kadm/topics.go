package kadm

import (
	"context"
	"sort"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// ListTopics issues a metadata request and returns TopicDetails. Specific
// topics to describe can be passed as additional arguments. If no topics are
// specified, all topics are requested. Internal topics are not returned unless
// specifically requested. To see all topics including internal topics, use
// ListTopicsWithInternal.
//
// This returns an error if the request fails to be issued, or an *AuthError.
func (cl *Client) ListTopics(
	ctx context.Context,
	topics ...string,
) (TopicDetails, error) {
	t, err := cl.ListTopicsWithInternal(ctx, topics...)
	if err != nil {
		return nil, err
	}
	t.FilterInternal()
	return t, nil
}

// ListTopicsWithInternal is the same as ListTopics, but does not filter
// internal topics before returning.
func (cl *Client) ListTopicsWithInternal(
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

// CreateTopicRepsonses contains per-topic responses for created topics.
type CreateTopicResponses map[string]CreateTopicResponse

// Sorted returns all create topic responses sorted first by topic ID, then by
// topic name.
func (rs CreateTopicResponses) Sorted() []CreateTopicResponse {
	s := make([]CreateTopicResponse, 0, len(rs))
	for _, d := range rs {
		s = append(s, d)
	}
	sort.Slice(s, func(i, j int) bool {
		l, r := s[i], s[j]
		if l.ID.Less(r.ID) {
			return true
		}
		return l.Topic < r.Topic
	})
	return s
}

// On calls fn for the response topic if it exists, returning the response and
// the error returned from fn. If fn is nil, this simply returns the response.
//
// The fn is given a copy of the response. This function returns the copy as
// well; any modifications within fn are modifications on the returned copy.
//
// If the topic does not exist, this returns kerr.UnknownTopicOrPartition.
func (rs CreateTopicResponses) On(topic string, fn func(*CreateTopicResponse) error) (CreateTopicResponse, error) {
	if len(rs) > 0 {
		r, ok := rs[topic]
		if ok {
			if fn == nil {
				return r, nil
			}
			return r, fn(&r)
		}
	}
	return CreateTopicResponse{}, kerr.UnknownTopicOrPartition
}

// CreateTopics issues a create topics request with the given partitions,
// replication factor, and (optional) configs for every topic. Under the hood,
// this uses the default 15s request timeout and lets Kafka choose where to
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
) (CreateTopicResponses, error) {
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
) (CreateTopicResponses, error) {
	return cl.createTopics(ctx, true, partitions, replicationFactor, configs, topics)
}

func (cl *Client) createTopics(ctx context.Context, dry bool, p int32, rf int16, configs map[string]*string, topics []string) (CreateTopicResponses, error) {
	if len(topics) == 0 {
		return make(CreateTopicResponses), nil
	}

	req := kmsg.NewCreateTopicsRequest()
	req.TimeoutMillis = cl.timeoutMillis
	req.ValidateOnly = dry
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

	rs := make(CreateTopicResponses)
	for _, t := range resp.Topics {
		rs[t.Topic] = CreateTopicResponse{
			Topic: t.Topic,
			ID:    t.TopicID,
			Err:   kerr.ErrorForCode(t.ErrorCode),
		}
	}
	return rs, nil
}

// DeleteTopicResponse contains the response for an individual deleted topic.
type DeleteTopicResponse struct {
	Topic string  // Topic is the topic that was deleted, if not using topic IDs.
	ID    TopicID // ID is the topic ID for this topic, if talking to Kafka v2.8+ and using topic IDs.
	Err   error   // Err is any error preventing this topic from being deleted.
}

// DeleteTopicResponses contains per-topic responses for deleted topics.
type DeleteTopicResponses map[string]DeleteTopicResponse

// Sorted returns all delete topic responses sorted first by topic ID, then by
// topic name.
func (rs DeleteTopicResponses) Sorted() []DeleteTopicResponse {
	s := make([]DeleteTopicResponse, 0, len(rs))
	for _, d := range rs {
		s = append(s, d)
	}
	sort.Slice(s, func(i, j int) bool {
		l, r := s[i], s[j]
		if l.ID.Less(r.ID) {
			return true
		}
		return l.Topic < r.Topic
	})
	return s
}

// On calls fn for the response topic if it exists, returning the response and
// the error returned from fn. If fn is nil, this simply returns the response.
//
// The fn is given a copy of the response. This function returns the copy as
// well; any modifications within fn are modifications on the returned copy.
//
// If the topic does not exist, this returns kerr.UnknownTopicOrPartition.
func (rs DeleteTopicResponses) On(topic string, fn func(*DeleteTopicResponse) error) (DeleteTopicResponse, error) {
	if len(rs) > 0 {
		r, ok := rs[topic]
		if ok {
			if fn == nil {
				return r, nil
			}
			return r, fn(&r)
		}
	}
	return DeleteTopicResponse{}, kerr.UnknownTopicOrPartition
}

// DeleteTopics issues a delete topics request for the given topic names with a
// 15s timeout.
//
// This does not return an error on authorization failures, instead,
// authorization failures are included in the responses. This only returns an
// error if the request fails to be issued.
func (cl *Client) DeleteTopics(ctx context.Context, topics ...string) (DeleteTopicResponses, error) {
	if len(topics) == 0 {
		return make(DeleteTopicResponses), nil
	}

	req := kmsg.NewDeleteTopicsRequest()
	req.TimeoutMillis = cl.timeoutMillis
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

	rs := make(DeleteTopicResponses)
	for _, t := range resp.Topics {
		// A valid Kafka will return non-nil topics here, because we
		// are deleting by topic name, not ID. We still check to be
		// sure, but multiple invalid (nil) topics will collide.
		var topic string
		if t.Topic != nil {
			topic = *t.Topic
		}
		rs[topic] = DeleteTopicResponse{
			Topic: topic,
			ID:    t.TopicID,
			Err:   kerr.ErrorForCode(t.ErrorCode),
		}
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

// Lookup returns the response at t and p and whether it exists.
func (ds DeleteRecordsResponses) Lookup(t string, p int32) (DeleteRecordsResponse, bool) {
	if len(ds) == 0 {
		return DeleteRecordsResponse{}, false
	}
	ps := ds[t]
	if len(ps) == 0 {
		return DeleteRecordsResponse{}, false
	}
	r, exists := ps[p]
	return r, exists
}

// Each calls fn for every delete records response.
func (ds DeleteRecordsResponses) Each(fn func(DeleteRecordsResponse)) {
	for _, ps := range ds {
		for _, d := range ps {
			fn(d)
		}
	}
}

// Sorted returns all delete records responses sorted first by topic, then by
// partition.
func (rs DeleteRecordsResponses) Sorted() []DeleteRecordsResponse {
	var s []DeleteRecordsResponse
	for _, ps := range rs {
		for _, d := range ps {
			s = append(s, d)
		}
	}
	sort.Slice(s, func(i, j int) bool {
		l, r := s[i], s[j]
		if l.Topic < r.Topic {
			return true
		}
		if l.Topic > r.Topic {
			return false
		}
		return l.Partition < r.Partition
	})
	return s
}

// On calls fn for the response topic/partition if it exists, returning the
// response and the error returned from fn. If fn is nil, this simply returns
// the response.
//
// The fn is given a copy of the response. This function returns the copy as
// well; any modifications within fn are modifications on the returned copy.
//
// If the topic or partition does not exist, this returns
// kerr.UnknownTopicOrPartition.
func (rs DeleteRecordsResponses) On(topic string, partition int32, fn func(*DeleteRecordsResponse) error) (DeleteRecordsResponse, error) {
	if len(rs) > 0 {
		t, ok := rs[topic]
		if ok {
			p, ok := t[partition]
			if ok {
				if fn == nil {
					return p, nil
				}
				return p, fn(&p)
			}
		}
	}
	return DeleteRecordsResponse{}, kerr.UnknownTopicOrPartition
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
		return make(DeleteRecordsResponses), nil
	}

	req := kmsg.NewPtrDeleteRecordsRequest()
	req.TimeoutMillis = cl.timeoutMillis
	for t, ps := range os {
		rt := kmsg.NewDeleteRecordsRequestTopic()
		rt.Topic = t
		for p, o := range ps {
			rp := kmsg.NewDeleteRecordsRequestTopicPartition()
			rp.Partition = p
			rp.Offset = o.At
			rt.Partitions = append(rt.Partitions, rp)
		}
		req.Topics = append(req.Topics, rt)
	}

	shards := cl.cl.RequestSharded(ctx, req)
	rs := make(DeleteRecordsResponses)
	return rs, shardErrEach(req, shards, func(kr kmsg.Response) error {
		resp := kr.(*kmsg.DeleteRecordsResponse)
		for _, t := range resp.Topics {
			rt, exists := rs[t.Topic]
			if !exists { // topic could be spread around brokers, we need to check existence
				rt = make(map[int32]DeleteRecordsResponse)
				rs[t.Topic] = rt
			}
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

// CreatePartitionsResponses contains per-topic responses for a create
// partitions request.
type CreatePartitionsResponses map[string]CreatePartitionsResponse

// Sorted returns all create partitions responses sorted by topic.
func (rs CreatePartitionsResponses) Sorted() []CreatePartitionsResponse {
	var s []CreatePartitionsResponse
	for _, r := range rs {
		s = append(s, r)
	}
	sort.Slice(s, func(i, j int) bool { return s[i].Topic < s[j].Topic })
	return s
}

// On calls fn for the response topic if it exists, returning the response and
// the error returned from fn. If fn is nil, this simply returns the response.
//
// The fn is given a copy of the response. This function returns the copy as
// well; any modifications within fn are modifications on the returned copy.
//
// If the topic does not exist, this returns kerr.UnknownTopicOrPartition.
func (rs CreatePartitionsResponses) On(topic string, fn func(*CreatePartitionsResponse) error) (CreatePartitionsResponse, error) {
	if len(rs) > 0 {
		r, ok := rs[topic]
		if ok {
			if fn == nil {
				return r, nil
			}
			return r, fn(&r)
		}
	}
	return CreatePartitionsResponse{}, kerr.UnknownTopicOrPartition
}

// CreatePartitions issues a create partitions request for the given topics,
// adding "add" partitions to each topic. This request lets Kafka choose where
// the new partitions should be.
//
// This does not return an error on authorization failures for the create
// partitions request itself, instead, authorization failures are included in
// the responses. Before adding partitions, this request must issue a metadata
// request to learn the current count of partitions. If that fails, this
// returns the metadata request error. If you already know the final amount of
// partitions you want, you can use UpdatePartitions to set the count directly
// (rather than adding to the current count). You may consider checking
// ValidateCreatePartitions before using this method.
func (cl *Client) CreatePartitions(ctx context.Context, add int, topics ...string) (CreatePartitionsResponses, error) {
	return cl.createPartitions(ctx, false, add, -1, topics)
}

// UpdatePartitions issues a create partitions request for the given topics,
// setting the final partition count to "set" for each topic. This request lets
// Kafka choose where the new partitions should be.
//
// This does not return an error on authorization failures for the create
// partitions request itself, instead, authorization failures are included in
// the responses. Unlike CreatePartitions, this request uses your "set" value
// to set the new final count of partitions. "set" must be equal to or larger
// than the current count of partitions in the topic. All topics will have the
// same final count of partitions (unlike CreatePartitions, which allows you to
// add a specific count of partitions to topics that have a different amount of
// current partitions). You may consider checking ValidateUpdatePartitions
// before using this method.
func (cl *Client) UpdatePartitions(ctx context.Context, set int, topics ...string) (CreatePartitionsResponses, error) {
	return cl.createPartitions(ctx, false, -1, set, topics)
}

// ValidateCreatePartitions validates a create partitions request for adding
// "add" partitions to the given topics.
//
// This uses the same logic as CreatePartitions, but with the request's
// ValidateOnly field set to true. The response is the same response you would
// receive from CreatePartitions, but no partitions are actually added.
func (cl *Client) ValidateCreatePartitions(ctx context.Context, add int, topics ...string) (CreatePartitionsResponses, error) {
	return cl.createPartitions(ctx, true, add, -1, topics)
}

// ValidateUpdatePartitions validates a create partitions request for setting
// the partition count on the given topics to "set".
//
// This uses the same logic as UpdatePartitions, but with the request's
// ValidateOnly field set to true. The response is the same response you would
// receive from UpdatePartitions, but no partitions are actually added.
func (cl *Client) ValidateUpdatePartitions(ctx context.Context, set int, topics ...string) (CreatePartitionsResponses, error) {
	return cl.createPartitions(ctx, true, -1, set, topics)
}

func (cl *Client) createPartitions(ctx context.Context, dry bool, add, set int, topics []string) (CreatePartitionsResponses, error) {
	if len(topics) == 0 {
		return make(CreatePartitionsResponses), nil
	}

	var td TopicDetails
	var err error
	if add != -1 {
		td, err = cl.ListTopics(ctx, topics...)
		if err != nil {
			return nil, err
		}
	}

	req := kmsg.NewCreatePartitionsRequest()
	req.TimeoutMillis = cl.timeoutMillis
	req.ValidateOnly = dry
	for _, t := range topics {
		rt := kmsg.NewCreatePartitionsRequestTopic()
		rt.Topic = t
		if add == -1 {
			rt.Count = int32(set)
		} else {
			rt.Count = int32(len(td[t].Partitions) + add)
		}
		req.Topics = append(req.Topics, rt)
	}

	resp, err := req.RequestWith(ctx, cl.cl)
	if err != nil {
		return nil, err
	}

	rs := make(CreatePartitionsResponses)
	for _, t := range resp.Topics {
		rs[t.Topic] = CreatePartitionsResponse{
			Topic: t.Topic,
			Err:   kerr.ErrorForCode(t.ErrorCode),
		}
	}
	return rs, nil
}
