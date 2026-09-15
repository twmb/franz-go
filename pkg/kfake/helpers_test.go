package kfake

import (
	"context"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"net"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

type testMemFS struct{ fs fs }

func newTestMemFS() *testMemFS { return &testMemFS{fs: newMemFS()} }
func (m *testMemFS) opt() Opt  { return withFS(m.fs) }

func newCluster(t *testing.T, opts ...Opt) *Cluster {
	t.Helper()
	opts = append([]Opt{BrokerConfigs(map[string]string{
		"group.consumer.heartbeat.interval.ms": "100",
	})}, opts...)
	c, err := NewCluster(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(c.Close)
	return c
}

func newPlainClient(t *testing.T, c *Cluster, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	opts = append([]kgo.Opt{kgo.SeedBrokers(c.ListenAddrs()...)}, opts...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func newShareConsumer(t *testing.T, c *Cluster, topic, group string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	opts = append([]kgo.Opt{
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics(topic),
		kgo.ShareGroup(group),
		kgo.FetchMaxWait(200 * time.Millisecond),
	}, opts...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

// verifyZeroRecords polls for the given duration and fails if any records
// are returned.
func verifyZeroRecords(t *testing.T, cl *kgo.Client, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var got int
	for ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		got += len(fetches.Records())
	}
	if got > 0 {
		t.Errorf("expected zero records, got %d", got)
	}
}

// produceN creates a plain client, produces n records with values "v0".."vN-1"
// to the given topic, then closes the client.
func produceN(t *testing.T, c *Cluster, topic string, n int) {
	t.Helper()
	cl := newPlainClient(t, c)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := range n {
		r := &kgo.Record{Topic: topic, Value: fmt.Appendf(nil, "v%d", i)}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatal(err)
		}
	}
}

func stringp(s string) *string { return &s }

// timestampBatch builds the wire bytes of one v2 record batch whose records
// carry the given millisecond timestamps, patching Length and a valid
// Castagnoli CRC.
func timestampBatch(tss []int64) []byte {
	first := tss[0]
	var recs []byte
	for i, ts := range tss {
		r := kmsg.Record{
			OffsetDelta:      int32(i),
			TimestampDelta64: ts - first,
			Value:            []byte("v"),
		}
		// Length counts the bytes after the length varint itself, so we
		// serialize once with Length 0 to measure and once with it set.
		// Both are a one byte varint for records this small.
		r.Length = int32(len(r.AppendTo(nil)) - 1)
		recs = append(recs, r.AppendTo(nil)...)
	}
	rb := kmsg.RecordBatch{
		PartitionLeaderEpoch: -1,
		Magic:                2,
		LastOffsetDelta:      int32(len(tss)) - 1,
		FirstTimestamp:       first,
		MaxTimestamp:         slices.Max(tss),
		ProducerID:           -1,
		ProducerEpoch:        -1,
		FirstSequence:        -1,
		NumRecords:           int32(len(tss)),
		Records:              recs,
	}
	raw := rb.AppendTo(nil)
	// Length covers every byte after FirstOffset(8)+Length(4), and the
	// Castagnoli CRC covers from Attributes (byte 21) onward.
	binary.BigEndian.PutUint32(raw[8:12], uint32(len(raw)-12))
	binary.BigEndian.PutUint32(raw[17:21], crc32.Checksum(raw[21:], crc32.MakeTable(crc32.Castagnoli)))
	return raw
}

// produceBatches sends one Produce request per inner slice, each carrying a
// single record batch whose records have the given millisecond timestamps.
// A test that needs exact batch boundaries has to put them on the wire
// itself. A long linger with a Flush per batch does not give you one batch
// per flush: a drain loop can still be running from the previous flush, and
// it sends whatever is buffered the moment its in flight slot frees, which
// splits the batch you are building.
func produceBatches(t *testing.T, c *Cluster, topic string, batches [][]int64) {
	t.Helper()
	v := kversion.Stable()
	v.SetMaxKeyVersion(0, 12) // Produce v13 identifies topics by ID; v12 takes the name
	cl := newPlainClient(t, c, kgo.MaxVersions(v))
	defer cl.Close()

	var offset int64
	for _, tss := range batches {
		req := kmsg.NewPtrProduceRequest()
		req.Acks = -1
		req.TimeoutMillis = 5000
		rt := kmsg.NewProduceRequestTopic()
		rt.Topic = topic
		rp := kmsg.NewProduceRequestTopicPartition()
		rp.Records = timestampBatch(tss)
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)
		resp, err := req.RequestWith(context.Background(), cl)
		if err != nil {
			t.Fatalf("producing %v: %v", tss, err)
		}
		p := resp.Topics[0].Partitions[0]
		if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
			t.Fatalf("producing %v: %v", tss, err)
		}
		if p.BaseOffset != offset {
			t.Fatalf("batch %v landed at offset %d, want %d", tss, p.BaseOffset, offset)
		}
		offset += int64(len(tss))
	}
}

// groupCommits returns the group's committed offsets, or nil if the group
// does not exist.
func groupCommits(c *Cluster, group string) map[string]map[int32]GroupCommit {
	g := c.GroupInfo(group)
	if g == nil {
		return nil
	}
	return g.Commits
}

// produceShareN creates a plain client, sets share.auto.offset.reset=earliest
// for the given group, produces n string records to the topic, and flushes.
func produceShareN(t *testing.T, c *Cluster, topic, group string, n int) {
	t.Helper()
	cl := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
	c.SetGroupConfigs(group, map[string]string{"share.auto.offset.reset": "earliest"})
	for i := range n {
		cl.Produce(context.Background(), kgo.StringRecord(strconv.Itoa(i)), func(_ *kgo.Record, err error) {
			if err != nil {
				t.Errorf("produce %d: %v", i, err)
			}
		})
	}
	if err := cl.Flush(context.Background()); err != nil {
		t.Fatalf("flush: %v", err)
	}
}

// joinShareGroupRaw joins a share group via raw heartbeat+metadata and returns
// the member ID and topic UUID.
func joinShareGroupRaw(t *testing.T, cl *kgo.Client, group, topic string) (memberID string, topicID [16]byte) {
	t.Helper()
	hbReq := kmsg.NewPtrShareGroupHeartbeatRequest()
	hbReq.GroupID = group
	hbReq.MemberID = "test-member-1"
	hbReq.MemberEpoch = 0
	hbReq.SubscribedTopicNames = []string{topic}
	hbResp, err := hbReq.RequestWith(context.Background(), cl)
	if err != nil {
		t.Fatalf("heartbeat: %v", err)
	}
	if hbResp.ErrorCode != 0 {
		t.Fatalf("heartbeat error: %v", kerr.ErrorForCode(hbResp.ErrorCode))
	}
	memberID = *hbResp.MemberID

	metaReq := kmsg.NewPtrMetadataRequest()
	metaReqTopic := kmsg.NewMetadataRequestTopic()
	metaReqTopic.Topic = kmsg.StringPtr(topic)
	metaReq.Topics = append(metaReq.Topics, metaReqTopic)
	metaResp, err := metaReq.RequestWith(context.Background(), cl)
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	topicID = metaResp.Topics[0].TopicID
	return memberID, topicID
}

// rawShareFetch sends a ShareFetch request and returns the response and count
// of acquired records.
func rawShareFetch(t *testing.T, cl *kgo.Client, group, memberID string, topicID [16]byte, epoch int32) (*kmsg.ShareFetchResponse, int) {
	t.Helper()
	sfReq := kmsg.NewPtrShareFetchRequest()
	sfReq.GroupID = &group
	sfReq.MemberID = &memberID
	sfReq.ShareSessionEpoch = epoch
	sfReq.MaxRecords = 500
	sfTopic := kmsg.NewShareFetchRequestTopic()
	sfTopic.TopicID = topicID
	sfPart := kmsg.NewShareFetchRequestTopicPartition()
	sfPart.Partition = 0
	sfPart.PartitionMaxBytes = 1 << 20
	sfTopic.Partitions = append(sfTopic.Partitions, sfPart)
	sfReq.Topics = append(sfReq.Topics, sfTopic)
	sfResp, err := sfReq.RequestWith(context.Background(), cl)
	if err != nil {
		t.Fatalf("ShareFetch: %v", err)
	}
	var acquired int
	for _, rt := range sfResp.Topics {
		for _, rp := range rt.Partitions {
			for _, ab := range rp.AcquiredRecords {
				acquired += int(ab.LastOffset-ab.FirstOffset) + 1
			}
		}
	}
	return sfResp, acquired
}

// newClient848 creates a kgo client with the KIP-848 context opt-in enabled.
func newClient848(t *testing.T, c *Cluster, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	ctx := context.WithValue(context.Background(), "opt_in_kafka_next_gen_balancer_beta", true)
	opts = append([]kgo.Opt{kgo.SeedBrokers(c.ListenAddrs()...), kgo.WithContext(ctx)}, opts...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(cl.Close)
	return cl
}

func produceSync(t *testing.T, cl *kgo.Client, records ...*kgo.Record) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := cl.ProduceSync(ctx, records...).FirstErr(); err != nil {
		t.Fatalf("produce failed: %v", err)
	}
}

func produceNStrings(t *testing.T, cl *kgo.Client, topic string, n int) {
	t.Helper()
	var records []*kgo.Record
	for i := range n {
		r := kgo.StringRecord("value-" + strconv.Itoa(i))
		r.Topic = topic
		r.Key = []byte("key-" + strconv.Itoa(i))
		records = append(records, r)
	}
	produceSync(t, cl, records...)
}

// consumeN polls until n records arrive, fataling on a fetch error or on the
// timeout.
func consumeN(t *testing.T, cl *kgo.Client, n int, timeout time.Duration) []*kgo.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var records []*kgo.Record
	for len(records) < n {
		fs := cl.PollFetches(ctx)
		if errs := fs.Errors(); len(errs) > 0 {
			for _, e := range errs {
				if e.Err == context.DeadlineExceeded || e.Err == context.Canceled {
					t.Fatalf("timeout consuming records: got %d/%d", len(records), n)
				}
			}
			t.Fatalf("consume errors: %v", errs)
		}
		fs.EachRecord(func(r *kgo.Record) {
			records = append(records, r)
		})
	}
	return records
}

// isInitialJoin reports whether a heartbeat is a member's first: KIP-848
// joins at epoch 0.
func isInitialJoin(kreq kmsg.Request) bool {
	return kreq.(*kmsg.ConsumerGroupHeartbeatRequest).MemberEpoch == 0
}

// waitStable waits for the group to be Stable with nMembers members. This
// covers classic and 848 groups alike.
func waitStable(t *testing.T, c *Cluster, group string, nMembers int) *GroupInfo {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	g, err := c.WaitGroupStable(ctx, group, nMembers)
	if err != nil {
		t.Fatalf("group %s not stable with %d members: %v", group, nMembers, err)
	}
	return g
}

// newGroupConsumer creates a kgo client configured for group consuming with
// sensible test defaults: ConsumeTopics, ConsumerGroup, AtStart reset,
// and 250ms FetchMaxWait. Additional opts are appended after the defaults.
func newGroupConsumer(t *testing.T, c *Cluster, topic, group string, opts ...kgo.Opt) *kgo.Client {
	t.Helper()
	base := []kgo.Opt{
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250 * time.Millisecond),
	}
	return newClient848(t, c, append(base, opts...)...)
}

// poll1FromEachClient polls each client until every one has received at least
// one record, or the timeout expires.
func poll1FromEachClient(t *testing.T, timeout time.Duration, clients ...*kgo.Client) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	remaining := make(map[int]*kgo.Client, len(clients))
	for i, cl := range clients {
		remaining[i] = cl
	}
	for len(remaining) > 0 {
		for i, cl := range remaining {
			fs := cl.PollRecords(ctx, 10)
			if fs.NumRecords() > 0 {
				delete(remaining, i)
			}
		}
		if ctx.Err() != nil {
			t.Fatalf("timeout waiting for all clients to get records: %d/%d remaining", len(remaining), len(clients))
		}
	}
}

// shareAdminTopic seeds a one partition topic, points the group at the start
// of it, produces n records, and returns the cluster and the client that
// produced them.
func shareAdminTopic(t *testing.T, topic, group string, n int) (*Cluster, *kgo.Client) {
	t.Helper()
	c := newCluster(t, SeedTopics(1, topic))
	admin := newPlainClient(t, c, kgo.DefaultProduceTopic(topic))
	c.SetGroupConfigs(group, map[string]string{"share.auto.offset.reset": "earliest"})
	produceNStrings(t, admin, topic, n)
	return c, admin
}

// pollShareOnce polls until one record arrives, which is how you know the
// member joined and holds an assignment.
func pollShareOnce(t *testing.T, cl *kgo.Client, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	for {
		if len(cl.PollFetches(ctx).Records()) > 0 {
			return
		}
		if ctx.Err() != nil {
			t.Fatal("timeout waiting for records")
		}
	}
}

// drainShareAccept polls n records, accepts each, and flushes after every
// poll so the SPSO advances on the broker.
func drainShareAccept(t *testing.T, cl *kgo.Client, n int, timeout time.Duration) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var got int
	for got < n {
		fs := cl.PollFetches(ctx)
		for _, r := range fs.Records() {
			r.Ack(kgo.AckAccept)
			got++
		}
		flushCtx, flushCancel := context.WithTimeout(context.Background(), 5*time.Second)
		cl.FlushAcks(flushCtx)
		flushCancel()
		if ctx.Err() != nil {
			break
		}
	}
	if got < n {
		t.Fatalf("got %d/%d records", got, n)
	}
}

// describeShareOffsets returns the group's DescribeShareGroupOffsets response
// for partition 0 of the topic.
func describeShareOffsets(t *testing.T, cl *kgo.Client, group, topic string) *kmsg.DescribeShareGroupOffsetsResponse {
	t.Helper()
	req := kmsg.NewPtrDescribeShareGroupOffsetsRequest()
	rg := kmsg.NewDescribeShareGroupOffsetsRequestGroup()
	rg.GroupID = group
	rt := kmsg.NewDescribeShareGroupOffsetsRequestGroupTopic()
	rt.Topic = topic
	rt.Partitions = []int32{0}
	rg.Topics = append(rg.Topics, rt)
	req.Groups = append(req.Groups, rg)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("describe offsets: %v", err)
	}
	return resp
}

// rawBatch builds the wire bytes of one v2 record batch carrying a single
// record, patching Length and a valid Castagnoli CRC.
func rawBatch(attrs int16, pid int64, epoch int16, firstSeq int32, rec kmsg.Record) []byte {
	rec.Length = int32(len(rec.AppendTo(nil)) - 1)
	now := time.Now().UnixMilli()
	batch := kmsg.RecordBatch{
		PartitionLeaderEpoch: -1,
		Magic:                2,
		Attributes:           attrs,
		LastOffsetDelta:      0,
		FirstTimestamp:       now,
		MaxTimestamp:         now,
		ProducerID:           pid,
		ProducerEpoch:        epoch,
		FirstSequence:        firstSeq,
		NumRecords:           1,
		Records:              rec.AppendTo(nil),
	}
	raw := batch.AppendTo(nil)
	batch.Length = int32(len(raw) - 12)
	raw = batch.AppendTo(nil)
	batch.CRC = int32(crc32.Checksum(raw[21:], crc32.MakeTable(crc32.Castagnoli)))
	return batch.AppendTo(nil)
}

// kvRecord is the one record rawBatch callers send when the bytes do not
// matter.
func kvRecord() kmsg.Record {
	return kmsg.Record{Key: []byte("k"), Value: []byte("v")}
}

// produceRawV11 sends one batch as a Produce v11, which names the topic
// rather than identifying it by ID, and returns the partition response.
func produceRawV11(t *testing.T, cl *kgo.Client, topic string, batch []byte) kmsg.ProduceResponseTopicPartition {
	t.Helper()
	req := kmsg.NewProduceRequest()
	req.Version = 11
	req.Acks = -1
	req.TimeoutMillis = 5000
	rt := kmsg.NewProduceRequestTopic()
	rt.Topic = topic
	rp := kmsg.NewProduceRequestTopicPartition()
	rp.Partition = 0
	rp.Records = batch
	rt.Partitions = append(rt.Partitions, rp)
	req.Topics = append(req.Topics, rt)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
	return resp.Topics[0].Partitions[0]
}

// shareAckCollector gathers every ShareAckResult the client reports, for a
// test to sift through once the run is over.
type shareAckCollector struct {
	mu      sync.Mutex
	results []kgo.ShareAckResult
}

// opt installs the collector on a client.
func (s *shareAckCollector) opt() kgo.Opt {
	return kgo.ShareAckCallback(func(_ *kgo.Client, res kgo.ShareAckResults) {
		s.mu.Lock()
		s.results = append(s.results, res...)
		s.mu.Unlock()
	})
}

// snapshot copies what has been collected so far.
func (s *shareAckCollector) snapshot() []kgo.ShareAckResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.results)
}

// leaveShareGroupRaw sends the epoch -1 heartbeat that takes a member out
// of a share group.
func leaveShareGroupRaw(t *testing.T, cl *kgo.Client, group, memberID string) {
	t.Helper()
	req := kmsg.NewPtrShareGroupHeartbeatRequest()
	req.GroupID = group
	req.MemberID = memberID
	req.MemberEpoch = -1
	if _, err := req.RequestWith(context.Background(), cl); err != nil {
		t.Fatalf("leave heartbeat: %v", err)
	}
}

// soleBroker adds the cluster's first listener to a crafted metadata
// response as the given node, and makes it the controller.
func soleBroker(c *Cluster, resp *kmsg.MetadataResponse, id int32) {
	host, portStr, _ := net.SplitHostPort(c.ListenAddrs()[0])
	port, _ := strconv.Atoi(portStr)
	b := kmsg.NewMetadataResponseBroker()
	b.NodeID = id
	b.Host = host
	b.Port = int32(port)
	resp.Brokers = append(resp.Brokers, b)
	resp.ControllerID = id
}
