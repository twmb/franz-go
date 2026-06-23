package kfake

// Regression tests for partition-count changes on a live topic (no
// recreation): legal grows via CreatePartitions, and the transiently
// inconsistent metadata views that accompany them (brokers serve metadata
// from their own caches, which can lag the controller and each other).

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

// captureMetadata issues a raw Metadata request for the topic and returns the
// response, for replaying (stale) views via ControlKey later.
func captureMetadata(t *testing.T, cl *kgo.Client, topic string) *kmsg.MetadataResponse {
	t.Helper()
	req := kmsg.NewPtrMetadataRequest()
	mt := kmsg.NewMetadataRequestTopic()
	mt.Topic = kmsg.StringPtr(topic)
	req.Topics = append(req.Topics, mt)
	resp, err := req.RequestWith(context.Background(), cl)
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	return resp
}

// serveStaleMetadata answers all Metadata requests with the canned response
// until the returned stop function is called.
func serveStaleMetadata(c *Cluster, canned *kmsg.MetadataResponse) (stop func()) {
	var stopped atomic.Bool
	c.ControlKey(int16(kmsg.Metadata), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		if stopped.Load() {
			c.DropControl()
			return nil, nil, false
		}
		c.KeepControl()
		dup := *canned
		dup.Version = kreq.GetVersion()
		return &dup, nil, true
	})
	return func() { stopped.Store(true) }
}

// truncateMetadataPartitions returns a copy of the response with the topic's
// partition list filtered to partitions < keep, mimicking the view of a
// broker that has not yet seen a partition-count increase.
func truncateMetadataPartitions(resp *kmsg.MetadataResponse, topic string, keep int32) *kmsg.MetadataResponse {
	dup := *resp
	dup.Topics = slices.Clone(resp.Topics)
	for i := range dup.Topics {
		rt := &dup.Topics[i]
		if rt.Topic == nil || *rt.Topic != topic {
			continue
		}
		var ps []kmsg.MetadataResponseTopicPartition
		for _, p := range rt.Partitions {
			if p.Partition < keep {
				ps = append(ps, p)
			}
		}
		rt.Partitions = ps
	}
	return &dup
}

func growPartitions(t *testing.T, cl *kgo.Client, topic string, total int32) {
	t.Helper()
	req := kmsg.NewPtrCreatePartitionsRequest()
	rt := kmsg.NewCreatePartitionsRequestTopic()
	rt.Topic = topic
	rt.Count = total
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(context.Background(), cl)
	if err != nil {
		t.Fatalf("create partitions: %v", err)
	}
	if ec := resp.Topics[0].ErrorCode; ec != 0 {
		t.Fatalf("create partitions: %v", kerr.ErrorForCode(ec))
	}
}

// observeHeartbeatEpochs watches requests of the given heartbeat key and
// tracks the highest member epoch seen in any request.
func observeHeartbeatEpochs(c *Cluster, key int16) *atomic.Int32 {
	var hi atomic.Int32
	hi.Store(-1 << 30)
	c.ControlKey(key, func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		var epoch int32
		switch req := kreq.(type) {
		case *kmsg.ShareGroupHeartbeatRequest:
			epoch = req.MemberEpoch
		case *kmsg.ConsumerGroupHeartbeatRequest:
			epoch = req.MemberEpoch
		default:
			return nil, nil, false
		}
		for {
			cur := hi.Load()
			if epoch <= cur || hi.CompareAndSwap(cur, epoch) {
				return nil, nil, false
			}
		}
	})
	return &hi
}

func waitEpochAbove(t *testing.T, hi *atomic.Int32, above int32, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if hi.Load() > above {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("heartbeat member epoch never exceeded %d (last %d)", above, hi.Load())
}

// A metadata response from an out of date broker can omit a partition we
// know about (e.g. the broker has not yet seen a CreatePartitions). Buffered
// records for that partition must survive the transient omission and deliver
// once metadata recovers. Previously the first such refresh failed buffered
// (never-sent) records with errMissingMetadataPartition.
func TestProduceTransientMissingPartitionKeepsRecords(t *testing.T) {
	t.Parallel()
	c := newCluster(t, SeedTopics(2, "t"))
	cl := newPlainClient(t, c,
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ProducerLinger(time.Second),
		kgo.UnknownTopicRetries(50), // we are exercising transience, not the fail bound
	)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// Prove the produce path works and load client metadata (2 partitions).
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "t", Partition: 1, Value: []byte("canary")}).FirstErr(); err != nil {
		t.Fatalf("canary produce: %v", err)
	}

	stale := truncateMetadataPartitions(captureMetadata(t, cl, "t"), "t", 1)
	stop := serveStaleMetadata(c, stale)

	done := make(chan error, 1)
	cl.Produce(ctx, &kgo.Record{Topic: "t", Partition: 1, Value: []byte("survivor")}, func(_ *kgo.Record, err error) {
		done <- err
	})
	cl.ForceMetadataRefresh()

	// The stale refresh (and the internal ~250ms retry refreshes it
	// schedules) must not fail the buffered record.
	select {
	case err := <-done:
		t.Fatalf("record finished during the stale-metadata window: %v", err)
	case <-time.After(500 * time.Millisecond):
	}

	stop()
	cl.ForceMetadataRefresh()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("record failed after metadata recovered: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("record was not delivered after metadata recovered")
	}
}

// The transient tolerance must stay bounded: when metadata persistently
// omits a partition (e.g. the topic was recreated with fewer partitions),
// buffered records still fail once the unknown fail limit trips.
func TestProducePersistentMissingPartitionStillFails(t *testing.T) {
	t.Parallel()
	c := newCluster(t, SeedTopics(2, "t"))
	cl := newPlainClient(t, c,
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
		kgo.ProducerLinger(10*time.Second), // keep the record buffered while strikes accumulate
	)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Load client metadata with the true two-partition view BEFORE
	// producing the doomed record, matching the sibling transient test's
	// canary. Without it the test is racy: bufferedRecords is incremented
	// at produce admission (producer.go, before loadPartsAndPartition), so
	// BufferedProduceRecords()!=0 only proves the record was admitted, not
	// that it was partitioned against the two-partition view. Under load
	// the record can still sit in the unknown-topics wait list when the
	// stale one-partition view below lands, and its FIRST partitioning then
	// runs against that view - failing via doPartition's out-of-range
	// "invalid partitioning choice" (NOT_BUGS #10) instead of the
	// errMissingMetadataPartition path under test. The canary makes the
	// topic known with two partitions, so the doomed record partitions
	// synchronously onto partition 1's recBuf, which the stale view then
	// makes vanish.
	if err := cl.ProduceSync(ctx, &kgo.Record{Topic: "t", Partition: 1, Value: []byte("canary")}).FirstErr(); err != nil {
		t.Fatalf("canary produce: %v", err)
	}

	done := make(chan error, 1)
	cl.Produce(ctx, &kgo.Record{Topic: "t", Partition: 1, Value: []byte("doomed")}, func(_ *kgo.Record, err error) {
		done <- err
	})

	stale := truncateMetadataPartitions(captureMetadata(t, cl, "t"), "t", 1)
	stop := serveStaleMetadata(c, stale)
	defer stop()

	cl.ForceMetadataRefresh()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected the record to fail, got successful delivery")
		}
		if !strings.Contains(err.Error(), "missing a partition") {
			t.Fatalf("expected a missing-partition failure, got: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("record neither failed nor delivered under a persistently missing partition")
	}
}

// A share-group coordinator can assign newly added partitions while the
// client's metadata still has the old partition count. The member acks the
// new epoch regardless, so the broker never re-sends the assignment; the
// client must retry activating the skipped partition itself once metadata
// catches up. Previously the partition was silently never consumed.
func TestShareAssignedNewPartitionStaleMetadata(t *testing.T) {
	t.Parallel()
	c := newCluster(t, SeedTopics(1, "t"))
	const group = "g-share-new-partition"

	produceShareN(t, c, "t", group, 3)

	hbEpochs := observeHeartbeatEpochs(c, int16(kmsg.ShareGroupHeartbeat))

	sc := newShareConsumer(t, c, "t", group)
	collectRecords(t, sc, 3, 15*time.Second)
	ackedEpoch := hbEpochs.Load()

	// From here, all Metadata responses replay the pre-grow single
	// partition view: the coordinator is "ahead" of every broker's
	// metadata as far as this client can tell.
	stop := serveStaleMetadata(c, captureMetadata(t, sc, "t"))

	admin := newPlainClient(t, c)
	growPartitions(t, admin, "t", 2)

	// Wait until the member acks the post-grow epoch: the assignment
	// containing partition 1 was delivered and processed against stale
	// metadata, and the broker now considers it delivered.
	waitEpochAbove(t, hbEpochs, ackedEpoch, 15*time.Second)

	stop()

	// Produce to the new partition with a fresh client (fresh metadata).
	p1cl := newPlainClient(t, c, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := range 3 {
		r := &kgo.Record{Topic: "t", Partition: 1, Value: fmt.Appendf(nil, "p1-%d", i)}
		if err := p1cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatalf("produce to new partition: %v", err)
		}
	}

	recs := collectRecords(t, sc, 3, 20*time.Second)
	for _, r := range recs {
		if !strings.HasPrefix(string(r.Value), "p1-") {
			t.Fatalf("unexpected record %q; want only new-partition records", r.Value)
		}
	}
}

// The regular 848 consumer in the same situation self-heals: an assigned
// partition unknown to local metadata is queued as an offset load, the load
// is retried (UnknownTopicOrPartition) gated on metadata updates, and the
// partition is consumed once metadata catches up. Guard that chain.
func Test848AssignedNewPartitionStaleMetadata(t *testing.T) {
	t.Parallel()
	c := newCluster(t,
		SeedTopics(1, "t"),
		BrokerConfigs(map[string]string{"group.consumer.heartbeat.interval.ms": "100"}),
	)
	const group = "g-848-new-partition"

	produceN(t, c, "t", 3)

	hbEpochs := observeHeartbeatEpochs(c, int16(kmsg.ConsumerGroupHeartbeat))

	ctx848 := context.WithValue(context.Background(), "opt_in_kafka_next_gen_balancer_beta", true)
	cl := newPlainClient(t, c,
		kgo.WithContext(ctx848),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics("t"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	collectRecords(t, cl, 3, 15*time.Second)
	ackedEpoch := hbEpochs.Load()

	stop := serveStaleMetadata(c, captureMetadata(t, cl, "t"))

	admin := newPlainClient(t, c)
	growPartitions(t, admin, "t", 2)

	waitEpochAbove(t, hbEpochs, ackedEpoch, 15*time.Second)
	// Give the client a moment to run its assignment and offset loads
	// against the stale view, so the retry path is genuinely exercised.
	time.Sleep(500 * time.Millisecond)
	stop()

	p1cl := newPlainClient(t, c, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for i := range 3 {
		r := &kgo.Record{Topic: "t", Partition: 1, Value: fmt.Appendf(nil, "p1-%d", i)}
		if err := p1cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatalf("produce to new partition: %v", err)
		}
	}
	recs := collectRecords(t, cl, 3, 20*time.Second)
	for _, r := range recs {
		if !strings.HasPrefix(string(r.Value), "p1-") {
			t.Fatalf("unexpected record %q; want only new-partition records", r.Value)
		}
	}
}

// Real brokers validate that committed partitions exist (API layer,
// UNKNOWN_TOPIC_OR_PARTITION) before the coordinator stores offsets; kfake
// previously accepted any partition blindly. Pinned to OffsetCommit v9: v10
// addresses topics by ID and unknown IDs take the UNKNOWN_TOPIC_ID path.
func TestOffsetCommitUnknownPartitionRejected(t *testing.T) {
	t.Parallel()
	c := newCluster(t, SeedTopics(1, "t"))
	v := kversion.Stable()
	v.SetMaxKeyVersion(int16(kmsg.OffsetCommit), 9)
	cl := newPlainClient(t, c, kgo.MaxVersions(v))
	const group = "g-commit-validation"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req := kmsg.NewPtrOffsetCommitRequest()
	req.Group = group
	req.Generation = -1
	rt := kmsg.NewOffsetCommitRequestTopic()
	rt.Topic = "t"
	for _, p := range []int32{0, 5} {
		rp := kmsg.NewOffsetCommitRequestTopicPartition()
		rp.Partition = p
		rp.Offset = 1
		rt.Partitions = append(rt.Partitions, rp)
	}
	rmissing := kmsg.NewOffsetCommitRequestTopic()
	rmissing.Topic = "does-not-exist"
	rp := kmsg.NewOffsetCommitRequestTopicPartition()
	rp.Partition = 0
	rp.Offset = 1
	rmissing.Partitions = append(rmissing.Partitions, rp)
	req.Topics = append(req.Topics, rt, rmissing)

	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("offset commit: %v", err)
	}
	got := make(map[string]map[int32]int16)
	for _, rt := range resp.Topics {
		ps := make(map[int32]int16)
		for _, rp := range rt.Partitions {
			ps[rp.Partition] = rp.ErrorCode
		}
		got[rt.Topic] = ps
	}
	if ec := got["t"][0]; ec != 0 {
		t.Errorf("t/0: got error %v, want none", kerr.ErrorForCode(ec))
	}
	if ec := got["t"][5]; ec != kerr.UnknownTopicOrPartition.Code {
		t.Errorf("t/5: got error code %d, want UNKNOWN_TOPIC_OR_PARTITION", ec)
	}
	if ec := got["does-not-exist"][0]; ec != kerr.UnknownTopicOrPartition.Code {
		t.Errorf("does-not-exist/0: got error code %d, want UNKNOWN_TOPIC_OR_PARTITION", ec)
	}

	// Only the valid partition's commit may be stored.
	freq := kmsg.NewPtrOffsetFetchRequest()
	freq.Group = group
	ft := kmsg.NewOffsetFetchRequestTopic()
	ft.Topic = "t"
	ft.Partitions = []int32{0, 5}
	freq.Topics = append(freq.Topics, ft)
	fg := kmsg.NewOffsetFetchRequestGroup()
	fg.Group = group
	fgt := kmsg.NewOffsetFetchRequestGroupTopic()
	fgt.Topic = "t"
	fgt.Partitions = []int32{0, 5}
	fg.Topics = append(fg.Topics, fgt)
	freq.Groups = append(freq.Groups, fg)

	fresp, err := freq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatalf("offset fetch: %v", err)
	}
	offsets := make(map[int32]int64)
	if len(fresp.Groups) > 0 {
		for _, gt := range fresp.Groups[0].Topics {
			for _, gp := range gt.Partitions {
				offsets[gp.Partition] = gp.Offset
			}
		}
	} else {
		for _, ftr := range fresp.Topics {
			for _, fp := range ftr.Partitions {
				offsets[fp.Partition] = fp.Offset
			}
		}
	}
	if got := offsets[0]; got != 1 {
		t.Errorf("t/0 committed offset: got %d, want 1", got)
	}
	if got := offsets[5]; got != -1 {
		t.Errorf("t/5 committed offset: got %d, want -1 (not stored)", got)
	}
}
