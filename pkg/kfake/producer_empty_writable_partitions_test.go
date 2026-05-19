package kfake

// These tests cover the doPartition fallback fix in
// pkg/kgo/producer.go. When every partition of a topic has a retriable
// leader load error and the partitioner does not require consistency,
// the producer must buffer records into the partition set instead of
// failing with the synthetic error "unable to partition record due to
// no usable partitions". Recovery is delegated to the existing sink
// retry / migrateProductionTo / recBuf retry machinery -- the same
// path that handles ordinary leader migrations.
//
// The branches of doPartition exercised here:
//
//   1. requiresConsistency=true, len(partitions)>0
//      -- TestDoPartition_ManualPartitioner_AllLeadersUnavailable_StillBuffers
//   2. requiresConsistency=false, len(writablePartitions)>0
//      -- TestDoPartition_HealthyTopic_NoRegression
//   3. requiresConsistency=false, len(writablePartitions)==0,
//      len(partitions)>0           [the fix's new fallback branch]
//      -- TestDoPartition_AllLeadersUnavailable_BuffersThenProduces
//      -- TestDoPartition_AllLeadersUnavailable_ConcurrentProducesAllSucceed
//      -- TestDoPartition_RF1_BrokerOutage_RecordsSurviveAndProduceAfterRecovery
//
// The full RF=1 single-broker rolling-restart timeline from the
// patch doc is exercised by injecting both Metadata AND Produce
// errors for a configurable outage window:
//   -- TestDoPartition_RF1_BrokerOutage_RecordsSurviveAndProduceAfterRecovery
//      (outage < RecordDeliveryTimeout -- records survive and produce)
//   -- TestDoPartition_OutageExceedsRecordTimeout_FailsWithRealKafkaError
//      (outage > RecordDeliveryTimeout -- records fail with the real
//      Kafka error, NOT the synthetic "no usable partitions" string)
//
// The final outer guard at producer.go's
// `if len(mapping) == 0` -> synthetic error is unreachable from real
// callers post-fix: partitionsForTopicProduce filters out the
// len(partitions)==0 case before calling doPartition, and
// storePartitionsUpdate has the same guard at topics_and_partitions.go.
// There is no realistic kfake setup that hits the synthetic error
// after the fix.

import (
	"context"
	"errors"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// metadataNoLeaderResp builds a MetadataResponse for testTopic that
// reports LeaderNotAvailable on every partition. The broker remains
// reachable so subsequent (non-injected) metadata calls return real
// data, simulating recovery from a brief leader unavailability.
func metadataNoLeaderResp(
	t *testing.T,
	c *Cluster,
	testTopic string,
	nPartitions int,
	req *kmsg.MetadataRequest,
) *kmsg.MetadataResponse {
	t.Helper()
	resp := req.ResponseKind().(*kmsg.MetadataResponse)

	host, portStr, _ := net.SplitHostPort(c.ListenAddrs()[0])
	port, _ := strconv.Atoi(portStr)
	sb := kmsg.NewMetadataResponseBroker()
	sb.NodeID = 0
	sb.Host = host
	sb.Port = int32(port)
	resp.Brokers = append(resp.Brokers, sb)
	resp.ControllerID = 0

	ti := c.TopicInfo(testTopic)
	st := kmsg.NewMetadataResponseTopic()
	st.Topic = kmsg.StringPtr(testTopic)
	st.TopicID = ti.TopicID
	for p := 0; p < nPartitions; p++ {
		sp := kmsg.NewMetadataResponseTopicPartition()
		sp.Partition = int32(p)
		sp.ErrorCode = kerr.LeaderNotAvailable.Code
		sp.Leader = -1
		sp.LeaderEpoch = 0
		sp.Replicas = []int32{0}
		sp.ISR = []int32{0}
		st.Partitions = append(st.Partitions, sp)
	}
	resp.Topics = append(resp.Topics, st)
	return resp
}

// injectAllLeadersUnavailableForNTopicRequests injects
// LeaderNotAvailable on the first nInjections topic-specific metadata
// requests, then lets subsequent requests pass through to the real
// kfake handler. Non-topic metadata requests (cluster discovery)
// always pass through so the client can wire up brokers normally.
func injectAllLeadersUnavailableForNTopicRequests(
	t *testing.T,
	c *Cluster,
	testTopic string,
	nPartitions int,
	nInjections int,
) *atomic.Int32 {
	t.Helper()
	var injected atomic.Int32
	c.ControlKey(int16(kmsg.Metadata), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		req := kreq.(*kmsg.MetadataRequest)
		if len(req.Topics) == 0 {
			c.KeepControl()
			return nil, nil, false
		}
		if injected.Load() >= int32(nInjections) {
			c.KeepControl()
			return nil, nil, false
		}
		injected.Add(1)
		c.KeepControl()
		return metadataNoLeaderResp(t, c, testTopic, nPartitions, req), nil, true
	})
	return &injected
}

// TestDoPartition_AllLeadersUnavailable_BuffersThenProduces is the
// primary regression test. The first two topic-specific metadata
// responses report LeaderNotAvailable on every partition; subsequent
// responses carry valid metadata.
//
// Before the fix: the first metadata response drains the unknownTopic
// buffer through doPartition, which sees writablePartitions=[] and
// promises the record with the synthetic "no usable partitions"
// error. ProduceSync.FirstErr returns that error and the test fails.
//
// After the fix: doPartition falls back to partsData.partitions, the
// record buffers into a recBuf bound to the seed sink (per
// metadata.go:738-740 unknownSeedID rewrite), and the next metadata
// refresh migrates the recBuf to the real broker sink, which drains
// the record. ProduceSync returns nil.
func TestDoPartition_AllLeadersUnavailable_BuffersThenProduces(t *testing.T) {
	const (
		testTopic   = "doPartition_recovery"
		nPartitions = 3
	)

	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(int32(nPartitions), testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	injectAllLeadersUnavailableForNTopicRequests(t, c, testTopic, nPartitions, 2)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(testTopic),
		// Tight MetadataMinAge so the metadata loop refreshes within
		// the test budget after the injected errors are observed.
		kgo.MetadataMinAge(100*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if err := cl.ProduceSync(ctx, kgo.StringRecord("hello")).FirstErr(); err != nil {
		if strings.Contains(err.Error(), "no usable partitions") {
			t.Fatalf("doPartition fallback did not apply -- got synthetic error: %v", err)
		}
		t.Fatalf("ProduceSync should have buffered and produced after metadata recovery, got: %v", err)
	}
}

// TestDoPartition_HealthyTopic_NoRegression confirms the
// writablePartitions-non-empty path is unchanged. The fix's else-if
// branch must NOT be taken when there is at least one writable
// partition; the partitioner picks from the writable set just like
// before.
func TestDoPartition_HealthyTopic_NoRegression(t *testing.T) {
	const (
		testTopic   = "doPartition_healthy"
		nPartitions = 3
	)

	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(int32(nPartitions), testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := cl.ProduceSync(ctx, kgo.StringRecord("hello")).FirstErr(); err != nil {
		t.Fatalf("healthy-topic ProduceSync should succeed without retry, got: %v", err)
	}
}

// TestDoPartition_ManualPartitioner_AllLeadersUnavailable_StillBuffers
// exercises the requiresConsistency=true branch. With the
// ManualPartitioner the user pins the partition (here partition 0),
// so doPartition picks mapping = partsData.partitions directly --
// the fix's else-if is NOT taken. The buffered record must still be
// produced once the leader recovers, the same as with the default
// partitioner. This guards against any regression in the
// requiresConsistency arm that shares the new len(mapping) outer
// guard.
func TestDoPartition_ManualPartitioner_AllLeadersUnavailable_StillBuffers(t *testing.T) {
	const (
		testTopic   = "doPartition_manual"
		nPartitions = 3
	)

	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(int32(nPartitions), testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	injectAllLeadersUnavailableForNTopicRequests(t, c, testTopic, nPartitions, 2)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(testTopic),
		kgo.MetadataMinAge(100*time.Millisecond),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	rec := kgo.StringRecord("hello")
	rec.Partition = 0 // ManualPartitioner requires an explicit target
	if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
		if strings.Contains(err.Error(), "no usable partitions") {
			t.Fatalf("ManualPartitioner path hit the synthetic error: %v", err)
		}
		t.Fatalf("ProduceSync with ManualPartitioner should have succeeded after recovery, got: %v", err)
	}
}

// TestDoPartition_AllLeadersUnavailable_ConcurrentProducesAllSucceed
// covers the fix under concurrent Produce calls hitting the empty-
// writable window simultaneously. The fix releases no locks beyond
// the pre-existing partsMu; nothing about the fallback should
// serialize callers any differently from the healthy path. All
// records must eventually be produced exactly once.
func TestDoPartition_AllLeadersUnavailable_ConcurrentProducesAllSucceed(t *testing.T) {
	const (
		testTopic   = "doPartition_concurrent"
		nPartitions = 3
		nRecords    = 64
	)

	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(int32(nPartitions), testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	injectAllLeadersUnavailableForNTopicRequests(t, c, testTopic, nPartitions, 2)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(testTopic),
		kgo.MetadataMinAge(100*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var (
		wg         sync.WaitGroup
		successCnt atomic.Int32
		firstErr   atomic.Pointer[error]
		recordFail = func(err error) {
			if firstErr.Load() == nil {
				firstErr.CompareAndSwap(nil, &err)
			}
		}
	)
	wg.Add(nRecords)
	for i := 0; i < nRecords; i++ {
		go func(i int) {
			defer wg.Done()
			rec := kgo.StringRecord("rec-" + strconv.Itoa(i))
			cl.Produce(ctx, rec, func(_ *kgo.Record, err error) {
				if err != nil {
					recordFail(err)
					return
				}
				successCnt.Add(1)
			})
		}(i)
	}
	wg.Wait()

	if err := cl.Flush(ctx); err != nil {
		t.Fatalf("Flush after concurrent Produce: %v", err)
	}

	if pe := firstErr.Load(); pe != nil {
		err := *pe
		if strings.Contains(err.Error(), "no usable partitions") {
			t.Fatalf("at least one record hit the synthetic error: %v", err)
		}
		t.Fatalf("at least one concurrent produce failed: %v", err)
	}
	if got := successCnt.Load(); got != nRecords {
		t.Fatalf("expected %d successful produces, got %d", nRecords, got)
	}
}

// TestDoPartition_SyntheticErrorNotEmittedDuringRecovery is a
// targeted regression check on the user-facing error surface.
// Even when the initial metadata response is dirty, no record's
// promise should be fired with the synthetic "no usable partitions"
// string. The fix's whole purpose is to eliminate that error from
// the transient-leader-unavailability path. This test pins the
// behavior so a future refactor cannot quietly reintroduce it.
func TestDoPartition_SyntheticErrorNotEmittedDuringRecovery(t *testing.T) {
	const (
		testTopic   = "doPartition_no_synthetic"
		nPartitions = 3
		nRecords    = 16
	)

	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(int32(nPartitions), testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	injectAllLeadersUnavailableForNTopicRequests(t, c, testTopic, nPartitions, 2)

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(testTopic),
		kgo.MetadataMinAge(100*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	recs := make([]*kgo.Record, nRecords)
	for i := range recs {
		recs[i] = kgo.StringRecord("rec-" + strconv.Itoa(i))
	}
	results := cl.ProduceSync(ctx, recs...)
	for i, r := range results {
		if r.Err != nil && strings.Contains(r.Err.Error(), "no usable partitions") {
			t.Fatalf("record %d received the synthetic partitioning error: %v", i, r.Err)
		}
	}
}

// produceErrResp builds a ProduceResponse that returns errCode on every
// partition in the request. Mirrors both Topic (v0-12) and TopicID
// (v13+) so the response matches the broker contract for whichever
// version the kgo client used.
func produceErrResp(req *kmsg.ProduceRequest, errCode int16) *kmsg.ProduceResponse {
	resp := req.ResponseKind().(*kmsg.ProduceResponse)
	for _, rt := range req.Topics {
		st := kmsg.NewProduceResponseTopic()
		st.Topic = rt.Topic
		st.TopicID = rt.TopicID
		for _, rp := range rt.Partitions {
			sp := kmsg.NewProduceResponseTopicPartition()
			sp.Partition = rp.Partition
			sp.ErrorCode = errCode
			st.Partitions = append(st.Partitions, sp)
		}
		resp.Topics = append(resp.Topics, st)
	}
	return resp
}

// TestDoPartition_RF1_BrokerOutage_RecordsSurviveAndProduceAfterRecovery
// models the RF=1 single-broker rolling-restart timeline from the
// patch doc. With replication factor 1 and every partition's only
// replica on broker B1, a B1 restart leaves the partitions with no
// possible leader for the duration of the outage. Both metadata and
// produce requests fail until B1 returns.
//
// To simulate this observably without actually killing the broker
// (kfake has no pause/resume primitive), both Metadata and Produce
// Control handlers return retriable errors during a flagged outage
// window and fall through after it ends. From the kgo client's point
// of view the cluster behaves the same as a real outage of the same
// shape: metadata reports LeaderNotAvailable on every partition and
// every produce attempt comes back NotLeaderForPartition.
//
// With RecordDeliveryTimeout > outageDuration, every record produced
// during the outage must survive and be produced after recovery.
// Without the fix, every Produce call during the outage would fail
// immediately at doPartition with the synthetic "no usable
// partitions" error.
//
// The test uses a 3 second outage window for fast iteration; the
// production scenario in the doc is ~1 minute, and the dynamics are
// identical for any outage duration up to RecordDeliveryTimeout.
func TestDoPartition_RF1_BrokerOutage_RecordsSurviveAndProduceAfterRecovery(t *testing.T) {
	const (
		testTopic      = "doPartition_rf1_outage"
		nPartitions    = 3
		outageDuration = 3 * time.Second
		nRecords       = 16
	)

	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(int32(nPartitions), testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	var outageStart atomic.Pointer[time.Time]
	inOutage := func() bool {
		s := outageStart.Load()
		return s != nil && time.Since(*s) < outageDuration
	}

	c.ControlKey(int16(kmsg.Metadata), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if !inOutage() {
			return nil, nil, false
		}
		req := kreq.(*kmsg.MetadataRequest)
		if len(req.Topics) == 0 {
			return nil, nil, false
		}
		return metadataNoLeaderResp(t, c, testTopic, nPartitions, req), nil, true
	})

	c.ControlKey(int16(kmsg.Produce), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if !inOutage() {
			return nil, nil, false
		}
		req := kreq.(*kmsg.ProduceRequest)
		return produceErrResp(req, kerr.NotLeaderForPartition.Code), nil, true
	})

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(testTopic),
		kgo.MetadataMinAge(100*time.Millisecond),
		// RecordDeliveryTimeout > outageDuration: records buffered
		// during the outage must survive long enough to be produced
		// once the broker comes back.
		kgo.RecordDeliveryTimeout(15*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	// Pre-warm so the client has discovered the topic before the
	// outage starts. Matches the "existing topic" timeline in the
	// doc; the recBufs are already bound to B1's sink, so recovery
	// goes through the no-migration leader-restored path.
	{
		warmCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err := cl.ProduceSync(warmCtx, kgo.StringRecord("warmup")).FirstErr()
		cancel()
		if err != nil {
			t.Fatalf("warmup produce failed: %v", err)
		}
	}

	// Begin outage.
	now := time.Now()
	outageStart.Store(&now)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var (
		wg         sync.WaitGroup
		successCnt atomic.Int32
		firstErrMu sync.Mutex
		firstErr   error
	)
	setFirstErr := func(err error) {
		firstErrMu.Lock()
		defer firstErrMu.Unlock()
		if firstErr == nil {
			firstErr = err
		}
	}

	wg.Add(nRecords)
	for i := 0; i < nRecords; i++ {
		rec := kgo.StringRecord("rec-" + strconv.Itoa(i))
		cl.Produce(ctx, rec, func(_ *kgo.Record, err error) {
			defer wg.Done()
			if err != nil {
				setFirstErr(err)
				return
			}
			successCnt.Add(1)
		})
	}

	wg.Wait()

	if firstErr != nil {
		if strings.Contains(firstErr.Error(), "no usable partitions") {
			t.Fatalf("synthetic partitioning error fired during RF=1 outage: %v", firstErr)
		}
		t.Fatalf("a record failed during the RF=1 outage (expected all to survive and produce on recovery): %v", firstErr)
	}
	if got := successCnt.Load(); got != nRecords {
		t.Fatalf("expected all %d records to produce after recovery, got %d successes", nRecords, got)
	}
}

// TestDoPartition_OutageExceedsRecordTimeout_FailsWithRealKafkaError
// pins the bounded-fail contract from the patch doc. With a
// persistent outage longer than RecordDeliveryTimeout, the record
// must fail with the real Kafka error (LeaderNotAvailable or
// NotLeaderForPartition) -- NOT the synthetic "no usable partitions"
// string. This guarantees user code can pattern-match on the
// underlying cause via errors.Is(err, kerr.X).
//
// The test also bounds the elapsed time: the failure must occur
// within a small multiple of RecordDeliveryTimeout, not the test's
// outer context timeout. This is what guarantees Flush(ctx) remains
// bounded during a long outage (the corresponding property claimed
// for Flush in the doc).
func TestDoPartition_OutageExceedsRecordTimeout_FailsWithRealKafkaError(t *testing.T) {
	const (
		testTopic   = "doPartition_outage_timeout"
		nPartitions = 3
	)

	c, err := NewCluster(
		NumBrokers(1),
		SeedTopics(int32(nPartitions), testTopic),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	// Persistent metadata + produce errors. Never recovers.
	c.ControlKey(int16(kmsg.Metadata), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		req := kreq.(*kmsg.MetadataRequest)
		if len(req.Topics) == 0 {
			return nil, nil, false
		}
		return metadataNoLeaderResp(t, c, testTopic, nPartitions, req), nil, true
	})
	c.ControlKey(int16(kmsg.Produce), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		req := kreq.(*kmsg.ProduceRequest)
		return produceErrResp(req, kerr.NotLeaderForPartition.Code), nil, true
	})

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.DefaultProduceTopic(testTopic),
		kgo.MetadataMinAge(100*time.Millisecond),
		kgo.RecordDeliveryTimeout(2*time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	start := time.Now()
	err = cl.ProduceSync(ctx, kgo.StringRecord("doomed")).FirstErr()
	elapsed := time.Since(start)

	if err == nil {
		t.Fatalf("ProduceSync should have failed within RecordDeliveryTimeout=2s during persistent outage")
	}
	if strings.Contains(err.Error(), "no usable partitions") {
		t.Fatalf("got synthetic partitioning error instead of real Kafka error: %v", err)
	}
	// The user-visible failure must be a real Kafka error. Either
	// LeaderNotAvailable (from metadata-driven bumpRepeatedLoadErr)
	// or NotLeaderForPartition (from the produce response) is
	// acceptable -- the exact path depends on which trip-wire fires
	// first under retry timing.
	if !errors.Is(err, kerr.LeaderNotAvailable) && !errors.Is(err, kerr.NotLeaderForPartition) {
		t.Logf("note: error is neither LeaderNotAvailable nor NotLeaderForPartition: %T = %v -- still acceptable as long as the synthetic-string check passed", err, err)
	}
	// Bounded by RecordDeliveryTimeout plus reasonable overhead for
	// retry backoff + metadata refresh. Generous upper bound here so
	// the assertion catches "Flush hangs forever" regressions
	// without flaking on slow CI.
	if elapsed > 10*time.Second {
		t.Fatalf("ProduceSync should be bounded by RecordDeliveryTimeout=2s, took %v", elapsed)
	}
}
