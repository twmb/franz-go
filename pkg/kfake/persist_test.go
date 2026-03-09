package kfake

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

func TestPersistProduceCloseReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Phase 1: create cluster, produce records, close
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
			SeedTopics(1, "test-topic"),
		)
		if err != nil {
			t.Fatal(err)
		}
		cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for i := range 10 {
			r := &kgo.Record{
				Topic: "test-topic",
				Key:   fmt.Appendf(nil, "key-%d", i),
				Value: fmt.Appendf(nil, "value-%d", i),
			}
			if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
		cl.Close()
		c.Close()
	}

	// Verify snapshot was written during clean shutdown
	snapPath := filepath.Join(dir, "partitions", "test-topic-0", "snapshot.json")
	if _, err := os.Stat(snapPath); err != nil {
		t.Fatalf("expected snapshot.json after clean shutdown: %v", err)
	}

	// Phase 2: reopen, consume and verify
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		cl := newPlainClient(t, c,
			kgo.ConsumeTopics("test-topic"),
			kgo.FetchMaxWait(250*time.Millisecond),
		)

		records := collectRecords(t, cl, 10, 5*time.Second)
		if len(records) != 10 {
			t.Fatalf("expected 10 records, got %d", len(records))
		}
		for i, r := range records {
			if string(r.Key) != fmt.Sprintf("key-%d", i) {
				t.Fatalf("record %d: expected key key-%d, got %s", i, i, string(r.Key))
			}
			if string(r.Value) != fmt.Sprintf("value-%d", i) {
				t.Fatalf("record %d: expected value value-%d, got %s", i, i, string(r.Value))
			}
		}
	}
}

func TestPersistSyncWritesCrashRecovery(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Phase 1: create cluster with SyncWrites, produce, then simulate crash (don't Close)
	{
		c, err := NewCluster(
			DataDir(dir),
			SyncWrites(),
			NumBrokers(1),
			SeedTopics(1, "sync-topic"),
		)
		if err != nil {
			t.Fatal(err)
		}
		cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for i := range 5 {
			r := &kgo.Record{
				Topic: "sync-topic",
				Key:   fmt.Appendf(nil, "k%d", i),
				Value: fmt.Appendf(nil, "v%d", i),
			}
			if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
		cl.Close()
		// Simulate crash: don't call c.Close() - just abandon it
		// The SyncWrites mode should have fsynced each batch to disk already.
	}

	// Phase 2: reopen without Close having been called, verify data recovered
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		cl := newPlainClient(t, c,
			kgo.ConsumeTopics("sync-topic"),
			kgo.FetchMaxWait(250*time.Millisecond),
		)

		records := collectRecords(t, cl, 5, 5*time.Second)
		if len(records) != 5 {
			t.Fatalf("expected 5 records, got %d", len(records))
		}
	}
}

func TestPersistGroupCommitsRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	const topic = "group-topic"
	const group = "test-group"

	// Phase 1: produce, consume with group, commit offsets, close
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
			SeedTopics(1, topic),
		)
		if err != nil {
			t.Fatal(err)
		}

		// Produce records
		produceN(t, c, topic, 10)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Consume 5 records and commit
		consCl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup(group),
			kgo.HeartbeatInterval(100*time.Millisecond),
			kgo.FetchMaxWait(250*time.Millisecond),
		)
		if err != nil {
			t.Fatal(err)
		}
		var consumed int
		for consumed < 5 {
			fetches := consCl.PollFetches(ctx)
			fetches.EachRecord(func(_ *kgo.Record) { consumed++ })
		}
		if err := consCl.CommitUncommittedOffsets(ctx); err != nil {
			t.Fatal(err)
		}
		consCl.Close()
		c.Close()
	}

	// Phase 2: reopen, consume from group - should resume from offset 5
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		// Check committed offset via admin
		adm := kadm.NewClient(newPlainClient(t, c))
		offsets, err := adm.FetchOffsets(context.Background(), group)
		if err != nil {
			t.Fatal(err)
		}
		o, ok := offsets.Lookup(topic, 0)
		if !ok {
			t.Fatal("expected committed offset for partition 0")
		}
		if o.At < 5 {
			t.Fatalf("expected committed offset >= 5, got %d", o.At)
		}
		if o.LeaderEpoch < 0 {
			t.Fatalf("expected leaderEpoch >= 0, got %d", o.LeaderEpoch)
		}
	}
}

func TestPersistPIDEpochRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Phase 1: init a producer ID, close
	var origPID int64
	var origEpoch int16
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
			SeedTopics(1, "pid-topic"),
		)
		if err != nil {
			t.Fatal(err)
		}
		cl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.TransactionalID("test-txn"),
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// The client auto-inits the producer ID when used transactionally
		if err := cl.BeginTransaction(); err != nil {
			t.Fatal(err)
		}
		r := &kgo.Record{Topic: "pid-topic", Value: []byte("txn-data")}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatal(err)
		}
		if err := cl.EndTransaction(ctx, kgo.TryCommit); err != nil {
			t.Fatal(err)
		}

		// Get the PID/epoch by describing producers
		adm := kadm.NewClient(newPlainClient(t, c))
		txns, err := adm.DescribeTransactions(ctx, "test-txn")
		if err != nil {
			t.Fatal(err)
		}
		if len(txns) == 0 {
			t.Fatal("expected DescribeTransactions to return the transaction, got 0 results")
		}
		for _, txn := range txns {
			origPID = txn.ProducerID
			origEpoch = txn.ProducerEpoch
		}
		cl.Close()
		c.Close()
	}

	// Phase 2: reopen, verify PID is recoverable
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		// Init a new client with same txn ID - should get same PID with bumped epoch
		cl := newPlainClient(t, c,
			kgo.TransactionalID("test-txn"),
		)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		if err := cl.BeginTransaction(); err != nil {
			t.Fatal(err)
		}
		r := &kgo.Record{Topic: "pid-topic", Value: []byte("txn-data-2")}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatal(err)
		}
		if err := cl.EndTransaction(ctx, kgo.TryCommit); err != nil {
			t.Fatal(err)
		}

		adm := kadm.NewClient(newPlainClient(t, c))
		txns, err := adm.DescribeTransactions(ctx, "test-txn")
		if err != nil {
			t.Fatal(err)
		}
		if len(txns) == 0 {
			t.Fatal("expected DescribeTransactions to return the transaction, got 0 results")
		}
		for _, txn := range txns {
			if txn.ProducerID != origPID {
				t.Fatalf("expected same PID %d after restart, got %d", origPID, txn.ProducerID)
			}
			if txn.ProducerEpoch <= origEpoch {
				t.Fatalf("expected epoch > %d after restart, got %d", origEpoch, txn.ProducerEpoch)
			}
		}
	}
}

func TestPersistACLsRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Phase 1: create with ACLs, close
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
			EnableSASL(),
			Superuser("PLAIN", "admin", "admin"),
			EnableACLs(),
		)
		if err != nil {
			t.Fatal(err)
		}
		saslOpt := kgo.SASL(plain.Plain(func(_ context.Context) (plain.Auth, error) {
			return plain.Auth{User: "admin", Pass: "admin"}, nil
		}))
		adm := kadm.NewClient(newPlainClient(t, c, saslOpt))

		// Create an ACL
		results, err := adm.CreateACLs(context.Background(), kadm.NewACLs().
			Allow("User:testuser").
			AllowHosts("*").
			Topics("my-topic").
			Operations(kadm.OpRead).
			ResourcePatternType(kadm.ACLPatternLiteral),
		)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range results {
			if r.Err != nil {
				t.Fatal(r.Err)
			}
		}
		c.Close()
	}

	// Phase 2: reopen, verify ACLs persisted
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
			EnableSASL(),
			Superuser("PLAIN", "admin", "admin"),
			EnableACLs(),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		saslOpt := kgo.SASL(plain.Plain(func(_ context.Context) (plain.Auth, error) {
			return plain.Auth{User: "admin", Pass: "admin"}, nil
		}))
		adm := kadm.NewClient(newPlainClient(t, c, saslOpt))
		described, err := adm.DescribeACLs(context.Background(), kadm.NewACLs().
			Allow("User:testuser").
			AllowHosts("*").
			Topics("my-topic").
			Operations(kadm.OpRead).
			ResourcePatternType(kadm.ACLPatternLiteral),
		)
		if err != nil {
			t.Fatal(err)
		}
		if len(described) == 0 {
			t.Fatal("expected ACLs to be persisted after restart, got 0")
		}
	}
}

func TestPersistBrokerConfigsRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Phase 1: set broker configs, close
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
			BrokerConfigs(map[string]string{
				"log.retention.ms": "86400000",
			}),
		)
		if err != nil {
			t.Fatal(err)
		}
		c.Close()
	}

	// Phase 2: reopen, verify config persisted
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		cl := newPlainClient(t, c)
		req := kmsg.NewDescribeConfigsRequest()
		rc := kmsg.NewDescribeConfigsRequestResource()
		rc.ResourceType = kmsg.ConfigResourceTypeBroker
		rc.ResourceName = "0"
		rc.ConfigNames = []string{"log.retention.ms"}
		req.Resources = append(req.Resources, rc)

		resp, err := req.RequestWith(context.Background(), cl)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range resp.Resources {
			for _, c := range r.Configs {
				if c.Name == "log.retention.ms" && c.Value != nil && *c.Value == "86400000" {
					return // success
				}
			}
		}
		t.Fatal("expected log.retention.ms=86400000 to be persisted")
	}
}

func TestPersistTopicConfigsRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	topic := "config-topic"

	// Phase 1: create topic, set topic configs via AlterConfigs, close.
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
			SeedTopics(1, topic),
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		adm := kadm.NewClient(newPlainClient(t, c))
		retBytes := "1048576"
		retMs := "3600000"
		resp, err := adm.AlterTopicConfigs(ctx, []kadm.AlterConfig{
			{Name: "retention.bytes", Value: &retBytes},
			{Name: "retention.ms", Value: &retMs},
		}, topic)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range resp {
			if r.Err != nil {
				t.Fatalf("AlterTopicConfigs: %v", r.Err)
			}
		}
		c.Close()
	}

	// Phase 2: reopen, verify topic configs persisted.
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		cl := newPlainClient(t, c)
		req := kmsg.NewDescribeConfigsRequest()
		rc := kmsg.NewDescribeConfigsRequestResource()
		rc.ResourceType = kmsg.ConfigResourceTypeTopic
		rc.ResourceName = topic
		rc.ConfigNames = []string{"retention.bytes", "retention.ms"}
		req.Resources = append(req.Resources, rc)

		resp, err := req.RequestWith(context.Background(), cl)
		if err != nil {
			t.Fatal(err)
		}
		found := map[string]string{}
		for _, r := range resp.Resources {
			for _, c := range r.Configs {
				if c.Value != nil {
					found[c.Name] = *c.Value
				}
			}
		}
		if found["retention.bytes"] != "1048576" {
			t.Fatalf("retention.bytes: got %q, want %q", found["retention.bytes"], "1048576")
		}
		if found["retention.ms"] != "3600000" {
			t.Fatalf("retention.ms: got %q, want %q", found["retention.ms"], "3600000")
		}
	}
}

func TestPersistCRCCorruption(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Phase 1: create cluster with SyncWrites, produce, close properly
	{
		c, err := NewCluster(
			DataDir(dir),
			SyncWrites(),
			NumBrokers(1),
			SeedTopics(1, "crc-topic"),
		)
		if err != nil {
			t.Fatal(err)
		}
		produceN(t, c, "crc-topic", 5)
		c.Close()
	}

	// Delete snapshot.json so Phase 2 forces a full replay through
	// the corrupted segment files (not the snapshot path).
	partDir := filepath.Join(dir, "partitions", "crc-topic-0")
	os.Remove(filepath.Join(partDir, "snapshot.json"))

	// Corrupt the last entry in a segment file.
	entries, err := os.ReadDir(partDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".dat" {
			path := filepath.Join(partDir, e.Name())
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if len(data) > 10 {
				// Corrupt the last few bytes
				data[len(data)-3] ^= 0xFF
				data[len(data)-5] ^= 0xFF
				if err := os.WriteFile(path, data, 0o644); err != nil {
					t.Fatal(err)
				}
			}
		}
	}

	// Phase 2: reopen with full replay - should truncate corrupt entry
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		// HWM should reflect only the recovered (non-corrupt) batches.
		pi := c.PartitionInfo("crc-topic", 0)
		if pi == nil {
			t.Fatal("partition info not found after reopen")
		}
		if pi.HighWatermark >= 5 {
			t.Fatalf("expected HWM < 5 after corruption truncation, got %d", pi.HighWatermark)
		}

		cl := newPlainClient(t, c,
			kgo.ConsumeTopics("crc-topic"),
			kgo.FetchMaxWait(250*time.Millisecond),
		)

		records := collectRecords(t, cl, 1, 3*time.Second)
		if len(records) >= 5 {
			t.Fatalf("expected fewer than 5 records after corruption, got %d", len(records))
		}
	}
}

func TestPersistSegmentRollover(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Use a very small segment size to force rollover
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
			SeedTopics(1, "seg-topic"),
			BrokerConfigs(map[string]string{
				"log.segment.bytes": "100", // tiny segment size
			}),
		)
		if err != nil {
			t.Fatal(err)
		}
		cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for i := range 20 {
			r := &kgo.Record{Topic: "seg-topic", Value: fmt.Appendf(nil, "value-%d-padding-data", i)}
			if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
		cl.Close()
		c.Close()
	}

	// Verify multiple segment files were created
	partDir := filepath.Join(dir, "partitions", "seg-topic-0")
	entries, err := os.ReadDir(partDir)
	if err != nil {
		t.Fatal(err)
	}
	var segCount int
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".dat" {
			segCount++
		}
	}
	if segCount < 2 {
		t.Fatalf("expected multiple segment files with tiny segment size, got %d", segCount)
	}
	// Reopen and verify all records
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		cl := newPlainClient(t, c,
			kgo.ConsumeTopics("seg-topic"),
			kgo.FetchMaxWait(250*time.Millisecond),
		)

		records := collectRecords(t, cl, 20, 5*time.Second)
		if len(records) != 20 {
			t.Fatalf("expected 20 records after segment rollover restart, got %d", len(records))
		}
	}
}

func TestPersistNoDataDir(t *testing.T) {
	t.Parallel()

	// Without DataDir, persistence should be a no-op
	c := newCluster(t,
		NumBrokers(1),
		SeedTopics(1, "no-persist"),
	)

	cl := newPlainClient(t, c)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	r := &kgo.Record{Topic: "no-persist", Value: []byte("test")}
	if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
		t.Fatal(err)
	}
}

func TestPersistMultipleTopics(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	topics := []string{"topic-a", "topic-b", "topic-c"}

	// Phase 1: create multiple topics with data
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
			SeedTopics(2, topics...),
		)
		if err != nil {
			t.Fatal(err)
		}
		cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for _, topic := range topics {
			for i := range 3 {
				r := &kgo.Record{Topic: topic, Value: fmt.Appendf(nil, "%s-v%d", topic, i)}
				if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
					t.Fatal(err)
				}
			}
		}
		cl.Close()
		c.Close()
	}

	// Phase 2: reopen and verify all topics
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		adm := kadm.NewClient(newPlainClient(t, c))
		tl, err := adm.ListTopics(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		for _, topic := range topics {
			if _, ok := tl[topic]; !ok {
				t.Fatalf("expected topic %s to exist after restart", topic)
			}
		}
	}
}

// TestPersistSyncWritesGroupCommitCrash verifies that group offset
// commits are durable with SyncWrites even without a clean shutdown.
// This exercises the live persistGroupEntry path for OffsetCommit.
func TestPersistSyncWritesGroupCommitCrash(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	const topic = "sw-group-topic"
	const group = "sw-group"

	// Phase 1: produce, consume with group, commit, then crash (no Close)
	{
		c, err := NewCluster(
			DataDir(dir),
			SyncWrites(),
			NumBrokers(1),
			SeedTopics(1, topic),
		)
		if err != nil {
			t.Fatal(err)
		}

		produceN(t, c, topic, 10)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		consCl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup(group),
			kgo.HeartbeatInterval(100*time.Millisecond),
			kgo.FetchMaxWait(250*time.Millisecond),
		)
		if err != nil {
			t.Fatal(err)
		}
		var consumed int
		for consumed < 5 {
			fetches := consCl.PollFetches(ctx)
			fetches.EachRecord(func(_ *kgo.Record) { consumed++ })
		}
		if err := consCl.CommitUncommittedOffsets(ctx); err != nil {
			t.Fatal(err)
		}
		consCl.Close()
		// Simulate crash: don't call c.Close()
	}

	// Phase 2: reopen, verify committed offsets survived the crash
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		adm := kadm.NewClient(newPlainClient(t, c))
		offsets, err := adm.FetchOffsets(context.Background(), group)
		if err != nil {
			t.Fatal(err)
		}
		o, ok := offsets.Lookup(topic, 0)
		if !ok {
			t.Fatal("expected committed offset to survive crash with SyncWrites")
		}
		if o.At < 5 {
			t.Fatalf("expected committed offset >= 5, got %d", o.At)
		}
	}
}

// TestPersistSyncWritesOffsetDeleteCrash verifies that OffsetDelete
// entries survive a crash with SyncWrites.
func TestPersistSyncWritesOffsetDeleteCrash(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	const topic = "del-topic"
	const group = "del-group"

	// Phase 1: produce, commit offsets, then delete them, then crash
	{
		c, err := NewCluster(
			DataDir(dir),
			SyncWrites(),
			NumBrokers(1),
			SeedTopics(1, topic),
		)
		if err != nil {
			t.Fatal(err)
		}

		produceN(t, c, topic, 5)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		consCl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup(group),
			kgo.HeartbeatInterval(100*time.Millisecond),
			kgo.FetchMaxWait(250*time.Millisecond),
		)
		if err != nil {
			t.Fatal(err)
		}
		var consumed int
		for consumed < 5 {
			fetches := consCl.PollFetches(ctx)
			fetches.EachRecord(func(_ *kgo.Record) { consumed++ })
		}
		if err := consCl.CommitUncommittedOffsets(ctx); err != nil {
			t.Fatal(err)
		}
		consCl.Close()

		// Delete the committed offset via protocol
		cl := newPlainClient(t, c)
		req := kmsg.NewOffsetDeleteRequest()
		req.Group = group
		rt := kmsg.NewOffsetDeleteRequestTopic()
		rt.Topic = topic
		rp := kmsg.NewOffsetDeleteRequestTopicPartition()
		rp.Partition = 0
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)
		if _, err := req.RequestWith(ctx, cl); err != nil {
			t.Fatal(err)
		}
		// Crash: don't call c.Close()
	}

	// Phase 2: reopen, verify the offset delete survived.
	// The group may not exist at all (GROUP_ID_NOT_FOUND) since
	// the only commit was deleted - that's also a valid outcome.
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		adm := kadm.NewClient(newPlainClient(t, c))
		offsets, err := adm.FetchOffsets(context.Background(), group)
		if errors.Is(err, kerr.GroupIDNotFound) {
			return // group has no state - valid after offset delete
		}
		if err != nil {
			t.Fatal(err)
		}
		_, ok := offsets.Lookup(topic, 0)
		if ok {
			t.Fatal("expected deleted offset to remain deleted after crash with SyncWrites")
		}
	}
}

// TestPersistSyncWritesTxnOffsetCommitCrash verifies that transactional
// offset commits (applied at EndTxn) are durable with SyncWrites even
// without a clean shutdown.
func TestPersistSyncWritesTxnOffsetCommitCrash(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	const topic = "txn-oc-topic"
	const group = "txn-oc-group"

	// Phase 1: produce, then use transactional offset commit, crash
	{
		c, err := NewCluster(
			DataDir(dir),
			SyncWrites(),
			NumBrokers(1),
			SeedTopics(1, topic),
		)
		if err != nil {
			t.Fatal(err)
		}

		// Produce some records
		produceN(t, c, topic, 10)

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Use all raw kmsg requests for the transactional flow so we
		// control the ProducerID/Epoch explicitly.
		cl := newPlainClient(t, c)

		// InitProducerID
		initReq := kmsg.NewInitProducerIDRequest()
		initReq.TransactionalID = stringp("txn-oc-txid")
		initReq.TransactionTimeoutMillis = 30000
		initReq.ProducerID = -1
		initReq.ProducerEpoch = -1
		initResp, err := initReq.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		if initResp.ErrorCode != 0 {
			t.Fatalf("InitProducerID failed with code %d", initResp.ErrorCode)
		}
		pid := initResp.ProducerID
		epoch := initResp.ProducerEpoch

		// AddOffsetsToTxn: register the group with the transaction
		aotReq := kmsg.NewAddOffsetsToTxnRequest()
		aotReq.TransactionalID = "txn-oc-txid"
		aotReq.ProducerID = pid
		aotReq.ProducerEpoch = epoch
		aotReq.Group = group
		aotResp, err := aotReq.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		if aotResp.ErrorCode != 0 {
			t.Fatalf("AddOffsetsToTxn failed with code %d", aotResp.ErrorCode)
		}

		// TxnOffsetCommit: commit offset 7 for the group
		tocReq := kmsg.NewTxnOffsetCommitRequest()
		tocReq.TransactionalID = "txn-oc-txid"
		tocReq.ProducerID = pid
		tocReq.ProducerEpoch = epoch
		tocReq.Group = group
		tocReq.Generation = -1
		tocReq.MemberID = ""
		tocT := kmsg.NewTxnOffsetCommitRequestTopic()
		tocT.Topic = topic
		tocP := kmsg.NewTxnOffsetCommitRequestTopicPartition()
		tocP.Partition = 0
		tocP.Offset = 7
		tocP.LeaderEpoch = -1
		tocT.Partitions = append(tocT.Partitions, tocP)
		tocReq.Topics = append(tocReq.Topics, tocT)
		tocR, err := tocReq.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		for _, rt := range tocR.Topics {
			for _, rp := range rt.Partitions {
				if rp.ErrorCode != 0 {
					t.Fatalf("TxnOffsetCommit %s/%d failed with code %d", rt.Topic, rp.Partition, rp.ErrorCode)
				}
			}
		}

		// EndTxn: commit the transaction
		endReq := kmsg.NewEndTxnRequest()
		endReq.TransactionalID = "txn-oc-txid"
		endReq.ProducerID = pid
		endReq.ProducerEpoch = epoch
		endReq.Commit = true
		endResp, err := endReq.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		if endResp.ErrorCode != 0 {
			t.Fatalf("EndTxn failed with code %d", endResp.ErrorCode)
		}

		cl.Close()
		// Crash: don't call c.Close()
	}

	// Phase 2: reopen, verify transactional offset commit survived
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		adm := kadm.NewClient(newPlainClient(t, c))
		offsets, err := adm.FetchOffsets(context.Background(), group)
		if err != nil {
			t.Fatal(err)
		}
		o, ok := offsets.Lookup(topic, 0)
		if !ok {
			t.Fatal("expected transactional committed offset to survive crash with SyncWrites")
		}
		if o.At != 7 {
			t.Fatalf("expected transactional committed offset 7, got %d", o.At)
		}
	}
}

// TestPersistClassicGroupGenerationCrash verifies that classic group
// generation survives a crash with SyncWrites. Multiple rebalance cycles
// should persist each generation bump.
func TestPersistClassicGroupGenerationCrash(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	const topic = "gen-topic"
	const group = "gen-group"

	// Phase 1: create group, trigger multiple rebalance cycles, crash
	var lastGeneration int32
	{
		c, err := NewCluster(
			DataDir(dir),
			SyncWrites(),
			NumBrokers(1),
			SeedTopics(1, topic),
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()

		// Produce a record so PollFetches completes quickly.
		prodCl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
		if err != nil {
			t.Fatal(err)
		}
		if err := prodCl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("x")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
		prodCl.Close()

		// Join and leave 3 times to bump generation. Each kgo
		// join/close cycle triggers two completeRebalance calls
		// (one on join, one on leave), each bumping generation.
		for range 3 {
			cl, err := kgo.NewClient(
				kgo.SeedBrokers(c.ListenAddrs()...),
				kgo.ConsumeTopics(topic),
				kgo.ConsumerGroup(group),
				kgo.HeartbeatInterval(100*time.Millisecond),
				kgo.FetchMaxWait(250*time.Millisecond),
			)
			if err != nil {
				t.Fatal(err)
			}
			cl.PollFetches(ctx)
			cl.Close()
		}

		// Read groups.log to find the max persisted generation.
		lastGeneration = maxGenerationInGroupsLog(t, filepath.Join(dir, "groups.log"))
		if lastGeneration < 3 {
			t.Fatalf("expected generation >= 3 after 3 rebalances, got %d", lastGeneration)
		}
		// Crash: don't call c.Close()
	}

	// Phase 2: reopen, raw JoinGroup to verify generation continues.
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		cl := newPlainClient(t, c)

		recoveredGen := rawJoinGeneration(ctx, t, cl, group, topic)
		// After recovery, the next join should produce generation > lastGeneration.
		if recoveredGen <= lastGeneration {
			t.Fatalf("expected recovered generation > %d, got %d", lastGeneration, recoveredGen)
		}
	}
}

// rawJoinGeneration performs a raw JoinGroup+SyncGroup to observe the generation.
func rawJoinGeneration(ctx context.Context, t *testing.T, cl *kgo.Client, group, topic string, instanceID ...string) int32 {
	t.Helper()

	proto := kmsg.NewJoinGroupRequestProtocol()
	proto.Name = "range"
	meta := kmsg.NewConsumerMemberMetadata()
	meta.Topics = []string{topic}
	proto.Metadata = meta.AppendTo(nil)

	joinReq := kmsg.NewJoinGroupRequest()
	joinReq.Group = group
	joinReq.SessionTimeoutMillis = 30000
	joinReq.RebalanceTimeoutMillis = 60000
	joinReq.ProtocolType = "consumer"
	joinReq.Protocols = []kmsg.JoinGroupRequestProtocol{proto}
	if len(instanceID) > 0 {
		joinReq.InstanceID = &instanceID[0]
	}

	joinResp, err := joinReq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if joinResp.ErrorCode == 79 { // MEMBER_ID_REQUIRED
		joinReq.MemberID = joinResp.MemberID
		joinResp, err = joinReq.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
	}
	if joinResp.ErrorCode != 0 {
		t.Fatalf("JoinGroup error code %d", joinResp.ErrorCode)
	}

	// Complete the SyncGroup to stabilize.
	syncReq := kmsg.NewSyncGroupRequest()
	syncReq.Group = group
	syncReq.Generation = joinResp.Generation
	syncReq.MemberID = joinResp.MemberID
	syncReq.ProtocolType = stringp("consumer")
	syncReq.Protocol = stringp("range")
	if joinResp.LeaderID == joinResp.MemberID {
		for _, m := range joinResp.Members {
			ga := kmsg.NewSyncGroupRequestGroupAssignment()
			ga.MemberID = m.MemberID
			ma := kmsg.NewConsumerMemberAssignment()
			ma.Topics = []kmsg.ConsumerMemberAssignmentTopic{{
				Topic:      topic,
				Partitions: []int32{0},
			}}
			ga.MemberAssignment = ma.AppendTo(nil)
			syncReq.GroupAssignment = append(syncReq.GroupAssignment, ga)
		}
	}
	syncResp, err := syncReq.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	if syncResp.ErrorCode != 0 {
		t.Fatalf("SyncGroup error code %d", syncResp.ErrorCode)
	}

	return joinResp.Generation
}

// TestPersistStaticMemberDeleteCrash verifies that static member deletions
// survive a crash. After removing a static member and crashing, the
// reopened cluster should not have a phantom static member mapping.
func TestPersistStaticMemberDeleteCrash(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	const topic = "static-del-topic"
	const group = "static-del-group"
	const instanceID = "static-instance-1"

	// Phase 1: join with static member, explicitly leave, crash
	{
		c, err := NewCluster(
			DataDir(dir),
			SyncWrites(),
			NumBrokers(1),
			SeedTopics(1, topic),
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Produce a record so PollFetches completes quickly.
		prodCl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
		if err != nil {
			t.Fatal(err)
		}
		if err := prodCl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("x")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
		prodCl.Close()

		// Join with a static member via kgo.
		cl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup(group),
			kgo.InstanceID(instanceID),
			kgo.HeartbeatInterval(100*time.Millisecond),
			kgo.FetchMaxWait(250*time.Millisecond),
		)
		if err != nil {
			t.Fatal(err)
		}
		cl.PollFetches(ctx)
		// kgo does NOT send LeaveGroup on Close for static members
		// (correct per KIP-345). Close without leaving.
		cl.Close()

		// Verify static set entry exists.
		glPath := filepath.Join(dir, "groups.log")
		var hasStaticSet bool
		for _, entry := range readGroupsLogEntries(t, glPath) {
			if entry["type"] == "static" {
				iid, _ := entry["instance"].(string)
				mid, _ := entry["member"].(string)
				if iid == instanceID && mid != "" {
					hasStaticSet = true
				}
			}
		}
		if !hasStaticSet {
			t.Fatal("expected static member set entry in groups.log")
		}

		// Now send an explicit LeaveGroup v3 with the instanceID to
		// remove the static member. This requires a plain client.
		plainCl := newPlainClient(t, c)
		leaveReq := kmsg.NewLeaveGroupRequest()
		leaveReq.Group = group
		leaveReq.Version = 3
		lm := kmsg.NewLeaveGroupRequestMember()
		lm.InstanceID = stringp(instanceID)
		leaveReq.Members = append(leaveReq.Members, lm)
		leaveResp, err := leaveReq.RequestWith(ctx, plainCl)
		if err != nil {
			t.Fatal(err)
		}
		for _, m := range leaveResp.Members {
			if m.ErrorCode != 0 {
				t.Fatalf("LeaveGroup member error %d", m.ErrorCode)
			}
		}
		plainCl.Close()

		// Verify the deletion was persisted.
		var hasStaticDel bool
		for _, entry := range readGroupsLogEntries(t, glPath) {
			if entry["type"] == "static" {
				iid, _ := entry["instance"].(string)
				mid, _ := entry["member"].(string)
				if iid == instanceID && mid == "" {
					hasStaticDel = true
				}
			}
		}
		if !hasStaticDel {
			t.Fatal("expected static member delete entry in groups.log")
		}

		// Crash: don't call c.Close()
	}

	// Phase 2: reopen, raw JoinGroup with same instanceID - should succeed.
	// If the static member deletion wasn't persisted, the recovered cluster
	// would have a phantom mapping (instanceID -> old memberID). A new join
	// with the same instanceID would fence the phantom member and get
	// FENCED_INSTANCE_ID. Using the instanceID here (not a dynamic join)
	// ensures we actually exercise that code path.
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		cl := newPlainClient(t, c)

		gen := rawJoinGeneration(ctx, t, cl, group, topic, instanceID)
		if gen <= 0 {
			t.Fatalf("expected positive generation, got %d", gen)
		}
	}
}

// TestPersistFullReplayAbortedTxns verifies that abortedTxns are correctly
// reconstructed during full replay (when snapshot is missing). A read_committed
// consumer should not see records from an aborted transaction.
func TestPersistAbortedTxnsRestart(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name           string
		deleteSnapshot bool
	}{
		{"full-replay", true},
		{"snapshot", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			dir := t.TempDir()
			topic := "abort-" + tc.name

			// Phase 1: produce committed + aborted transactional records, clean shutdown.
			{
				c, err := NewCluster(DataDir(dir), SyncWrites(), NumBrokers(1), SeedTopics(1, topic))
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				cl, err := kgo.NewClient(
					kgo.SeedBrokers(c.ListenAddrs()...),
					kgo.TransactionalID("txn-"+tc.name),
					kgo.TransactionTimeout(30*time.Second),
					kgo.RecordPartitioner(kgo.ManualPartitioner()),
				)
				if err != nil {
					t.Fatal(err)
				}
				if err := cl.BeginTransaction(); err != nil {
					t.Fatal(err)
				}
				for i := range 3 {
					r := &kgo.Record{Topic: topic, Partition: 0, Value: fmt.Appendf(nil, "committed-%d", i)}
					if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
						t.Fatal(err)
					}
				}
				if err := cl.EndTransaction(ctx, kgo.TryCommit); err != nil {
					t.Fatal(err)
				}
				if err := cl.BeginTransaction(); err != nil {
					t.Fatal(err)
				}
				for i := range 2 {
					r := &kgo.Record{Topic: topic, Partition: 0, Value: fmt.Appendf(nil, "aborted-%d", i)}
					if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
						t.Fatal(err)
					}
				}
				if err := cl.EndTransaction(ctx, kgo.TryAbort); err != nil {
					t.Fatal(err)
				}
				cl.Close()
				c.Close()
			}

			snapPath := filepath.Join(dir, "partitions", topic+"-0", "snapshot.json")
			if tc.deleteSnapshot {
				if err := os.Remove(snapPath); err != nil {
					t.Fatal(err)
				}
			} else {
				if _, err := os.Stat(snapPath); err != nil {
					t.Fatalf("expected snapshot.json to exist: %v", err)
				}
			}

			// Phase 2: reopen and verify read_committed filters aborted records.
			{
				c, err := NewCluster(DataDir(dir), NumBrokers(1))
				if err != nil {
					t.Fatal(err)
				}
				defer c.Close()
				cl := newPlainClient(t, c,
					kgo.FetchIsolationLevel(kgo.ReadCommitted()),
					kgo.FetchMaxWait(250*time.Millisecond),
					kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
						topic: {0: kgo.NewOffset().AtStart()},
					}),
				)
				records := collectRecords(t, cl, 3, 3*time.Second)
				if len(records) != 3 {
					var vals []string
					for _, r := range records {
						vals = append(vals, string(r.Value))
					}
					t.Fatalf("expected 3 committed records, got %d: %v", len(records), vals)
				}
				for _, r := range records {
					v := string(r.Value)
					if len(v) >= 8 && v[:8] == "aborted-" {
						t.Fatalf("read_committed consumer received aborted record: %s", v)
					}
				}
				pi := c.PartitionInfo(topic, 0)
				if pi == nil {
					t.Fatal("partition info not found")
				}
				if pi.HighWatermark <= 3 {
					t.Fatalf("expected HWM > 3 (aborted batches should exist), got %d", pi.HighWatermark)
				}
			}
		})
	}
}

// TestPersistSaveGroupsLogCloseBeforeTruncate verifies that the
// saveGroupsLog close-before-truncate fix works correctly. Multiple
// groups with committed offsets should survive a clean shutdown where
// saveGroupsLog rewrites groups.log via O_TRUNC.
func TestPersistSaveGroupsLogCloseBeforeTruncate(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	const topic = "trunc-topic"

	// Phase 1: create multiple groups with live offset commits, close cleanly.
	{
		c, err := NewCluster(
			DataDir(dir),
			SyncWrites(),
			NumBrokers(1),
			SeedTopics(1, topic),
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Produce records
		produceN(t, c, topic, 20)

		// Create 5 groups, each commits offsets. This exercises the
		// live groupsLogFile (SyncWrites appends commit entries), then
		// Close() calls saveGroupsLog which must close the live handle
		// before truncating and rewriting.
		for i := range 3 {
			group := fmt.Sprintf("trunc-group-%d", i)
			cl, err := kgo.NewClient(
				kgo.SeedBrokers(c.ListenAddrs()...),
				kgo.ConsumeTopics(topic),
				kgo.ConsumerGroup(group),
				kgo.HeartbeatInterval(100*time.Millisecond),
				kgo.FetchMaxWait(250*time.Millisecond),
			)
			if err != nil {
				t.Fatal(err)
			}
			cl.PollFetches(ctx)
			// Commit current offsets
			if err := cl.CommitUncommittedOffsets(ctx); err != nil {
				t.Fatal(err)
			}
			cl.Close()
		}

		c.Close() // clean shutdown - saveGroupsLog truncates+rewrites
	}

	// Phase 2: reopen, verify all 3 groups' offsets survived.
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		adm := kadm.NewClient(newPlainClient(t, c))
		for i := range 3 {
			group := fmt.Sprintf("trunc-group-%d", i)
			offsets, err := adm.FetchOffsets(context.Background(), group)
			if err != nil {
				t.Fatalf("group %s: %v", group, err)
			}
			o, ok := offsets.Lookup(topic, 0)
			if !ok {
				t.Fatalf("group %s: expected committed offset", group)
			}
			if o.At <= 0 {
				t.Fatalf("group %s: expected positive offset, got %d", group, o.At)
			}
		}
	}
}

// readGroupsLogEntries reads the binary-framed entries from groups.log.
// Format per entry: [4 length LE][4 crc LE][2 version LE][json_data].
func readGroupsLogEntries(t *testing.T, path string) []map[string]any {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var entries []map[string]any
	pos := 0
	for pos+10 <= len(raw) { // 4+4+2 = 10 byte header
		length := binary.LittleEndian.Uint32(raw[pos : pos+4])
		if length < 2 {
			break
		}
		end := pos + 4 + 4 + int(length)
		if end > len(raw) {
			break
		}
		jsonData := raw[pos+10 : end]
		var entry map[string]any
		if err := json.Unmarshal(jsonData, &entry); err != nil {
			pos = end
			continue
		}
		entries = append(entries, entry)
		pos = end
	}
	return entries
}

// maxGenerationInGroupsLog returns the highest generation from "meta" entries.
func maxGenerationInGroupsLog(t *testing.T, path string) int32 {
	t.Helper()
	var maxGen int32
	for _, entry := range readGroupsLogEntries(t, path) {
		if entry["type"] == "meta" {
			if gen, ok := entry["gen"].(float64); ok && int32(gen) > maxGen {
				maxGen = int32(gen)
			}
		}
	}
	return maxGen
}

// TestPersistFullReplayInFlightTxn verifies that after a crash with an
// uncommitted transaction, full replay implicitly aborts the in-flight
// txn, advances the LSO, and fences the producer by bumping its epoch.
func TestPersistFullReplayInFlightTxn(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	const topic = "inflight-txn-topic"

	// Phase 1: produce committed records (txn1), then start a second
	// transaction and produce records but DON'T commit - simulate crash.
	{
		c, err := NewCluster(
			DataDir(dir),
			SyncWrites(),
			NumBrokers(1),
			SeedTopics(1, topic),
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		cl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.TransactionalID("txn-inflight-test"),
			kgo.TransactionTimeout(30*time.Second),
			kgo.RecordPartitioner(kgo.ManualPartitioner()),
		)
		if err != nil {
			t.Fatal(err)
		}

		// Transaction 1: produce 3 records and COMMIT.
		if err := cl.BeginTransaction(); err != nil {
			t.Fatal(err)
		}
		for i := range 3 {
			r := &kgo.Record{Topic: topic, Partition: 0, Value: fmt.Appendf(nil, "committed-%d", i)}
			if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
		if err := cl.EndTransaction(ctx, kgo.TryCommit); err != nil {
			t.Fatal(err)
		}

		// Transaction 2: produce 2 records but DON'T commit.
		if err := cl.BeginTransaction(); err != nil {
			t.Fatal(err)
		}
		for i := range 2 {
			r := &kgo.Record{Topic: topic, Partition: 0, Value: fmt.Appendf(nil, "inflight-%d", i)}
			if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}

		// Simulate crash: don't call EndTransaction or cl.Close or c.Close.
		// SyncWrites ensures all data+PID entries are fsynced.
	}

	// Delete the partition snapshot to force full replay.
	snapPath := filepath.Join(dir, "partitions", "inflight-txn-topic-0", "snapshot.json")
	os.Remove(snapPath) // may not exist since we didn't close cleanly

	// Phase 2: reopen (full replay), verify in-flight txn is implicitly aborted.
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		// Verify LSO has advanced past all records (in-flight was aborted).
		pi := c.PartitionInfo(topic, 0)
		if pi == nil {
			t.Fatal("partition info not found")
		}
		if pi.LastStableOffset != pi.HighWatermark {
			t.Fatalf("LSO should equal HWM after implicit abort: LSO=%d HWM=%d", pi.LastStableOffset, pi.HighWatermark)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		plainCl := newPlainClient(t, c)

		// Verify PID epoch was bumped by checking InitProducerID.
		// Pre-crash the epoch was 1 (init=0, EndTxn bumped to 1 via
		// KIP-890). The crash-abort bump adds 1, and InitProducerID
		// recovery adds 1 more, so we expect epoch >= 3.
		initReq := kmsg.NewInitProducerIDRequest()
		txnID := "txn-inflight-test"
		initReq.TransactionalID = &txnID
		initReq.TransactionTimeoutMillis = 30000
		initReq.ProducerID = -1
		initReq.ProducerEpoch = -1
		initResp, err := initReq.RequestWith(ctx, plainCl)
		if err != nil {
			t.Fatal(err)
		}
		if initResp.ErrorCode != 0 {
			t.Fatalf("InitProducerID error code %d", initResp.ErrorCode)
		}
		if initResp.ProducerEpoch < 3 {
			t.Fatalf("expected PID epoch >= 3 after crash-abort bump, got %d", initResp.ProducerEpoch)
		}

		// read_committed consumer should see only the 3 committed records.
		committedCl := newPlainClient(t, c,
			kgo.FetchIsolationLevel(kgo.ReadCommitted()),
			kgo.FetchMaxWait(250*time.Millisecond),
			kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
				topic: {0: kgo.NewOffset().AtStart()},
			}),
		)

		committed := collectRecords(t, committedCl, 3, 3*time.Second)
		if len(committed) != 3 {
			var vals []string
			for _, r := range committed {
				vals = append(vals, string(r.Value))
			}
			t.Fatalf("read_committed: expected 3 records, got %d: %v", len(committed), vals)
		}
		for _, r := range committed {
			v := string(r.Value)
			if len(v) >= 9 && v[:9] == "inflight-" {
				t.Fatalf("read_committed consumer received in-flight record: %s", v)
			}
		}

		// read_uncommitted consumer should see all records (committed
		// + implicitly aborted). This verifies the abort logic didn't
		// delete batches - it only marked them as aborted.
		uncommittedCl := newPlainClient(t, c,
			kgo.FetchIsolationLevel(kgo.ReadUncommitted()),
			kgo.FetchMaxWait(250*time.Millisecond),
			kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
				topic: {0: kgo.NewOffset().AtStart()},
			}),
		)

		all := collectRecords(t, uncommittedCl, 5, 3*time.Second)
		// 3 committed + 2 in-flight = 5 data records. Control batches
		// (commit + implicit abort) are not returned to consumers.
		if len(all) < 5 {
			var vals []string
			for _, r := range all {
				vals = append(vals, string(r.Value))
			}
			t.Fatalf("read_uncommitted: expected >= 5 records, got %d: %v", len(all), vals)
		}
	}
}

// TestPersistCleanRestartInProgressTxn verifies that in-progress transactions
// survive a clean shutdown and restart. Records produced in the in-progress
// transaction should remain invisible to read_committed consumers until the
// transaction is committed after restart.
func TestPersistCleanRestartInProgressTxn(t *testing.T) {
	t.Parallel()
	mfs := newTestMemFS()

	const topic = "clean-restart-txn"

	var savedPID int64
	var savedEpoch int16

	// Phase 1: produce committed records, then produce in-progress
	// records and do a CLEAN shutdown (c.Close()).
	{
		c, err := NewCluster(
			mfs.opt(),
			NumBrokers(1),
			SeedTopics(1, topic),
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		cl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.TransactionalID("txn-clean-restart"),
			kgo.TransactionTimeout(30*time.Second),
			kgo.RecordPartitioner(kgo.ManualPartitioner()),
		)
		if err != nil {
			t.Fatal(err)
		}

		// Transaction 1: produce 3 records and COMMIT.
		if err := cl.BeginTransaction(); err != nil {
			t.Fatal(err)
		}
		for i := range 3 {
			r := &kgo.Record{Topic: topic, Partition: 0, Value: fmt.Appendf(nil, "committed-%d", i)}
			if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
		if err := cl.EndTransaction(ctx, kgo.TryCommit); err != nil {
			t.Fatal(err)
		}

		// Transaction 2: produce 2 records but DON'T commit.
		if err := cl.BeginTransaction(); err != nil {
			t.Fatal(err)
		}
		for i := range 2 {
			r := &kgo.Record{Topic: topic, Partition: 0, Value: fmt.Appendf(nil, "inflight-%d", i)}
			if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}

		// Save PID/epoch for manual EndTxn after restart.
		initReq := kmsg.NewInitProducerIDRequest()
		txnID := "txn-clean-restart"
		initReq.TransactionalID = &txnID
		initReq.TransactionTimeoutMillis = 30000
		initReq.ProducerID = -1
		initReq.ProducerEpoch = -1
		plainCl := newPlainClient(t, c)
		initResp, err := initReq.RequestWith(ctx, plainCl)
		if err != nil {
			t.Fatal(err)
		}
		savedPID = initResp.ProducerID
		savedEpoch = initResp.ProducerEpoch
		plainCl.Close()

		cl.Close()
		c.Close()
	}

	// Phase 2: reopen with same memFS, verify in-progress txn is preserved.
	{
		c, err := NewCluster(
			mfs.opt(),
			NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		// LSO should be behind HWM (in-progress transaction).
		pi := c.PartitionInfo(topic, 0)
		if pi == nil {
			t.Fatal("partition info not found")
		}
		if pi.LastStableOffset >= pi.HighWatermark {
			t.Fatalf("LSO should be behind HWM for in-progress txn: LSO=%d HWM=%d",
				pi.LastStableOffset, pi.HighWatermark)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// read_committed should see only the 3 committed records.
		committedCl := newPlainClient(t, c,
			kgo.FetchIsolationLevel(kgo.ReadCommitted()),
			kgo.FetchMaxWait(250*time.Millisecond),
			kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
				topic: {0: kgo.NewOffset().AtStart()},
			}),
		)

		committed := collectRecords(t, committedCl, 3, 3*time.Second)
		if len(committed) != 3 {
			t.Fatalf("read_committed before EndTxn: expected 3 records, got %d", len(committed))
		}

		// Commit the in-progress transaction using a raw EndTxn
		// with the saved PID/epoch.
		plainCl := newPlainClient(t, c)

		endReq := kmsg.NewEndTxnRequest()
		endReq.TransactionalID = "txn-clean-restart"
		endReq.ProducerID = savedPID
		endReq.ProducerEpoch = savedEpoch
		endReq.Commit = true
		endResp, err := endReq.RequestWith(ctx, plainCl)
		if err != nil {
			t.Fatal(err)
		}
		if endResp.ErrorCode != 0 {
			t.Fatalf("EndTxn error code %d (%s)", endResp.ErrorCode, kerr.ErrorForCode(endResp.ErrorCode))
		}
		plainCl.Close()

		// After commit, LSO should equal HWM.
		pi = c.PartitionInfo(topic, 0)
		if pi.LastStableOffset != pi.HighWatermark {
			t.Fatalf("after EndTxn commit: LSO=%d HWM=%d (should be equal)",
				pi.LastStableOffset, pi.HighWatermark)
		}

		// read_committed should now see all 5 records.
		committedCl2 := newPlainClient(t, c,
			kgo.FetchIsolationLevel(kgo.ReadCommitted()),
			kgo.FetchMaxWait(250*time.Millisecond),
			kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
				topic: {0: kgo.NewOffset().AtStart()},
			}),
		)

		all := collectRecords(t, committedCl2, 5, 3*time.Second)
		if len(all) != 5 {
			var vals []string
			for _, r := range all {
				vals = append(vals, string(r.Value))
			}
			t.Fatalf("read_committed after EndTxn: expected 5 records, got %d: %v", len(all), vals)
		}
	}
}

// TestPersistTxnAutoAbortExpiredOnRestart verifies that an in-progress
// transaction whose timeout has elapsed is auto-aborted on restart rather
// than being restored.
func TestPersistTxnAutoAbortExpiredOnRestart(t *testing.T) {
	t.Parallel()
	mfs := newTestMemFS()

	const topic = "txn-auto-abort"

	// Phase 1: produce in a transaction, close without committing.
	{
		c, err := NewCluster(
			mfs.opt(),
			NumBrokers(1),
			SeedTopics(1, topic),
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		cl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.TransactionalID("txn-expire"),
			// Short timeout so it's expired by the time we restart.
			kgo.TransactionTimeout(200*time.Millisecond),
			kgo.RecordPartitioner(kgo.ManualPartitioner()),
		)
		if err != nil {
			t.Fatal(err)
		}
		if err := cl.BeginTransaction(); err != nil {
			t.Fatal(err)
		}
		for i := range 3 {
			r := &kgo.Record{Topic: topic, Partition: 0, Value: fmt.Appendf(nil, "expire-%d", i)}
			if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}

		cl.Close()
		c.Close()
	}

	// Sleep to guarantee the 200ms timeout has elapsed.
	time.Sleep(300 * time.Millisecond)

	// Phase 2: reopen — the expired txn should be auto-aborted.
	{
		c, err := NewCluster(
			mfs.opt(),
			NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		// LSO should equal HWM — the txn was aborted, not restored.
		pi := c.PartitionInfo(topic, 0)
		if pi == nil {
			t.Fatal("partition info not found")
		}
		if pi.LastStableOffset != pi.HighWatermark {
			t.Fatalf("expired txn should be auto-aborted: LSO=%d HWM=%d (should be equal)",
				pi.LastStableOffset, pi.HighWatermark)
		}

		// read_committed should see 0 records (all aborted).
		cl := newPlainClient(t, c,
			kgo.FetchIsolationLevel(kgo.ReadCommitted()),
			kgo.FetchMaxWait(200*time.Millisecond),
			kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
				topic: {0: kgo.NewOffset().AtStart()},
			}),
		)

		var count int
		verifyCtx, verifyCancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
		defer verifyCancel()
		for verifyCtx.Err() == nil {
			fetches := cl.PollFetches(verifyCtx)
			fetches.EachRecord(func(_ *kgo.Record) { count++ })
		}
		if count != 0 {
			t.Fatalf("expected 0 committed records after auto-abort, got %d", count)
		}
	}
}

// TestPersistGroupPhantomMemberExpiry verifies that group members whose
// sessions have expired across repeated restarts are correctly evicted.
// This is the core fix for the phantom member bug: without persisting
// LastHeartbeat, each restart would reset the member's timeout to "just
// now", preventing eviction indefinitely.
//
// The three-phase design is critical. A single close→sleep→restart would
// pass even without the fix because enough wall time elapses. Phase 2's
// intermediate restart advances shutdownAt: without the fix, phase 3
// falls back to shutdownAt (recent), thinks the member is alive, and
// the test correctly fails. With the fix, phase 3 uses the real
// LastHeartbeat from phase 1 (old), and the member is properly expired.
func TestPersistGroupPhantomMemberExpiry(t *testing.T) {
	t.Parallel()
	mfs := newTestMemFS()

	const topic = "phantom-topic"
	const group = "phantom-group"

	// Phase 1: create a consumer group, then close the server FIRST so
	// the client's LeaveGroup fails — simulating a restart.
	{
		c, err := NewCluster(
			mfs.opt(),
			NumBrokers(1),
			SeedTopics(1, topic),
			GroupMinSessionTimeout(500*time.Millisecond),
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Produce a record so the consumer has something to fetch.
		prodCl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
		if err != nil {
			t.Fatal(err)
		}
		if err := prodCl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("v")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
		prodCl.Close()

		consCl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup(group),
			kgo.HeartbeatInterval(50*time.Millisecond),
			kgo.SessionTimeout(1*time.Second),
			kgo.FetchMaxWait(50*time.Millisecond),
			kgo.RetryTimeout(50*time.Millisecond),
		)
		if err != nil {
			t.Fatal(err)
		}
		// Wait for group to stabilize.
		for {
			fetches := consCl.PollFetches(ctx)
			if fetches.NumRecords() > 0 {
				break
			}
		}

		// Close server first — client's LeaveGroup will fail,
		// leaving a phantom member in the saved session state.
		c.Close()
		consCl.Close()
	}

	// Sleep 500ms before phase 2. This widens the gap between
	// phase 1's real LastHeartbeat and phase 2's shutdownAt. Without
	// this, the gap is only ~4ms (memFS is fast), leaving no room
	// to distinguish the two in phase 3.
	time.Sleep(500 * time.Millisecond)

	// Phase 2: restart, verify phantom is alive (500ms < 1s), close.
	// This advances shutdownAt to ~500ms after the real LastHeartbeat.
	{
		c, err := NewCluster(
			mfs.opt(),
			NumBrokers(1),
			GroupMinSessionTimeout(500*time.Millisecond),
		)
		if err != nil {
			t.Fatal(err)
		}

		adm := kadm.NewClient(newPlainClient(t, c))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		described, err := adm.DescribeGroups(ctx, group)
		if err != nil {
			t.Fatal(err)
		}
		dg := described[group]
		if dg.Err != nil {
			t.Fatalf("describe group: %v", dg.Err)
		}
		if dg.State != "Stable" {
			t.Fatalf("phase 2: expected Stable, got %s", dg.State)
		}
		if len(dg.Members) != 1 {
			t.Fatalf("phase 2: expected 1 phantom member, got %d", len(dg.Members))
		}
		c.Close()
	}

	// Sleep 600ms. Total from real LastHeartbeat: ~1100ms (> 1s timeout).
	// Total from phase 2's shutdownAt: ~600ms (< 1s timeout).
	// Without the fix (fallback to shutdownAt), the member appears
	// alive at restore. With the fix, it's correctly expired.
	time.Sleep(600 * time.Millisecond)

	// Phase 3: restart again. With the fix, LastHeartbeat is from
	// phase 1 (~1.1s ago) → expired. Without the fix, fallback to
	// shutdownAt from phase 2 (~600ms ago) → alive (bug).
	{
		c, err := NewCluster(
			mfs.opt(),
			NumBrokers(1),
			GroupMinSessionTimeout(500*time.Millisecond),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		adm := kadm.NewClient(newPlainClient(t, c))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		described, err := adm.DescribeGroups(ctx, group)
		if err != nil {
			t.Fatal(err)
		}
		dg := described[group]
		if dg.Err != nil {
			t.Fatalf("describe group: %v", dg.Err)
		}
		if dg.State != "Empty" {
			t.Fatalf("phase 3: expected Empty (phantom expired), got %s", dg.State)
		}
		if len(dg.Members) != 0 {
			t.Fatalf("phase 3: expected 0 members (phantom expired), got %d", len(dg.Members))
		}
	}
}

// TestPersistGroupPhantomMemberExpiry848 is the KIP-848 counterpart to
// TestPersistGroupPhantomMemberExpiry. It verifies that 848 consumer
// group members whose sessions have expired across repeated restarts
// are correctly evicted via persisted LastHeartbeat.
//
// Same three-phase design: phase 2's intermediate restart advances
// shutdownAt so that phase 3 can distinguish persisted LastHeartbeat
// (old, from phase 1) from the shutdownAt fallback (recent, from
// phase 2).
func TestPersistGroupPhantomMemberExpiry848(t *testing.T) {
	t.Parallel()
	mfs := newTestMemFS()

	const topic = "phantom-848-topic"
	const group = "phantom-848-group"

	//nolint:revive,staticcheck
	ctx848 := context.WithValue(context.Background(), "opt_in_kafka_next_gen_balancer_beta", true)

	brokerCfgs := BrokerConfigs(map[string]string{
		"group.consumer.session.timeout.ms":    "1000",
		"group.consumer.heartbeat.interval.ms": "100",
	})

	// describe848 uses ConsumerGroupDescribe (key 69) which reports
	// 848 consumer members, unlike DescribeGroups (key 15) which
	// only reports classic members.
	describe848 := func(t *testing.T, c *Cluster) (string, int) {
		t.Helper()
		cl := newPlainClient(t, c)
		req := kmsg.NewPtrConsumerGroupDescribeRequest()
		req.Groups = []string{group}
		resp, err := req.RequestWith(context.Background(), cl)
		if err != nil {
			t.Fatal(err)
		}
		if len(resp.Groups) != 1 {
			t.Fatalf("expected 1 group in response, got %d", len(resp.Groups))
		}
		g := resp.Groups[0]
		if err := kerr.ErrorForCode(g.ErrorCode); err != nil {
			// GroupIDNotFound means group doesn't exist (= Empty).
			if err == kerr.GroupIDNotFound {
				return "Empty", 0
			}
			t.Fatalf("describe group: %v", err)
		}
		return g.State, len(g.Members)
	}

	// Phase 1: create an 848 consumer group, then close the server
	// FIRST so the client's leave heartbeat fails.
	{
		c, err := NewCluster(
			mfs.opt(),
			NumBrokers(1),
			SeedTopics(1, topic),
			brokerCfgs,
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(ctx848, 5*time.Second)
		defer cancel()

		prodCl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
		if err != nil {
			t.Fatal(err)
		}
		if err := prodCl.ProduceSync(ctx, &kgo.Record{Topic: topic, Value: []byte("v")}).FirstErr(); err != nil {
			t.Fatal(err)
		}
		prodCl.Close()

		consCl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.WithContext(ctx848),
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup(group),
			kgo.FetchMaxWait(50*time.Millisecond),
			kgo.RetryTimeout(50*time.Millisecond),
		)
		if err != nil {
			t.Fatal(err)
		}
		for {
			fetches := consCl.PollFetches(ctx)
			if fetches.NumRecords() > 0 {
				break
			}
		}

		c.Close()
		consCl.Close()
	}

	// Sleep 500ms to widen the gap between phase 1's real
	// LastHeartbeat and phase 2's shutdownAt.
	time.Sleep(500 * time.Millisecond)

	// Phase 2: restart, verify phantom is alive (500ms < 1s), close.
	{
		c, err := NewCluster(
			mfs.opt(),
			NumBrokers(1),
			brokerCfgs,
		)
		if err != nil {
			t.Fatal(err)
		}

		state, members := describe848(t, c)
		if state != "Stable" {
			t.Fatalf("phase 2: expected Stable, got %s", state)
		}
		if members != 1 {
			t.Fatalf("phase 2: expected 1 phantom member, got %d", members)
		}
		c.Close()
	}

	// Sleep 600ms. Total from real LastHeartbeat: ~1100ms (> 1s).
	// Total from phase 2's shutdownAt: ~600ms (< 1s).
	time.Sleep(600 * time.Millisecond)

	// Phase 3: restart. With the fix, LastHeartbeat from phase 1
	// (~1.1s ago) → expired. Without the fix, fallback to shutdownAt
	// from phase 2 (~600ms ago) → alive (bug).
	{
		c, err := NewCluster(
			mfs.opt(),
			NumBrokers(1),
			brokerCfgs,
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		state, members := describe848(t, c)
		if state != "Empty" {
			t.Fatalf("phase 3: expected Empty (phantom expired), got %s", state)
		}
		if members != 0 {
			t.Fatalf("phase 3: expected 0 members (phantom expired), got %d", members)
		}
	}
}

// TestPersistSnapshotNbytesRetention verifies that pd.nbytes is correctly
// accumulated on snapshot-based restart and that retention.bytes works
// after restart.
func TestPersistSnapshotNbytesRetention(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	const topic = "nbytes-topic"

	// Phase 1: produce records, clean shutdown (creates snapshot).
	var savedNbytes int64
	{
		c, err := NewCluster(
			DataDir(dir),
			SyncWrites(),
			NumBrokers(1),
			SeedTopics(1, topic),
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
		if err != nil {
			t.Fatal(err)
		}

		for i := range 50 {
			r := &kgo.Record{
				Topic: topic,
				Value: fmt.Appendf(nil, "value-%04d-padding-to-increase-record-size", i),
			}
			if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
		cl.Close()

		pi := c.PartitionInfo(topic, 0)
		if pi == nil {
			t.Fatal("partition info not found")
		}
		savedNbytes = pi.NumBytes
		if savedNbytes <= 0 {
			t.Fatalf("expected positive nbytes before shutdown, got %d", savedNbytes)
		}
		c.Close()
	}

	// Phase 2: reopen from snapshot, verify exact nbytes match.
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		pi := c.PartitionInfo(topic, 0)
		if pi == nil {
			t.Fatal("partition info not found after reopen")
		}
		if pi.NumBytes != savedNbytes {
			t.Fatalf("nbytes mismatch after snapshot reopen: got %d, want %d", pi.NumBytes, savedNbytes)
		}

		// Verify retention actually works after restart by setting a
		// tight retention.bytes via topic config and producing a record
		// to trigger retention.
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		adm := kadm.NewClient(newPlainClient(t, c))
		// Set retention.bytes to half the current size - should trim old records.
		retBytes := fmt.Sprintf("%d", savedNbytes/2)
		resp, err := adm.AlterTopicConfigs(ctx, []kadm.AlterConfig{
			{Name: "retention.bytes", Value: &retBytes},
		}, topic)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range resp {
			if r.Err != nil {
				t.Fatalf("AlterTopicConfigs error: %v", r.Err)
			}
		}

		// Produce one more record to trigger retention compaction.
		cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
		if err != nil {
			t.Fatal(err)
		}
		r := &kgo.Record{Topic: topic, Value: []byte("trigger-retention")}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			t.Fatal(err)
		}
		cl.Close()

		c.ApplyRetention()

		// After retention, logStartOffset should have advanced.
		pi2 := c.PartitionInfo(topic, 0)
		if pi2 == nil {
			t.Fatal("partition info not found after retention")
		}
		if pi2.LogStartOffset <= 0 {
			t.Fatalf("expected logStartOffset > 0 after retention, got %d", pi2.LogStartOffset)
		}
	}
}

// TestPersistSnapshotTruncatedSegment verifies that when a snapshot exists
// but a segment file is truncated (partial corruption), the HWM and LSO
// are clamped to match the actual loaded batches.
func TestPersistSnapshotTruncatedSegment(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	topic := "trunc-topic"

	// Phase 1: produce records, close cleanly (writes snapshot + segments).
	{
		c, err := NewCluster(
			DataDir(dir),
			SyncWrites(),
			NumBrokers(1),
			SeedTopics(1, topic),
		)
		if err != nil {
			t.Fatal(err)
		}
		produceN(t, c, topic, 10)

		pi := c.PartitionInfo(topic, 0)
		if pi == nil || pi.HighWatermark != 10 {
			t.Fatalf("expected HWM=10 before close, got %v", pi)
		}
		c.Close()
	}

	// Truncate the segment file to roughly half its size, but keep
	// the snapshot.json intact. This simulates a crash that lost the
	// tail of the segment file.
	partDir := filepath.Join(dir, "partitions", topic+"-0")
	entries, err := os.ReadDir(partDir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if filepath.Ext(e.Name()) != ".dat" {
			continue
		}
		path := filepath.Join(partDir, e.Name())
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		// Truncate to half the file size.
		half := len(data) / 2
		if half > 0 {
			if err := os.WriteFile(path, data[:half], 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}

	// Phase 2: reopen with snapshot + truncated segment.
	// HWM should be clamped to match the actual loaded batches.
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		pi := c.PartitionInfo(topic, 0)
		if pi == nil {
			t.Fatal("partition not found after reopen")
		}
		if pi.HighWatermark >= 10 {
			t.Fatalf("expected HWM < 10 after segment truncation, got %d", pi.HighWatermark)
		}
		if pi.HighWatermark <= 0 {
			t.Fatalf("expected HWM > 0 (some batches should survive), got %d", pi.HighWatermark)
		}

		// Verify consumers can read up to the clamped HWM without errors.
		cl := newPlainClient(t, c,
			kgo.ConsumeTopics(topic),
			kgo.FetchMaxWait(250*time.Millisecond),
		)

		collectRecords(t, cl, 1, 3*time.Second)
	}
}

// TestPersistQuotasRestart verifies that client quotas survive a restart.
func TestPersistQuotasRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Phase 1: set quotas, close.
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		cl := newPlainClient(t, c)
		req := kmsg.NewAlterClientQuotasRequest()
		entry := kmsg.NewAlterClientQuotasRequestEntry()
		comp := kmsg.NewAlterClientQuotasRequestEntryEntity()
		comp.Type = "user"
		comp.Name = kmsg.StringPtr("testuser")
		entry.Entity = append(entry.Entity, comp)
		op := kmsg.NewAlterClientQuotasRequestEntryOp()
		op.Key = "producer_byte_rate"
		op.Value = 1048576
		entry.Ops = append(entry.Ops, op)
		req.Entries = append(req.Entries, entry)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range resp.Entries {
			if e.ErrorCode != 0 {
				t.Fatalf("AlterClientQuotas error: %d", e.ErrorCode)
			}
		}
		c.Close()
	}

	// Phase 2: reopen, verify quotas persisted.
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		cl := newPlainClient(t, c)
		req := kmsg.NewDescribeClientQuotasRequest()
		comp := kmsg.NewDescribeClientQuotasRequestComponent()
		comp.EntityType = "user"
		comp.Match = kmsg.StringPtr("testuser")
		comp.MatchType = 0 // exact match
		req.Components = append(req.Components, comp)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		resp, err := req.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		if resp.ErrorCode != 0 {
			t.Fatalf("DescribeClientQuotas error: %d", resp.ErrorCode)
		}
		if len(resp.Entries) == 0 {
			t.Fatal("expected quota entries after restart, got 0")
		}
		found := false
		for _, e := range resp.Entries {
			for _, v := range e.Values {
				if v.Key == "producer_byte_rate" && v.Value == 1048576 {
					found = true
				}
			}
		}
		if !found {
			t.Fatal("expected producer_byte_rate=1048576 after restart")
		}
	}
}

// TestPersistTopicDeletionRestart verifies that deleting a topic removes
// its data from the snapshot and cleans up partition directories on disk,
// so the topic does not reappear after restart.
func TestPersistTopicDeletionRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	const (
		keepTopic   = "keep-topic"
		deleteTopic = "delete-topic"
	)

	// Phase 1: create two topics, produce data, delete one, close.
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
			SeedTopics(1, keepTopic, deleteTopic),
		)
		if err != nil {
			t.Fatal(err)
		}
		cl := newPlainClient(t, c)
		adm := kadm.NewClient(cl)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Produce to both topics.
		for _, topic := range []string{keepTopic, deleteTopic} {
			for i := range 5 {
				r := &kgo.Record{Topic: topic, Value: fmt.Appendf(nil, "v%d", i)}
				if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
					t.Fatal(err)
				}
			}
		}

		// Delete one topic.
		resps, err := adm.DeleteTopics(ctx, deleteTopic)
		if err != nil {
			t.Fatal(err)
		}
		for _, r := range resps {
			if r.Err != nil {
				t.Fatalf("delete %s: %v", r.Topic, r.Err)
			}
		}

		c.Close()
	}

	// Verify partition directory for deleted topic is gone.
	deletedDir := filepath.Join(dir, "partitions", deleteTopic+"-0")
	if _, err := os.Stat(deletedDir); !os.IsNotExist(err) {
		t.Fatalf("expected deleted topic partition dir to be removed, got err=%v", err)
	}

	// Phase 2: reopen and verify.
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		adm := kadm.NewClient(newPlainClient(t, c))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		tl, err := adm.ListTopics(ctx)
		if err != nil {
			t.Fatal(err)
		}
		if _, ok := tl[deleteTopic]; ok {
			t.Fatalf("deleted topic %s should not exist after restart", deleteTopic)
		}
		if _, ok := tl[keepTopic]; !ok {
			t.Fatal("kept topic should still exist after restart")
		}

		// Verify the kept topic still has data.
		cl := newPlainClient(t, c,
			kgo.ConsumeTopics(keepTopic),
			kgo.FetchMaxWait(250*time.Millisecond),
		)

		records := collectRecords(t, cl, 5, 3*time.Second)
		if len(records) != 5 {
			t.Fatalf("expected 5 records in kept topic, got %d", len(records))
		}
	}
}

func TestPersistSessionStateClassicGroup(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	const topic = "session-classic-topic"
	const group = "session-classic-group"

	// Phase 1: produce, create a classic consumer group with one member, close
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
			SeedTopics(1, topic),
		)
		if err != nil {
			t.Fatal(err)
		}

		// Produce records
		produceN(t, c, topic, 10)

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		// Create consumer group, consume some records, commit
		consCl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup(group),
			kgo.HeartbeatInterval(100*time.Millisecond),
			kgo.FetchMaxWait(250*time.Millisecond),
			kgo.SessionTimeout(45*time.Second),
		)
		if err != nil {
			t.Fatal(err)
		}
		var consumed int
		for consumed < 5 {
			fetches := consCl.PollFetches(ctx)
			fetches.EachRecord(func(_ *kgo.Record) { consumed++ })
		}
		if err := consCl.CommitUncommittedOffsets(ctx); err != nil {
			t.Fatal(err)
		}
		// Close cluster first - simulates server restart while client
		// is still connected. The client's LeaveGroup will fail (conn
		// closed), preserving members in the group.
		c.Close()
		consCl.Close()
	}

	// Verify session_state.json was written
	ssPath := filepath.Join(dir, "session_state.json")
	if _, err := os.Stat(ssPath); err != nil {
		t.Fatalf("session_state.json should exist after clean shutdown: %v", err)
	}

	// Phase 2: reopen - session state should be loaded and file deleted
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		// session_state.json should be deleted after load
		if _, err := os.Stat(ssPath); !os.IsNotExist(err) {
			t.Fatal("session_state.json should be deleted after load")
		}

		// Describe the classic group - should have members in Stable state
		adm := kadm.NewClient(newPlainClient(t, c))
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		described, err := adm.DescribeGroups(ctx, group)
		if err != nil {
			t.Fatalf("describe groups: %v", err)
		}
		dg, ok := described[group]
		if !ok {
			t.Fatal("group not found in describe response")
		}
		if dg.Err != nil {
			t.Fatalf("describe group error: %v", dg.Err)
		}
		if dg.State != "Stable" {
			t.Fatalf("expected Stable state, got %s", dg.State)
		}
		if len(dg.Members) == 0 {
			t.Fatal("expected at least one member in restored group")
		}
	}
}

// TestPersistSeqWindowDedup verifies that sequence window deduplication
// works correctly across a clean restart. A produce retry after restart
// should return the original offset, not write a duplicate batch.
func TestPersistSeqWindowDedup(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	const topic = "dedup-topic"

	buildProduce := func(pid int64, epoch int16, seq int32) *kmsg.ProduceRequest {
		rec := kmsg.Record{Key: fmt.Appendf(nil, "k-%d", seq), Value: fmt.Appendf(nil, "v-%d", seq)}
		rec.Length = int32(len(rec.AppendTo(nil)) - 1)
		now := time.Now().UnixMilli()
		batch := kmsg.RecordBatch{
			PartitionLeaderEpoch: -1,
			Magic:                2,
			LastOffsetDelta:      0,
			FirstTimestamp:       now,
			MaxTimestamp:         now,
			ProducerID:           pid,
			ProducerEpoch:        epoch,
			FirstSequence:        seq,
			NumRecords:           1,
			Records:              rec.AppendTo(nil),
		}
		raw := batch.AppendTo(nil)
		batch.Length = int32(len(raw) - 12)
		raw = batch.AppendTo(nil)
		batch.CRC = int32(crc32.Checksum(raw[21:], crc32.MakeTable(crc32.Castagnoli)))

		req := kmsg.NewProduceRequest()
		req.Version = 11
		req.Acks = -1
		req.TimeoutMillis = 5000
		rt := kmsg.NewProduceRequestTopic()
		rt.Topic = topic
		rp := kmsg.NewProduceRequestTopicPartition()
		rp.Partition = 0
		rp.Records = batch.AppendTo(nil)
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)
		return &req
	}

	var pid int64
	var epoch int16
	var origOffset int64

	// Phase 1: init PID, produce 3 batches, close cleanly
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
			SeedTopics(1, topic),
		)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		v := kversion.Stable()
		v.SetMaxKeyVersion(0, 11) // produce: cap at v11 (topic names, not IDs)
		cl := newPlainClient(t, c, kgo.MaxVersions(v))

		// InitProducerID (idempotent, no txid)
		initReq := kmsg.NewInitProducerIDRequest()
		initReq.ProducerID = -1
		initReq.ProducerEpoch = -1
		initResp, err := initReq.RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		if initResp.ErrorCode != 0 {
			t.Fatalf("InitProducerID: error code %d", initResp.ErrorCode)
		}
		pid = initResp.ProducerID
		epoch = initResp.ProducerEpoch

		// Produce 3 batches: seq=0, seq=1, seq=2
		for seq := int32(0); seq < 3; seq++ {
			resp, err := buildProduce(pid, epoch, seq).RequestWith(ctx, cl)
			if err != nil {
				t.Fatal(err)
			}
			if ec := resp.Topics[0].Partitions[0].ErrorCode; ec != 0 {
				t.Fatalf("produce seq=%d: error %v", seq, kerr.ErrorForCode(ec))
			}
			if seq == 2 {
				origOffset = resp.Topics[0].Partitions[0].BaseOffset
			}
		}

		cl.Close()
		c.Close()
	}

	// Phase 2: reopen, retry the last produce - should be a dup
	{
		c, err := NewCluster(
			DataDir(dir),
			NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		v := kversion.Stable()
		v.SetMaxKeyVersion(0, 11)
		cl := newPlainClient(t, c, kgo.MaxVersions(v))

		// Retry the last produce (seq=2) - should be deduplicated
		resp, err := buildProduce(pid, epoch, 2).RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		p := resp.Topics[0].Partitions[0]
		if p.ErrorCode != 0 {
			t.Fatalf("dup produce: error %v", kerr.ErrorForCode(p.ErrorCode))
		}
		if p.BaseOffset != origOffset {
			t.Fatalf("dup produce: expected offset %d (original), got %d (duplicate written!)", origOffset, p.BaseOffset)
		}

		// Produce the NEXT batch (seq=3) - should succeed as new
		resp2, err := buildProduce(pid, epoch, 3).RequestWith(ctx, cl)
		if err != nil {
			t.Fatal(err)
		}
		p2 := resp2.Topics[0].Partitions[0]
		if p2.ErrorCode != 0 {
			t.Fatalf("new produce: error %v", kerr.ErrorForCode(p2.ErrorCode))
		}
		if p2.BaseOffset <= origOffset {
			t.Fatalf("new produce: expected offset > %d, got %d", origOffset, p2.BaseOffset)
		}

		// Verify record count: should have exactly 4 records (3 original + 1 new, no dups)
		consumer := newPlainClient(t, c,
			kgo.FetchMaxWait(250*time.Millisecond),
			kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
				topic: {0: kgo.NewOffset().AtStart()},
			}),
		)

		records := collectRecords(t, consumer, 4, 3*time.Second)
		if len(records) != 4 {
			var offsets []int64
			for _, r := range records {
				offsets = append(offsets, r.Offset)
			}
			t.Fatalf("expected exactly 4 records (no dups), got %d; offsets=%v", len(records), offsets)
		}
	}
}

// TestPersistLoadedGroupNotKilledByOffsetCommit ensures that a group loaded
// from disk is not deleted when the first post-restart OffsetCommit fails
// validation (e.g., generation mismatch on an empty group). This was the root
// cause of GROUP_ID_NOT_FOUND after restart.
func TestPersistLoadedGroupNotKilledByOffsetCommit(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	const (
		topic = "t"
		group = "g"
	)

	// Phase 1: create cluster, join group, commit offsets, close.
	{
		c, err := NewCluster(
			NumBrokers(1),
			DataDir(dir),
			SeedTopics(-1, topic),
		)
		if err != nil {
			t.Fatal(err)
		}
		cl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.ConsumerGroup(group),
			kgo.ConsumeTopics(topic),
		)
		if err != nil {
			t.Fatal(err)
		}
		// Poll once to trigger JoinGroup/SyncGroup.
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		cl.PollRecords(ctx, 1)
		cancel()

		// Commit an offset so the group has state in groups.json.
		if err := cl.CommitUncommittedOffsets(context.Background()); err != nil {
			t.Fatal(err)
		}
		cl.Close()
		c.Close()
	}

	// Phase 2: reopen cluster. The group is loaded from groups.json with
	// metadata and committed offsets, but no live members (session state
	// only saves Stable groups with members - none survive since the client
	// was closed). Send a raw OffsetCommit with a stale generation - this
	// should fail with IllegalGeneration but NOT kill the group.
	{
		c, err := NewCluster(
			NumBrokers(1),
			DataDir(dir),
			Ports(0),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		cl := newPlainClient(t, c)

		// Send a raw OffsetCommit with generation=1 to an empty group.
		// This exercises the code path that previously triggered
		// firstJoin(false) and killed the group.
		req := kmsg.NewPtrOffsetCommitRequest()
		req.Group = group
		req.Generation = 1
		req.MemberID = "fake-member"
		rt := kmsg.NewOffsetCommitRequestTopic()
		rt.Topic = topic
		rp := kmsg.NewOffsetCommitRequestTopicPartition()
		rp.Partition = 0
		rp.Offset = 0
		rt.Partitions = append(rt.Partitions, rp)
		req.Topics = append(req.Topics, rt)

		resp, err := req.RequestWith(context.Background(), cl)
		if err != nil {
			t.Fatal(err)
		}
		// Should get an error (IllegalGeneration or UnknownMemberID) but
		// NOT crash the group.
		if len(resp.Topics) == 0 || len(resp.Topics[0].Partitions) == 0 {
			t.Fatal("expected partition response")
		}
		errCode := resp.Topics[0].Partitions[0].ErrorCode
		if errCode == 0 {
			t.Fatal("expected error code for stale commit, got 0")
		}
		// Verify the group is still alive by sending a Heartbeat.
		// Heartbeat goes through handleHijack - if the group was
		// killed, handleHijack returns false and the response is
		// GROUP_ID_NOT_FOUND.
		hbReq := kmsg.NewPtrHeartbeatRequest()
		hbReq.Group = group
		hbReq.Generation = 1
		hbReq.MemberID = "fake-member"

		hbResp, err := hbReq.RequestWith(context.Background(), cl)
		if err != nil {
			t.Fatal(err)
		}
		ec := kerr.ErrorForCode(hbResp.ErrorCode)
		if ec == kerr.GroupIDNotFound {
			t.Fatal("group was killed after OffsetCommit - the firstJoin bug is present")
		}
	}
}

func TestPersistLogCompaction(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	const topic = "compact-topic"
	const group = "compact-group"

	// Use a low threshold so compaction triggers quickly.
	c, err := NewCluster(
		DataDir(dir),
		NumBrokers(1),
		SeedTopics(1, topic),
		BrokerConfigs(map[string]string{
			"state.log.compact.bytes": "1024",
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	cl, err := kgo.NewClient(
		kgo.SeedBrokers(c.ListenAddrs()...),
		kgo.ConsumeTopics(topic),
		kgo.ConsumerGroup(group),
		kgo.HeartbeatInterval(100*time.Millisecond),
		kgo.FetchMaxWait(250*time.Millisecond),
	)
	if err != nil {
		t.Fatal(err)
	}

	// Produce records
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	produceN(t, c, topic, 50)

	// Consume and commit in a loop to grow groups.log past threshold.
	var totalConsumed int
	for totalConsumed < 50 {
		fetches := cl.PollFetches(ctx)
		fetches.EachRecord(func(_ *kgo.Record) { totalConsumed++ })
		if err := cl.CommitUncommittedOffsets(ctx); err != nil {
			t.Fatal(err)
		}
	}

	// Keep committing to grow the log further, triggering compaction.
	for range 100 {
		if err := cl.CommitUncommittedOffsets(ctx); err != nil {
			t.Fatal(err)
		}
	}
	cl.Close()

	// Verify groups.log was compacted (size should be small).
	groupsLogPath := filepath.Join(dir, "groups.log")
	info, err := os.Stat(groupsLogPath)
	if err != nil {
		t.Fatal(err)
	}
	// After compaction, the file should be much smaller than 100 commits
	// would produce (~100 * ~80 bytes = ~8KB raw, compacted to ~2 entries).
	if info.Size() > 1024 {
		t.Fatalf("groups.log not compacted: size %d > 1024", info.Size())
	}

	// Verify state is correct after compaction - offsets still readable.
	adm := kadm.NewClient(newPlainClient(t, c))
	offsets, err := adm.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	o, ok := offsets.Lookup(topic, 0)
	if !ok {
		t.Fatal("expected committed offset for partition 0")
	}
	if o.At != 50 {
		t.Fatalf("expected committed offset 50, got %d", o.At)
	}

	c.Close()

	// Reopen and verify state survives restart after compaction.
	c2, err := NewCluster(
		DataDir(dir),
		NumBrokers(1),
		BrokerConfigs(map[string]string{
			"state.log.compact.bytes": "1024",
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	adm2 := kadm.NewClient(newPlainClient(t, c2))
	offsets2, err := adm2.FetchOffsets(ctx, group)
	if err != nil {
		t.Fatal(err)
	}
	o2, ok := offsets2.Lookup(topic, 0)
	if !ok {
		t.Fatal("expected committed offset after restart")
	}
	if o2.At != 50 {
		t.Fatalf("expected committed offset 50 after restart, got %d", o2.At)
	}
}

// TestPersistLogCompactionCrashGroups verifies that groups.log compaction
// doesn't lose committed offsets if the process crashes right after compaction.
// It snapshots the data dir mid-operation (simulating a crash) and verifies
// that offsets survive loading from the snapshot.
func TestPersistLogCompactionCrashGroups(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	const topic = "crash-topic"

	c, err := NewCluster(
		DataDir(dir),
		NumBrokers(1),
		SeedTopics(1, topic),
		BrokerConfigs(map[string]string{
			"state.log.compact.bytes": "128", // very aggressive
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Produce records
	produceN(t, c, topic, 20)

	// Run 3 groups concurrently doing rapid commits to trigger compaction
	// while groups are actively writing to groups.log.
	const nGroups = 3
	const nCommits = 100
	var wg errgroup
	for g := range nGroups {
		group := fmt.Sprintf("crash-group-%d", g)
		wg.do(func() error {
			cl, err := kgo.NewClient(
				kgo.SeedBrokers(c.ListenAddrs()...),
				kgo.ConsumeTopics(topic),
				kgo.ConsumerGroup(group),
				kgo.HeartbeatInterval(100*time.Millisecond),
				kgo.FetchMaxWait(250*time.Millisecond),
			)
			if err != nil {
				return err
			}
			defer cl.Close()

			// Consume all records
			consumed := 0
			for consumed < 20 {
				fetches := cl.PollFetches(ctx)
				fetches.EachRecord(func(_ *kgo.Record) { consumed++ })
				if err := cl.CommitUncommittedOffsets(ctx); err != nil {
					return err
				}
			}
			// Hammer commits to grow the log and trigger compaction
			for range nCommits {
				if err := cl.CommitUncommittedOffsets(ctx); err != nil {
					return err
				}
			}
			return nil
		})
	}
	if err := wg.wait(); err != nil {
		t.Fatal(err)
	}

	// Snapshot: copy data dir to simulate crash state.
	// The groups.log on disk may have been compacted, losing any
	// entries written between collect and rename.
	crashDir := t.TempDir()
	copyDir(t, dir, crashDir)

	// Record what the live cluster thinks the offsets are.
	adm := kadm.NewClient(newPlainClient(t, c))
	liveOffsets := make(map[string]int64)
	for g := range nGroups {
		group := fmt.Sprintf("crash-group-%d", g)
		offsets, err := adm.FetchOffsets(ctx, group)
		if err != nil {
			t.Fatal(err)
		}
		offsets.Each(func(o kadm.OffsetResponse) {
			if o.Err == nil {
				liveOffsets[fmt.Sprintf("%s/%s-%d", group, o.Topic, o.Partition)] = o.At
			}
		})
	}
	c.Close()

	// Open from crash snapshot (no Close was called on original - simulates crash).
	c2, err := NewCluster(
		DataDir(crashDir),
		NumBrokers(1),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	adm2 := kadm.NewClient(newPlainClient(t, c2))
	for g := range nGroups {
		group := fmt.Sprintf("crash-group-%d", g)
		offsets, err := adm2.FetchOffsets(ctx, group)
		if err != nil {
			t.Fatal(err)
		}
		offsets.Each(func(o kadm.OffsetResponse) {
			if o.Err != nil {
				return
			}
			key := fmt.Sprintf("%s/%s-%d", group, o.Topic, o.Partition)
			live := liveOffsets[key]
			if o.At != live {
				t.Errorf("crash recovery %s: expected offset %d, got %d", key, live, o.At)
			}
		})
	}
}

// TestPersistLogCompactionCrashPIDs verifies pids.log compaction crash safety.
// This test triggers compaction and verifies PID state survives loading
// from a crash-simulated snapshot.
func TestPersistLogCompactionCrashPIDs(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	const topic = "txn-crash-topic"

	c, err := NewCluster(
		DataDir(dir),
		NumBrokers(1),
		SeedTopics(1, topic),
		BrokerConfigs(map[string]string{
			"state.log.compact.bytes": "128", // very aggressive
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	// Do many transactional produce cycles to grow pids.log
	const nTxns = 50
	for i := range nTxns {
		txid := fmt.Sprintf("txn-%d", i%5) // reuse 5 txn IDs
		cl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.TransactionalID(txid),
			kgo.TransactionTimeout(30*time.Second),
		)
		if err != nil {
			t.Fatal(err)
		}
		if err := cl.BeginTransaction(); err != nil {
			cl.Close()
			t.Fatal(err)
		}
		r := &kgo.Record{Topic: topic, Value: fmt.Appendf(nil, "txn-v%d", i)}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			cl.Close()
			t.Fatal(err)
		}
		if err := cl.EndTransaction(ctx, kgo.TryCommit); err != nil {
			cl.Close()
			t.Fatal(err)
		}
		cl.Close()
	}

	// Check pids.log size - should have been compacted
	pidsPath := filepath.Join(dir, "pids.log")
	_, err = os.Stat(pidsPath)
	if err != nil {
		t.Fatal(err)
	}
	// Snapshot for crash simulation
	crashDir := t.TempDir()
	copyDir(t, dir, crashDir)

	c.Close()

	// Open from crash snapshot
	c2, err := NewCluster(
		DataDir(crashDir),
		NumBrokers(1),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer c2.Close()

	// Verify we can still produce with the same txn IDs (PIDs survived)
	for i := range 5 {
		txid := fmt.Sprintf("txn-%d", i)
		cl, err := kgo.NewClient(
			kgo.SeedBrokers(c2.ListenAddrs()...),
			kgo.TransactionalID(txid),
			kgo.TransactionTimeout(30*time.Second),
		)
		if err != nil {
			t.Fatalf("txn %s: new client: %v", txid, err)
		}
		if err := cl.BeginTransaction(); err != nil {
			cl.Close()
			t.Fatalf("txn %s: begin: %v", txid, err)
		}
		r := &kgo.Record{Topic: topic, Value: []byte("post-crash")}
		if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
			cl.Close()
			t.Fatalf("txn %s: produce after crash: %v", txid, err)
		}
		if err := cl.EndTransaction(ctx, kgo.TryCommit); err != nil {
			cl.Close()
			t.Fatalf("txn %s: end after crash: %v", txid, err)
		}
		cl.Close()
	}
}

type errgroup struct {
	wg   sync.WaitGroup
	mu   sync.Mutex
	errs []error
}

func (eg *errgroup) do(fn func() error) {
	eg.wg.Add(1)
	go func() {
		defer eg.wg.Done()
		if err := fn(); err != nil {
			eg.mu.Lock()
			eg.errs = append(eg.errs, err)
			eg.mu.Unlock()
		}
	}()
}

func (eg *errgroup) wait() error {
	eg.wg.Wait()
	return errors.Join(eg.errs...)
}

// copyDir recursively copies src to dst for crash simulation.
func copyDir(t *testing.T, src, dst string) {
	t.Helper()
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		sp := filepath.Join(src, e.Name())
		dp := filepath.Join(dst, e.Name())
		if e.IsDir() {
			if err := os.MkdirAll(dp, 0o755); err != nil {
				t.Fatal(err)
			}
			copyDir(t, sp, dp)
		} else {
			data, err := os.ReadFile(sp)
			if err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(dp, data, 0o644); err != nil {
				t.Fatal(err)
			}
		}
	}
}

// TestPersistShareGroupSPSO verifies that acknowledged records are not
// redelivered after a clean restart. The SPSO should advance past them.
func TestPersistShareGroupSPSO(t *testing.T) {
	t.Parallel()
	tmem := newTestMemFS()

	const topic = "share-persist-spso"
	const group = "share-persist-spso-grp"
	const total = 10

	// Phase 1: produce, consume all records via share group, ack, close.
	{
		c, err := NewCluster(tmem.opt(), NumBrokers(1), SeedTopics(1, topic))
		if err != nil {
			t.Fatal(err)
		}

		produceShareN(t, c, topic, group, total)

		cl := newShareConsumer(t, c, topic, group)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		var got int
		for got < total {
			fetches := cl.PollFetches(ctx)
			for _, r := range fetches.Records() {
				got++
				r.Ack(kgo.AckAccept)
			}
			if ctx.Err() != nil {
				break
			}
		}
		if got < total {
			t.Fatalf("phase 1: expected %d, got %d", total, got)
		}
		cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if _, err := cl.CommitAcks(cCtx); err != nil {
			t.Fatal(err)
		}
		cCancel()
		cl.Close()
		c.Close()
	}

	// Phase 2: reopen, verify no records are redelivered.
	{
		c, err := NewCluster(tmem.opt(), NumBrokers(1), SeedTopics(1, topic))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		cl := newShareConsumer(t, c, topic, group)
		verifyZeroRecords(t, cl, 500*time.Millisecond)
	}
}

// TestPersistShareGroupAcquiredReleasedOnRestart verifies that records that
// were acquired but not acked become available again after restart.
func TestPersistShareGroupAcquiredReleasedOnRestart(t *testing.T) {
	t.Parallel()
	tmem := newTestMemFS()

	const topic = "share-persist-acq"
	const group = "share-persist-acq-grp"
	const total = 5

	// Phase 1: produce, acquire records via raw ShareFetch (no ack), close.
	{
		c, err := NewCluster(tmem.opt(), NumBrokers(1), SeedTopics(1, topic))
		if err != nil {
			t.Fatal(err)
		}

		produceShareN(t, c, topic, group, total)

		cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
		if err != nil {
			t.Fatal(err)
		}
		memberID, topicID := joinShareGroupRaw(t, cl, group, topic)
		_, acquired := rawShareFetch(t, cl, group, memberID, topicID, 0)
		if acquired < total {
			t.Fatalf("phase 1: expected %d acquired, got %d", total, acquired)
		}
		// Do NOT ack. Close cluster — acquired records should be saved.
		cl.Close()
		c.Close()
	}

	// Phase 2: reopen, verify the records are available for redelivery.
	{
		c, err := NewCluster(tmem.opt(), NumBrokers(1), SeedTopics(1, topic))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		cl := newShareConsumer(t, c, topic, group)
		records := collectRecords(t, cl, total, 10*time.Second)
		if len(records) < total {
			t.Fatalf("phase 2: expected %d redelivered, got %d", total, len(records))
		}
	}
}

// TestPersistShareGroupConfigRestart verifies that share group configs
// (share.auto.offset.reset) survive a clean restart.
func TestPersistShareGroupConfigRestart(t *testing.T) {
	t.Parallel()
	tmem := newTestMemFS()

	const topic = "share-persist-cfg"
	const group = "share-persist-cfg-grp"

	// Phase 1: set share.auto.offset.reset=earliest, produce records, close.
	{
		c, err := NewCluster(tmem.opt(), NumBrokers(1), SeedTopics(1, topic))
		if err != nil {
			t.Fatal(err)
		}

		cl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
		if err != nil {
			t.Fatal(err)
		}
		setShareAutoOffsetReset(t, cl, group)
		produceN(t, c, topic, 10)
		cl.Close()
		c.Close()
	}

	// Phase 2: reopen, join share group. If the config survived, the SPSO
	// should start at 0 (earliest) and we should see the 10 records.
	{
		c, err := NewCluster(tmem.opt(), NumBrokers(1), SeedTopics(1, topic))
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		cl := newShareConsumer(t, c, topic, group)
		records := collectRecords(t, cl, 10, 10*time.Second)
		if len(records) < 10 {
			t.Fatalf("phase 2: expected 10 records (earliest), got %d", len(records))
		}
	}
}

// TestPersistShareGroupArchivedNotRedelivered verifies that records archived
// via max delivery count remain archived after restart and are not redelivered.
func TestPersistShareGroupArchivedNotRedelivered(t *testing.T) {
	t.Parallel()
	tmem := newTestMemFS()

	const topic = "share-persist-arch"
	const group = "share-persist-arch-grp"
	const total = 5

	// Phase 1: produce records, release them twice (max delivery = 2),
	// causing archival, then close.
	{
		c, err := NewCluster(tmem.opt(), NumBrokers(1),
			SeedTopics(1, topic),
			BrokerConfigs(map[string]string{
				"group.share.delivery.count.limit": "2",
			}),
		)
		if err != nil {
			t.Fatal(err)
		}

		produceShareN(t, c, topic, group, total)

		// Delivery 1: fetch and release.
		cl1 := newShareConsumer(t, c, topic, group)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		var got int
		for got < total {
			fetches := cl1.PollFetches(ctx)
			for _, r := range fetches.Records() {
				got++
				r.Ack(kgo.AckRelease)
			}
			if ctx.Err() != nil {
				break
			}
		}
		cancel()
		if got < total {
			t.Fatalf("delivery 1: expected %d, got %d", total, got)
		}
		cCtx, cCancel := context.WithTimeout(context.Background(), 5*time.Second)
		if _, err := cl1.CommitAcks(cCtx); err != nil {
			t.Fatal(err)
		}
		cCancel()
		cl1.Close()

		// Delivery 2: fetch and release again — triggers archival.
		cl2 := newShareConsumer(t, c, topic, group)
		ctx2, cancel2 := context.WithTimeout(context.Background(), 10*time.Second)
		got = 0
		for got < total {
			fetches := cl2.PollFetches(ctx2)
			for _, r := range fetches.Records() {
				got++
				r.Ack(kgo.AckRelease)
			}
			if ctx2.Err() != nil {
				break
			}
		}
		cancel2()
		if got < total {
			t.Fatalf("delivery 2: expected %d, got %d", total, got)
		}
		cCtx, cCancel = context.WithTimeout(context.Background(), 5*time.Second)
		if _, err := cl2.CommitAcks(cCtx); err != nil {
			t.Fatal(err)
		}
		cCancel()
		cl2.Close()

		c.Close()
	}

	// Phase 2: reopen with same max delivery config, verify no records.
	{
		c, err := NewCluster(tmem.opt(), NumBrokers(1),
			SeedTopics(1, topic),
			BrokerConfigs(map[string]string{
				"group.share.delivery.count.limit": "2",
			}),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		cl := newShareConsumer(t, c, topic, group)
		verifyZeroRecords(t, cl, 500*time.Millisecond)
	}
}
