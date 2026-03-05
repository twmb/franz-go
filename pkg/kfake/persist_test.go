package kfake_test

import (
	"context"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
	"github.com/twmb/franz-go/pkg/sasl/plain"
)

func TestPersistProduceCloseReopen(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Phase 1: create cluster, produce records, close
	var addrs []string
	{
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
			kfake.SeedTopics(1, "test-topic"),
		)
		if err != nil {
			t.Fatal(err)
		}
		addrs = c.ListenAddrs()
		cl, err := kgo.NewClient(kgo.SeedBrokers(addrs...))
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for i := range 10 {
			r := &kgo.Record{
				Topic: "test-topic",
				Key:   []byte(fmt.Sprintf("key-%d", i)),
				Value: []byte(fmt.Sprintf("value-%d", i)),
			}
			if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
		cl.Close()
		c.Close()
	}

	// Phase 2: reopen, consume and verify
	{
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		cl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.ConsumeTopics("test-topic"),
			kgo.FetchMaxWait(250*time.Millisecond),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var records []*kgo.Record
		for len(records) < 10 {
			fetches := cl.PollFetches(ctx)
			if errs := fetches.Errors(); len(errs) > 0 {
				t.Fatal(errs)
			}
			fetches.EachRecord(func(r *kgo.Record) {
				records = append(records, r)
			})
		}
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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.SyncWrites(),
			kfake.NumBrokers(1),
			kfake.SeedTopics(1, "sync-topic"),
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
				Key:   []byte(fmt.Sprintf("k%d", i)),
				Value: []byte(fmt.Sprintf("v%d", i)),
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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		cl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.ConsumeTopics("sync-topic"),
			kgo.FetchMaxWait(250*time.Millisecond),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var records []*kgo.Record
		for len(records) < 5 {
			fetches := cl.PollFetches(ctx)
			if errs := fetches.Errors(); len(errs) > 0 {
				t.Fatal(errs)
			}
			fetches.EachRecord(func(r *kgo.Record) {
				records = append(records, r)
			})
		}
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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
			kfake.SeedTopics(1, topic),
		)
		if err != nil {
			t.Fatal(err)
		}

		// Produce records
		prodCl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for i := range 10 {
			r := &kgo.Record{Topic: topic, Value: []byte(fmt.Sprintf("v%d", i))}
			if err := prodCl.ProduceSync(ctx, r).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
		prodCl.Close()

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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
			kfake.SeedTopics(1, "pid-topic"),
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
			origEpoch = int16(txn.ProducerEpoch)
		}
		cl.Close()
		c.Close()
	}

	// Phase 2: reopen, verify PID is recoverable
	{
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		// Init a new client with same txn ID - should get same PID with bumped epoch
		cl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.TransactionalID("test-txn"),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()

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
			if int16(txn.ProducerEpoch) <= origEpoch {
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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
			kfake.EnableSASL(),
			kfake.Superuser("PLAIN", "admin", "admin"),
			kfake.EnableACLs(),
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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
			kfake.EnableSASL(),
			kfake.Superuser("PLAIN", "admin", "admin"),
			kfake.EnableACLs(),
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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
			kfake.BrokerConfigs(map[string]string{
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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
			kfake.SeedTopics(1, topic),
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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.SyncWrites(),
			kfake.NumBrokers(1),
			kfake.SeedTopics(1, "crc-topic"),
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
			r := &kgo.Record{Topic: "crc-topic", Value: []byte(fmt.Sprintf("v%d", i))}
			if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
		cl.Close()
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
				os.WriteFile(path, data, 0644)
			}
		}
	}

	// Phase 2: reopen with full replay - should truncate corrupt entry
	{
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
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

		cl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.ConsumeTopics("crc-topic"),
			kgo.FetchMaxWait(250*time.Millisecond),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()

		var records []*kgo.Record
		for {
			fetches := cl.PollFetches(ctx)
			if ctx.Err() != nil {
				break
			}
			fetches.EachRecord(func(r *kgo.Record) {
				records = append(records, r)
			})
			if len(records) > 0 {
				break // got some records, corruption only affected the tail
			}
		}
		if len(records) == 0 {
			t.Fatal("expected at least 1 recovered record after corruption, got 0")
		}
		if len(records) >= 5 {
			t.Fatalf("expected fewer than 5 records after corruption, got %d", len(records))
		}
		t.Logf("recovered %d of 5 records after corruption (HWM=%d)", len(records), pi.HighWatermark)
	}
}

func TestPersistSegmentRollover(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Use a very small segment size to force rollover
	{
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
			kfake.SeedTopics(1, "seg-topic"),
			kfake.BrokerConfigs(map[string]string{
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
			r := &kgo.Record{Topic: "seg-topic", Value: []byte(fmt.Sprintf("value-%d-padding-data", i))}
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
	t.Logf("created %d segment files", segCount)

	// Reopen and verify all records
	{
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		cl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.ConsumeTopics("seg-topic"),
			kgo.FetchMaxWait(250*time.Millisecond),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var records []*kgo.Record
		for len(records) < 20 {
			fetches := cl.PollFetches(ctx)
			if ctx.Err() != nil {
				break
			}
			fetches.EachRecord(func(r *kgo.Record) {
				records = append(records, r)
			})
		}
		if len(records) != 20 {
			t.Fatalf("expected 20 records after segment rollover restart, got %d", len(records))
		}
	}
}

func TestPersistSnapshotFastStartup(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Phase 1: produce records, close cleanly (creates snapshot)
	{
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
			kfake.SeedTopics(1, "snap-topic"),
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
			r := &kgo.Record{Topic: "snap-topic", Value: []byte(fmt.Sprintf("v%d", i))}
			if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
		cl.Close()
		c.Close()
	}

	// Verify snapshot.json exists
	snapPath := filepath.Join(dir, "partitions", "snap-topic-0", "snapshot.json")
	if _, err := os.Stat(snapPath); err != nil {
		t.Fatalf("expected snapshot.json to exist: %v", err)
	}

	// Phase 2: reopen, verify snapshot-based load works
	{
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		cl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.ConsumeTopics("snap-topic"),
			kgo.FetchMaxWait(250*time.Millisecond),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		var records []*kgo.Record
		for len(records) < 10 {
			fetches := cl.PollFetches(ctx)
			if ctx.Err() != nil {
				break
			}
			fetches.EachRecord(func(r *kgo.Record) {
				records = append(records, r)
			})
		}
		if len(records) != 10 {
			t.Fatalf("expected 10 records from snapshot-based load, got %d", len(records))
		}
	}
}

func TestPersistEntryFraming(t *testing.T) {
	t.Parallel()

	// Test the entry framing round-trip directly
	testData := []byte("hello world entry data")

	// Create a temp file and write an entry
	dir := t.TempDir()
	path := filepath.Join(dir, "test.log")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatal(err)
	}

	// Write using the same framing as the internal writeEntry
	// Format: [4 bytes length][4 bytes CRC32][2 bytes version][N bytes data]
	length := uint32(2 + len(testData))
	var hdr [10]byte
	binary.LittleEndian.PutUint32(hdr[0:4], length)
	binary.LittleEndian.PutUint16(hdr[8:10], 1) // version 1
	// CRC covers version + data
	var vbuf [2]byte
	binary.LittleEndian.PutUint16(vbuf[:], 1)
	crcVal := uint32(0)
	// Use CRC32 IEEE
	tab := crcTable()
	crcVal = crc32Update(tab, crcVal, vbuf[:])
	crcVal = crc32Update(tab, crcVal, testData)
	binary.LittleEndian.PutUint32(hdr[4:8], crcVal)

	f.Write(hdr[:])
	f.Write(testData)
	f.Close()

	// Read back and verify
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	// Verify the entry can be decoded
	if len(raw) != 10+len(testData) {
		t.Fatalf("expected %d bytes, got %d", 10+len(testData), len(raw))
	}

	storedLen := binary.LittleEndian.Uint32(raw[0:4])
	if storedLen != uint32(2+len(testData)) {
		t.Fatalf("expected length %d, got %d", 2+len(testData), storedLen)
	}

	storedVer := binary.LittleEndian.Uint16(raw[8:10])
	if storedVer != 1 {
		t.Fatalf("expected version 1, got %d", storedVer)
	}

	recoveredData := raw[10:]
	if string(recoveredData) != string(testData) {
		t.Fatalf("data mismatch: %q vs %q", recoveredData, testData)
	}
}

func TestPersistNoDataDir(t *testing.T) {
	t.Parallel()

	// Without DataDir, persistence should be a no-op
	c, err := kfake.NewCluster(
		kfake.NumBrokers(1),
		kfake.SeedTopics(1, "no-persist"),
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
	r := &kgo.Record{Topic: "no-persist", Value: []byte("test")}
	if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
		t.Fatal(err)
	}
	cl.Close()
	c.Close() // Should not create any files
}

func TestPersistMultipleTopics(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	topics := []string{"topic-a", "topic-b", "topic-c"}

	// Phase 1: create multiple topics with data
	{
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
			kfake.SeedTopics(2, topics...),
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
				r := &kgo.Record{Topic: topic, Value: []byte(fmt.Sprintf("%s-v%d", topic, i))}
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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.SyncWrites(),
			kfake.NumBrokers(1),
			kfake.SeedTopics(1, topic),
		)
		if err != nil {
			t.Fatal(err)
		}

		prodCl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for i := range 10 {
			r := &kgo.Record{Topic: topic, Value: []byte(fmt.Sprintf("v%d", i))}
			if err := prodCl.ProduceSync(ctx, r).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
		prodCl.Close()

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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.SyncWrites(),
			kfake.NumBrokers(1),
			kfake.SeedTopics(1, topic),
		)
		if err != nil {
			t.Fatal(err)
		}

		prodCl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for i := range 5 {
			r := &kgo.Record{Topic: topic, Value: []byte(fmt.Sprintf("v%d", i))}
			if err := prodCl.ProduceSync(ctx, r).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
		prodCl.Close()

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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.SyncWrites(),
			kfake.NumBrokers(1),
			kfake.SeedTopics(1, topic),
		)
		if err != nil {
			t.Fatal(err)
		}

		// Produce some records
		prodCl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		for i := range 10 {
			r := &kgo.Record{Topic: topic, Value: []byte(fmt.Sprintf("v%d", i))}
			if err := prodCl.ProduceSync(ctx, r).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
		prodCl.Close()

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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.SyncWrites(),
			kfake.NumBrokers(1),
			kfake.SeedTopics(1, topic),
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
		t.Logf("generation before crash: %d", lastGeneration)

		// Crash: don't call c.Close()
	}

	// Phase 2: reopen, raw JoinGroup to verify generation continues.
	{
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		cl := newPlainClient(t, c)
		defer cl.Close()

		recoveredGen := rawJoinGeneration(t, ctx, cl, group, topic)
		// After recovery, the next join should produce generation > lastGeneration.
		if recoveredGen <= lastGeneration {
			t.Fatalf("expected recovered generation > %d, got %d", lastGeneration, recoveredGen)
		}
		t.Logf("recovered generation: %d (was %d)", recoveredGen, lastGeneration)
	}
}

// rawJoinGeneration performs a raw JoinGroup+SyncGroup to observe the generation.
func rawJoinGeneration(t *testing.T, ctx context.Context, cl *kgo.Client, group, topic string, instanceID ...string) int32 {
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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.SyncWrites(),
			kfake.NumBrokers(1),
			kfake.SeedTopics(1, topic),
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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		cl := newPlainClient(t, c)
		defer cl.Close()

		gen := rawJoinGeneration(t, ctx, cl, group, topic, instanceID)
		if gen <= 0 {
			t.Fatalf("expected positive generation, got %d", gen)
		}
	}
}

// TestPersistFullReplayAbortedTxns verifies that abortedTxns are correctly
// reconstructed during full replay (when snapshot is missing). A read_committed
// consumer should not see records from an aborted transaction.
func TestPersistFullReplayAbortedTxns(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	const topic = "abort-replay-topic"

	// Phase 1: produce committed + aborted transactional records, clean shutdown.
	{
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.SyncWrites(),
			kfake.NumBrokers(1),
			kfake.SeedTopics(1, topic),
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		cl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.TransactionalID("txn-abort-test"),
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
			r := &kgo.Record{Topic: topic, Partition: 0, Value: []byte(fmt.Sprintf("committed-%d", i))}
			if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
		if err := cl.EndTransaction(ctx, kgo.TryCommit); err != nil {
			t.Fatal(err)
		}

		// Transaction 2: produce 2 records and ABORT.
		if err := cl.BeginTransaction(); err != nil {
			t.Fatal(err)
		}
		for i := range 2 {
			r := &kgo.Record{Topic: topic, Partition: 0, Value: []byte(fmt.Sprintf("aborted-%d", i))}
			if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
		if err := cl.EndTransaction(ctx, kgo.TryAbort); err != nil {
			t.Fatal(err)
		}

		cl.Close()
		c.Close() // clean shutdown - writes snapshot
	}

	// Delete the partition snapshot to force full replay on next open.
	snapPath := filepath.Join(dir, "partitions", "abort-replay-topic-0", "snapshot.json")
	if err := os.Remove(snapPath); err != nil {
		t.Fatal(err)
	}

	// Phase 2: reopen (full replay), verify read_committed filters aborted records.
	{
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// read_committed consumer
		cl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.FetchIsolationLevel(kgo.ReadCommitted()),
			kgo.FetchMaxWait(250*time.Millisecond),
			kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
				topic: {0: kgo.NewOffset().AtStart()},
			}),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()

		// Collect all records.
		var records []*kgo.Record
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			fetches := cl.PollFetches(ctx)
			fetches.EachRecord(func(r *kgo.Record) {
				records = append(records, r)
			})
			if len(records) >= 3 {
				break
			}
		}

		// Should see exactly 3 committed records, no aborted records.
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

		// Verify HWM > 3 (aborted batches + control batches exist on disk).
		pi := c.PartitionInfo(topic, 0)
		if pi == nil {
			t.Fatal("partition info not found")
		}
		if pi.HighWatermark <= 3 {
			t.Fatalf("expected HWM > 3 (aborted batches should exist), got %d", pi.HighWatermark)
		}
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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.SyncWrites(),
			kfake.NumBrokers(1),
			kfake.SeedTopics(1, topic),
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		// Produce records
		prodCl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
		if err != nil {
			t.Fatal(err)
		}
		for i := range 20 {
			r := &kgo.Record{Topic: topic, Value: []byte(fmt.Sprintf("v%d", i))}
			if err := prodCl.ProduceSync(ctx, r).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
		prodCl.Close()

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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
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
			t.Logf("skipping unparseable entry: %v", err)
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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.SyncWrites(),
			kfake.NumBrokers(1),
			kfake.SeedTopics(1, topic),
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
			r := &kgo.Record{Topic: topic, Partition: 0, Value: []byte(fmt.Sprintf("committed-%d", i))}
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
			r := &kgo.Record{Topic: topic, Partition: 0, Value: []byte(fmt.Sprintf("inflight-%d", i))}
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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
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
		defer plainCl.Close()

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
		committedCl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.FetchIsolationLevel(kgo.ReadCommitted()),
			kgo.FetchMaxWait(250*time.Millisecond),
			kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
				topic: {0: kgo.NewOffset().AtStart()},
			}),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer committedCl.Close()

		var committed []*kgo.Record
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			fetches := committedCl.PollFetches(ctx)
			fetches.EachRecord(func(r *kgo.Record) {
				committed = append(committed, r)
			})
			if len(committed) >= 3 {
				break
			}
		}
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
		uncommittedCl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.FetchIsolationLevel(kgo.ReadUncommitted()),
			kgo.FetchMaxWait(250*time.Millisecond),
			kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
				topic: {0: kgo.NewOffset().AtStart()},
			}),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer uncommittedCl.Close()

		var all []*kgo.Record
		deadline = time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			fetches := uncommittedCl.PollFetches(ctx)
			fetches.EachRecord(func(r *kgo.Record) {
				all = append(all, r)
			})
			if len(all) >= 5 {
				break
			}
		}
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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.SyncWrites(),
			kfake.NumBrokers(1),
			kfake.SeedTopics(1, topic),
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
				Value: []byte(fmt.Sprintf("value-%04d-padding-to-increase-record-size", i)),
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
		t.Logf("nbytes before shutdown: %d", savedNbytes)

		c.Close()
	}

	// Phase 2: reopen from snapshot, verify exact nbytes match.
	{
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
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
		t.Logf("after retention: logStartOffset=%d nbytes=%d (was %d)", pi2.LogStartOffset, pi2.NumBytes, savedNbytes)
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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.SyncWrites(),
			kfake.NumBrokers(1),
			kfake.SeedTopics(1, topic),
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
			r := &kgo.Record{Topic: topic, Value: []byte(fmt.Sprintf("v%d", i))}
			if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
		cl.Close()

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
		if filepath.Ext(e.Name()) == ".dat" {
			path := filepath.Join(partDir, e.Name())
			data, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			// Truncate to half the file size.
			half := len(data) / 2
			if half > 0 {
				os.WriteFile(path, data[:half], 0644)
			}
		}
	}

	// Phase 2: reopen with snapshot + truncated segment.
	// HWM should be clamped to match the actual loaded batches.
	{
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
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
		cl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.ConsumeTopics(topic),
			kgo.FetchMaxWait(250*time.Millisecond),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		var records []*kgo.Record
		for {
			fetches := cl.PollFetches(ctx)
			if ctx.Err() != nil {
				break
			}
			fetches.EachRecord(func(r *kgo.Record) {
				records = append(records, r)
			})
			if len(records) > 0 {
				break
			}
		}
		if len(records) == 0 {
			t.Fatal("expected at least 1 record after truncated segment load")
		}
		t.Logf("recovered %d of 10 records, HWM=%d", len(records), pi.HighWatermark)
	}
}

// TestPersistQuotasRestart verifies that client quotas survive a restart.
func TestPersistQuotasRestart(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	// Phase 1: set quotas, close.
	{
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
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

// TestPersistSnapshotAbortedTxns verifies that abortedTxns metadata
// survives a clean shutdown via the snapshot path. This is the DEFAULT
// startup path (unlike TestPersistFullReplayAbortedTxns which tests
// full replay). A read_committed consumer should not see aborted records.
func TestPersistSnapshotAbortedTxns(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	const topic = "abort-snap-topic"

	// Phase 1: produce committed + aborted transactional records, clean shutdown.
	{
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
			kfake.SeedTopics(1, topic),
		)
		if err != nil {
			t.Fatal(err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		cl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.TransactionalID("txn-snap-abort"),
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
			r := &kgo.Record{Topic: topic, Partition: 0, Value: []byte(fmt.Sprintf("committed-%d", i))}
			if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
		if err := cl.EndTransaction(ctx, kgo.TryCommit); err != nil {
			t.Fatal(err)
		}

		// Transaction 2: produce 2 records and ABORT.
		if err := cl.BeginTransaction(); err != nil {
			t.Fatal(err)
		}
		for i := range 2 {
			r := &kgo.Record{Topic: topic, Partition: 0, Value: []byte(fmt.Sprintf("aborted-%d", i))}
			if err := cl.ProduceSync(ctx, r).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
		if err := cl.EndTransaction(ctx, kgo.TryAbort); err != nil {
			t.Fatal(err)
		}

		cl.Close()
		c.Close() // clean shutdown - writes snapshot with abortedTxns
	}

	// Verify snapshot exists (this test uses the snapshot path).
	snapPath := filepath.Join(dir, "partitions", "abort-snap-topic-0", "snapshot.json")
	if _, err := os.Stat(snapPath); err != nil {
		t.Fatalf("expected snapshot.json to exist: %v", err)
	}

	// Phase 2: reopen via snapshot, verify read_committed filters aborted records.
	{
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		cl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.FetchIsolationLevel(kgo.ReadCommitted()),
			kgo.FetchMaxWait(250*time.Millisecond),
			kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
				topic: {0: kgo.NewOffset().AtStart()},
			}),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()

		var records []*kgo.Record
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			fetches := cl.PollFetches(ctx)
			fetches.EachRecord(func(r *kgo.Record) {
				records = append(records, r)
			})
			if len(records) >= 3 {
				break
			}
		}

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

		// Verify HWM > 3 (aborted batches + control batches exist on disk).
		pi := c.PartitionInfo(topic, 0)
		if pi == nil {
			t.Fatal("partition info not found")
		}
		if pi.HighWatermark <= 3 {
			t.Fatalf("expected HWM > 3 (aborted batches should exist), got %d", pi.HighWatermark)
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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
			kfake.SeedTopics(1, keepTopic, deleteTopic),
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
				r := &kgo.Record{Topic: topic, Value: []byte(fmt.Sprintf("v%d", i))}
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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
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
		cl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.ConsumeTopics(keepTopic),
			kgo.FetchMaxWait(250*time.Millisecond),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()

		var records []*kgo.Record
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			fetches := cl.PollFetches(ctx)
			fetches.EachRecord(func(r *kgo.Record) {
				records = append(records, r)
			})
			if len(records) >= 5 {
				break
			}
		}
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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
			kfake.SeedTopics(1, topic),
		)
		if err != nil {
			t.Fatal(err)
		}

		// Produce records
		prodCl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for i := range 10 {
			r := &kgo.Record{Topic: topic, Value: []byte(fmt.Sprintf("v%d", i))}
			if err := prodCl.ProduceSync(ctx, r).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
		prodCl.Close()

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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
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

func TestPersistSessionStateExpired(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()

	const topic = "session-expire-topic"
	const group = "session-expire-group"

	// Phase 1: produce, create classic consumer group, close cluster first
	{
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
			kfake.SeedTopics(1, topic),
		)
		if err != nil {
			t.Fatal(err)
		}

		prodCl, err := kgo.NewClient(kgo.SeedBrokers(c.ListenAddrs()...))
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		for i := range 5 {
			r := &kgo.Record{Topic: topic, Value: []byte(fmt.Sprintf("v%d", i))}
			if err := prodCl.ProduceSync(ctx, r).FirstErr(); err != nil {
				t.Fatal(err)
			}
		}
		prodCl.Close()

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
		for consumed < 3 {
			fetches := consCl.PollFetches(ctx)
			fetches.EachRecord(func(_ *kgo.Record) { consumed++ })
		}
		if err := consCl.CommitUncommittedOffsets(ctx); err != nil {
			t.Fatal(err)
		}
		c.Close()
		consCl.Close()
	}

	// Manually edit the session state file to make the shutdown time old
	// enough that all sessions have expired.
	ssPath := filepath.Join(dir, "session_state.json")
	data, err := os.ReadFile(ssPath)
	if err != nil {
		t.Fatalf("reading session_state.json: %v", err)
	}
	var ss map[string]json.RawMessage
	if err := json.Unmarshal(data, &ss); err != nil {
		t.Fatalf("parsing session_state.json: %v", err)
	}
	// Set shutdownAt to 1 hour ago to guarantee expiry
	oldTime, _ := json.Marshal(time.Now().Add(-1 * time.Hour))
	ss["shutdownAt"] = oldTime
	newData, _ := json.Marshal(ss)
	if err := os.WriteFile(ssPath, newData, 0644); err != nil {
		t.Fatalf("writing modified session_state.json: %v", err)
	}

	// Phase 2: reopen - sessions expired, members should NOT be restored
	{
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		// session_state.json should still be deleted
		if _, err := os.Stat(ssPath); !os.IsNotExist(err) {
			t.Fatal("session_state.json should be deleted even when sessions are expired")
		}

		// Group should exist (from groups.log) but be Empty (no members)
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
		if dg.State != "Empty" {
			t.Fatalf("expected Empty state (expired sessions), got %s", dg.State)
		}
		if len(dg.Members) != 0 {
			t.Fatalf("expected no members (expired sessions), got %d", len(dg.Members))
		}
	}
}

func TestPersistSessionStateNoDataDir(t *testing.T) {
	t.Parallel()

	// Without DataDir, session state should not be saved
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	// Just close - this should not panic or error
	c.Close()
}

// TestPersistSeqWindowDedup verifies that sequence window deduplication
// works correctly across a clean restart. A produce retry after restart
// should return the original offset, not write a duplicate batch.
func TestPersistSeqWindowDedup(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	const topic = "dedup-topic"

	buildProduce := func(pid int64, epoch int16, seq int32) *kmsg.ProduceRequest {
		rec := kmsg.Record{Key: []byte(fmt.Sprintf("k-%d", seq)), Value: []byte(fmt.Sprintf("v-%d", seq))}
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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
			kfake.SeedTopics(1, topic),
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
		c, err := kfake.NewCluster(
			kfake.DataDir(dir),
			kfake.NumBrokers(1),
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
		defer cl.Close()

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
		consumer, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
			kgo.FetchMaxWait(250*time.Millisecond),
			kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
				topic: {0: kgo.NewOffset().AtStart()},
			}),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer consumer.Close()

		var records []*kgo.Record
		deadline := time.Now().Add(3 * time.Second)
		for time.Now().Before(deadline) {
			fetches := consumer.PollFetches(ctx)
			fetches.EachRecord(func(r *kgo.Record) {
				records = append(records, r)
			})
			if len(records) >= 4 {
				break
			}
		}
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
		c, err := kfake.NewCluster(
			kfake.NumBrokers(1),
			kfake.DataDir(dir),
			kfake.SeedTopics(-1, topic),
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
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
		c, err := kfake.NewCluster(
			kfake.NumBrokers(1),
			kfake.DataDir(dir),
			kfake.Ports(0),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer c.Close()

		cl, err := kgo.NewClient(
			kgo.SeedBrokers(c.ListenAddrs()...),
		)
		if err != nil {
			t.Fatal(err)
		}
		defer cl.Close()

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
		t.Logf("offset commit error (expected): %s", kerr.ErrorForCode(errCode))

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
		// UNKNOWN_MEMBER_ID is expected (the member doesn't exist),
		// but the group itself is alive.
		t.Logf("heartbeat error (expected): %s - group is alive", ec)
	}
}

// Helper functions for CRC computation in tests
func crcTable() *crc32.Table { return crc32.IEEETable }

func crc32Update(tab *crc32.Table, crc uint32, data []byte) uint32 {
	return crc32.Update(crc, tab, data)
}
