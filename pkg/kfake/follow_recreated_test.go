package kfake

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

// consumeFollowing consumes n records, allowing the UNKNOWN_TOPIC_ID polls
// a recreated topic surfaces before the client adds it back.
func consumeFollowing(t *testing.T, cl *kgo.Client, n int, timeout time.Duration) []*kgo.Record {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	var records []*kgo.Record
	for len(records) < n {
		fs := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("timed out consuming: got %d/%d", len(records), n)
		}
		for _, e := range fs.Errors() {
			if !errors.Is(e.Err, kerr.UnknownTopicID) {
				t.Fatalf("consume error %s/%d: %v", e.Topic, e.Partition, e.Err)
			}
		}
		records = append(records, fs.Records()...)
	}
	return records
}

// With FollowRecreatedTopics, the client purges a recreated topic and adds
// it back itself: consumers resume with the new topic per the reset policy
// and producers resume with the next record.
func TestFollowRecreatedTopics(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name       string
		group      bool
		use848     bool
		partitions bool
	}{
		{name: "direct"},
		{name: "partitions", partitions: true},
		{name: "classic", group: true},
		{name: "848", group: true, use848: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			topic := "t-follow-" + test.name
			group := "g-follow-" + test.name
			c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
			prod := newPlainClient(t, c)
			produceNStrings(t, prod, topic, 3)

			opts := []kgo.Opt{
				kgo.FollowRecreatedTopics(),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
				kgo.FetchMaxWait(100 * time.Millisecond),
				kgo.MetadataMinAge(50 * time.Millisecond),
				kgo.RetryBackoffFn(func(int) time.Duration { return 10 * time.Millisecond }),
			}
			switch {
			case test.partitions:
				opts = append(opts, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart()}}))
			case test.group:
				opts = append(opts, kgo.ConsumeTopics(topic), kgo.ConsumerGroup(group), kgo.DisableAutoCommit())
			default:
				opts = append(opts, kgo.ConsumeTopics(topic))
			}
			var cl *kgo.Client
			if test.use848 {
				cl = newClient848(t, c, opts...)
			} else {
				cl = newPlainClient(t, c, opts...)
			}
			consumeN(t, cl, 3, 5*time.Second)

			recreateTopic(t, c, topic)
			produceNStrings(t, newPlainClient(t, c), topic, 2)

			got := values(consumeFollowing(t, cl, 2, 15*time.Second))
			if got[0] != "value-0" || got[1] != "value-1" {
				t.Fatalf("consumed %v after the recreation, want the new topic from its start", got)
			}
			if test.group {
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if err := cl.CommitUncommittedOffsets(ctx); err != nil {
					t.Fatalf("commit after following the recreation: %v", err)
				}
				if commit, ok := groupCommits(c, group)[topic][0]; !ok || commit.Offset != 2 {
					t.Fatalf("committed %+v for the new topic, want offset 2", commit)
				}
			}
		})
	}
}

// Records buffered for the old topic are produced to the new one, by ID and
// by name, and nothing is produced twice or failed.
func TestFollowRecreatedTopicsProducer(t *testing.T) {
	t.Parallel()
	for _, byName := range []bool{false, true} {
		name := "by-id"
		if byName {
			name = "by-name"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			topic := "t-follow-produce-" + name
			c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
			opts := []kgo.Opt{
				kgo.FollowRecreatedTopics(),
				kgo.DefaultProduceTopic(topic),
				kgo.ProducerLinger(time.Second),
				kgo.MetadataMinAge(50 * time.Millisecond),
			}
			if byName {
				v := kversion.Stable()
				v.SetMaxKeyVersion(int16(kmsg.Produce), 12)
				opts = append(opts, kgo.MaxVersions(v))
			}
			cl := newPlainClient(t, c, opts...)
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			noErrs := func(errs <-chan error, n int) {
				t.Helper()
				for _, err := range collectErrs(ctx, t, errs, n) {
					if err != nil {
						t.Fatalf("produce: %v", err)
					}
				}
			}

			before := produceAsync(ctx, cl, "value-", 3)
			if err := cl.Flush(ctx); err != nil {
				t.Fatalf("flush: %v", err)
			}
			noErrs(before, 3)

			recreateTopic(t, c, topic)

			// Buffered under the old topic and lingering; the refresh
			// that reports the new ID carries them over.
			buffered := produceAsync(ctx, cl, "value-", 3)
			cl.ForceMetadataRefresh()
			noErrs(buffered, 3)
			if end := c.PartitionInfo(topic, 0).HighWatermark; end != 3 {
				t.Fatalf("the recreated topic holds %d records, want the 3 carried over", end)
			}
			if err := cl.ProduceSync(ctx, kgo.StringRecord("after")).FirstErr(); err != nil {
				t.Fatalf("produce after the recreation: %v", err)
			}
			if end := c.PartitionInfo(topic, 0).HighWatermark; end != 4 {
				t.Fatalf("the recreated topic holds %d records, want 4", end)
			}
		})
	}
}
