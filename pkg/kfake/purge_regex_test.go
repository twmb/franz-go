package kfake

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// A regex consumer that purges a topic must consume it again once the regex
// rediscovers it, which happens on the next metadata refresh; the test asks
// for one. Under KIP-848 the broker resolves the regex itself, so the purge
// cannot leave the topic out of the subscription the way a named
// subscription can: the broker keeps assigning the topic, the assignment
// does not change by name, and nothing restarted the session to assign the
// fresh cursors.
func TestPurgeRegexTopic(t *testing.T) {
	t.Parallel()
	for _, protocol := range []string{"classic", "848"} {
		t.Run(protocol, func(t *testing.T) {
			t.Parallel()
			topic := "t-purge-regex-" + protocol
			group := "g-purge-regex-" + protocol
			c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
			prod := newPlainClient(t, c)
			produceNStrings(t, prod, topic, 3)

			opts := []kgo.Opt{
				kgo.ConsumeTopics("^" + topic + "$"),
				kgo.ConsumeRegex(),
				kgo.ConsumerGroup(group),
				kgo.DisableAutoCommit(),
				kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
				kgo.FetchMaxWait(100 * time.Millisecond),
				kgo.MetadataMinAge(50 * time.Millisecond),
				kgo.RetryBackoffFn(func(int) time.Duration { return 10 * time.Millisecond }),
			}
			var cl *kgo.Client
			if protocol == "848" {
				cl = newClient848(t, c, opts...)
			} else {
				cl = newPlainClient(t, c, opts...)
			}
			consumeN(t, cl, 3, 5*time.Second)

			cl.PurgeTopicsFromClient(topic)
			cl.ForceMetadataRefresh()
			produceNStrings(t, prod, topic, 2)

			// Nothing was committed, so the rediscovered topic is
			// consumed from its start again.
			if got := consumeN(t, cl, 5, 15*time.Second); len(got) != 5 {
				t.Fatalf("consumed %d records after purging, want 5", len(got))
			}
		})
	}
}

// The same for a classic consumer, with the regex rediscovering the topic
// after the session-end revoke gave it up and before the member rejoins. The
// purge empties the subscription, so that revoke hands the partition back and
// drops the topic from what we own; the regex then adds the topic back, and
// because we no longer own it, metadata notes nothing in reassign. The rejoin
// subscribes to the topic again and the broker assigns the same partition
// back, so the assignment does not change by name: only dropping the topic
// from what we last owned makes the next session see it as added and give it
// fresh cursors.
//
// The revoke callback is what orders this. The member cannot rejoin until the
// callback returns, so we refresh metadata from the test while it is blocked,
// twice: the client fetches metadata one request at a time and evaluates the
// regex before the next request goes out, so seeing the second request proves
// the first response added the topic back.
func TestPurgeRegexTopicRevokedFirst(t *testing.T) {
	t.Parallel()
	const (
		topic = "t-purge-regex-revoked"
		group = "g-purge-regex-revoked"
	)
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	prod := newPlainClient(t, c)
	produceNStrings(t, prod, topic, 3)

	// A regex consumer asks for every topic; the producer asks by name.
	metas := make(chan struct{}, 16)
	c.ControlKey(int16(kmsg.Metadata), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		if len(kreq.(*kmsg.MetadataRequest).Topics) == 0 {
			select {
			case metas <- struct{}{}:
			default:
			}
		}
		return nil, nil, false
	})

	var (
		revoked     = make(chan struct{})
		release     = make(chan struct{})
		revokeOnce  sync.Once
		releaseOnce sync.Once
	)
	releaseFn := func() { releaseOnce.Do(func() { close(release) }) }
	cl := newPlainClient(t, c,
		kgo.ConsumeTopics("^"+topic+"$"),
		kgo.ConsumeRegex(),
		kgo.ConsumerGroup(group),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(100*time.Millisecond),
		kgo.MetadataMinAge(50*time.Millisecond),
		kgo.RetryBackoffFn(func(int) time.Duration { return 10 * time.Millisecond }),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, revoking map[string][]int32) {
			if _, ok := revoking[topic]; ok {
				revokeOnce.Do(func() {
					close(revoked)
					<-release
				})
			}
		}),
	)
	t.Cleanup(releaseFn) // never leave the revoke blocked, closing waits on it
	consumeN(t, cl, 3, 5*time.Second)

	cl.PurgeTopicsFromClient(topic)
	waitCh(t, revoked, "purge never revoked the topic")
	drainCh(metas) // count only the refreshes we ask for below
	for range 2 {
		cl.ForceMetadataRefresh()
		waitCh(t, metas, "the regex consumer stopped asking for metadata")
	}
	releaseFn()

	produceNStrings(t, prod, topic, 2)
	if got := consumeN(t, cl, 5, 15*time.Second); len(got) != 5 {
		t.Fatalf("consumed %d records after purging, want 5", len(got))
	}
}

// The same under KIP-848, with the regex rediscovering the topic before the
// next heartbeat goes out: a heartbeat is held at the broker across the
// purge, so metadata adds the topic back first. The heartbeat built after
// the release then subscribes to and owns exactly what the last one did, so
// it goes out as a keepalive, and the broker answers a keepalive with no
// assignment: nothing would restart the session for the fresh cursors. A
// classic consumer sends its subscription in JoinGroup rather than in the
// heartbeat, so this is an 848 rule only.
func TestPurgeRegexTopicRediscoveredFirst(t *testing.T) {
	t.Parallel()
	const (
		topic = "t-purge-regex-held"
		group = "g-purge-regex-held"
	)
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	prod := newPlainClient(t, c)
	produceNStrings(t, prod, topic, 3)

	cl := newClient848(t, c,
		kgo.ConsumeTopics("^"+topic+"$"),
		kgo.ConsumeRegex(),
		kgo.ConsumerGroup(group),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(100*time.Millisecond),
		kgo.MetadataMinAge(50*time.Millisecond),
		kgo.RetryBackoffFn(func(int) time.Duration { return 10 * time.Millisecond }),
	)
	consumeN(t, cl, 3, 5*time.Second)
	time.Sleep(500 * time.Millisecond) // the heartbeats settle into keepalives

	var (
		held     = make(chan struct{})
		release  = make(chan struct{})
		holdOnce sync.Once
	)
	c.ControlKey(int16(kmsg.ConsumerGroupHeartbeat), func(kmsg.Request) (kmsg.Response, error, bool) {
		holdOnce.Do(func() {
			close(held)
			c.SleepControl(func() { <-release })
		})
		return nil, nil, false
	})
	<-held
	cl.PurgeTopicsFromClient(topic)
	cl.ForceMetadataRefresh()
	time.Sleep(300 * time.Millisecond) // the regex rediscovers the topic
	close(release)

	produceNStrings(t, prod, topic, 2)
	if got := consumeN(t, cl, 5, 15*time.Second); len(got) != 5 {
		t.Fatalf("consumed %d records after purging, want 5", len(got))
	}
}
