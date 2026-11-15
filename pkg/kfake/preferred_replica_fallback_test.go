package kfake

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// TestPreferredReplicaFallsBackToLeader covers a preferred read replica that
// dies while the partition's leader stays up: the client must go back to the
// leader instead of retrying the dead replica until the recheck interval, and
// must follow the leader back to the replica once it returns.
//
// The leader stops naming the replica while it is down, as a broker does once
// the replica's broker is fenced. The partition's leader and epoch never
// change, so metadata alone does not move the cursor.
func TestPreferredReplicaFallsBackToLeader(t *testing.T) {
	t.Parallel()
	const (
		topic = "foo"
		batch = 10
		// Far beyond the test's deadline: only the fallback can move the
		// cursor off the dead replica in time.
		recheckInterval = time.Hour
		deadline        = 15 * time.Second
	)

	c := newCluster(t, NumBrokers(2), SeedTopics(1, topic))
	ti := c.TopicInfo(topic)
	pi := c.PartitionInfo(topic, 0)
	leader := pi.Leader
	follower := (leader + 1) % 2
	c.SetFollowers(topic, 0, []int32{follower})

	var (
		followerDown            atomic.Bool
		followerReqs, deadFails atomic.Int32
		leaderServed            atomic.Int32
	)
	c.ControlKey(int16(kmsg.Fetch), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		req := kreq.(*kmsg.FetchRequest)
		if c.CurrentNode() == follower {
			if followerDown.Load() {
				// Closing the connection on every request stands in for
				// a dead broker.
				deadFails.Add(1)
				return nil, errors.New("follower is down"), true
			}
			followerReqs.Add(1)
			return nil, nil, false
		}
		if followerDown.Load() || req.Rack == "" {
			leaderServed.Add(1)
			return nil, nil, false
		}
		// The leader names the follower and serves nothing itself.
		p := c.PartitionInfo(topic, 0)
		resp := req.ResponseKind().(*kmsg.FetchResponse)
		rt := kmsg.NewFetchResponseTopic()
		rt.Topic = topic
		rt.TopicID = ti.TopicID
		rp := kmsg.NewFetchResponseTopicPartition()
		rp.Partition = 0
		rp.HighWatermark = p.HighWatermark
		rp.LastStableOffset = p.LastStableOffset
		rp.LogStartOffset = 0
		rp.PreferredReadReplica = follower
		rt.Partitions = append(rt.Partitions, rp)
		resp.Topics = append(resp.Topics, rt)
		return resp, nil, true
	})

	cl := newPlainClient(t, c,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{topic: {0: kgo.NewOffset().AtStart()}}),
		kgo.Rack("b"),
		kgo.FetchMaxWait(100*time.Millisecond),
		kgo.RecheckPreferredReplicaInterval(recheckInterval),
	)

	var consumed int
	consumeTo := func(phase string, n int) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), deadline)
		defer cancel()
		for consumed < n && ctx.Err() == nil {
			fs := cl.PollFetches(ctx)
			for _, r := range fs.Records() {
				if r.Offset != int64(consumed) {
					t.Fatalf("%s: got offset %d, want %d", phase, r.Offset, consumed)
				}
				consumed++
			}
		}
		if consumed < n {
			t.Fatalf("%s: consumed %d of %d records within %s (follower fetches %d, failed fetches to the dead follower %d, leader fetches served %d)",
				phase, consumed, n, deadline, followerReqs.Load(), deadFails.Load(), leaderServed.Load())
		}
	}

	// The client reads from the follower the leader names.
	produceN(t, c, topic, batch)
	consumeTo("follower up", batch)
	if followerReqs.Load() == 0 {
		t.Fatal("the client never fetched from the preferred replica")
	}

	// The follower dies; the leader keeps the partition. The client must
	// go back to the leader for the next records.
	followerDown.Store(true)
	served := leaderServed.Load()
	produceN(t, c, topic, batch)
	consumeTo("follower down", 2*batch)
	if deadFails.Load() == 0 {
		t.Fatal("the client never fetched from the dead follower, so the fallback was not exercised")
	}
	if leaderServed.Load() == served {
		t.Fatal("the leader served no fetch while the follower was down")
	}

	// The follower returns and the leader names it again: the client
	// follows, without a restart.
	followerDown.Store(false)
	before := followerReqs.Load()
	produceN(t, c, topic, batch)
	consumeTo("follower back", 3*batch)
	if followerReqs.Load() == before {
		t.Fatal("the client did not return to the follower once the leader named it again")
	}
}
