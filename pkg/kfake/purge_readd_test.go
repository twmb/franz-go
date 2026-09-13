package kfake

import (
	"errors"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// Purging a topic and adding it back at once must leave the member consuming
// it. The join or heartbeat that the purge forces reads the subscription
// live, so the re-add would already be in it: the broker would never see the
// topic leave, would keep the assignment as it was, and the fresh cursors
// would never be assigned. The purge leaves the topic out of that one
// request; the next one subscribes to it again.
//
// To make the window certain each case holds a heartbeat at the broker while
// purging and re-adding, so the request built after the release sees the
// topic back. The killed case instead fails the first full heartbeat after
// the purge and re-add: a heartbeat error forgets what we last sent, and the
// re-add has to survive that.
func TestPurgeAndReaddImmediately(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		cfg  recreationCfg
		key  int16
		// kill fails the first full heartbeat after the purge and
		// re-add rather than holding the next one.
		kill bool
		// want is how many records the re-added topic yields. Nothing
		// is committed, so a classic or 848 member reads the three
		// from before the purge and the two after it; a share group
		// starts at the latest offset and reads only the two.
		want int
	}{
		{
			name: "classic",
			cfg:  recreationCfg{name: "purge-readd-classic", group: true, opts: []kgo.Opt{kgo.DisableAutoCommit()}},
			key:  int16(kmsg.Heartbeat),
			want: 5,
		},
		{
			name: "848",
			cfg:  recreationCfg{name: "purge-readd-848", next: true, opts: []kgo.Opt{kgo.DisableAutoCommit()}},
			key:  int16(kmsg.ConsumerGroupHeartbeat),
			want: 5,
		},
		{
			// The broker sends a share group's assignment to a full
			// heartbeat or when the assignment changes, so a purge
			// and re-add that leaves the subscription as it was
			// would be a keepalive and the fresh cursors would never
			// be assigned. The purge makes the next heartbeat full.
			name: "share",
			cfg:  recreationCfg{name: "purge-readd-share", share: true},
			key:  int16(kmsg.ShareGroupHeartbeat),
			want: 2,
		},
		{
			name: "848-killed-heartbeat",
			cfg:  recreationCfg{name: "purge-hberr", next: true, opts: []kgo.Opt{kgo.DisableAutoCommit()}},
			key:  int16(kmsg.ConsumerGroupHeartbeat),
			kill: true,
			want: 5,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			r := startRecreation(t, test.cfg)

			if test.kill {
				full := func(kreq kmsg.Request) bool {
					return kreq.(*kmsg.ConsumerGroupHeartbeatRequest).SubscribedTopicNames != nil
				}
				killed, _ := hold(r.c, holdReq{Key: test.key, When: full, Fail: errors.New("killing the first full heartbeat")})
				r.cl.PurgeTopicsFromClient(r.topic)
				r.cl.AddConsumeTopics(r.topic)
				select {
				case <-killed:
				case <-time.After(10 * time.Second):
					t.Fatal("no full heartbeat followed the purge and re-add")
				}
			} else {
				held, release := hold(r.c, holdReq{Key: test.key})
				<-held
				r.cl.PurgeTopicsFromClient(r.topic)
				r.cl.AddConsumeTopics(r.topic)
				// Let metadata load the re-added topic, so the
				// request after the release would carry it.
				time.Sleep(200 * time.Millisecond)
				release()
			}

			produceNStrings(t, r.prod, r.topic, 2)
			if got := consumeN(t, r.cl, test.want, 15*time.Second); len(got) != test.want {
				t.Fatalf("consumed %d records after purging and re-adding, want %d", len(got), test.want)
			}
		})
	}
}

// A purged topic that no full heartbeat subscribed to (added and purged
// within one heartbeat, or never consumed) is nothing the broker has to
// revoke. Leaving it out of the next heartbeat would leave the subscription
// equal to the last one, a keepalive, and the exclusion would stay until an
// unrelated full heartbeat: adding the topic would not subscribe to it.
func TestPurgeUnsubscribedTopicThenAdd(t *testing.T) {
	t.Parallel()
	const unsubscribed = "t-purge-unsub-other"
	r := startRecreation(t, recreationCfg{
		name:  "purge-unsub",
		next:  true,
		extra: []string{unsubscribed},
		opts:  []kgo.Opt{kgo.DisableAutoCommit()},
	})
	produceNStrings(t, r.prod, unsubscribed, 3)
	time.Sleep(300 * time.Millisecond) // the heartbeats settle into keepalives

	r.cl.PurgeTopicsFromClient(unsubscribed)
	time.Sleep(300 * time.Millisecond) // a few heartbeats without the topic
	r.cl.AddConsumeTopics(unsubscribed)
	if got := consumeN(t, r.cl, 3, 15*time.Second); len(got) != 3 {
		t.Fatalf("consumed %d records after adding the purged topic, want 3", len(got))
	}
}
