package kfake

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
	"github.com/twmb/franz-go/pkg/kversion"
)

// A deleted and recreated topic keeps its name and comes back under a new
// topic ID. The client never adopts the new ID: cursors and record buffers
// keep failing with UNKNOWN_TOPIC_ID until the topic is purged and re-added,
// and nothing is consumed from, produced to, or committed for the new topic
// under the old subscription. The tests below cover both sides, the group
// commit, and the purge + re-add heal.

//////////////
// PLUMBING //
//////////////
//
// Everything to the next banner drives the broker rather than saying
// anything about recreation. It is shaped like the kfake API we expect to
// grow for this, so that adopting that is a delete rather than a rewrite.

// holdReq selects requests to intercept: Skip lets that many matching
// requests through first, Count is how many to then take (default 1), and
// When filters by content.
type holdReq struct {
	Key   int16
	Skip  int
	Count int
	When  func(kmsg.Request) bool
	Fail  error // fail the requests rather than holding them
}

// hold intercepts requests at the broker. held closes once the last one is
// taken, and release lets them all go. A holdReq with Fail set never holds,
// so its release is a no-op.
func hold(c *Cluster, h holdReq) (held <-chan struct{}, release func()) {
	if h.Count == 0 {
		h.Count = 1
	}
	var (
		fired   = make(chan struct{})
		unblock = make(chan struct{})
		mu      sync.Mutex
		matched int
		relOnce sync.Once
	)
	c.ControlKey(h.Key, func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if h.When != nil && !h.When(kreq) {
			return nil, nil, false
		}
		mu.Lock()
		matched++
		n := matched
		mu.Unlock()
		if n <= h.Skip || n > h.Skip+h.Count {
			return nil, nil, false
		}
		if n == h.Skip+h.Count {
			close(fired)
		}
		if h.Fail != nil {
			return nil, h.Fail, true
		}
		c.SleepControl(func() { <-unblock })
		return nil, nil, false
	})
	return fired, func() { relOnce.Do(func() { close(unblock) }) }
}

// counter counts requests of one key at the broker.
type counter struct {
	n       atomic.Int32
	stopped atomic.Bool
}

func countRequests(c *Cluster, key int16) *counter {
	cnt := new(counter)
	c.ControlKey(key, func(kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if !cnt.stopped.Load() {
			cnt.n.Add(1)
		}
		return nil, nil, false
	})
	return cnt
}

func (c *counter) Count() int32 { return c.n.Load() }
func (c *counter) Stop()        { c.stopped.Store(true) }

func topicMetadata(t *testing.T, cl *kgo.Client, topic string) *kmsg.MetadataResponse {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req := kmsg.NewPtrMetadataRequest()
	rt := kmsg.NewMetadataRequestTopic()
	rt.Topic = kmsg.StringPtr(topic)
	req.Topics = append(req.Topics, rt)
	resp, err := req.RequestWith(ctx, cl)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

// serveMetadata answers metadata with resp whenever on returns true. This is
// the one place a canned response is stamped with the request's version.
func serveMetadata(c *Cluster, resp *kmsg.MetadataResponse, on func() bool) {
	c.ControlKey(int16(kmsg.Metadata), func(kreq kmsg.Request) (kmsg.Response, error, bool) {
		c.KeepControl()
		if !on() {
			return nil, nil, false
		}
		serve := *resp
		serve.Version = kreq.(*kmsg.MetadataRequest).Version
		return &serve, nil, true
	})
}

// commitAsync starts a commit of the offsets and returns a func that waits
// for it and reports the error the group saw. It returns as soon as the
// commit is queued, so a caller can order something else behind it. A commit
// is answered per partition, so we check that the response mirrors the
// request rather than trusting the top level error alone.
func commitAsync(ctx context.Context, t *testing.T, cl *kgo.Client, offsets map[string]map[int32]kgo.EpochOffset) func() error {
	t.Helper()
	var (
		done = make(chan struct{})
		got  error
	)
	cl.CommitOffsets(ctx, offsets, func(_ *kgo.Client, req *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
		defer close(done)
		if err != nil {
			got = err
			return
		}
		var wantParts, gotParts int
		for _, rt := range req.Topics {
			wantParts += len(rt.Partitions)
		}
		for _, rt := range resp.Topics {
			gotParts += len(rt.Partitions)
			for _, p := range rt.Partitions {
				if err := kerr.ErrorForCode(p.ErrorCode); err != nil {
					got = err
				}
			}
		}
		if len(resp.Topics) != len(req.Topics) || gotParts != wantParts {
			got = errors.New("the commit response does not mirror the request")
		}
	})
	return func() error { <-done; return got }
}

// produceAsync produces n records and hands back their promise errors.
func produceAsync(ctx context.Context, cl *kgo.Client, value string, n int) <-chan error {
	errs := make(chan error, n)
	for i := range n {
		cl.Produce(ctx, kgo.StringRecord(value+strconv.Itoa(i)), func(_ *kgo.Record, err error) { errs <- err })
	}
	return errs
}

func collectErrs(ctx context.Context, t *testing.T, errs <-chan error, n int) []error {
	t.Helper()
	var got []error
	for range n {
		select {
		case err := <-errs:
			got = append(got, err)
		case <-ctx.Done():
			t.Fatalf("produce promises did not complete: got %d of %d", len(got), n)
		}
	}
	return got
}

func endOffset(t *testing.T, cl *kgo.Client, topic string, partition int32) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ends, err := kadm.NewClient(cl).ListEndOffsets(ctx, topic)
	if err != nil {
		t.Fatalf("list end offsets: %v", err)
	}
	end, ok := ends.Lookup(topic, partition)
	if !ok || end.Err != nil {
		t.Fatalf("no end offset for %s/%d: ok=%v err=%v", topic, partition, ok, end.Err)
	}
	return end.Offset
}

// committedOffset returns the group's committed offset for the partition,
// or -1 when nothing is committed.
func committedOffset(t *testing.T, cl *kgo.Client, group, topic string, partition int32) int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	offsets, err := kadm.NewClient(cl).FetchOffsets(ctx, group)
	if err != nil {
		t.Fatalf("fetch offsets: %v", err)
	}
	o, ok := offsets.Lookup(topic, partition)
	if !ok {
		return -1
	}
	if o.Err != nil {
		t.Fatalf("fetch offsets %s/%d: %v", topic, partition, o.Err)
	}
	return o.At
}

// recreationLogger closes confirmed when the client logs that a topic was
// recreated: the point after which the client refuses the topic's offsets.
// The wire fails fetches before that, so a test that must reach the client's
// own refusal waits on this rather than on the fetch errors.
type recreationLogger struct {
	once      sync.Once
	confirmed chan struct{}
}

func (*recreationLogger) Level() kgo.LogLevel { return kgo.LogLevelWarn }

func (l *recreationLogger) Log(_ kgo.LogLevel, msg string, _ ...any) {
	if strings.Contains(msg, "deleted and recreated") {
		l.once.Do(func() { close(l.confirmed) })
	}
}

func values(rs []*kgo.Record) []string {
	var vs []string
	for _, r := range rs {
		vs = append(vs, string(r.Value))
	}
	return vs
}

//////////////
// FIXTURE  //
//////////////

// recreationCfg shapes what startRecreation builds.
type recreationCfg struct {
	name       string
	partitions int32    // of the topic, default 1
	extra      []string // topics to seed besides the one under test
	group      bool     // consume in a classic group
	next       bool     // consume in a KIP-848 group
	share      bool     // consume in a share group
	txnID      string   // consume in a transact session that has begun
	opts       []kgo.Opt
}

// recreation is the shape these tests share: one topic holding three
// records, and a client that has consumed them. prod is a second client: it
// writes those records, recreates the topic, and reads offsets back, so the
// client under test never does any of that itself.
type recreation struct {
	t      *testing.T
	ctx    context.Context
	c      *Cluster
	topic  string
	group  string
	prod   *kgo.Client
	cl     *kgo.Client
	s      *kgo.GroupTransactSession // set when cfg.txnID is
	first  []*kgo.Record             // the three records consumed before the recreation
	logger *recreationLogger
}

func startRecreation(t *testing.T, cfg recreationCfg) *recreation {
	t.Helper()
	if cfg.partitions == 0 {
		cfg.partitions = 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	r := &recreation{
		t:      t,
		ctx:    ctx,
		topic:  "t-" + cfg.name,
		group:  "g-" + cfg.name,
		logger: &recreationLogger{confirmed: make(chan struct{})},
	}

	copts := []Opt{
		NumBrokers(1),
		SeedTopics(cfg.partitions, append([]string{r.topic}, cfg.extra...)...),
	}
	if cfg.share {
		copts = append(copts, BrokerConfigs(map[string]string{"group.share.heartbeat.interval.ms": "100"}))
	}
	r.c = newCluster(t, copts...)
	r.prod = newPlainClient(t, r.c)

	opts := append(recreationOpts(r.topic), kgo.WithLogger(r.logger))
	switch {
	case cfg.share:
		opts = append(opts, kgo.ShareGroup(r.group))
	case cfg.group || cfg.next || cfg.txnID != "":
		opts = append(opts, kgo.ConsumerGroup(r.group))
	}
	opts = append(opts, cfg.opts...)
	switch {
	case cfg.txnID != "":
		opts = append(opts, kgo.SeedBrokers(r.c.ListenAddrs()...), kgo.TransactionalID(cfg.txnID))
		s, err := kgo.NewGroupTransactSession(opts...)
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(s.Close)
		if err := s.Begin(); err != nil {
			t.Fatal(err)
		}
		r.s, r.cl = s, s.Client()
	case cfg.next:
		r.cl = newClient848(t, r.c, opts...)
	default:
		r.cl = newPlainClient(t, r.c, opts...)
	}

	if cfg.share {
		// Share groups start at the latest offset: establish the
		// assignment before producing.
		pollCtx, pollCancel := context.WithTimeout(context.Background(), time.Second)
		r.cl.PollFetches(pollCtx)
		pollCancel()
	}
	produceNStrings(t, r.prod, r.topic, 3)
	r.first = consumeN(t, r.cl, 3, 5*time.Second)
	if cfg.share {
		for _, rec := range r.first {
			rec.Ack(kgo.AckAccept)
		}
	}
	return r
}

// recreationOpts speeds up the paths a recreation goes through: fetch
// errors trigger metadata refreshes gated by the min age, and the client
// strips the first few UNKNOWN_TOPIC_ID responses with the retry backoff
// between them.
func recreationOpts(topic string) []kgo.Opt {
	return []kgo.Opt{
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchMaxWait(100 * time.Millisecond),
		kgo.MetadataMinAge(50 * time.Millisecond),
		kgo.RetryBackoffFn(func(int) time.Duration { return 10 * time.Millisecond }),
	}
}

// recreate deletes the topic and creates it again, then writes two records
// to the new one from a client that never knew the old.
func (r *recreation) recreate() {
	r.t.Helper()
	recreateTopic(r.t, r.c, r.topic)
	produceNStrings(r.t, newPlainClient(r.t, r.c), r.topic, 2)
}

// stall waits for the topic's partitions to surface UNKNOWN_TOPIC_ID, then
// polls a few more times: the error keeps coming and no record ever arrives.
// One poll may come back empty, because a group session can restart under it
// and discard what was buffered.
func (r *recreation) stall(partitions int) {
	r.t.Helper()
	check := func(fs kgo.Fetches) int {
		fs.EachRecord(func(rec *kgo.Record) {
			r.t.Fatalf("consumed %s/%d@%d from the recreated topic", rec.Topic, rec.Partition, rec.Offset)
		})
		for _, e := range fs.Errors() {
			if e.Topic != r.topic || !errors.Is(e.Err, kerr.UnknownTopicID) {
				r.t.Fatalf("unexpected fetch error %s/%d: %v", e.Topic, e.Partition, e.Err)
			}
		}
		return len(fs.Errors())
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	seen := make(map[int32]struct{})
	for len(seen) < partitions {
		fs := r.cl.PollFetches(ctx)
		if ctx.Err() != nil {
			r.t.Fatalf("timed out waiting for UNKNOWN_TOPIC_ID on %d partitions of %s, saw %v", partitions, r.topic, seen)
		}
		check(fs)
		for _, e := range fs.Errors() {
			seen[e.Partition] = struct{}{}
		}
	}
	var unknown int
	for range 5 {
		pollCtx, pollCancel := context.WithTimeout(context.Background(), 2*time.Second)
		fs := r.cl.PollFetches(pollCtx)
		pollCancel()
		if check(fs) > 0 {
			unknown++
		}
	}
	if unknown < 4 {
		r.t.Fatalf("UNKNOWN_TOPIC_ID surfaced on %d of 5 polls after the stall", unknown)
	}
}

// confirmed waits for the client to decide the topic was recreated.
func (r *recreation) confirmed() {
	r.t.Helper()
	select {
	case <-r.logger.confirmed:
	case <-time.After(10 * time.Second):
		r.t.Fatal("the client never confirmed the recreation")
	}
}

// heal purges the topic and adds it back, then consumes the new topic from
// its start.
func (r *recreation) heal() {
	r.t.Helper()
	r.cl.PurgeTopicsFromClient(r.topic)
	r.cl.AddConsumeTopics(r.topic)
	if got := values(consumeN(r.t, r.cl, 2, 15*time.Second)); got[0] != "value-0" || got[1] != "value-1" {
		r.t.Fatalf("after re-adding, consumed %v, want the new topic from its start", got)
	}
}

func (r *recreation) committed() int64 {
	r.t.Helper()
	return committedOffset(r.t, r.prod, r.group, r.topic, 0)
}

///////////////
// RECREATION //
///////////////

// The topic stalls with UNKNOWN_TOPIC_ID however it is consumed, and purging
// it and adding it back is the heal. While stalled the client must also be
// quiet: it neither refetches nor refreshes metadata in a loop.
func TestRecreationStallsAndHeals(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		cfg  recreationCfg
		// parts is how many of the topic's partitions must stall.
		parts int
		// quiet bounds requests of quietKey in a second of not polling.
		quietKey  int16
		quietMax  int32
		quietWhat string
		recreate  func(*recreation)
		heal      func(*recreation)
	}{
		{
			// The broker resolves a session partition's name when the
			// partition enters the session, so it answers the old
			// entry INCONSISTENT_TOPIC_ID on every fetch. We strip
			// that error, so with another topic in the same session
			// the fetch loop used to spin at round trip speed and
			// the poll never surfaced anything. We reset the session
			// instead, and the full fetch that follows carries the
			// old ID and is answered UNKNOWN_TOPIC_ID.
			name: "sibling-topic",
			cfg: recreationCfg{
				name:  "recreate-sibling",
				extra: []string{"t-recreate-sibling-idle"},
				opts:  []kgo.Opt{kgo.ConsumeTopics("t-recreate-sibling", "t-recreate-sibling-idle")},
			},
			parts:     1,
			quietKey:  int16(kmsg.Fetch),
			quietMax:  10,
			quietWhat: "fetch requests",
		},
		{
			// A topic recreated with more partitions than before: the
			// partitions the old topic did not have take the old ID
			// too, so the whole topic fails rather than the new
			// partitions consuming the new topic.
			name:  "more-partitions",
			cfg:   recreationCfg{name: "recreate-grow"},
			parts: 3,
			recreate: func(r *recreation) {
				recreateTopicN(r.t, r.c, r.topic, 3)
				newProd := newPlainClient(r.t, r.c)
				for p := range int32(3) {
					rec := kgo.StringRecord("value-" + strconv.Itoa(int(p)))
					rec.Topic, rec.Partition = r.topic, p
					produceSync(r.t, newProd, rec)
				}
			},
			heal: func(r *recreation) {
				r.cl.PurgeTopicsFromClient(r.topic)
				r.cl.AddConsumeTopics(r.topic)
				if got := consumeN(r.t, r.cl, 3, 15*time.Second); len(got) != 3 {
					r.t.Fatalf("after re-adding, consumed %d records, want 3", len(got))
				}
			},
		},
		{
			// KIP-848 assigns the recreated topic by its new ID. We
			// resolve that to the name rather than leaving it
			// unresolved, which refreshed metadata on every heartbeat,
			// also after the heal, which then never consumed. The
			// heartbeat interval is 100ms.
			name: "next-gen-group",
			cfg: recreationCfg{
				name: "recreate-848",
				next: true,
				// Later opts win: keep fetch-error refreshes rare.
				opts: []kgo.Opt{kgo.MetadataMinAge(time.Second)},
			},
			parts:     1,
			quietKey:  int16(kmsg.Metadata),
			quietMax:  5,
			quietWhat: "metadata requests",
		},
		{
			// A share group also assigns by topic ID and reads
			// through the same cursor grace counter.
			name:  "share-group",
			cfg:   recreationCfg{name: "recreate-share", share: true},
			parts: 1,
			heal: func(r *recreation) {
				r.cl.PurgeTopicsFromClient(r.topic)
				r.cl.AddConsumeTopics(r.topic)
				// A share partition starts at its latest offset
				// when it is first assigned; produce until a
				// poll returns a record.
				newProd := newPlainClient(r.t, r.c)
				for deadline := time.Now().Add(15 * time.Second); ; {
					produceNStrings(r.t, newProd, r.topic, 1)
					ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
					fs := r.cl.PollFetches(ctx)
					cancel()
					if len(fs.Records()) > 0 {
						return
					}
					if time.Now().After(deadline) {
						r.t.Fatalf("no records from the new topic after re-adding; last poll errors: %v", fs.Errors())
					}
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			r := startRecreation(t, test.cfg)
			if test.recreate != nil {
				test.recreate(r)
			} else {
				r.recreate()
			}
			r.stall(test.parts)

			if test.quietWhat != "" {
				n := countRequests(r.c, test.quietKey)
				before := n.Count()
				time.Sleep(time.Second)
				got := n.Count() - before
				n.Stop()
				if got > test.quietMax {
					t.Fatalf("%d %s in a second while stalled and not polling", got, test.quietWhat)
				}
			}

			if test.heal != nil {
				test.heal(r)
			} else {
				r.heal()
			}
		})
	}
}

// A group's offsets for a recreated topic belong to the old topic: a commit
// that landed under the name would have a fresh consumer of the new topic
// start from them. We wait for the client to confirm the recreation, because
// a broker that keys commits by ID refuses them before that and it is the
// client we are testing.
func TestRecreationCommitRefused(t *testing.T) {
	t.Parallel()
	byName := kversion.Stable()
	byName.SetMaxKeyVersion(int16(kmsg.Fetch), 12)       // fetches carry no topic ID
	byName.SetMaxKeyVersion(int16(kmsg.OffsetCommit), 9) // commits go by name

	for _, test := range []struct {
		name string
		opts []kgo.Opt
		// mark records the offsets by marking rather than polling.
		mark bool
		// byName means nothing on the wire carries a topic ID, so the
		// consumer keeps reading and never stalls.
		byName bool
		commit func(*kgo.Client, context.Context) error
	}{
		{
			// Polled offsets: the fetch told us which topic they
			// came from.
			name:   "polled",
			opts:   []kgo.Opt{kgo.AutoCommitInterval(time.Hour)},
			commit: (*kgo.Client).CommitUncommittedOffsets,
		},
		{
			// Marked offsets never went through a fetch, so they
			// carry no topic ID of their own.
			name:   "marked",
			opts:   []kgo.Opt{kgo.AutoCommitMarks(), kgo.AutoCommitInterval(time.Hour)},
			mark:   true,
			commit: (*kgo.Client).CommitMarkedOffsets,
		},
		{
			// Below Kafka 3.1 a fetch answers by name, so the
			// consumer reads the new topic and those offsets have no
			// ID either; below OffsetCommit v10 the broker cannot
			// refuse the commit. Both are Redpanda at every version.
			name:   "no-wire-ids",
			opts:   []kgo.Opt{kgo.DisableAutoCommit(), kgo.MaxVersions(byName)},
			byName: true,
			commit: (*kgo.Client).CommitUncommittedOffsets,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			r := startRecreation(t, recreationCfg{name: "commit-" + test.name, group: true, opts: test.opts})
			if test.mark {
				for _, rec := range r.first {
					r.cl.MarkCommitRecords(rec)
				}
			}
			r.recreate()
			if !test.byName {
				r.stall(1)
			}
			r.confirmed()

			if err := test.commit(r.cl, r.ctx); !errors.Is(err, kerr.UnknownTopicID) {
				t.Fatalf("committing the old topic's offsets returned %v, want UNKNOWN_TOPIC_ID", err)
			}
			if at := r.committed(); at != -1 {
				t.Fatalf("group has a committed offset of %d for the recreated topic, want none", at)
			}
		})
	}
}

// Purging rebalances, and the revoke commit of that rebalance has nothing of
// the old topic left to send; the re-added topic then commits normally. The
// user is told to purge on UNKNOWN_TOPIC_ID, and the first of those arrives
// before the client has seen the new ID, so the purge must be safe both
// before and after the client confirms the recreation.
func TestRecreationHealCommits(t *testing.T) {
	t.Parallel()
	for _, confirm := range []bool{true, false} {
		name := "after-confirmation"
		if !confirm {
			name = "before-confirmation"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			r := startRecreation(t, recreationCfg{
				name:  "heal-" + name,
				group: true,
				opts:  []kgo.Opt{kgo.AutoCommitInterval(time.Hour)}, // only the revoke commits
			})

			var stale atomic.Bool
			if !confirm {
				// Answering with the old ID until the purge is
				// done keeps the client from confirming first.
				stale.Store(true)
				serveMetadata(r.c, topicMetadata(t, r.cl, r.topic), stale.Load)
			}
			recreateTopic(t, r.c, r.topic)
			r.stall(1)

			if confirm {
				r.confirmed()
				// The commit we refuse is answered per partition.
				err := commitAsync(r.ctx, t, r.cl, map[string]map[int32]kgo.EpochOffset{r.topic: {0: {Offset: 3}}})()
				if !errors.Is(err, kerr.UnknownTopicID) {
					t.Fatalf("explicit commit for the old topic returned %v, want UNKNOWN_TOPIC_ID per partition", err)
				}
			}

			r.cl.PurgeTopicsFromClient(r.topic)
			stale.Store(false)
			r.cl.AddConsumeTopics(r.topic)
			produceNStrings(t, newPlainClient(t, r.c), r.topic, 2)
			if got := values(consumeN(t, r.cl, 2, 15*time.Second)); got[0] != "value-0" || got[1] != "value-1" {
				t.Fatalf("after re-adding, consumed %v, want the new topic from its start", got)
			}

			// Consuming the re-added topic means the purge's
			// rebalance, and its revoke commit, are over.
			if at := r.committed(); at != -1 {
				t.Fatalf("the revoke after purging committed %d for the recreated topic, want none", at)
			}
			if err := r.cl.CommitUncommittedOffsets(r.ctx); err != nil {
				t.Fatalf("committing the new topic's offsets: %v", err)
			}
			if at := r.committed(); at != 2 {
				t.Fatalf("committed %d for the new topic, want 2", at)
			}
		})
	}
}

// A commit waits for the commit before it to finish before it goes out. The
// client decides what the commit carries when it is asked for, not when it
// goes out: a purge in between removes the recreated topic from what the
// client knows, and a commit built after it would go by name to the new
// topic. The rebalance a purge causes cannot start while a commit is in
// flight, so the generation is still good when the second commit goes out.
func TestRecreationCommitQueuedBehindPurge(t *testing.T) {
	t.Parallel()
	const other = "t-recreate-queued-other"
	r := startRecreation(t, recreationCfg{
		name:  "recreate-queued",
		group: true,
		extra: []string{other},
		opts:  []kgo.Opt{kgo.ConsumeTopics("t-recreate-queued", other), kgo.DisableAutoCommit()},
	})
	r.recreate()
	r.stall(1)
	r.confirmed()

	held, release := hold(r.c, holdReq{Key: int16(kmsg.OffsetCommit)})
	firstDone := make(chan struct{})
	r.cl.CommitOffsets(r.ctx, map[string]map[int32]kgo.EpochOffset{other: {0: {Offset: 0}}}, func(*kgo.Client, *kmsg.OffsetCommitRequest, *kmsg.OffsetCommitResponse, error) {
		close(firstDone)
	})
	<-held

	// The commit is queued behind the held one before we purge.
	queued := commitAsync(r.ctx, t, r.cl, map[string]map[int32]kgo.EpochOffset{r.topic: {0: {Offset: 3}}})
	r.cl.PurgeTopicsFromClient(r.topic)
	release()
	<-firstDone
	if err := queued(); !errors.Is(err, kerr.UnknownTopicID) {
		t.Fatalf("the queued commit for the recreated topic returned %v, want UNKNOWN_TOPIC_ID", err)
	}
	if at := r.committed(); at != -1 {
		t.Fatalf("group has a committed offset of %d for the recreated topic, want none", at)
	}
}

// A transactional session cannot commit when a topic it used was recreated.
// Its offsets go through TxnOffsetCommit, which carries names only, so the
// broker cannot refuse them itself; and records it produced to a recreated
// topic are gone with the old topic.
func TestRecreationTxnSessionAborts(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name string
		// produced recreates the topic the session produces to rather
		// than the one it consumes.
		produced bool
		wantErrs []error
	}{
		{"consumed-topic", false, []error{kerr.UnknownTopicID}},
		{"produced-topic", true, []error{kerr.TransactionAbortable, kerr.UnknownTopicID}},
	} {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			out := "t-txn-" + test.name + "-out"
			r := startRecreation(t, recreationCfg{
				name:  "txn-" + test.name,
				extra: []string{out},
				txnID: "txn-" + test.name,
				opts:  []kgo.Opt{kgo.DefaultProduceTopic(out)},
			})
			if err := r.s.ProduceSync(r.ctx, kgo.StringRecord("derived")).FirstErr(); err != nil {
				t.Fatalf("produce in transaction: %v", err)
			}

			// A transactional commit carries no topic ID, so one sent
			// before the client confirms the recreation lands: wait
			// for the confirmation, not just for the fetch errors.
			if test.produced {
				recreateTopic(t, r.c, out)
				r.cl.ForceMetadataRefresh()
			} else {
				recreateTopic(t, r.c, r.topic)
				r.stall(1)
			}
			r.confirmed()

			committed, err := r.s.End(r.ctx, kgo.TryCommit)
			if committed {
				t.Fatal("End committed a transaction that used a recreated topic")
			}
			for _, want := range test.wantErrs {
				if !errors.Is(err, want) {
					t.Fatalf("End returned %v, want it to wrap %v", err, want)
				}
			}
			if at := r.committed(); at != -1 {
				t.Fatalf("the input has a committed offset of %d after the abort, want none", at)
			}
			// The abort left the session usable.
			if err := r.s.Begin(); err != nil {
				t.Fatalf("begin after the abort: %v", err)
			}
			if _, err := r.s.End(r.ctx, kgo.TryAbort); err != nil {
				t.Fatalf("abort the empty transaction: %v", err)
			}
		})
	}
}

// The same rule on a plain transactional producer: TryCommit is refused and
// the transaction stays open for the abort.
func TestRecreationTxnEndRefusesCommit(t *testing.T) {
	t.Parallel()
	const topic = "t-recreate-txn-end"
	c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))
	logger := &recreationLogger{confirmed: make(chan struct{})}
	cl := newPlainClient(t, c,
		kgo.TransactionalID("txn-recreate-end"),
		kgo.DefaultProduceTopic(topic),
		kgo.MetadataMinAge(50*time.Millisecond),
		kgo.WithLogger(logger),
	)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	if err := cl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := cl.ProduceSync(ctx, kgo.StringRecord("v")).FirstErr(); err != nil {
		t.Fatalf("produce in transaction: %v", err)
	}
	recreateTopic(t, c, topic)
	cl.ForceMetadataRefresh()
	select {
	case <-logger.confirmed:
	case <-time.After(10 * time.Second):
		t.Fatal("the client never confirmed the recreation")
	}

	if err := cl.EndTransaction(ctx, kgo.TryCommit); !errors.Is(err, kerr.TransactionAbortable) {
		t.Fatalf("EndTransaction(TryCommit) returned %v, want TRANSACTION_ABORTABLE", err)
	}
	if err := cl.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatalf("EndTransaction(TryAbort): %v", err)
	}
	if err := cl.BeginTransaction(); err != nil {
		t.Fatalf("begin after the abort: %v", err)
	}
	if err := cl.EndTransaction(ctx, kgo.TryAbort); err != nil {
		t.Fatalf("abort the empty transaction: %v", err)
	}
}

// Produce v13+ carries the topic ID and the broker rejects the old one; below
// that, produce carries the name and the new topic would accept the records.
// Either way the metadata refresh that reports the new ID fails everything
// buffered and everything produced after, and nothing reaches the new topic.
func TestRecreationProducerFails(t *testing.T) {
	t.Parallel()
	for _, byName := range []bool{false, true} {
		name := "by-id"
		if byName {
			name = "by-name"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			topic := "t-recreate-produce-" + name
			c := newCluster(t, NumBrokers(1), SeedTopics(1, topic))

			// The linger holds records we buffer after the
			// recreation until the metadata refresh fails them; a
			// flush sends what we want sent.
			opts := []kgo.Opt{
				kgo.DefaultProduceTopic(topic),
				kgo.ProducerLinger(5 * time.Second),
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

			before := produceAsync(ctx, cl, "value-", 3)
			if err := cl.Flush(ctx); err != nil {
				t.Fatalf("flush: %v", err)
			}
			for _, err := range collectErrs(ctx, t, before, 3) {
				if err != nil {
					t.Fatalf("produce before the recreation: %v", err)
				}
			}

			recreateTopic(t, c, topic)

			// Buffered under the old metadata, lingering; the refresh
			// fails them.
			buffered := produceAsync(ctx, cl, "value-", 3)
			cl.ForceMetadataRefresh()
			for _, err := range collectErrs(ctx, t, buffered, 3) {
				if !errors.Is(err, kerr.UnknownTopicID) {
					t.Fatalf("buffered record failed with %v, want UNKNOWN_TOPIC_ID", err)
				}
			}
			// New records fail before they buffer: the linger does
			// not apply.
			start := time.Now()
			if err := cl.ProduceSync(ctx, kgo.StringRecord("after")).FirstErr(); !errors.Is(err, kerr.UnknownTopicID) {
				t.Fatalf("produce after the recreation returned %v, want UNKNOWN_TOPIC_ID", err)
			}
			if took := time.Since(start); took > time.Second {
				t.Fatalf("produce after the recreation took %v to fail; it should fail before buffering", took)
			}
			if end := endOffset(t, cl, topic, 0); end != 0 {
				t.Fatalf("the recreated topic holds %d records, want 0", end)
			}

			cl.PurgeTopicsFromProducing(topic)
			healed := produceAsync(ctx, cl, "healed-", 1)
			if err := cl.Flush(ctx); err != nil {
				t.Fatalf("flush after re-adding: %v", err)
			}
			if err := collectErrs(ctx, t, healed, 1)[0]; err != nil {
				t.Fatalf("produce after purging: %v", err)
			}
			if end := endOffset(t, cl, topic, 0); end != 1 {
				t.Fatalf("the recreated topic holds %d records after purging and producing, want 1", end)
			}
		})
	}
}
