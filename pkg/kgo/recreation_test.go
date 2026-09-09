package kgo

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// waitTopicReady waits until the broker reports every partition of topic
// with a leader. CreateTopics returns before the broker hosts the partition,
// and a produce that lands in that gap burns its UnknownTopicRetries at the
// quick metadata cadence, about a second, which a loaded broker can exceed.
func waitTopicReady(tb testing.TB, topic string) {
	tb.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		req := kmsg.NewPtrMetadataRequest()
		mt := kmsg.NewMetadataRequestTopic()
		mt.Topic = kmsg.StringPtr(topic)
		req.Topics = append(req.Topics, mt)
		resp, err := req.RequestWith(context.Background(), adm())
		if err == nil && len(resp.Topics) == 1 && resp.Topics[0].ErrorCode == 0 && len(resp.Topics[0].Partitions) > 0 {
			ready := true
			for _, p := range resp.Topics[0].Partitions {
				if p.ErrorCode != 0 || p.Leader < 0 {
					ready = false
				}
			}
			if ready {
				return
			}
		}
		if time.Now().After(deadline) {
			tb.Fatalf("topic %s not ready after 30s; last err %v", topic, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// recreateTestTopic deletes and recreates topic with partitions partitions
// against the real test broker. Deletion propagates asynchronously, so the
// create retries while the broker still answers TopicAlreadyExists, and the
// broker's metadata view lags the controller, so this also waits until
// metadata reports a topic ID different from the old topic's: a client
// starting in the gap would briefly consume the old topic.
func recreateTestTopic(tb testing.TB, topic string, partitions int32) {
	tb.Helper()
	oldID := metadataTopicID(topic)

	delReq := kmsg.NewPtrDeleteTopicsRequest()
	delReq.TopicNames = []string{topic}
	dt := kmsg.NewDeleteTopicsRequestTopic()
	dt.Topic = kmsg.StringPtr(topic)
	delReq.Topics = append(delReq.Topics, dt)
	delResp, err := delReq.RequestWith(context.Background(), adm())
	if err == nil {
		err = kerr.ErrorForCode(delResp.Topics[0].ErrorCode)
	}
	if err != nil {
		tb.Fatalf("unable to delete %q for recreation: %v", topic, err)
	}

	req := kmsg.NewPtrCreateTopicsRequest()
	rt := kmsg.NewCreateTopicsRequestTopic()
	rt.Topic = topic
	rt.NumPartitions = partitions
	rt.ReplicationFactor = int16(testrf)
	req.Topics = append(req.Topics, rt)
	deadline := time.Now().Add(60 * time.Second)
	for {
		resp, err := req.RequestWith(context.Background(), adm())
		if err == nil {
			err = kerr.ErrorForCode(resp.Topics[0].ErrorCode)
		}
		if err == nil {
			break
		}
		if !errors.Is(err, kerr.TopicAlreadyExists) || time.Now().After(deadline) {
			tb.Fatalf("unable to recreate %q: %v", topic, err)
		}
		time.Sleep(250 * time.Millisecond)
	}

	if oldID == ([16]byte{}) {
		return // broker predates metadata topic IDs; nothing to confirm
	}
	for {
		if id := metadataTopicID(topic); id != oldID && id != ([16]byte{}) || time.Now().After(deadline) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
}

// metadataTopicID returns the topic's ID as the test broker's metadata
// reports it, or zero below metadata v10.
func metadataTopicID(topic string) [16]byte {
	req := kmsg.NewPtrMetadataRequest()
	mt := kmsg.NewMetadataRequestTopic()
	mt.Topic = kmsg.StringPtr(topic)
	req.Topics = append(req.Topics, mt)
	resp, err := req.RequestWith(context.Background(), adm())
	if err != nil || len(resp.Topics) != 1 {
		return [16]byte{}
	}
	return resp.Topics[0].TopicID
}

// collectRecreationVals polls until every wanted value arrived, failing on
// any unexpected value: an old-topic record or a duplicate delivery.
func collectRecreationVals(ctx context.Context, t *testing.T, cl *Client, want ...string) {
	t.Helper()
	wanted := make(map[string]bool, len(want))
	for _, w := range want {
		wanted[w] = false
	}
	seen := 0
	for seen < len(want) {
		if ctx.Err() != nil {
			t.Fatalf("timed out with %d/%d wanted values", seen, len(want))
		}
		fs := cl.PollFetches(ctx)
		fs.EachRecord(func(r *Record) {
			v := string(r.Value)
			had, ok := wanted[v]
			if !ok || had {
				t.Errorf("unexpected or duplicate value %q (offset %d, leader epoch %d)", v, r.Offset, r.LeaderEpoch)
				return
			}
			wanted[v] = true
			seen++
		})
	}
}

// logCapture is a minimal capturing logger for asserting that a specific
// log line fired.
type logCapture struct {
	mu sync.Mutex
	b  strings.Builder
}

func (*logCapture) Level() LogLevel { return LogLevelInfo }
func (l *logCapture) Log(_ LogLevel, msg string, _ ...any) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.b.WriteString(msg)
	l.b.WriteByte('\n')
}

func (l *logCapture) contains(substr string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return strings.Contains(l.b.String(), substr)
}

// A fetch taken just before the swap to a recreated topic carries the old
// topic's ID; the group must not record its offsets as committable.
func TestUpdateUncommittedSkipsOldTopicID(t *testing.T) {
	t.Parallel()

	oldID, newID := [16]byte{1}, [16]byte{2}
	tps := newTopicsPartitions()
	tps.storeTopics([]string{"t"})
	tp := tps.load()["t"]
	d := *tp.load()
	d.id = newID
	tp.v.Store(&d)
	g := &groupConsumer{cfg: &cfg{logger: new(nopLogger)}, tps: tps}

	fetch := func(id [16]byte) Fetches {
		return Fetches{{Topics: []FetchTopic{{
			Topic:      "t",
			TopicID:    id,
			Partitions: []FetchPartition{{Partition: 0, Records: []*Record{{Offset: 4}}}},
		}}}}
	}
	g.updateUncommitted(fetch(oldID))
	if _, ok := g.uncommitted["t"][0]; ok {
		t.Fatal("a fetch of the old topic's ID became committable")
	}
	g.updateUncommitted(fetch(newID))
	if e, ok := g.uncommitted["t"][0]; !ok || e.dirty.Offset != 5 {
		t.Fatalf("a fetch of the current ID did not become committable: %+v, %v", e, ok)
	}
}

func TestGetUncommittedSkipsOldTopicID(t *testing.T) {
	t.Parallel()

	oldID, newID := [16]byte{1}, [16]byte{2}
	tps := newTopicsPartitions()
	tps.storeTopics([]string{"t"})
	tp := tps.load()["t"]
	d := *tp.load()
	d.id = newID
	tp.v.Store(&d)
	g := &groupConsumer{cfg: &cfg{logger: new(nopLogger)}, tps: tps}

	stale := EpochOffset{Epoch: 3, Offset: 100}
	g.uncommitted = uncommitted{"t": {0: uncommit{dirty: stale, head: stale, id: oldID}}}
	if got := g.getUncommitted(false); len(got["t"]) != 0 {
		t.Fatalf("an entry recorded under the old topic's ID is committable: %v", got)
	}

	g.updateUncommitted(Fetches{{Topics: []FetchTopic{{
		Topic:      "t",
		TopicID:    newID,
		Partitions: []FetchPartition{{Partition: 0, Records: []*Record{{Offset: 4}}}},
	}}}})
	e := g.uncommitted["t"][0]
	if e.id != newID || e.committed.Offset != 0 || e.dirty.Offset != 5 {
		t.Fatalf("a poll of the current topic did not replace the old topic's entry: %+v", e)
	}
	if got := g.getUncommitted(true); got["t"][0].Offset != 5 {
		t.Fatalf("the replaced entry is not committable: %v", got)
	}
}

// TestTopicRecreation runs the core recreation contract against a real
// broker: after a delete and recreate under the same name, the producer
// continues with no surfaced error, and the consumer restarts from the new
// topic's beginning, delivering exactly the new topic's records.
func TestTopicRecreation(t *testing.T) {
	t.Parallel()

	topic, cleanup := tmpTopicPartitions(t, 1)
	defer cleanup()
	waitTopicReady(t, topic)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	producer, err := newTestClient(DefaultProduceTopic(topic))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	consumer, err := newTestClient(
		ConsumeTopics(topic),
		ConsumeResetOffset(NewOffset().AtStart()),
		FetchMaxWait(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	var a []string
	for i := range 5 {
		a = append(a, fmt.Sprintf("a%d", i))
		if err := producer.ProduceSync(ctx, StringRecord(a[i])).FirstErr(); err != nil {
			t.Fatalf("produce before recreation: %v", err)
		}
	}
	collectRecreationVals(ctx, t, consumer, a...)

	// Fewer records than the consumed offset: detection converges at
	// every version, in the worst case through an out of range reset.
	recreateTestTopic(t, topic, 1)
	for _, v := range []string{"b0", "b1"} {
		if err := producer.ProduceSync(ctx, StringRecord(v)).FirstErr(); err != nil {
			t.Fatalf("produce across recreation: %v", err)
		}
	}
	collectRecreationVals(ctx, t, consumer, "b0", "b1")

	if err := producer.ProduceSync(ctx, StringRecord("c0")).FirstErr(); err != nil {
		t.Fatalf("produce after recreation: %v", err)
	}
	collectRecreationVals(ctx, t, consumer, "c0")
}

// TestTopicRecreationTransactions runs the transactional recreation
// contract against a real broker: a transaction across a recreation either
// commits into the new topic, where a write by topic name landed before
// any metadata update, or fails loudly wrapping TRANSACTION_ABORTABLE or,
// for the commit refusal, OPERATION_NOT_ATTEMPTED. Aborting recovers, and
// within a few rounds a transaction commits cleanly into the new topic,
// which read-committed consumption then sees exactly.
//
// The contract needs topic IDs, so we skip below 2.8. Below 2.5 the broker
// also rejects our continued sequence with UNKNOWN_PRODUCER_ID, which is not
// recoverable there.
func TestTopicRecreationTransactions(t *testing.T) {
	t.Parallel()

	topic, cleanup := tmpTopicPartitions(t, 1)
	defer cleanup()
	waitTopicReady(t, topic)
	if metadataTopicID(topic) == ([16]byte{}) {
		t.Skip("metadata carries no topic IDs (below 2.8); the client cannot detect the recreation")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	cl, err := newTestClient(
		DefaultProduceTopic(topic),
		TransactionalID(randsha()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()

	if err := cl.BeginTransaction(); err != nil {
		t.Fatal(err)
	}
	if err := cl.ProduceSync(ctx, StringRecord("t0")).FirstErr(); err != nil {
		t.Fatalf("produce in the first transaction: %v", err)
	}
	if err := cl.EndTransaction(ctx, TryCommit); err != nil {
		t.Fatalf("commit before recreation: %v", err)
	}

	recreateTestTopic(t, topic, 1)

	loud := func(err error) bool {
		return errors.Is(err, kerr.TransactionAbortable) || errors.Is(err, kerr.OperationNotAttempted)
	}
	var committed string
	for round := 0; ; round++ {
		if round > 8 || ctx.Err() != nil {
			t.Fatal("no transaction committed within the retry budget after recreation")
		}
		if err := cl.BeginTransaction(); err != nil {
			t.Fatalf("begin round %d: %v", round, err)
		}
		val := fmt.Sprintf("post%d", round)
		perr := cl.ProduceSync(ctx, StringRecord(val)).FirstErr()
		var cerr error
		if perr == nil {
			cerr = cl.EndTransaction(ctx, TryCommit)
		}
		t.Logf("round %d: produce err %v; commit err %v", round, perr, cerr)
		if perr == nil && cerr == nil {
			committed = val
			break
		}
		for _, err := range []error{perr, cerr} {
			if err != nil && !loud(err) {
				t.Fatalf("round %d failed outside the designed loud classes: %v", round, err)
			}
		}
		if err := cl.EndTransaction(ctx, TryAbort); err != nil {
			t.Fatalf("abort after recreation (round %d): %v", round, err)
		}
	}

	// Read committed sees exactly the committed transaction; the first
	// transaction's record died with the old topic.
	consumer, err := newTestClient(
		ConsumeTopics(topic),
		ConsumeResetOffset(NewOffset().AtStart()),
		FetchIsolationLevel(ReadCommitted()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	collectRecreationVals(ctx, t, consumer, committed)
}

// TestTopicRecreationPausedAdopts pins against a real broker that a
// metadata observation alone adopts a recreation: a paused consumer fetches
// nothing, so no broker can reject the ID it holds, and it still swaps on
// the next metadata refresh. Only metadata with topic IDs (2.8+) can show a
// recreation this way, so we skip below that.
func TestTopicRecreationPausedAdopts(t *testing.T) {
	t.Parallel()

	topic, cleanup := tmpTopicPartitions(t, 1)
	defer cleanup()
	waitTopicReady(t, topic)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	producer, err := newTestClient(DefaultProduceTopic(topic))
	if err != nil {
		t.Fatal(err)
	}
	defer producer.Close()
	var lc logCapture
	consumer, err := newTestClient(
		ConsumeTopics(topic),
		ConsumeResetOffset(NewOffset().AtStart()),
		FetchMaxWait(time.Second),
		WithLogger(&lc),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()

	for _, v := range []string{"a0", "a1"} {
		if err := producer.ProduceSync(ctx, StringRecord(v)).FirstErr(); err != nil {
			t.Fatalf("produce before recreation: %v", err)
		}
	}
	collectRecreationVals(ctx, t, consumer, "a0", "a1")

	if metadataTopicID(topic) == ([16]byte{}) {
		t.Skip("metadata carries no topic IDs (below 2.8); a paused consumer has no signal to adopt")
	}

	// No fetches while paused: nothing can produce wire evidence, so the
	// only path to a swap is the metadata observation.
	consumer.PauseFetchTopics(topic)
	time.Sleep(2 * time.Second) // drain in-flight fetches (FetchMaxWait is 1s)

	recreateTestTopic(t, topic, 1)
	consumer.ForceMetadataRefresh()

	const swapMsg = "topic recreation detected"
	deadline := time.Now().Add(10 * time.Second)
	for !lc.contains(swapMsg) && time.Now().Before(deadline) {
		time.Sleep(100 * time.Millisecond)
	}
	if !lc.contains(swapMsg) {
		t.Fatal("paused consumer did not adopt the recreation from a single metadata observation")
	}

	// The new topic reads from its beginning.
	for _, v := range []string{"b0", "b1"} {
		if err := producer.ProduceSync(ctx, StringRecord(v)).FirstErr(); err != nil {
			t.Fatalf("produce across recreation: %v", err)
		}
	}
	consumer.ResumeFetchTopics(topic)
	collectRecreationVals(ctx, t, consumer, "b0", "b1")
}

// TestRecreationEOS runs a producer, a group consumer, and a transactional
// read-process-write against a real broker while the input topic is deleted
// and recreated under all three. The producer writes total records to the
// input topic and recreates it every perLife of them without flushing, so
// records are in flight and buffered at every recreation. The group
// consumer reads the input topic throughout. The transact session reads the
// input topic and writes every record it reads to the output topic.
//
// Every produce must land, the group consumer must see every record of the
// final incarnation and no data loss, and the output topic read committed
// must hold each of those records exactly once. Earlier incarnations can
// repeat in the output: their input was deleted, so a transaction that read
// them either aborts or commits without their offsets, and the records the
// next round reads are a new incarnation's.
//
// The contract needs topic IDs, so we skip below 2.8.
func TestRecreationEOS(t *testing.T) {
	t.Parallel()

	total := testRecordLimit
	if total < 10 {
		t.Skipf("KGO_TEST_RECORDS=%d is too small for eight recreations", total)
	}
	perLife := total / 10     // records produced between recreations
	finalStart := perLife * 8 // the last recreation; what follows is the final incarnation
	finalN := total - finalStart

	in, inCleanup := tmpTopicPartitions(t, 4)
	defer inCleanup()
	out, outCleanup := tmpTopicPartitions(t, 4)
	defer outCleanup()
	waitTopicReady(t, in)
	waitTopicReady(t, out)
	// Detection needs metadata topic IDs (2.8), but the exactly once
	// contract asserted below needs fetch v13 (3.1). Below v13 a fetch is
	// served by name at the old topic's offset, so the new topic's
	// records can be read at the wrong position and delivered again after
	// the swap restarts the partition.
	if !allowFetchTopicIDs {
		t.Skip("fetch is below v13 (broker below 3.1), so fetches go by topic name")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// idx returns a record's index within the final incarnation.
	idx := func(v []byte) (int, bool) {
		i, _ := strconv.Atoi(string(v))
		return i - finalStart, i >= finalStart
	}

	// The group consumer reads the input topic until it has seen every
	// record of the final incarnation.
	rgroup, rgroupCleanup := tmpGroup(t)
	defer rgroupCleanup()
	rcl, err := newTestClient(
		ConsumeTopics(in),
		ConsumerGroup(rgroup),
		ConsumeResetOffset(NewOffset().AtStart()),
		MetadataMinAge(250*time.Millisecond),
		FetchMaxWait(time.Second),
		WithLogger(testLogger()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer rcl.Close()
	seen := make([]bool, finalN)
	nseen := 0
	rdone := make(chan struct{})
	go func() {
		defer close(rdone)
		for nseen < finalN && ctx.Err() == nil {
			fs := rcl.PollFetches(ctx)
			fs.EachError(func(_ string, _ int32, err error) {
				if errors.As(err, new(*ErrDataLoss)) {
					t.Errorf("data loss consuming the input topic: %v", err)
				}
			})
			fs.EachRecord(func(r *Record) {
				if i, ok := idx(r.Value); ok && !seen[i] {
					seen[i] = true
					nseen++
				}
			})
		}
	}()

	// The transact session reads the input topic and writes what it reads
	// to the output topic. A transaction that fails aborts itself and the
	// next one picks up from the last commit.
	tgroup, tgroupCleanup := tmpGroup(t)
	defer tgroupCleanup()
	s, err := NewGroupTransactSession(testClientOpts(
		ConsumeTopics(in),
		ConsumerGroup(tgroup),
		TransactionalID(randsha()),
		ConsumeResetOffset(NewOffset().AtStart()),
		MetadataMinAge(250*time.Millisecond),
		FetchMaxWait(time.Second),
		UnknownTopicRetries(-1),
		WithLogger(testLogger()),
	)...)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	var tstop atomic.Bool
	tdone := make(chan struct{})
	go func() {
		defer close(tdone)
		for !tstop.Load() && ctx.Err() == nil {
			if err := s.Begin(); err != nil {
				t.Errorf("begin transaction: %v", err)
				return
			}
			pctx, pcancel := context.WithTimeout(ctx, time.Second)
			s.PollFetches(pctx).EachRecord(func(r *Record) {
				s.Produce(ctx, &Record{Topic: out, Value: r.Value}, nil)
			})
			pcancel()
			if _, err := s.End(ctx, TryCommit); err != nil {
				t.Logf("transaction end: %v", err)
			}
		}
	}()

	// The producer recreates the topic as it goes. recreateTestTopic
	// fatals, so the producing runs on this goroutine.
	pcl, err := newTestClient(
		DefaultProduceTopic(in),
		MetadataMinAge(250*time.Millisecond),
		UnknownTopicRetries(-1), // the topic is gone for a moment at every recreation
		WithLogger(testLogger()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer pcl.Close()
	var promises sync.WaitGroup
	for i := range total {
		if i > 0 && i%perLife == 0 && i <= finalStart {
			recreateTestTopic(t, in, 4)
		}
		promises.Add(1)
		pcl.Produce(ctx, StringRecord(strconv.Itoa(i)), func(_ *Record, err error) {
			defer promises.Done()
			if err != nil {
				t.Errorf("produce across the recreations: %v", err)
			}
		})
	}
	promises.Wait()
	<-rdone

	// The output topic, read committed, holds every record of the final
	// incarnation exactly once. We read while the transactions still run,
	// stop them once everything is there, then read what is left: a record
	// written twice is an exactly once violation.
	vcl, err := newTestClient(
		ConsumeTopics(out),
		ConsumeResetOffset(NewOffset().AtStart()),
		FetchIsolationLevel(ReadCommitted()),
		FetchMaxWait(time.Second),
		WithLogger(testLogger()),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer vcl.Close()
	counts := make([]int, finalN)
	distinct := 0
	count := func(fs Fetches) {
		fs.EachRecord(func(r *Record) {
			if i, ok := idx(r.Value); ok {
				counts[i]++
				if counts[i] == 1 {
					distinct++
				}
			}
		})
	}
	for distinct < finalN && ctx.Err() == nil {
		count(vcl.PollFetches(ctx))
	}
	tstop.Store(true)
	<-tdone
	pctx, pcancel := context.WithTimeout(ctx, 5*time.Second)
	count(vcl.PollFetches(pctx))
	pcancel()

	var dups int
	for _, n := range counts {
		if n > 1 {
			dups++
		}
	}
	if nseen != finalN || distinct != finalN || dups > 0 {
		t.Fatalf("of the final %d records the consumer saw %d, %s holds %d, and %d are in %s more than once",
			finalN, nseen, out, distinct, dups, out)
	}
}
